"""The harness namespace: context, compaction, models, and the thread goal."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Context:
    tokens: int | None
    window: int | None
    percent: float | None
    compaction_scheduled: bool


@dataclass(frozen=True)
class Model:
    provider: str
    id: str
    name: str
    selector: str


@dataclass(frozen=True)
class Goal:
    objective: str
    status: str
    token_budget: int | None
    tokens_used: int
    remaining_tokens: int | None
    elapsed_ms: int


async def context() -> Context:
    from . import host_request

    payload = await host_request("compact.status")
    percent = payload.get("percent")
    if percent is not None and (isinstance(percent, bool) or not isinstance(percent, (int, float))):
        raise RuntimeError("compact.status returned an invalid percent")
    scheduled = payload.get("scheduled")
    if not isinstance(scheduled, bool):
        raise RuntimeError("compact.status returned an invalid scheduled flag")
    return Context(
        tokens=_optional_int(payload, "tokens"),
        window=_optional_int(payload, "context_window"),
        percent=percent,
        compaction_scheduled=scheduled,
    )


async def compact(instructions: str | None = None) -> bool:
    """Schedule a compaction for the next turn boundary; False when there is nothing to compact."""
    if instructions is not None and not isinstance(instructions, str):
        raise TypeError(f"instructions must be str or None, got {type(instructions).__name__}")
    from . import host_request

    payload: dict[str, Any] = {}
    if instructions is not None:
        payload["instructions"] = instructions
    reply = await host_request("compact.run", payload)
    scheduled = reply.get("scheduled")
    if not isinstance(scheduled, bool):
        raise RuntimeError("compact.run returned an invalid scheduled flag")
    return scheduled


async def models(query: str = "", limit: int = 8) -> list[Model]:
    if not isinstance(query, str):
        raise TypeError(f"query must be str, got {type(query).__name__}")
    if isinstance(limit, bool) or not isinstance(limit, int):
        raise TypeError(f"limit must be int, got {type(limit).__name__}")
    from . import host_request

    payload = await host_request("rlm.find_models", {"query": query, "limit": limit})
    rows = payload.get("models")
    if not isinstance(rows, list):
        raise RuntimeError("rlm.find_models returned an invalid models list")
    return [_model_from_row(row) for row in rows]


class _GoalNamespace:
    async def create(self, objective: str, token_budget: int | None = None) -> Goal:
        if not isinstance(objective, str):
            raise TypeError(f"objective must be str, got {type(objective).__name__}")
        if token_budget is not None and (isinstance(token_budget, bool) or not isinstance(token_budget, int)):
            raise TypeError(f"token_budget must be int or None, got {type(token_budget).__name__}")
        from . import host_request

        payload: dict[str, Any] = {"objective": objective}
        if token_budget is not None:
            payload["token_budget"] = token_budget
        return _goal_from_response(await host_request("goal.create", payload))

    async def get(self) -> Goal | None:
        from . import host_request

        return _goal_from_response(await host_request("goal.get"))

    async def complete(self) -> Goal:
        from . import host_request

        goal = _goal_from_response(await host_request("goal.complete"))
        if goal is None:
            raise RuntimeError("goal.complete returned no goal")
        return goal


goal = _GoalNamespace()


def _model_from_row(row: Any) -> Model:
    if not isinstance(row, dict):
        raise RuntimeError("rlm.find_models returned an invalid model entry")
    if not all(isinstance(row.get(field), str) and row[field] for field in ("provider", "id", "name", "selector")):
        raise RuntimeError("rlm.find_models returned an invalid model entry")
    return Model(provider=row["provider"], id=row["id"], name=row["name"], selector=row["selector"])


def _goal_from_response(response: Any) -> Goal | None:
    if not isinstance(response, dict):
        raise RuntimeError("goal response is invalid")
    row = response.get("goal")
    if row is None:
        return None
    if not isinstance(row, dict):
        raise RuntimeError("goal response is invalid")
    objective = row.get("objective")
    status = row.get("status")
    if not isinstance(objective, str) or not isinstance(status, str):
        raise RuntimeError("goal response is invalid")
    tokens_used = row.get("tokens_used")
    if isinstance(tokens_used, bool) or not isinstance(tokens_used, int):
        raise RuntimeError("goal response is invalid")
    elapsed_seconds = row.get("time_used_seconds")
    if isinstance(elapsed_seconds, bool) or not isinstance(elapsed_seconds, int):
        raise RuntimeError("goal response is invalid")
    return Goal(
        objective=objective,
        status=status,
        token_budget=_optional_int(row, "token_budget"),
        tokens_used=tokens_used,
        remaining_tokens=_optional_int(response, "remaining_tokens"),
        elapsed_ms=elapsed_seconds * 1000,
    )


def _optional_int(payload: dict[str, Any], field: str) -> int | None:
    value = payload.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"goal response has an invalid {field}")
    return value
