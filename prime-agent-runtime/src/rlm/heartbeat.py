"""The heartbeat namespace: recurring self-wakeups for this session."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# `list` below names the catalog call, so the builtin stays reachable for checks.
_builtin_list = list


@dataclass(frozen=True)
class Heartbeat:
    id: str
    instruction: str
    every: str
    label: str | None
    status: str
    delivery_mode: str
    next_run_at: str | None

    async def update(
        self,
        instruction: str | None = None,
        every: str | None = None,
        label: str | None = None,
        delivery_mode: str | None = None,
    ) -> "Heartbeat":
        payload: dict[str, Any] = {"id": self.id}
        if instruction is not None:
            payload["instruction"] = _string(instruction, "instruction")
        if every is not None:
            payload["interval"] = _string(every, "every")
        if label is not None:
            payload["label"] = _string(label, "label")
        delivery_mode = _normalize_delivery_mode(delivery_mode)
        if delivery_mode is not None:
            payload["delivery_mode"] = delivery_mode
        return _from_response(await _host_request("rlm_heartbeat.update", payload))

    async def pause(self) -> "Heartbeat":
        return await self._status("pause")

    async def resume(self) -> "Heartbeat":
        return await self._status("resume")

    async def delete(self) -> None:
        await _host_request("rlm_heartbeat.delete", {"id": self.id})

    async def _status(self, status: str) -> "Heartbeat":
        return _from_response(await _host_request("rlm_heartbeat.update", {"id": self.id, "status": status}))


async def create(
    instruction: str,
    every: str | None = None,
    label: str | None = None,
    delivery_mode: str | None = None,
) -> Heartbeat:
    """Create a recurring heartbeat; ``every`` defaults to the host's 5-minute schedule."""
    payload: dict[str, Any] = {"instruction": _string(instruction, "instruction")}
    if every is not None:
        payload["interval"] = _string(every, "every")
    if label is not None:
        payload["label"] = _string(label, "label")
    normalized = _normalize_delivery_mode(delivery_mode)
    if normalized is not None:
        payload["delivery_mode"] = normalized
    return _from_response(await _host_request("rlm_heartbeat.create", payload))


async def list(include_inactive: bool = False) -> list[Heartbeat]:
    if not isinstance(include_inactive, bool):
        raise TypeError(f"include_inactive must be bool, got {type(include_inactive).__name__}")
    response = await _host_request("rlm_heartbeat.list", {"include_inactive": include_inactive})
    rows = response.get("heartbeats")
    if not isinstance(rows, _builtin_list):
        raise RuntimeError("rlm_heartbeat.list returned an invalid heartbeats list")
    return [_from_row(row) for row in rows]


async def _host_request(request_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    from . import host_request

    return await host_request(request_type, payload)


def _normalize_delivery_mode(delivery_mode: str | None) -> str | None:
    if delivery_mode is None:
        return None
    if not isinstance(delivery_mode, str):
        raise TypeError(f"delivery_mode must be str or None, got {type(delivery_mode).__name__}")
    if delivery_mode not in ("steer", "follow_up"):
        raise ValueError('delivery_mode must be "steer", "follow_up", or None')
    return delivery_mode


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be str, got {type(value).__name__}")
    return value


def _from_response(response: Any) -> Heartbeat:
    if not isinstance(response, dict):
        raise RuntimeError("rlm_heartbeat response is invalid")
    row = response.get("heartbeat")
    if row is None:
        raise RuntimeError("rlm_heartbeat response carries no heartbeat")
    return _from_row(row)


def _from_row(row: Any) -> Heartbeat:
    if not isinstance(row, dict):
        raise RuntimeError("rlm_heartbeat response is invalid")
    for field in ("id", "instruction", "status", "delivery_mode"):
        if not isinstance(row.get(field), str) or not row[field]:
            raise RuntimeError(f"rlm_heartbeat heartbeat has an invalid {field}")
    schedule = row.get("schedule")
    if not isinstance(schedule, dict) or not isinstance(schedule.get("expression"), str):
        raise RuntimeError("rlm_heartbeat heartbeat has an invalid schedule")
    label = row.get("label")
    if label is not None and not isinstance(label, str):
        raise RuntimeError("rlm_heartbeat heartbeat has an invalid label")
    next_run_at = row.get("next_run_at")
    if next_run_at is not None and not isinstance(next_run_at, str):
        raise RuntimeError("rlm_heartbeat heartbeat has an invalid next_run_at")
    return Heartbeat(
        id=row["id"],
        instruction=row["instruction"],
        every=schedule["expression"],
        label=label,
        status=row["status"],
        delivery_mode=row["delivery_mode"],
        next_run_at=next_run_at,
    )
