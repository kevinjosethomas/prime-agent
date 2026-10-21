"""The agents namespace: spawning, family rosters, and child results."""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

Relationship = Literal["parent", "sibling", "child"]

# `list` below names the roster call, so the builtin stays reachable for checks.
_builtin_list = list
Status = Literal["running", "idle", "done", "error"]

NOTE_MAX_LENGTH = 512
_COLLECT_MAX_TIMEOUT_MS = 2_147_483_647
_MESSAGE_DISPLAY_MIME = "application/vnd.prime-agent.agent-message+json"

_REGISTRY_STATUS = {"running": "running", "completed": "done", "error": "error", "cancelled": "error"}
_ROSTER_STATUS = {"running": "running", "idle": "idle", "inactive": "idle"}
_COLLECT_STATUS = {"queued": "running", "running": "running", "done": "done", "error": "error", "cancelled": "error"}


@dataclass(frozen=True)
class Agent:
    """One family member; a snapshot taken when it was returned."""

    name: str | None
    session_id: str | None
    relationship: Relationship
    status: Status
    note: str | None = None
    model: str | None = None
    child_id: str | None = None
    session_dir: Path | None = None
    top_level: bool = False

    async def send(self, text: str) -> "Receipt":
        if not isinstance(text, str):
            raise TypeError(f"message must be str, got {type(text).__name__}")
        from . import host_request

        payload: dict[str, Any] = {"message": text, "receiver_role": self.relationship}
        if self.relationship != "parent":
            selector = self.session_id or self.child_id or self.name
            if not selector:
                raise ValueError("this Agent has no addressable name, session id, or child id")
            payload["receiver_name"] = selector
        receipt = await host_request("agent_message.send", payload)
        _emit_sent_message(receipt, self.relationship)
        return _receipt_from_payload(receipt)

    async def recent(self, limit: int = 5) -> list["Message"]:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError(f"limit must be int, got {type(limit).__name__}")
        from . import host_request

        target = self.session_id or self.child_id or self.name
        if not target:
            raise ValueError("this Agent has no addressable name, session id, or child id")
        payload = await host_request(
            "agent_observe.recent", {"target": target, "limit": limit, "max_chars": 800}
        )
        rows = payload.get("messages")
        if not isinstance(rows, _builtin_list):
            raise RuntimeError("agent_observe.recent returned an invalid messages list")
        return [_message_from_row(row) for row in rows]

    async def wait(self, timeout: float | None = None) -> "Result":
        """Block until this child settles; ``timeout`` is seconds, ``None`` waits forever."""
        child_id = self._require_child_id()
        if timeout is None:
            timeout_ms = _COLLECT_MAX_TIMEOUT_MS
        elif isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout < 0:
            raise TypeError("timeout must be a non-negative number of seconds or None")
        else:
            timeout_ms = min(int(timeout * 1000), _COLLECT_MAX_TIMEOUT_MS)
        while True:
            row = (await _collect_children([child_id], timeout_ms))[0]
            if row.settled or timeout is not None:
                return _result_from_row(row)

    async def stop(self) -> None:
        await _delete_child(self._require_child_id())

    async def rename(self, name: str) -> "Agent":
        child_id = self._require_child_id()
        new_name = await _rename(name, child_id)
        return dataclasses.replace(self, name=new_name)

    def _require_child_id(self) -> str:
        if not self.child_id:
            raise TypeError("this Agent is not one of this session's spawned agents")
        return self.child_id


@dataclass(frozen=True)
class Receipt:
    delivered: bool
    message_id: str


@dataclass(frozen=True)
class Message:
    role: str
    text: str
    timestamp: int | None


@dataclass(frozen=True)
class Result:
    status: Status
    answer: str | None
    error: str | None
    duration_ms: int | None
    tool_use_count: int | None


async def spawn(
    prompt: str,
    *,
    name: str | None = None,
    model: str | None = None,
    thinking: str | None = None,
    top_level: bool = False,
    cwd: str | None = None,
) -> Agent:
    """Spawn a child (``top_level=False``) or a resident depth-0 peer (``top_level=True``)."""
    if not isinstance(prompt, str):
        raise TypeError(f"prompt must be str, got {type(prompt).__name__}")
    from . import host_request

    kwargs: dict[str, Any] = {}
    if name is not None:
        kwargs["name"] = name
    if model is not None:
        kwargs["model"] = model
    if thinking is not None:
        kwargs["thinking"] = thinking
    if top_level:
        if cwd is not None:
            kwargs["cwd"] = cwd
        payload = await host_request("rlm.create_session", {"prompt": prompt, "kwargs": kwargs})
        _require_fields(payload, ("active_session_id", "session_id", "name", "session_file", "model"), "rlm.create_session")
        child_id = payload.get("rlm_child_id")
        if child_id is not None and not isinstance(child_id, str):
            raise RuntimeError("rlm.create_session returned an invalid rlm_child_id")
        return Agent(
            name=payload["name"],
            session_id=payload["session_id"],
            relationship="child",
            status="running",
            model=payload["model"],
            child_id=child_id,
            top_level=True,
        )
    if name is None:
        raise TypeError("a child needs a name: agents.spawn(prompt, name='worker')")
    payload = await host_request("rlm.run", {"prompt": prompt, "kwargs": kwargs})
    _require_fields(payload, ("rlm_child_id", "name", "session_dir", "model"), "rlm.spawn")
    session_id = payload.get("session_id")
    if session_id is not None and not isinstance(session_id, str):
        raise RuntimeError("rlm.spawn returned an invalid session_id")
    return Agent(
        name=payload["name"],
        session_id=session_id,
        relationship="child",
        status="running",
        model=payload["model"],
        child_id=payload["rlm_child_id"],
        session_dir=Path(payload["session_dir"]),
    )


async def list() -> list[Agent]:
    """Every agent this session started: children and top-level peers."""
    rows = await _list_children()
    return [
        Agent(
            name=row.session_name,
            session_id=row.session_id,
            relationship="child",
            status=_REGISTRY_STATUS[row.status],
            note=row.note,
            model=None,
            child_id=row.rlm_child_id,
            session_dir=row.session_dir,
            top_level=row.top_level,
        )
        for row in rows
    ]


async def parent() -> Agent | None:
    """This session's parent, or None at depth 0."""
    for row in await _roster():
        if isinstance(row, dict) and row.get("relationship") == "parent":
            return _agent_from_roster_row(row)
    return None


async def siblings() -> list[Agent]:
    """The root sessions beside this one (all depth-0 siblings see each other)."""
    return [
        _agent_from_roster_row(row)
        for row in await _roster()
        if isinstance(row, dict) and row.get("relationship") == "sibling"
    ]


async def broadcast(text: str) -> list[Receipt]:
    """Send one message to every family member; per-member failures report delivered=False."""
    if not isinstance(text, str):
        raise TypeError(f"message must be str, got {type(text).__name__}")
    from . import host_request

    payload = await host_request("agent_message.send", {"target": "all", "message": text})
    receipts = payload.get("receipts")
    if not isinstance(receipts, _builtin_list):
        raise RuntimeError("agent_message.send returned an invalid receipts list")
    out: list[Receipt] = []
    for row in receipts:
        if not isinstance(row, dict):
            raise RuntimeError("agent_message.send returned an invalid receipt")
        if "deliveryStatus" in row:
            _emit_sent_message(row)
            message_id = row.get("id")
            out.append(Receipt(delivered=row["deliveryStatus"] == "delivered", message_id=str(message_id or "")))
        else:
            out.append(Receipt(delivered=False, message_id=""))
    return out


async def note(text: str) -> bool:
    """Report one-line progress to the parent; False when throttled (about one per 10 s)."""
    if not isinstance(text, str):
        raise TypeError(f"message must be str, got {type(text).__name__}")
    stripped = text.strip()
    if not stripped:
        raise ValueError("message must not be empty")
    # The host measures message.length in UTF-16 code units; measure the same way.
    if len(stripped.encode("utf-16-le", "surrogatepass")) // 2 > NOTE_MAX_LENGTH:
        raise ValueError(f"message must be at most {NOTE_MAX_LENGTH} characters")
    from . import host_request

    payload = await host_request("rlm.progress.note", {"message": stripped})
    accepted = payload.get("accepted")
    if not isinstance(accepted, bool):
        raise RuntimeError("rlm.progress.note returned an invalid accepted flag")
    return accepted


async def rename(name: str) -> str:
    """Rename this session; returns the applied name."""
    return await _rename(name, None)


# -- internal host-request helpers (the factory executor shares them) ----------


@dataclass(frozen=True)
class _SpawnRow:
    rlm_child_id: str
    name: str
    session_dir: Path
    model: str
    session_id: str | None


@dataclass(frozen=True)
class _ChildRow:
    rlm_child_id: str
    session_id: str | None
    session_name: str
    session_dir: Path
    status: str
    note: str | None
    top_level: bool


@dataclass(frozen=True)
class _CollectRow:
    rlm_child_id: str
    session_name: str | None
    session_dir: Path | None
    status: str
    settled: bool
    answer_preview: str | None
    answer_text: str | None
    error: str | None
    duration_ms: int | None
    tool_use_count: int | None
    replied_since_task: bool | None


async def _spawn_child(
    prompt: str,
    *,
    name: str,
    model: str | None = None,
    thinking: str | None = None,
) -> _SpawnRow:
    if not isinstance(prompt, str):
        raise TypeError(f"prompt must be str, got {type(prompt).__name__}")
    from . import host_request

    kwargs: dict[str, Any] = {"name": name}
    if model is not None:
        kwargs["model"] = model
    if thinking is not None:
        kwargs["thinking"] = thinking
    payload = await host_request("rlm.run", {"prompt": prompt, "kwargs": kwargs})
    _require_fields(payload, ("rlm_child_id", "name", "session_dir", "model"), "rlm.spawn")
    session_id = payload.get("session_id")
    if session_id is not None and not isinstance(session_id, str):
        raise RuntimeError("rlm.spawn returned an invalid session_id")
    return _SpawnRow(
        rlm_child_id=payload["rlm_child_id"],
        name=payload["name"],
        session_dir=Path(payload["session_dir"]),
        model=payload["model"],
        session_id=session_id,
    )


async def _list_children() -> list[_ChildRow]:
    from . import host_request

    payload = await host_request("rlm.list_subagents")
    rows = payload.get("subagents")
    if not isinstance(rows, _builtin_list):
        raise RuntimeError("rlm.list_subagents returned an invalid subagents registry")
    return [_child_row(entry) for entry in rows]


async def _collect_children(targets: list[str], timeout_ms: int = 0) -> list[_CollectRow]:
    if isinstance(timeout_ms, bool) or not isinstance(timeout_ms, int) or timeout_ms < 0:
        raise TypeError("timeout_ms must be a non-negative int")
    if not isinstance(targets, _builtin_list) or not all(isinstance(target, str) and target.strip() for target in targets):
        raise TypeError("targets must be a list of child ids or names")
    from . import host_request

    payload = await host_request(
        "rlm.collect", {"targets": [target.strip() for target in targets], "timeout_ms": timeout_ms}
    )
    rows = payload.get("results")
    if not isinstance(rows, _builtin_list):
        raise RuntimeError("rlm.collect returned an invalid results list")
    return [_collect_row(entry) for entry in rows]


async def _delete_child(target: str) -> _ChildRow:
    selector = target.strip() if isinstance(target, str) else ""
    if not selector:
        raise ValueError("target must not be empty")
    from . import host_request

    payload = await host_request("rlm.delete_subagent", {"target": selector})
    return _child_row(payload.get("subagent"))


async def _rename(new_name: str, session_id: str | None) -> str:
    if not isinstance(new_name, str):
        raise TypeError(f"name must be str, got {type(new_name).__name__}")
    from . import host_request

    payload: dict[str, Any] = {"name": new_name}
    if session_id is not None:
        payload["session_id"] = session_id
    reply = await host_request("rlm.rename", payload)
    name = reply.get("name")
    if not isinstance(name, str):
        raise RuntimeError("rlm.rename returned an invalid name")
    return name


async def _roster() -> list[Any]:
    from . import host_request

    payload = await host_request("agent_observe.list")
    rows = payload.get("agents")
    if not isinstance(rows, _builtin_list):
        raise RuntimeError("agent_observe.list returned an invalid agents list")
    return rows


def _agent_from_roster_row(row: dict[str, Any]) -> Agent:
    if not isinstance(row, dict):
        raise RuntimeError("agent_observe.list returned an invalid roster row")
    relationship = row.get("relationship")
    if relationship not in ("parent", "sibling", "child"):
        raise RuntimeError("agent_observe.list row has an invalid relationship")
    status = row.get("status")
    if status not in _ROSTER_STATUS:
        raise RuntimeError("agent_observe.list row has an invalid status")
    session_id = row.get("sessionId")
    if not isinstance(session_id, str) or not session_id:
        raise RuntimeError("agent_observe.list row is missing sessionId")
    note = row.get("note")
    if note is not None and not isinstance(note, str):
        raise RuntimeError("agent_observe.list row has an invalid note")
    name = row.get("sessionName")
    return Agent(
        name=name if isinstance(name, str) else None,
        session_id=session_id,
        relationship=relationship,
        status=_ROSTER_STATUS[status],
        note=note,
    )


def _receipt_from_payload(receipt: Any) -> Receipt:
    if not isinstance(receipt, dict):
        raise RuntimeError("agent_message.send returned an invalid receipt")
    message_id = receipt.get("id")
    if not isinstance(message_id, str) or not message_id:
        raise RuntimeError("agent_message.send returned an invalid receipt")
    return Receipt(delivered=receipt.get("deliveryStatus") == "delivered", message_id=message_id)


def _message_from_row(row: Any) -> Message:
    if not isinstance(row, dict):
        raise RuntimeError("agent_observe.recent returned an invalid message row")
    role = row.get("role")
    text = row.get("text")
    if not isinstance(role, str) or not isinstance(text, str):
        raise RuntimeError("agent_observe.recent returned an invalid message row")
    timestamp = row.get("timestamp")
    if timestamp is not None and (isinstance(timestamp, bool) or not isinstance(timestamp, int)):
        raise RuntimeError("agent_observe.recent returned an invalid message timestamp")
    return Message(role=role, text=text, timestamp=timestamp)


def _result_from_row(row: _CollectRow) -> Result:
    return Result(
        status=_COLLECT_STATUS[row.status],
        answer=row.answer_text or row.answer_preview,
        error=row.error,
        duration_ms=row.duration_ms,
        tool_use_count=row.tool_use_count,
    )


def _require_fields(payload: Any, fields: tuple[str, ...], operation: str) -> None:
    if not isinstance(payload, dict) or not all(
        isinstance(payload.get(field), str) and payload[field] for field in fields
    ):
        raise RuntimeError(f"{operation} returned an invalid payload")


def _optional_str(payload: dict[str, Any], field: str, operation: str) -> str | None:
    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"{operation} entry has an invalid {field}")
    return value


def _optional_int(payload: dict[str, Any], field: str, operation: str) -> int | None:
    value = payload.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"{operation} entry has an invalid {field}")
    return value


def _optional_bool(payload: dict[str, Any], field: str, operation: str) -> bool | None:
    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise RuntimeError(f"{operation} entry has an invalid {field}")
    return value


def _child_row(entry: Any) -> _ChildRow:
    if not isinstance(entry, dict):
        raise RuntimeError("rlm.list_subagents entry is invalid")
    _require_fields(entry, ("rlm_child_id",), "rlm.list_subagents")
    child_id = entry["rlm_child_id"]
    if not isinstance(entry.get("session_name"), str) or not entry["session_name"]:
        raise RuntimeError("rlm.list_subagents entry is missing session_name")
    session_dir = entry.get("session_dir")
    if not isinstance(session_dir, str) or not session_dir:
        raise RuntimeError("rlm.list_subagents entry is missing session_dir")
    status = entry.get("status")
    if status not in _REGISTRY_STATUS:
        raise RuntimeError("rlm.list_subagents entry has an invalid status")
    session_id = _optional_str(entry, "session_id", "rlm.list_subagents")
    note = entry.get("note")
    if note is None:
        note = entry.get("progress_note")
    if note is not None and not isinstance(note, str):
        raise RuntimeError("rlm.list_subagents entry has an invalid note")
    top_level = entry.get("top_level")
    if top_level is None:
        top_level = False
    if not isinstance(top_level, bool):
        raise RuntimeError("rlm.list_subagents entry has an invalid top_level")
    return _ChildRow(
        rlm_child_id=child_id,
        session_id=session_id,
        session_name=entry["session_name"],
        session_dir=Path(session_dir),
        status=status,
        note=note,
        top_level=top_level,
    )


def _collect_row(entry: Any) -> _CollectRow:
    if not isinstance(entry, dict):
        raise RuntimeError("rlm.collect entry is invalid")
    _require_fields(entry, ("rlm_child_id",), "rlm.collect")
    status = entry.get("status")
    if status not in _COLLECT_STATUS:
        raise RuntimeError("rlm.collect entry has an invalid status")
    settled = entry.get("settled")
    if not isinstance(settled, bool):
        raise RuntimeError("rlm.collect entry has an invalid settled flag")
    session_dir = _optional_str(entry, "session_dir", "rlm.collect")
    return _CollectRow(
        rlm_child_id=entry["rlm_child_id"],
        session_name=_optional_str(entry, "session_name", "rlm.collect"),
        session_dir=Path(session_dir) if session_dir else None,
        status=status,
        settled=settled,
        answer_preview=_optional_str(entry, "answer_preview", "rlm.collect"),
        answer_text=_optional_str(entry, "answer_text", "rlm.collect"),
        error=_optional_str(entry, "error", "rlm.collect"),
        duration_ms=_optional_int(entry, "duration_ms", "rlm.collect"),
        tool_use_count=_optional_int(entry, "tool_use_count", "rlm.collect"),
        replied_since_task=_optional_bool(entry, "replied_since_task", "rlm.collect"),
    )


def _emit_sent_message(receipt: dict[str, Any], receiver_role: str | None = None) -> None:
    try:
        from . import emit

        label = "Agent message queued" if receipt.get("deliveryStatus") == "queued" else "Agent message sent"
        display_receipt = dict(receipt)
        if receiver_role in ("parent", "sibling", "child"):
            display_receipt["receiverRole"] = receiver_role
        emit({_MESSAGE_DISPLAY_MIME: display_receipt, "text/plain": label})
    except Exception:
        pass
