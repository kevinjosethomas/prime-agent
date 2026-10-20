"""RLM-native task state API backed by Primer's local store.

The daemon injects the trusted local reporter configuration into every kernel
environment it provisions (PRIME_AGENT_INTERNAL_TASKS_REPORTER); the package
binds that configuration lazily on first use. The package does not create a
separate database, listener, or agent engine. All state calls are async.
Importing performs no I/O and requires only the standard library.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._client import Client
    from rlm import RLMCreateSessionHandle, RLMSpawnHandle, RLMSubagent

from ._models import Link, Status, Task

__all__ = ["Link", "Status", "Task", "create", "get", "list"]

#: The trusted env var the daemon stamps on every kernel it provisions.
_REPORTER_ENV = "PRIME_AGENT_INTERNAL_TASKS_REPORTER"
#: The builtin list type, captured before the public `list` facade shadows it.
_LIST_TYPE = type([])

_client: "Client | None" = None


def _bind(client: "Client | None") -> None:
    """Internal kernel-bootstrap hook; only ever called with trusted local setup."""
    global _client
    _client = client


def _configured() -> "Client":
    global _client
    if _client is None:
        raw = os.environ.get(_REPORTER_ENV, "")
        if raw:
            _client = _from_environment(raw)
    if _client is None:
        raise RuntimeError("Tasks reporter is not configured; open /root in Primer")
    return _client


def _from_environment(raw: str) -> "Client":
    """Parse the trusted env configuration; never source it from task content."""
    from ._client import Client

    try:
        spec = json.loads(raw)
    except ValueError as error:
        raise RuntimeError(f"Invalid {_REPORTER_ENV} in the kernel environment") from error
    if not isinstance(spec, dict):
        raise RuntimeError(f"Invalid {_REPORTER_ENV} in the kernel environment")
    command = spec.get("command")
    env = spec.get("env")
    timeout = spec.get("timeout", 15)
    if (not isinstance(command, _LIST_TYPE) or not command
            or not all(isinstance(arg, str) and arg for arg in command)
            or (env is not None and not isinstance(env, dict))
            or not isinstance(timeout, (int, float))):
        raise RuntimeError(f"Invalid {_REPORTER_ENV} in the kernel environment")
    try:
        return Client(command=command, env=env, timeout=timeout)
    except ValueError as error:
        raise RuntimeError(f"Invalid {_REPORTER_ENV} in the kernel environment") from error


async def list(*, status: Status | None = None, archived: bool | None = False) -> tuple[Task, ...]:
    """List visible tasks; archived=True selects hidden ones, None includes all."""
    return await _configured().list(status=status, archived=archived)


async def get(id: str) -> Task:
    """Read one current immutable snapshot and async handle by stable ID."""
    return await _configured().get(id)


async def create(id: str, title: str, summary: str = "", description: str = "") -> Task:
    """Create a distinct task; raises when the ID already exists."""
    return await _configured().create(id, title, summary=summary, description=description)
