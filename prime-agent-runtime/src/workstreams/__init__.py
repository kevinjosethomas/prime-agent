"""RLM-native workstream state API backed by Prime Agent Desktop's local store.

Desktop supplies configure(command=..., env=...) once per kernel. The package
does not create a separate database, listener, or agent engine. Agent admission
delegates to native RLM. All state calls are async. Importing performs no I/O
and requires only the standard library.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Sequence

if TYPE_CHECKING:
    from rlm import RLMCreateSessionHandle, RLMSpawnHandle, RLMSubagent

from ._client import Client
from ._models import Link, Status, Step, Workstream
from ._transport import AssignmentError, DeliveryError, Report

__all__ = ["AssignmentError", "Client", "DeliveryError", "Link", "Report", "Status", "Step", "Workstream",
           "archive", "assign", "associate", "classify", "configuration", "configure", "context", "create", "get", "list",
           "restore", "retry", "spawn", "start_agent", "sync_agents", "update", "upsert"]

_client: Client | None = None


def configure(*, command: Sequence[str], env: Mapping[str, str] | None = None,
              timeout: float = 15) -> Client:
    """Bind trusted local reporter argv once, never a command from source content."""
    global _client
    _client = Client(command=command, env=env, timeout=timeout)
    return _client


def _configured() -> Client:
    if _client is None:
        raise RuntimeError("Workstreams reporter is not configured; open /root in Prime Agent Desktop")
    return _client


def configuration() -> dict[str, Any]:
    """Copy trusted local reporter configuration for another authorized kernel."""
    return _configured().configuration()


async def context() -> dict[str, Any]:
    """Read metadata, linked agents and recent turn IDs, without message bodies."""
    return await _configured().context()


async def list(*, status: Status | None = None, parent_id: str | None = None,
               archived: bool | None = False) -> tuple[Workstream, ...]:
    """List visible workstreams; archived=True selects hidden ones, None includes all."""
    return await _configured().list(status=status, parent_id=parent_id, archived=archived)


async def get(id: str) -> Workstream:
    """Read a current immutable snapshot and async handle by stable ID."""
    return await _configured().get(id)


async def create(id: str, title: str, **fields: Any) -> Workstream:
    """Create a distinct outcome; raises when the ID already exists."""
    return await _configured().create(id, title, **fields)


async def upsert(id: str, title: str, **fields: Any) -> Workstream:
    """Replace complete state for a stable ID; omitted fields take defaults."""
    return await _configured().upsert(id, title, **fields)


async def update(id: str, **changes: Any) -> Workstream:
    """Update named fields without resetting unrelated fields."""
    return await _configured().update(id, **changes)


async def archive(id: str) -> Workstream:
    """Hide a workstream without changing its status or timestamps."""
    return await _configured().archive(id)


async def restore(id: str) -> Workstream:
    """Show a workstream again without changing its status or timestamps."""
    return await _configured().restore(id)


async def associate(workstream_id: str, session_id: str, *, name: str, role: str = "worker") -> Any:
    """Link an actual observed Prime Agent session to a workstream."""
    return await _configured().associate(workstream_id, session_id, name=name, role=role)


async def assign(workstream_id: str, agent: RLMCreateSessionHandle | RLMSpawnHandle | RLMSubagent, *, role: str = "worker") -> Any:
    """Assign an actual native session/spawn handle or RLM roster row to an outcome."""
    return await _configured().assign(workstream_id, agent, role=role)


async def spawn(workstream_id: str, prompt: str, *, name: str, model: str | None = None,
                thinking: str | None = None, role: str = "worker") -> RLMSpawnHandle:
    """Spawn a native RLM child and inherit trusted local workstream setup."""
    return await _configured().spawn(workstream_id, prompt, name=name, model=model, thinking=thinking, role=role)


async def start_agent(workstream_id: str, prompt: str, *, name: str, model: str | None = None,
                      thinking: str | None = None, cwd: str | None = None,
                      role: str = "worker-parent") -> RLMCreateSessionHandle:
    """Create an independent native depth-0 agent; this is not an RLM child spawn."""
    return await _configured().start_agent(workstream_id, prompt, name=name, model=model,
                                           thinking=thinking, cwd=cwd, role=role)


async def sync_agents(workstream_id: str) -> tuple[RLMSubagent, ...]:
    """Observe the direct-child roster once and refresh only already assigned children."""
    return await _configured().sync_agents(workstream_id)


async def classify(turn_ids: Sequence[str], workstream_ids: Sequence[str]) -> Any:
    """Apply an explicitly user-requested correction, protected from automatic relabeling."""
    return await _configured().classify(turn_ids, workstream_ids)


async def retry(report: Report) -> Any:
    """Retry the same event ID and payload after uncertain delivery."""
    return await _configured().retry(report)
