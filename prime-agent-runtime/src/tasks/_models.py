"""Immutable task values and fluent async handles."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from ._client import Client
    from rlm import RLMCreateSessionHandle, RLMSpawnHandle, RLMSubagent

Status = Literal["todo", "active", "needs_input", "ready", "backlog", "done"]
STATUSES = ("todo", "active", "needs_input", "ready", "backlog", "done")
LINK_KINDS = ("chat", "pr", "source", "commit")


@dataclass(frozen=True)
class Link:
    """A chat, pull request, source, or commit (an http(s) commit URL, not a bare SHA)."""

    kind: Literal["chat", "pr", "source", "commit"]
    url: str
    title: str | None = None


@dataclass(frozen=True)
class Task:
    """Snapshot plus handle. Mutations fetch current state and return a new snapshot.

    A subtask is a task whose parent_id points at another task.
    """

    id: str
    title: str
    status: Status
    summary: str
    question: str
    owner: str
    updated_at: int | float
    _client: Client = field(repr=False, compare=False)
    description: str = ""
    parent_id: str | None = None
    links: tuple[Link, ...] = ()
    archived: bool = False

    async def refresh(self) -> Task:
        return await self._client.get(self.id)

    async def update(self, **changes: Any) -> Task:
        """Update named fields, preserving the latest unrelated state."""
        return await self._client.update(self.id, **changes)

    async def archive(self) -> Task:
        """Hide this task without changing its status or timestamps."""
        return await self._client.archive(self.id)

    async def restore(self) -> Task:
        """Show this task again without changing its status or timestamps."""
        return await self._client.restore(self.id)

    async def needs_input(self, question: str, *, summary: str | None = None) -> Task:
        """Show the actual decision or approval that prevents progress."""
        changes: dict[str, Any] = {"status": "needs_input", "question": question}
        if summary is not None:
            changes["summary"] = summary
        return await self.update(**changes)

    async def complete(self, *, summary: str | None = None) -> Task:
        """Mark actual finished work done."""
        changes: dict[str, Any] = {"status": "done", "question": ""}
        if summary is not None:
            changes["summary"] = summary
        return await self.update(**changes)

    async def associate(self, session_id: str, *, name: str, role: str = "worker") -> Any:
        """Link an actual Prime Agent session; does not spawn or message it."""
        return await self._client.associate(self.id, session_id, name=name, role=role)

    async def assign(self, agent: RLMCreateSessionHandle | RLMSpawnHandle | RLMSubagent, *, role: str = "worker") -> Any:
        """Assign an actual RLM spawn handle/roster row without guessing a session ID."""
        return await self._client.assign(self.id, agent, role=role)

    async def spawn(self, prompt: str, *, name: str, model: str | None = None,
                    thinking: str | None = None, role: str = "worker") -> RLMSpawnHandle:
        """Spawn a native RLM child with this task and trusted local reporter setup."""
        return await self._client.spawn(self.id, prompt, name=name, model=model, thinking=thinking, role=role)

    async def start_agent(self, prompt: str, *, name: str, model: str | None = None,
                          thinking: str | None = None, cwd: str | None = None,
                          role: str = "worker-parent") -> RLMCreateSessionHandle:
        """Start an independent native depth-0 agent, with its own optional RLM children."""
        return await self._client.start_agent(self.id, prompt, name=name, model=model,
                                              thinking=thinking, cwd=cwd, role=role)

    async def sync_agents(self) -> tuple[RLMSubagent, ...]:
        """Resolve assigned children to actual roster session IDs with one observation."""
        return await self._client.sync_agents(self.id)

    async def subtask(self, id: str, title: str, **fields: Any) -> Task:
        """Create a subtask under this task (a task with parent_id=self.id)."""
        return await self._client.create(id, title, parent_id=self.id, **fields)
