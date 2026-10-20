"""Immutable task snapshots and fluent async handles."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from ._client import Client

Status = Literal["todo", "active", "needs_input", "ready", "backlog", "done"]
STATUSES = ("todo", "active", "needs_input", "ready", "backlog", "done")
LINK_KINDS = ("chat", "pr", "source", "commit")

# Keyword default meaning "leave this field unchanged".
UNSET = ...  # type: Any


@dataclass(frozen=True)
class Link:
    """A chat, pull request, source, or commit (an http(s) commit URL, not a bare SHA)."""

    kind: Literal["chat", "pr", "source", "commit"]
    url: str
    title: str | None = None


@dataclass(frozen=True)
class Task:
    """Snapshot plus handle. Mutations return a fresh snapshot of current state.

    A subtask is a task whose parent_id points at another task. The question
    holds the real blocking question while status is needs_input, and is empty
    in every other status.
    """

    id: str
    title: str
    status: Status
    question: str
    updated_at: int | float
    _client: Client = field(repr=False, compare=False)
    parent_id: str | None = None
    links: tuple[Link, ...] = ()
    archived: bool = False

    async def subtask(self, id: str, title: str, *, status: Status = "todo",
                      question: str = "") -> Task:
        """Create a subtask under this task (a task with parent_id=self.id)."""
        return await self._client.create(id, title, status=status, question=question,
                                        parent_id=self.id)

    async def update(self, *, status: Any = UNSET, question: Any = UNSET,
                     links: Any = UNSET) -> Task:
        """Patch the named fields; the host preserves every unrelated field."""
        return await self._client.update(self.id, status=status, question=question,
                                         links=links)

    async def archive(self) -> Task:
        """Hide this task without changing its status or timestamps."""
        return await self._client.archive(self.id)

    async def restore(self) -> Task:
        """Show this task again without changing its status or timestamps."""
        return await self._client.restore(self.id)

    async def link(self, agent: Any) -> Any:
        """Record an actual native handle/roster row or frozen Agent shape."""
        return await self._client.link(self.id, agent)
