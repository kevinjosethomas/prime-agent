"""Immutable workstream values and fluent async handles."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from ._client import Client
    from rlm import RLMCreateSessionHandle, RLMSpawnHandle, RLMSubagent

Status = Literal["todo", "active", "needs_input", "ready", "backlog", "done"]
STATUSES = ("todo", "active", "needs_input", "ready", "backlog", "done")


@dataclass(frozen=True)
class Step:
    """A real, independently checkable milestone; no guessed percentage."""

    id: str
    title: str
    done: bool = False


@dataclass(frozen=True)
class Link:
    kind: Literal["chat", "pr", "source"]
    url: str
    title: str | None = None


@dataclass(frozen=True)
class Workstream:
    """Snapshot plus handle. Mutations fetch current state and return a new snapshot."""

    id: str
    title: str
    status: Status
    summary: str
    question: str
    owner: str
    steps: tuple[Step, ...]
    updated_at: int | float
    _client: Client = field(repr=False, compare=False)
    description: str = ""
    parent_id: str | None = None
    links: tuple[Link, ...] = ()

    @property
    def progress(self) -> tuple[int, int] | None:
        """Completed/total explicit milestones, or None when no plan exists."""
        return (sum(step.done for step in self.steps), len(self.steps)) if self.steps else None

    async def refresh(self) -> Workstream:
        return await self._client.get(self.id)

    async def update(self, **changes: Any) -> Workstream:
        """Update named fields, preserving the latest unrelated state."""
        return await self._client.update(self.id, **changes)

    async def milestone(self, id: str, title: str, *, done: bool = False) -> Workstream:
        """Add or replace a named, explicit milestone."""
        current = await self.refresh()
        step = Step(id, title, done)
        steps = tuple(step if old.id == id else old for old in current.steps)
        if not any(old.id == id for old in current.steps):
            steps += (step,)
        return await self._client.update(self.id, steps=steps)

    async def check(self, step_id: str, *, done: bool = True) -> Workstream:
        """Record observed milestone completion; reject a mistyped milestone ID."""
        current = await self.refresh()
        if not any(step.id == step_id for step in current.steps):
            raise KeyError(f"Unknown milestone: {step_id}")
        steps = tuple(Step(step.id, step.title, done if step.id == step_id else step.done)
                      for step in current.steps)
        return await self._client.update(self.id, steps=steps)

    async def needs_input(self, question: str, *, summary: str | None = None) -> Workstream:
        """Show the actual decision or approval that prevents progress."""
        changes: dict[str, Any] = {"status": "needs_input", "question": question}
        if summary is not None:
            changes["summary"] = summary
        return await self.update(**changes)

    async def complete(self, *, summary: str | None = None) -> Workstream:
        """Complete actual work; unfinished explicit milestones are rejected."""
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
        """Spawn a native RLM child with this workstream and trusted local reporter setup."""
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

    async def subtask(self, id: str, title: str, **fields: Any) -> Workstream:
        """Create a distinct outcome under this workstream, without invented progress."""
        return await self._client.create(id, title, parent_id=self.id, **fields)
