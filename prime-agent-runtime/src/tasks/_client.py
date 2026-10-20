"""Agent-agnostic async adapter to canonical desktop task metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Sequence

if TYPE_CHECKING:
    from rlm import RLMCreateSessionHandle, RLMSpawnHandle, RLMSubagent

from ._models import UNSET, Link, Status, Task, STATUSES
from ._transport import DeliveryError, Report, Reporter
from ._validation import text, validate_changes, validate_question, validate_task


class Client:
    """Internal reporter configuration for a kernel, hook, or local integration."""

    def __init__(self, *, command: Sequence[str],
                 env: Mapping[str, str] | None = None, timeout: float = 15):
        self._reporter = Reporter(command, env, timeout)

    async def _context(self) -> dict[str, Any]:
        """Read current state and recent turn IDs; message bodies are not stored here."""
        value = await self._reporter.call("context")
        if not isinstance(value, dict) or not isinstance(value.get("tasks"), list):
            raise DeliveryError("Tasks reporter returned an invalid snapshot")
        return value

    def _handle(self, value: dict[str, Any]) -> Task:
        return Task(
            id=value["id"], title=value["title"], status=value["status"],
            question=value.get("question", ""),
            updated_at=value["updatedAt"],
            parent_id=value.get("parentId"),
            links=tuple(Link(**link) for link in value.get("links", [])),
            archived=value.get("archived", False), _client=self,
        )

    async def list(self, *, status: Status | None = None,
                   archived: bool | None = False) -> tuple[Task, ...]:
        """List visible snapshots; archived=True selects hidden ones, None includes all."""
        if status is not None and status not in STATUSES:
            raise ValueError("Invalid task status")
        if archived is not None and type(archived) is not bool:
            raise TypeError("archived must be a boolean or None")
        return tuple(self._handle(value) for value in (await self._context())["tasks"]
                     if (status is None or value["status"] == status)
                     and (archived is None or value.get("archived", False) == archived))

    async def get(self, id: str) -> Task:
        """Read one current task; raise KeyError when the ID does not exist."""
        text(id, "task ID")
        for value in (await self._context())["tasks"]:
            if value["id"] == id:
                return self._handle(value)
        raise KeyError(f"Unknown task: {id}")

    async def create(self, id: str, title: str, *, status: Status = "todo",
                     question: str = "", parent_id: str | None = None) -> Task:
        """Create a distinct task; the host rejects an existing ID atomically."""
        if any(value["id"] == id for value in (await self._context())["tasks"]):
            raise ValueError(f"Task already exists: {id}")
        value: dict[str, Any] = dict(
            id=id, title=title, status=status, question=question,
            owner="/root", links=[])
        if parent_id is not None:
            value["parentId"] = parent_id
        await self._deliver(Report.create("create", task=validate_task(value)))
        return await self.get(id)

    async def update(self, id: str, *, status: Any = UNSET, question: Any = UNSET,
                     links: Any = UNSET) -> Task:
        """Patch named fields; the host preserves every unrelated field in one transaction."""
        current = await self.get(id)
        changes: dict[str, Any] = {}
        if status is not UNSET:
            if status not in STATUSES:
                raise ValueError("Invalid task status")
            changes["status"] = status
        if question is not UNSET:
            changes["question"] = text(question, "question", 2000, empty=True)
        if links is not UNSET:
            changes["links"] = links
        effective_status = changes.get("status", current.status)
        effective_question = changes.get("question", current.question)
        if effective_status == "needs_input" and not effective_question.strip():
            raise ValueError("Needs input requires the question")
        elif "question" in changes and changes["question"].strip():
            # Only needs_input carries a question; the host clears the current
            # question itself when the status moves away.
            raise ValueError("Only needs_input carries a question")
        if not changes:
            return current
        await self._deliver(Report.create("patch", taskId=id, changes=validate_changes(changes)))
        return await self.get(id)

    async def archive(self, id: str) -> Task:
        """Hide a task without changing its status or timestamps."""
        await self._deliver(Report.create("set-archived", taskId=text(id, "task ID"), archived=True))
        return await self.get(id)

    async def restore(self, id: str) -> Task:
        """Show a task again without changing its status or timestamps."""
        await self._deliver(Report.create("set-archived", taskId=text(id, "task ID"), archived=False))
        return await self.get(id)

    async def link(self, task_id: str,
                   agent: "RLMCreateSessionHandle | RLMSpawnHandle | RLMSubagent") -> Any:
        """Record an actual native spawn/create_session handle or roster row."""
        import rlm

        text(task_id, "task ID")
        if isinstance(agent, rlm.RLMCreateSessionHandle):
            agent_value = {"taskId": task_id,
                           "sessionId": text(agent.session_id, "session ID"),
                           "name": text(agent.name, "agent name", 160),
                           "role": "worker"}
            return await self._deliver(Report.create("associate", agent=agent_value))
        if not isinstance(agent, (rlm.RLMSpawnHandle, rlm.RLMSubagent)):
            raise TypeError("link expects an RLMCreateSessionHandle, RLMSpawnHandle or RLMSubagent")
        agent_value = {"taskId": task_id,
                       "rlmChildId": text(agent.rlm_child_id, "RLM child ID"),
                       "sessionDir": text(str(agent.session_dir), "session directory", 4096),
                       "name": text(agent.name if isinstance(agent, rlm.RLMSpawnHandle) else agent.session_name,
                                    "agent name", 160), "role": "worker"}
        if isinstance(agent, rlm.RLMSubagent) and agent.session_id is not None:
            agent_value["sessionId"] = text(agent.session_id, "session ID")
        return await self._deliver(Report.create("assign", agent=agent_value))

    async def _deliver(self, report: Report) -> Any:
        """Deliver a report; after uncertain delivery, redeliver the exact same event."""
        try:
            return await self._reporter.call("report", report)
        except DeliveryError:
            pass
        return await self._reporter.call("report", report)
