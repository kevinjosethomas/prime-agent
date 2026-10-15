"""Agent-agnostic async adapter to canonical desktop workstream metadata."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from ._models import Link, Status, Step, Workstream, STATUSES
from ._transport import DeliveryError, Report, Reporter
from ._validation import text, validate_stream


class Client:
    """Independent reporter configuration for a kernel, hook, or local integration."""

    def __init__(self, *, command: Sequence[str], env: Mapping[str, str] | None = None,
                 timeout: float = 15):
        self._reporter = Reporter(command, env, timeout)

    async def context(self) -> dict[str, Any]:
        """Read current state and recent turn IDs; message bodies are not stored here."""
        value = await self._reporter.call("context")
        if not isinstance(value, dict) or not isinstance(value.get("workstreams"), list):
            raise DeliveryError("Workstream reporter returned an invalid snapshot")
        return value

    def _handle(self, value: dict[str, Any]) -> Workstream:
        return Workstream(
            id=value["id"], title=value["title"], status=value["status"],
            summary=value["summary"], question=value["question"], owner=value["owner"],
            steps=tuple(Step(**step) for step in value["steps"]), updated_at=value["updatedAt"],
            description=value.get("description", ""), parent_id=value.get("parentId"),
            links=tuple(Link(**link) for link in value.get("links", [])), _client=self,
        )

    async def list(self, *, status: Status | None = None, parent_id: str | None = None) -> tuple[Workstream, ...]:
        """List snapshots, optionally restricted to a status or a parent workstream."""
        if status is not None and status not in STATUSES:
            raise ValueError("Invalid workstream status")
        return tuple(self._handle(value) for value in (await self.context())["workstreams"]
                     if (status is None or value["status"] == status)
                     and (parent_id is None or value.get("parentId") == parent_id))

    async def get(self, id: str) -> Workstream:
        """Read one current workstream; raise KeyError when the ID does not exist."""
        text(id, "workstream ID")
        for value in (await self.context())["workstreams"]:
            if value["id"] == id:
                return self._handle(value)
        raise KeyError(f"Unknown workstream: {id}")

    async def create(self, id: str, title: str, **fields: Any) -> Workstream:
        """Create a stable outcome ID; refuse to overwrite an existing workstream."""
        if any(value["id"] == id for value in (await self.context())["workstreams"]):
            raise ValueError(f"Workstream already exists: {id}; use update or upsert")
        return await self.upsert(id, title, **fields)

    async def upsert(self, id: str, title: str, *, status: Status = "todo", summary: str = "",
                     description: str = "", owner: str = "/root", question: str = "",
                     steps: Sequence[Step | Mapping[str, Any]] = (), parent_id: str | None = None,
                     links: Sequence[Link | Mapping[str, Any]] = ()) -> Workstream:
        """Replace complete state for a stable ID. Prefer update for partial changes."""
        value: dict[str, Any] = dict(id=id, title=title, status=status, summary=summary,
                                    description=description, owner=owner, question=question,
                                    steps=steps, links=links)
        if parent_id is not None:
            value["parentId"] = parent_id
        return await self._upsert(value)

    async def _upsert(self, value: dict[str, Any]) -> Workstream:
        value = validate_stream(value)
        await self.retry(Report.create("upsert", workstream=value))
        return await self.get(value["id"])

    async def update(self, id: str, **changes: Any) -> Workstream:
        """Patch current state. Full-state writes are last-writer-wins across agents."""
        current = next((value for value in (await self.context())["workstreams"] if value["id"] == id), None)
        if current is None:
            raise KeyError(f"Unknown workstream: {id}")
        value = {key: item for key, item in current.items() if key != "updatedAt"}
        if "id" in changes:
            raise TypeError("Workstream IDs are immutable")
        if "parent_id" in changes:
            parent_id = changes.pop("parent_id")
            if parent_id is None:
                value.pop("parentId", None)
            else:
                changes["parentId"] = parent_id
        value.update(changes)
        if value["status"] != "needs_input" and "question" not in changes:
            value["question"] = ""
        return await self._upsert(value)

    async def associate(self, workstream_id: str, session_id: str, *, name: str,
                        role: str = "worker") -> Any:
        """Record an observed session ID without spawning or contacting an agent."""
        agent = {"workstreamId": text(workstream_id, "workstream ID"),
                 "sessionId": text(session_id, "session ID"),
                 "name": text(name, "agent name", 160), "role": text(role, "agent role", 160)}
        return await self.retry(Report.create("associate", agent=agent))

    async def classify(self, turn_ids: Sequence[str], workstream_ids: Sequence[str]) -> Any:
        """Correct whole exchanges explicitly; ordinary Clef labeling needs no call here."""
        for values in (turn_ids, workstream_ids):
            if isinstance(values, (str, bytes)) or not 1 <= len(values) <= 200:
                raise ValueError("Pass 1–200 turn IDs and workstream IDs as sequences")
            for value in values:
                text(value, "classification ID")
        return await self.retry(Report.create("classify", source="root", turnIds=list(turn_ids),
                                            workstreamIds=list(workstream_ids)))

    async def retry(self, report: Report) -> Any:
        """Deliver the exact same immutable event again after uncertain delivery."""
        if not isinstance(report, Report):
            raise TypeError("retry expects a Report, usually DeliveryError.report")
        return await self._reporter.call("report", report)
