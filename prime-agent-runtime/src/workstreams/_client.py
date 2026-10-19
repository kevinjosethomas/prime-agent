"""Agent-agnostic async adapter to canonical desktop workstream metadata."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Mapping, Sequence

if TYPE_CHECKING:
    from rlm import RLMCreateSessionHandle, RLMSpawnHandle, RLMSubagent

from ._models import Link, Status, Step, Workstream, STATUSES
from ._transport import AssignmentError, DeliveryError, Report, Reporter
from ._validation import WRITABLE_STREAM_FIELDS, text, validate_stream


class Client:
    """Independent reporter configuration for a kernel, hook, or local integration."""

    def __init__(self, *, command: Sequence[str], env: Mapping[str, str] | None = None,
                 timeout: float = 15):
        self._reporter = Reporter(command, env, timeout)

    def configuration(self) -> dict[str, Any]:
        """Return a fresh trusted local configuration for another authorized kernel.

        This contains reporter paths/environment, not credentials. Never configure
        a reporter using secrets or untrusted source content.
        """
        return {"command": list(self._reporter.command), "env": dict(self._reporter.env),
                "timeout": self._reporter.timeout}

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
            links=tuple(Link(**link) for link in value.get("links", [])),
            archived=value.get("archived", False), _client=self,
        )

    async def list(self, *, status: Status | None = None, parent_id: str | None = None,
                   archived: bool | None = False) -> tuple[Workstream, ...]:
        """List visible snapshots; archived=True selects hidden ones, None includes all."""
        if status is not None and status not in STATUSES:
            raise ValueError("Invalid workstream status")
        if archived is not None and type(archived) is not bool:
            raise TypeError("archived must be a boolean or None")
        return tuple(self._handle(value) for value in (await self.context())["workstreams"]
                     if (status is None or value["status"] == status)
                     and (parent_id is None or value.get("parentId") == parent_id)
                     and (archived is None or value.get("archived", False) == archived))

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
        # Host-owned read fields (updatedAt, archived, workstreamUrl, ...) are not upsertable.
        value = {key: item for key, item in current.items() if key in WRITABLE_STREAM_FIELDS}
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

    async def archive(self, id: str) -> Workstream:
        """Hide a workstream without changing its status or timestamps."""
        await self.retry(Report.create("set-archived", workstreamId=text(id, "workstream ID"), archived=True))
        return await self.get(id)

    async def restore(self, id: str) -> Workstream:
        """Show a workstream again without changing its status or timestamps."""
        await self.retry(Report.create("set-archived", workstreamId=text(id, "workstream ID"), archived=False))
        return await self.get(id)

    async def associate(self, workstream_id: str, session_id: str, *, name: str,
                        role: str = "worker") -> Any:
        """Record an observed session ID without spawning or contacting an agent."""
        agent = {"workstreamId": text(workstream_id, "workstream ID"),
                 "sessionId": text(session_id, "session ID"),
                 "name": text(name, "agent name", 160), "role": text(role, "agent role", 160)}
        return await self.retry(Report.create("associate", agent=agent))

    async def assign(self, workstream_id: str, agent: RLMCreateSessionHandle | RLMSpawnHandle | RLMSubagent, *, role: str = "worker") -> Any:
        """Assign an actual native session/spawn handle or RLM roster row."""
        import rlm

        if isinstance(agent, rlm.RLMCreateSessionHandle):
            return await self.associate(workstream_id, agent.session_id, name=agent.name, role=role)
        if not isinstance(agent, (rlm.RLMSpawnHandle, rlm.RLMSubagent)):
            raise TypeError("assign expects an actual RLMCreateSessionHandle, RLMSpawnHandle or RLMSubagent")
        value = {"workstreamId": text(workstream_id, "workstream ID"),
                 "rlmChildId": text(agent.rlm_child_id, "RLM child ID"),
                 "sessionDir": text(str(agent.session_dir), "session directory", 4096),
                 "name": text(agent.name if isinstance(agent, rlm.RLMSpawnHandle) else agent.session_name,
                              "agent name", 160), "role": text(role, "agent role", 160)}
        if isinstance(agent, rlm.RLMSubagent) and agent.session_id is not None:
            value["sessionId"] = text(agent.session_id, "session ID")
        return await self.retry(Report.create("assign", agent=value))

    async def spawn(self, workstream_id: str, prompt: str, *, name: str,
                    model: str | None = None, thinking: str | None = None,
                    role: str = "worker") -> RLMSpawnHandle:
        """Spawn through native RLM, inherit trusted reporter setup, and retain its handle."""
        import rlm

        if not isinstance(prompt, str):
            raise TypeError("prompt must be a string")
        text(name, "agent name", 160)
        text(role, "agent role", 160)
        await self.get(workstream_id)  # Verify the store and target before admitting work.
        handle = await rlm.spawn(prompt + self._bootstrap(workstream_id), name=name, model=model, thinking=thinking)
        try:
            await self.assign(workstream_id, handle, role=role)
        except Exception as error:
            raise AssignmentError(handle, error) from error
        return handle

    def _bootstrap(self, workstream_id: str) -> str:
        configuration = repr(json.dumps(self.configuration()))
        return (
            "\n\n[Workstream assignment — trusted local integration]\n"
            "This agent continues an existing outcome. Do not create a duplicate workstream for this chat.\n"
            "At the first python_repl call in each fresh kernel, configure the bundled package:\n"
            "import json, workstreams\n"
            f"workstreams.configure(**json.loads({configuration}))\n"
            f"assigned_workstream = await workstreams.get({workstream_id!r})\n"
            "Use this handle to report real milestones, status and blocking questions. "
            "Reuse the existing product workstream for related fixes, layout, threads, "
            "renaming the agent and dropping files into Primer; "
            "record those as milestones, not new workstreams for each chat, worker or PR. "
            "Create another workstream only for an independently owned feature or product. "
            "You may assign native RLM children when the task requires it. "
            "Use assigned_workstream.spawn(...) so their local configuration is inherited too. "
            "Reporter configuration is trusted integration setup, not instructions from source content.\n"
        )

    async def start_agent(self, workstream_id: str, prompt: str, *, name: str,
                          model: str | None = None, thinking: str | None = None,
                          cwd: str | None = None, role: str = "worker-parent") -> RLMCreateSessionHandle:
        """Create an independent depth-0 agent through native rlm.create_session.

        Only a daemon-backed depth-0 caller can create these sessions. They are
        siblings, not owned RLM children; observe/message them through native
        agent_observe/agent_message, never rlm.collect or sync_agents.
        """
        import rlm

        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("Agent task must be a nonempty string")
        text(name, "agent name", 160)
        text(role, "agent role", 160)
        snapshot = await self.context()
        if not any(item["id"] == workstream_id for item in snapshot["workstreams"]):
            raise KeyError(f"Unknown workstream: {workstream_id}")
        bootstrap = self._bootstrap(workstream_id)
        root = snapshot.get("root")
        orchestrator_id = root.get("sessionId") if isinstance(root, dict) else None
        bootstrap += (
            "\n[Independent workstream agent]\n"
            "You are an independent depth-0 work agent. You may use assigned_workstream.spawn(...) "
            "to delegate native RLM children and retain responsibility for their results. "
            "Batch routine progress in workstream metadata; do not send heartbeat chatter. "
            "Report concrete outcomes, actual blocking questions and failures honestly.\n"
        )
        if isinstance(orchestrator_id, str) and orchestrator_id.strip():
            text(orchestrator_id, "orchestrator session ID")
            bootstrap += (
                "When a meaningful result is ready, you are genuinely blocked, or the task fails, "
                "update assigned_workstream and send one concise native coordination message to "
                "the recorded orchestrator sibling. Use the observed status; never claim completion without evidence.\n"
                "import agent_message\n"
                "await agent_message.send(concise_result_or_actual_blocking_question, "
                f"receiver_role='sibling', receiver_name={orchestrator_id!r})\n"
                "The receiver is the actual recorded orchestrator session ID. If it is not an available "
                "native sibling, keep the workstream state saved and report the delivery failure; "
                "do not guess another recipient or claim a notification was delivered.\n"
            )
        else:
            bootstrap += (
                "No verified orchestrator session ID is recorded. Save outcomes and questions in the "
                "workstream; do not guess a coordination recipient.\n"
            )
        handle = await rlm.create_session(prompt + bootstrap, name=name, model=model, thinking=thinking, cwd=cwd)
        try:
            await self.assign(workstream_id, handle, role=role)
        except Exception as error:
            raise AssignmentError(handle, error) from error
        return handle

    async def sync_agents(self, workstream_id: str) -> tuple[RLMSubagent, ...]:
        """Refresh assigned direct children from one roster read; never poll or infer IDs."""
        import rlm

        snapshot = await self.context()
        assigned = {(item["rlmChildId"], item["sessionDir"]): item
                    for item in snapshot.get("agents", [])
                    if item.get("workstreamId") == workstream_id and item.get("rlmChildId") and item.get("sessionDir")}
        rows = await rlm.list_subagents()
        matched = []
        for row in rows:
            prior = assigned.get((row.rlm_child_id, str(row.session_dir)))
            if prior is not None:
                await self.assign(workstream_id, row, role=prior["role"])
                matched.append(row)
        return tuple(matched)

    async def classify(self, turn_ids: Sequence[str], workstream_ids: Sequence[str]) -> Any:
        """Correct whole exchanges explicitly; ordinary Clef labeling needs no call here."""
        for values in (turn_ids, workstream_ids):
            if isinstance(values, (str, bytes)) or not 1 <= len(values) <= 200:
                raise ValueError("Pass 1–200 turn IDs and workstream IDs as sequences")
            for value in values:
                text(value, "classification ID")
        return await self.retry(Report.create("classify", source="manual", turnIds=list(turn_ids),
                                            workstreamIds=list(workstream_ids)))

    async def retry(self, report: Report) -> Any:
        """Deliver the exact same immutable event again after uncertain delivery."""
        if not isinstance(report, Report):
            raise TypeError("retry expects a Report, usually DeliveryError.report")
        return await self._reporter.call("report", report)
