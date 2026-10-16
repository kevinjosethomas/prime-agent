"""Public Python API exercised through an actual isolated reporter subprocess.

The fixture simulates the reporter contract; desktop tests own SQLite semantics.
No daemon, network, kernel installation, or model inference is needed.
"""

from __future__ import annotations

import asyncio
import ast
from dataclasses import FrozenInstanceError
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import rlm
import workstreams
from workstreams import Client, DeliveryError, Link, Step


REPORTER = r'''
import json, os, pathlib, sys
assert sys.argv[1] == "literal ; $(not-a-shell)"
store = pathlib.Path(os.environ["WORKSTREAM_TEST_STORE"])
state = json.loads(store.read_text()) if store.exists() else {
    "root": None, "workstreams": [], "agents": [], "turns": [], "events": [], "reports": []}
if sys.argv[2] == "context":
    print(json.dumps(state))
else:
    event = json.load(sys.stdin)
    if event["eventId"] in state["events"]:
        print(json.dumps({"disposition": "duplicate"}))
        sys.exit(0)
    state["events"].append(event["eventId"])
    state["reports"].append(event)
    if event["operation"] == "upsert":
        value = event["workstream"]
        value["updatedAt"] = len(state["reports"])
        state["workstreams"] = [item for item in state["workstreams"] if item["id"] != value["id"]] + [value]
    elif event["operation"] == "associate":
        state["agents"].append(event["agent"])
    elif event["operation"] == "assign":
        agent = event["agent"]
        key = lambda item: (item.get("workstreamId"), item.get("rlmChildId"), item.get("sessionDir"))
        state["agents"] = [item for item in state["agents"] if key(item) != key(agent)] + [agent]
    store.write_text(json.dumps(state))
    # Fail after commit once, proving retry retains the exact deduplication ID.
    if event.get("workstream", {}).get("summary") == "uncertain":
        sys.exit(42)
    if event.get("agent", {}).get("name") == "fail-assignment":
        sys.exit(43)
    print(json.dumps({"disposition": "applied"}))
'''


class WorkstreamTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="workstream test ")
        self.addCleanup(self.directory.cleanup)
        self.store = Path(self.directory.name) / "state.json"
        reporter = Path(self.directory.name) / "reporter with spaces.py"
        reporter.write_text(REPORTER)
        self.config = {"command": [sys.executable, str(reporter), "literal ; $(not-a-shell)"],
                       "env": {"WORKSTREAM_TEST_STORE": str(self.store)}}
        self.client = Client(**self.config)

    def state(self):
        return json.loads(self.store.read_text())

    async def test_restart_roundtrip_subtasks_links_and_real_association(self):
        case = await self.client.create("o1", "Prepare O1 case", status="active")
        intake = await case.subtask("intake", "Complete Lighthouse intake", links=[
            Link("source", "https://example.com/intake", "Intake")])
        await intake.associate("actual-session-id", name="intake-worker", role="researcher")
        restarted = Client(**self.config)
        self.assertEqual((await restarted.get("intake")).parent_id, case.id)
        self.assertEqual((await restarted.list(parent_id="o1"))[0].links,
                         (Link("source", "https://example.com/intake", "Intake"),))
        self.assertEqual((await restarted.context())["agents"], [{
            "workstreamId": "intake", "sessionId": "actual-session-id",
            "name": "intake-worker", "role": "researcher"}])
        self.assertIsNone(case.progress)

    async def test_milestones_completion_question_and_immutable_snapshots(self):
        task = await self.client.create("parser", "Parse JSON in Prime CLI logs",
                                        steps=[Step("parse", "Parse records"), Step("test", "Verify malformed records")])
        with self.assertRaisesRegex(ValueError, "Unfinished"):
            await task.complete()
        with self.assertRaises(KeyError):
            await task.check("typo")
        with self.assertRaises(FrozenInstanceError):
            task.title = "mutated"
        advanced = await task.check("parse")
        self.assertEqual((task.progress, advanced.progress), ((0, 2), (1, 2)))
        blocked = await task.needs_input("Should malformed records remain plain text?")
        self.assertEqual(blocked.question, "Should malformed records remain plain text?")
        resumed = await blocked.update(status="active")
        self.assertEqual((resumed.question, resumed.progress), ("", (1, 2)))
        await resumed.check("test")
        completed = await task.complete(summary="Parser verified")
        self.assertEqual((completed.status, completed.progress), ("done", (2, 2)))

    async def test_partial_update_refreshes_stale_handle_and_explicit_removal(self):
        parent = await self.client.create("parent", "Parent outcome")
        task = await parent.subtask("child", "Child outcome", owner="worker",
                                    description="Detailed routing context", links=[Link("pr", "https://example.com/pr")])
        await self.client.update("child", summary="A newer update")
        changed = await task.update(status="active")
        self.assertEqual((changed.summary, changed.owner, changed.description, changed.parent_id),
                         ("A newer update", "worker", "Detailed routing context", "parent"))
        detached = await changed.update(parent_id=None, links=[])
        self.assertEqual((detached.parent_id, detached.links), (None, ()))

    async def test_uncertain_commit_retry_deduplicates_exact_event(self):
        task = await self.client.create("export", "Export checkpoint")
        with self.assertRaises(DeliveryError) as caught:
            await task.update(summary="uncertain")
        event = caught.exception.report
        self.assertIsNotNone(event)
        before = self.state()
        receipt = await self.client.retry(event)
        self.assertEqual(receipt, {"disposition": "duplicate"})
        self.assertEqual(self.state(), before)
        self.assertEqual((await task.refresh()).summary, "uncertain")

    async def test_reject_invalid_state_before_report_delivery(self):
        task = await self.client.create("task", "A concrete outcome")
        before = self.state()["reports"]
        for changes in ({"status": "needs_input", "question": " "},
                        {"progress": 80}, {"steps": [Step("same", "One"), Step("same", "Two")]},
                        {"links": [Link("source", "https://@example.com/path")]},
                        {"links": [Link("source", "file:///etc/passwd")]}):
            with self.subTest(changes=changes):
                with self.assertRaises((TypeError, ValueError)):
                    await task.update(**changes)
        self.assertEqual(self.state()["reports"], before)

    async def test_context_correction_ids_and_source_content_remain_data(self):
        marker = Path(self.directory.name) / "must-not-exist"
        title = f"Parse $(touch '{marker}') ; JSON"
        task = await self.client.create("json", title)
        self.assertEqual(task.title, title)
        self.assertFalse(marker.exists())
        await self.client.classify(["turn-1"], ["json", "other"])
        report = self.state()["reports"][-1]
        self.assertEqual({key: value for key, value in report.items() if key != "eventId"},
                         {"operation": "classify", "source": "manual", "turnIds": ["turn-1"],
                          "workstreamIds": ["json", "other"]})
        with self.assertRaises(ValueError):
            await self.client.classify("turn-1", ["json"])

    async def test_configured_module_facade_and_duplicate_create(self):
        workstreams.configure(**self.config)
        task = await workstreams.create("one", "One outcome")
        with self.assertRaisesRegex(ValueError, "already exists"):
            await workstreams.create("one", "Accidental overwrite")
        await workstreams.update("one", status="active")
        self.assertEqual(tuple(item.id for item in await workstreams.list(status="active")), (task.id,))
        self.assertEqual((await workstreams.get("one")).title, "One outcome")

    async def test_timeout_terminates_child_and_does_not_block_event_loop(self):
        client = Client(command=[sys.executable, "-c", "import threading; threading.Event().wait()"], timeout=0.2)
        operation = asyncio.create_task(client.context())
        ready = asyncio.Event()
        asyncio.get_running_loop().call_soon(ready.set)
        await asyncio.wait_for(ready.wait(), 0.1)
        self.assertFalse(operation.done())
        with self.assertRaisesRegex(DeliveryError, "timed out"):
            await operation

    async def test_reporter_errors_do_not_echo_stderr(self):
        client = Client(command=[sys.executable, "-c", "import sys; sys.stderr.write('sensitive-fixture'); sys.exit(9)"])
        with self.assertRaises(DeliveryError) as caught:
            await client.context()
        self.assertEqual(str(caught.exception), "Workstream reporter exited with code 9")

    async def test_native_spawn_inherits_configuration_and_retains_queued_identity(self):
        task = await self.client.create("parser", "Parse JSON in Prime CLI logs")
        handle = rlm.RLMSpawnHandle("real-child-id", "parser-worker", Path(self.directory.name) / "child", "provider/model")
        with patch.object(rlm, "spawn", AsyncMock(return_value=handle)) as spawn:
            actual = await task.spawn("Implement parsing", name="parser-worker", model="provider/model",
                                      thinking="low", role="implementer")
        self.assertIs(actual, handle)
        spawn.assert_awaited_once()
        prompt = spawn.await_args.args[0]
        self.assertTrue(prompt.startswith("Implement parsing\n\n"))
        self.assertEqual(spawn.await_args.kwargs, {"name": "parser-worker", "model": "provider/model", "thinking": "low"})
        line = next(line for line in prompt.splitlines() if line.startswith("workstreams.configure("))
        decoded = json.loads(ast.literal_eval(ast.parse(line).body[0].value.keywords[0].value.args[0]))
        self.assertEqual(decoded, self.client.configuration())
        self.assertEqual(self.state()["agents"], [{"workstreamId": "parser", "rlmChildId": "real-child-id",
                                                 "sessionDir": str(handle.session_dir), "name": "parser-worker", "role": "implementer"}])
        exported = self.client.configuration()
        exported["command"].append("mutated")
        exported["env"]["WORKSTREAM_TEST_STORE"] = "mutated"
        self.assertEqual(self.client.configuration()["env"], self.config["env"])
        self.assertEqual(self.client.configuration()["command"], self.config["command"])

    async def test_sync_agents_uses_actual_session_id_and_scoped_child_identity(self):
        task = await self.client.create("parser", "Parse JSON in Prime CLI logs")
        directory = Path(self.directory.name) / "child"
        await task.assign(rlm.RLMSpawnHandle("child", "worker", directory, "provider/model"), role="reviewer")
        assigned = rlm.RLMSubagent("child", "actual-active-id", "actual-session-id", "worker", directory, "running")
        other = rlm.RLMSubagent("child", "wrong-active-id", "wrong-session-id", "worker", directory / "other-scope", "running")
        with patch.object(rlm, "list_subagents", AsyncMock(return_value=[other, assigned])) as roster:
            self.assertEqual(await task.sync_agents(), (assigned,))
        roster.assert_awaited_once_with()
        self.assertEqual(self.state()["agents"], [{"workstreamId": "parser", "rlmChildId": "child",
                                                 "sessionDir": str(directory), "name": "worker", "role": "reviewer",
                                                 "sessionId": "actual-session-id"}])
        with self.assertRaises(TypeError):
            await task.assign({"sessionId": "guessed"})

    async def test_assignment_failure_preserves_child_handle_and_retry_event(self):
        task = await self.client.create("parser", "Parse JSON in Prime CLI logs")
        handle = rlm.RLMSpawnHandle("child", "fail-assignment", Path(self.directory.name) / "child", "provider/model")
        with patch.object(rlm, "spawn", AsyncMock(return_value=handle)) as spawn:
            with self.assertRaises(workstreams.AssignmentError) as caught:
                await task.spawn("Implement parsing", name="fail-assignment")
        self.assertIs(caught.exception.handle, handle)
        self.assertIsNotNone(caught.exception.report)
        self.assertEqual(await self.client.retry(caught.exception.report), {"disposition": "duplicate"})
        spawn.assert_awaited_once()

    async def test_start_agent_uses_native_independent_session_and_verified_orchestrator(self):
        task = await self.client.create("parser", "Parse JSON in Prime CLI logs")
        snapshot = self.state()
        snapshot["root"] = {"sessionId": "fixture-orchestrator-id"}
        self.store.write_text(json.dumps(snapshot))
        handle = rlm.RLMCreateSessionHandle("native-active-id", "native-session-id", "parser-parent",
                                            Path(self.directory.name) / "native-session-id.jsonl", "provider/model")
        with patch.object(rlm, "create_session", AsyncMock(return_value=handle)) as create, \
             patch.object(rlm, "spawn", AsyncMock()) as spawn, \
             patch.object(rlm, "collect", AsyncMock()) as collect:
            actual = await task.start_agent("Implement parsing", name="parser-parent", model="provider/model",
                                            thinking="low", cwd="/verified/project")
        self.assertIs(actual, handle)
        create.assert_awaited_once()
        spawn.assert_not_awaited()
        collect.assert_not_awaited()
        self.assertEqual(create.await_args.kwargs, {"name": "parser-parent", "model": "provider/model",
                                                   "thinking": "low", "cwd": "/verified/project"})
        prompt = create.await_args.args[0]
        self.assertTrue(prompt.startswith("Implement parsing\n\n"))
        bootstrap_line = next(line for line in prompt.splitlines() if line.startswith("workstreams.configure("))
        decoded = json.loads(ast.literal_eval(ast.parse(bootstrap_line).body[0].value.keywords[0].value.args[0]))
        self.assertEqual(decoded, self.client.configuration())
        callback_line = next(line for line in prompt.splitlines() if line.startswith("await agent_message.send("))
        call = ast.parse(callback_line).body[0].value.value
        self.assertEqual({keyword.arg: ast.literal_eval(keyword.value) for keyword in call.keywords},
                         {"receiver_role": "sibling", "receiver_name": "fixture-orchestrator-id"})
        self.assertEqual(self.state()["agents"], [{"workstreamId": "parser", "sessionId": "native-session-id",
                                                 "name": "parser-parent", "role": "worker-parent"}])

    async def test_start_agent_without_recorded_root_never_guesses_a_recipient(self):
        workstreams.configure(**self.config)
        await workstreams.create("parser", "Parse JSON in Prime CLI logs")
        handle = rlm.RLMCreateSessionHandle("active", "session", "parent", Path("/fixture/session.jsonl"), "provider/model")
        with patch.object(rlm, "create_session", AsyncMock(return_value=handle)) as create:
            actual = await workstreams.start_agent("parser", "Implement parsing", name="parent")
        self.assertIs(actual, handle)
        self.assertNotIn("await agent_message.send(", create.await_args.args[0])
        self.assertEqual(self.state()["agents"][0]["sessionId"], "session")

    async def test_start_agent_assignment_failure_retains_native_handle_without_recreation(self):
        task = await self.client.create("parser", "Parse JSON in Prime CLI logs")
        handle = rlm.RLMCreateSessionHandle("active", "session", "fail-assignment", Path("/fixture/session.jsonl"), "provider/model")
        with patch.object(rlm, "create_session", AsyncMock(return_value=handle)) as create:
            with self.assertRaises(workstreams.AssignmentError) as caught:
                await task.start_agent("Implement parsing", name="parent")
            self.assertIs(caught.exception.handle, handle)
            self.assertEqual(await self.client.retry(caught.exception.report), {"disposition": "duplicate"})
        create.assert_awaited_once()

    async def test_start_agent_validates_task_before_admitting_native_work(self):
        with patch.object(rlm, "create_session", AsyncMock()) as create:
            with self.assertRaises(KeyError):
                await self.client.start_agent("unknown", "Implement parsing", name="parent")
            with self.assertRaises(ValueError):
                await self.client.start_agent("unknown", " ", name="parent")
        create.assert_not_awaited()

    def test_configuration_refuses_shell_strings_and_invalid_timeouts(self):
        with self.assertRaises(ValueError):
            Client(command="echo unsafe")
        for timeout in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                Client(command=[sys.executable], timeout=timeout)


if __name__ == "__main__":
    unittest.main()
