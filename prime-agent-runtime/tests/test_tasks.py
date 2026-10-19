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
import tasks
from tasks import Client, DeliveryError, Link


REPORTER = r'''
import json, os, pathlib, sys
assert sys.argv[1] == "literal ; $(not-a-shell)"
store = pathlib.Path(os.environ["TASKS_TEST_STORE"])
state = json.loads(store.read_text()) if store.exists() else {
    "root": None, "tasks": [], "agents": [], "turns": [], "events": [], "reports": []}
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
        value = dict(event["task"])
        assert "archived" not in value  # Host upserts do not accept this field.
        prior = next((item for item in state["tasks"] if item["id"] == value["id"]), None)
        if prior is not None and "archived" in prior:
            value["archived"] = prior["archived"]
        value["updatedAt"] = len(state["reports"])
        state["tasks"] = [item for item in state["tasks"] if item["id"] != value["id"]] + [value]
    elif event["operation"] == "set-archived":
        value = next((item for item in state["tasks"] if item["id"] == event["taskId"]), None)
        if value is None:
            sys.exit(44)
        value["archived"] = event["archived"]
    elif event["operation"] == "associate":
        state["agents"].append(event["agent"])
    elif event["operation"] == "assign":
        agent = event["agent"]
        key = lambda item: (item.get("taskId"), item.get("rlmChildId"), item.get("sessionDir"))
        state["agents"] = [item for item in state["agents"] if key(item) != key(agent)] + [agent]
    store.write_text(json.dumps(state))
    # Fail after commit once, proving retry retains the exact deduplication ID.
    if event.get("task", {}).get("summary") == "uncertain":
        sys.exit(42)
    if event.get("agent", {}).get("name") == "fail-assignment":
        sys.exit(43)
    print(json.dumps({"disposition": "applied"}))
'''


class TaskTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="tasks test ")
        self.addCleanup(self.directory.cleanup)
        self.store = Path(self.directory.name) / "state.json"
        reporter = Path(self.directory.name) / "reporter with spaces.py"
        reporter.write_text(REPORTER)
        self.config = {"command": [sys.executable, str(reporter), "literal ; $(not-a-shell)"],
                       "env": {"TASKS_TEST_STORE": str(self.store)}}
        self.client = Client(**self.config)

    def state(self):
        return json.loads(self.store.read_text())

    async def test_restart_roundtrip_subtasks_links_and_real_association(self):
        product = await self.client.create("cli", "Prime CLI", status="active")
        parser = await product.subtask("parser", "Structured log parser", links=[
            Link("source", "https://example.com/parser", "Parser design")])
        await parser.associate("actual-session-id", name="parser-owner", role="implementer")
        restarted = Client(**self.config)
        self.assertEqual((await restarted.get("parser")).parent_id, product.id)
        self.assertEqual((await restarted.list(parent_id="cli"))[0].links,
                         (Link("source", "https://example.com/parser", "Parser design"),))
        self.assertEqual((await restarted.context())["agents"], [{
            "taskId": "parser", "sessionId": "actual-session-id",
            "name": "parser-owner", "role": "implementer"}])

    async def test_completion_question_and_immutable_snapshots(self):
        task = await self.client.create("parser", "Parse JSON in Prime CLI logs")
        with self.assertRaises(FrozenInstanceError):
            task.title = "mutated"
        blocked = await task.needs_input("Should malformed records remain plain text?")
        self.assertEqual(blocked.question, "Should malformed records remain plain text?")
        resumed = await blocked.update(status="active")
        self.assertEqual(resumed.question, "")
        completed = await task.complete(summary="Parser verified")
        self.assertEqual((completed.status, completed.summary, completed.question),
                         ("done", "Parser verified", ""))
        self.assertFalse(hasattr(completed, "progress") or hasattr(completed, "steps"))

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
                        {"progress": 80}, {"steps": []},
                        {"links": [Link("wiki", "https://example.com/page")]},
                        {"links": [Link("commit", "3a8a666b5")]},
                        {"links": [Link("source", "https://@example.com/path")]},
                        {"links": [Link("source", "file:///etc/passwd")]}):
            with self.subTest(changes=changes):
                with self.assertRaises((TypeError, ValueError)):
                    await task.update(**changes)
        self.assertEqual(self.state()["reports"], before)

    async def test_source_content_remains_data(self):
        marker = Path(self.directory.name) / "must-not-exist"
        title = f"Parse $(touch '{marker}') ; JSON"
        task = await self.client.create("json", title)
        self.assertEqual(task.title, title)
        self.assertFalse(marker.exists())

    async def test_assign_messages_records_manual_moves(self):
        tasks.configure(**self.config)
        await self.client.assign_messages(["m1", "m2"], "json")
        await tasks.assign_messages(["m3"], None)
        self.assertEqual([{key: value for key, value in report.items() if key != "eventId"}
                          for report in self.state()["reports"]], [
            {"operation": "set-message-task", "messageIds": ["m1", "m2"], "taskId": "json", "source": "manual"},
            {"operation": "set-message-task", "messageIds": ["m3"], "taskId": None, "source": "manual"}])
        for args, kwargs in ((("m1", "json"), {}), (([], "json"), {}), ((["m"] * 201, "json"), {}),
                             (([" "], "json"), {}), ((["m1"], ""), {})):
            with self.subTest(args=args, kwargs=kwargs):
                with self.assertRaises(ValueError):
                    await self.client.assign_messages(*args, **kwargs)
        self.assertEqual(len(self.state()["reports"]), 2)

    async def test_commit_links_roundtrip(self):
        link = Link("commit", "https://github.com/kevinjosethomas/prime-agent/commit/3a8a666b5", "Rename")
        task = await self.client.create("rename", "Rename to tasks", links=[link])
        self.assertEqual(task.links, (link,))
        self.assertEqual(self.state()["reports"][-1]["task"]["links"],
                         [{"kind": "commit", "url": link.url, "title": "Rename"}])

    async def test_subtask_creates_child_task_and_lists_under_parent(self):
        primer = await self.client.create("primer", "Primer", status="active")
        item = await primer.subtask("primer-drop-files", "Drop files into Primer", status="active")
        self.assertEqual((item.parent_id, item.status), ("primer", "active"))
        self.assertEqual(self.state()["reports"][-1]["task"]["parentId"], "primer")
        self.assertEqual(await self.client.list(parent_id="primer"), (item,))
        with self.assertRaisesRegex(ValueError, "already exists"):
            await primer.subtask("primer-drop-files", "Duplicate")
        with self.assertRaises(TypeError):
            await primer.subtask("child", "Child", parent_id="other")

    async def test_configured_module_facade_and_duplicate_create(self):
        tasks.configure(**self.config)
        task = await tasks.create("one", "One outcome")
        with self.assertRaisesRegex(ValueError, "already exists"):
            await tasks.create("one", "Accidental overwrite")
        await tasks.update("one", status="active")
        self.assertEqual(tuple(item.id for item in await tasks.list(status="active")), (task.id,))
        self.assertEqual((await tasks.get("one")).title, "One outcome")

    async def test_archive_restore_snapshots_filters_and_module_facade(self):
        tasks.configure(**self.config)
        task = await tasks.create("primer", "Primer", status="active")
        other = await tasks.create("other", "Independent product", status="todo")
        self.assertFalse(task.archived)  # Older hosts may omit the flag entirely.
        archived = await task.archive()
        self.assertEqual(archived, await self.client.get(task.id))
        self.assertEqual((archived.archived, archived.status, archived.updated_at),
                         (True, task.status, task.updated_at))
        self.assertFalse(task.archived)  # The original snapshot stays immutable.
        self.assertEqual(await tasks.list(), (other,))
        self.assertEqual(await tasks.list(archived=False), (other,))
        self.assertEqual(await tasks.list(archived=True, status="active"), (archived,))
        self.assertEqual(await tasks.list(archived=None), (archived, other))
        restored = await tasks.restore(task.id)
        self.assertEqual(restored, task)
        self.assertEqual(await tasks.archive(task.id), archived)
        self.assertEqual(await archived.restore(), task)
        reports = self.state()["reports"][-4:]
        self.assertEqual([{key: value for key, value in report.items() if key != "eventId"}
                          for report in reports], [
            {"operation": "set-archived", "taskId": task.id, "archived": flag}
            for flag in (True, False, True, False)])
        for invalid in (0, "false"):
            with self.assertRaises(TypeError):
                await tasks.list(archived=invalid)
        with self.assertRaises(DeliveryError):
            await self.client.archive("missing")

    async def test_update_archived_task_preserves_host_flag_without_upserting_it(self):
        task = await self.client.create("primer", "Primer", status="active")
        await task.archive()
        changed = await task.update(summary="Layout verified")
        self.assertEqual((changed.archived, changed.status), (True, task.status))
        self.assertNotIn("archived", self.state()["reports"][-1]["task"])
        with self.assertRaisesRegex(TypeError, "Unknown task fields: archived"):
            await task.update(archived=False)
        self.assertTrue((await task.refresh()).archived)
        self.assertEqual(await self.client.list(), ())
        restored = await self.client.restore(task.id)
        self.assertEqual((restored.archived, restored.updated_at, restored.status),
                         (False, changed.updated_at, changed.status))

    async def test_update_keeps_only_writable_fields_from_host_read_records(self):
        parent = await self.client.create("primer", "Primer", status="active")
        task = await parent.subtask("layout", "Fix layout", description="Sidebar", owner="worker",
                                    links=[Link("pr", "https://example.com/pr/1")])
        await task.archive()
        state = self.state()
        record = next(item for item in state["tasks"] if item["id"] == "layout")
        record["taskUrl"] = "primer://tasks/layout"
        self.store.write_text(json.dumps(state))
        changed = await self.client.update("layout", summary="Layout verified")
        self.assertEqual((changed.summary, changed.description, changed.owner, changed.parent_id,
                          changed.links, changed.archived),
                         ("Layout verified", "Sidebar", "worker", "primer",
                          (Link("pr", "https://example.com/pr/1"),), True))
        written = self.state()["reports"][-1]["task"]
        self.assertEqual(set(written), {"id", "title", "status", "summary", "description", "question",
                                        "owner", "parentId", "links"})
        blocked = await changed.needs_input("Which sidebar width?")
        self.assertEqual((await blocked.update(summary="Waiting")).question, "Which sidebar width?")
        self.assertEqual((await blocked.update(status="active")).question, "")
        self.assertEqual((await blocked.update(status="active", question="kept")).question, "kept")
        detached = await blocked.update(parent_id=None)
        self.assertIsNone(detached.parent_id)
        self.assertNotIn("parentId", self.state()["reports"][-1]["task"])
        for field in ("updatedAt", "archived", "taskUrl", "steps"):
            with self.subTest(field=field):
                with self.assertRaisesRegex(TypeError, "Unknown task fields"):
                    await self.client.update("layout", **{field: None})

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
        self.assertEqual(str(caught.exception), "Tasks reporter exited with code 9")

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
        line = next(line for line in prompt.splitlines() if line.startswith("tasks.configure("))
        decoded = json.loads(ast.literal_eval(ast.parse(line).body[0].value.keywords[0].value.args[0]))
        self.assertEqual(decoded, self.client.configuration())
        self.assertEqual(self.state()["agents"], [{"taskId": "parser", "rlmChildId": "real-child-id",
                                                 "sessionDir": str(handle.session_dir), "name": "parser-worker", "role": "implementer"}])
        exported = self.client.configuration()
        exported["command"].append("mutated")
        exported["env"]["TASKS_TEST_STORE"] = "mutated"
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
        self.assertEqual(self.state()["agents"], [{"taskId": "parser", "rlmChildId": "child",
                                                 "sessionDir": str(directory), "name": "worker", "role": "reviewer",
                                                 "sessionId": "actual-session-id"}])
        with self.assertRaises(TypeError):
            await task.assign({"sessionId": "guessed"})

    async def test_assignment_failure_preserves_child_handle_and_retry_event(self):
        task = await self.client.create("parser", "Parse JSON in Prime CLI logs")
        handle = rlm.RLMSpawnHandle("child", "fail-assignment", Path(self.directory.name) / "child", "provider/model")
        with patch.object(rlm, "spawn", AsyncMock(return_value=handle)) as spawn:
            with self.assertRaises(tasks.AssignmentError) as caught:
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
        bootstrap_line = next(line for line in prompt.splitlines() if line.startswith("tasks.configure("))
        decoded = json.loads(ast.literal_eval(ast.parse(bootstrap_line).body[0].value.keywords[0].value.args[0]))
        self.assertEqual(decoded, self.client.configuration())
        callback_line = next(line for line in prompt.splitlines() if line.startswith("await agent_message.send("))
        call = ast.parse(callback_line).body[0].value.value
        self.assertEqual({keyword.arg: ast.literal_eval(keyword.value) for keyword in call.keywords},
                         {"receiver_role": "sibling", "receiver_name": "fixture-orchestrator-id"})
        self.assertEqual(self.state()["agents"], [{"taskId": "parser", "sessionId": "native-session-id",
                                                 "name": "parser-parent", "role": "worker-parent"}])

    async def test_start_agent_without_recorded_root_never_guesses_a_recipient(self):
        tasks.configure(**self.config)
        await tasks.create("parser", "Parse JSON in Prime CLI logs")
        handle = rlm.RLMCreateSessionHandle("active", "session", "parent", Path("/fixture/session.jsonl"), "provider/model")
        with patch.object(rlm, "create_session", AsyncMock(return_value=handle)) as create:
            actual = await tasks.start_agent("parser", "Implement parsing", name="parent")
        self.assertIs(actual, handle)
        self.assertNotIn("await agent_message.send(", create.await_args.args[0])
        self.assertEqual(self.state()["agents"][0]["sessionId"], "session")

    async def test_start_agent_assignment_failure_retains_native_handle_without_recreation(self):
        task = await self.client.create("parser", "Parse JSON in Prime CLI logs")
        handle = rlm.RLMCreateSessionHandle("active", "session", "fail-assignment", Path("/fixture/session.jsonl"), "provider/model")
        with patch.object(rlm, "create_session", AsyncMock(return_value=handle)) as create:
            with self.assertRaises(tasks.AssignmentError) as caught:
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
