"""Public slim tasks API exercised through an actual isolated reporter subprocess.

The fixture simulates the desktop reporter contract (create/patch/set-archived/
associate/assign with eventId dedup and merged-state question enforcement);
desktop tests own SQLite semantics. No daemon, network, kernel installation, or
model inference is needed.
"""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import rlm
import tasks
from tasks import Link
from tasks._client import Client
from tasks._transport import DeliveryError, Report


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
    operation = event["operation"]
    if operation == "create":
        value = dict(event["task"])
        assert "archived" not in value  # Host creates do not accept this field.
        if any(item["id"] == value["id"] for item in state["tasks"]):
            sys.exit(46)  # Atomic duplicate rejection under the event's transaction.
        value["updatedAt"] = len(state["reports"])
        state["tasks"].append(value)
    elif operation == "patch":
        value = next((item for item in state["tasks"] if item["id"] == event["taskId"]), None)
        if value is None:
            sys.exit(44)
        for key, item in event["changes"].items():
            value[key] = item
        # Host contract: the question is required exactly while the merged
        # status is needs_input, and is cleared on every other status.
        if value["status"] == "needs_input" and not value.get("question", "").strip():
            sys.exit(45)
        if value["status"] != "needs_input":
            value["question"] = ""
        value["updatedAt"] = len(state["reports"])
    elif operation == "set-archived":
        value = next((item for item in state["tasks"] if item["id"] == event["taskId"]), None)
        if value is None:
            sys.exit(44)
        value["archived"] = event["archived"]
    elif operation == "associate":
        state["agents"].append(event["agent"])
    elif operation == "assign":
        agent = event["agent"]
        key = lambda item: (item.get("taskId"), item.get("rlmChildId"), item.get("sessionDir"))
        state["agents"] = [item for item in state["agents"] if key(item) != key(agent)] + [agent]
    store.write_text(json.dumps(state))
    # Fail after commit once, proving the internal redelivery retains the exact event ID.
    if event.get("taskId") == "uncertain":
        sys.exit(42)
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

    async def test_restart_roundtrip_subtasks_links_and_native_link(self):
        product = await self.client.create("cli", "Prime CLI")
        parser = await product.subtask("parser", "Structured log parser", status="active")
        parser = await parser.update(links=[
            Link("source", "https://example.com/parser", "Parser design")])
        await parser.link(rlm.RLMSpawnHandle("child", "parser-owner",
                                            Path(self.directory.name) / "child", "provider/model"))
        restarted = Client(**self.config)
        self.assertEqual((await restarted.get("parser")).parent_id, product.id)
        children = [item for item in await restarted.list() if item.parent_id == product.id]
        self.assertEqual([item.id for item in children], ["parser"])
        self.assertEqual(children[0].links, (Link("source", "https://example.com/parser", "Parser design"),))
        self.assertEqual((await restarted._context())["agents"], [{
            "taskId": "parser", "rlmChildId": "child",
            "sessionDir": str(Path(self.directory.name) / "child"),
            "name": "parser-owner", "role": "worker"}])

    async def test_patch_updates_preserve_unrelated_fields(self):
        task = await self.client.create("task", "A concrete outcome")
        with self.assertRaises(FrozenInstanceError):
            task.title = "mutated"
        changed = await task.update(status="active")
        self.assertEqual((changed.status, changed.question, changed.parent_id, changed.links),
                         ("active", "", None, ()))
        self.assertEqual(self.state()["reports"][-1]["changes"], {"status": "active"})
        final = await changed.update(links=[Link("pr", "https://example.com/pr")])
        self.assertEqual(final.links, (Link("pr", "https://example.com/pr"),))
        # No named changes: no event is delivered, and the snapshot stays current.
        before = self.state()["reports"]
        self.assertEqual(await final.update(), final)
        self.assertEqual(self.state()["reports"], before)

    async def test_duplicate_create_is_rejected_and_host_rejects_atomically(self):
        task = await self.client.create("task", "A concrete outcome")
        with self.assertRaisesRegex(ValueError, "already exists"):
            await self.client.create("task", "Accidental overwrite")
        with self.assertRaises(DeliveryError):
            await self.client._deliver(Report.create("create", task={
                "id": task.id, "title": "Race loser", "status": "todo", "question": "",
                "owner": "/root", "links": []}))
        self.assertEqual(len(self.state()["tasks"]), 1)

    async def test_uncertain_commit_redelivers_the_exact_event_id(self):
        task = await self.client.create("uncertain", "Export checkpoint")
        updated = await task.update(status="backlog")
        self.assertEqual(updated.status, "backlog")
        reports = self.state()["reports"]
        self.assertEqual(len(reports), 2)  # create + one patch; the redelivery deduplicated.
        self.assertEqual(self.state()["events"], [report["eventId"] for report in reports])
        self.assertEqual((await self.client.get("uncertain")).status, "backlog")

    async def test_reject_invalid_state_before_report_delivery(self):
        task = await self.client.create("task", "A concrete outcome")
        before = self.state()["reports"]
        for changes in ({"status": "needs_input"},
                        {"question": "Why?"},
                        {"links": [Link("wiki", "https://example.com/page")]},
                        {"links": [Link("commit", "3a8a666b5")]},
                        {"links": [Link("source", "https://@example.com/path")]},
                        {"links": [Link("source", "file:///etc/passwd")]}):
            with self.subTest(changes=changes):
                with self.assertRaises((TypeError, ValueError)):
                    await task.update(**changes)
        with self.assertRaises(TypeError):
            await task.update(progress=80)
        with self.assertRaises(ValueError):
            await self.client.create("bad", "x" * 161)
        with self.assertRaises(ValueError):
            await self.client.create("bad", "No question here", question="why?")
        self.assertEqual(self.state()["reports"], before)

    async def test_question_required_exactly_for_needs_input(self):
        task = await self.client.create("layout", "Fix layout")
        blocked = await task.update(status="needs_input", question="Which sidebar width?")
        self.assertEqual((blocked.status, blocked.question),
                         ("needs_input", "Which sidebar width?"))
        self.assertEqual(self.state()["reports"][-1]["changes"],
                         {"status": "needs_input", "question": "Which sidebar width?"})
        # The host clears the question when the status moves away.
        resumed = await blocked.update(status="active")
        self.assertEqual((resumed.status, resumed.question), ("active", ""))
        # Re-entering needs_input requires an explicit or inherited question.
        with self.assertRaises(ValueError):
            await resumed.update(status="needs_input")
        again = await resumed.update(status="needs_input", question="Retried yet?")
        self.assertEqual(again.question, "Retried yet?")
        # A question may be replaced while blocked, never carried elsewhere.
        replaced = await again.update(question="Still blocked: which width?")
        self.assertEqual(replaced.question, "Still blocked: which width?")
        with self.assertRaises(ValueError):
            await replaced.update(status="ready", question="carried over")
        # Atomic needs_input on create (provisional question keyword).
        born = await self.client.create("born-blocked", "Starts blocked",
                                       status="needs_input", question="Is the fixture green?")
        self.assertEqual((born.status, born.question), ("needs_input", "Is the fixture green?"))
        with self.assertRaises(ValueError):
            await self.client.create("bad-blocked", "Needs a question", status="needs_input")
        with self.assertRaises(ValueError):
            await self.client.create("bad-active", "No question here", question="why?")

    async def test_source_content_remains_data(self):
        marker = Path(self.directory.name) / "must-not-exist"
        title = f"Parse $(touch '{marker}') ; JSON"
        task = await self.client.create("json", title)
        self.assertEqual(task.title, title)
        self.assertFalse(marker.exists())

    async def test_archive_restore_snapshots_and_filters(self):
        task = await self.client.create("primer", "Primer", status="active")
        other = await self.client.create("other", "Independent product")
        self.assertFalse(task.archived)  # Older hosts may omit the flag entirely.
        archived = await task.archive()
        self.assertEqual(archived, await self.client.get(task.id))
        self.assertEqual((archived.archived, archived.status), (True, task.status))
        self.assertFalse(task.archived)  # The original snapshot stays immutable.
        self.assertEqual(await self.client.list(), (other,))
        self.assertEqual(await self.client.list(archived=False), (other,))
        self.assertEqual(await self.client.list(archived=True, status="active"), (archived,))
        self.assertEqual(await self.client.list(archived=None), (archived, other))
        restored = await archived.restore()
        self.assertEqual((restored.archived, restored.status), (False, task.status))
        reports = self.state()["reports"][-2:]
        self.assertEqual([{key: value for key, value in report.items() if key != "eventId"}
                          for report in reports], [
            {"operation": "set-archived", "taskId": task.id, "archived": flag}
            for flag in (True, False)])
        for invalid in (0, "false"):
            with self.assertRaises(TypeError):
                await self.client.list(archived=invalid)
        for bad_status in ("done-done", "progress"):
            with self.subTest(bad_status=bad_status):
                with self.assertRaises(ValueError):
                    await self.client.list(status=bad_status)
        with self.assertRaises(DeliveryError):
            await self.client.archive("missing")

    async def test_update_archived_task_preserves_host_flag(self):
        task = await self.client.create("primer", "Primer", status="active")
        await task.archive()
        changed = await task.update(status="ready")
        self.assertEqual((changed.archived, changed.status), (True, "ready"))
        self.assertNotIn("archived", self.state()["reports"][-1]["changes"])
        self.assertTrue((await self.client.get("primer")).archived)
        self.assertEqual(await self.client.list(), ())

    async def test_link_accepts_native_handles_and_roster_rows(self):
        task = await self.client.create("parser", "Parse JSON in Prime CLI logs")
        handle = rlm.RLMSpawnHandle("child", "worker", Path(self.directory.name) / "child", "provider/model")
        await task.link(handle)
        roster_row = rlm.RLMSubagent("child", "actual-active-id", "actual-session-id",
                                     "worker", Path(self.directory.name) / "child", "running")
        await task.link(roster_row)
        self.assertEqual((await self.client._context())["agents"], [{
            "taskId": "parser", "rlmChildId": "child",
            "sessionDir": str(Path(self.directory.name) / "child"),
            "name": "worker", "role": "worker", "sessionId": "actual-session-id"}])
        native = rlm.RLMCreateSessionHandle("active", "native-session-id", "parser-parent",
                                           Path(self.directory.name) / "native.jsonl", "provider/model")
        await task.link(native)
        self.assertIn({"taskId": "parser", "sessionId": "native-session-id",
                       "name": "parser-parent", "role": "worker"},
                      (await self.client._context())["agents"])
        with self.assertRaises(TypeError):
            await task.link({"sessionId": "guessed"})

    async def test_timeout_terminates_child_and_does_not_block_event_loop(self):
        client = Client(command=[sys.executable, "-c", "import threading; threading.Event().wait()"], timeout=0.2)
        operation = asyncio.create_task(client._context())
        ready = asyncio.Event()
        asyncio.get_running_loop().call_soon(ready.set)
        await asyncio.wait_for(ready.wait(), 0.1)
        self.assertFalse(operation.done())
        with self.assertRaisesRegex(DeliveryError, "timed out"):
            await operation

    async def test_uncertain_redelivery_still_fails_when_delivery_never_lands(self):
        client = Client(command=[sys.executable, "-c", "import sys; sys.exit(9)"])
        report = Report.create("patch", taskId="x", changes={"status": "ready"})
        with self.assertRaises(DeliveryError) as caught:
            await client._deliver(report)
        self.assertIs(caught.exception.report, report)

    def test_reporter_errors_do_not_echo_stderr(self):
        client = Client(command=[sys.executable, "-c", "import sys; sys.stderr.write('sensitive-fixture'); sys.exit(9)"])
        with self.assertRaises(DeliveryError) as caught:
            asyncio.run(client._context())
        self.assertEqual(str(caught.exception), "Tasks reporter exited with code 9")

    async def test_module_facade_autoconfigures_from_the_trusted_env(self):
        tasks._client = None
        with mock.patch.dict(os.environ, {tasks._REPORTER_ENV: json.dumps(self.config)}):
            primer = await tasks.create("primer", "Primer")
            item = await primer.subtask("drop", "Drop files into Primer", status="active")
            self.assertEqual(await tasks.get("drop"), item)
            self.assertEqual([t.id for t in await tasks.list(status="active")], ["drop"])
            self.assertEqual([t.id for t in await tasks.list() if t.parent_id == "primer"], ["drop"])
        tasks._client = None
        with mock.patch.dict(os.environ):
            os.environ.pop(tasks._REPORTER_ENV, None)
            with self.assertRaisesRegex(RuntimeError, "not configured"):
                await tasks.list()
            with mock.patch.dict(os.environ, {tasks._REPORTER_ENV: "not json"}):
                with self.assertRaisesRegex(RuntimeError, "Invalid PRIME_AGENT_INTERNAL_TASKS_REPORTER"):
                    await tasks.get("primer")
            with mock.patch.dict(os.environ, {tasks._REPORTER_ENV: json.dumps({"command": []})}):
                with self.assertRaisesRegex(RuntimeError, "Invalid PRIME_AGENT_INTERNAL_TASKS_REPORTER"):
                    await tasks.create("x", "X")
        tasks._client = None

    def test_public_surface_is_exactly_slim(self):
        self.assertEqual(set(tasks.__all__), {"Link", "Status", "Task", "create", "get", "list"})
        for name in ("configure", "configuration", "context", "upsert", "update", "archive",
                     "restore", "retry", "assign", "associate", "assign_messages", "spawn",
                     "start_agent", "sync_agents", "Client", "REPORTER_ENV"):
            self.assertFalse(hasattr(tasks, name), f"tasks.{name} must not exist")
        fields = set(tasks.Task.__dataclass_fields__)
        self.assertEqual(fields & {"summary", "description", "owner"}, set(),
                         "summary/description/owner left the public model")
        self.assertIn("question", fields)

    def test_configuration_refuses_shell_strings_and_invalid_timeouts(self):
        with self.assertRaises(ValueError):
            Client(command="echo unsafe")
        for timeout in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                Client(command=[sys.executable], timeout=timeout)


if __name__ == "__main__":
    unittest.main()
