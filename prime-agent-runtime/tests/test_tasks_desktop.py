"""Optional real reporter interoperability, using only a temporary desktop DB.

Set TASKS_DESKTOP_REPORTER to JSON argv [node, built task-cli.cjs] whose CLI
implements the slim create/patch/set-archived/associate/assign wire.
Pure Python CI does not depend on a neighboring desktop checkout or Node install.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest

import rlm
from tasks import Link
from tasks._client import Client


@unittest.skipUnless(os.environ.get("TASKS_DESKTOP_REPORTER"), "No built desktop reporter configured")
class DesktopInteropTests(unittest.IsolatedAsyncioTestCase):
    async def test_canonical_sqlite_roundtrip_patch_and_needs_input(self):
        with tempfile.TemporaryDirectory(prefix="tasks canonical ") as directory:
            argv = json.loads(os.environ["TASKS_DESKTOP_REPORTER"])
            command = [*argv, "--db", str(Path(directory) / "state.sqlite")]
            client = Client(command=command, env={"ELECTRON_RUN_AS_NODE": "1"})
            parent = await client.create("o1", "Prepare O1 case")
            task = await parent.subtask("intake", "Complete Lighthouse intake",
                                        description="Intake only; judging is a sibling outcome",
                                        links=[Link("source", "https://example.com/intake", "Fixture intake")])
            blocked = await task.update(status="needs_input", summary="Are these intake answers ready for review?")
            self.assertEqual((blocked.status, blocked.summary),
                             ("needs_input", "Are these intake answers ready for review?"))
            resumed = await blocked.update(status="active")
            self.assertEqual((resumed.status, resumed.parent_id), ("active", "o1"))
            with self.assertRaises(ValueError):
                await client.create("intake", "Accidental duplicate")
            completed = await task.update(status="done", summary="Verified fixture answers")
            restarted = Client(command=command, env={"ELECTRON_RUN_AS_NODE": "1"})
            persisted = await restarted.get("intake")
            self.assertEqual((persisted.status, persisted.parent_id, persisted.links),
                             ("done", "o1", (Link("source", "https://example.com/intake", "Fixture intake"),)))
            self.assertIsNone((await restarted.get("o1")).parent_id)

    async def test_canonical_archive_restore_and_patch_preserve_host_metadata(self):
        with tempfile.TemporaryDirectory(prefix="tasks archive ") as directory:
            argv = json.loads(os.environ["TASKS_DESKTOP_REPORTER"])
            command = [*argv, "--db", str(Path(directory) / "state.sqlite")]
            client = Client(command=command, env={"ELECTRON_RUN_AS_NODE": "1"})
            task = await client.create("primer", "Primer")
            active = await task.update(status="active")
            archived = await task.archive()
            self.assertEqual((archived.archived, archived.status), (True, active.status))
            self.assertEqual(await client.list(status="active"), ())
            self.assertEqual(await client.list(status="active", archived=True), (archived,))
            self.assertEqual(await client.list(status="active", archived=None), (archived,))
            changed = await task.update(summary="Layout verified")
            self.assertTrue(changed.archived)
            self.assertEqual(changed.status, "active")
            restored = await changed.restore()
            self.assertEqual((restored.archived, restored.status), (False, "active"))
            self.assertEqual(await client.list(status="active"), (restored,))

    async def test_canonical_link_queued_then_actual_roster_session(self):
        with tempfile.TemporaryDirectory(prefix="tasks assignment ") as directory:
            argv = json.loads(os.environ["TASKS_DESKTOP_REPORTER"])
            client = Client(command=[*argv, "--db", str(Path(directory) / "state.sqlite")])
            task = await client.create("fixture", "Verify native assignment metadata")
            child_dir = Path(directory) / "fixture-child"
            await task.link(rlm.RLMSpawnHandle("fixture-child", "fixture-worker", child_dir, "fixture/model"))
            agents = (await client._context())["agents"]
            self.assertEqual(len(agents), 1)
            self.assertNotIn("sessionId", agents[0])
            await task.link(rlm.RLMSubagent("fixture-child", "fixture-active-id", "fixture-session-id",
                                            "fixture-worker", child_dir, "running"))
            agents = (await client._context())["agents"]
            self.assertEqual(len(agents), 1)
            self.assertEqual(agents[0]["sessionId"], "fixture-session-id")
            self.assertEqual((agents[0]["rlmChildId"], agents[0]["sessionDir"]), ("fixture-child", str(child_dir)))
