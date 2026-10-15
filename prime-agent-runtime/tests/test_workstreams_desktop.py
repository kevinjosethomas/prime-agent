"""Optional real reporter interoperability, using only a temporary desktop DB.

Set WORKSTREAM_DESKTOP_REPORTER to JSON argv [node, built workstream-cli.cjs].
Pure Python CI does not depend on a neighboring desktop checkout or Node install.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest

import rlm
from workstreams import Client, DeliveryError, Link, Step


@unittest.skipUnless(os.environ.get("WORKSTREAM_DESKTOP_REPORTER"), "No built desktop reporter configured")
class DesktopInteropTests(unittest.IsolatedAsyncioTestCase):
    async def test_canonical_sqlite_roundtrip_parent_validation_and_timestamp(self):
        with tempfile.TemporaryDirectory(prefix="workstreams canonical ") as directory:
            argv = json.loads(os.environ["WORKSTREAM_DESKTOP_REPORTER"])
            command = [*argv, "--db", str(Path(directory) / "state.sqlite")]
            client = Client(command=command, env={"ELECTRON_RUN_AS_NODE": "1"})
            parent = await client.create("o1", "Prepare O1 case", status="active")
            task = await parent.subtask("intake", "Complete Lighthouse intake",
                                        description="Intake only; judging is a sibling outcome",
                                        steps=[Step("verify", "Verify intake answers")],
                                        links=[Link("source", "https://example.com/intake", "Fixture intake")])
            await task.needs_input("Are these intake answers ready for review?")
            resumed = await task.update(status="active")
            self.assertEqual((resumed.question, resumed.parent_id, resumed.progress), ("", "o1", (0, 1)))
            with self.assertRaisesRegex(ValueError, "Unfinished"):
                await task.complete()
            with self.assertRaises(DeliveryError):
                await parent.update(parent_id="intake")  # Canonical host rejects cycles.
            await task.check("verify")
            completed = await task.complete(summary="Verified fixture answers")
            no_change = await completed.update(summary=completed.summary)
            self.assertEqual(no_change.updated_at, completed.updated_at)
            restarted = Client(command=command, env={"ELECTRON_RUN_AS_NODE": "1"})
            persisted = await restarted.get("intake")
            self.assertEqual((persisted.status, persisted.parent_id, persisted.links, persisted.progress),
                             ("done", "o1", (Link("source", "https://example.com/intake", "Fixture intake"),), (1, 1)))
            self.assertIsNone((await restarted.get("o1")).parent_id)

    async def test_canonical_assignment_queued_then_actual_roster_session(self):
        with tempfile.TemporaryDirectory(prefix="workstreams assignment ") as directory:
            argv = json.loads(os.environ["WORKSTREAM_DESKTOP_REPORTER"])
            client = Client(command=[*argv, "--db", str(Path(directory) / "state.sqlite")])
            task = await client.create("fixture", "Verify native assignment metadata")
            child_dir = Path(directory) / "fixture-child"
            await task.assign(rlm.RLMSpawnHandle("fixture-child", "fixture-worker", child_dir, "fixture/model"))
            agents = (await client.context())["agents"]
            self.assertEqual(len(agents), 1)
            self.assertNotIn("sessionId", agents[0])
            await task.assign(rlm.RLMSubagent("fixture-child", "fixture-active-id", "fixture-session-id",
                                             "fixture-worker", child_dir, "running"))
            agents = (await client.context())["agents"]
            self.assertEqual(len(agents), 1)
            self.assertEqual(agents[0]["sessionId"], "fixture-session-id")
            self.assertEqual((agents[0]["rlmChildId"], agents[0]["sessionDir"]), ("fixture-child", str(child_dir)))
