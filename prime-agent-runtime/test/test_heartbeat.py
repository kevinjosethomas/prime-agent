from __future__ import annotations

import asyncio
import importlib
import unittest
from unittest.mock import AsyncMock, patch


rlm_module = importlib.import_module("rlm")
heartbeat_module = importlib.import_module("rlm.heartbeat")


HEARTBEAT_ROW = {
    "id": "cron-1",
    "instruction": "watch the mission",
    "label": "watcher",
    "status": "active",
    "delivery_mode": "steer",
    "schedule": {"kind": "interval", "expression": "every 15m", "intervalMs": 900000},
    "next_run_at": "2026-10-20T23:00:00Z",
}


class HeartbeatTest(unittest.TestCase):
    def test_create_sends_instruction_interval_label_and_mode(self) -> None:
        host_request = AsyncMock(return_value={"heartbeat": HEARTBEAT_ROW})

        with patch.object(rlm_module, "host_request", host_request):
            hb = asyncio.run(heartbeat_module.create("watch the mission", every="every 15m", label="watcher"))

        host_request.assert_awaited_once_with(
            "rlm_heartbeat.create",
            {"instruction": "watch the mission", "interval": "every 15m", "label": "watcher"},
        )
        self.assertEqual(hb.id, "cron-1")
        self.assertEqual(hb.instruction, "watch the mission")
        self.assertEqual(hb.every, "every 15m")
        self.assertEqual(hb.label, "watcher")
        self.assertEqual(hb.status, "active")
        self.assertEqual(hb.delivery_mode, "steer")
        self.assertEqual(hb.next_run_at, "2026-10-20T23:00:00Z")

    def test_create_omits_unset_fields_and_rejects_bad_modes(self) -> None:
        host_request = AsyncMock(return_value={"heartbeat": {**HEARTBEAT_ROW, "schedule": {"kind": "interval", "expression": "every 5m"}}})
        with patch.object(rlm_module, "host_request", host_request):
            asyncio.run(heartbeat_module.create("watch"))
        host_request.assert_awaited_once_with("rlm_heartbeat.create", {"instruction": "watch"})

        with self.assertRaisesRegex(ValueError, "delivery_mode must be"):
            asyncio.run(heartbeat_module.create("watch", delivery_mode="sideways"))
        with self.assertRaisesRegex(TypeError, "instruction must be str"):
            asyncio.run(heartbeat_module.create(42))

    def test_list_reads_the_catalog(self) -> None:
        host_request = AsyncMock(return_value={"heartbeats": [HEARTBEAT_ROW, {**HEARTBEAT_ROW, "id": "cron-2", "label": None}]})

        with patch.object(rlm_module, "host_request", host_request):
            heartbeats = asyncio.run(heartbeat_module.list(include_inactive=True))

        host_request.assert_awaited_once_with("rlm_heartbeat.list", {"include_inactive": True})
        self.assertEqual([hb.id for hb in heartbeats], ["cron-1", "cron-2"])
        self.assertIsNone(heartbeats[1].label)
        with self.assertRaisesRegex(TypeError, "include_inactive must be bool"):
            asyncio.run(heartbeat_module.list("yes"))

    def test_update_maps_every_to_interval(self) -> None:
        host_request = AsyncMock(return_value={"heartbeat": {**HEARTBEAT_ROW, "status": "paused", "instruction": "new watch", "schedule": {"kind": "interval", "expression": "every 30m"}}})
        hb = heartbeat_module.Heartbeat(
            id="cron-1", instruction="watch the mission", every="every 15m", label="watcher", status="active", delivery_mode="steer", next_run_at=None
        )

        with patch.object(rlm_module, "host_request", host_request):
            updated = asyncio.run(hb.update(instruction="new watch", every="every 30m", delivery_mode="follow_up"))

        host_request.assert_awaited_once_with(
            "rlm_heartbeat.update",
            {"id": "cron-1", "instruction": "new watch", "interval": "every 30m", "delivery_mode": "follow_up"},
        )
        self.assertEqual(updated.status, "paused")
        self.assertEqual(updated.every, "every 30m")

    def test_pause_resume_and_delete(self) -> None:
        hb = heartbeat_module.Heartbeat(
            id="cron-1", instruction="watch the mission", every="every 15m", label=None, status="active", delivery_mode="steer", next_run_at=None
        )
        host_request = AsyncMock(return_value={"heartbeat": {**HEARTBEAT_ROW, "status": "paused"}})
        with patch.object(rlm_module, "host_request", host_request):
            paused = asyncio.run(hb.pause())
        host_request.assert_awaited_once_with("rlm_heartbeat.update", {"id": "cron-1", "status": "pause"})
        self.assertEqual(paused.status, "paused")

        host_request = AsyncMock(return_value={"heartbeat": {**HEARTBEAT_ROW, "status": "active"}})
        with patch.object(rlm_module, "host_request", host_request):
            resumed = asyncio.run(hb.resume())
        host_request.assert_awaited_once_with("rlm_heartbeat.update", {"id": "cron-1", "status": "resume"})
        self.assertEqual(resumed.status, "active")

        host_request = AsyncMock(return_value={"heartbeat": None})
        with patch.object(rlm_module, "host_request", host_request):
            self.assertIsNone(asyncio.run(hb.delete()))
        host_request.assert_awaited_once_with("rlm_heartbeat.delete", {"id": "cron-1"})

    def test_invalid_payloads_fail_loudly(self) -> None:
        for reply in ({"heartbeat": None}, {"heartbeat": {"id": "x"}}, {}):
            with self.subTest(reply=reply):
                host_request = AsyncMock(return_value=reply)
                with patch.object(rlm_module, "host_request", host_request):
                    with self.assertRaises(RuntimeError):
                        asyncio.run(heartbeat_module.create("watch"))
        host_request = AsyncMock(return_value={"heartbeats": "no"})
        with patch.object(rlm_module, "host_request", host_request):
            with self.assertRaises(RuntimeError):
                asyncio.run(heartbeat_module.list())


if __name__ == "__main__":
    unittest.main()
