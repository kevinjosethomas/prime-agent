"""Run the shipped readiness assertion against current and stale tasks APIs."""

from __future__ import annotations

import json
from pathlib import Path
import re
import unittest
from unittest.mock import patch

import tasks


class RuntimeReadinessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[2]
        source = (root / "crates/pa-core/src/kernel/bootstrap/venv/probe.rs").read_text()
        cls.probe = json.loads(re.search(r'const RUNTIME_READY_CHECK: &str = (".*");', source).group(1))

    def test_current_runtime_is_ready(self):
        exec(self.probe, {})

    def test_pre_archive_facade_is_not_ready(self):
        with patch.object(tasks, "archive", None):
            with self.assertRaises(AssertionError):
                exec(self.probe, {})

    def test_pre_archive_handles_are_not_ready(self):
        for target in (tasks.Client, tasks.Task):
            with self.subTest(target=target.__name__):
                with patch.object(target, "restore", None):
                    with self.assertRaises(AssertionError):
                        exec(self.probe, {})

    def test_missing_assign_messages_is_not_ready(self):
        with patch.object(tasks, "assign_messages", None):
            with self.assertRaises(AssertionError):
                exec(self.probe, {})

    def test_pre_archive_list_is_not_ready(self):
        async def old_list(*, status=None, parent_id=None):
            return ()

        for target in (tasks, tasks.Client):
            with self.subTest(target=target.__name__):
                with patch.object(target, "list", old_list):
                    with self.assertRaises(AssertionError):
                        exec(self.probe, {})
