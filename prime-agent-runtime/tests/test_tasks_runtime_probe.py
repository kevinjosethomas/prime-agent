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

    def test_missing_read_apis_are_not_ready(self):
        for target in (tasks, tasks.Task):
            with self.subTest(target=target.__name__):
                with patch.object(target, "get" if target is tasks else "restore", None):
                    with self.assertRaises(AssertionError):
                        exec(self.probe, {})

    def test_removed_legacy_helpers_are_not_ready(self):
        # A stale pre-slim runtime still exposes upsert or a configure surface;
        # the probe must refuse it.
        with patch.object(tasks, "upsert", lambda *args, **kwargs: None, create=True):
            with self.assertRaises(AssertionError):
                exec(self.probe, {})
        with patch.object(tasks, "configure", lambda *args, **kwargs: None, create=True):
            with self.assertRaises(AssertionError):
                exec(self.probe, {})

    def test_pre_slim_list_signature_is_not_ready(self):
        async def old_list(*, status=None, parent_id=None):
            return ()

        with patch.object(tasks, "list", old_list):
            with self.assertRaises(AssertionError):
                exec(self.probe, {})
