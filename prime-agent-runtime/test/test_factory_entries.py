"""The private saved-entry reader behind factory's stored lookups.

The Rust host owns the shared harness store; factory reads saved entries
through ``rlm._harness_entries``. The compact axes here: local-only
unprefixed lookups with the ``local:``/``global:`` prefixes, the
``(path, title, id)`` list order, empty reads for unconfigured or corrupt
stores, fresh resolution per call (late env assignment and disk rewrites),
and independence from the refinement settings switch.
"""

from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from rlm._harness_entries import SavedHarnessEntries


class SavedHarnessEntriesTest(unittest.TestCase):
    def setUp(self) -> None:
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name).resolve()
        self.local_dir = root / "local"
        self.global_dir = root / "global"
        self.store = SavedHarnessEntries(state_dir=self.local_dir, global_state_dir=self.global_dir)

    def write_store(self, directory: Path, entries: dict[str, Any]) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "harness_state.json").write_text(
            json.dumps({"schema": 1, "entries": entries, "refinements": []}),
            encoding="utf-8",
        )

    def env(self, **values: str) -> None:
        # Set-but-empty values read as unset in the resolution.
        merged = {name: "" for name in ("RLM_HARNESS_STATE_DIR", "RLM_SESSION_DIR", "RLM_GLOBAL_HARNESS_STATE_DIR")}
        merged.update(values)
        patcher = patch.dict(os.environ, merged)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_unprefixed_reads_local_only_and_the_prefixes_route(self) -> None:
        self.write_store(self.local_dir, {"factory": {"sw": {"title": "Local", "content": "c"}}})
        self.write_store(self.global_dir, {"factory": {
            "sw": {"title": "Global", "content": "c"},
            "only": {"title": "Only", "content": "c"},
        }})
        store = SavedHarnessEntries()  # env-resolved, not the explicit dirs
        self.env(
            RLM_HARNESS_STATE_DIR=str(self.local_dir),
            RLM_GLOBAL_HARNESS_STATE_DIR=str(self.global_dir),
        )
        self.assertEqual(store.get("factory", "sw").title, "Local")
        self.assertEqual(store.get("factory", "local:sw").title, "Local")
        self.assertEqual(store.get("factory", "global:sw").title, "Global")
        # A global-only id is invisible to the unprefixed local lookup.
        self.assertIsNone(store.get("factory", "only"))
        self.assertEqual(store.get("factory", "global:only").title, "Only")

    def test_list_sorts_by_path_title_id(self) -> None:
        self.write_store(self.local_dir, {"subagent": {
            "b": {"title": "B", "content": "c"},
            "a": {"title": "B", "content": "c"},
            "z": {"title": "A", "content": "c"},
            "p": {"title": "B", "content": "c", "path": "policy"},
        }})
        self.assertEqual([entry.id for entry in self.store.list("subagent")], ["z", "a", "b", "p"])

    def test_unconfigured_and_corrupt_stores_read_empty(self) -> None:
        self.env()  # no local or global dirs resolve anywhere
        store = SavedHarnessEntries()
        self.assertIsNone(store.get("factory", "sw"))
        self.assertEqual(store.list("subagent"), [])
        # A corrupt document reads as empty, never raises.
        self.write_store(self.local_dir, {"factory": {"sw": {"title": "Local", "content": "c"}}})
        (self.local_dir / "harness_state.json").write_text("{broken", encoding="utf-8")
        self.assertIsNone(self.store.get("factory", "sw"))
        self.assertEqual(self.store.list("factory"), [])

    def test_reads_resolve_fresh_late_env_and_disk_rewrites(self) -> None:
        self.env()
        store = SavedHarnessEntries()  # built before the env assignment
        self.assertIsNone(store.get("factory", "sw"))
        # A late RLM_HARNESS_STATE_DIR lands on the next access.
        os.environ["RLM_HARNESS_STATE_DIR"] = str(self.local_dir)
        self.assertIsNone(store.get("factory", "sw"))
        self.write_store(self.local_dir, {"factory": {"sw": {"title": "First", "content": "c"}}})
        self.assertEqual(store.get("factory", "sw").title, "First")
        # A host rewrite of the file is visible without rebuilding the reader.
        self.write_store(self.local_dir, {"factory": {"sw": {"title": "Second", "content": "c"}}})
        self.assertEqual(store.get("factory", "sw").title, "Second")

    def test_the_refinement_setting_never_blocks_reads(self) -> None:
        agent_temp = TemporaryDirectory()
        self.addCleanup(agent_temp.cleanup)
        settings_path = Path(agent_temp.name) / "settings.json"
        settings_path.write_text(json.dumps({"harnessRefinements": False}), encoding="utf-8")
        self.env(
            PRIME_AGENT_CODING_AGENT_DIR=str(agent_temp.name),
            RLM_HARNESS_STATE_DIR=str(self.local_dir),
        )
        self.write_store(self.local_dir, {"factory": {"sw": {"title": "Local", "content": "c"}}})
        store = SavedHarnessEntries()
        self.assertEqual(store.get("factory", "sw").title, "Local")
        self.assertEqual([entry.id for entry in store.list("factory")], ["sw"])


if __name__ == "__main__":
    unittest.main()
