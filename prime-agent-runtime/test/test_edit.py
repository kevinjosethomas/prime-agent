from __future__ import annotations

import asyncio
import importlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


rlm_module = importlib.import_module("rlm")
edit_module = importlib.import_module("rlm.edit")
from rlm.edit import edit


class EditTest(unittest.TestCase):
    def test_replaces_one_unique_occurrence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "file.txt"
            target.write_text("alpha\nbeta\ngamma\n", encoding="utf-8")

            result = asyncio.run(edit(str(target), "beta", "BETA"))

            self.assertEqual(target.read_text(encoding="utf-8"), "alpha\nBETA\ngamma\n")
            self.assertIn(str(target.resolve()), result)

    def test_reports_missing_and_ambiguous_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "file.txt"
            target.write_text("same\nsame\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "need exactly 1"):
                asyncio.run(edit(str(target), "same", "other"))
        with self.assertRaises(FileNotFoundError):
            asyncio.run(edit("/nonexistent-path-xyz/file.txt", "nope", "other"))

    def test_emits_the_diff_display_event(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "file.txt"
            target.write_text("one\n", encoding="utf-8")
            emitted = []
            with patch.object(rlm_module, "emit", side_effect=lambda data: emitted.append(data)):
                asyncio.run(edit(str(target), "one", "two"))
        payload = emitted[0]["application/vnd.prime-agent.diff+json"]
        self.assertEqual(payload["old_str"], "one")
        self.assertEqual(payload["new_str"], "two")
        self.assertEqual(payload["start_line"], 1)


if __name__ == "__main__":
    unittest.main()
