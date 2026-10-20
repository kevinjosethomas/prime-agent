from __future__ import annotations

import asyncio
import importlib
import os
import unittest
from unittest.mock import patch


websearch_module = importlib.import_module("rlm.websearch")
from rlm.websearch import _format_serper_results, websearch


class WebsearchTest(unittest.TestCase):
    def test_formats_serper_results(self) -> None:
        data = {
            "knowledgeGraph": {"title": "Prime Agent", "description": "A harness", "attributes": {"kind": "agent"}},
            "organic": [
                {"title": "First", "link": "https://example.test/1", "snippet": "snippet one"},
                {"title": "", "link": "", "snippet": ""},
            ],
            "peopleAlsoAsk": [{"question": "What?", "snippet": "This."}],
        }

        text = _format_serper_results(data, "prime agent", num_results=5)

        self.assertIn("Knowledge Graph: Prime Agent", text)
        self.assertIn("Result 0: First", text)
        self.assertIn("URL: https://example.test/1", text)
        self.assertIn("Q: What?", text)

    def test_reports_a_missing_api_key(self) -> None:
        with patch.object(websearch_module, "_resolve_api_key", return_value=""):
            output = asyncio.run(websearch("anything"))
        self.assertIn("no Serper API key is configured", output)
        self.assertIn("https://serper.dev", output)

    def test_truncates_oversized_output(self) -> None:
        with patch.object(websearch_module, "_resolve_api_key", return_value="key"), patch.object(
            websearch_module,
            "_fetch_serper",
            side_effect=RuntimeError("boom"),
        ):
            output = asyncio.run(websearch("q"))
        self.assertIn("Error searching for 'q'", output)

    def test_num_results_env_default(self) -> None:
        with patch.dict(os.environ, {"PRIME_AGENT_WEBSEARCH_NUM_RESULTS": "2"}, clear=False):
            captured = {}

            async def fake_fetch(query, api_key, timeout, num_results):
                captured["num_results"] = num_results
                return "found"

            with patch.object(websearch_module, "_resolve_api_key", return_value="key"), patch.object(
                websearch_module, "_fetch_serper", fake_fetch
            ):
                asyncio.run(websearch("q"))
        self.assertEqual(captured["num_results"], 2)


if __name__ == "__main__":
    unittest.main()
