from __future__ import annotations

import asyncio
import importlib
import unittest
from unittest.mock import AsyncMock, patch


rlm_module = importlib.import_module("rlm")
harness_module = importlib.import_module("rlm.harness")
from rlm.harness import Context, Goal, Model


class HarnessContextTest(unittest.TestCase):
    def test_context_reads_usage_and_pending_compaction(self) -> None:
        host_request = AsyncMock(
            return_value={"tokens": 40_000, "context_window": 200_000, "percent": 20.0, "scheduled": True}
        )

        with patch.object(rlm_module, "host_request", host_request):
            context = asyncio.run(harness_module.context())

        host_request.assert_awaited_once_with("compact.status")
        self.assertEqual(
            context,
            Context(tokens=40_000, window=200_000, percent=20.0, compaction_scheduled=True),
        )

    def test_context_allows_null_usage_right_after_compaction(self) -> None:
        host_request = AsyncMock(
            return_value={"tokens": None, "context_window": None, "percent": None, "scheduled": False}
        )

        with patch.object(rlm_module, "host_request", host_request):
            context = asyncio.run(harness_module.context())

        self.assertEqual(
            context,
            Context(tokens=None, window=None, percent=None, compaction_scheduled=False),
        )

    def test_compact_schedules_and_reports_nothing_to_compact(self) -> None:
        host_request = AsyncMock(return_value={"scheduled": True})
        with patch.object(rlm_module, "host_request", host_request):
            self.assertTrue(asyncio.run(harness_module.compact("focus on the lane")))
        host_request.assert_awaited_once_with("compact.run", {"instructions": "focus on the lane"})

        host_request = AsyncMock(return_value={"scheduled": False, "reason": "no active turn"})
        with patch.object(rlm_module, "host_request", host_request):
            self.assertFalse(asyncio.run(harness_module.compact()))
        host_request.assert_awaited_once_with("compact.run", {})

        with self.assertRaisesRegex(TypeError, "instructions must be str or None"):
            asyncio.run(harness_module.compact(5))

    def test_models_searches_the_registry(self) -> None:
        host_request = AsyncMock(
            return_value={
                "models": [
                    {
                        "provider": "anthropic",
                        "id": "claude-opus-4-7",
                        "name": "Claude Opus 4.7",
                        "selector": "anthropic/claude-opus-4-7",
                    }
                ]
            }
        )

        with patch.object(rlm_module, "host_request", host_request):
            models = asyncio.run(harness_module.models("opus", limit=3))

        host_request.assert_awaited_once_with("rlm.find_models", {"query": "opus", "limit": 3})
        self.assertEqual(
            models,
            [Model(provider="anthropic", id="claude-opus-4-7", name="Claude Opus 4.7", selector="anthropic/claude-opus-4-7")],
        )

    def test_models_validates_arguments_and_payloads(self) -> None:
        with self.assertRaisesRegex(TypeError, "query must be str"):
            asyncio.run(harness_module.models(123))
        with self.assertRaisesRegex(TypeError, "limit must be int"):
            asyncio.run(harness_module.models("opus", limit="3"))
        host_request = AsyncMock(return_value={"models": [{"provider": "anthropic"}]})
        with patch.object(rlm_module, "host_request", host_request):
            with self.assertRaisesRegex(RuntimeError, "invalid model entry"):
                asyncio.run(harness_module.models())


GOAL_ROW = {
    "objective": "finish the port",
    "status": "active",
    "token_budget": 1000,
    "tokens_used": 250,
    "time_used_seconds": 90,
}


class HarnessGoalTest(unittest.TestCase):
    def test_goal_create_and_get_round_trip(self) -> None:
        host_request = AsyncMock(return_value={"goal": GOAL_ROW, "remaining_tokens": 750})
        with patch.object(rlm_module, "host_request", host_request):
            goal = asyncio.run(harness_module.goal.create("finish the port", token_budget=1000))
        host_request.assert_awaited_once_with("goal.create", {"objective": "finish the port", "token_budget": 1000})
        self.assertEqual(
            goal,
            Goal(objective="finish the port", status="active", token_budget=1000, tokens_used=250, remaining_tokens=750, elapsed_ms=90_000),
        )

        host_request = AsyncMock(return_value={"goal": GOAL_ROW, "remaining_tokens": 750})
        with patch.object(rlm_module, "host_request", host_request):
            read = asyncio.run(harness_module.goal.get())
        host_request.assert_awaited_once_with("goal.get")
        self.assertEqual(read, goal)

    def test_goal_get_returns_none_without_a_goal(self) -> None:
        host_request = AsyncMock(return_value={"goal": None, "remaining_tokens": None})
        with patch.object(rlm_module, "host_request", host_request):
            self.assertIsNone(asyncio.run(harness_module.goal.get()))

    def test_goal_create_omits_an_unset_budget(self) -> None:
        host_request = AsyncMock(return_value={"goal": {**GOAL_ROW, "token_budget": None}, "remaining_tokens": None})
        with patch.object(rlm_module, "host_request", host_request):
            asyncio.run(harness_module.goal.create("finish the port"))
        host_request.assert_awaited_once_with("goal.create", {"objective": "finish the port"})

    def test_goal_create_validates_arguments(self) -> None:
        with self.assertRaisesRegex(TypeError, "objective must be str"):
            asyncio.run(harness_module.goal.create(42))
        with self.assertRaisesRegex(TypeError, "token_budget must be int or None"):
            asyncio.run(harness_module.goal.create("o", token_budget="1000"))

    def test_goal_complete_returns_the_completed_goal(self) -> None:
        host_request = AsyncMock(
            return_value={
                "goal": {**GOAL_ROW, "status": "complete", "tokens_used": 1000},
                "remaining_tokens": 0,
                "completion_budget_report": "budget report",
            }
        )
        with patch.object(rlm_module, "host_request", host_request):
            goal = asyncio.run(harness_module.goal.complete())
        host_request.assert_awaited_once_with("goal.complete")
        self.assertEqual(goal.status, "complete")
        self.assertEqual(goal.remaining_tokens, 0)


if __name__ == "__main__":
    unittest.main()
