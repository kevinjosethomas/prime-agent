from __future__ import annotations

import asyncio
import importlib
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


rlm_module = importlib.import_module("rlm")
agents_module = importlib.import_module("rlm.agents")
from rlm.agents import Agent, Message, Receipt


SPAWN_PAYLOAD = {
    "rlm_child_id": "sub-a1b2c3d4",
    "session_id": "session-child",
    "name": "api-reviewer",
    "session_dir": "/tmp/parent/sub-a1b2c3d4",
    "model": "anthropic/claude-sonnet-4-5",
}

CREATE_SESSION_PAYLOAD = {
    "active_session_id": "active-root",
    "session_id": "session-root",
    "rlm_child_id": "sub-top1",
    "name": "researcher",
    "session_file": "/tmp/sessions/researcher/session.jsonl",
    "model": "prime-inference/deepseek/deepseek-v4-flash",
}


class AgentsSpawnTest(unittest.TestCase):
    def test_spawn_child_returns_a_child_agent(self) -> None:
        host_request = AsyncMock(return_value=SPAWN_PAYLOAD)

        with patch.object(rlm_module, "host_request", host_request):
            agent = asyncio.run(agents_module.spawn("check the API", name="api-reviewer", model="anthropic/claude-sonnet-4-5"))

        host_request.assert_awaited_once_with(
            "rlm.run",
            {
                "prompt": "check the API",
                "kwargs": {"name": "api-reviewer", "model": "anthropic/claude-sonnet-4-5"},
            },
        )
        self.assertEqual(
            agent,
            Agent(
                name="api-reviewer",
                session_id="session-child",
                relationship="child",
                status="running",
                model="anthropic/claude-sonnet-4-5",
                child_id="sub-a1b2c3d4",
                session_dir=Path("/tmp/parent/sub-a1b2c3d4"),
            ),
        )

    def test_spawn_top_level_creates_a_sibling(self) -> None:
        host_request = AsyncMock(return_value=CREATE_SESSION_PAYLOAD)

        with patch.object(rlm_module, "host_request", host_request):
            agent = asyncio.run(
                agents_module.spawn(
                    "analyze shard 1",
                    name="researcher",
                    thinking="off",
                    top_level=True,
                    cwd="/tmp/project",
                )
            )

        host_request.assert_awaited_once_with(
            "rlm.create_session",
            {
                "prompt": "analyze shard 1",
                "kwargs": {"name": "researcher", "thinking": "off", "cwd": "/tmp/project"},
            },
        )
        self.assertEqual(agent.relationship, "sibling")
        self.assertTrue(agent.top_level)
        self.assertEqual(agent.status, "running")
        self.assertEqual(agent.session_id, "session-root")
        self.assertEqual(agent.child_id, "sub-top1")
        self.assertIsNone(agent.session_dir)

    def test_spawn_requires_a_child_name(self) -> None:
        host_request = AsyncMock()
        with patch.object(rlm_module, "host_request", host_request):
            with self.assertRaisesRegex(TypeError, "needs a name"):
                asyncio.run(agents_module.spawn("check the API"))
        host_request.assert_not_awaited()
        with self.assertRaisesRegex(TypeError, "prompt must be str"):
            asyncio.run(agents_module.spawn(42, name="w"))


def registry_row(**overrides):
    row = {
        "rlm_child_id": "sub-a1b2c3d4",
        "active_session_id": None,
        "session_id": "session-child",
        "session_name": "api-reviewer",
        "session_dir": "/tmp/parent/sub-a1b2c3d4",
        "status": "running",
    }
    row.update(overrides)
    return row


class AgentsListTest(unittest.TestCase):
    def test_list_maps_registry_rows_to_agents(self) -> None:
        host_request = AsyncMock(
            return_value={
                "subagents": [
                    registry_row(status="completed", note="halfway done"),
                    registry_row(
                        rlm_child_id="sub-2",
                        session_id=None,
                        session_name="worker-2",
                        session_dir="/tmp/parent/sub-2",
                        status="cancelled",
                        progress_note="legacy note field",
                    ),
                    registry_row(
                        rlm_child_id="sub-top",
                        session_id="session-top",
                        session_name="root-2",
                        session_dir="/tmp/roots/root-2",
                        status="running",
                        top_level=True,
                    ),
                ]
            }
        )

        with patch.object(rlm_module, "host_request", host_request):
            agents = asyncio.run(agents_module.list())

        host_request.assert_awaited_once_with("rlm.list_subagents")
        self.assertEqual([agent.status for agent in agents], ["done", "error", "running"])
        self.assertEqual(agents[0].note, "halfway done")
        self.assertEqual(agents[1].note, "legacy note field")
        self.assertEqual(agents[0].child_id, "sub-a1b2c3d4")
        self.assertEqual(agents[0].session_dir, Path("/tmp/parent/sub-a1b2c3d4"))
        self.assertFalse(agents[0].top_level)
        self.assertEqual(agents[2].relationship, "sibling")
        self.assertTrue(agents[2].top_level)
        self.assertEqual(agents[2].session_id, "session-top")

    def test_list_rejects_invalid_rows(self) -> None:
        for row in ({"status": "completed"}, registry_row(status="nonsense"), {"subagents": "no"}):
            with self.subTest(row=row):
                host_request = AsyncMock(return_value={"subagents": [row]} if isinstance(row, dict) else row)
                with patch.object(rlm_module, "host_request", host_request):
                    with self.assertRaises(RuntimeError):
                        asyncio.run(agents_module.list())


def roster_row(relationship, **overrides):
    row = {
        "activeSessionId": "live-1",
        "sessionId": "session-1",
        "sessionName": "worker",
        "relationship": relationship,
        "status": "running",
        "isCurrent": False,
    }
    row.update(overrides)
    return row


class AgentsRosterTest(unittest.TestCase):
    def test_parent_and_sibling_roster_reads(self) -> None:
        host_request = AsyncMock(
            return_value={
                "agents": [
                    roster_row("parent", sessionName="orchestrator", status="idle", note="waiting"),
                    roster_row("sibling", sessionId="sib-1"),
                    roster_row("sibling", sessionId="sib-2", status="inactive"),
                    {"sessionId": "self", "relationship": None, "status": "running"},
                ]
            }
        )

        with patch.object(rlm_module, "host_request", host_request):
            parent = asyncio.run(agents_module.parent())
            siblings = asyncio.run(agents_module.siblings())

        self.assertEqual(parent.name, "orchestrator")
        self.assertEqual(parent.status, "idle")
        self.assertEqual(parent.note, "waiting")
        self.assertEqual([(sibling.session_id, sibling.status) for sibling in siblings], [("sib-1", "running"), ("sib-2", "idle")])
        self.assertEqual([call.args[0] for call in host_request.await_args_list], ["agent_observe.list", "agent_observe.list"])

    def test_parent_is_none_without_one(self) -> None:
        host_request = AsyncMock(return_value={"agents": []})
        with patch.object(rlm_module, "host_request", host_request):
            self.assertIsNone(asyncio.run(agents_module.parent()))


class AgentsNoteTest(unittest.TestCase):
    def test_note_round_trips_accepted_and_throttled(self) -> None:
        host_request = AsyncMock(return_value={"accepted": True})
        with patch.object(rlm_module, "host_request", host_request):
            self.assertTrue(asyncio.run(agents_module.note("building tests")))
        host_request.assert_awaited_once_with("rlm.progress.note", {"message": "building tests"})

        host_request = AsyncMock(return_value={"accepted": False, "retry_after_ms": 7000})
        with patch.object(rlm_module, "host_request", host_request):
            self.assertFalse(asyncio.run(agents_module.note("second note")))

    def test_note_validates_the_message(self) -> None:
        with self.assertRaisesRegex(TypeError, "message must be str"):
            asyncio.run(agents_module.note(42))
        with self.assertRaisesRegex(ValueError, "empty"):
            asyncio.run(agents_module.note("   "))
        with self.assertRaisesRegex(ValueError, "at most 512"):
            asyncio.run(agents_module.note("x" * 513))
        # 512 astral characters are 1024 UTF-16 code units under the host's measure.
        with self.assertRaisesRegex(ValueError, "at most 512"):
            asyncio.run(agents_module.note("🎉" * 512))

    def test_rename_dispatches_self_and_child_selectors(self) -> None:
        host_request = AsyncMock(return_value={"name": "bench-runner"})
        with patch.object(rlm_module, "host_request", host_request):
            self.assertEqual(asyncio.run(agents_module.rename("bench-runner")), "bench-runner")
        host_request.assert_awaited_once_with("rlm.rename", {"name": "bench-runner"})

        child = Agent(name="w", session_id=None, relationship="child", status="running", child_id="sub-a1b2c3d4")
        host_request = AsyncMock(return_value={"name": "bench-runner"})
        with patch.object(rlm_module, "host_request", host_request):
            renamed = asyncio.run(child.rename("bench-runner"))
        self.assertEqual(renamed.name, "bench-runner")
        self.assertEqual(renamed.child_id, "sub-a1b2c3d4")
        host_request.assert_awaited_once_with("rlm.rename", {"name": "bench-runner", "session_id": "sub-a1b2c3d4"})


CHILD = Agent(name="api-reviewer", session_id=None, relationship="child", status="running", child_id="sub-a1b2c3d4")
TOP_LEVEL_PEER = Agent(
    name="researcher", session_id="session-root", relationship="sibling", status="running", child_id="sub-top1", top_level=True
)


class AgentMessagingTest(unittest.TestCase):
    def test_send_addresses_each_relationship(self) -> None:
        cases = [
            (Agent(name="orchestrator", session_id="s-parent", relationship="parent", status="running"), {"receiver_role": "parent"}),
            (Agent(name="peer", session_id="s-peer", relationship="sibling", status="running"), {"receiver_role": "sibling", "receiver_name": "s-peer"}),
            (CHILD, {"receiver_role": "child", "receiver_name": "sub-a1b2c3d4"}),
        ]
        for agent, expected in cases:
            with self.subTest(relationship=agent.relationship):
                host_request = AsyncMock(return_value={"id": "agentmsg_1", "deliveryStatus": "delivered"})
                with patch.object(rlm_module, "host_request", host_request):
                    receipt = asyncio.run(agent.send("hi" if agent.relationship != "parent" else "done"))
                self.assertEqual(receipt, Receipt(delivered=True, message_id="agentmsg_1"))
                self.assertEqual(host_request.await_args.args[0], "agent_message.send")
                payload = host_request.await_args.args[1]
                self.assertEqual({key: value for key, value in payload.items() if key != "message"}, expected)

    def test_send_emits_the_display_receipt(self) -> None:
        emitted = []
        with patch.object(rlm_module, "emit", side_effect=lambda data: emitted.append(data)), patch.object(
            rlm_module,
            "host_request",
            AsyncMock(return_value={"id": "agentmsg_2", "deliveryStatus": "queued"}),
        ):
            receipt = asyncio.run(CHILD.send("halfway"))
        self.assertEqual(receipt.delivered, False)
        self.assertIn("application/vnd.prime-agent.agent-message+json", emitted[0])
        self.assertEqual(emitted[0]["application/vnd.prime-agent.agent-message+json"]["receiverRole"], "child")

    def test_broadcast_sends_once_per_family_member(self) -> None:
        host_request = AsyncMock(
            return_value={
                "receipts": [
                    {"id": "agentmsg_1", "deliveryStatus": "delivered", "target": {"sessionId": "a"}},
                    {"id": "agentmsg_2", "deliveryStatus": "queued", "target": {"sessionId": "b"}},
                    {"target": "c", "error": "unreachable"},
                ]
            }
        )

        with patch.object(rlm_module, "host_request", host_request):
            receipts = asyncio.run(agents_module.broadcast("status update"))

        host_request.assert_awaited_once_with("agent_message.send", {"target": "all", "message": "status update"})
        self.assertEqual(
            receipts,
            [Receipt(delivered=True, message_id="agentmsg_1"), Receipt(delivered=False, message_id="agentmsg_2"), Receipt(delivered=False, message_id="")],
        )

    def test_recent_reads_bounded_previews(self) -> None:
        host_request = AsyncMock(
            return_value={
                "messages": [
                    {"role": "user", "text": "first", "timestamp": 1757700000000},
                    {"role": "assistant", "text": "second", "timestamp": None},
                ]
            }
        )
        sibling = Agent(name="peer", session_id="s-peer", relationship="sibling", status="running")

        with patch.object(rlm_module, "host_request", host_request):
            messages = asyncio.run(sibling.recent(limit=5))

        host_request.assert_awaited_once_with(
            "agent_observe.recent", {"target": "s-peer", "limit": 5, "max_chars": 800}
        )
        self.assertEqual(
            messages, [Message(role="user", text="first", timestamp=1757700000000), Message(role="assistant", text="second", timestamp=None)]
        )


class AgentWaitStopTest(unittest.TestCase):
    def test_wait_converts_seconds_to_host_milliseconds(self) -> None:
        host_request = AsyncMock(
            return_value={
                "results": [
                    {
                        "rlm_child_id": "sub-a1b2c3d4",
                        "status": "running",
                        "settled": False,
                        "answer_preview": None,
                    }
                ]
            }
        )

        with patch.object(rlm_module, "host_request", host_request):
            result = asyncio.run(CHILD.wait(2.5))

        host_request.assert_awaited_once_with("rlm.collect", {"targets": ["sub-a1b2c3d4"], "timeout_ms": 2500})
        self.assertEqual(result.status, "running")
        self.assertIsNone(result.answer)

    def test_wait_maps_terminal_statuses_to_results(self) -> None:
        for status, expected in (("done", "done"), ("error", "error"), ("cancelled", "error"), ("queued", "running")):
            with self.subTest(status=status):
                host_request = AsyncMock(
                    return_value={
                        "results": [
                            {
                                "rlm_child_id": "sub-a1b2c3d4",
                                "status": status,
                                "settled": True,
                                "answer_preview": "preview text",
                                "answer_text": "full answer",
                                "error": None,
                                "duration_ms": 4321,
                                "tool_use_count": 3,
                            }
                        ]
                    }
                )
                with patch.object(rlm_module, "host_request", host_request):
                    result = asyncio.run(CHILD.wait(0))
                self.assertEqual(
                    result,
                    agents_module.Result(status=expected, answer="full answer", error=None, duration_ms=4321, tool_use_count=3),
                )

    def test_wait_without_a_timeout_uses_the_host_maximum(self) -> None:
        host_request = AsyncMock(
            return_value={"results": [{"rlm_child_id": "sub-a1b2c3d4", "status": "done", "settled": True, "answer_preview": "ok"}]}
        )

        with patch.object(rlm_module, "host_request", host_request):
            asyncio.run(CHILD.wait())

        self.assertEqual(host_request.await_args.args[1]["timeout_ms"], agents_module._COLLECT_MAX_TIMEOUT_MS)

    def test_wait_and_stop_need_a_spawned_child_id(self) -> None:
        for agent in (
            Agent(name="peer", session_id="s-peer", relationship="sibling", status="running"),
            Agent(name="child-without-id", session_id=None, relationship="child", status="running"),
        ):
            with self.subTest(relationship=agent.relationship):
                with self.assertRaisesRegex(TypeError, "not one of this session's spawned agents"):
                    asyncio.run(agent.wait())
                with self.assertRaisesRegex(TypeError, "not one of this session's spawned agents"):
                    asyncio.run(agent.stop())

    def test_stop_deletes_the_child(self) -> None:
        host_request = AsyncMock(return_value={"subagent": registry_row(status="completed")})
        with patch.object(rlm_module, "host_request", host_request):
            asyncio.run(CHILD.stop())
        host_request.assert_awaited_once_with("rlm.delete_subagent", {"target": "sub-a1b2c3d4"})

    def test_wait_and_stop_work_on_spawned_top_level_peers(self) -> None:
        host_request = AsyncMock(
            return_value={"results": [{"rlm_child_id": "sub-top1", "status": "done", "settled": True, "answer_preview": "ok"}]}
        )
        with patch.object(rlm_module, "host_request", host_request):
            result = asyncio.run(TOP_LEVEL_PEER.wait(1))
        self.assertEqual(result.status, "done")

        host_request = AsyncMock(return_value={"subagent": registry_row(status="completed")})
        with patch.object(rlm_module, "host_request", host_request):
            asyncio.run(TOP_LEVEL_PEER.stop())
        host_request.assert_awaited_once_with("rlm.delete_subagent", {"target": "sub-top1"})

    def test_wait_gathers_several_children(self) -> None:
        # The multi-child wait is asyncio.gather over one-child waits; no extra API.
        kids = [
            Agent(name="a", session_id=None, relationship="child", status="running", child_id="sub-1"),
            Agent(name="b", session_id=None, relationship="child", status="running", child_id="sub-2"),
        ]
        replies = {
            "sub-1": {"rlm_child_id": "sub-1", "status": "done", "settled": True, "answer_preview": "one"},
            "sub-2": {"rlm_child_id": "sub-2", "status": "error", "settled": True, "error": "boom"},
        }

        async def fake_host(request_type, payload):
            self.assertEqual(request_type, "rlm.collect")
            return {"results": [replies[payload["targets"][0]]]}

        async def scenario():
            with patch.object(rlm_module, "host_request", AsyncMock(side_effect=fake_host)):
                return await asyncio.gather(*(kid.wait(1) for kid in kids))

        results = asyncio.run(scenario())
        self.assertEqual([result.status for result in results], ["done", "error"])


if __name__ == "__main__":
    unittest.main()
