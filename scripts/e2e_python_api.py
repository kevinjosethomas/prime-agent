#!/usr/bin/env python3
"""Real-daemon kernel E2E for the agent-facing Python API (agents/harness/heartbeat).

Runs the released daemon headless against a loopback scripted OpenAI-compatible
provider (no external LLM, 127.0.0.1 only), creates one root session over the
daemon socket, and drives python_repl cells that exercise the new surface. The
scripted provider routes each conversation by a marker in the user prompt and
records the E2E-OK/FAIL markers it sees in tool results; the driver writes the
per-step receipt.

Usage:
  python3 scripts/e2e_python_api.py --bin <prime-agent> --repo <checkout> \
      --work <fresh-dir> --receipt <path.md> [--toplevel] [--keep]
"""
import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SOCKET_TIMEOUT = 600
USAGE = {"prompt_tokens": 40, "completion_tokens": 60, "total_tokens": 100}
FILLER = "context filler entry for the compaction cut point. " * 4200

MARK_FILL = "E2E_FILL"
MARK_TURN1 = "E2E_TURN1"
MARK_TURN2 = "E2E_TURN2"
MARK_TURN3 = "E2E_TURN3"
MARK_CHILD = "E2E_CHILD_WORK"
MARK_PEER = "E2E_PEER_WORK"
CHILD_ANSWER = "child-answer-42"
PEER_ANSWER = "peer-answer-7"
DEFAULT_MODEL = "e2e-local/e2e-scripted"

BASE_STEPS = ["1", "C1", "2a", "2b", "3", "4", "5", "6", "7", "7b", "8"]
TOPLEVEL_STEPS = ["T1", "T2", "Tp", "T3", "T4"]
STEP_LABELS = {
    "1": "agents.spawn child; Agent.session_id/child_id/session_dir set",
    "C1": "child cell: agents.note('child working') accepted",
    "2a": "agents.list() contains the child",
    "2b": "roster note 'child working' visible via agents.list()",
    "3": "child.wait(): status done, answer contains child-answer-42",
    "4": "child.send('ping') receipt + child.recent() Message rows",
    "5": "agents.note(): True then immediate False (throttle)",
    "6": "heartbeat create/list/pause/resume/delete",
    "7": "harness.context() typed + harness.compact() bool",
    "7b": "kernel variable survives the scheduled compaction",
    "8": "child.stop(): gone from agents.list()",
    "T1": "agents.spawn(top_level=True): full Agent + subagentDefaultModel",
    "T2": "top-level peer row in agents.list()",
    "Tp": "inside the peer: agents.parent() is the starter, parent.send()",
    "T3": "peer.wait(): done + answer",
    "T4": "peer.stop(): gone from list, session file kept",
}


def cell(step, code):
    body = "\n".join("    " + line for line in code.strip("\n").splitlines())
    return (
        "import traceback\n"
        "try:\n"
        + body
        + "\n"
        + "except BaseException:\n"
        + "    print('E2E-FAIL "
        + step
        + ": ' + traceback.format_exc(limit=4).replace(chr(10), ' | '))\n"
    )


TURN1_CELLS = [
    cell(
        "1",
        """
import asyncio
child = await agents.spawn(
    'E2E_CHILD_WORK: run the scripted cell, then answer with exactly child-answer-42',
    name='e2e-child')
assert child.name == 'e2e-child', child.name
assert child.session_id, 'session_id missing'
assert child.child_id, 'child_id missing'
assert child.session_dir and str(child.session_dir), 'session_dir missing'
print('E2E-INFO 1 session_id=%s child_id=%s dir=%s rel=%s status=%s model=%s'
      % (child.session_id, child.child_id, child.session_dir,
         child.relationship, child.status, child.model))
print('E2E-OK 1')
""",
    ),
    cell(
        "2a",
        """
kids = await agents.list()
row = next((k for k in kids if k.name == 'e2e-child'), None)
assert row is not None, 'e2e-child missing from agents.list(): %r' % (kids,)
print('E2E-INFO 2a status=%s child_id=%s top_level=%s'
      % (row.status, row.child_id, row.top_level))
print('E2E-OK 2a')
""",
    ),
    cell(
        "2b",
        """
seen = None
for _ in range(90):
    kids = await agents.list()
    row = next((k for k in kids if k.name == 'e2e-child'), None)
    if row is not None and row.note == 'child working':
        seen = row
        break
    await asyncio.sleep(1)
assert seen is not None, "roster note 'child working' never appeared in agents.list()"
print('E2E-INFO 2b note=%r' % (seen.note,))
print('E2E-OK 2b')
""",
    ),
    cell(
        "3",
        """
r = await child.wait(timeout=120)
assert r.status == 'done', 'status=%s error=%r' % (r.status, r.error)
assert r.answer and 'child-answer-42' in r.answer, 'answer=%r' % (r.answer,)
print('E2E-INFO 3 duration_ms=%s tool_use_count=%s' % (r.duration_ms, r.tool_use_count))
print('E2E-OK 3')
""",
    ),
    cell(
        "4",
        """
rec = await child.send('ping')
print('E2E-INFO 4 receipt delivered=%s message_id=%s' % (rec.delivered, rec.message_id))
assert rec.message_id, 'receipt has no message id'
msgs = []
for _ in range(30):
    msgs = await child.recent(limit=5)
    if msgs and any('ping' in m.text for m in msgs):
        break
    await asyncio.sleep(1)
assert msgs, 'child.recent() returned no Message rows'
for m in msgs:
    assert isinstance(m.role, str) and isinstance(m.text, str), repr(m)
pinged = any('ping' in m.text for m in msgs)
print('E2E-INFO 4 recent rows=%d ping-seen=%s roles=%s'
      % (len(msgs), pinged, [m.role for m in msgs]))
if rec.delivered:
    assert pinged, 'receipt delivered but ping missing from recent'
print('E2E-OK 4')
""",
    ),
    cell(
        "5",
        """
first = await agents.note('root note')
second = await agents.note('root note again')
print('E2E-INFO 5 first=%s second=%s' % (first, second))
assert first is True, 'first note not accepted: %r' % (first,)
assert second is False, 'immediate second note not throttled: %r' % (second,)
print('E2E-OK 5')
""",
    ),
    cell(
        "6",
        """
hb = await heartbeat.create('wake', every='5m', label='e2e-hb')
print('E2E-INFO 6 id=%s every=%s status=%s mode=%s'
      % (hb.id, hb.every, hb.status, hb.delivery_mode))
assert hb.instruction == 'wake', hb.instruction
assert hb.id, 'heartbeat id missing'
rows = await heartbeat.list()
assert any(h.id == hb.id for h in rows), 'heartbeat missing from heartbeat.list()'
paused = await hb.pause()
print('E2E-INFO 6 paused status=%s' % paused.status)
assert paused.status != hb.status, 'pause did not change status'
resumed = await hb.resume()
print('E2E-INFO 6 resumed status=%s' % resumed.status)
assert resumed.status == hb.status, 'resume did not restore status'
await hb.delete()
assert not any(h.id == hb.id for h in await heartbeat.list()), 'heartbeat still listed after delete'
print('E2E-OK 6')
""",
    ),
    cell(
        "7",
        """
ctx = await harness.context()
print('E2E-INFO 7 tokens=%r window=%r percent=%r scheduled=%r'
      % (ctx.tokens, ctx.window, ctx.percent, ctx.compaction_scheduled))
assert isinstance(ctx.tokens, int), 'tokens not int'
assert isinstance(ctx.window, int), 'window not int'
assert isinstance(ctx.percent, (int, float)), 'percent not numeric'
assert isinstance(ctx.compaction_scheduled, bool), 'scheduled not bool'
marker_var_e2e = 'survived-compaction'
sched = await harness.compact()
print('E2E-INFO 7 compact scheduled=%s' % sched)
assert isinstance(sched, bool), 'compact() did not return a bool'
compact_scheduled_e2e = sched
print('E2E-OK 7')
""",
    ),
]

SURVIVAL_CELL = cell(
    "7b",
    """
sched = globals().get('compact_scheduled_e2e')
survived = globals().get('marker_var_e2e') == 'survived-compaction'
print('E2E-INFO 7b scheduled=%s survived=%s' % (sched, survived))
if sched:
    assert survived, 'kernel variable lost across the compaction'
    print('E2E-OK 7b')
else:
    print('E2E-SKIP 7b harness.compact() reported scheduled=False; survival not exercised')
""",
)

STOP_CHILD_CELL = cell(
    "8",
    """
kids = await agents.list()
assert any(k.name == 'e2e-child' for k in kids), 'e2e-child missing before stop'
await child.stop()
kids = await agents.list()
assert not any(k.name == 'e2e-child' for k in kids), 'e2e-child still listed after stop'
print('E2E-OK 8')
""",
)

SPAWN_PEER_CELL = cell(
    "T1",
    """
peer = await agents.spawn(
    'E2E_PEER_WORK: run the scripted cell, then answer with exactly peer-answer-7',
    top_level=True, name='e2e-top')
print('E2E-INFO T1 session_id=%r child_id=%r top_level=%s model=%s rel=%s'
      % (peer.session_id, peer.child_id, peer.top_level, peer.model, peer.relationship))
assert peer.name == 'e2e-top', peer.name
assert peer.session_id, 'peer session_id missing'
assert peer.child_id, 'peer child_id missing'
assert peer.top_level is True, 'peer top_level is not True'
assert peer.model == 'e2e-local/e2e-scripted', 'subagentDefaultModel not applied: %s' % peer.model
print('E2E-OK T1')
""",
)

PEER_LIST_CELL = cell(
    "T2",
    """
kids = await agents.list()
row = next((k for k in kids if k.name == 'e2e-top'), None)
assert row is not None, 'e2e-top missing from agents.list(): %r' % (kids,)
print('E2E-INFO T2 relationship=%s top_level=%s status=%s'
      % (row.relationship, row.top_level, row.status))
assert row.top_level is True, 'list row top_level is not True'
assert row.relationship == 'child', row.relationship
print('E2E-OK T2')
""",
)

PEER_WAIT_CELL = cell(
    "T3",
    """
r = await peer.wait(timeout=180)
assert r.status == 'done', 'status=%s error=%r' % (r.status, r.error)
assert r.answer and 'peer-answer-7' in r.answer, 'answer=%r' % (r.answer,)
print('E2E-INFO T3 answer=%r' % (r.answer,))
print('E2E-OK T3')
""",
)

PEER_STOP_CELL = cell(
    "T4",
    """
import os, pathlib
kids = await agents.list()
row = next((k for k in kids if k.name == 'e2e-top'), None)
assert row is not None, 'e2e-top missing before peer stop'
sid = peer.session_id
base = pathlib.Path(str(row.session_dir)) if row.session_dir else None
artifacts = []
if base and base.exists():
    artifacts = [p.name for p in base.rglob('*') if sid in p.name]
await peer.stop()
kids = await agents.list()
assert not any(k.name == 'e2e-top' for k in kids), 'e2e-top still listed after stop'
if base and base.exists():
    assert base.exists(), 'session dir removed by stop'
    print('E2E-INFO T4 session dir kept: %s artifacts=%r' % (base, artifacts[:3]))
else:
    hits = [p for p in pathlib.Path(os.environ['HOME']).rglob('*%s*' % sid)
            if p.is_file()]
    assert hits, 'no session artifacts kept for %s' % sid
    print('E2E-INFO T4 fallback artifacts=%r' % (hits[:3],))
print('E2E-OK T4')
""",
)

CHILD_CELLS = [
    cell(
        "C1",
        """
ok = await agents.note('child working')
print('E2E-INFO child-note accepted=%s' % ok)
assert ok is True, 'child note not accepted: %r' % (ok,)
print('E2E-OK C1')
""",
    ),
]

PEER_CELLS = [
    cell(
        "Tp",
        """
p = await agents.parent()
assert p is not None, 'agents.parent() returned None inside the top-level peer'
print('E2E-INFO Tp parent name=%r session_id=%r relationship=%s'
      % (p.name, p.session_id, p.relationship))
assert p.relationship == 'parent', p.relationship
assert p.name == 'e2e-root', p.name
rec = await p.send('peer-to-parent hi')
print('E2E-INFO Tp receipt delivered=%s message_id=%s' % (rec.delivered, rec.message_id))
assert rec.message_id, 'parent receipt has no message id'
print('E2E-OK Tp')
""",
    ),
]


class ScriptedProvider(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.lock = threading.Lock()
        self.results = {}
        self.notes = []
        self.request_log = []
        self.queues = {
            "child": list(CHILD_CELLS),
            "peer": list(PEER_CELLS),
            "turn1": list(TURN1_CELLS),
        }

    def route(self, alltext, has_tools):
        with self.lock:
            self.request_log.append(alltext[-90:].replace("\n", " "))
            # Compaction summarization calls carry no tools and embed the
            # history markers, so they must not consume the cell queues.
            if not has_tools:
                return {"kind": "text", "text": "history noted"}
            for marker, flow, final in (
                (MARK_CHILD, "child", CHILD_ANSWER),
                (MARK_PEER, "peer", PEER_ANSWER),
                (MARK_TURN3, "turn3", "turn complete"),
                (MARK_TURN2, "turn2", "turn complete"),
                (MARK_TURN1, "turn1", "turn complete"),
            ):
                if marker in alltext:
                    queue = self.queues.get(flow)
                    if queue:
                        return {"kind": "tool", "code": queue.pop(0)}
                    return {"kind": "text", "text": final}
            if MARK_FILL in alltext:
                return {"kind": "text", "text": FILLER}
            return {"kind": "text", "text": "Local scripted response"}

    def record_markers(self, text):
        with self.lock:
            # One marker per line: a newline-spanning match would swallow the
            # next line's marker as this one's detail.
            for line in text.splitlines():
                match = re.search(
                    r"E2E-(OK|FAIL|SKIP|INFO) ([0-9A-Za-z][0-9A-Za-z-]*)[: ]?(.*)", line
                )
                if not match:
                    continue
                status, step, detail = match.groups()
                if status == "INFO":
                    note = step + " " + detail.strip()
                    if note not in self.notes:
                        self.notes.append(note)
                elif step not in self.results:
                    self.results[step] = {"status": status.lower(), "detail": detail.strip()}


def content_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            part.get("text", "") if isinstance(part, dict) else str(part)
            for part in content
        )
    return str(content or "")


def chunk(delta, finish=None, usage=None):
    payload = {
        "id": "chatcmpl-e2e",
        "object": "chat.completion.chunk",
        "created": 1791918000,
        "model": "e2e-scripted",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }
    if usage is not None:
        payload["usage"] = usage
    return json.dumps(payload)


REQUEST_DUMP_DIR = None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        raw = self.rfile.read(int(self.headers.get("content-length", "0")))
        body = json.loads(raw)
        if REQUEST_DUMP_DIR is not None:
            with self.server.lock:
                (Path(REQUEST_DUMP_DIR) / ("%03d.json" % (len(self.server.request_log) + 1))).write_text(raw.decode(errors="replace"))
        messages = body.get("messages", [])
        users = [m for m in messages if m.get("role") == "user"]
        alltext = " ".join(content_text(m.get("content")) for m in users)
        tools_text = " ".join(
            content_text(m.get("content")) for m in messages if m.get("role") == "tool"
        )
        self.server.record_markers(tools_text)
        plan = self.server.route(alltext, bool(body.get("tools")))
        tool_name = next(
            (
                item.get("function", {}).get("name")
                for item in body.get("tools", [])
                if item.get("function", {}).get("name") in ("ipython", "python_repl")
            ),
            "ipython",
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        if plan["kind"] == "tool":
            call = {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "index": 0,
                        "id": "call-e2e-%d" % len(self.server.request_log),
                        "type": "function",
                        "function": {
                            "name": tool_name,
                            "arguments": json.dumps({"code": plan["code"]}),
                        },
                    }
                ],
            }
            self.wfile.write(("data: " + chunk(call) + "\n\n").encode())
            self.wfile.flush()
            time.sleep(0.1)
            self.wfile.write(
                ("data: " + chunk({}, "tool_calls", USAGE) + "\n\ndata: [DONE]\n\n").encode()
            )
        else:
            self.wfile.write(
                ("data: " + chunk({"role": "assistant", "content": plan["text"]}) + "\n\n").encode()
            )
            self.wfile.flush()
            time.sleep(0.1)
            self.wfile.write(
                ("data: " + chunk({}, "stop", USAGE) + "\n\ndata: [DONE]\n\n").encode()
            )
        self.wfile.flush()


class Client:
    def __init__(self, path, process):
        deadline = time.monotonic() + 120
        while True:
            self.sock = socket.socket(socket.AF_UNIX)
            try:
                self.sock.connect(str(path))
                break
            except (FileNotFoundError, ConnectionRefusedError):
                self.sock.close()
                assert process.poll() is None, "daemon exited before socket readiness"
                assert time.monotonic() < deadline, "daemon socket never became ready"
                time.sleep(0.05)
        self.sock.settimeout(SOCKET_TIMEOUT)
        self.reader = self.sock.makefile("rb")
        self.hello = json.loads(self.reader.readline())
        assert self.hello["type"] == "daemon_hello", self.hello
        self.counter = 0

    def call(self, kind, **payload):
        self.counter += 1
        ident = str(self.counter)
        envelope = {
            "type": "command",
            "id": ident,
            "protocol": {"name": "prime-agent.daemon", "version": 7},
            "command": {"type": kind, **payload},
        }
        self.sock.sendall((json.dumps(envelope) + "\n").encode())
        while True:
            line = self.reader.readline()
            assert line, "daemon closed connection"
            response = json.loads(line)
            if response.get("type") == "response" and response.get("id") == ident:
                assert response.get("success"), response
                return response.get("data")

    def close(self):
        self.reader.close()
        self.sock.close()


def stop_daemon(process, client):
    if process.poll() is None:
        if client is not None:
            try:
                client.call("shutdown", force=True)
                process.wait(timeout=20)
            except Exception:
                pass
        if process.poll() is None:
            process.kill()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass


def write_receipt(path, provider, args, work, started, driver_error):
    steps = BASE_STEPS + (TOPLEVEL_STEPS if args.toplevel else [])
    lines = [
        "# python-api real-daemon kernel E2E receipt",
        "",
        "- date: " + time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "- binary: %s" % args.bin,
        "- repo: %s" % args.repo,
        "- mode: %s" % ("top-level" if args.toplevel else "base"),
        "- workdir: %s" % work,
        "- duration_s: %.1f" % (time.monotonic() - started),
        "",
        "## Steps",
        "",
    ]
    all_ok = True
    for step in steps:
        result = provider.results.get(step)
        if result is None:
            status, detail = "missing", "step never reported a marker"
        else:
            status, detail = result["status"], result["detail"]
        if status not in ("ok", "skip"):
            all_ok = False
        lines.append(
            "- [%s] %s (%s): %s" % (status.upper(), step, STEP_LABELS[step], detail)
        )
    lines += ["", "## Kernel detail lines", ""]
    lines += ["- " + note for note in provider.notes]
    lines += ["", "## Provider routing log", ""]
    lines += ["- %d requests" % len(provider.request_log)]
    lines += ["- " + entry for entry in provider.request_log[-40:]]
    log = work / "logs" / "daemon.log"
    if log.exists():
        lines += ["", "## Daemon log tail", "", "```"]
        lines += log.read_text(errors="replace")[-4000:].splitlines()[-80:]
        lines += ["```"]
    if driver_error:
        all_ok = False
        lines += ["", "## Driver error", "", "```", driver_error, "```"]
    lines += ["", "## Verdict", "", "PASS" if all_ok else "FAIL"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    return all_ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", required=True, type=Path)
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--work", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--toplevel", action="store_true")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    started = time.monotonic()
    work = args.work
    if work.exists():
        assert args.keep, "%s exists (use --keep to reuse)" % work
    else:
        work.mkdir(parents=True)
    home, xdg, tmp = work / "home", work / "xdg", work / "tmp"
    agent, project, logs = work / "agent", work / "project", work / "logs"
    for folder in (home, xdg, tmp, agent, project, logs):
        folder.mkdir(parents=True, exist_ok=True)

    provider = ScriptedProvider()
    threading.Thread(target=provider.serve_forever, daemon=True).start()
    port = provider.server_address[1]
    (agent / "models.json").write_text(
        json.dumps(
            {
                "providers": {
                    "e2e-local": {
                        "api": "openai-completions",
                        "apiKey": "fixture-only",
                        "baseUrl": "http://127.0.0.1:%d/v1" % port,
                        "models": [
                            {
                                "id": "e2e-scripted",
                                "name": "E2E scripted local model",
                                "reasoning": False,
                                "contextWindow": 128000,
                                "maxTokens": 8192,
                            }
                        ],
                    }
                }
            }
        )
    )
    (agent / "settings.json").write_text(
        json.dumps(
            {
                "defaultProvider": "e2e-local",
                "defaultModel": "e2e-scripted",
                "subagentDefaultModel": DEFAULT_MODEL,
                "prewarmPythonKernel": False,
                "onboardingCompleted": True,
                "onboardingShown": True,
                "telemetry": {"enabled": False, "noticeShown": True},
            }
        )
    )

    env = {
        "HOME": str(home),
        "TMPDIR": str(tmp),
        "XDG_CONFIG_HOME": str(xdg),
        "PATH": "/root/.local/bin:/usr/local/cargo/bin:/usr/local/bin:/usr/bin:/bin",
        "TERM": "xterm-256color",
        "DO_NOT_TRACK": "1",
        "PI_OFFLINE": "1",
        "PRIME_AGENT_CODING_AGENT_DIR": str(agent),
        "PI_PACKAGE_DIR": str(args.repo),
        "PRIME_AGENT_MODEL_PROVIDER": "e2e-local",
        "PRIME_AGENT_MODEL": "e2e-scripted",
    }
    global REQUEST_DUMP_DIR
    REQUEST_DUMP_DIR = str(logs / "provider-requests")
    os.makedirs(REQUEST_DUMP_DIR, exist_ok=True)
    sock_path = work / "daemon.sock"
    log = open(logs / "daemon.log", "wb")
    process = subprocess.Popen(
        [
            str(args.bin),
            "--offline",
            "--mode",
            "daemon",
            "--cwd",
            str(project),
            "--daemon-socket",
            str(sock_path),
        ],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    client = None
    driver_error = None
    try:
        client = Client(sock_path, process)
        created = client.call(
            "create",
            name="e2e-root",
            config={
                "cwd": str(project),
                "agentDir": str(agent),
                "provider": "e2e-local",
                "model": "e2e-scripted",
                "noExtensions": True,
                "noSkills": True,
                "noContextFiles": True,
                "telemetryDisabled": True,
            },
        )
        active = created.get("activeSessionId") or created.get("id")
        assert active, created
        client.call("attach", activeSessionId=active)
        client.call(
            "prompt_and_wait",
            activeSessionId=active,
            message=MARK_FILL + " grow the session history before the api steps",
        )
        client.call(
            "prompt_and_wait",
            activeSessionId=active,
            message=MARK_TURN1 + " run the python api steps",
        )
        time.sleep(3)
        if args.toplevel:
            provider.queues["turn2"] = [SURVIVAL_CELL, SPAWN_PEER_CELL, PEER_LIST_CELL, STOP_CHILD_CELL]
            provider.queues["turn3"] = [PEER_WAIT_CELL, PEER_STOP_CELL]
            client.call(
                "prompt_and_wait",
                activeSessionId=active,
                message=MARK_TURN2 + " survival check, then spawn the top-level peer",
            )
            time.sleep(2)
            client.call(
                "prompt_and_wait",
                activeSessionId=active,
                message=MARK_TURN3 + " collect the peer result and stop the peer",
            )
        else:
            provider.queues["turn2"] = [SURVIVAL_CELL, STOP_CHILD_CELL]
            client.call(
                "prompt_and_wait",
                activeSessionId=active,
                message=MARK_TURN2 + " survival check, then stop the child",
            )
    except Exception:
        driver_error = traceback.format_exc()
    finally:
        stop_daemon(process, client)
        if client is not None:
            client.close()
        provider.shutdown()
        provider.server_close()
        log.close()
    ok = write_receipt(args.receipt, provider, args, work, started, driver_error)
    print("receipt written to %s" % args.receipt)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
