# Python tasks API

All state operations are async and work inside `python_repl`. `configure` is local
setup supplied by Desktop, not something to rebuild each turn. The package is part
of `prime-agent-runtime`, rather than a separately downloaded dependency. Importing
it performs no I/O and requires only the Python standard library.

## Read and manage tasks

```python
items = await tasks.list(status="active")   # visible immutable snapshots
archived = await tasks.list(archived=True)  # only archived tasks
all_items = await tasks.list(archived=None) # include visible and archived
task = await tasks.get("prime-cli-json")    # KeyError if missing
task = await tasks.update("prime-cli-json", summary="Parser verified")
task = await task.refresh()
```

`create(id, title, **fields)` refuses an already observed ID. `upsert(id, title,
**fields)` replaces editable state; omitted fields take defaults. The host-owned
archive flag is preserved. Prefer `update` to change selected fields; it writes only
`id`, `title`, `status`, `summary`, `description`, `question`, `owner`, `parentId`
and `links`. Host-owned read fields such as `updatedAt`, `archived` and `taskUrl`
cannot be upserted. Supported fields:

| Field | Meaning |
| --- | --- |
| `status` | `todo` (default), `active`, `needs_input`, `ready`, `backlog`, `done` |
| `summary` | Latest meaningful result or next action, at most 2,000 characters |
| `description` | Scope context for routing, at most 6,000 characters |
| `owner` | Human-readable owner/agent, default `/root` |
| `question` | Actual unresolved question; mandatory for `needs_input` |
| `parent_id` | Existing parent task ID (makes this a subtask); `None` removes it on update |
| `links` | Up to 20 `Link(kind, url, title=None)` values or matching dictionaries |

Titles are at most 160 characters. IDs remain stable and are at most 512 characters.
Links accept `chat`, `pr`, `source`, or `commit`; URLs must be HTTP(S) without embedded
credentials. A `commit` link is the commit's web URL, never a bare SHA:
`Link("commit", "https://github.com/org/repo/commit/<sha>", "Short title")`.
Desktop enforces parent existence and rejects cycles. Timestamps come from the host.
Handle fields are snapshots: operations return new handles and fetch current state
before partial writes. Independent agents' full-state updates are last-writer-wins;
this version does not offer compare-and-swap or transactional multi-task edits.
The duplicate check in `create` is not an atomic create-if-absent across processes.

## Tasks and subtasks

A subtask is a task whose `parent_id` points at another task. Reuse the existing
task (for example Primer) and record each distinct request or feedback item from
Kevin as a subtask under it. Check existing subtasks first and do not duplicate one.
Create a new top-level task only for a product or independently owned outcome.
Chats, workers, and PRs do not create new tasks.

```python
primer = await tasks.get("primer")  # use the existing observed stable ID
existing = await tasks.list(parent_id=primer.id, archived=None)
item = next((t for t in existing if t.title == "Drop files into Primer"), None)
if item is None:
    item = await primer.subtask("primer-drop-files", "Drop files into Primer", status="active")
item = await item.update(links=[
    Link("pr", "https://github.com/org/repo/pull/12", "Drop files"),
    Link("commit", "https://github.com/org/repo/commit/3a8a666b5", "Accept dropped files"),
])
await item.associate(actual_session_id, name="drop-files-worker", role="implementer")
```

`task.subtask(id, title, **fields)` is `create(id, title, parent_id=task.id, **fields)`.
`list(parent_id=task.id)` lists its visible subtasks.

Only top-level tasks have threads. Subtasks track status, details, links and agent
associations, not separate conversations. Clicking a subtask focuses its parent's
thread and shows its details in the side panel. Creating or backfilling a subtask
must not assign messages to it.

`associate` stores an actual existing session ID; it does not independently verify
daemon availability or agent ownership. Discover the ID through supported RLM/roster
APIs, not by guessing from a chat title. `context()` returns the metadata snapshot,
root binding, associated agents, and the last 30 indexed turns' IDs; it contains no
message bodies. It is not an API for reading a session's conversation.

## Archive and restore

```python
task = await task.archive()
task = await tasks.restore(task.id)
# Equivalent client forms: await client.archive(id), await client.restore(id).
```

`archive` and `restore` return refreshed immutable handles. They use the host's
`set-archived` operation, preserving status, subtasks and `updated_at`. The snapshot
field `archived` defaults to `False` when an older host omits it. `list()` and
`list(archived=False)` hide archived entries; `archived=True` selects only archived
entries and `archived=None` includes all. Status and parent filters still apply.
`get(id)` can always read an archived entry. Partial updates preserve its host-owned
archive flag; `archived` is not an `upsert` or `update` field. Use these methods instead.

Archive duplicate or dropped tasks rather than deleting their history. Archiving
does not merge tasks, move subtasks, or mark work complete. Move useful subtasks to
the existing task first with `update(parent_id=...)`. Restore an existing archived
task when it resumes instead of creating another task for the new chat.

## Independent work agents for the orchestrator

Agent One starts independent work agents instead of owning ordinary RLM children:

```python
worker = await task.start_agent(
    "Own structured CLI parsing. Verify the result and report only a meaningful outcome or blocker.",
    name="parser-owner", model="provider/model", thinking="low", cwd="/verified/project",
)
```

Signature: `start_agent(prompt, *, name, model=None, thinking=None, cwd=None,
role="worker-parent")`. Client/module forms take `task_id` first. The wrapper
calls existing `rlm.create_session`, which requires a daemon-backed depth-0 caller.
It returns the original `rlm.RLMCreateSessionHandle` with `active_session_id`,
`session_id`, `name`, `session_file` and `model`. These are actual native identities;
no child ID or guessed session ID is invented. The independent resident has its own
durable native session and may use `task.spawn` for its own RLM children.

The bootstrap configures the same trusted reporter and existing task ID inside
the new agent. When `context().root.sessionId` is recorded, it instructs that agent
to update task state and send one concise meaningful result, blocking question,
or failure through typed `agent_message.send(..., receiver_role="sibling",
receiver_name=actual_orchestrator_id)`. Routine progress stays in metadata. If no
verified orchestrator ID exists, it omits the callback rather than guessing a recipient.
The wrapper itself does not send a message on the worker's behalf.

Independent agents are siblings. They do not appear in the creator's
`rlm.list_subagents`, cannot be passed to `rlm.collect`, and are not refreshed by
`task.sync_agents`. Use the existing native sibling observation/messaging APIs:

```python
import agent_observe, agent_message
status = await agent_observe.get_agent(worker.session_id)
# For an authorized follow-up to the same actual worker:
await agent_message.send("Continue with the verified approach.",
                         receiver_role="sibling", receiver_name=worker.session_id)
```

An idle agent is not proof that its task succeeded. Require actual result evidence
and reported state. There is no new completion watcher or automatic terminal notice
for these independent roots. A crashed worker cannot self-report, and native sibling
messaging currently requires an available live family peer. Do not promise delivery
or immediate recovery across daemon/worker replacement; preserve durable state and
surface an unavailable recipient honestly.

If creation succeeds but association fails, `AssignmentError.handle` retains the
actual independent session. Retry its exact `report` when present; otherwise call
`await task.assign(error.handle, role="worker-parent")`. Never create another agent
just to repair metadata. `assign` accepts `RLMCreateSessionHandle` in addition to
the RLM child handles described below.

This orchestrator/worker separation is **role guidance only**. It does not hard-block
`rlm.spawn` for Agent One, change depth/capacity limits, or add a new protocol. A future
per-session capability could enforce that distinction. Setting global or inherited
RLM depth to zero is not an equivalent solution: native independent sessions inherit
that limit and would lose their own child-spawning capability too. A newly added
Python helper is available only after its runtime package is actually deployed;
existing native `rlm.create_session` plus `task.associate(handle.session_id, ...)`
can be used when an older installed package lacks `start_agent`.

## Native RLM children owned by work agents

The state API is available to every authorized agent whose kernel has the trusted
local reporter configuration. Work agents create/update tasks normally, then
spawn and assign their own native children without a second agent engine:

```python
child = await task.spawn(
    "Implement structured log parsing and verify malformed records.",
    name="parser-worker", model="provider/model", thinking="low", role="implementer",
)
# child is the original rlm.RLMSpawnHandle, suitable for native collect/observation.
results = await rlm.collect(child, timeout_ms=30_000)
await task.sync_agents()
```

`spawn` delegates to `rlm.spawn(prompt, name=..., model=..., thinking=...)`. It adds
trusted bootstrap instructions that configure `import tasks` inside the child
kernel and load the same stable task ID. Children using `task.spawn` pass this
setup onward to their children. Model selection, task execution, resource budgets,
and agent lifecycles remain owned by RLM. Omit `model`/`thinking` to retain RLM defaults.
`collect` returns snapshots: a timeout can leave a child running. Inspect its actual
status before claiming completion. The native runtime owns collection and lifecycle.

Assign an already observed child with `await task.assign(handle, role="reviewer")`,
or `await tasks.assign(task.id, roster_row)`. The input must be an actual
`rlm.RLMSpawnHandle` or `rlm.RLMSubagent`, not a guessed dictionary. A queued spawn
handle supplies `rlm_child_id` and `session_dir`, but no session ID. The metadata
records that real scoped identity without inventing a clickable chat. Once a roster
row has a real `session_id`, `assign(row)` adds it. `sync_agents()` reads the direct-child
roster once and updates only children already assigned to this task, matching
both child ID and session directory. It does not poll, sleep, or claim to see other
agents' private descendant rosters. An owner can also assign a supplied verified row.

If spawn succeeds but recording the assignment fails, `AssignmentError.handle`
retains the admitted child. **Do not spawn again** to repair metadata:

```python
try:
    child = await task.spawn("Verify parser", name="parser-reviewer")
except tasks.AssignmentError as error:
    child = error.handle
    if error.report is not None:
        await tasks.retry(error.report)
    else:
        await task.assign(child)
```

For an existing independently created local kernel, the parent can share
`client.configuration()` and that agent can call `tasks.configure(**configuration)`
once. The returned configuration is a fresh copy of reporter argv, environment and
timeout. It contains trusted local paths/setup, not credentials: do not put secrets
in reporter argv/environment or source this configuration from incoming task data.
No global files, startup services, permissions or network endpoints are created.

## Status

```python
task = await task.needs_input("May I merge the verified change?")
task = await task.update(status="ready", summary="Verified change ready for review")
task = await task.complete(summary="Merged")  # sets status done
```

Moving out of `needs_input` clears its old question unless a replacement question is
explicitly supplied. There is no progress percentage or checklist; split distinct
work into subtasks and give each its own status.

## Message assignment and failure recovery

Clef places messages automatically. A message moves only by Kevin's choice: a
focused-thread send in Primer, or a move he explicitly asks for. To record such a
move, pass whole message IDs (from `context()`) and the top-level task, or `None`
for no task. Clef, focused sends and right-click attachment all use top-level tasks:

```python
await tasks.assign_messages([actual_message_id], "prime-root-orchestrator")
await tasks.assign_messages([greeting_id], None)
```

`assign_messages(message_ids, task_id)` takes 1–200 message IDs and records a manual
placement that Clef never overrides. Do not use it to classify messages yourself,
and do not guess IDs. A subtask ID is not a thread destination. In the desktop
data model, `message_tasks.task_id` points only at a top-level task (or is NULL).
Existing subtask placements roll up to their top-level parent in a one-time
migration, keeping their original source.

Each report gets a unique UUID event ID. If delivery fails or times out, its commit
may be uncertain. Retry the exact event, not a newly generated equivalent update:

```python
try:
    task = await task.update(summary="Export verified")
except tasks.DeliveryError as error:
    if error.report is not None:
        await tasks.retry(error.report)
    else:
        raise  # Reading state failed; diagnose the reporter connection.
```

Desktop deduplicates repeated event IDs. Errors do not echo subprocess stderr or
configuration values. A successful report followed by a failed refresh can mean
the write committed: read current state before deciding to issue another change.

## Host and hook setup

Desktop supplies trusted absolute paths once in `/root`'s Python kernel:

```python
tasks.configure(
    command=[electron_path, reporter_path, "--db", desktop_database_path],
    env={"ELECTRON_RUN_AS_NODE": "1"},
    timeout=15,
)
```

This stores an argv vector; it never executes a shell. The adapter appends `context`
or `report` and sends report JSON on stdin. There is no network transport or token
handling. An independent hook can use `Client(command=..., env=...)` without changing
the module's default client. Share only this reporter configuration with authorized
local agents; a remote/cloud kernel cannot reach a local desktop process without a
separate, explicitly approved bridge. Restarted or replaced Python kernels need
configuration again. Durable state remains owned by Desktop's SQLite store.
