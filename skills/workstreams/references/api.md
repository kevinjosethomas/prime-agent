# Python workstream API

All state operations are async and work inside `python_repl`. `configure` is local
setup supplied by Desktop, not something to rebuild each turn. The package is part
of `prime-agent-runtime`, rather than a separately downloaded dependency. Importing
it performs no I/O and requires only the Python standard library.

## Read and manage outcomes

```python
items = await workstreams.list(status="active")  # immutable tuple of Workstream
task = await workstreams.get("prime-cli-json")    # KeyError if missing
task = await workstreams.update("prime-cli-json", summary="Parser verified")
task = await task.refresh()
```

`create(id, title, **fields)` refuses an already observed ID. `upsert(id, title,
**fields)` replaces complete state; omitted fields take defaults. Prefer `update`
to change selected fields. Supported fields:

| Field | Meaning |
| --- | --- |
| `status` | `todo` (default), `active`, `needs_input`, `ready`, `backlog`, `done` |
| `summary` | Latest meaningful result or next action, at most 2,000 characters |
| `description` | Outcome/scope context for routing, at most 6,000 characters |
| `owner` | Human-readable owner/agent, default `/root` |
| `question` | Actual unresolved question; mandatory for `needs_input` |
| `steps` | Up to 100 `Step(id, title, done=False)` values or matching dictionaries |
| `parent_id` | Existing parent workstream ID; `None` removes a parent on update |
| `links` | Up to 20 `Link(kind, url, title=None)` values or matching dictionaries |

Titles are at most 160 characters. IDs remain stable and are at most 512 characters.
Links accept `chat`, `pr`, or `source`; URLs must be HTTP(S) without embedded credentials.
Desktop enforces parent existence and rejects cycles. Timestamps come from the host.
Handle fields are snapshots: operations return new handles and fetch current state
before partial writes. Independent agents' full-state updates are last-writer-wins;
this version does not offer compare-and-swap or transactional multi-workstream edits.
The duplicate check in `create` is not an atomic create-if-absent across processes.

```python
case = await workstreams.create("o1", "Prepare O1 case", status="active")
intake = await case.subtask("o1-intake", "Complete Lighthouse O1 intake")
intake = await intake.update(links=[
    Link("source", "https://example.com/intake", "Intake questionnaire"),
])
children = await workstreams.list(parent_id="o1")
await intake.associate(actual_session_id, name="intake-worker", role="researcher")
```

`associate` stores an actual existing session ID; it does not independently verify
daemon availability or agent ownership. Discover the ID through supported RLM/roster
APIs, not by guessing from a chat title. `context()` returns the metadata snapshot,
root binding, associated agents, and the last 30 indexed turns' IDs; it contains no
message bodies. It is not an API for reading a session's conversation.

## Native RLM assignment from any agent

The API is available to every agent whose kernel has the trusted local reporter
configuration. It is not restricted to `/root`. Create/update workstreams normally,
then spawn and assign native children without a second agent engine:

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
trusted bootstrap instructions that configure `import workstreams` inside the child
kernel and load the same stable workstream ID. Children using `task.spawn` pass this
setup onward to their children. Model selection, task execution, resource budgets,
and agent lifecycles remain owned by RLM. Omit `model`/`thinking` to retain RLM defaults.
`collect` returns snapshots: a timeout can leave a child running. Inspect its actual
status before claiming completion. The native runtime owns collection and lifecycle.

Assign an already observed child with `await task.assign(handle, role="reviewer")`,
or `await workstreams.assign(task.id, roster_row)`. The input must be an actual
`rlm.RLMSpawnHandle` or `rlm.RLMSubagent`, not a guessed dictionary. A queued spawn
handle supplies `rlm_child_id` and `session_dir`, but no session ID. The metadata
records that real scoped identity without inventing a clickable chat. Once a roster
row has a real `session_id`, `assign(row)` adds it. `sync_agents()` reads the direct-child
roster once and updates only children already assigned to this workstream, matching
both child ID and session directory. It does not poll, sleep, or claim to see other
agents' private descendant rosters. An owner can also assign a supplied verified row.

If spawn succeeds but recording the assignment fails, `AssignmentError.handle`
retains the admitted child. **Do not spawn again** to repair metadata:

```python
try:
    child = await task.spawn("Verify parser", name="parser-reviewer")
except workstreams.AssignmentError as error:
    child = error.handle
    if error.report is not None:
        await workstreams.retry(error.report)
    else:
        await task.assign(child)
```

For an existing independently created local kernel, the parent can share
`client.configuration()` and that agent can call `workstreams.configure(**configuration)`
once. The returned configuration is a fresh copy of reporter argv, environment and
timeout. It contains trusted local paths/setup, not credentials: do not put secrets
in reporter argv/environment or source this configuration from incoming task data.
No global files, startup services, permissions or network endpoints are created.

## Milestones and status

```python
task = await task.milestone("test", "Verify parser on malformed records")
task = await task.check("test")                # unknown ID raises KeyError
task.progress                                 # (completed, total), or None
task = await task.needs_input("May I merge the verified change?")
task = await task.update(status="ready", summary="Verified change ready for review")
task = await task.complete()                   # rejects unfinished steps
```

Moving out of `needs_input` clears its old question unless a replacement question is
explicitly supplied. `complete` does not check all steps for you. Unknown progress
stays unknown; parent progress is not calculated from guessed subtask weights.

## Corrections and failure recovery

Clef automatically labels user/assistant exchanges using workstream context and
recent conversation. A missing/unavailable classifier must remain visible as an
unclassified state; this package does not quietly call a larger model instead.
Only when the user explicitly requests a correction, pass indexed turn IDs from `context()`:

```python
await workstreams.classify([actual_turn_id], ["o1", "travel"])
```

These corrections are recorded with source `manual`, replacing earlier labels while
remaining protected from later automatic classification. Do not use this API for
routine message labeling or an unsolicited guess.

Each report gets a unique UUID event ID. If delivery fails or times out, its commit
may be uncertain. Retry the exact event, not a newly generated equivalent update:

```python
try:
    task = await task.update(summary="Export verified")
except workstreams.DeliveryError as error:
    if error.report is not None:
        await workstreams.retry(error.report)
    else:
        raise  # Reading state failed; diagnose the reporter connection.
```

Desktop deduplicates repeated event IDs. Errors do not echo subprocess stderr or
configuration values. A successful report followed by a failed refresh can mean
the write committed: read current state before deciding to issue another change.

## Host and hook setup

Desktop supplies trusted absolute paths once in `/root`'s Python kernel:

```python
workstreams.configure(
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
