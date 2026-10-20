# Python tasks API

All state operations are async and work inside `python_repl`. The package is
part of `prime-agent-runtime`, not a separately downloaded dependency.
Importing it performs no I/O and requires only the Python standard library.

The daemon provisions the trusted local reporter configuration into every
kernel environment automatically; there is no configuration surface and no
prompt-based setup. If the configuration is absent (for example a kernel
spawned before the desktop pushed it), the first call raises a clear
`RuntimeError`; report it honestly instead of inventing paths.

## Exact public surface

```python
from tasks import Link

items = await tasks.list(status=None, archived=False)  # visible snapshots
archived = await tasks.list(archived=True)              # only archived
all_items = await tasks.list(archived=None)             # visible and archived
task = await tasks.get("prime-cli-json")                # KeyError if missing
task = await tasks.create("prime-cli-json", "Parse CLI logs")        # status todo
task = await tasks.create("born-blocked", "Starts blocked",
                          status="needs_input", question="Is it green?")  # ValueError on a duplicate id
child = await task.subtask("cli-parser", "Structured log parser")   # create under this parent
```

There are no other module functions. `status=None` lists every status;
`archived` is `False` (visible), `True` (archived only) or `None` (all).
`list()` does not filter by parent: filter the returned snapshots client-side
on `task.parent_id`:

```python
children = [t for t in await tasks.list(archived=None) if t.parent_id == parent.id]
```

`create` starts a task at `todo` unless a `status` is given; `question` is
only accepted with `needs_input` (it must be nonempty there and empty
anywhere else). The host rejects an already observed ID atomically (a
client-side duplicate check only gives a friendly error, the store is
authoritative). `task.subtask(...)` is `create` with `parent_id=task.id`.

## Task handle methods

```python
task = await task.update(status="active",
                         links=[Link("pr", "https://github.com/org/repo/pull/12")])
task = await task.archive()
task = await task.restore()
await task.link(agent)  # rlm.spawn/rlm.create_session handle or list_subagents row
```

- `update` patches only the named fields (`status`, `question`, `links`); the
  host preserves every unrelated field in one transaction, so concurrent
  partial updates from different agents do not clobber each other. Omitting a
  field leaves it unchanged; `links` replaces the whole tuple.
- `archive`/`restore` use the host's `set-archived` operation, preserving
  status and timestamps. `get(id)` can always read an archived entry; the
  `archived` snapshot field defaults to `False` when an older host omits it.
- `link` records an actual observed agent identity: an `rlm.create_session`
  handle (its durable session id), an `rlm.spawn` handle (child id + session
  directory, no session id yet), or an `rlm.list_subagents()` row (which
  carries the durable session id once observed). Linking records metadata
  only; it never spawns, messages, or grants access.

Snapshots are immutable; every operation returns a fresh snapshot of current
state. The public model has exactly id, title, status, question, parent_id,
links and archived (plus updated_at).

## Statuses and the question

`todo`, `active`, `needs_input`, `ready`, `backlog`, `done`.

`question` exists exactly while the status is `needs_input`: required and
nonempty there, empty in every other status. The host enforces the invariant
on the merged state and clears the question when the status moves away; the
package applies the same rules before delivering:

```python
task = await task.update(status="needs_input", question="May I merge the verified change?")
task = await task.update(status="active")        # the host clears the question
with self.assertRaises(ValueError):
    await task.update(question="not while active")   # only needs_input carries a question
```

There is no progress percentage, checklist, summary or description; split
distinct work into subtasks and give each its own status.

## Field limits

| Field | Rule |
| --- | --- |
| `id` | Stable, at most 512 characters |
| `title` | At most 160 characters |
| `status` | One of the six statuses |
| `question` | At most 2,000 characters; nonempty iff needs_input |

Links: up to 20 per task, kinds `chat`, `pr`, `source`, `commit`; URLs must be
HTTP(S) without embedded credentials. A `commit` link is the commit's web URL,
never a bare SHA: `Link("commit", "https://github.com/org/repo/commit/<sha>",
"Short title")`. Desktop enforces parent existence and rejects cycles.
Timestamps (`updated_at`) are host-owned. Independent agents' writes are
last-writer-wins per field set; the create duplicate check is authoritative
in the store, not merely client-side.

## Failure behavior

Every report gets a unique event ID and the package redelivers the exact same
event after an uncertain delivery (timeout or unknown exit), so a redelivery
is a duplicate in the store, never a repeated change. Delivery failures raise
`tasks`'s internal delivery error and never echo subprocess stderr or
configuration values. A successful report followed by a failed refresh can
mean the write committed: read current state before deciding to issue
another change. Reports are bounded (256 KiB) and the reporter process never
runs through a shell.

The configuration is trusted only because the daemon provisioned it; the
package never derives a reporter command from prompt or task content, and a
remote/cloud kernel cannot reach a local desktop process without a separate,
explicitly approved bridge.
