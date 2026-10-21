# pa-client-node

Node-API (napi-rs) binding crate for the shared Prime Agent daemon client
(`pa-client`): the native addon that Node 22 / Electron 44 hosts load to
delegate connect/request/direct/error/raw-frame channels without any
TypeScript transport machinery.

## Why an isolated workspace

The crate is a `cdylib` excluded from the root workspace (root `Cargo.toml`
`exclude`): a unit-test binary would link a N-API-dependent executable
(unresolved `napi_*` symbols on Linux). Behavior tests run against the real
addon in Node/Electron; the Rust side is gated by `cargo clippy -D warnings`
(no link). The crate carries its own committed `Cargo.lock`. Path deps into
`../pa-client`/`../pa-types` resolve their workspace-inherited dependencies
from the root `Cargo.toml` unchanged.

## Public API (see `packages/client` for the TypeScript wrapper)

- `clientMetadata()` — derived build metadata: crate version, daemon protocol
  name+version (7), schema id + revision (`DAEMON_SCHEMA_ID`), harness
  commit, target triple, os, arch.
- `connect(socketPath, options?)` — hello handshake on the binding-owned
  tokio runtime. Options: `frameBuffer` (bounded frame-queue capacity,
  default 1024, a COUNT cap — with the 64 MiB opt-in frame cap the
  worst-case queue memory is `frameBuffer * maxFrameBytes`), `maxFrameBytes`
  and `maxPending` (the `pa-client` transport caps). Zero or absurd caps are
  rejected at construction.
- `nextFrame()` — pull one raw application frame (the lossless 3c1 raw tap:
  every decoded frame after the handshake, verbatim, unknown frame types and
  the full event meta included, in per-socket wire order; NO cross-socket
  ordering claimed). Single-consumer: a concurrent pull rejects. Returns
  `null` at end-of-stream.
- `nextStatus()` — `open` initially, then changes only.
- `request`/`requestOk(command)` — serde `DaemonCommand`/`DaemonResponse`
  at the boundary (never a hand-maintained TS mirror; wire types come from
  the generated client contract package). No auto-resend, ever.
- `upgradeDirect`/`dropDirect`/`directSessionId`, `hello`, `protocol`,
  `supportsServerCapability`, `socketPath`, `caps`, `currentStatus`, `close`.

## Frame delivery: pull-based, fail-closed

ONE bounded queue (default 1024 frames) between the core tap and the JS
pull. The pump drains the core's (unbounded, by design — see `pa-client`)
tap channel promptly and is the queue's single producer. When the bounded
queue is full the binding does not drop and continue: the overflow tears
the connection down and the frame stream becomes terminal — every pending
and later `nextFrame`/`nextStatus` observes the sticky `Overflow` error, so
the consumer never continues on a partial stream. Frames are never dropped,
coalesced, or silently reordered.

The bounded queue bounds the JS-side delivery backlog only. It does NOT
bound the transient core-side channel backlog (that channel is unbounded by
design); the pump's prompt drain is the only backpressure transfer point.

## Error contract (structural, not textual)

The binding preserves `pa-client`'s classes through a JSON payload the
TypeScript wrapper decodes:

- `NotSent` — provable non-delivery (dead reader, writer exit, pending-cap
  refusal, command deserialize failure, post-teardown sends with the
  overflow cause surfaced).
- `Uncertain` — the line was queued and no answer arrived (timeouts, socket
  loss, teardown of in-flight requests with cause `overflow` or `closed`).
  Never resent.
- `Rejected` — the daemon's refusal, `errorInfo` preserved.
- `Overflow` — the terminal frame-channel error after queue overflow (a
  channel state, never a request class: in-flight requests stay `Uncertain`).
- `Transport` — anything else.

## Teardown (the landed shutdown-core work, wired in)

`close()` marks the connection closed and refuses new operations, wakes
every pending pull (`nextFrame` drains already-queued frames and returns
`null`; the sticky overflow error wins if overflow happened), severs the JS
queues, shuts the core transport down, drains the binding-owned operation
bridges, and then joins the core's registered task set (`shutdown()` +
`join_tasks()`) to a real completion. Dropping the JS handle without
`close()` schedules the SAME full close chain on the binding-owned runtime
from the napi finalizer (async-safe; no core calls from a GC thread) —
nothing leaks past a GC; with no JS handle outstanding the shared runtime
parks and Node exits on its own.

## Managed session (`startSession`)

The same binding also exposes the shared Rust managed lifecycle
(`pa-client` `lifecycle.rs` + `registry.rs`) as `startSession(options) ->
ManagedDaemonSession`, so hosts delete their TypeScript
reconnect/restore-attach/dedupe state machines instead of re-implementing
them:

- `options`: `socketPath`; `eventBuffer?` (bounded event queue, default
  1024); `reconnect?` (omitted = the existing Rust desktop policy; explicit
  `{ update?: Window|null, shutdown?: Window|null, lost: Window }` where a
  null/omitted slot falls back to the `lost` window and an omitted
  `windowMs` is unbounded); `attachCapabilities?` (default
  `["attach_snapshot", "event_sequence"]`); `dedupeSessionEvents?` (default
  true); `restoreAttemptTimeoutMs` (REQUIRED, nonzero — the desktop's own
  attach rides its 30 s request class); `maxFrameBytes?` (default 64 MiB),
  `maxPending?` (default 128); `clientId?` (default keeps the core's
  `daemon-tui:{pid}` identity — a desktop host passes its own unique one).
- `status()` — the core's synchronous snapshot
  `{ state, socketPath, appVersion, capabilities, error, errorCode }`;
  `errorCode` is the STRUCTURAL `io::ErrorKind` label of the last connect
  failure (`not_found`, `connection_refused`, ...; `None` on every other
  status) — never message-parsed.
- `nextEvent()` — single-consumer pull of the serialized stream:
  `{ type: 'status', status }`, `{ type: 'frame', frame }` (the same
  verbatim raw tap frames `nextFrame` yields, full meta and unknown types
  included; per-socket wire order), `{ type: 'reattachBegin' |
  'reattached' | 'reattachFailed', activeSessionId, data?/error }`, and
  `{ type: 'reconnectExpired', kind }` (the ACTUAL closing kind). Fail-closed
  like `nextFrame`: one bounded queue, and a full queue tears the whole
  session down — the sticky `Overflow` error precedes any buffered event. An
  explicit `stop()` (or a GC drop) marks teardown, wakes every pending
  pull, drains the already-queued events, and then resolves `null`;
  `stop()` itself settles independently of the consumer draining.
- `request`/`requestOk(command)` — serde boundary, generated contract types,
  class-based timeouts (30 s default / 600 s `prompt_and_wait`/
  `wait_for_idle`), the honest `NotSent`/`Uncertain`/`Rejected` classes with
  the raw binding's worker mechanics (pre-poll barrier; biased core-first
  select), no auto-resend.
- `registerAttachment(id, capabilities?)` / `forgetAttachment(id)` — the
  core registry (idempotent per id). Attach/reattach/detach/kill responses
  adopt/drop registry entries automatically; normal flows never need these.
- `stop()` — awaited, idempotent: the core stop (refuse requests, epoch
  bump, client shutdown + joins, the driver's final join), then the
  binding-owned operation drain. The driver's honest completion surfaces
  (`Err` on a driver failure for every caller), never faked.

## Non-goals

- No transport, retry, cursor, or reconnect machinery (owned by `pa-client`;
  the managed lifecycle is exposed through `startSession` and the wrapper
  adds only serialization, the JS event queue, and teardown scheduling).
- No hand-maintained TypeScript wire mirror; no per-Electron rebuild (pure
  Node-API, one `.node` per platform).

## Build

`cargo build --release -p pa-client-node` (from this directory's workspace)
emits the addon; `packages/client` owns the loader, provenance verification,
and the Node test battery.
