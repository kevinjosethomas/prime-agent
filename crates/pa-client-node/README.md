# pa-client-node

Node-API (napi-rs) binding crate for the shared Prime Agent daemon client
(`pa-client`): the native addon that Node 22 / Electron 44 hosts load to
delegate connect/request/direct/error/raw-frame channels without any
TypeScript transport machinery.

**Merge status: CANNOT MERGE until the core shutdown PR lands.** The
binding explicitly does not join the core transport's reader/writer tasks;
synchronous teardown is the shutdown-core work, and this crate wires that
call in when it exists. Until then the crate is implementation-complete
with the interim semantics below, honestly labeled.

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

## Interim teardown semantics (pre shutdown-PR)

`close()` stops the binding's pump, wakes every pending pull (`nextFrame`
then drains already-queued frames and returns `null`; the sticky overflow
error wins if overflow happened), severs the JS queues, and calls core
`close()`. Synchronous joining of the core's internal reader/writer tasks is
NOT claimed — that is the shutdown-core work this binding will call once it
lands. Dropping the JS handle without `close()` leaks the connection (the
runtime threads and core tasks persist until process exit); the napi
finalizer performs no core teardown from a GC thread.

## Non-goals

- No transport, retry, cursor, or reconnect machinery (owned by `pa-client`;
  the managed driver is exposed after the shared-lifecycle work).
- No hand-maintained TypeScript wire mirror; no per-Electron rebuild (pure
  Node-API, one `.node` per platform).

## Build

`cargo build --release -p pa-client-node` (from this directory's workspace)
emits the addon; `packages/client` owns the loader, provenance verification,
and the Node test battery.
