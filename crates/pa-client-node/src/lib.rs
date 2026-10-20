//! Prime Agent's Node-API binding crate: the native addon Node 22 and
//! Electron 44 hosts load to talk to the daemon through the shared
//! `pa-client` transport.
//!
//! Scope rules (the review's hard constraints):
//!
//! - **Runtime locality:** every core future is bridged onto this crate's
//!   shared tokio runtime ([`runtime::bridge`]); the napi async fns only
//!   await the bridge's join handle. `pa-client`'s internal reader/writer
//!   task spawns therefore always land on the binding-owned reactor, never
//!   on napi's private `tokio_rt`.
//! - **Frame delivery is pull-based and fail-closed:** one bounded queue
//!   (1024 frames by default, a COUNT cap — with the 64 MiB opt-in frame
//!   cap the worst-case queue memory is `frameBuffer * maxFrameBytes`), a
//!   single-consumer `nextFrame`, and a terminal `Overflow` error when the
//!   queue fills — frames are never dropped or coalesced. The core's tap
//!   channel is unbounded by design (see `pa-client`): the pump draining
//!   promptly is the only backpressure transfer point, and no hard bound on
//!   the transient core-side backlog is claimed.
//! - **Ordering:** tap frames arrive in per-socket wire order; `nextFrame`
//!   yields frames interleaved across sockets with NO cross-socket ordering
//!   claimed (the 3c1 tap contract carries over verbatim).
//! - **Errors are structural:** the binding preserves `pa-client`'s
//!   `NotSent`/`Uncertain`/`Rejected` classes through a JSON payload the
//!   TypeScript wrapper decodes into typed errors; in-flight requests torn
//!   down by overflow settle as `Uncertain` with cause `overflow` (never
//!   re-classified as the channel's `Overflow`), new requests after
//!   overflow fail as `NotSent` with the overflow cause surfaced.
//! - **Interim teardown honesty (pre shutdown-PR):** explicit `close()`
//!   stops the binding's pumps, wakes every pending pull, severs the JS
//!   queues, and calls core `close()`. Synchronous joining of the core's
//!   reader/writer tasks is NOT claimed here — that is the shutdown-core
//!   work this binding will call once it lands, and the crate is explicitly
//!   marked non-mergeable until then. Dropping the JS handle without
//!   `close()` leaks the connection (runtime threads + core tasks persist
//!   until process exit); the napi finalizer performs no core teardown.
//!
//! See `README.md` for the crate's scope, non-goals, and public API.

mod client;
mod errors;
mod meta;
mod runtime;

pub use client::{connect, Caps, ConnectOptions, PrimeDaemonClient};
pub use meta::client_metadata;
