# pa-client-node

Node-API (napi-rs) binding crate for the shared Prime Agent daemon client
(`pa-client`): the native addon that Node 22 / Electron 44 hosts load to
delegate connect/request/direct/error/raw-frame channels without any
TypeScript transport machinery.

**Status: spike.** This crate currently only verifies the platform approach
(isolated workspace, napi-rs pin, tokio_rt async fns, ThreadsafeFunction
cleanup, natural Node exit). The real adapter API lands with the same crate.

## Why an isolated workspace

The crate is a `cdylib` excluded from the root workspace (root `Cargo.toml`
`exclude`): a unit-test binary would link a N-API-dependent executable
(unresolved `napi_*` symbols on Linux). Behavior tests run against the real
addon in Node/Electron; the Rust side is gated by `cargo clippy -D warnings`
(no link). The crate carries its own committed `Cargo.lock`. Path deps into
`../pa-client`/`../pa-types` resolve their workspace-inherited dependencies
from the root `Cargo.toml` unchanged.

## Non-goals

- No transport, retry, cursor, or reconnect machinery (owned by `pa-client`;
  the managed driver arrives with the shared-lifecycle work, exposed after).
- No hand-maintained TypeScript wire mirror: the boundary speaks
  `DaemonCommand`/`DaemonResponse` serde; TS types come from the generated
  client contract package.
- No per-Electron rebuild: pure Node-API, one `.node` per platform.

## Build

`cargo build --release -p pa-client-node` (from this directory's workspace)
emits the addon; `spike/spike.mjs` is the Node smoke script.
