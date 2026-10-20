# pa-client

The shared daemon transport.

## Scope
The client half of the daemon wire protocol, extracted from `pa-tui`'s transport: the JSONL supervisor client (`daemon_client`: connect and `daemon_hello` handshake, `type: "command"` envelopes matched by envelope id, non-response frames forwarded as events; bounded connect/handshake retries; `request`/`request_ok`/`request_ok_via_supervisor` with the TS default timeout classes; the `daemon_closing` update-resume contract; typed refusal classification — `is_daemon_rejection`, `is_daemon_timeout`, `is_daemon_unreachable`, `is_kernel_not_running`, `is_update_restarting_rejection`, `rejected_provider_unauthenticated`, `RequestRejected` — for the callers' error paths) and the direct worker link (`direct_transport`: the capability-gated direct-attach ticket path — `GetDirectWorkerTransport` ticket validation, `peer_auth` handshake, the live link and its reader/writer pumps; every failure degrades silently to supervisor routing). The two modules share crate-internal state (`Shared`, `client_event_from_value`, the direct link set) and keep it crate-internal.

## Non-goals
No wire vocabulary of its own: every frame is a `pa-types::daemon` type, and the crate depends on `pa-types` only. No engine, session, settings, auth, model discovery, or update logic; no process supervision — clients of this crate connect to a running daemon, never spawn one. No UI: rendering, keybinding, and composition concerns stay in `pa-tui`/`pa-cli`. `pa-cli`'s own daemon client (its startup, discovery, and behavioral wiring) is a separate, not-yet-converged copy; adopting this crate there is future thin-client work, out of scope here.

## Public API
`daemon_client::{DaemonClient, DaemonClientEvent, DaemonClosingUpdate, DEFAULT_REQUEST_TIMEOUT_MS, LONG_RUNNING_REQUEST_TIMEOUT_MS, is_daemon_rejection, is_daemon_timeout, is_daemon_unreachable, is_kernel_not_running, is_update_restarting_rejection, rejected_provider_unauthenticated, RequestRejected}` — the attach/request surface pa-tui consumes and re-exports under `pa_tui::daemon_client`. The direct-transport half (`DirectLink`, `DirectState`, `connect_direct`, ticket validation, the capability constant) is crate-internal by design: only `DaemonClient`'s `upgrade_direct`/`drop_direct`/`direct_session_id`/`close`/`hard_close` methods expose it.

## Depends on
`pa-types` only (plus anyhow/serde_json/tokio for the socket plumbing).

## Ownership

The daemon transport is owned by this crate; `pa-tui` re-exports `daemon_client` so its existing `crate::daemon_client` callers and `pa_tui::daemon_client` users keep their paths. The extraction from `pa-tui` is a move with no wire or behavior change.
