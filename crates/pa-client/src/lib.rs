//! The shared daemon transport for frontends that attach to a daemon over
//! the wire: the JSONL supervisor client (`daemon_client`) and the
//! capability-gated direct worker link that upgrades it
//! (`direct_transport`).
//!
//! This crate owns the client half of the wire protocol — connect and
//! `daemon_hello` handshake, command envelopes matched by envelope id, and
//! the direct-link ticket/auth exchange — and no wire shapes or behavior of
//! its own: every frame is a `pa-types::daemon` type. `pa-tui` consumes it
//! and re-exports `daemon_client` under its own path. The two modules share
//! crate-internal state (`Shared`, `client_event_from_value`, the direct
//! link set) and keep it crate-internal.

pub mod daemon_client;
pub mod direct_transport;
pub mod registry;
