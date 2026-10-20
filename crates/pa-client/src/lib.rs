//! The shared daemon transport every Prime Agent frontend speaks: the
//! JSONL supervisor client (`daemon_client`) and the capability-gated
//! direct worker link that upgrades it (`direct_transport`).
//!
//! Extraction note: both halves moved here verbatim from `pa-tui`; the move
//! owns the transport but changes no wire behavior. The two modules share
//! crate-internal state (`Shared`, `client_event_from_value`, the direct
//! link set), which is why they live in one crate behind crate-internal
//! visibility. `pa-tui` re-exports the modules so existing
//! `crate::daemon_client` callers and `pa_tui::daemon_client` users keep
//! their paths.
//!
//! `pa-cli` still carries its own separate `daemon_client` (its non-transport
//! behavior — startup, settings, discovery — is out of scope for the
//! extraction); a later PR converges `pa-cli` onto this crate as the thin
//! client adapter, not this one.

pub mod daemon_client;
pub mod direct_transport;
