//! Error classification across the JS boundary.
//!
//! The binding rejects with a [`napi::Error`] whose message is the JSON
//! encoding of [`ErrorPayload`]; the TypeScript wrapper decodes it into the
//! typed error classes. The classes mirror `pa-client`'s transport classes
//! one-to-one and never merge:
//!
//! - `NotSent` — provable non-delivery: the command never reached the wire.
//!   Includes boundary deserialize failures. The host may re-route by its
//!   own policy; the binding never auto-resends.
//! - `Uncertain` — the command may have executed; no answer arrived.
//!   Includes overflow/close teardown of in-flight requests. Never resend.
//! - `Rejected` — the daemon answered a refusal.
//! - `Overflow` — the bounded frame queue overflowed and the connection was
//!   torn down; the frame stream is terminal.
//! - `Transport` — anything else.

use pa_client::daemon_client::{
    is_command_not_sent, is_command_result_uncertain, is_daemon_rejection, RequestRejected,
};
use serde::{Deserialize, Serialize};

/// The wire-encoded error shape crossing the napi boundary.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ErrorPayload {
    pub kind: &'static str,
    pub message: String,
    /// `overflow` or `closed` for teardown-resolved request errors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<&'static str>,
    /// The daemon's typed refusal info (`errorInfo`), serialized verbatim,
    /// for `Rejected` payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info: Option<serde_json::Value>,
}

/// Rejection with the `NotSent` class.
pub(crate) fn not_sent(message: String) -> napi::Error {
    payload(ErrorPayload {
        kind: "NotSent",
        message,
        cause: None,
        info: None,
    })
}

/// Rejection with the `Uncertain` class and a teardown cause.
pub(crate) fn uncertain(message: String, cause: Option<&'static str>) -> napi::Error {
    payload(ErrorPayload {
        kind: "Uncertain",
        message,
        cause,
        info: None,
    })
}

/// Rejection with the `Rejected` class and the daemon's error code.
pub(crate) fn rejected(
    message: String,
    info: Option<serde_json::Value>,
) -> napi::Error {
    payload(ErrorPayload {
        kind: "Rejected",
        message,
        cause: None,
        info,
    })
}

/// Rejection with the `NotSent` class and a teardown cause surfaced.
pub(crate) fn not_sent_cause(cause: Option<&'static str>) -> napi::Error {
    let cause_text = match cause {
        Some("overflow") => "the connection was torn down after the frame queue overflowed",
        _ => "the connection was torn down",
    };
    payload(ErrorPayload {
        kind: "NotSent",
        message: format!("{cause_text}; the command was never queued"),
        cause,
        info: None,
    })
}

/// Rejection with the terminal `Overflow` class.
pub(crate) fn overflow() -> napi::Error {
    payload(ErrorPayload {
        kind: "Overflow",
        message: "the frame queue overflowed and the connection was torn down".to_string(),
        cause: Some("overflow"),
        info: None,
    })
}

/// Rejection with the `Transport` class.
pub(crate) fn transport(message: String) -> napi::Error {
    payload(ErrorPayload {
        kind: "Transport",
        message,
        cause: None,
        info: None,
    })
}

fn payload(payload: ErrorPayload) -> napi::Error {
    napi::Error::new(
        napi::Status::GenericFailure,
        serde_json::to_string(&payload).unwrap_or_else(|_| payload.kind.to_string()),
    )
}

/// Classify one `anyhow` chain from a core request per the transport rules.
pub(crate) fn classify(error: anyhow::Error, cause: Option<&'static str>) -> napi::Error {
    if is_command_not_sent(&error) {
        not_sent(error.to_string())
    } else if is_command_result_uncertain(&error) {
        uncertain(error.to_string(), cause)
    } else if is_daemon_rejection(&error) {
        let info = error
            .chain()
            .filter_map(|cause| cause.downcast_ref::<RequestRejected>())
            .find_map(|rejected| rejected.error_info.as_ref())
            .map(serde_json::to_value)
            .and_then(Result::ok);
        rejected(error.to_string(), info)
    } else {
        transport(error.to_string())
    }
}
