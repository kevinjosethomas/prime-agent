//! Dedicated System One evaluation transport, never a chat-completion adapter.
//! Wire contract verified against upstream's historical transport at commit
//! 56408e2a83af8b84983e83670f1e485d71d71f3d and <https://system-one.dev/en/api>.
//! The full experimental Decision API was reverted upstream; this narrow client
//! intentionally adds no control loops, model catalog entries, or agent execution.

use std::time::Duration;

use pa_types::decision::{SystemOneRequest, SystemOneResponse};

use crate::types::StreamOptions;

const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_REQUEST_BYTES: usize = 256 * 1024;

/// Sanitized evaluation failures. Provider bodies, request state, URLs and
/// authentication material never appear in these errors.
#[derive(Debug, thiserror::Error)]
pub enum SystemOneError {
    #[error("Invalid System One evaluation request")]
    InvalidRequest,
    #[error("Invalid System One authentication headers")]
    InvalidHeaders,
    #[error("System One evaluation is unavailable")]
    Unavailable,
    #[error("System One evaluation returned HTTP {0}")]
    Http(u16),
    #[error("System One evaluation timed out")]
    Timeout,
    #[error("System One evaluation was cancelled")]
    Cancelled,
    #[error("Invalid System One evaluation response")]
    InvalidResponse,
}

/// POST a bounded joint evaluation to `base_url/systemone` with host-resolved
/// credentials and headers. No generated message, tools, or history is involved.
///
/// # Errors
/// Returns a sanitized transport or response error; provider bodies are discarded.
#[tracing::instrument(skip_all)]
pub async fn evaluate(
    base_url: &str,
    request: &SystemOneRequest,
    options: &StreamOptions,
) -> Result<SystemOneResponse, SystemOneError> {
    if options
        .signal
        .as_ref()
        .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
    {
        return Err(SystemOneError::Cancelled);
    }
    if request.questions.is_empty()
        || request.questions.len() > 32
        || request.state.is_null()
        || request
            .questions
            .values()
            .any(|question| question.instructions.trim().is_empty() || question.criteria.len() < 2)
    {
        return Err(SystemOneError::InvalidRequest);
    }
    let request_body = serde_json::to_vec(request).map_err(|_| SystemOneError::InvalidRequest)?;
    if request_body.len() > MAX_REQUEST_BYTES {
        return Err(SystemOneError::InvalidRequest);
    }
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        reqwest::header::HeaderValue::from_static("application/json"),
    );
    if let Some(key) = options.api_key.as_deref() {
        let value = reqwest::header::HeaderValue::from_str(&format!("Bearer {key}"))
            .map_err(|_| SystemOneError::InvalidHeaders)?;
        headers.insert(reqwest::header::AUTHORIZATION, value);
    }
    for (name, value) in options.headers.iter().flatten() {
        let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| SystemOneError::InvalidHeaders)?;
        let value = reqwest::header::HeaderValue::from_str(value)
            .map_err(|_| SystemOneError::InvalidHeaders)?;
        headers.insert(name, value);
    }
    let client = crate::utils::tls::http_client_builder()
        .timeout(Duration::from_millis(options.timeout_ms.unwrap_or(20_000)))
        .redirect(reqwest::redirect::Policy::none())
        .default_headers(headers)
        .build()
        .map_err(|_| SystemOneError::Unavailable)?;
    let url = format!("{}/systemone", base_url.trim_end_matches('/'));
    let round_trip = async {
        let mut response = client
            .post(url)
            .body(request_body)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    SystemOneError::Timeout
                } else {
                    SystemOneError::Unavailable
                }
            })?;
        if !response.status().is_success() {
            return Err(SystemOneError::Http(response.status().as_u16()));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            if error.is_timeout() {
                SystemOneError::Timeout
            } else {
                SystemOneError::InvalidResponse
            }
        })? {
            if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
                return Err(SystemOneError::InvalidResponse);
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| SystemOneError::InvalidResponse)
    };
    if let Some(signal) = &options.signal {
        tokio::select! {
            () = signal.cancelled() => Err(SystemOneError::Cancelled),
            result = round_trip => result,
        }
    } else {
        round_trip.await
    }
}

#[cfg(test)]
mod tests;
