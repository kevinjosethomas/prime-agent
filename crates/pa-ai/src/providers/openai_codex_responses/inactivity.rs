//! Bound a silent Codex socket without replaying an ambiguously accepted request.

use std::time::Duration;

use tokio::sync::mpsc;

use super::errors::{CodexApiError, CodexStreamError};
use super::websocket::WorkerEvent;

pub(super) const STREAM_INACTIVITY: Duration = Duration::from_secs(10 * 60);

pub(super) async fn next_event(
    events: &mut mpsc::Receiver<WorkerEvent>,
) -> Result<Option<WorkerEvent>, CodexStreamError> {
    tokio::time::timeout(STREAM_INACTIVITY, events.recv())
        .await
        .map_err(|_| {
            // Non-transport error: the socket accepted this request. Falling back to
            // SSE here could duplicate it; only the caller may authorize another attempt.
            CodexStreamError::Api(CodexApiError {
                message: "Model response timed out after 10 minutes without stream activity."
                    .to_string(),
                code: Some("response_stream_inactive".to_string()),
                status: None,
                retry_after_ms: None,
                payload: None,
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test(start_paused = true)]
    async fn codex_stream_inactivity_is_bounded_without_transport_replay() {
        let (_sender, mut receiver) = mpsc::channel(1);
        let result = tokio::time::timeout(
            STREAM_INACTIVITY + Duration::from_secs(1),
            next_event(&mut receiver),
        )
        .await
        .expect("inactive stream must settle before the outer deadline")
        .err()
        .expect("silent live channel must fail");
        assert!(result.is_non_transport_error());
        assert_eq!(
            result.to_string(),
            "Model response timed out after 10 minutes without stream activity."
        );
    }

    #[tokio::test(start_paused = true)]
    async fn codex_stream_inactivity_restarts_after_a_real_event() {
        let (sender, mut receiver) = mpsc::channel(1);
        let pending = tokio::spawn(async move {
            tokio::time::sleep(STREAM_INACTIVITY - Duration::from_secs(1)).await;
            sender
                .send(WorkerEvent::Event(json!({ "type": "response.created" })))
                .await
                .expect("event receiver");
            sender
        });
        assert!(matches!(
            next_event(&mut receiver).await.expect("live event"),
            Some(WorkerEvent::Event(_))
        ));
        let sender = pending.await.expect("sender task");
        let started = tokio::time::Instant::now();
        assert!(next_event(&mut receiver).await.is_err());
        assert_eq!(started.elapsed(), STREAM_INACTIVITY);
        drop(sender);
    }

    #[tokio::test]
    async fn codex_stream_inactivity_preserves_completed_channel_semantics() {
        let (sender, mut receiver) = mpsc::channel(1);
        drop(sender);
        assert!(next_event(&mut receiver)
            .await
            .expect("closed channel")
            .is_none());
    }
}
