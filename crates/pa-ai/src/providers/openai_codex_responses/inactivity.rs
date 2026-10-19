//! Bound a silent Codex socket without replaying an ambiguously accepted request.

use std::time::Duration;

use tokio::sync::mpsc;

use super::errors::{CodexApiError, CodexStreamError};
use super::websocket::WorkerEvent;

pub(super) const STREAM_INACTIVITY: Duration = Duration::from_secs(10 * 60);

const PROBE_INTERVAL: Duration = Duration::from_secs(5);
const PAUSE_GAP: Duration = Duration::from_secs(30);

pub(super) async fn next_event(
    events: &mut mpsc::Receiver<WorkerEvent>,
) -> Result<Option<WorkerEvent>, CodexStreamError> {
    wait_for_activity(events.recv()).await
}

/// Bound one network wait, including a socket left silent across suspend/resume.
/// An ambiguous accepted request is surfaced once; this guard never resends it.
pub(super) async fn wait_for_activity<T>(
    activity: impl std::future::Future<Output = T>,
) -> Result<T, CodexStreamError> {
    wait_with_clock(activity, std::time::SystemTime::now).await
}

async fn wait_with_clock<T>(
    activity: impl std::future::Future<Output = T>,
    mut wall_now: impl FnMut() -> std::time::SystemTime,
) -> Result<T, CodexStreamError> {
    tokio::pin!(activity);
    let deadline = tokio::time::sleep(STREAM_INACTIVITY);
    tokio::pin!(deadline);
    let mut probe =
        tokio::time::interval_at(tokio::time::Instant::now() + PROBE_INTERVAL, PROBE_INTERVAL);
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut previous = wall_now();
    loop {
        tokio::select! {
            // A real event already available after wake wins over a pause warning.
            biased;
            result = &mut activity => return Ok(result),
            () = &mut deadline => return Err(interruption(
                "response_stream_inactive",
                "Model response timed out after 10 minutes without stream activity. Check activity before continuing.",
            )),
            _ = probe.tick() => {
                let current = wall_now();
                let paused = current.duration_since(previous).is_ok_and(|gap| gap >= PAUSE_GAP);
                previous = current;
                if paused {
                    return Err(interruption(
                        "response_stream_interrupted",
                        "Model connection interrupted while this process was paused. Check activity before continuing.",
                    ));
                }
            }
        }
    }
}

fn interruption(code: &str, message: &str) -> CodexStreamError {
    CodexStreamError::Api(CodexApiError {
        message: message.to_string(),
        code: Some(code.to_string()),
        status: None,
        retry_after_ms: None,
        payload: None,
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
            "Model response timed out after 10 minutes without stream activity. Check activity before continuing."
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

    #[tokio::test(start_paused = true)]
    async fn codex_stream_inactivity_detects_suspend_without_transport_replay() {
        let mut samples = 0;
        let result = wait_with_clock(std::future::pending::<()>(), || {
            samples += 1;
            std::time::UNIX_EPOCH
                + if samples == 1 {
                    Duration::ZERO
                } else {
                    Duration::from_secs(60)
                }
        })
        .await
        .expect_err("suspended silent request must settle");
        assert!(result.is_non_transport_error());
        assert!(result.to_string().contains("process was paused"));
    }

    #[tokio::test(start_paused = true)]
    async fn codex_stream_inactivity_keeps_a_ready_event_after_wake() {
        let result = wait_with_clock(async { 42 }, || std::time::UNIX_EPOCH).await;
        assert_eq!(result.unwrap(), 42);
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
