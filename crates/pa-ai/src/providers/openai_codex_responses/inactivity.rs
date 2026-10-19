//! Bound a silent Codex socket without replaying an ambiguously accepted request.

use std::time::Duration;

use tokio::sync::mpsc;

use super::errors::{CodexApiError, CodexStreamError};
use super::websocket::WorkerEvent;

pub(super) const STREAM_INACTIVITY: Duration = Duration::from_mins(10);

const PROBE_INTERVAL: Duration = Duration::from_secs(5);
const PAUSE_GAP: Duration = Duration::from_secs(30);

/// Content progress is distinct from socket keepalives and repeated lifecycle metadata.
pub(super) struct ActivityWindow {
    deadline: tokio::time::Instant,
}

impl ActivityWindow {
    pub(super) fn new() -> Self {
        Self {
            deadline: tokio::time::Instant::now() + STREAM_INACTIVITY,
        }
    }

    pub(super) fn observe(&mut self, event: &serde_json::Value) {
        let kind = event
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let meaningful = kind == "error"
            || kind == "response.completed"
            || kind == "response.done"
            || kind == "response.incomplete"
            || kind == "response.failed"
            || kind.starts_with("response.output_")
            || kind.starts_with("response.reasoning_")
            || kind.starts_with("response.function_call_arguments.")
            || kind.starts_with("response.content_part.");
        // An event-type suffix, not a file extension: the wire casing is exact.
        #[allow(clippy::case_sensitive_file_extension_comparisons)]
        let nonempty = !kind.ends_with(".delta")
            || event
                .get("delta")
                .is_some_and(|delta| delta.as_str().is_none_or(|text| !text.is_empty()));
        if meaningful && nonempty {
            self.deadline = tokio::time::Instant::now() + STREAM_INACTIVITY;
        }
    }

    pub(super) async fn wait<T>(
        &self,
        activity: impl std::future::Future<Output = T>,
    ) -> Result<T, CodexStreamError> {
        wait_with_clock(
            activity,
            std::time::SystemTime::now,
            self.deadline
                .saturating_duration_since(tokio::time::Instant::now()),
        )
        .await
    }

    pub(super) async fn next_event(
        &mut self,
        events: &mut mpsc::Receiver<WorkerEvent>,
    ) -> Result<Option<WorkerEvent>, CodexStreamError> {
        let event = self.wait(events.recv()).await?;
        if let Some(WorkerEvent::Event(value)) = &event {
            self.observe(value);
        }
        Ok(event)
    }
}

#[cfg(test)]
async fn next_event(
    events: &mut mpsc::Receiver<WorkerEvent>,
) -> Result<Option<WorkerEvent>, CodexStreamError> {
    ActivityWindow::new().next_event(events).await
}

/// Bound one network wait, including a socket left silent across suspend/resume.
/// An ambiguous accepted request is surfaced once; this guard never resends it.
pub(super) async fn wait_for_activity<T>(
    activity: impl std::future::Future<Output = T>,
) -> Result<T, CodexStreamError> {
    wait_with_clock(activity, std::time::SystemTime::now, STREAM_INACTIVITY).await
}

async fn wait_with_clock<T>(
    activity: impl std::future::Future<Output = T>,
    mut wall_now: impl FnMut() -> std::time::SystemTime,
    remaining: Duration,
) -> Result<T, CodexStreamError> {
    if remaining.is_zero() {
        return Err(interruption("response_stream_inactive", "Model response timed out after 10 minutes without stream activity. Check activity before continuing."));
    }
    tokio::pin!(activity);
    let deadline = tokio::time::sleep(remaining);
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
            tokio::time::sleep(STREAM_INACTIVITY.saturating_sub(Duration::from_secs(1))).await;
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
        let result = wait_with_clock(
            std::future::pending::<()>(),
            || {
                samples += 1;
                std::time::UNIX_EPOCH
                    + if samples == 1 {
                        Duration::ZERO
                    } else {
                        Duration::from_secs(60)
                    }
            },
            STREAM_INACTIVITY,
        )
        .await
        .expect_err("suspended silent request must settle");
        assert!(result.is_non_transport_error());
        assert!(result.to_string().contains("process was paused"));
    }

    #[tokio::test(start_paused = true)]
    async fn codex_stream_inactivity_keeps_a_ready_event_after_wake() {
        let result =
            wait_with_clock(async { 42 }, || std::time::UNIX_EPOCH, STREAM_INACTIVITY).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test(start_paused = true)]
    async fn codex_stream_inactivity_ignores_keepalives_but_counts_reasoning_and_tool_deltas() {
        let mut window = ActivityWindow::new();
        let original = window.deadline;
        tokio::time::advance(Duration::from_secs(60)).await;
        for kind in [
            "response.created",
            "response.in_progress",
            "heartbeat",
            "vendor.metadata",
        ] {
            window.observe(&json!({"type":kind}));
            assert_eq!(window.deadline, original);
        }
        for kind in [
            "response.reasoning_summary_text.delta",
            "response.function_call_arguments.delta",
            "response.output_text.delta",
        ] {
            tokio::time::advance(Duration::from_secs(60)).await;
            window.observe(&json!({"type":kind,"delta":"real progress"}));
            assert_eq!(
                window.deadline,
                tokio::time::Instant::now() + STREAM_INACTIVITY
            );
        }
        let last = window.deadline;
        window.observe(&json!({"type":"response.output_text.delta","delta":""}));
        assert_eq!(window.deadline, last);
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
