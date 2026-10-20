//! Always-on per-response timing: the product-surface measurement behind
//! `get_state`'s cumulative `responseTiming` summary.
//!
//! The wrapper sits at the same `StreamFn` boundary as the opt-in
//! [`super::request_timing`] diagnostic logging, but is independent of it and
//! of telemetry consent: timing is a first-class session fact, not an
//! opt-in diagnostic.
//!
//! Measurements (real instants only, one decimal, ms — never estimates):
//!
//! * `ttftMs`: request sent (the provider fired the payload hook) -> the
//!   first nonempty streamed text/thinking delta.
//! * `generationMs`: that first delta -> the terminal `done`/`error` event.
//! * `llmTimeMs`: request sent -> the terminal event.
//!
//! A response measures only what actually happened: the marks are set when
//! their events fire, and any field whose endpoints did not both fire stays
//! absent (a tool-only response carries `llmTimeMs` without a TTFT; a
//! request whose payload hook never fired carries no `llmTimeMs` either).
//! A terminal `error` event is measured like `done`; a hard user abort
//! that closes the stream without a terminal event synthesizes its aborted
//! message outside this seam, and that row honestly carries no timing.
//!
//! The terminal event's message is stamped in place and the same measured
//! timing rides `result()`'s final message, so the measurement persists with
//! the session JSONL under the assistant row's `timing` block and folds into
//! the daemon's whole-file scan. Scope:
//! assistant responses of the agent loop; the compaction/branch summarizer
//! calls are excluded by design (they never cross this seam — see
//! `compaction_exec`'s `SummarizerFn`).

use std::sync::{Arc, Mutex};
use std::time::Instant;

use pa_agent::stream::{
    AssistantMessageEvent, ModelStream, OnPayloadHook, StreamFn, StreamRequestOptions,
};
use pa_agent::types::{AssistantMessage, AssistantMessageTiming};

/// The per-request measurement state, shared by the payload hook and the
/// event-stream wrapper.
#[derive(Debug, Default)]
struct ResponseClock {
    inner: Mutex<ResponseClockInner>,
}

#[derive(Debug, Default)]
struct ResponseClockInner {
    request_sent_at: Option<Instant>,
    first_delta_at: Option<Instant>,
    terminal_at: Option<Instant>,
    /// The measured timing of the terminal event: `result()` carries it onto
    /// the resolved message (the provider's final row, not a cached event
    /// message) so the persisted row keeps both its final content and its
    /// timing.
    stamped_timing: Option<AssistantMessageTiming>,
}

impl ResponseClock {
    /// Request sent: only a fired payload hook ever sets the mark (no
    /// fabricated fallback timestamps).
    fn mark_request_sent(&self) {
        let mut inner = self.lock();
        if inner.request_sent_at.is_none() {
            inner.request_sent_at = Some(Instant::now());
        }
    }

    /// The first nonempty streamed text/thinking delta clears the wait; tool
    /// call deltas are not generation tokens and never start the interval.
    fn mark_first_delta(&self, event: &AssistantMessageEvent) {
        let delta = match event {
            AssistantMessageEvent::TextDelta { delta, .. }
            | AssistantMessageEvent::ThinkingDelta { delta, .. } => delta.as_str(),
            _ => return,
        };
        if delta.is_empty() {
            return;
        }
        let mut inner = self.lock();
        if inner.first_delta_at.is_none() {
            inner.first_delta_at = Some(Instant::now());
        }
    }

    /// Stamp the terminal message (the measurement ends at the terminal
    /// event) and remember it for `result()`. Idempotent: the first terminal
    /// event wins and a later one changes nothing.
    fn stamp_terminal(&self, message: &mut AssistantMessage) {
        let mut inner = self.lock();
        if inner.terminal_at.is_some() {
            return;
        }
        inner.terminal_at = Some(Instant::now());
        let Some(timing) = measured_timing(&inner) else {
            return;
        };
        message.timing = Some(timing);
        inner.stamped_timing = Some(timing);
    }

    /// `result()` carries the terminal event's measured timing onto the
    /// provider's final message; a stream that ended without a terminal
    /// event measures at the resolution moment (the real end the loop
    /// observed).
    fn stamp_resolved(&self, message: AssistantMessage) -> AssistantMessage {
        let stamped = self.lock().stamped_timing;
        let mut message = message;
        match stamped {
            Some(timing) => message.timing = Some(timing),
            None => self.stamp_terminal(&mut message),
        }
        message
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ResponseClockInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The measured fields for the recorded instants; every field whose
/// endpoints did not both fire stays `None` (serialized away). `None` marks a
/// request with no measurable span at all.
fn measured_timing(inner: &ResponseClockInner) -> Option<AssistantMessageTiming> {
    let terminal_at = inner.terminal_at?;
    let ttft_ms = inner
        .request_sent_at
        .zip(inner.first_delta_at)
        .map(|(sent, delta)| round_ms(elapsed_ms(sent, delta)));
    let generation_ms = inner
        .first_delta_at
        .map(|delta| round_ms(elapsed_ms(delta, terminal_at)));
    let llm_time_ms = inner
        .request_sent_at
        .map(|sent| round_ms(elapsed_ms(sent, terminal_at)));
    if ttft_ms.is_none() && generation_ms.is_none() && llm_time_ms.is_none() {
        return None;
    }
    Some(AssistantMessageTiming {
        ttft_ms,
        generation_ms,
        llm_time_ms,
    })
}

/// Milliseconds between two instants.
fn elapsed_ms(from: Instant, to: Instant) -> f64 {
    (to - from).as_secs_f64() * 1000.0
}

/// One decimal of precision.
fn round_ms(delta_ms: f64) -> f64 {
    (delta_ms * 10.0).round() / 10.0
}

/// Compose the always-on response-timing measurement over a stream function.
///
/// The wrapped call marks request-sent through the payload hook (chained
/// over any existing hook), stamps the final assistant message on the
/// terminal event and on an early `result()` resolution, and passes every
/// event through unchanged otherwise. The measurement cannot fail the request:
/// a clock that never saw its events simply stamps nothing.
#[must_use]
pub fn instrument_response_timing(stream_fn: StreamFn) -> StreamFn {
    std::sync::Arc::new(move |model, context, options: StreamRequestOptions| {
        let stream_fn = std::sync::Arc::clone(&stream_fn);
        Box::pin(async move {
            let clock = std::sync::Arc::new(ResponseClock::default());
            let mut options = options;
            let inner_payload: Option<OnPayloadHook> = options.on_payload.take();
            let clock_for_payload = std::sync::Arc::clone(&clock);
            options.on_payload = Some(std::sync::Arc::new(move |payload, model| {
                // The composed hook runs first: the request leaves for
                // the provider only once the (possibly transformed)
                // payload is final, so the mark follows the chain.
                let payload = match inner_payload.as_ref() {
                    Some(hook) => hook(payload, model),
                    None => Some(payload),
                };
                clock_for_payload.mark_request_sent();
                payload
            }));
            let stream = stream_fn(model, context, options).await?;
            Ok(Box::new(TimedStream {
                inner: stream,
                clock,
            }) as Box<dyn ModelStream>)
        })
    })
}

/// Wraps the provider stream: marks the first content delta, stamps the
/// terminal event's message, and carries the measured timing onto the final
/// message `result()` resolves.
struct TimedStream {
    inner: Box<dyn ModelStream>,
    clock: Arc<ResponseClock>,
}

impl ModelStream for TimedStream {
    fn next_event(&mut self) -> pa_agent::BoxFut<'_, Option<AssistantMessageEvent>> {
        let clock = Arc::clone(&self.clock);
        Box::pin(async move {
            let mut event = self.inner.next_event().await?;
            clock.mark_first_delta(&event);
            match &mut event {
                AssistantMessageEvent::Done { message, .. } => {
                    clock.stamp_terminal(message);
                }
                AssistantMessageEvent::Error { error, .. } => {
                    clock.stamp_terminal(error);
                }
                _ => {}
            }
            Some(event)
        })
    }

    fn result(&mut self) -> pa_agent::BoxFut<'_, anyhow::Result<AssistantMessage>> {
        let clock = Arc::clone(&self.clock);
        Box::pin(async move {
            let message = self.inner.result().await?;
            Ok(clock.stamp_resolved(message))
        })
    }

    fn close(&mut self) {
        self.inner.close();
    }
}

#[cfg(test)]
mod tests;
