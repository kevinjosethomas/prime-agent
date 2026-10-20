//! Always-on per-response timing behind `get_state`'s cumulative
//! `responseTiming`: independent of the request-timing diagnostic and of
//! telemetry consent. Real instants only (ms, one decimal — never estimates):
//! ttftMs = payload hook (request sent) -> first nonempty text/thinking delta;
//! generationMs = that delta -> terminal done/error event;
//! llmTimeMs = request sent -> terminal event.
//! A field whose endpoints did not both fire stays absent; `result()` carries
//! the terminal event's timing onto the fresh final message, so the persisted
//! assistant row keeps content and timing. Scope: agent-loop assistant
//! responses only — summarizers never cross this seam; user aborts (no
//! terminal event) honestly carry no timing.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use pa_agent::stream::{
    AssistantMessageEvent, ModelStream, OnPayloadHook, StreamFn, StreamRequestOptions,
};
use pa_agent::types::{AssistantMessage, AssistantMessageTiming};

#[derive(Debug, Default)]
struct ResponseClock {
    inner: Mutex<ResponseClockInner>,
}

#[derive(Debug, Default)]
struct ResponseClockInner {
    request_sent_at: Option<Instant>,
    first_delta_at: Option<Instant>,
    terminal_at: Option<Instant>,
    /// result() carries this onto the fresh final message, not a cached event message.
    stamped_timing: Option<AssistantMessageTiming>,
}

impl ResponseClock {
    /// Only a fired payload hook sets the mark — no fabricated fallback timestamps.
    fn mark_request_sent(&self) {
        let mut inner = self.lock();
        if inner.request_sent_at.is_none() {
            inner.request_sent_at = Some(Instant::now());
        }
    }

    /// Tool-call deltas never start the interval.
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

    /// Idempotent: the first terminal event wins.
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

/// None unless one pair of endpoints fired.
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

/// Always-on wrapper: marks request sent (after the composed payload hook),
/// stamps the terminal message, carries timing through result(). Cannot
/// fail the request.
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
                // The mark follows the composed hook: the request leaves only once the payload is final.
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
