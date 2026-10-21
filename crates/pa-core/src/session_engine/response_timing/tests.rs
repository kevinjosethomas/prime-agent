//! The response-timing wrapper: measured fields for scripted provider
//! streams, and the absence rules (no payload hook, no delta, early result).

use std::sync::Arc;

use pa_agent::stream::{event_stream, AssistantMessageEvent, StreamFn, StreamRequestOptions};
use pa_agent::types::{AssistantContent, AssistantMessage, StopReason, TextContent};

use super::instrument_response_timing;

fn base_message() -> AssistantMessage {
    AssistantMessage {
        content: vec![AssistantContent::Text(TextContent {
            text: String::new(),
            text_signature: None,
        })],
        api: String::new(),
        provider: String::new(),
        model: String::new(),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: pa_agent::types::Usage::zero(),
        stop_reason: StopReason::Stop,
        stop_reason_raw: None,
        error_message: None,
        timing: None,
        timestamp: 0,
    }
}

/// A scripted stream: fires the payload hook (request sent), pushes the
/// given events, ends, and resolves the final message.
fn scripted_stream(events: Vec<AssistantMessageEvent>, result: AssistantMessage) -> StreamFn {
    // `StreamFn` is an `Fn`: the events drain once from shared storage.
    let events = Arc::new(std::sync::Mutex::new(events));
    Arc::new(move |_model, _context, mut options: StreamRequestOptions| {
        let payload_hook = options.on_payload.take();
        let result = result.clone();
        let events = events.lock().unwrap().drain(..).collect::<Vec<_>>();
        Box::pin(async move {
            if let Some(hook) = payload_hook {
                hook(serde_json::json!({}), &model_for_hook());
            }
            let (handle, consumer) = event_stream();
            for event in events {
                handle.push(event);
            }
            handle.end(Some(result));
            Ok(Box::new(consumer) as Box<dyn pa_agent::stream::ModelStream>)
        })
    })
}

fn model_for_hook() -> pa_agent::types::Model {
    serde_json::from_value(serde_json::json!({
        "id": "faux-1", "provider": "faux", "api": "messages",
    }))
    .expect("model wire shape")
}

async fn collect(mut stream: Box<dyn pa_agent::stream::ModelStream>) -> Option<AssistantMessage> {
    let mut terminal = None;
    while let Some(event) = stream.next_event().await {
        if let Some(message) = event.terminal_message() {
            terminal = Some(message.clone());
        }
    }
    let result = stream.result().await.expect("stream result resolves");
    Some(terminal.unwrap_or(result))
}

#[tokio::test]
async fn stamps_ttft_generation_and_llm_time_on_the_terminal_message() {
    let message = base_message();
    let message_clone = message.clone();
    let stream_fn = instrument_response_timing(scripted_stream(
        vec![
            AssistantMessageEvent::TextDelta {
                content_index: 0,
                delta: "he".to_string(),
                partial: message.clone(),
            },
            AssistantMessageEvent::TextDelta {
                content_index: 0,
                delta: "llo".to_string(),
                partial: message.clone(),
            },
            AssistantMessageEvent::Done {
                reason: StopReason::Stop,
                message: message_clone,
            },
        ],
        message,
    ));
    let stream = stream_fn(
        model_for_hook(),
        pa_agent::stream::LlmContext::default(),
        StreamRequestOptions::default(),
    )
    .await
    .expect("stream opens");
    let final_message = collect(stream).await.expect("final message");
    let timing = final_message.timing.expect("timing stamped");
    let (ttft, generation, llm) = (
        timing.ttft_ms.expect("ttft measured"),
        timing.generation_ms.expect("generation measured"),
        timing.llm_time_ms.expect("llm time measured"),
    );
    assert!(ttft >= 0.0);
    assert!(generation >= 0.0);
    assert!(llm >= ttft);
}

#[tokio::test]
async fn tool_only_response_carries_llm_time_without_ttft_or_generation() {
    let mut message = base_message();
    message.content.clear();
    message.stop_reason = StopReason::ToolUse;
    let message_clone = message.clone();
    let stream_fn = instrument_response_timing(scripted_stream(
        vec![AssistantMessageEvent::Done {
            reason: StopReason::ToolUse,
            message: message_clone,
        }],
        message,
    ));
    let stream = stream_fn(
        model_for_hook(),
        pa_agent::stream::LlmContext::default(),
        StreamRequestOptions::default(),
    )
    .await
    .expect("stream opens");
    let final_message = collect(stream).await.expect("final message");
    let timing = final_message.timing.expect("llm time measured");
    assert!(
        timing.ttft_ms.is_none(),
        "no text delta means no TTFT: {timing:?}"
    );
    assert!(timing.generation_ms.is_none());
    assert!(timing.llm_time_ms.is_some());
}

#[tokio::test]
async fn empty_deltas_never_start_the_ttft_clock() {
    let message = base_message();
    let message_clone = message.clone();
    let stream_fn = instrument_response_timing(scripted_stream(
        vec![
            AssistantMessageEvent::TextDelta {
                content_index: 0,
                delta: String::new(),
                partial: message.clone(),
            },
            AssistantMessageEvent::ThinkingDelta {
                content_index: 0,
                delta: String::new(),
                partial: message.clone(),
            },
            AssistantMessageEvent::Done {
                reason: StopReason::Stop,
                message: message_clone,
            },
        ],
        message,
    ));
    let stream = stream_fn(
        model_for_hook(),
        pa_agent::stream::LlmContext::default(),
        StreamRequestOptions::default(),
    )
    .await
    .expect("stream opens");
    let final_message = collect(stream).await.expect("final message");
    let timing = final_message.timing.expect("llm time measured");
    assert!(
        timing.ttft_ms.is_none(),
        "empty deltas are not tokens: {timing:?}"
    );
    assert!(timing.generation_ms.is_none());
}

#[tokio::test]
async fn without_a_payload_hook_nothing_is_measured() {
    // The scripted stream here never fires the payload hook.
    let message = base_message();
    let message_clone = message.clone();
    let stream_fn = instrument_response_timing(Arc::new(
        move |_model, _context, _options: StreamRequestOptions| {
            let message = message_clone.clone();
            Box::pin(async move {
                let (handle, consumer) = event_stream();
                handle.push(AssistantMessageEvent::Done {
                    reason: StopReason::Stop,
                    message: message.clone(),
                });
                handle.end(Some(message));
                Ok(Box::new(consumer) as Box<dyn pa_agent::stream::ModelStream>)
            })
        },
    ));
    let stream = stream_fn(
        model_for_hook(),
        pa_agent::stream::LlmContext::default(),
        StreamRequestOptions::default(),
    )
    .await
    .expect("stream opens");
    let final_message = collect(stream).await.expect("final message");
    // Without request-sent, only the generation interval (first delta ->
    // terminal) could exist; this stream had no delta either, so nothing is
    // measurable and the row carries no timing at all.
    assert!(final_message.timing.is_none());
}

#[tokio::test]
async fn result_resolution_without_terminal_event_stamps_the_llm_time() {
    let message = base_message();
    let result = message.clone();
    let stream_fn = instrument_response_timing(scripted_stream(vec![], result));
    let mut stream = stream_fn(
        model_for_hook(),
        pa_agent::stream::LlmContext::default(),
        StreamRequestOptions::default(),
    )
    .await
    .expect("stream opens");
    // Consume nothing; resolve straight from result().
    let final_message = stream.result().await.expect("stream result resolves");
    let timing = final_message.timing.expect("llm time measured");
    assert!(timing.llm_time_ms.is_some());
    assert!(timing.ttft_ms.is_none());
}

#[tokio::test]
async fn terminal_error_event_is_measured_like_done() {
    let mut error = base_message();
    error.stop_reason = StopReason::Error;
    error.error_message = Some("provider exploded".to_string());
    let error_clone = error.clone();
    let stream_fn = instrument_response_timing(scripted_stream(
        vec![AssistantMessageEvent::Error {
            reason: StopReason::Error,
            error: error_clone,
        }],
        error,
    ));
    let stream = stream_fn(
        model_for_hook(),
        pa_agent::stream::LlmContext::default(),
        StreamRequestOptions::default(),
    )
    .await
    .expect("stream opens");
    let final_message = collect(stream).await.expect("final message");
    assert!(final_message
        .timing
        .is_some_and(|timing| timing.llm_time_ms.is_some()));
}

#[tokio::test]
async fn existing_payload_hook_still_runs_and_sees_the_payload() {
    let seen = Arc::new(std::sync::Mutex::new(None));
    let seen_for_hook = Arc::clone(&seen);
    let message = base_message();
    let result = message.clone();
    let stream_fn = instrument_response_timing(scripted_stream(vec![], result));
    let options = StreamRequestOptions {
        on_payload: Some(Arc::new(move |payload, _model| {
            *seen_for_hook.lock().unwrap() = Some(payload);
            None
        })),
        ..StreamRequestOptions::default()
    };
    let mut stream = stream_fn(
        model_for_hook(),
        pa_agent::stream::LlmContext::default(),
        options,
    )
    .await
    .expect("stream opens");
    drop(stream.result().await.expect("stream result resolves"));
    assert!(seen.lock().unwrap().is_some(), "inner payload hook ran");
}
