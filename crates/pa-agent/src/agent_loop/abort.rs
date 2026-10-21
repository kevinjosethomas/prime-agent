//! Abort and settlement helpers for the agent loop: the `raceWithAbort`
//! wrapper, `settlePostTurn`, `pollMessagesUnlessAborted`, and aborted
//! assistant-message construction.

use crate::abort::{is_abort_error, AbortSignal, ABORT_ERROR_MESSAGE};
use crate::types::{AgentMessage, AssistantContent, AssistantMessage, StopReason};
use std::future::Future;

use super::{AgentLoopConfig, PollMessagesFn};

pub(crate) async fn race_with_abort<T>(
    operation: impl Future<Output = anyhow::Result<T>>,
    signal: Option<&AbortSignal>,
) -> anyhow::Result<T> {
    match signal {
        None => operation.await,
        Some(signal) => crate::abort::race_with_abort(operation, signal)
            .await
            .and_then(|result| result),
    }
}

pub(crate) enum PostTurnResult<T> {
    Completed(T),
    Aborted,
}

/// Abort rejections collapse into `Aborted`; every other error propagates.
pub(crate) async fn settle_post_turn<T>(
    operation: impl Future<Output = anyhow::Result<T>>,
    signal: Option<&AbortSignal>,
) -> anyhow::Result<PostTurnResult<T>> {
    match operation.await {
        Ok(value) => Ok(PostTurnResult::Completed(value)),
        Err(error) => {
            if signal.is_some_and(AbortSignal::is_aborted) && is_abort_error(&error) {
                Ok(PostTurnResult::Aborted)
            } else {
                Err(error)
            }
        }
    }
}

pub(crate) async fn poll_messages_unless_aborted(
    poll: Option<&PollMessagesFn>,
    signal: Option<&AbortSignal>,
) -> anyhow::Result<Vec<AgentMessage>> {
    let Some(poll) = poll else {
        return Ok(Vec::new());
    };
    if signal.is_some_and(AbortSignal::is_aborted) {
        return Ok(Vec::new());
    }
    race_with_abort(poll(), signal).await
}

fn clone_assistant_content(content: &[AssistantContent]) -> Vec<AssistantContent> {
    content
        .iter()
        .map(|part| match part {
            // TS clones the arguments object per toolCall; Value::clone is a
            // full copy, which is at least as safe.
            AssistantContent::ToolCall(tool_call) => AssistantContent::ToolCall(tool_call.clone()),
            AssistantContent::Text(text) => AssistantContent::Text(text.clone()),
            AssistantContent::Thinking(thinking) => AssistantContent::Thinking(thinking.clone()),
        })
        .collect()
}

pub(crate) fn create_aborted_assistant_message(
    config: &AgentLoopConfig,
    partial_message: Option<&AssistantMessage>,
) -> AssistantMessage {
    AssistantMessage {
        content: partial_message.map_or_else(
            || {
                vec![AssistantContent::Text(crate::types::TextContent {
                    text: String::new(),
                    text_signature: None,
                })]
            },
            |partial| clone_assistant_content(&partial.content),
        ),
        api: partial_message.map_or_else(|| config.model.api.clone(), |p| p.api.clone()),
        provider: partial_message
            .map_or_else(|| config.model.provider.clone(), |p| p.provider.clone()),
        model: partial_message.map_or_else(|| config.model.id.clone(), |p| p.model.clone()),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: partial_message.map_or_else(crate::types::Usage::zero, |p| p.usage.clone()),
        stop_reason: StopReason::Aborted,
        stop_reason_raw: None,
        error_message: Some(ABORT_ERROR_MESSAGE.to_string()),
        timing: None,
        timestamp: crate::now_ms(),
    }
}

#[cfg(test)]
mod timing_tests {
    use super::super::AgentLoopConfig;
    use super::*;
    use crate::types::Model;

    /// An aborted response is synthesized outside the stream-timing seam: the
    /// aborted row must carry no timing and no stale fields — a mid-stream
    /// abort discards the partial measurement rather than fabricating one
    /// (measured-only; user-aborted responses are unmeasured by design).
    #[test]
    fn the_aborted_message_carries_no_timing_with_or_without_a_partial() {
        let config =
            AgentLoopConfig::new(Model::unknown(), AgentLoopConfig::default_convert_to_llm());
        let aborted = create_aborted_assistant_message(&config, None);
        assert!(aborted.timing.is_none());
        assert_eq!(aborted.stop_reason, StopReason::Aborted);

        // A partial that could never carry timing (timing is stamped only on
        // terminal rows) still produces an unmeasured aborted row.
        let mut partial = AssistantMessage {
            content: vec![AssistantContent::Text(crate::types::TextContent {
                text: "partial".to_string(),
                text_signature: None,
            })],
            api: String::new(),
            provider: String::new(),
            model: String::new(),
            response_model: None,
            response_id: None,
            diagnostics: None,
            usage: crate::types::Usage::zero(),
            stop_reason: StopReason::Stop,
            stop_reason_raw: None,
            error_message: None,
            timing: None,
            timestamp: 0,
        };
        partial.timing = None;
        let aborted = create_aborted_assistant_message(&config, Some(&partial));
        assert!(aborted.timing.is_none());
    }
}
