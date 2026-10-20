//! Single tool-call pipeline: argument preparation and schema validation,
//! the `beforeToolCall`/`afterToolCall` hooks, abort racing, streamed
//! `tool_execution_update` emission, and the resulting tool outcome.

use std::sync::Arc;

use crate::abort::{is_abort_error, AbortSignal};
use crate::types::{
    AfterToolCallContext, AgentContext, AgentEvent, AgentMessage, AgentTool, AgentToolResult,
    AgentToolUpdateCallback, AssistantMessage, BeforeToolCallContext, ToolCall, ToolResultMessage,
};

use super::abort::race_with_abort;
use super::run::clone_context;
use super::tools::{find_tool, FinalizedToolCallOutcome, Preparation, PreparedToolCall};
use super::{AgentEventSink, AgentLoopConfig};

/// Tool lookup, `prepareArguments`, schema validation, and the
/// `beforeToolCall` hook. Never fails; errors become immediate error tool
/// results exactly like the TS catch-all.
pub(crate) async fn prepare_tool_call(
    current_context: &AgentContext,
    assistant_message: &AssistantMessage,
    tool_call: &ToolCall,
    config: &AgentLoopConfig,
    signal: Option<&AbortSignal>,
) -> Preparation {
    let Some(tool) = find_tool(&current_context.tools, &tool_call.name) else {
        return Preparation::Immediate {
            result: AgentToolResult::error(format!("Tool {} not found", tool_call.name)),
            is_error: true,
        };
    };

    let result = prepare_tool_call_inner(
        tool,
        assistant_message,
        tool_call,
        current_context,
        config,
        signal,
    )
    .await;
    match result {
        Ok(preparation) => preparation,
        Err(error) => Preparation::Immediate {
            result: AgentToolResult::error(format!("{error:#}")),
            is_error: true,
        },
    }
}

async fn prepare_tool_call_inner(
    tool: &Arc<dyn AgentTool>,
    assistant_message: &AssistantMessage,
    tool_call: &ToolCall,
    current_context: &AgentContext,
    config: &AgentLoopConfig,
    signal: Option<&AbortSignal>,
) -> anyhow::Result<Preparation> {
    let prepared_tool_call = match tool.prepare_arguments(&tool_call.arguments) {
        Some(prepared) => ToolCall {
            arguments: prepared,
            ..tool_call.clone()
        },
        None => tool_call.clone(),
    };
    let validated_args = crate::validation::validate_tool_arguments(
        tool_call.name.as_str(),
        tool.parameters(),
        &prepared_tool_call.arguments,
    )
    .map_err(anyhow::Error::msg)?;

    if let Some(before_tool_call) = config.before_tool_call.as_ref() {
        let before_result = race_with_abort(
            before_tool_call(
                BeforeToolCallContext {
                    assistant_message: assistant_message.clone(),
                    tool_call: tool_call.clone(),
                    args: validated_args.clone(),
                    context: clone_context(current_context),
                },
                signal.cloned().unwrap_or_default(),
            ),
            signal,
        )
        .await?;
        if before_result.as_ref().is_some_and(|r| r.block) {
            let reason = before_result
                .and_then(|r| r.reason)
                .unwrap_or_else(|| "Tool execution was blocked".to_string());
            return Ok(Preparation::Immediate {
                result: AgentToolResult::error(reason),
                is_error: true,
            });
        }
    }

    Ok(Preparation::Prepared(PreparedToolCall {
        tool_call: tool_call.clone(),
        tool: Arc::clone(tool),
        args: validated_args,
    }))
}

pub(crate) struct ExecutedToolCallOutcome {
    result: AgentToolResult,
    is_error: bool,
    /// Wall time of the tool execution itself (one decimal, ms): the
    /// measurement spans the `execute` call only, never the update-emitter
    /// drain or the argument preparation.
    duration_ms: f64,
}

/// Race the tool against abort, stream `tool_execution_update` events
/// through a background emitter task, and return an error tool result when
/// the tool fails (or aborts, with the TS message "Tool execution aborted").
// The tool executor's own drain/error ladder sits at 103 lines with the
// execution-duration measurement added; the style gate stays at 100, so the
// narrow allow documents the one-function exception rather than splitting a
// single-use helper out (the no-single-use-helpers rule wins).
#[allow(clippy::too_many_lines)]
pub(crate) async fn execute_prepared_tool_call(
    prepared: &PreparedToolCall,
    signal: Option<&AbortSignal>,
    emit: &AgentEventSink,
) -> ExecutedToolCallOutcome {
    let accepting_updates = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let (update_tx, mut update_rx) = tokio::sync::mpsc::unbounded_channel::<AgentToolResult>();
    let update_tx_for_callback = update_tx.clone();

    let tool_call_id = prepared.tool_call.id.clone();
    let tool_name = prepared.tool.name().to_string();
    let args = prepared.tool_call.arguments.clone();
    let sink = Arc::clone(emit);
    let signal_for_updates = signal.cloned().unwrap_or_default();
    let emit_updates = tokio::spawn(async move {
        while let Some(partial_result) = update_rx.recv().await {
            sink(AgentEvent::ToolExecutionUpdate {
                tool_call_id: tool_call_id.clone(),
                tool_name: tool_name.clone(),
                args: args.clone(),
                partial_result,
            })
            .await?;
        }
        Ok(())
    });

    let on_update: AgentToolUpdateCallback = {
        let accepting_updates = Arc::clone(&accepting_updates);
        Arc::new(move |partial_result: AgentToolResult| {
            if !accepting_updates.load(std::sync::atomic::Ordering::SeqCst)
                || signal_for_updates.is_aborted()
            {
                return;
            }
            let _ = update_tx_for_callback.send(partial_result);
        })
    };

    let execute_started_at = std::time::Instant::now();
    let execute_result = race_with_abort(
        (Arc::clone(&prepared.tool)).execute(
            prepared.tool_call.id.clone(),
            prepared.args.clone(),
            signal.cloned().unwrap_or_default(),
            on_update,
        ),
        signal,
    )
    .await;
    let duration_ms = round_ms(elapsed_ms(execute_started_at));

    accepting_updates.store(false, std::sync::atomic::Ordering::SeqCst);
    drop(update_tx);

    match execute_result {
        Ok(result) => {
            match race_with_abort(
                async {
                    emit_updates.await.map_err(|error| {
                        anyhow::anyhow!("Tool update emitter task failed: {error}")
                    })
                },
                signal,
            )
            .await
            {
                Ok(update_result) => {
                    if let Err(error) = update_result {
                        // Success path: a failing update emitter mirrors a
                        // rejecting updateEvents promise in TS.
                        return ExecutedToolCallOutcome {
                            result: error_tool_result(signal, &error),
                            is_error: true,
                            duration_ms,
                        };
                    }
                }
                Err(error) => {
                    let aborted = signal.is_some_and(AbortSignal::is_aborted);
                    if !(aborted && is_abort_error(&error)) {
                        return ExecutedToolCallOutcome {
                            result: error_tool_result(signal, &error),
                            is_error: true,
                            duration_ms,
                        };
                    }
                }
            }
            ExecutedToolCallOutcome {
                result,
                is_error: false,
                duration_ms,
            }
        }
        Err(error) => {
            // TS drains the pending update emissions on the error path,
            // swallowing every failure (`Promise.all(...).catch(() => undefined)`).
            let _ = race_with_abort(
                async {
                    emit_updates
                        .await
                        .map_err(anyhow::Error::from)
                        .and_then(|drained| drained)
                },
                signal,
            )
            .await;
            ExecutedToolCallOutcome {
                result: error_tool_result(signal, &error),
                is_error: true,
                duration_ms,
            }
        }
    }
}

/// Milliseconds between the measurement start and now.
fn elapsed_ms(from: std::time::Instant) -> f64 {
    from.elapsed().as_secs_f64() * 1000.0
}

/// One decimal of precision.
fn round_ms(delta_ms: f64) -> f64 {
    (delta_ms * 10.0).round() / 10.0
}

fn error_tool_result(signal: Option<&AbortSignal>, error: &anyhow::Error) -> AgentToolResult {
    if signal.is_some_and(AbortSignal::is_aborted) {
        AgentToolResult::error("Tool execution aborted")
    } else {
        AgentToolResult::error(format!("{error:#}"))
    }
}

/// Apply the `afterToolCall` overrides.
pub(crate) async fn finalize_executed_tool_call(
    current_context: &AgentContext,
    assistant_message: &AssistantMessage,
    prepared: &PreparedToolCall,
    executed: ExecutedToolCallOutcome,
    config: &AgentLoopConfig,
    signal: Option<&AbortSignal>,
) -> anyhow::Result<FinalizedToolCallOutcome> {
    let ExecutedToolCallOutcome {
        mut result,
        mut is_error,
        duration_ms,
    } = executed;

    if let Some(after_tool_call) = config.after_tool_call.as_ref() {
        let after_result = race_with_abort(
            after_tool_call(
                AfterToolCallContext {
                    assistant_message: assistant_message.clone(),
                    tool_call: prepared.tool_call.clone(),
                    args: prepared.args.clone(),
                    result: result.clone(),
                    is_error,
                    context: clone_context(current_context),
                },
                signal.cloned().unwrap_or_default(),
            ),
            signal,
        )
        .await;
        match after_result {
            Ok(Some(overrides)) => {
                // Field-by-field merge, no deep merge (TS semantics).
                let mut merged = result;
                if let Some(content) = overrides.content {
                    merged.content = content;
                }
                if let Some(details) = overrides.details {
                    merged.details = details;
                }
                if let Some(terminate) = overrides.terminate {
                    merged.terminate = Some(terminate);
                }
                if let Some(error) = overrides.is_error {
                    is_error = error;
                }
                result = merged;
            }
            Ok(None) => {}
            Err(error) => {
                result = AgentToolResult::error(format!("{error:#}"));
                is_error = true;
            }
        }
    }

    Ok(FinalizedToolCallOutcome {
        tool_call: prepared.tool_call.clone(),
        result,
        is_error,
        duration_ms: Some(duration_ms),
    })
}

pub(crate) async fn emit_tool_execution_end(
    finalized: &FinalizedToolCallOutcome,
    emit: &AgentEventSink,
) -> anyhow::Result<()> {
    emit(AgentEvent::ToolExecutionEnd {
        tool_call_id: finalized.tool_call.id.clone(),
        tool_name: finalized.tool_call.name.clone(),
        result: finalized.result.clone(),
        is_error: finalized.is_error,
    })
    .await
}

pub(crate) fn create_tool_result_message(
    finalized: &FinalizedToolCallOutcome,
) -> ToolResultMessage {
    // The measured execution duration rides the row's `details` block as
    // `durationMs` (the TS wire name, reused): this fork stamps the loop's
    // uniform full-call wall duration for every executed tool call — the
    // TS python rows carried the narrower kernel-execution value, and a
    // cumulative tool-time metric must not mix spans, so the loop's value
    // overwrites a tool-local one (a documented, intentional fork
    // deviation from the TS timing values). A scalar details value is
    // pathological and keeps its shape unmeasured rather than being dropped.
    let mut details = if finalized.result.details.is_null() {
        None
    } else {
        Some(finalized.result.details.clone())
    };
    if let Some(duration_ms) = finalized.duration_ms {
        details = match details {
            Some(serde_json::Value::Object(mut map)) => {
                map.insert("durationMs".to_string(), serde_json::json!(duration_ms));
                Some(serde_json::Value::Object(map))
            }
            None => Some(serde_json::json!({ "durationMs": duration_ms })),
            Some(other) => Some(other),
        };
    }
    ToolResultMessage {
        tool_call_id: finalized.tool_call.id.clone(),
        tool_name: finalized.tool_call.name.clone(),
        content: finalized.result.content.clone(),
        details,
        is_error: finalized.is_error,
        timestamp: crate::now_ms(),
    }
}

pub(crate) async fn emit_tool_result_message(
    tool_result_message: &ToolResultMessage,
    emit: &AgentEventSink,
) -> anyhow::Result<()> {
    emit(AgentEvent::MessageStart {
        message: AgentMessage::from(tool_result_message.clone()),
    })
    .await?;
    emit(AgentEvent::MessageEnd {
        message: AgentMessage::from(tool_result_message.clone()),
    })
    .await
}
