//! The input handlers behind dispatch: prompt delivery, queue
//! operations, and agent-message delivery.
use super::{
    enqueue_priority, json, oneshot, parse_custom_message, parse_prompt_images, response_success,
    sender_is_child_of, AgentFamilyRelationship, AgentMessagePromptPayload, Lane, QueueCheckpoint,
    QueuePriority, QueuedItem, TurnPolicy, Worker, AGENT_MESSAGE_SOURCE,
    DEFAULT_AGENT_MESSAGE_MAX_PENDING_PER_SESSION, QUEUED_INPUT_SUSPENDED,
};

use serde_json::Value;

use crate::protocol::{response_failure, DaemonResponse};


/// Validate against the loaded branch only. No history reads, global context, or tree movement.
fn parse_focused_reply(
    payload: &Value,
    core: &super::SessionCore,
) -> Result<Option<pa_types::daemon::FocusedThreadReply>, String> {
    if !["clientInputId", "replyToMessageId", "replyContext"].iter().any(|key| payload.get(key).is_some()) {
        return Ok(None);
    }
    let route: pa_types::daemon::FocusedThreadReply = serde_json::from_value(payload.clone())
        .map_err(|_| "Invalid focused thread reply".to_string())?;
    if route.client_input_id.is_empty() || route.client_input_id.len() > 128
        || route.reply_to_message_id.is_empty() || route.reply_to_message_id.len() > 512
        || route.reply_context.len() > 8
        || payload.get("message").and_then(Value::as_str).is_some_and(|text|
            crate::session_commands::parse_prompt_session_command(text).is_some())
        || payload.get("customMessage").is_some_and(|v| !v.is_null())
    {
        return Err("Invalid focused thread reply".to_string());
    }
    let store = core.store.as_ref().ok_or("Session is still initializing")?;
    let branch = store.branch();
    let parent = branch.iter().find(|entry| entry.id == route.reply_to_message_id)
        .and_then(|entry| entry.fields.get("message"));
    if parent.and_then(|message| message.get("role")).and_then(Value::as_str) != Some("user") {
        return Err("Reply parent is not a loaded user message in this session".to_string());
    }
    if branch.iter().any(|entry| entry.fields.get("message")
        .and_then(|message| message.get("clientInputId")).and_then(Value::as_str)
            == Some(route.client_input_id.as_str()))
        || core.steering.iter().chain(&core.follow_up).any(|item| item.focused_reply.as_ref()
            .is_some_and(|reply| reply.client_input_id == route.client_input_id))
    {
        return Err("clientInputId already admitted; do not resend unknown acknowledgements".to_string());
    }
    let mut seen = std::collections::HashSet::new();
    for row in &route.reply_context {
        if !seen.insert(row.id.as_str()) || row.id.is_empty() || row.id.len() > 512 || row.text.len() > 1000 || !matches!(row.role.as_str(), "user" | "assistant")
            || !branch.iter().any(|entry| entry.id == row.id
                && entry.fields.get("message").and_then(|message| message.get("role"))
                    .and_then(Value::as_str) == Some(row.role.as_str()))
        {
            return Err("Reply context must name loaded messages and fit its byte limit".to_string());
        }
    }
    Ok(Some(route))
}

impl Worker {
    pub(crate) async fn handle_prompt(&self, payload: &Value, wait: bool) -> DaemonResponse {
        if let Err(response) = self.require_created("prompt") {
            return response;
        }
        let message = payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        // TS has no empty check; this port still rejects a prompt with neither text nor images.
        let images = parse_prompt_images(payload);
        if message.is_empty() && images.is_empty() {
            return response_failure(None, "prompt", "Prompt cannot be empty", None);
        }
        let streaming_behavior = payload.get("streamingBehavior").and_then(Value::as_str);
        let custom_message = match parse_custom_message(payload.get("customMessage")) {
            Ok(custom_message) => custom_message,
            Err(error) => return response_failure(None, "prompt", &error, None),
        };
        // The reserved child-status kinds are daemon provenance: a prompt row
        // claiming one is always a spoof — answered loudly, never parked.
        if let Some(row) = custom_message.as_ref() {
            if crate::child_status_notices::is_reserved_child_status_custom_type(row) {
                return response_failure(
                    None,
                    "prompt",
                    &crate::child_status_notices::reserved_intake_error(),
                    None,
                );
            }
        }
        {
            let core = self.core.lock().unwrap();
            if let Err(error) = parse_focused_reply(payload, &core) {
                return response_failure(None, "prompt", &error, None);
            }
        }
        // TS daemon prompts map `resumeIfIdle` to
        // `command.streamingBehavior !== undefined`: while the queued-input
        // suspension is set (post `abort`/manual `compact`), a plain prompt
        // on an idle session is rejected with the TS admission error and a
        // prompt carrying `streamingBehavior` resumes the suspension
        // (TS `_prompt`'s `_resumeSessionInputAdmission()` +
        // `_assertSessionActionAdmissionAvailable()` pair).
        {
            let mut core = self.core.lock().unwrap();
            if core.queued_input_suspended && !core.busy {
                if streaming_behavior.is_none() {
                    drop(core);
                    return response_failure(
                        None,
                        if wait { "prompt_and_wait" } else { "prompt" },
                        QUEUED_INPUT_SUSPENDED,
                        None,
                    );
                }
                core.queued_input_suspended = false;
            }
        }
        // The prompt-admission bookkeeping (wave b9): a prompt carrying an
        // admission id registers it worker-side; the queued item carries
        // it so the turn runner clears the admission at settle.
        let admission_id = payload
            .get("admissionId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_string);
        if let Some(admission_id) = &admission_id {
            self.register_prompt_admission(admission_id);
        }
        let (done_tx, done_rx) = oneshot::channel();
        let done = if wait { Some(done_tx) } else { None };
        let (snapshot, queued_behind_work) = {
            let mut core = self.core.lock().unwrap();
            let focused_reply = match parse_focused_reply(payload, &core) {
                Ok(route) => route,
                Err(error) => return response_failure(None, "prompt", &error, None),
            };
            // TS commits before accepting the prompt into its action queue.
            // Hold the queue lock across this transition and enqueue so a
            // cancellation cannot mistake an accepted prompt for waiting.
            if let Some(id) = &admission_id {
                if !self.prompt_admissions.commit(id) {
                    return response_failure(
                        None,
                        if wait { "prompt_and_wait" } else { "prompt" },
                        "Prompt admission was cancelled.",
                        None,
                    );
                }
            }
            // An idle session runs the prompt immediately: the lane is the
            // work hand-off, not a queue, so the projection did not change
            // (TS prompt admission with queueIfBusy=false never queues).
            let queued_behind_work = core.busy;
            let lane = match streaming_behavior {
                Some("steer") => Lane::Steering,
                // An idle session's prompt IS the next run, so it takes the steering
                // lane — otherwise a steering delivery arriving in the same window would
                // jump the prompt's turn.
                Some(_) | None => {
                    if core.busy {
                        Lane::FollowUp
                    } else {
                        Lane::Steering
                    }
                }
            };
            // This RPC command is human-origin only for a plain user row.
            // Caller-supplied custom rows never gain human priority.
            let item = QueuedItem {
                focused_reply,
                priority: if custom_message.is_some() {
                    QueuePriority::Background
                } else {
                    QueuePriority::Human
                },
                preview: None,
                message: message.to_string(),
                custom_message,
                agent_message: None,
                queue_key: None,
                admission_id: admission_id.clone(),
                images: images.clone(),
                done,
                queue_visible: queued_behind_work,
                policy: if queued_behind_work {
                    TurnPolicy::Queued
                } else {
                    TurnPolicy::Direct
                },
                forced_batch: false,
            };
            match lane {
                Lane::Steering => enqueue_priority(&mut core.steering, item),
                Lane::FollowUp => enqueue_priority(&mut core.follow_up, item),
            }
            let snapshot = Self::snapshot_locked(&core);
            (snapshot, queued_behind_work)
        };
        // The admission checkpoint: the admitted prompt is undelivered
        // live work until its turn settles.
        self.checkpoint_queue(QueueCheckpoint::Admitted {
            operation: "prompt_accepted",
        });
        if queued_behind_work {
            let _ = self.emit_action_update(&snapshot);
        }
        self.work_notify.notify_one();
        if !wait {
            return response_success(None, "prompt", payload.get("clientInputId").map(|id| json!({"clientInputId": id, "accepted": true})));
        }
        match done_rx.await {
            Ok(settle) => match settle.wire_error() {
                None => response_success(None, "prompt_and_wait", None),
                Some(error) => response_failure(None, "prompt_and_wait", &error, None),
            },
            Err(_) => response_failure(None, "prompt_and_wait", "Prompt did not complete", None),
        }
    }

    pub(crate) fn handle_queue(&self, payload: &Value, lane: Lane) -> DaemonResponse {
        if let Err(response) = self.require_created(lane.as_str()) {
            return response;
        }
        // These commands are resume sites: an admitted turn with
        // `wake: "immediate"` resumes the suspension.
        let message = payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let custom_message = match parse_custom_message(payload.get("customMessage")) {
            Ok(custom_message) => custom_message,
            Err(error) => return response_failure(None, lane.as_str(), &error, None),
        };
        // The reserved child-status kinds are daemon provenance: a caller-supplied
        // row claiming one is answered loudly, never parked. The daemon's own
        // notice injection rides this command with the one-shot capability.
        if let Some(row) = custom_message.as_ref() {
            if crate::child_status_notices::is_reserved_child_status_custom_type(row) {
                let minted = crate::child_status_notices::consume(
                    payload.get("rlmNoticeNonce").and_then(Value::as_str),
                );
                if !minted {
                    return response_failure(
                        None,
                        lane.as_str(),
                        &crate::child_status_notices::reserved_intake_error(),
                        None,
                    );
                }
            }
        }
        let mut core = self.core.lock().unwrap();
        let images = parse_prompt_images(payload);
        let focused_reply = match parse_focused_reply(payload, &core) {
            Ok(route) => route,
            Err(error) => return response_failure(None, lane.as_str(), &error, None),
        };
        core.queued_input_suspended = false;
        let item = QueuedItem {
            focused_reply,
            priority: if custom_message.is_some() {
                QueuePriority::Background
            } else {
                QueuePriority::Human
            },
            preview: None,
            message: message.to_string(),
            custom_message,
            agent_message: None,
            queue_key: None,
            admission_id: None,
            images,
            done: None,
            queue_visible: true,
            policy: TurnPolicy::Queued,
            forced_batch: false,
        };
        match lane {
            Lane::Steering => enqueue_priority(&mut core.steering, item),
            Lane::FollowUp => enqueue_priority(&mut core.follow_up, item),
        }
        let snapshot = Self::snapshot_locked(&core);
        drop(core);
        // The queue-write checkpoint: an undelivered lane is live work. The
        // operation names are TS's journal strings, so the journals stay
        // comparable record-for-record.
        let queued_operation = match lane {
            Lane::Steering => "steer_queued",
            Lane::FollowUp => "follow_up_queued",
        };
        self.checkpoint_queue(QueueCheckpoint::Admitted {
            operation: queued_operation,
        });
        let _ = self.emit_action_update(&snapshot);
        self.work_notify.notify_one();
        let command = if lane == Lane::Steering {
            "steer"
        } else {
            "follow_up"
        };
        let mut receipt = json!({ "queued": true });
        if let Some(id) = payload.get("clientInputId") { receipt["clientInputId"] = id.clone(); }
        response_success(None, command, Some(receipt))
    }

    /// Agent-to-agent delivery: render the `[agent-message from ...]` prompt and queue
    /// it on the requested lane with the `agent_message` custom row (the turn renders
    /// the collapsed card). Answers `queued` when running, else `delivered`.
    pub(crate) fn handle_worker_deliver_message(&self, payload: &Value) -> DaemonResponse {
        if let Err(response) = self.require_created("worker_deliver_message") {
            return response;
        }
        let message = payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if let Err(error) =
            pa_core::session_engine::agent_messaging::normalize_agent_session_message(message)
        {
            return response_failure(None, "worker_deliver_message", &error.to_string(), None);
        }
        // The paused gate (TS `sendAgentSessionMessage` refuses with the
        // same error while `agent_messages_pause` holds the flag).
        if let Err(response) = self.refuse_delivery_if_paused() {
            return response;
        }
        // On a suspended idle session the delivery is rejected with the same
        // admission error as a plain prompt; only the busy carve-out queues it
        // parked.
        {
            let core = self.core.lock().unwrap();
            if core.queued_input_suspended && !core.busy && !core.compacting {
                drop(core);
                return response_failure(
                    None,
                    "worker_deliver_message",
                    QUEUED_INPUT_SUSPENDED,
                    None,
                );
            }
        }
        let sender = payload.get("sender").cloned().unwrap_or(Value::Null);
        // A delivery from one of this session's RLM children counts as the
        // child's reply: the settle watcher withholds the no-reply notice.
        if let Some(child) = sender
            .get("activeSessionId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            self.engine.mark_child_reply(child);
        }
        // Sender label precedence (TS `createAgentSessionMessagePrompt`):
        // session name, session id, active session id, client id.
        let sender_name = ["sessionName", "sessionId", "activeSessionId", "clientId"]
            .iter()
            .find_map(|key| sender.get(*key).and_then(Value::as_str))
            .unwrap_or("unknown")
            .to_string();
        // The relationship label derives from the sender's durable parent edge,
        // never the runtime kind alone: a subagent spawned by a DIFFERENT parent
        // is not this session's child.
        let from_relationship = {
            let core = self
                .core
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            sender_is_child_of(&sender, &core).then_some(AgentFamilyRelationship::Child)
        };
        let prompt = pa_core::session_engine::agent_messaging::create_agent_session_message_prompt(
            &AgentMessagePromptPayload {
                message: message.to_string(),
                sender_name,
                from_relationship,
            },
        );
        let lane = if payload.get("deliveryMode").and_then(Value::as_str) == Some("follow_up") {
            Lane::FollowUp
        } else {
            Lane::Steering
        };
        let (id, queued, snapshot, target) = {
            let (mut core, inputs) = self.summary_inputs();
            let pending = core.steering.len() + core.follow_up.len();
            if let Err(error) =
                pa_core::session_engine::agent_messaging::assert_agent_message_queue_capacity(
                    pending,
                    DEFAULT_AGENT_MESSAGE_MAX_PENDING_PER_SESSION,
                )
            {
                drop(core);
                return response_failure(None, "worker_deliver_message", &error.to_string(), None);
            }
            let id = pa_core::session_engine::agent_messaging::create_agent_session_message_id();
            let queued = core.busy;
            let summary = self.summary_locked(&core, inputs);
            // The receiving session's endpoint: the receipt's `target`
            // and the delivered row's `details.target` share the one
            // shape.
            let mut target = json!({
                "activeSessionId": summary.active_session_id.clone().unwrap_or_default(),
                "sessionId": summary.session_id,
                "runtimeKind": summary
                    .runtime_kind
                    .clone()
                    .unwrap_or_else(|| "top-level".to_string()),
            });
            if let Some(name) = summary.session_name.filter(|name| !name.is_empty()) {
                target["sessionName"] = json!(name);
            }
            // The queued turn carries the `agent_message` row so the transcript renders
            // the collapsed card, while the row's `content` IS the rendered prompt —
            // the model context stays byte-identical to the plain-prompt delivery.
            let custom_message =
                pa_core::session_engine::agent_messaging::create_agent_session_message_row(
                    &pa_core::session_engine::agent_messaging::AgentSessionMessageRowPayload {
                        id: &id,
                        prompt: &prompt,
                        message,
                        from: &sender,
                        from_relationship,
                        target: Some(&target),
                        timestamp: crate::util::now_ms(),
                    },
                );
            let item = QueuedItem {
                focused_reply: None,
                priority: QueuePriority::Background,
                // The labeled queue-strip row: "Agent message
                // received: <details.message>".
                preview: Some(format!(
                    "{}: {message}",
                    pa_core::session_engine::agent_messaging::AGENT_MESSAGE_RECEIVED_PREVIEW_LABEL
                )),
                message: prompt,
                custom_message: Some(custom_message),
                // The agent-message marker: `agent_messages_clear` /
                // `agent_messages_pause` remove exactly these items.
                agent_message: Some(message.to_string()),
                queue_key: None,
                admission_id: None,
                images: Vec::new(),
                done: None,
                queue_visible: true,
                policy: TurnPolicy::Injected,
                forced_batch: false,
            };
            match lane {
                Lane::Steering => enqueue_priority(&mut core.steering, item),
                Lane::FollowUp => enqueue_priority(&mut core.follow_up, item),
            }
            let snapshot = Self::snapshot_locked(&core);
            (id, queued, snapshot, target)
        };
        // The queued agent message is admitted live work — a restart must revive
        // the worker to deliver it (no client reopens the session).
        self.checkpoint_queue(QueueCheckpoint::Admitted {
            operation: match lane {
                Lane::Steering => "steer_queued",
                Lane::FollowUp => "follow_up_queued",
            },
        });
        let _ = self.emit_action_update(&snapshot);
        self.work_notify.notify_one();
        let timestamp = crate::util::now_iso();
        let mut receipt = json!({
            "id": id,
            "source": AGENT_MESSAGE_SOURCE,
            "target": target,
            "message": message,
            // TS receipts always report `steer`; the follow-up lane is the
            // Rust extension for queue-behind-current-work delivery.
            "deliveryMode": if lane == Lane::FollowUp { "follow_up" } else { "steer" },
        });
        if queued {
            receipt["deliveryStatus"] = json!("queued");
            receipt["queuedAt"] = json!(timestamp);
        } else {
            receipt["deliveryStatus"] = json!("delivered");
            receipt["deliveredAt"] = json!(timestamp);
        }
        if !sender.is_null() {
            receipt["from"] = json!(sender);
        }
        response_success(None, "worker_deliver_message", Some(receipt))
    }
}

#[cfg(test)]
mod focused_reply_tests {
    use super::*;

    fn core_and_payload() -> (super::super::SessionCore, Value) {
        let mut store = crate::session_store::SessionFile::create("/tmp", None, 0);
        let parent = store.append_message(&json!({"role":"user","content":"parent","timestamp":1}));
        let payload = json!({"message":"same text", "clientInputId":"input-a", "replyToMessageId":parent,
            "replyContext":[{"id":parent,"role":"user","text":"parent"}]});
        (super::super::SessionCore::test_core(Some(store), "/tmp".to_string()), payload)
    }

    #[test]
    fn reply_validation_rejects_cross_session_and_oversized_utf8() {
        let (core, mut payload) = core_and_payload();
        assert!(parse_focused_reply(&payload, &core).unwrap().is_some());
        payload["replyContext"][0]["text"] = json!("é".repeat(501));
        assert!(parse_focused_reply(&payload, &core).is_err());
        payload["replyContext"] = json!([]);
        payload["replyToMessageId"] = json!("other-session-entry");
        assert!(parse_focused_reply(&payload, &core).is_err());
        assert!(parse_focused_reply(&json!({"message":"ordinary"}), &core).unwrap().is_none());
    }

    #[tokio::test]
    async fn every_human_input_mode_keeps_the_exact_route() {
        for command in ["prompt", "steer", "follow_up"] {
            let worker = super::super::tests::created_dispatch_worker().await;
            let (core, payload) = core_and_payload();
            worker.core.lock().unwrap().store = core.store;
            let response = worker.dispatch(command, &payload).await;
            assert!(response.success, "{command}: {response:?}");
            let core = worker.core.lock().unwrap();
            let item = core.steering.front().or_else(|| core.follow_up.front()).unwrap();
            assert_eq!(item.message, "same text");
            let route = item.focused_reply.as_ref().unwrap();
            assert_eq!(route.client_input_id, "input-a");
            assert_eq!(route.reply_to_message_id, payload["replyToMessageId"].as_str().unwrap());
        }
    }

    #[test]
    fn historical_native_ids_are_entry_ids_without_rewriting_payloads() {
        let (core, payload) = core_and_payload();
        let store = core.store.as_ref().unwrap();
        let parent = payload["replyToMessageId"].as_str().unwrap();
        assert_eq!(store.messages()[0]["nativeMessageId"], parent);
        assert_eq!(store.messages_before(1)[0]["nativeMessageId"], parent);
        assert_eq!(store.windowed_messages(1).messages[0]["nativeMessageId"], parent);
        assert!(store.entry(parent).unwrap().fields["message"].get("nativeMessageId").is_none());
    }
}
