//! The queue-lane command surface: `mutate_queued_message`, `resume_queue`,
//! and the queued-input pair. Wire contract: `get_queue` and the
//! `session_action_update` events expose each lane's message previews,
//! and a mutation addresses one preview by `lane` + `index` + `expectedText`.
//! `list_queued_inputs` and `mutate_queued_input` address a queued human input by
//! its stable id instead; a row already taken for delivery answers `already_sent`.

use serde_json::Value;

use crate::protocol::{response_failure, response_success, DaemonResponse};
use crate::worker::Lane;
use crate::worker::{parse_prompt_images, QueueCheckpoint, QueuedItem, TurnSettle, Worker};

/// The wire item of one queued human input.
fn queued_input_view(item: &QueuedItem, lane: Lane) -> Option<Value> {
    let id = item.input_id.as_ref().filter(|_| item.queue_visible)?;
    Some(serde_json::json!({
        "id": id,
        "text": item.message,
        "deliveryMode": match lane {
            Lane::Steering => "steer",
            Lane::FollowUp => "follow_up",
        },
        "imageCount": item.images.len(),
    }))
}

/// Locate one queued human input by id across both lanes.
fn find_queued_input(core: &crate::worker::SessionCore, id: &str) -> Option<(Lane, usize)> {
    let position = |lane: &std::collections::VecDeque<QueuedItem>| {
        lane.iter()
            .position(|item| item.queue_visible && item.input_id.as_deref() == Some(id))
    };
    position(&core.steering)
        .map(|index| (Lane::Steering, index))
        .or_else(|| position(&core.follow_up).map(|index| (Lane::FollowUp, index)))
}

/// The wire lane names: `"steering"` and `"followUp"`.
fn wire_lane(value: Option<&Value>) -> Option<Lane> {
    match value.and_then(Value::as_str) {
        Some("steering") => Some(Lane::Steering),
        Some("followUp") => Some(Lane::FollowUp),
        _ => None,
    }
}

impl Worker {
    /// `mutate_queued_message { lane, index, expectedText, mutation }`:
    /// apply one delete/move/replace against the queue preview.
    /// Rejections are statuses, not errors; only a malformed request fails the command.
    pub(crate) fn handle_mutate_queued_message(&self, payload: &Value) -> DaemonResponse {
        // The delete error the rejected waiter sees; a prompt_and_wait
        // caller surfaces it as the command failure.
        const DELETED: &str = crate::worker::QUEUED_PROMPT_DELETED;
        if let Err(response) = self.require_created("mutate_queued_message") {
            return response;
        }
        let Some(lane) = wire_lane(payload.get("lane")) else {
            return response_failure(
                None,
                "mutate_queued_message",
                "mutate_queued_message requires lane \"steering\" or \"followUp\"",
                None,
            );
        };
        let Some(index) = payload.get("index").and_then(Value::as_u64) else {
            return response_failure(
                None,
                "mutate_queued_message",
                "mutate_queued_message requires an index",
                None,
            );
        };
        let expected = payload
            .get("expectedText")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(mutation) = payload.get("mutation") else {
            return response_failure(
                None,
                "mutate_queued_message",
                "mutate_queued_message requires a mutation",
                None,
            );
        };
        let mutation_type = mutation
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !matches!(mutation_type, "delete" | "move" | "replace") {
            return response_failure(
                None,
                "mutate_queued_message",
                "mutate_queued_message requires mutation.type \"delete\", \"move\", or \"replace\"",
                None,
            );
        }
        let index = index as usize;
        let (status, queue_changed): (&'static str, bool) = {
            let mut core = self.core.lock().unwrap();
            let status = match lane {
                Lane::Steering => mutate_lane(
                    &mut core.steering,
                    index,
                    expected,
                    mutation,
                    mutation_type,
                    DELETED,
                ),
                Lane::FollowUp => mutate_lane(
                    &mut core.follow_up,
                    index,
                    expected,
                    mutation,
                    mutation_type,
                    DELETED,
                ),
            };
            if status == "applied" {
                // A replace onto another lane is an arrival there: the item
                // keeps its priority and enqueues by it. The non-overlapping
                // direction pairs only - `target != lane` holds for every reached arm.
                if mutation_type == "replace" {
                    if let Some(target) =
                        wire_lane(mutation.get("lane")).filter(|target| *target != lane)
                    {
                        let item = match (lane, target) {
                            (Lane::Steering, Lane::FollowUp) => core.steering.remove(index),
                            (Lane::FollowUp, Lane::Steering) => core.follow_up.remove(index),
                            _ => None,
                        };
                        if let Some(item) = item {
                            let target_lane = match target {
                                Lane::Steering => &mut core.steering,
                                Lane::FollowUp => &mut core.follow_up,
                            };
                            crate::worker::enqueue_priority(target_lane, item);
                        }
                    }
                }
                (status, true)
            } else {
                (status, false)
            }
        };
        // Every applied mutation resumes the suspension (TS's delete and
        // replace arms call resumeQueuedWork).
        if status == "applied" {
            self.resume_queued_input();
        }
        let response = response_success(
            None,
            "mutate_queued_message",
            Some(serde_json::json!({ "status": status })),
        );
        if !queue_changed {
            return response;
        }
        // Same post-mutation flow as the admission paths: persist the lanes,
        // push the projection, and wake the turn runner.
        let core = self.core.lock().unwrap();
        let snapshot = Self::snapshot_locked(&core);
        drop(core);
        // The edit refreshed the lanes: the verdict follows them (a delete
        // of the last queued item settles the session back to idle).
        self.checkpoint_queue(crate::worker::QueueCheckpoint::Settle {
            operation: "queue_mutated",
        });
        let _ = self.emit_action_update(&snapshot);
        if mutation_type != "move" {
            self.work_notify.notify_one();
        }
        response
    }

    /// `list_queued_inputs`: the queued human inputs in delivery-lane order.
    pub(crate) fn handle_list_queued_inputs(&self) -> DaemonResponse {
        if let Err(response) = self.require_created("list_queued_inputs") {
            return response;
        }
        let core = self.core.lock().unwrap();
        let items: Vec<Value> = core
            .steering
            .iter()
            .filter_map(|item| queued_input_view(item, Lane::Steering))
            .chain(
                core.follow_up
                    .iter()
                    .filter_map(|item| queued_input_view(item, Lane::FollowUp)),
            )
            .collect();
        response_success(
            None,
            "list_queued_inputs",
            Some(serde_json::json!({ "items": items })),
        )
    }

    /// `mutate_queued_input { inputId, action: "remove" | "edit" | "steer", text? }`: one
    /// atomic change to a queued human input under the core lock, so it can never race
    /// the runner taking the row. A row no longer queued answers `already_sent`.
    /// Remove and edit leave scheduling alone; steer moves the row to the steering lane,
    /// which stops the running turn at its next boundary like a `steer` command.
    pub(crate) fn handle_mutate_queued_input(&self, payload: &Value) -> DaemonResponse {
        const COMMAND: &str = "mutate_queued_input";
        if let Err(response) = self.require_created(COMMAND) {
            return response;
        }
        let Some(id) = payload.get("inputId").and_then(Value::as_str) else {
            return response_failure(
                None,
                COMMAND,
                "mutate_queued_input requires an inputId",
                None,
            );
        };
        let action = payload
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !matches!(action, "remove" | "edit" | "steer") {
            return response_failure(
                None,
                COMMAND,
                "mutate_queued_input requires action \"remove\", \"edit\", or \"steer\"",
                None,
            );
        }
        let text = payload.get("text").and_then(Value::as_str);
        if action == "edit" && text.is_none() {
            return response_failure(
                None,
                COMMAND,
                "mutate_queued_input edit requires text",
                None,
            );
        }
        let (item, snapshot) = {
            let mut core = self.core.lock().unwrap();
            let Some((lane, index)) = find_queued_input(&core, id) else {
                return response_success(
                    None,
                    COMMAND,
                    Some(serde_json::json!({ "status": "already_sent" })),
                );
            };
            let lane_items = match lane {
                Lane::Steering => &mut core.steering,
                Lane::FollowUp => &mut core.follow_up,
            };
            let item = match action {
                "remove" => {
                    let mut item = lane_items.remove(index).expect("located row");
                    if let Some(done) = item.done.take() {
                        let _ = done.send(TurnSettle::Withdrawn(
                            crate::worker::QUEUED_PROMPT_DELETED.to_string(),
                        ));
                    }
                    None
                }
                "edit" => {
                    let item = &mut lane_items[index];
                    let text = text.unwrap_or_default();
                    if text.trim().is_empty() && item.images.is_empty() {
                        return response_failure(
                            None,
                            COMMAND,
                            "A queued message cannot be empty",
                            None,
                        );
                    }
                    if item.focused_reply.is_some()
                        && crate::session_commands::parse_prompt_session_command(text).is_some()
                    {
                        return response_failure(
                            None,
                            COMMAND,
                            "A thread reply cannot be a session command",
                            None,
                        );
                    }
                    item.message = text.to_string();
                    queued_input_view(item, lane)
                }
                _ => {
                    if lane == Lane::FollowUp {
                        let item = core.follow_up.remove(index).expect("located row");
                        crate::worker::enqueue_priority(&mut core.steering, item);
                    }
                    core.queued_input_suspended = false;
                    find_queued_input(&core, id).and_then(|(lane, index)| {
                        let items = match lane {
                            Lane::Steering => &core.steering,
                            Lane::FollowUp => &core.follow_up,
                        };
                        queued_input_view(&items[index], lane)
                    })
                }
            };
            (item, Self::snapshot_locked(&core))
        };
        self.checkpoint_queue(if action == "steer" {
            QueueCheckpoint::Admitted {
                operation: "steer_queued",
            }
        } else {
            QueueCheckpoint::Settle {
                operation: "queue_mutated",
            }
        });
        let _ = self.emit_action_update(&snapshot);
        if action == "steer" {
            self.work_notify.notify_one();
        }
        let mut data = serde_json::json!({ "status": "applied" });
        if let Some(item) = item {
            data["item"] = item;
        }
        response_success(None, COMMAND, Some(data))
    }

    /// `resume_queue`: the queue's queued work resumes (the turn runner
    /// drains the lanes when idle); the failure string is TS-verbatim for the empty queue.
    pub(crate) fn handle_resume_queue(&self) -> DaemonResponse {
        if let Err(response) = self.require_created("resume_queue") {
            return response;
        }
        // The suspension clears first, so `resume_queue` is a resume
        // site even when it answers "No queued work to resume".
        self.resume_queued_input();
        let has_queued_work = {
            let core = self.core.lock().unwrap();
            !core.steering.is_empty() || !core.follow_up.is_empty()
        };
        if !has_queued_work {
            return response_failure(None, "resume_queue", "No queued work to resume", None);
        }
        self.work_notify.notify_one();
        response_success(None, "resume_queue", None)
    }
}

/// Apply one mutation to a lane; the caller owns the cross-lane move a
/// replace-with-lane-change implies.
fn mutate_lane(
    lane: &mut std::collections::VecDeque<crate::worker::QueuedItem>,
    index: usize,
    expected: &str,
    mutation: &Value,
    mutation_type: &str,
    deleted_message: &str,
) -> &'static str {
    let Some(item) = lane.get_mut(index) else {
        return "rejected";
    };
    // The visible preview is the row the client saw in
    // `get_queue`/`session_action_update`: the labeled preview when
    // present, else the message text.
    if item.preview.as_deref().unwrap_or(item.message.as_str()) != expected {
        return "rejected";
    }
    match mutation_type {
        "delete" => {
            if let Some(mut item) = lane.remove(index) {
                if let Some(done) = item.done.take() {
                    let _ = done.send(TurnSettle::Withdrawn(deleted_message.to_string()));
                }
            }
        }
        "move" => {
            let direction = mutation
                .get("direction")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let target = index as i64 + direction;
            if target < 0 || target as usize >= lane.len() || direction == 0 {
                return "rejected";
            }
            lane.swap(index, target as usize);
        }
        "replace" => {
            // A replace is rejected when the turn's primary delivery record
            // is not a plain user message: editing only `message` would
            // leave the turn delivering the old injected row.
            if item.custom_message.is_some() || item.agent_message.is_some() {
                return "rejected";
            }
            let Some(text) = mutation.get("text").and_then(Value::as_str) else {
                return "rejected";
            };
            if item.focused_reply.is_some()
                && crate::session_commands::parse_prompt_session_command(text).is_some()
            {
                return "rejected";
            }
            item.message = text.to_string();
            // The edited row loses its labeled preview: the client's
            // own text becomes the preview.
            item.preview = None;
            // `images` present clears or replaces the attachments; absent keeps them.
            if mutation.get("images").is_some() {
                item.images = parse_prompt_images(mutation);
            }
        }
        _ => return "rejected",
    }
    "applied"
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    async fn created_worker() -> Arc<Worker> {
        let dir = std::env::temp_dir().join(format!("pa-worker-qc-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = crate::worker::WorkerConfig {
            socket_path: dir.join("worker.sock"),
            supervisor_socket_path: std::path::PathBuf::new(),
            token: "token".to_string(),
            worker_instance_id: String::new(),
            active_session_id: "queue-session".to_string(),
            agent_dir: dir.join("agent"),
            recovery_journal_path: dir.join("recovery.jsonl"),
            telemetry_disabled: None,
            script: Some(json!({ "responses": ["ack"] })),
        };
        let worker = Arc::new(Worker::new(config, None));
        let created = worker
            .dispatch(
                "create",
                &json!({ "noSession": true, "cwd": "/tmp", "name": "queued" }),
            )
            .await;
        assert!(created.success, "create failed: {created:?}");
        worker
    }

    fn lane_texts(worker: &Worker, lane: Lane) -> Vec<String> {
        let core = worker.core.lock().unwrap();
        match lane {
            Lane::Steering => &core.steering,
            Lane::FollowUp => &core.follow_up,
        }
        .iter()
        .map(|item| item.message.clone())
        .collect()
    }

    async fn queued_inputs(worker: &Worker) -> Vec<Value> {
        let listed = worker.dispatch("list_queued_inputs", &json!({})).await;
        assert!(listed.success, "list failed: {listed:?}");
        listed.data.expect("list data")["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
    }

    async fn mutate_input(worker: &Worker, payload: Value) -> Value {
        let response = worker.dispatch("mutate_queued_input", &payload).await;
        assert!(response.success, "mutate failed: {response:?}");
        response.data.expect("mutate data")
    }

    async fn queue_human(worker: &Worker, command: &str, message: &str) -> String {
        let admitted = worker
            .dispatch(
                command,
                &json!({ "message": message, "images": [
                { "type": "image", "data": "AAAA", "mimeType": "image/png" }
            ] }),
            )
            .await;
        assert!(admitted.success, "{command}: {admitted:?}");
        admitted.data.expect("receipt")["inputId"]
            .as_str()
            .expect("human input id")
            .to_string()
    }

    /// Identical texts stay distinct rows: each id addresses only its own row.
    #[tokio::test]
    async fn queued_inputs_have_distinct_ids_and_remove_only_the_addressed_row() {
        let worker = created_worker().await;
        worker.core.lock().unwrap().busy = true;
        let first = queue_human(&worker, "follow_up", "same").await;
        let second = queue_human(&worker, "follow_up", "same").await;
        assert_ne!(first, second);
        let listed = queued_inputs(&worker).await;
        assert_eq!(
            listed,
            vec![
                json!({ "id": first, "text": "same", "deliveryMode": "follow_up", "imageCount": 1 }),
                json!({ "id": second, "text": "same", "deliveryMode": "follow_up", "imageCount": 1 }),
            ]
        );
        let removed = mutate_input(&worker, json!({ "inputId": second, "action": "remove" })).await;
        assert_eq!(removed, json!({ "status": "applied" }));
        let listed = queued_inputs(&worker).await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0]["id"], json!(first));
        let again = mutate_input(&worker, json!({ "inputId": second, "action": "remove" })).await;
        assert_eq!(again, json!({ "status": "already_sent" }));
    }

    /// Edit keeps the id, images and lane; it does not wake or resume the queue.
    #[tokio::test]
    async fn edit_replaces_text_and_keeps_identity_and_images() {
        let worker = created_worker().await;
        {
            let mut core = worker.core.lock().unwrap();
            core.busy = true;
        }
        let id = queue_human(&worker, "follow_up", "draft").await;
        worker.core.lock().unwrap().queued_input_suspended = true;
        let edited = mutate_input(
            &worker,
            json!({ "inputId": id, "action": "edit", "text": "final words" }),
        )
        .await;
        assert_eq!(
            edited,
            json!({
                "status": "applied",
                "item": { "id": id, "text": "final words", "deliveryMode": "follow_up", "imageCount": 1 },
            })
        );
        {
            let core = worker.core.lock().unwrap();
            assert!(
                core.queued_input_suspended,
                "edit must not resume the queue"
            );
            let item = core.follow_up.front().expect("edited row");
            assert_eq!(item.message, "final words");
            assert_eq!(item.images.len(), 1);
        }
        let blank = worker
            .dispatch(
                "mutate_queued_input",
                &json!({ "inputId": "qi_missing", "action": "edit" }),
            )
            .await;
        assert!(!blank.success, "edit without text is malformed");
    }

    /// Steer moves the same row into the steering lane ahead of background steering,
    /// like a fresh `steer`, and clears a queued-input suspension.
    #[tokio::test]
    async fn steer_moves_the_follow_up_into_the_steering_lane() {
        let worker = created_worker().await;
        worker.core.lock().unwrap().busy = true;
        let delivered = worker
            .dispatch(
                "worker_deliver_message",
                &json!({
                    "targetActiveSessionId": "queue-session",
                    "message": "the research is done",
                    "sender": { "activeSessionId": "source-session" },
                    "deliveryMode": "steer",
                }),
            )
            .await;
        assert!(delivered.success, "deliver failed: {delivered:?}");
        let id = queue_human(&worker, "follow_up", "do this now").await;
        worker.core.lock().unwrap().queued_input_suspended = true;
        let steered = mutate_input(&worker, json!({ "inputId": id, "action": "steer" })).await;
        assert_eq!(steered["status"], "applied");
        assert_eq!(steered["item"]["deliveryMode"], "steer");
        assert_eq!(steered["item"]["id"], json!(id));
        assert_eq!(lane_texts(&worker, Lane::FollowUp), Vec::<String>::new());
        let core = worker.core.lock().unwrap();
        assert!(!core.queued_input_suspended);
        assert_eq!(
            core.steering.front().unwrap().input_id.as_deref(),
            Some(id.as_str())
        );
        assert_eq!(core.steering.len(), 2);
    }

    /// A row the runner already took answers `already_sent` for every action.
    #[tokio::test]
    async fn a_dequeued_input_answers_already_sent() {
        let worker = created_worker().await;
        worker.core.lock().unwrap().busy = true;
        let id = queue_human(&worker, "steer", "taken").await;
        let taken = worker
            .core
            .lock()
            .unwrap()
            .steering
            .pop_front()
            .expect("queued row");
        assert_eq!(taken.input_id.as_deref(), Some(id.as_str()));
        for action in [
            json!({ "inputId": id, "action": "remove" }),
            json!({ "inputId": id, "action": "edit", "text": "late" }),
            json!({ "inputId": id, "action": "steer" }),
        ] {
            assert_eq!(
                mutate_input(&worker, action).await,
                json!({ "status": "already_sent" })
            );
        }
    }

    /// Agent, notice and caller-custom rows get no id and never list.
    #[tokio::test]
    async fn only_plain_human_rows_are_queued_inputs() {
        let worker = created_worker().await;
        worker.core.lock().unwrap().busy = true;
        worker
            .dispatch(
                "worker_deliver_message",
                &json!({
                    "targetActiveSessionId": "queue-session",
                    "message": "agent text",
                    "sender": { "activeSessionId": "source-session" },
                }),
            )
            .await;
        let custom = worker
            .dispatch(
                "follow_up",
                &json!({
                    "message": "notice",
                    "customMessage": { "role": "custom", "customType": "bash_notice", "content": "x" },
                }),
            )
            .await;
        assert!(custom.success, "custom follow-up: {custom:?}");
        assert!(custom.data.expect("receipt").get("inputId").is_none());
        let prompt = worker
            .dispatch("prompt", &json!({ "message": "busy prompt" }))
            .await;
        let prompt_id = prompt.data.expect("prompt receipt")["inputId"].clone();
        let listed = queued_inputs(&worker).await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0]["id"], prompt_id);
        assert_eq!(listed[0]["text"], "busy prompt");
    }

    /// The id survives the recovery record; a pre-field record restores without one.
    #[test]
    fn queued_input_id_round_trips_through_the_recovery_record() {
        let record: crate::journal::WorkerQueueItemRecord =
            serde_json::from_value(json!({ "message": "old" })).unwrap();
        assert_eq!(record.input_id, None);
        let record = crate::journal::WorkerQueueItemRecord {
            input_id: Some("qi_1".to_string()),
            ..record
        };
        let wire = serde_json::to_value(&record).unwrap();
        assert_eq!(wire["input_id"], "qi_1");
        let back: crate::journal::WorkerQueueItemRecord = serde_json::from_value(wire).unwrap();
        assert_eq!(back.input_id.as_deref(), Some("qi_1"));
    }

    #[tokio::test]
    async fn a_labeled_preview_row_is_addressed_and_edited_by_its_preview() {
        let worker = created_worker().await;
        {
            let mut core = worker.core.lock().unwrap();
            core.steering.push_back(crate::worker::QueuedItem {
                focused_reply: None,
                priority: crate::worker::QueuePriority::Background,
                message: "[heartbeat: every 10m run#0]\n\nnudge the mission".to_string(),
                preview: Some(
                    "Heartbeat prompt: [heartbeat: every 10m run#0]\n\nnudge the mission"
                        .to_string(),
                ),
                custom_message: None,
                agent_message: None,
                queue_key: None,
                admission_id: None,
                images: Vec::new(),
                done: None,
                queue_visible: true,
                policy: crate::worker::TurnPolicy::Injected,
                forced_batch: false,
                input_id: None,
            });
        }
        let queue = worker.dispatch("get_queue", &json!({})).await;
        assert!(queue.success);
        let data = queue.data.expect("queue data");
        assert_eq!(
            data["steering"][0],
            "Heartbeat prompt: [heartbeat: every 10m run#0]\n\nnudge the mission"
        );
        // The preview text addresses the row; the edited text becomes
        // the message and the label drops.
        let mutate = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "lane": "steering",
                    "index": 0,
                    "expectedText": "Heartbeat prompt: [heartbeat: every 10m run#0]\n\nnudge the mission",
                    "mutation": { "type": "replace", "text": "edited while parked" },
                }),
            )
            .await;
        assert!(mutate.success, "mutate failed: {mutate:?}");
        assert_eq!(mutate.data.expect("mutate data")["status"], "applied");
        assert_eq!(lane_texts(&worker, Lane::Steering), ["edited while parked"]);
        let queue = worker.dispatch("get_queue", &json!({})).await;
        let data = queue.data.expect("queue data");
        assert_eq!(data["steering"][0], "edited while parked");
    }

    /// A parked heartbeat (injected custom row) answers `rejected` on
    /// replace so an edit can never report `applied` while the turn
    /// still delivers the old injected content; delete still applies.
    #[tokio::test]
    async fn an_injected_heartbeat_row_rejects_replace_but_deletes() {
        let worker = created_worker().await;
        {
            let mut core = worker.core.lock().unwrap();
            core.steering.push_back(crate::worker::QueuedItem {
                focused_reply: None,
                priority: crate::worker::QueuePriority::Background,
                message: "[heartbeat: every 10m run#0]\n\nnudge the mission".to_string(),
                preview: Some(
                    "Heartbeat prompt: [heartbeat: every 10m run#0]\n\nnudge the mission"
                        .to_string(),
                ),
                custom_message: Some(json!({
                    "role": "custom",
                    "customType": "heartbeat_prompt",
                    "content": "[heartbeat: every 10m run#0]\n\nnudge the mission",
                    "display": true,
                    "details": { "jobId": "hb-1" },
                })),
                agent_message: None,
                queue_key: Some("heartbeat:hb-1".to_string()),
                admission_id: None,
                images: Vec::new(),
                done: None,
                queue_visible: true,
                policy: crate::worker::TurnPolicy::Injected,
                forced_batch: false,
                input_id: None,
            });
        }
        let expected = "Heartbeat prompt: [heartbeat: every 10m run#0]\n\nnudge the mission";
        let replace = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "lane": "steering",
                    "index": 0,
                    "expectedText": expected,
                    "mutation": { "type": "replace", "text": "edited while parked" },
                }),
            )
            .await;
        assert!(replace.success, "mutate failed: {replace:?}");
        assert_eq!(replace.data.expect("replace data")["status"], "rejected");
        // The row is untouched: the parked heartbeat keeps its injected
        // delivery and its labeled preview.
        {
            let core = worker.core.lock().unwrap();
            let item = core.steering.front().expect("the parked row");
            assert!(item.custom_message.is_some());
            assert!(item.preview.is_some());
        }
        let delete = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "lane": "steering",
                    "index": 0,
                    "expectedText": expected,
                    "mutation": { "type": "delete" },
                }),
            )
            .await;
        assert!(delete.success, "delete failed: {delete:?}");
        assert_eq!(delete.data.expect("delete data")["status"], "applied");
        assert_eq!(lane_texts(&worker, Lane::Steering), Vec::<String>::new());
    }

    #[tokio::test]
    async fn mutate_delete_moves_and_replaces_match_ts_status_wire() {
        let worker = created_worker().await;
        worker
            .dispatch(
                "steer",
                &json!({ "activeSessionId": "queue-session", "message": "one" }),
            )
            .await;
        worker
            .dispatch(
                "steer",
                &json!({ "activeSessionId": "queue-session", "message": "two" }),
            )
            .await;

        let response = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "steering",
                    "index": 0,
                    "expectedText": "not the preview",
                    "mutation": { "type": "delete" },
                }),
            )
            .await;
        assert!(response.success);
        assert_eq!(response.data, Some(json!({ "status": "rejected" })));

        let response = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "steering",
                    "index": 0,
                    "expectedText": "one",
                    "mutation": { "type": "delete" },
                }),
            )
            .await;
        assert_eq!(response.data, Some(json!({ "status": "applied" })));
        assert_eq!(lane_texts(&worker, Lane::Steering), vec!["two"]);

        worker
            .dispatch(
                "steer",
                &json!({ "activeSessionId": "queue-session", "message": "three" }),
            )
            .await;
        let response = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "steering",
                    "index": 0,
                    "expectedText": "two",
                    "mutation": { "type": "move", "direction": 1 },
                }),
            )
            .await;
        assert_eq!(response.data, Some(json!({ "status": "applied" })));
        assert_eq!(lane_texts(&worker, Lane::Steering), vec!["three", "two"]);
        let response = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "steering",
                    "index": 1,
                    "expectedText": "two",
                    "mutation": { "type": "move", "direction": 1 },
                }),
            )
            .await;
        assert_eq!(response.data, Some(json!({ "status": "rejected" })));

        let response = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "steering",
                    "index": 0,
                    "expectedText": "three",
                    "mutation": { "type": "replace", "text": "edited", "lane": "followUp" },
                }),
            )
            .await;
        assert_eq!(response.data, Some(json!({ "status": "applied" })));
        assert_eq!(lane_texts(&worker, Lane::Steering), vec!["two"]);
        assert_eq!(lane_texts(&worker, Lane::FollowUp), vec!["edited"]);

        let response = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "steering",
                    "index": 9,
                    "expectedText": "two",
                    "mutation": { "type": "delete" },
                }),
            )
            .await;
        assert_eq!(response.data, Some(json!({ "status": "rejected" })));

        let response = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "bogus",
                    "index": 0,
                    "expectedText": "two",
                    "mutation": { "type": "delete" },
                }),
            )
            .await;
        assert!(!response.success);
        assert_eq!(response.command, "mutate_queued_message");
    }

    #[tokio::test]
    async fn resume_queue_matches_the_ts_wire_shapes() {
        let worker = created_worker().await;
        let response = worker.dispatch("resume_queue", &json!({})).await;
        assert!(!response.success);
        assert_eq!(response.error.as_deref(), Some("No queued work to resume"));
        assert_eq!(response.command, "resume_queue");

        worker
            .dispatch(
                "follow_up",
                &json!({ "activeSessionId": "queue-session", "message": "queued work" }),
            )
            .await;
        let response = worker.dispatch("resume_queue", &json!({})).await;
        assert!(response.success, "resume failed: {response:?}");
        assert_eq!(response.command, "resume_queue");
        assert!(response.data.is_none());
    }

    #[tokio::test]
    async fn delete_rejects_the_waiting_caller() {
        let worker = created_worker().await;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        {
            let mut core = worker.core.lock().unwrap();
            core.steering.push_back(crate::worker::QueuedItem {
                focused_reply: None,
                priority: crate::worker::QueuePriority::Human,
                message: "waiting prompt".to_string(),
                preview: None,
                custom_message: None,
                agent_message: None,
                queue_key: None,
                admission_id: None,
                images: Vec::new(),
                done: Some(done_tx),
                queue_visible: true,
                policy: crate::worker::TurnPolicy::Queued,
                forced_batch: false,
                input_id: None,
            });
        }
        let response = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "steering",
                    "index": 0,
                    "expectedText": "waiting prompt",
                    "mutation": { "type": "delete" },
                }),
            )
            .await;
        assert_eq!(response.data, Some(json!({ "status": "applied" })));
        let settled = done_rx.await.expect("deleted waiter settles");
        assert_eq!(
            settled,
            crate::worker::TurnSettle::Withdrawn(
                "Queued prompt was deleted before delivery.".to_string()
            )
        );
    }

    /// A `replace` on an accepted agent-message delivery is rejected: its content must stay
    /// byte-identical to the prompt the turn runs on. Move and delete stay applicable.
    #[tokio::test]
    async fn replace_rejects_a_queued_agent_message_delivery() {
        let worker = created_worker().await;
        let delivered = worker
            .dispatch(
                "worker_deliver_message",
                &json!({
                    "targetActiveSessionId": "queue-session",
                    "message": "the research is done",
                    "sender": {
                        "activeSessionId": "source-session",
                        "sessionName": "research-lane",
                        "runtimeKind": "subagent",
                    },
                }),
            )
            .await;
        assert!(delivered.success, "deliver failed: {delivered:?}");
        // The queue strip addresses the delivery by its labeled preview,
        // not the rendered prompt.
        let expected = "Agent message received: the research is done";
        // A second queued prompt gives the delivery a move neighbor.
        worker
            .core
            .lock()
            .unwrap()
            .steering
            .push_back(crate::worker::QueuedItem {
                focused_reply: None,
                priority: crate::worker::QueuePriority::Human,
                preview: None,
                message: "plain prompt".to_string(),
                custom_message: None,
                agent_message: None,
                queue_key: None,
                admission_id: None,
                images: Vec::new(),
                done: None,
                queue_visible: true,
                policy: crate::worker::TurnPolicy::Queued,
                forced_batch: false,
                input_id: None,
            });

        let replaced = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "steering",
                    "index": 0,
                    "expectedText": expected,
                    "mutation": { "type": "replace", "text": "edited" },
                }),
            )
            .await;
        assert_eq!(replaced.data, Some(json!({ "status": "rejected" })));

        let moved = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "steering",
                    "index": 0,
                    "expectedText": expected,
                    "mutation": { "type": "move", "direction": 1 },
                }),
            )
            .await;
        assert_eq!(moved.data, Some(json!({ "status": "applied" })));

        let deleted = worker
            .dispatch(
                "mutate_queued_message",
                &json!({
                    "activeSessionId": "queue-session",
                    "lane": "steering",
                    "index": 1,
                    "expectedText": expected,
                    "mutation": { "type": "delete" },
                }),
            )
            .await;
        assert_eq!(deleted.data, Some(json!({ "status": "applied" })));
        let remaining = worker
            .core
            .lock()
            .unwrap()
            .steering
            .front()
            .map(|item| item.message.clone());
        assert_eq!(remaining.as_deref(), Some("plain prompt"));
    }

    /// A cross-lane replace is an arrival in the target lane: the edited user
    /// item lands after earlier user items and before parked agent messages.
    #[tokio::test]
    async fn cross_lane_replace_lands_before_parked_agent_messages() {
        for (source, target, source_command, target_command, delivery_mode) in [
            ("followUp", "steering", "follow_up", "steer", "steer"),
            ("steering", "followUp", "steer", "follow_up", "follow_up"),
        ] {
            let worker = created_worker().await;
            worker.core.lock().unwrap().busy = true;
            for (command, payload) in [
                (target_command, json!({ "message": "earlier" })),
                (
                    "worker_deliver_message",
                    json!({
                        "targetActiveSessionId": "queue-session",
                        "message": "the research is done",
                        "sender": { "activeSessionId": "source-session" },
                        "deliveryMode": delivery_mode,
                    }),
                ),
                (source_command, json!({ "message": "draft" })),
            ] {
                let admitted = worker.dispatch(command, &payload).await;
                assert!(admitted.success, "{command}: {admitted:?}");
            }
            let replaced = worker
                .dispatch(
                    "mutate_queued_message",
                    &json!({
                        "lane": source,
                        "index": 0,
                        "expectedText": "draft",
                        "mutation": { "type": "replace", "text": "edited", "lane": target },
                    }),
                )
                .await;
            assert_eq!(replaced.data, Some(json!({ "status": "applied" })));
            let queue = worker.dispatch("get_queue", &json!({})).await;
            let data = queue.data.expect("queue data");
            assert_eq!(data[source], json!([]), "{source} -> {target}");
            assert_eq!(
                data[target],
                json!([
                    "earlier",
                    "edited",
                    "Agent message received: the research is done"
                ]),
                "{source} -> {target}"
            );
        }
    }
    #[test]
    fn focused_queue_mutations_preserve_route_and_reject_session_commands() {
        let route = pa_types::daemon::FocusedThreadReply {
            client_input_id: "input-a".to_string(),
            reply_to_message_id: "parent-a".to_string(),
            reply_context: vec![],
        };
        let item = |message: &str, focused_reply| crate::worker::QueuedItem {
            focused_reply,
            priority: crate::worker::QueuePriority::Human,
            preview: None,
            message: message.to_string(),
            custom_message: None,
            agent_message: None,
            queue_key: None,
            admission_id: None,
            images: vec![],
            done: None,
            queue_visible: true,
            policy: crate::worker::TurnPolicy::Queued,
            forced_batch: false,
            input_id: None,
        };
        let mut lane = std::collections::VecDeque::from([
            item("same", Some(route.clone())),
            item("same", None),
        ]);
        assert_eq!(
            mutate_lane(
                &mut lane,
                0,
                "same",
                &json!({"text":"/compact"}),
                "replace",
                "deleted"
            ),
            "rejected"
        );
        assert_eq!(lane[0].focused_reply.as_ref(), Some(&route));
        assert_ne!(
            mutate_lane(
                &mut lane,
                0,
                "same",
                &json!({"text":"edited"}),
                "replace",
                "deleted"
            ),
            "rejected"
        );
        assert_eq!(lane[0].focused_reply.as_ref(), Some(&route));
        assert_ne!(
            mutate_lane(
                &mut lane,
                0,
                "edited",
                &json!({"direction":1}),
                "move",
                "deleted"
            ),
            "rejected"
        );
        assert_eq!(lane[1].focused_reply.as_ref(), Some(&route));
        assert_ne!(
            mutate_lane(&mut lane, 1, "edited", &json!({}), "delete", "deleted"),
            "rejected"
        );
        assert!(lane[0].focused_reply.is_none());
    }
}
