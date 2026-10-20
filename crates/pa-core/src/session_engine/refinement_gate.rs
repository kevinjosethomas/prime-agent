//! The live master switch and model-boundary filtering. Durable history is untouched.

use pa_types::session::AgentMessage;

/// The single live policy read for scheduling, planning, apply, and context delivery.
pub(crate) fn enabled(settings: Option<&crate::settings::SettingsManager>) -> bool {
    settings.is_none_or(|settings| settings.reopen().get_harness_refinements())
}

impl super::AgentSession {
    pub(crate) fn harness_refinements_enabled(&self) -> bool {
        enabled(self.refinement_settings.as_ref())
    }
}

/// Remove only typed harness rows; never interpret a user's quoted text as metadata.
pub(crate) fn without_refinements(messages: Vec<AgentMessage>) -> Vec<AgentMessage> {
    messages
        .into_iter()
        .filter_map(|message| match message {
            AgentMessage::Custom(custom)
                if matches!(
                    custom.custom_type.as_str(),
                    "harness_digest" | "refinement_notice" | "refinement_outcome"
                ) =>
            {
                None
            }
            message @ (AgentMessage::User(_)
            | AgentMessage::Assistant(_)
            | AgentMessage::ToolResult(_)
            | AgentMessage::BashExecution(_)
            | AgentMessage::Custom(_)
            | AgentMessage::BranchSummary(_)
            | AgentMessage::CompactionSummary(_)) => Some(message),
        })
        .collect()
}

/// Resolve at every provider request, including tool continuations and retries.
/// I/O runs off the async worker. Filtering precedes conversion while provenance is typed.
pub(crate) fn convert_to_llm(
    settings: crate::settings::SettingsManager,
) -> pa_agent::agent_loop::ConvertToLlmFn {
    let settings = std::sync::Arc::new(settings);
    std::sync::Arc::new(move |messages| {
        let settings = std::sync::Arc::clone(&settings);
        Box::pin(async move {
            let enabled = tokio::task::spawn_blocking(move || enabled(Some(&settings))).await?;
            let messages = if enabled {
                messages
            } else {
                messages
                    .into_iter()
                    .filter_map(|message| match message {
                        message @ pa_agent::types::AgentMessage::Custom(_)
                            if message.role() == "focusedThreadUser" =>
                        {
                            Some(message)
                        }
                        pa_agent::types::AgentMessage::Custom(custom) => {
                            let typed =
                                serde_json::from_value(serde_json::to_value(custom).ok()?).ok()?;
                            without_refinements(vec![typed])
                                .into_iter()
                                .next()
                                .and_then(|row| super::session_message_to_loop(&row))
                        }
                        message @ pa_agent::types::AgentMessage::Standard(_) => Some(message),
                    })
                    .collect()
            };
            Ok(super::messages::loop_convert_to_llm(messages))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::SettingsManager;
    use pa_types::ai::{UserContent, UserMessage};

    fn user(text: &str) -> AgentMessage {
        AgentMessage::User(UserMessage {
            content: UserContent::Text(text.to_string()),
            timestamp: 0,
            rest: serde_json::Map::default(),
        })
    }

    fn saved_notice_row() -> pa_agent::types::AgentMessage {
        serde_json::from_value(serde_json::json!({
            "role": "custom",
            "customType": "refinement_notice",
            "content": "saved memory",
            "display": false,
            "details": { "summary": "saved memory" },
            "timestamp": 0
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn live_request_gate_filters_saved_rows_and_preserves_quoted_user_content() {
        let tmp = tempfile::tempdir().unwrap();
        let mut settings = SettingsManager::create(tmp.path(), tmp.path().join("agent"));
        let convert = convert_to_llm(settings.reopen());
        let notice = saved_notice_row();
        let quote =
            super::super::session_message_to_loop(&user("[refinement-notice] quoted user content"))
                .unwrap();
        let messages = vec![notice.clone(), quote.clone()];
        assert_eq!(convert(messages.clone()).await.unwrap().len(), 2);
        settings.set_harness_refinements(false).unwrap();
        let expected = super::super::messages::loop_convert_to_llm(vec![quote]);
        assert_eq!(convert(messages.clone()).await.unwrap(), expected);
        settings.set_harness_refinements(true).unwrap();
        assert_eq!(convert(messages).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn disabled_refinements_keeps_focused_normal_text_images_and_context() {
        let tmp = tempfile::tempdir().unwrap();
        let mut settings = SettingsManager::create(tmp.path(), tmp.path().join("agent"));
        settings.set_harness_refinements(false).unwrap();
        let route = pa_types::daemon::FocusedThreadReply {
            client_input_id: "input-a".to_string(),
            reply_to_message_id: "parent-a".to_string(),
            reply_context: vec![pa_types::daemon::FocusedThreadContextRow {
                id: "parent-a".to_string(),
                role: "user".to_string(),
                text: "prior context".to_string(),
            }],
        };
        let images = vec![pa_agent::types::ImageContent {
            data: "image".to_string(),
            mime_type: "image/png".to_string(),
        }];
        let message = super::super::focused_user_prompt_message("normal", &images, Some(&route));
        let result = convert_to_llm(settings)(vec![message]).await.unwrap();
        let value = serde_json::to_value(&result[0]).unwrap();
        assert!(value["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("prior context"));
        assert_eq!(value["content"][1]["text"], "normal");
        assert_eq!(value["content"][2]["data"], "image");
    }

    #[tokio::test]
    async fn cold_and_resumed_disabled_sessions_omit_saved_harness_context() {
        let tmp = tempfile::tempdir().unwrap();
        let agent_dir = tmp.path().join("agent");
        let mut settings = SettingsManager::create(tmp.path(), &agent_dir);
        settings.set_harness_refinements(false).unwrap();
        let model: pa_agent::types::Model = serde_json::from_value(serde_json::json!({
            "id":"m", "name":"m", "api":"test", "provider":"test", "baseUrl":"http://localhost",
            "reasoning":false, "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},
            "contextWindow":10000, "maxTokens":100
        }))
        .unwrap();
        for resumed in [false, true] {
            let mut manager = crate::session::manager::SessionManager::in_memory(tmp.path());
            manager.materialize_session_file(Some(tmp.path().join(if resumed {
                "resumed"
            } else {
                "cold"
            })));
            if resumed {
                manager
                    .append_custom_message(
                        "refinement_notice",
                        UserContent::Text("saved memory".to_string()),
                        false,
                        None,
                    )
                    .unwrap();
                manager.append_message(user("Continue my work")).unwrap();
            }
            let provider =
                std::sync::Arc::new(pa_agent::scripted::ScriptedProvider::new(model.clone()));
            let engine =
                super::super::engine::create_session(super::super::engine::SessionEngineConfig {
                    cwd: tmp.path().to_path_buf(),
                    agent_dir: agent_dir.clone(),
                    model: Some(model.clone()),
                    stream_fn: Some(provider.stream_fn()),
                    session_manager: Some(manager),
                    daemonless: true,
                    ..Default::default()
                })
                .await
                .unwrap();
            let live = engine.session.agent().state().await.messages;
            assert_eq!(
                super::super::messages::loop_convert_to_llm(live).len(),
                usize::from(resumed)
            );
            // Re-enable without a reload (the busy-session case): the next admission loads state.
            settings.set_harness_refinements(true).unwrap();
            settings.set_harness_refinements(false).unwrap();
            // The history remains inspectable even though it is absent from model context.
            if resumed {
                assert!(engine.session.entries().await.iter().any(|entry| matches!(entry,
                    pa_types::session::FileEntry::CustomMessage { payload, .. } if payload.custom_type == "refinement_notice")));
            }
        }
    }

    #[test]
    fn disabled_context_keeps_the_compaction_summary() {
        let summary = pa_types::session::CompactionSummaryMessage {
            summary: "Earlier decisions still apply".to_string(),
            tokens_before: 10,
            retained_message_count: None,
            custom_instructions: None,
            timestamp: 0,
        };
        assert_eq!(
            without_refinements(vec![AgentMessage::CompactionSummary(summary.clone())]),
            vec![AgentMessage::CompactionSummary(summary)]
        );
    }
}
