//! Provider-facing cache pinning and compaction refresh regression.
use super::*;
use pa_agent::scripted::ScriptedProvider;

#[tokio::test]
async fn prompt_sources_refresh_only_after_successful_compaction() {
    let registration =
        pa_ai::faux::register_faux_provider(pa_ai::faux::RegisterFauxProviderOptions::default());
    registration.set_responses(
        (0..2)
            .map(|_| {
                pa_ai::faux::FauxResponseStep::Message(pa_ai::faux::faux_assistant_text_message(
                    "## Goal\nContinue the work",
                    pa_ai::faux::FauxAssistantMessageOptions::default(),
                ))
            })
            .collect(),
    );
    let compact_model = registration.get_model();
    let model: Model = super::super::provider_adapter::json_round_trip(&compact_model).unwrap();
    let provider = Arc::new(ScriptedProvider::new(model.clone()));
    let tmp = tempfile::tempdir().unwrap();
    let cwd = tmp.path().join("project");
    let agent_dir = tmp.path().join("agent");
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::create_dir_all(&agent_dir).unwrap();
    let system = agent_dir.join("SYSTEM.md");
    let append = agent_dir.join("APPEND_SYSTEM.md");
    let context = cwd.join("AGENTS.md");
    std::fs::write(&system, "Original system").unwrap();
    std::fs::write(&append, "Original guidance").unwrap();
    std::fs::write(&context, "Original project context").unwrap();
    let engine = create_session(SessionEngineConfig {
        cwd,
        agent_dir,
        model: Some(model),
        stream_fn: Some(provider.stream_fn()),
        prewarm_ipython_kernel: Some(false),
        ..Default::default()
    })
    .await
    .unwrap();
    let original = engine.session.agent().state().await.system_prompt;
    std::fs::write(&system, "Updated system").unwrap();
    std::fs::write(&append, "Updated guidance").unwrap();
    std::fs::write(&context, "Updated project context").unwrap();
    let staged = tmp.path().join("desktop-root-guidance.md");
    std::fs::write(&staged, "Updated desktop guidance").unwrap();
    engine
        .session
        .set_prompt_guidelines(vec![staged.display().to_string()]);
    let skipped = engine
        .session
        .compact(None, &compact_model, None, None)
        .await
        .unwrap();
    assert!(matches!(
        skipped,
        super::super::compact_session::CompactOutcome::Skipped(_)
    ));
    assert_eq!(engine.session.agent().state().await.system_prompt, original);
    for turn in ["first turn", "second turn", "third turn"] {
        provider.push_text_turn("A response with enough words to form a conversation.");
        engine.prompt(turn, PromptOptions::default()).await.unwrap();
        engine.session.agent().wait_for_idle().await;
    }
    let before = provider.calls();
    for call in &before {
        assert_eq!(call.system_prompt.as_deref(), Some(original.as_str()));
    }
    // Cached conversation rows, not only the system text, remain untouched.
    assert_eq!(
        serde_json::to_vec(&before[1].messages[..before[0].messages.len()]).unwrap(),
        serde_json::to_vec(&before[0].messages).unwrap(),
    );
    engine
        .session
        .set_compaction_settings(super::super::compaction::CompactionSettings {
            keep_recent_tokens: 1,
            reserve_tokens: 100,
            ..Default::default()
        });
    let result = engine
        .session
        .compact(None, &compact_model, None, None)
        .await
        .unwrap();
    assert!(matches!(
        result,
        super::super::compact_session::CompactOutcome::Ran(_)
    ));
    provider.push_text_turn("After compaction");
    engine
        .prompt("continue", PromptOptions::default())
        .await
        .unwrap();
    engine.session.agent().wait_for_idle().await;
    let after = provider.calls();
    let refreshed = after.last().unwrap().system_prompt.as_deref().unwrap();
    assert!(refreshed.starts_with("Updated system"));
    assert!(refreshed.ends_with("Updated desktop guidance"));
    assert!(refreshed.contains("Updated project context"));
    assert!(!refreshed.contains("Original"));
    assert_eq!(
        engine.session.pinned_system_prompt().as_deref(),
        Some(refreshed)
    );
}
