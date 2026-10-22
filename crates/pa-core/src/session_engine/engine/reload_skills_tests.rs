//! `/reload`'s skill half: the inventory and the system prompt re-read
//! disk only at the explicit reload (never per turn).
use super::*;
use pa_agent::scripted::ScriptedProvider;

fn write_skill(agent_dir: &std::path::Path, name: &str) {
    let dir = agent_dir.join("skills").join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("SKILL.md"),
        format!(
            "---\nname: {name}\ndescription: The {name} description\n---\nRun the {name} flow."
        ),
    )
    .unwrap();
}

fn skill_names(engine: &SessionEngine) -> Vec<String> {
    engine
        .skills()
        .iter()
        .map(|skill| skill.name.clone())
        .collect()
}

/// The explicit reload re-runs the session's skill loader: a skill
/// deleted and a skill added under the running session both land — the
/// inventory, `/skill:` expansion, and the system prompt's
/// `<available_skills>` block match disk afterwards.
#[tokio::test]
async fn reload_rebuilds_the_skill_inventory_and_prompt_from_disk() {
    let registration =
        pa_ai::faux::register_faux_provider(pa_ai::faux::RegisterFauxProviderOptions::default());
    registration.set_responses(
        (0..2)
            .map(|_| {
                pa_ai::faux::FauxResponseStep::Message(pa_ai::faux::faux_assistant_text_message(
                    "A plain reply",
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
    write_skill(&agent_dir, "keep-skill");
    write_skill(&agent_dir, "drop-skill");
    let engine = create_session(SessionEngineConfig {
        cwd,
        agent_dir: agent_dir.clone(),
        model: Some(model),
        stream_fn: Some(provider.stream_fn()),
        prewarm_ipython_kernel: Some(false),
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(skill_names(&engine).contains(&"drop-skill".to_string()));
    let original_prompt = engine.session.agent().state().await.system_prompt;
    assert!(original_prompt.contains("drop-skill"));

    // A skill deleted and a skill added under the running session.
    std::fs::remove_dir_all(agent_dir.join("skills").join("drop-skill")).unwrap();
    write_skill(&agent_dir, "added-skill");
    engine
        .session
        .reload_skills_and_prompt(&compact_model)
        .await
        .unwrap();

    let names = skill_names(&engine);
    assert!(names.contains(&"keep-skill".to_string()));
    assert!(names.contains(&"added-skill".to_string()));
    assert!(!names.contains(&"drop-skill".to_string()));
    let reloaded_prompt = engine.session.agent().state().await.system_prompt;
    assert!(reloaded_prompt.contains("added-skill"));
    assert!(!reloaded_prompt.contains("drop-skill"));
    assert_eq!(
        engine.session.pinned_system_prompt().as_deref(),
        Some(reloaded_prompt.as_str()),
        "the reloaded prompt is the pinned one"
    );
    // `/skill:` expansion follows the reloaded inventory: the added
    // skill expands, the deleted one passes through untouched.
    let expanded = engine.expand_skill_submission("/skill:added-skill");
    assert!(expanded.contains("Run the added-skill flow."));
    assert_eq!(
        engine.expand_skill_submission("/skill:drop-skill"),
        "/skill:drop-skill"
    );
}

/// Without the explicit reload nothing rebuilds: turns keep the built
/// prompt (the provider's prompt cache stays stable) and the inventory
/// keeps the stale entry until `/reload` runs.
#[tokio::test]
async fn the_prompt_stays_pinned_until_the_explicit_reload() {
    let registration =
        pa_ai::faux::register_faux_provider(pa_ai::faux::RegisterFauxProviderOptions::default());
    registration.set_responses(
        (0..2)
            .map(|_| {
                pa_ai::faux::FauxResponseStep::Message(pa_ai::faux::faux_assistant_text_message(
                    "A plain reply",
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
    write_skill(&agent_dir, "drop-skill");
    let engine = create_session(SessionEngineConfig {
        cwd,
        agent_dir: agent_dir.clone(),
        model: Some(model),
        stream_fn: Some(provider.stream_fn()),
        prewarm_ipython_kernel: Some(false),
        ..Default::default()
    })
    .await
    .unwrap();
    let original_prompt = engine.session.agent().state().await.system_prompt;
    assert!(original_prompt.contains("drop-skill"));

    // The file disappears under the running session.
    std::fs::remove_dir_all(agent_dir.join("skills").join("drop-skill")).unwrap();
    provider.push_text_turn("A response after the delete.");
    engine
        .prompt("keep working", PromptOptions::default())
        .await
        .unwrap();
    engine.session.agent().wait_for_idle().await;
    for call in provider.calls() {
        assert_eq!(
            call.system_prompt.as_deref(),
            Some(original_prompt.as_str()),
            "a turn never rebuilds the prompt"
        );
    }
    assert!(
        skill_names(&engine).contains(&"drop-skill".to_string()),
        "the inventory keeps the stale entry until the reload"
    );

    engine
        .session
        .reload_skills_and_prompt(&compact_model)
        .await
        .unwrap();
    assert!(!skill_names(&engine).contains(&"drop-skill".to_string()));
    assert!(!engine
        .session
        .agent()
        .state()
        .await
        .system_prompt
        .contains("drop-skill"));
}
