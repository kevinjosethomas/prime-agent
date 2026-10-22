//! `/reload`'s session half: the request auth rebinds from the credential
//! store, the store a `/login` from another process writes.
use super::*;
use pa_core::session_engine::provider_adapter::ProviderTarget;

/// A custom provider whose configured key is a placeholder: the stored
/// credential wins over it, so the request auth follows the store (the
/// re-login scenario the reload must cover).
fn write_credential_backed_provider(agent_dir: &std::path::Path) {
    std::fs::create_dir_all(agent_dir).unwrap();
    std::fs::write(
        agent_dir.join("models.json"),
        serde_json::json!({
            "providers": {
                "battery": {
                    "api": "openai-completions",
                    "baseUrl": "http://127.0.0.1:9",
                    "apiKey": "sk-config-fallback",
                    "models": [
                        {
                            "id": "mock-1",
                            "name": "Mock 1",
                            "api": "openai-completions",
                            "contextWindow": 128_000,
                            "maxTokens": 4096,
                        }
                    ]
                }
            }
        })
        .to_string(),
    )
    .unwrap();
}

/// One stored OAuth credential for the custom provider.
fn write_oauth_credential(agent_dir: &std::path::Path, access: &str) {
    std::fs::create_dir_all(agent_dir).unwrap();
    std::fs::write(
        agent_dir.join("auth.json"),
        serde_json::json!({
            "battery": {
                "type": "oauth",
                "access": access,
                "refresh": "r",
                "expires": 4_102_444_800_000i64,
            }
        })
        .to_string(),
    )
    .unwrap();
}

fn credential_backed_engine_with_override(
    dir: &std::path::Path,
    api_key: Option<&str>,
) -> AgentSessionEngine {
    AgentSessionEngine::new(AgentEngineConfig {
        cwd: dir.to_path_buf(),
        agent_dir: dir.join("agent"),
        provider: Some("battery".to_string()),
        model: Some("mock-1".to_string()),
        api_key: api_key.map(str::to_string),
        thinking: None,
        session_dir: None,
        session_file: None,
        faux_script: None,
        supervisor_link: None,
        telemetry_disabled: Some(true),
        cron_store: None,
        queued_steering_probe: None,
    })
    .expect("engine")
}

fn credential_backed_engine(dir: &std::path::Path) -> AgentSessionEngine {
    AgentSessionEngine::new(AgentEngineConfig {
        cwd: dir.to_path_buf(),
        agent_dir: dir.join("agent"),
        provider: Some("battery".to_string()),
        model: Some("mock-1".to_string()),
        api_key: None,
        thinking: None,
        session_dir: None,
        session_file: None,
        faux_script: None,
        supervisor_link: None,
        telemetry_disabled: Some(true),
        cron_store: None,
        queued_steering_probe: None,
    })
    .expect("engine")
}

/// A credential another process wrote (the `/login` scenario) reaches the
/// running session through the reload: the provider target rebinds.
#[test]
fn reload_rebinds_the_request_auth_from_the_credential_store() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    write_credential_backed_provider(&agent_dir);
    write_oauth_credential(&agent_dir, "stale-access");
    let engine = credential_backed_engine(dir.path());
    let model = engine.resolve_model().expect("the custom model resolves");
    // The session's build-time target: the credential the engine saw.
    *engine
        .provider_target
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ProviderTarget {
        service_tier: None,
        api_key: Some("stale-access".to_string()),
        model,
        headers: None,
    });
    // A re-login from another process replaces the stored credential.
    write_oauth_credential(&agent_dir, "fresh-access");
    engine.reload_live_inputs().expect("the reload applies");
    let target = engine
        .provider_target
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("the reload keeps the target set");
    assert_eq!(
        target.api_key.as_deref(),
        Some("fresh-access"),
        "the reload rebinds the request auth to the fresh stored credential"
    );
}

/// With nothing stored for the live model the reload keeps the slot
/// untouched (a models.json lost mid-session loses nothing) — the MCP
/// re-read alone still runs.
#[test]
fn reload_with_nothing_stored_keeps_the_target() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    std::fs::create_dir_all(&agent_dir).unwrap();
    let engine = credential_backed_engine(dir.path());
    assert!(
        engine.resolve_model().is_err(),
        "no models.json: nothing resolves"
    );
    let model: pa_types::ai::Model =
        pa_core::session_engine::provider_adapter::json_round_trip(&serde_json::json!({
            "id": "mock-1",
            "name": "Mock 1",
            "api": "openai-completions",
            "provider": "battery",
            "baseUrl": "http://127.0.0.1:9",
            "reasoning": false,
            "input": ["text"],
            "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
            "contextWindow": 128_000,
            "maxTokens": 4096,
        }))
        .expect("the model converts");
    *engine
        .provider_target
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ProviderTarget {
        service_tier: None,
        api_key: Some("stale-access".to_string()),
        model,
        headers: None,
    });
    engine.reload_live_inputs().expect("the reload applies");
    let target = engine
        .provider_target
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("the target stays");
    assert_eq!(
        target.api_key.as_deref(),
        Some("stale-access"),
        "an unresolvable model leaves the target untouched"
    );
}

/// The two-model pair the retarget guard uses: the text model the
/// selection pins and a second model standing in for a routed episode's
/// serving target.
fn write_two_model_provider(agent_dir: &std::path::Path) {
    std::fs::create_dir_all(agent_dir).unwrap();
    std::fs::write(
        agent_dir.join("models.json"),
        serde_json::json!({
            "providers": {
                "battery": {
                    "api": "openai-completions",
                    "baseUrl": "http://127.0.0.1:9",
                    "apiKey": "sk-config-fallback",
                    "models": [
                        {
                            "id": "mock-1",
                            "name": "Mock 1",
                            "api": "openai-completions",
                            "contextWindow": 128_000,
                            "maxTokens": 4096,
                        },
                        {
                            "id": "mock-vision",
                            "name": "Mock Vision",
                            "api": "openai-completions",
                            "contextWindow": 128_000,
                            "maxTokens": 4096,
                        }
                    ]
                }
            }
        })
        .to_string(),
    )
    .unwrap();
}

/// The live model never retargets through a reload: a routed image
/// episode serves on its own target and an unflagged session must not
/// follow a changed settings default — only the request auth rebinds.
#[test]
fn reload_keeps_the_live_model_and_rebinds_its_auth() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    write_two_model_provider(&agent_dir);
    write_oauth_credential(&agent_dir, "fresh-access");
    let engine = credential_backed_engine(dir.path());
    // The selection pins mock-1; the slot holds a routed episode's target
    // on a DIFFERENT model.
    let session_model = engine.resolve_model().expect("the pinned model resolves");
    assert_eq!(session_model.id, "mock-1");
    let routed: pa_types::ai::Model =
        pa_core::session_engine::provider_adapter::json_round_trip(&serde_json::json!({
            "id": "mock-vision",
            "name": "Mock Vision",
            "api": "openai-completions",
            "provider": "battery",
            "baseUrl": "http://127.0.0.1:9",
            "reasoning": false,
            "input": ["text"],
            "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
            "contextWindow": 128_000,
            "maxTokens": 4096,
        }))
        .expect("the routed model converts");
    *engine
        .provider_target
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ProviderTarget {
        service_tier: None,
        api_key: Some("stale-access".to_string()),
        model: routed,
        headers: None,
    });
    engine.reload_live_inputs().expect("the reload applies");
    let target = engine
        .provider_target
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("the reload keeps the target set");
    assert_eq!(
        target.model.id, "mock-vision",
        "the reload never retargets the live model"
    );
    assert_eq!(
        target.api_key.as_deref(),
        Some("fresh-access"),
        "the live model's request auth rebinds from the store"
    );
}

/// Before the session's first build there is no live target to refresh:
/// the reload installs nothing (the build binds its own target) — the MCP
/// re-read alone runs.
#[test]
fn reload_without_a_live_target_installs_nothing() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    write_credential_backed_provider(&agent_dir);
    write_oauth_credential(&agent_dir, "fresh-access");
    let engine = credential_backed_engine(dir.path());
    engine.reload_live_inputs().expect("the reload applies");
    assert!(
        engine
            .provider_target
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none(),
        "the reload never installs a target"
    );
}

/// A routed image episode re-applies its armed target on every model-turn
/// attempt (overflow retries, failover restores): the reload must refresh
/// the armed route's auth too, or the next attempt reinstalls the stale
/// credentials a re-login just replaced.
#[test]
fn reload_refreshes_the_armed_image_route_target_auth() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    write_two_model_provider(&agent_dir);
    write_oauth_credential(&agent_dir, "fresh-access");
    let engine = credential_backed_engine(dir.path());
    let session_model = engine.resolve_model().expect("the pinned model resolves");
    let routed: pa_types::ai::Model =
        pa_core::session_engine::provider_adapter::json_round_trip(&serde_json::json!({
            "id": "mock-vision",
            "name": "Mock Vision",
            "api": "openai-completions",
            "provider": "battery",
            "baseUrl": "http://127.0.0.1:9",
            "reasoning": false,
            "input": ["text"],
            "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
            "contextWindow": 128_000,
            "maxTokens": 4096,
        }))
        .expect("the routed model converts");
    let override_model: pa_agent::types::Model =
        pa_core::session_engine::provider_adapter::json_round_trip(&routed)
            .expect("the override model converts");
    *engine
        .image_route
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) =
        Some(crate::image_route::ImageRoute {
            target: ProviderTarget {
                service_tier: None,
                api_key: Some("stale-access".to_string()),
                model: routed,
                headers: None,
            },
            agent_override: pa_agent::agent::AgentModelOverride {
                model: override_model,
                thinking_level: pa_agent::types::ThinkingLevel::default(),
            },
            // The saved session target a failing route clear restores: it
            // carries the pre-login key and must re-bind with the rest.
            session_target: Some(ProviderTarget {
                service_tier: None,
                api_key: Some("stale-session-access".to_string()),
                model: session_model,
                headers: None,
            }),
        });
    engine.reload_live_inputs().expect("the reload applies");
    let armed = engine
        .image_route
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .expect("the reload keeps the armed route");
    assert_eq!(
        armed.target.model.id, "mock-vision",
        "the armed route keeps its routed model"
    );
    assert_eq!(
        armed.target.api_key.as_deref(),
        Some("fresh-access"),
        "the armed route's request auth rebinds from the store"
    );
    let saved = armed
        .session_target
        .as_ref()
        .expect("the saved session target stays");
    assert_eq!(
        saved.api_key.as_deref(),
        Some("fresh-access"),
        "the saved session target's request auth rebinds from the store"
    );
}

/// A failover's primary restore re-resolves the request auth from the
/// store: a credential rotated (and reloaded) during the failover serves
/// from the store, never the pre-failover capture; the capture only backs
/// a resolution that yields nothing.
#[test]
fn restored_primary_target_serves_the_store_auth() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    write_credential_backed_provider(&agent_dir);
    write_oauth_credential(&agent_dir, "fresh-access");
    let engine = credential_backed_engine(dir.path());
    let primary = engine.resolve_model().expect("the primary resolves");
    let restored =
        engine.restored_primary_target(&primary, Some("stale-capture".to_string()), None);
    assert_eq!(
        restored.api_key.as_deref(),
        Some("fresh-access"),
        "the restored primary serves the stored credential"
    );
    assert_eq!(
        restored.model.id, primary.id,
        "the restore keeps the primary model"
    );

    // A provider with nothing configured anywhere: the capture backs the
    // restore, not an empty key.
    let ghost: pa_types::ai::Model =
        pa_core::session_engine::provider_adapter::json_round_trip(&serde_json::json!({
            "id": "ghost-1",
            "name": "Ghost 1",
            "api": "openai-completions",
            "provider": "ghost",
            "baseUrl": "http://127.0.0.1:9",
            "reasoning": false,
            "input": ["text"],
            "cost": { "input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0 },
            "contextWindow": 128_000,
            "maxTokens": 4096,
        }))
        .expect("the ghost model converts");
    let restored = engine.restored_primary_target(&ghost, Some("stale-capture".to_string()), None);
    assert_eq!(
        restored.api_key.as_deref(),
        Some("stale-capture"),
        "a resolution that yields nothing falls back to the capture"
    );

    // An unreadable store keeps the captured pair: the fresh resolution
    // would serve the configured fallback key, which is not a credential
    // — the same overwrite /reload's gate avoids.
    std::fs::write(agent_dir.join("auth.json"), "not json").unwrap();
    let restored = engine.restored_primary_target(
        &primary,
        Some("captured-key".to_string()),
        Some(team_headers("captured-team")),
    );
    assert!(
        restored.api_key.as_deref() == Some("captured-key"),
        "a failed store read keeps the last-good captured key"
    );
    assert!(
        restored.headers.as_ref() == Some(&team_headers("captured-team")),
        "a failed store read keeps the last-good captured headers"
    );
}

/// A custom `prime-inference` provider: the one provider whose stored
/// credentials can carry request headers (the team header), so the
/// reload's header replacement is observable against a real resolution.
fn write_prime_inference_provider(agent_dir: &std::path::Path) {
    std::fs::create_dir_all(agent_dir).unwrap();
    std::fs::write(
        agent_dir.join("models.json"),
        serde_json::json!({
            "providers": {
                "prime-inference": {
                    "api": "openai-completions",
                    "baseUrl": "http://127.0.0.1:9",
                    "apiKey": "sk-config-fallback",
                    "models": [
                        {
                            "id": "mock-1",
                            "name": "Mock 1",
                            "api": "openai-completions",
                            "contextWindow": 128_000,
                            "maxTokens": 4096,
                        }
                    ]
                }
            }
        })
        .to_string(),
    )
    .unwrap();
}

/// One stored `prime-inference` API-key credential, with or without the
/// team header `get_provider_headers` derives from `primeTeam`.
fn write_prime_inference_credential(agent_dir: &std::path::Path, key: &str, team_id: Option<&str>) {
    std::fs::create_dir_all(agent_dir).unwrap();
    let mut credential = serde_json::json!({ "type": "api_key", "key": key });
    if let Some(team_id) = team_id {
        credential["primeTeam"] =
            serde_json::json!({ "teamId": team_id, "name": format!("Team {team_id}") });
    }
    std::fs::write(
        agent_dir.join("auth.json"),
        serde_json::json!({ "prime-inference": credential }).to_string(),
    )
    .unwrap();
}

fn team_headers(team_id: &str) -> std::collections::BTreeMap<String, String> {
    [("X-Prime-Team-ID".to_string(), team_id.to_string())]
        .into_iter()
        .collect()
}

fn prime_inference_backed_engine(dir: &std::path::Path) -> AgentSessionEngine {
    AgentSessionEngine::new(AgentEngineConfig {
        cwd: dir.to_path_buf(),
        agent_dir: dir.join("agent"),
        provider: Some("prime-inference".to_string()),
        model: Some("mock-1".to_string()),
        api_key: None,
        thinking: None,
        session_dir: None,
        session_file: None,
        faux_script: None,
        supervisor_link: None,
        telemetry_disabled: Some(true),
        cron_store: None,
        queued_steering_probe: None,
    })
    .expect("engine")
}

/// A rotated credential REPLACES the request headers: a fresh key that
/// carries no team header must clear the previous credential's
/// `X-Prime-Team-ID`, never serve the stale team context on the new key.
#[test]
fn reload_clears_headers_when_the_fresh_credential_carries_none() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    write_prime_inference_provider(&agent_dir);
    write_prime_inference_credential(&agent_dir, "team-key", Some("team-1"));
    let engine = prime_inference_backed_engine(dir.path());
    let model = engine.resolve_model().expect("the custom model resolves");
    // The session's build-time target: the team credential the engine saw.
    *engine
        .provider_target
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ProviderTarget {
        service_tier: None,
        api_key: Some("team-key".to_string()),
        model,
        headers: Some(team_headers("team-1")),
    });
    // A re-login without the team replaces the stored credential.
    write_prime_inference_credential(&agent_dir, "solo-key", None);
    engine.reload_live_inputs().expect("the reload applies");
    let target = engine
        .provider_target
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("the reload keeps the target set");
    // The resolved credential never formats into a failure message: an
    // api key is sensitive data (CodeQL's cleartext-logging rule), so the
    // assertion compares without the assert_eq! formatting sink.
    assert!(
        target.api_key.as_deref() == Some("solo-key"),
        "the reload rebinds the request auth to the fresh stored credential"
    );
    assert!(
        target.headers.is_none(),
        "the fresh credential clears the stale team header"
    );
}

/// A failover's primary restore serves the resolved pair, never the
/// resolved key paired with the captured headers: a credential rotated
/// during the failover carries its own (empty) headers, clearing the
/// captured `X-Prime-Team-ID` the pre-failover credential pinned.
#[test]
fn restored_primary_target_serves_the_resolved_header_pair() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    write_prime_inference_provider(&agent_dir);
    write_prime_inference_credential(&agent_dir, "solo-key", None);
    let engine = prime_inference_backed_engine(dir.path());
    let primary = engine.resolve_model().expect("the primary resolves");
    let restored = engine.restored_primary_target(
        &primary,
        Some("captured-key".to_string()),
        Some(team_headers("old-team")),
    );
    // The resolved credential never formats into a failure message: an
    // api key is sensitive data (CodeQL's cleartext-logging rule), so the
    // assertion compares without the assert_eq! formatting sink.
    assert!(
        restored.api_key.as_deref() == Some("solo-key"),
        "the restored primary serves the stored credential"
    );
    assert!(
        restored.headers.is_none(),
        "the resolved pair replaces the captured headers"
    );
}

/// A reload over a malformed auth store reports the failure: the store
/// cannot be parsed, so the MCP manager keeps its previous credentials
/// and the reload answers with the storage error instead of a success
/// the session did not apply.
#[test]
fn reload_reports_the_auth_store_failure() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    write_credential_backed_provider(&agent_dir);
    write_oauth_credential(&agent_dir, "stale-access");
    let engine = credential_backed_engine(dir.path());
    let model = engine.resolve_model().expect("the custom model resolves");
    *engine
        .provider_target
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ProviderTarget {
        service_tier: None,
        api_key: Some("stale-access".to_string()),
        model,
        headers: None,
    });
    // Another process corrupts the store: the reload's re-read fails.
    std::fs::write(agent_dir.join("auth.json"), "not json").unwrap();
    let error = engine
        .reload_live_inputs()
        .expect_err("the malformed store fails the reload");
    assert!(!error.is_empty(), "the reload surfaces the storage error");
    // The failed reload never touched the target: the store gate ran
    // before any rebind, so the session keeps its last-good request auth
    // instead of falling to the provider's configured fallback key.
    let target = engine
        .provider_target
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("the failed reload keeps the target set");
    assert!(
        target.api_key.as_deref() == Some("stale-access"),
        "a failed reload preserves the last-good request auth"
    );
    // A repaired store reloads cleanly again.
    write_oauth_credential(&agent_dir, "fresh-access");
    engine
        .reload_live_inputs()
        .expect("the repaired store reloads");
    let target = engine
        .provider_target
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("the reload keeps the target set");
    assert!(
        target.api_key.as_deref() == Some("fresh-access"),
        "the repaired store rebinds the request auth"
    );
}

/// A create-config key override stands in memory regardless of the store:
/// when the store read fails, the restore keeps the override key and the
/// captured headers (the last-good team context), never the header-less
/// resolution the broken store produced.
#[test]
fn restored_primary_target_keeps_the_override_key_and_captured_headers_off_a_failed_store() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    write_credential_backed_provider(&agent_dir);
    write_oauth_credential(&agent_dir, "stored-key");
    let engine = credential_backed_engine_with_override(dir.path(), Some("override-key"));
    let primary = engine.resolve_model().expect("the primary resolves");
    // The store becomes unreadable mid-failover.
    std::fs::write(agent_dir.join("auth.json"), "not json").unwrap();
    let restored = engine.restored_primary_target(
        &primary,
        Some("override-key".to_string()),
        Some(team_headers("team-1")),
    );
    assert!(
        restored.api_key.as_deref() == Some("override-key"),
        "the override key stands in memory off a failed store"
    );
    assert!(
        restored.headers.as_ref() == Some(&team_headers("team-1")),
        "a failed store read keeps the captured headers"
    );
    // A healthy store still resolves the override pair fresh.
    write_oauth_credential(&agent_dir, "stored-key");
    let restored = engine.restored_primary_target(
        &primary,
        Some("override-key".to_string()),
        Some(team_headers("team-1")),
    );
    assert!(
        restored.api_key.as_deref() == Some("override-key"),
        "the healthy restore keeps the override key"
    );
}

/// A healthy store that no longer holds the provider's credential
/// leaves the target untouched: the resolution would serve the
/// configured models.json fallback key, which is not a stored
/// credential — the same never-clears contract a broken store keeps.
#[test]
fn reload_keeps_the_target_when_the_credential_leaves_the_store() {
    let dir = tempfile::TempDir::new().unwrap();
    let agent_dir = dir.path().join("agent");
    write_credential_backed_provider(&agent_dir);
    write_oauth_credential(&agent_dir, "live-access");
    let engine = credential_backed_engine(dir.path());
    let model = engine.resolve_model().expect("the custom model resolves");
    *engine
        .provider_target
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ProviderTarget {
        service_tier: None,
        api_key: Some("live-access".to_string()),
        model,
        headers: None,
    });
    // Another process removes the credential: the store stays healthy,
    // but nothing is stored for the provider.
    std::fs::write(agent_dir.join("auth.json"), "{}").unwrap();
    engine.reload_live_inputs().expect("the reload applies");
    let target = engine
        .provider_target
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("the reload keeps the target set");
    assert!(
        target.api_key.as_deref() == Some("live-access"),
        "an empty store keeps the last-good stored credential"
    );
    // A re-login restores the credential and the rebind resumes.
    write_oauth_credential(&agent_dir, "fresh-access");
    engine.reload_live_inputs().expect("the reload applies");
    let target = engine
        .provider_target
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .expect("the reload keeps the target set");
    assert!(
        target.api_key.as_deref() == Some("fresh-access"),
        "a restored credential rebinds the request auth"
    );
}

/// One markdown skill under the agent dir's user skills root.
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

/// The skill names the resource snapshot reports.
async fn snapshot_skill_names(engine: &std::sync::Arc<AgentSessionEngine>) -> Vec<String> {
    use crate::engine::SessionEngine as _;
    engine
        .resource_snapshot()
        .await
        .get("skills")
        .and_then(|skills| skills.as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|skill| skill.get("name").and_then(Value::as_str).map(String::from))
        .collect()
}

/// The live core session's system prompt.
async fn live_system_prompt(engine: &std::sync::Arc<AgentSessionEngine>) -> String {
    let guard = engine.session.lock().await;
    guard
        .as_deref()
        .expect("the session is built")
        .session
        .agent()
        .state()
        .await
        .system_prompt
}

/// `/reload`'s skill half: the snapshot hides a deleted skill file before
/// the reload (the cheap stat), and the explicit reload re-reads disk —
/// the deleted skill leaves the snapshot and the prompt, an added skill
/// lands in both. The prompt keeps the deleted skill until the reload.
#[tokio::test]
async fn reload_reloads_skills_and_the_snapshot_hides_deleted_files() {
    use crate::engine::SessionEngine as _;
    let (engine, _dir, agent_dir) = {
        let _faux = FAUX_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = tempfile::TempDir::new().unwrap();
        let agent_dir = dir.path().join("agent");
        write_skill(&agent_dir, "keep-skill");
        write_skill(&agent_dir, "drop-skill");
        let engine = AgentSessionEngine::new(AgentEngineConfig {
            cwd: dir.path().to_path_buf(),
            agent_dir: agent_dir.clone(),
            provider: None,
            model: None,
            api_key: None,
            thinking: None,
            session_dir: None,
            session_file: None,
            faux_script: Some(
                serde_json::json!({ "engine": "faux", "responses": [{ "text": "ok" }] })
                    .to_string(),
            ),
            supervisor_link: None,
            telemetry_disabled: None,
            cron_store: None,
            queued_steering_probe: None,
        })
        .unwrap();
        let engine = std::sync::Arc::new(engine);
        engine.register_arc();
        let model = engine.resolve_model().expect("the faux model resolves");
        drop(model);
        (engine, dir, agent_dir)
    };
    // Build the core session now (the resource loader snapshot happens here).
    let model = engine.resolve_model().expect("the faux model resolves");
    engine
        .ensure_core_session_async(&model)
        .await
        .expect("the session builds");
    let before = snapshot_skill_names(&engine).await;
    assert!(before.contains(&"keep-skill".to_string()));
    assert!(before.contains(&"drop-skill".to_string()));

    // A skill deleted and one added under the running session.
    std::fs::remove_dir_all(agent_dir.join("skills").join("drop-skill")).unwrap();
    write_skill(&agent_dir, "added-skill");

    // The snapshot hides the deleted file at its cheap stat; the prompt and
    // the inventory keep it until the explicit reload.
    let after_delete = snapshot_skill_names(&engine).await;
    assert!(after_delete.contains(&"keep-skill".to_string()));
    assert!(
        !after_delete.contains(&"drop-skill".to_string()),
        "the snapshot hides the deleted skill file"
    );
    assert!(!after_delete.contains(&"added-skill".to_string()));
    // The command list applies the same hiding rule.
    let command_names = engine
        .connection_commands()
        .await
        .iter()
        .filter(|command| command.get("source").and_then(Value::as_str) == Some("skill"))
        .filter_map(|command| command.get("name").and_then(Value::as_str))
        .map(|name| name.trim_start_matches("skill:").to_string())
        .collect::<Vec<String>>();
    assert!(command_names.contains(&"keep-skill".to_string()));
    assert!(
        !command_names.contains(&"drop-skill".to_string()),
        "the command list hides the deleted skill file"
    );
    assert!(!command_names.contains(&"added-skill".to_string()));
    let stale_prompt = live_system_prompt(&engine).await;
    assert!(stale_prompt.contains("drop-skill"));
    assert!(!stale_prompt.contains("added-skill"));

    // The explicit reload re-reads disk: the snapshot and the prompt match.
    engine
        .reload_skill_inventory()
        .await
        .expect("the reload applies");
    let after_reload = snapshot_skill_names(&engine).await;
    assert!(after_reload.contains(&"keep-skill".to_string()));
    assert!(after_reload.contains(&"added-skill".to_string()));
    assert!(!after_reload.contains(&"drop-skill".to_string()));
    let reloaded_prompt = live_system_prompt(&engine).await;
    assert!(reloaded_prompt.contains("added-skill"));
    assert!(!reloaded_prompt.contains("drop-skill"));

    // The engine owns a private runtime; dropping it from an async
    // context panics — the teardown rides a blocking thread.
    tokio::task::spawn_blocking(move || drop(engine))
        .await
        .expect("engine drop join");
}
