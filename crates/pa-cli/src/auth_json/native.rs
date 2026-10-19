//! Adapt the existing CLI auth services; no new credential storage or OAuth implementation.
use super::protocol::{Action, Method, Target};
use pa_core::auth::PRIME_AGENT_TRACES_PROVIDER_ID;
use pa_tui::auth_panel::{AuthPanelHandle, PastePromptTone, PasteStyle};
use pa_tui::client_auth::ClientAuthCommands;
use pa_tui::provider_auth::{
    AuthFlow, AuthStatusStyle, AuthType, ProviderAuthCommands, ProviderAuthOutcome, ProviderRow,
};
use pa_tui::traces::TraceLoginOutcome;
use serde_json::{json, Value};
use std::path::PathBuf;

fn method(provider: &ProviderRow) -> Method {
    match provider.auth_type {
        AuthType::Oauth => Method::Oauth,
        AuthType::ApiKey => Method::ApiKey,
    }
}

fn select_provider(
    rows: Vec<ProviderRow>,
    key: &str,
    method_id: Option<Method>,
) -> Option<ProviderRow> {
    rows.into_iter().find(|provider| {
        provider.id == key
            && provider.available
            && !provider.id.starts_with("mcp:")
            && provider.id != pa_core::auth::SERPER_CREDENTIAL_ID
            && method_id.is_none_or(|selected| method(provider) == selected)
    })
}

fn row(value: &ProviderRow) -> Value {
    json!({"id":value.id,"methodId":method(value),"name":value.name,"authType":match value.auth_type { AuthType::Oauth => "oauth", AuthType::ApiKey => "api_key" },"flow":match value.flow { AuthFlow::ApiKeyPrompt => "api_key", AuthFlow::TerminalFlow => "browser" },"configured":value.configured,"available":value.available,"status":value.status.as_ref().map(|status| json!({"style":match status.style {AuthStatusStyle::Success=>"success",AuthStatusStyle::Warning=>"warning",AuthStatusStyle::Muted=>"muted"},"label":status.label}))})
}
pub(super) async fn options(cwd: PathBuf, agent_dir: PathBuf) -> Value {
    let auth = crate::provider_login::ProviderAuth::new(cwd, agent_dir.clone());
    let providers = auth.login_options().await;
    let logout = auth.logout_options().await;
    let stored = logout
        .iter()
        .any(|provider| provider.id == PRIME_AGENT_TRACES_PROVIDER_ID);
    // The upload credential may also come from the environment or the Prime Inference key.
    let label = tokio::task::spawn_blocking(move || {
        pa_core::agent_traces::trace_credential(&agent_dir).map(|credential| credential.label)
    })
    .await
    .ok()
    .flatten();
    json!({"providers":providers.iter().map(row).collect::<Vec<_>>(),"logoutProviders":logout.iter().filter(|provider| !provider.id.starts_with("mcp:") && provider.id != pa_core::auth::SERPER_CREDENTIAL_ID && provider.id != PRIME_AGENT_TRACES_PROVIDER_ID).map(|provider| &provider.id).collect::<Vec<_>>(),"traces":{"configured":label.is_some(),"stored":stored,"label":label}})
}
pub(super) async fn flow(
    cwd: PathBuf,
    agent_dir: PathBuf,
    target: Target,
    action: Action,
    key: String,
    method_id: Option<Method>,
    panel: AuthPanelHandle,
) -> ProviderAuthOutcome {
    match target {
        Target::Provider => {
            let auth = crate::provider_login::ProviderAuth::new(cwd, agent_dir);
            let rows = if action == Action::Logout {
                auth.logout_options().await
            } else {
                auth.login_options().await
            };
            let Some(provider) = select_provider(rows, &key, method_id) else {
                return ProviderAuthOutcome::Error("Provider unavailable".to_string());
            };
            match action {
                Action::Logout => auth.logout(&provider).await,
                Action::Login => match provider.flow {
                    AuthFlow::TerminalFlow => auth.login_on_panel(&provider, panel).await,
                    AuthFlow::ApiKeyPrompt => {
                        let value = panel
                            .paste_prompt(
                                &format!("Enter API key for {}", provider.name),
                                PastePromptTone::Text,
                                PasteStyle::Masked,
                            )
                            .await;
                        if panel.cancelled() {
                            return ProviderAuthOutcome::Cancelled;
                        }
                        match value {
                            Some(value) => auth.login(&provider, Some(&value)).await,
                            None => ProviderAuthOutcome::Cancelled,
                        }
                    }
                },
                Action::Paste | Action::Key => {
                    ProviderAuthOutcome::Error("Provider action unavailable".to_string())
                }
            }
        }
        Target::Traces => match action {
            Action::Login => {
                match crate::traces_login::run_traces_login(&agent_dir, panel.clone()).await {
                    TraceLoginOutcome::Status(note) => ProviderAuthOutcome::Status(note),
                    TraceLoginOutcome::Error(error) => ProviderAuthOutcome::Error(error),
                    TraceLoginOutcome::Cancelled => ProviderAuthOutcome::Cancelled,
                }
            }
            Action::Logout => tokio::task::spawn_blocking(move || {
                let mut auth = pa_core::auth::AuthStorage::create(&agent_dir);
                auth.logout(PRIME_AGENT_TRACES_PROVIDER_ID);
                match auth.drain_errors().pop() {
                    Some(error) => ProviderAuthOutcome::Error(format!("Logout failed: {error}")),
                    None => ProviderAuthOutcome::Status("Logged out of Prime Agent Traces".into()),
                }
            })
            .await
            .unwrap_or_else(|_| ProviderAuthOutcome::Error("Logout failed".to_string())),
            Action::Paste | Action::Key => {
                ProviderAuthOutcome::Error("Traces action unavailable".to_string())
            }
        },
        Target::Mcp => {
            let auth = crate::mcp_login::TerminalMcpAuth::new(cwd, agent_dir);
            let result = match action {
                Action::Login => auth.login(&key, panel.clone()).await,
                Action::Logout => auth.logout(&key).await,
                Action::Paste => auth.paste_token(&key, panel.clone()).await,
                Action::Key => auth.api_key(&key, panel.clone()).await,
            };
            if panel.cancelled() {
                return ProviderAuthOutcome::Cancelled;
            }
            match result {
                Ok(note) => ProviderAuthOutcome::Status(note),
                Err(_) => {
                    ProviderAuthOutcome::Error("Connection authentication failed".to_string())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider(id: &str, auth_type: AuthType) -> ProviderRow {
        ProviderRow {
            id: id.to_string(),
            name: id.to_string(),
            auth_type,
            flow: match auth_type {
                AuthType::Oauth => AuthFlow::TerminalFlow,
                AuthType::ApiKey => AuthFlow::ApiKeyPrompt,
            },
            status: None,
            configured: false,
            available: true,
        }
    }

    #[test]
    fn explicit_method_routes_the_native_row_not_the_first_matching_provider() {
        for id in ["anthropic", "xai"] {
            for (first, second) in [
                (AuthType::Oauth, AuthType::ApiKey),
                (AuthType::ApiKey, AuthType::Oauth),
            ] {
                let rows = vec![provider(id, first), provider(id, second)];
                for (selected, expected) in [
                    (Method::Oauth, AuthType::Oauth),
                    (Method::ApiKey, AuthType::ApiKey),
                ] {
                    let selected = select_provider(rows.clone(), id, Some(selected)).unwrap();
                    assert_eq!(selected, provider(id, expected));
                    let catalog = row(&selected);
                    assert_eq!(
                        catalog["methodId"],
                        serde_json::to_value(method(&selected)).unwrap()
                    );
                }
                // Old v1 clients still get the native catalog's first row.
                assert_eq!(select_provider(rows, id, None), Some(provider(id, first)));
            }
        }
    }

    #[test]
    fn missing_or_unavailable_method_never_falls_back_to_another_row() {
        let rows = vec![provider("anthropic", AuthType::Oauth)];
        assert!(select_provider(rows.clone(), "anthropic", Some(Method::ApiKey)).is_none());
        assert!(select_provider(rows, "anthropic:api_key", Some(Method::Oauth)).is_none());
        let mut unavailable = provider("anthropic", AuthType::ApiKey);
        unavailable.available = false;
        assert!(select_provider(vec![unavailable], "anthropic", Some(Method::ApiKey)).is_none());
    }
}
