//! Adapt the existing CLI auth services; no new credential storage or OAuth implementation.
use super::protocol::{Action, Target};
use pa_tui::auth_panel::{AuthPanelHandle, PastePromptTone, PasteStyle};
use pa_tui::client_auth::ClientAuthCommands;
use pa_tui::provider_auth::{
    AuthFlow, AuthStatusStyle, AuthType, ProviderAuthCommands, ProviderAuthOutcome, ProviderRow,
};
use serde_json::{json, Value};
use std::path::PathBuf;

fn row(value: &ProviderRow) -> Value {
    json!({"id":value.id,"name":value.name,"authType":match value.auth_type { AuthType::Oauth => "oauth", AuthType::ApiKey => "api_key" },"flow":match value.flow { AuthFlow::ApiKeyPrompt => "api_key", AuthFlow::TerminalFlow => "browser" },"configured":value.configured,"available":value.available,"status":value.status.as_ref().map(|status| json!({"style":match status.style {AuthStatusStyle::Success=>"success",AuthStatusStyle::Warning=>"warning",AuthStatusStyle::Muted=>"muted"},"label":status.label}))})
}
pub(super) async fn options(cwd: PathBuf, agent_dir: PathBuf) -> Value {
    let auth = crate::provider_login::ProviderAuth::new(cwd, agent_dir);
    let providers = auth.login_options().await;
    let logout = auth.logout_options().await;
    json!({"providers":providers.iter().map(row).collect::<Vec<_>>(),"logoutProviders":logout.iter().filter(|provider| !provider.id.starts_with("mcp:") && provider.id != pa_core::auth::SERPER_CREDENTIAL_ID).map(|provider| &provider.id).collect::<Vec<_>>()})
}
pub(super) async fn flow(
    cwd: PathBuf,
    agent_dir: PathBuf,
    target: Target,
    action: Action,
    key: String,
    add_account: bool,
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
            let Some(provider) = rows.into_iter().find(|provider| {
                provider.id == key
                    && provider.available
                    && !provider.id.starts_with("mcp:")
                    && provider.id != pa_core::auth::SERPER_CREDENTIAL_ID
            }) else {
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
        Target::Mcp => {
            let auth = crate::mcp_login::TerminalMcpAuth::new(cwd, agent_dir);
            let result = match action {
                Action::Login if add_account => auth.login_add_account(&key, panel.clone()).await,
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
