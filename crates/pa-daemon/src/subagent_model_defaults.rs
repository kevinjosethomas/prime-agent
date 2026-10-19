//! Global subagent model preference: catalog validation and settings persistence only.

use std::path::Path;

use anyhow::{anyhow, ensure, Result};
use pa_core::settings::{SettingsManager, SettingsScope};
use pa_types::daemon::subagent_model_defaults::{SubagentModelDefault, SubagentModelDefaultSource};

pub(crate) const CAPABILITY: &str = "subagent_model_defaults";

pub(crate) fn read_model(agent_dir: &Path) -> Result<Option<String>> {
    let settings = SettingsManager::create(agent_dir, agent_dir);
    if let Some(error) = settings
        .errors()
        .iter()
        .find(|error| error.scope == SettingsScope::Global)
    {
        return Err(anyhow!(
            "Global settings could not be read: {}",
            error.message
        ));
    }
    if let Some(raw) = settings.global_raw() {
        ensure!(raw.is_object(), "Global settings must contain a JSON object");
        ensure!(
            raw.get("subagentDefaultModel").is_none_or(|model| model.is_null() || model.is_string()),
            "subagentDefaultModel must be a selector string or null"
        );
    }
    Ok(settings.get_subagent_default_model())
}

pub(crate) fn validate_model(agent_dir: &Path, reference: &str) -> Result<String> {
    let reference = reference.trim();
    let model = crate::rlm_child_model::catalog_models(agent_dir)
        .into_iter()
        .map(|model| format!("{}/{}", model.provider, model.id))
        .find(|selector| selector.eq_ignore_ascii_case(reference))
        .ok_or_else(|| anyhow!(
            "Subagent default model must be an available exact provider/model-id selector: {reference}"
        ))?;
    let allowlist = crate::model_allowlist::load(agent_dir, agent_dir);
    crate::model_allowlist::assert_allowed(&allowlist, &model)?;
    Ok(model)
}

pub(crate) fn get(agent_dir: &Path) -> Result<SubagentModelDefault> {
    let model = read_model(agent_dir)?;
    let source = if model.is_some() {
        SubagentModelDefaultSource::Configured
    } else {
        SubagentModelDefaultSource::InheritParent
    };
    Ok(SubagentModelDefault {
        model,
        scope: "global".to_string(),
        source,
        runtime_version: crate::protocol::app_version().to_string(),
        capabilities: vec![CAPABILITY.to_string()],
    })
}

pub(crate) fn set(agent_dir: &Path, reference: Option<&str>) -> Result<SubagentModelDefault> {
    let model = reference.map(|reference| validate_model(agent_dir, reference)).transpose()?;
    // Refuse unreadable input before the manager's lenient settings loader can treat it as unset.
    read_model(agent_dir)?;
    let mut settings = SettingsManager::create(agent_dir, agent_dir);
    settings.set_subagent_default_model(model)?;
    get(agent_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn catalog(agent_dir: &Path) {
        std::fs::write(agent_dir.join("models.json"), json!({
            "providers": {
                "test-provider": {
                    "baseUrl": "http://localhost:9",
                    "apiKey": "test-key", "api": "openai-completions",
                    "models": [{"id":"nested/model", "name":"Test", "contextWindow":1000, "maxTokens":100}]
                }
            }
        }).to_string()).unwrap();
    }

    #[test]
    fn exact_selector_persists_canonical_identity_and_reset_preserves_other_fields() {
        let dir = tempfile::TempDir::new().unwrap();
        catalog(dir.path());
        std::fs::write(dir.path().join("settings.json"), json!({
            "defaultModel":"root-model", "unrelated":{"kept":true}
        }).to_string()).unwrap();
        assert!(set(dir.path(), Some("nested/model")).is_err());
        assert!(set(dir.path(), Some("")).is_err());
        assert!(set(dir.path(), Some("other-provider/nested/model")).is_err());
        std::fs::create_dir_all(dir.path().join(".prime/agent")).unwrap();
        std::fs::write(dir.path().join(".prime/agent/settings.json"), json!({
            "subagentDefaultModel":"other-provider/project"
        }).to_string()).unwrap();
        let saved = set(dir.path(), Some("TEST-PROVIDER/nested/model")).unwrap();
        assert_eq!(saved.model.as_deref(), Some("test-provider/nested/model"));
        assert_eq!(get(dir.path()).unwrap(), saved);
        let cleared = set(dir.path(), None).unwrap();
        assert_eq!(cleared.source, SubagentModelDefaultSource::InheritParent);
        assert_eq!(cleared.model, None);
        let raw: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.path().join("settings.json")).unwrap()).unwrap();
        assert_eq!(raw, json!({"defaultModel":"root-model", "unrelated":{"kept":true}}));
    }

    #[cfg(unix)]
    #[test]
    fn missing_settings_inherit_but_metadata_errors_refuse() {
        let dir = tempfile::tempdir().unwrap();
        assert!(get(dir.path()).is_ok());
        let path = dir.path().join("settings.json");
        std::os::unix::fs::symlink("settings.json", &path).unwrap();
        assert!(get(dir.path()).is_err());
        assert!(set(dir.path(), None).is_err());
        assert!(std::fs::symlink_metadata(path).unwrap().file_type().is_symlink());
    }

    #[test]
    fn allowlist_or_corrupt_settings_refuse_without_false_success() {
        let dir = tempfile::TempDir::new().unwrap();
        catalog(dir.path());
        let path = dir.path().join("settings.json");
        std::fs::write(&path, json!({"allowedModels":["other/*"]}).to_string()).unwrap();
        assert!(set(dir.path(), Some("test-provider/nested/model")).is_err());
        std::fs::write(&path, "{corrupt").unwrap();
        assert!(get(dir.path()).is_err());
        assert!(set(dir.path(), None).is_err());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "{corrupt");
    }
}
