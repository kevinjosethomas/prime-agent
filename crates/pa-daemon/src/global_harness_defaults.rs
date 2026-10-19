//! Global Harness settings read/write seam. Never targets a session or worker.

use std::path::Path;

use anyhow::{anyhow, ensure, Result};
use pa_core::settings::{QueueModeSetting, SettingsManager, SettingsScope};
use pa_types::daemon::global_harness_defaults::{
    GlobalHarnessDefault, GlobalHarnessDefaultSource, GlobalHarnessDefaults,
    GlobalHarnessDefaultsPatch, HarnessQueueMode,
};

/// Read global settings only, without project, environment, or live-session overrides.
///
/// # Errors
/// Returns an error when the global settings document cannot be read or parsed.
pub(crate) fn get(agent_dir: &Path, runtime_version: &str) -> Result<GlobalHarnessDefaults> {
    let manager = SettingsManager::create(agent_dir, agent_dir);
    check_global_load(&manager)?;
    let global = manager.global_settings();
    Ok(GlobalHarnessDefaults {
        scope: "global".to_string(),
        runtime_version: runtime_version.to_string(),
        capabilities: vec!["global_harness_defaults".to_string()],
        auto_compaction_enabled: preference(
            global
                .compaction
                .as_ref()
                .and_then(|compaction| compaction.enabled),
            true,
        ),
        steering_mode: preference(global.steering_mode.map(queue_mode), HarnessQueueMode::All),
        follow_up_mode: preference(
            global.follow_up_mode.map(queue_mode),
            HarnessQueueMode::OneAtATime,
        ),
        max_depth: preference(
            global.rlm_max_depth,
            u64::from(pa_core::session_engine::rlm_host::DEFAULT_RLM_MAX_DEPTH),
        ),
    })
}

/// Save the supplied fields atomically and return a fresh persisted global read.
///
/// # Errors
/// Returns an error when global settings cannot be read, parsed, or saved.
pub(crate) fn set(
    agent_dir: &Path,
    patch: &GlobalHarnessDefaultsPatch,
    runtime_version: &str,
) -> Result<GlobalHarnessDefaults> {
    let mut manager = SettingsManager::create(agent_dir, agent_dir);
    check_global_load(&manager)?;
    manager.set_global_harness_defaults(patch)?;
    get(agent_dir, runtime_version)
}

fn check_global_load(manager: &SettingsManager) -> Result<()> {
    if let Some(error) = manager
        .errors()
        .iter()
        .find(|error| error.scope == SettingsScope::Global)
    {
        return Err(anyhow!(
            "Cannot read global Harness defaults: {}",
            error.message
        ));
    }
    if let Some(document) = manager.global_raw() {
        ensure!(
            document.is_object(),
            "Global settings.json must contain a JSON object"
        );
    }
    Ok(())
}

fn preference<T: Copy>(configured: Option<T>, default: T) -> GlobalHarnessDefault<T> {
    GlobalHarnessDefault {
        value: configured.unwrap_or(default),
        configured,
        source: if configured.is_some() {
            GlobalHarnessDefaultSource::Configured
        } else {
            GlobalHarnessDefaultSource::Default
        },
    }
}

fn queue_mode(mode: QueueModeSetting) -> HarnessQueueMode {
    match mode {
        QueueModeSetting::All => HarnessQueueMode::All,
        QueueModeSetting::OneAtATime => HarnessQueueMode::OneAtATime,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn get_preserves_unset_source_and_ignores_project_overrides() {
        let dir = tempfile::tempdir().unwrap();
        let agent_dir = dir.path();
        std::fs::create_dir_all(agent_dir.join(".prime/agent")).unwrap();
        std::fs::write(
            agent_dir.join("settings.json"),
            r#"{"steeringMode":"one-at-a-time","rlmMaxDepth":0}"#,
        )
        .unwrap();
        std::fs::write(
            agent_dir.join(".prime/agent/settings.json"),
            r#"{"compaction":{"enabled":false},"steeringMode":"all","followUpMode":"all","rlmMaxDepth":9}"#,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(get(agent_dir, "test-version").unwrap()).unwrap(),
            json!({
                "scope": "global",
                "runtimeVersion": "test-version",
                "capabilities": ["global_harness_defaults"],
                "autoCompactionEnabled": {"value":true,"configured":null,"source":"default"},
                "steeringMode": {"value":"one-at-a-time","configured":"one-at-a-time","source":"configured"},
                "followUpMode": {"value":"one-at-a-time","configured":null,"source":"default"},
                "maxDepth": {"value":0,"configured":0,"source":"configured"},
            })
        );
    }

    #[test]
    fn partial_patch_preserves_unrelated_keys_and_reads_back_persisted_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        let initial = json!({
            "compaction": {"enabled":true,"keepRecentTokens":2000},
            "steeringMode":"all",
            "followUpMode":"all",
            "rlmMaxDepth":7,
            "defaultModel":"unchanged",
            "futureSetting":{"keep":true},
        });
        std::fs::write(&path, serde_json::to_vec(&initial).unwrap()).unwrap();
        let patch = GlobalHarnessDefaultsPatch {
            auto_compaction_enabled: Some(false),
            max_depth: Some(0),
            ..GlobalHarnessDefaultsPatch::default()
        };
        let result = set(dir.path(), &patch, "test-version").unwrap();
        assert_eq!(result, get(dir.path(), "test-version").unwrap());
        let mut expected = initial;
        expected["compaction"]["enabled"] = json!(false);
        expected["rlmMaxDepth"] = json!(0);
        let persisted: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(persisted, expected);
        // A later external edit must be visible, rather than returning the manager snapshot.
        expected["followUpMode"] = json!("one-at-a-time");
        std::fs::write(&path, serde_json::to_vec(&expected).unwrap()).unwrap();
        assert_eq!(
            get(dir.path(), "test-version").unwrap().follow_up_mode,
            GlobalHarnessDefault {
                value: HarnessQueueMode::OneAtATime,
                configured: Some(HarnessQueueMode::OneAtATime),
                source: GlobalHarnessDefaultSource::Configured,
            }
        );
    }

    #[cfg(unix)]
    #[test]
    fn missing_settings_use_defaults_but_metadata_errors_refuse() {
        let dir = tempfile::tempdir().unwrap();
        assert!(get(dir.path(), "test-version").is_ok());
        let path = dir.path().join("settings.json");
        std::os::unix::fs::symlink("settings.json", &path).unwrap();
        assert!(get(dir.path(), "test-version").is_err());
        assert!(set(
            dir.path(),
            &GlobalHarnessDefaultsPatch {
                max_depth: Some(0),
                ..GlobalHarnessDefaultsPatch::default()
            },
            "test-version",
        )
        .is_err());
        assert!(std::fs::symlink_metadata(path).unwrap().file_type().is_symlink());
    }

    #[test]
    fn unreadable_documents_fail_without_replacing_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        for content in ["{broken", "[]", "null"] {
            std::fs::write(&path, content).unwrap();
            assert!(get(dir.path(), "test-version").is_err());
            assert!(set(
                dir.path(),
                &GlobalHarnessDefaultsPatch {
                    auto_compaction_enabled: Some(false),
                    ..GlobalHarnessDefaultsPatch::default()
                },
                "test-version",
            )
            .is_err());
            assert_eq!(std::fs::read_to_string(&path).unwrap(), content);
        }
    }
}
