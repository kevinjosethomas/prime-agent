use std::sync::Arc;

use serde_json::{json, Value};

use super::{InMemorySettingsStorage, SettingsManager, SettingsScope, SettingsStorage};

fn fixture(content: &str) -> (Arc<InMemorySettingsStorage>, SettingsManager) {
    let storage = Arc::new(InMemorySettingsStorage::default());
    storage
        .with_lock(SettingsScope::Global, &mut |_| Some(content.to_string()))
        .unwrap();
    let manager = SettingsManager::from_storage(storage.clone());
    (storage, manager)
}

fn document(storage: &dyn SettingsStorage) -> Value {
    serde_json::from_str(&storage.read(SettingsScope::Global).unwrap().unwrap()).unwrap()
}


#[test]
fn compaction_percentage_preserves_siblings_and_survives_reload() {
    let (storage, mut manager) =
        fixture(r#"{"compaction":{"enabled":false,"reserveTokens":1234,"future":"keep"}}"#);
    assert_eq!(manager.get_compaction_threshold_percent(), 95.0);
    for percent in [1.0, 62.5, 95.0, 100.0] {
        manager.set_compaction_threshold_percent(percent).unwrap();
        manager.reload().unwrap();
        assert_eq!(manager.get_compaction_threshold_percent(), percent);
        assert_eq!(
            document(storage.as_ref()),
            json!({"compaction":{
                "enabled":false,"reserveTokens":1234,"future":"keep","thresholdPercent":percent
            }})
        );
    }
}

#[test]
fn invalid_compaction_percentage_never_mutates_or_saves() {
    let (storage, mut manager) = fixture(r#"{"compaction":{"thresholdPercent":80.0}}"#);
    for percent in [
        0.0,
        -1.0,
        100.01,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        assert!(manager.set_compaction_threshold_percent(percent).is_err());
        assert_eq!(manager.get_compaction_threshold_percent(), 80.0);
        assert_eq!(
            document(storage.as_ref()),
            json!({"compaction":{"thresholdPercent":80.0}})
        );
    }
}

#[test]
fn invalid_loaded_percentage_keeps_other_compaction_settings() {
    for invalid in [
        json!(null),
        json!("80"),
        json!(false),
        json!(-1),
        json!(0),
        json!(101),
        json!({}),
        json!([]),
    ] {
        let settings = super::load::from_value_lenient(&json!({"compaction":{
            "enabled":false,"reserveTokens":1234,"thresholdPercent":invalid
        }}));
        let manager = SettingsManager::in_memory(&settings);
        assert_eq!(manager.get_compaction_threshold_percent(), 95.0);
        assert!(!manager.get_compaction_enabled());
        assert_eq!(
            manager
                .settings()
                .compaction
                .as_ref()
                .unwrap()
                .reserve_tokens,
            Some(1234)
        );
    }
}

#[test]
fn project_percentage_overrides_global_and_stale_writes_preserve_external_settings() {
    let (storage, mut first) = fixture(r#"{"compaction":{"thresholdPercent":80.0}}"#);
    let mut second = SettingsManager::from_storage(storage.clone());
    second.set_compaction_enabled(false).unwrap();
    first.set_compaction_threshold_percent(70.0).unwrap();
    assert_eq!(
        document(storage.as_ref()),
        json!({"compaction":{"enabled":false,"thresholdPercent":70.0}})
    );
    storage
        .with_lock(SettingsScope::Project, &mut |_| {
            Some(r#"{"compaction":{"thresholdPercent":60.5}}"#.into())
        })
        .unwrap();
    first.reload().unwrap();
    assert_eq!(first.get_compaction_threshold_percent(), 60.5);
    first.set_compaction_threshold_percent(90.0).unwrap();
    first.reload().unwrap();
    assert_eq!(first.get_compaction_threshold_percent(), 60.5);
}
