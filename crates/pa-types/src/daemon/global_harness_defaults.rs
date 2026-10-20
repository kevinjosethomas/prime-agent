//! Global defaults for future sessions, separate from any live session settings.

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[cfg_attr(feature = "contract", derive(schemars::JsonSchema))]
pub enum HarnessQueueMode {
    All,
    OneAtATime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GlobalHarnessDefaultSource {
    Configured,
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalHarnessDefault<T> {
    pub value: T,
    pub configured: Option<T>,
    pub source: GlobalHarnessDefaultSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GlobalHarnessDefaults {
    pub scope: String,
    pub runtime_version: String,
    pub capabilities: Vec<String>,
    pub auto_compaction_enabled: GlobalHarnessDefault<bool>,
    pub steering_mode: GlobalHarnessDefault<HarnessQueueMode>,
    pub follow_up_mode: GlobalHarnessDefault<HarnessQueueMode>,
    pub max_depth: GlobalHarnessDefault<u64>,
}

/// Omitted fields stay unchanged. Explicit null is not a reset operation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(feature = "contract", derive(schemars::JsonSchema))]
pub struct GlobalHarnessDefaultsPatch {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    #[cfg_attr(feature = "contract", schemars(with = "bool"))]
    pub auto_compaction_enabled: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    #[cfg_attr(feature = "contract", schemars(with = "HarnessQueueMode"))]
    pub steering_mode: Option<HarnessQueueMode>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    #[cfg_attr(feature = "contract", schemars(with = "HarnessQueueMode"))]
    pub follow_up_mode: Option<HarnessQueueMode>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_value"
    )]
    #[cfg_attr(feature = "contract", schemars(with = "u64"))]
    pub max_depth: Option<u64>,
}

fn present_value<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn partial_patch_preserves_omission_and_accepts_zero_depth() {
        assert_eq!(
            serde_json::from_value::<GlobalHarnessDefaultsPatch>(json!({
                "autoCompactionEnabled": false,
                "maxDepth": 0,
            }))
            .unwrap(),
            GlobalHarnessDefaultsPatch {
                auto_compaction_enabled: Some(false),
                max_depth: Some(0),
                ..GlobalHarnessDefaultsPatch::default()
            }
        );
    }

    #[test]
    fn patch_rejects_null_unknown_and_wrongly_typed_values() {
        for patch in [
            json!({"autoCompactionEnabled": null}),
            json!({"steeringMode": null}),
            json!({"followUpMode": null}),
            json!({"maxDepth": null}),
            json!({"activeSessionId": "session"}),
            json!({"steeringMode": "oneAtATime"}),
            json!({"followUpMode": "invalid"}),
            json!({"autoCompactionEnabled": "false"}),
            json!({"maxDepth": -1}),
            json!({"maxDepth": 1.5}),
        ] {
            assert!(serde_json::from_value::<GlobalHarnessDefaultsPatch>(patch).is_err());
        }
    }
}
