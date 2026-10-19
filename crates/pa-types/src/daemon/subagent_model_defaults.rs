//! Global child-model preference. No session state is part of this contract.

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SubagentModelDefaultSource {
    Configured,
    InheritParent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubagentModelDefault {
    pub model: Option<String>,
    pub scope: String,
    pub source: SubagentModelDefaultSource,
    pub runtime_version: String,
    pub capabilities: Vec<String>,
}

/// A present JSON null resets inheritance; an omitted field is not a reset operation.
///
/// # Errors
/// Returns a serde error for values other than a string or null.
pub(crate) fn required_nullable_model<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}
