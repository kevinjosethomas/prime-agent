//! Typed System One evaluation protocol. These decisions contain probabilities,
//! not generated chat messages; callers own classification and execution policy.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One joint evaluation of typed questions against shared structured state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemOneRequest {
    pub model: String,
    pub state: Value,
    pub questions: BTreeMap<String, ChoiceQuestion>,
    #[serde(flatten)]
    pub rest: crate::JsonMap,
}

/// A choice among named alternatives, evaluated in one forward pass.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChoiceQuestion {
    #[serde(rename = "type")]
    pub kind: ChoiceQuestionType,
    pub instructions: String,
    pub criteria: BTreeMap<String, String>,
    #[serde(flatten)]
    pub rest: crate::JsonMap,
}

/// The supported question kind for workstream membership evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChoiceQuestionType {
    Choice,
}

/// One response envelope, with a distribution for each requested question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemOneResponse {
    pub model: String,
    pub answers: BTreeMap<String, ChoiceAnswer>,
    #[serde(flatten)]
    pub rest: crate::JsonMap,
}

/// A selected alternative and the full distribution used to decide uncertainty.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChoiceAnswer {
    pub choice: String,
    pub confidence: f64,
    pub probabilities: BTreeMap<String, f64>,
    #[serde(flatten)]
    pub rest: crate::JsonMap,
}
