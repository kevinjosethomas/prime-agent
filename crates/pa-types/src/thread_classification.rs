//! Stateless semantic conversation-continuity wire types. No session or workstream state.
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadRole {
    User,
    Assistant,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadMessage {
    pub id: String,
    pub role: ThreadRole,
    pub text: String,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadCandidate {
    pub id: String,
    pub messages: Vec<ThreadMessage>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadClassificationRequest {
    pub id: String,
    pub target: ThreadMessage,
    pub recent_messages: Vec<ThreadMessage>,
    pub candidates: Vec<ThreadCandidate>,
}

#[derive(Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadScore {
    pub id: String,
    pub related: f64,
    pub unrelated: f64,
    pub confidence: f64,
}
