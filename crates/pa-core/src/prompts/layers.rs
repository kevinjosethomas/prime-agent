//! Static system-prompt layers assembled into the cache-stable prefix. Layers must never
//! contain session-specific values (the cache-safety guard test pins the boundary).

pub const CORE_LAYER: &str = include_str!("layers/core.md");

/// Source file of one layer (breakdown provenance).
#[must_use]
pub fn layer_source(name: &str) -> Option<&'static str> {
    match name {
        "core" => Some("prompts/layers/core.md"),
        "model-prompts" => Some("prompts/layers/model_prompts.toml"),
        _ => None,
    }
}
