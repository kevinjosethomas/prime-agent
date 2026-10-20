//! The `set_task_reporter` payload validation: the desktop's trusted local
//! reporter argv becomes the canonical JSON the daemon stamps into every
//! kernel environment it provisions. Never sourced from prompt or task
//! content; the desktop re-pushes it on every reconnect.

use anyhow::{bail, Result};
use serde_json::json;

/// Default reporter timeout, seconds.
const DEFAULT_TIMEOUT_SECONDS: f64 = 15.0;
/// Largest accepted reporter timeout, seconds.
const MAX_TIMEOUT_SECONDS: f64 = 60.0;

/// Validate the desktop's payload and build the canonical env value:
/// `{"command": [...], "env": {...}, "timeout": <seconds>}`.
pub(crate) fn canonicalize(
    command: &[String],
    env: Option<&std::collections::BTreeMap<String, String>>,
    timeout: Option<f64>,
) -> Result<String> {
    if command.is_empty()
        || command
            .iter()
            .any(|arg| arg.is_empty() || arg.contains('\0'))
    {
        bail!("set_task_reporter requires a nonempty argv of nonempty strings");
    }
    if let Some(env) = env {
        if env.iter().any(|(key, value)| {
            key.is_empty() || key.contains('\0') || key.contains('=') || value.contains('\0')
        }) {
            bail!("set_task_reporter env must map names to string values");
        }
    }
    let timeout = match timeout {
        None => DEFAULT_TIMEOUT_SECONDS,
        Some(value) if value.is_finite() && value > 0.0 && value <= MAX_TIMEOUT_SECONDS => value,
        Some(_) => bail!(
            "set_task_reporter timeout must be positive and at most {MAX_TIMEOUT_SECONDS} seconds"
        ),
    };
    Ok(serde_json::to_string(&json!({
        "command": command,
        "env": env.cloned().unwrap_or_default(),
        "timeout": timeout,
    }))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical shape the kernel env var and the Python package agree on.
    #[test]
    fn canonical_shape_and_defaults() {
        let canonical = canonicalize(&["a".into(), "b".into()], None, None).unwrap();
        assert_eq!(
            canonical,
            r#"{"command":["a","b"],"env":{},"timeout":15.0}"#
        );
    }

    #[test]
    fn rejects_empty_argv_and_bad_timeouts() {
        assert!(canonicalize(&[], None, None).is_err());
        assert!(canonicalize(&["".into()], None, None).is_err());
        assert!(canonicalize(&["a\0".into()], None, None).is_err());
        for timeout in [0.0, -1.0, f64::NAN, f64::INFINITY, 61.0] {
            assert!(canonicalize(&["a".into()], None, Some(timeout)).is_err());
        }
        assert!(canonicalize(&["a".into()], None, Some(60.0)).is_ok());
    }

    #[test]
    fn rejects_invalid_env_names() {
        let bad = std::collections::BTreeMap::from([("a=b".to_string(), "1".to_string())]);
        assert!(canonicalize(&["a".into()], Some(&bad), None).is_err());
        let good = std::collections::BTreeMap::from([(
            "ELECTRON_RUN_AS_NODE".to_string(),
            "1".to_string(),
        )]);
        assert_eq!(
            canonicalize(&["a".into()], Some(&good), Some(15.0)).unwrap(),
            r#"{"command":["a"],"env":{"ELECTRON_RUN_AS_NODE":"1"},"timeout":15.0}"#
        );
    }
}
