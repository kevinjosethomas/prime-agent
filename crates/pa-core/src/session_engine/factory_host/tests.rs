//! The factory host bridge battery: the request validation (the lane's
//! argument checks, mirroring the kernel frame's own validation).

use super::FactoryActivityRequest;

/// The request validation: known actions, required targets per action, and
/// the bounded watch timeout.
#[test]
fn factory_activity_requests_validate_like_the_kernel_frame() {
    let parse =
        |action: &str, run_id: Option<&str>, spec_id: Option<&str>, timeout_ms: Option<u64>| {
            FactoryActivityRequest::parse(action, run_id, spec_id, timeout_ms)
        };
    // the happy shapes
    assert_eq!(
        parse("graph", None, None, None).unwrap(),
        FactoryActivityRequest {
            action: "graph",
            run_id: None,
            spec_id: None,
            timeout_ms: None,
        }
    );
    assert_eq!(
        parse("graph", Some("  "), None, None).unwrap().run_id,
        None,
        "whitespace-only targets read as absent"
    );
    assert_eq!(
        parse("watch", Some("run-1"), None, Some(2_000))
            .unwrap()
            .timeout_ms,
        Some(2_000)
    );
    assert_eq!(
        parse("run", None, Some("spec-1"), None)
            .unwrap()
            .spec_id
            .as_deref(),
        Some("spec-1")
    );
    // unknown action
    assert_eq!(
        parse("bogus", None, None, None).unwrap_err().to_string(),
        "unknown factory activity action"
    );
    // required targets
    assert_eq!(
        parse("status", None, None, None).unwrap_err().to_string(),
        "factory activity status requires runId"
    );
    assert_eq!(
        parse("watch", None, None, None).unwrap_err().to_string(),
        "factory activity watch requires runId"
    );
    assert_eq!(
        parse("stop", None, None, None).unwrap_err().to_string(),
        "factory activity stop requires runId"
    );
    assert_eq!(
        parse("resume", None, None, None).unwrap_err().to_string(),
        "factory activity resume requires runId"
    );
    assert_eq!(
        parse("run", None, None, None).unwrap_err().to_string(),
        "factory activity run requires specId"
    );
    // the watch bound
    assert!(parse("watch", Some("r"), None, Some(60_000)).is_ok());
    let error = parse("watch", Some("r"), None, Some(60_001))
        .unwrap_err()
        .to_string();
    assert_eq!(error, "factory activity timeoutMs must be at most 60000");
}
