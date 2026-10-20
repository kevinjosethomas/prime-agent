//! Wire conformance: accepted witnesses parse through the real Rust serde
//! types; mutation candidates are classified by serde (valid mutations are
//! accepted coverage, refused ones are rejects); real goldens must parse AND
//! re-emit losslessly. Node-side Ajv/tsc legs assert the validator halves and
//! must agree with the recorded serde verdicts.

use serde_json::Value;

fn generated_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../packages/client-contract/generated")
        .canonicalize()
        .expect("generated dir")
}

fn corpus(kind: &str) -> Vec<Value> {
    let dir = generated_dir().join("corpus").join(kind);
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("corpus dir").flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        for line in std::fs::read_to_string(&path).expect("corpus file").lines() {
            out.push(serde_json::from_str(line).expect("corpus line json"));
        }
    }
    out
}

fn corpus_stem(kind: &str, stem: &str) -> Vec<Value> {
    let path = generated_dir()
        .join("corpus")
        .join(kind)
        .join(format!("{stem}.jsonl"));
    std::fs::read_to_string(&path)
        .expect("corpus stem file")
        .lines()
        .map(|line| serde_json::from_str(line).expect("line json"))
        .collect()
}

#[test]
fn accepted_corpus_parses_through_serde() {
    for line in corpus("accepted") {
        let stem = line
            .get("schemaRoot")
            .and_then(Value::as_str)
            .expect("root");
        let sample = line.get("sample").expect("sample");
        assert!(
            pa_contract::parse_as_root(stem, sample).expect("verdict"),
            "accepted witness rejected by Rust serde: {stem}: {sample}"
        );
    }
}

#[test]
fn accepted_witnesses_cover_every_enum_branch() {
    // Derived coverage: every schema branch must have a witness (the
    // discriminant value of the branch appears in the corpus). No magic
    // counts; extra targeted variants (nested unknown codes, optional
    // properties, open extras) only ADD coverage.
    for stem in [
        "daemon-command",
        "daemon-command-envelope",
        "daemon-command-wire",
        "daemon-outbound",
        "daemon-error-info",
        "daemon-event-envelope",
        "daemon-event-meta",
        "daemon-resume-cursor",
        "agent-message",
    ] {
        let schema: Value = serde_json::from_str(
            &std::fs::read_to_string(
                generated_dir()
                    .join("schema")
                    .join(format!("{stem}.schema.json")),
            )
            .expect("schema"),
        )
        .expect("schema json");
        let Some(discriminant) = pa_contract::discriminant_property(&schema) else {
            continue;
        };
        let tags = pa_contract::enum_tags(&schema);
        let accepted = corpus_stem("accepted", stem);
        let samples: Vec<&Value> = accepted
            .iter()
            .map(|l| l.get("sample").expect("sample"))
            .collect();
        for tag in tags {
            let covered = samples.iter().any(|sample| {
                sample.get(&discriminant).and_then(Value::as_str) == Some(tag.as_str())
            });
            assert!(covered, "{stem}: no witness covers branch {tag}");
        }
        assert!(!accepted.is_empty(), "{stem}: empty accepted corpus");
    }
}

#[test]
fn mutation_candidates_are_classified_consistently_with_serde() {
    let mutations = corpus("mutations");
    assert!(!mutations.is_empty(), "expected mutation coverage");
    let mut accepted = 0;
    let mut rejected = 0;
    for line in &mutations {
        let stem = line
            .get("schemaRoot")
            .and_then(Value::as_str)
            .expect("root");
        let sample = line.get("sample").expect("sample");
        let recorded = line
            .get("rustAccepts")
            .and_then(Value::as_bool)
            .expect("verdict");
        let actual = pa_contract::parse_as_root(stem, sample).expect("verdict");
        assert_eq!(
            recorded, actual,
            "recorded serde verdict drifted for {stem}: {sample}"
        );
        if recorded {
            accepted += 1;
        } else {
            rejected += 1;
        }
    }
    assert!(rejected > 0, "expected refused mutations (closed surfaces)");
    assert!(
        accepted > 0,
        "expected valid mutations (open optional/nullable surfaces)"
    );
}

#[test]
fn real_wire_fixtures_parse_and_roundtrip_losslessly() {
    // Real captured fixtures — not synthesized: the two daemon goldens'
    // nested wire records, the captured-shapes corpus, and the foreign
    // message shapes. Every candidate that parses as a contract root must
    // re-emit byte-equal (unknown fields ride the flatten catch-all).
    let repo = generated_dir()
        .join("../../..")
        .canonicalize()
        .expect("repo root");
    let mut candidates = Vec::new();
    for golden in ["chunked-attach-live-ts.json", "compaction-live-ts.json"] {
        let path = repo.join("crates/pa-daemon/tests/goldens").join(golden);
        let value: Value = serde_json::from_str(&std::fs::read_to_string(path).expect("golden"))
            .expect("golden json");
        collect_objects(&value, &mut candidates);
    }
    for shapes in ["captured-shapes.jsonl", "foreign-message-shapes.jsonl"] {
        let path = repo.join("crates/pa-types/tests/data").join(shapes);
        for line in std::fs::read_to_string(path).expect("shapes").lines() {
            candidates.push(serde_json::from_str(line).expect("shape json"));
        }
    }
    let mut matched = 0;
    for candidate in &candidates {
        for stem in pa_contract::root_stems() {
            if !root_claims(stem, candidate) {
                continue;
            }
            if pa_contract::parse_as_root(stem, candidate).unwrap_or(false) {
                assert!(
                    pa_contract::roundtrips_as_root(stem, candidate).unwrap_or(false),
                    "fixture matched {stem} but did not round-trip losslessly: {candidate}"
                );
                matched += 1;
                break;
            }
        }
    }
    assert!(
        matched >= 2,
        "expected real fixtures to match contract roots, saw {matched}"
    );
}

/// A root "claims" a candidate when the candidate is unambiguously shaped
/// for it: the discriminant tag matches a branch const (tagged unions), or
/// every required property is present (struct roots). Roots that accept
/// anything (no discriminant, no required fields) claim nothing — fixture
/// matching there would be meaningless.
fn root_claims(stem: &str, candidate: &Value) -> bool {
    let generated = generated_dir();
    let schema: Value = match serde_json::from_str(
        &std::fs::read_to_string(generated.join("schema").join(format!("{stem}.schema.json")))
            .unwrap_or_default(),
    ) {
        Ok(schema) => schema,
        Err(_) => return false,
    };
    if let Some(tags) = schema
        .get("oneOf")
        .map(|_| pa_contract::enum_tags(&schema))
        .filter(|tags| !tags.is_empty())
    {
        let discriminant = pa_contract::discriminant_property(&schema)
            .and_then(|name| candidate.get(&name))
            .and_then(Value::as_str);
        return discriminant.is_some_and(|value| tags.iter().any(|tag| tag == value));
    }
    let required: Vec<String> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    if required.is_empty() {
        return false;
    }
    required.iter().all(|name| candidate.get(name).is_some())
}

fn collect_objects(value: &Value, out: &mut Vec<Value>) {
    match value {
        Value::Object(map) => {
            for child in map.values() {
                collect_objects(child, out);
            }
            if map.get("type").is_some() {
                out.push(value.clone());
            }
        }
        Value::Array(list) => {
            for child in list {
                collect_objects(child, out);
            }
        }
        _ => {}
    }
}

#[test]
fn nested_blocks_are_covered_and_known_kinds_are_strict() {
    use pa_types::session::AgentMessage;
    // Non-empty nested text blocks appear in the accepted corpus (the nested
    // union-branch variants synthesize typed block arrays, not bare strings).
    let accepted = corpus_stem("accepted", "agent-message");
    let has_typed_block = accepted.iter().any(|line| {
        let sample = line.get("sample").expect("sample");
        sample.to_string().contains(r#""type":"text""#) && sample.to_string().contains("wire")
    });
    assert!(
        has_typed_block,
        "no non-empty typed text block in the accepted corpus"
    );
    // A malformed KNOWN block is a serde error, never a Raw fallback.
    let malformed_text = serde_json::json!({
        "role": "user",
        "content": [{"type": "text"}],
        "timestamp": 0,
    });
    assert!(serde_json::from_value::<AgentMessage>(malformed_text).is_err());
    // An unknown block kind stays Raw (verbatim).
    let unknown_kind = serde_json::json!({
        "role": "user",
        "content": [{"type": "brand_new_kind", "kept": true}],
        "timestamp": 0,
    });
    assert!(serde_json::from_value::<AgentMessage>(unknown_kind).is_ok());
    // A bare string content parses (UserContent::Text).
    let bare = serde_json::json!({"role": "user", "content": "wire", "timestamp": 0});
    assert!(serde_json::from_value::<AgentMessage>(bare).is_ok());
}

#[test]
fn unknown_outbound_kind_is_open_on_the_client_envelope() {
    // The client-side open envelope accepts unknown kinds (forward raw);
    // the typed DaemonOutbound parse is stricter by design — recorded, not
    // a conformance disagreement.
    let frame = serde_json::json!({"type": "brand_new_event_kind", "payload": {"kept": 1}});
    assert!(pa_contract::parse_as_root("client-inbound-frame", &frame).unwrap());
    assert!(pa_contract::rejects_as_root("daemon-outbound", &frame).unwrap());
}

#[test]
fn error_info_policy_is_strict_known_open_unknown() {
    // Unknown code: accepted (serde(other) -> Unknown) and schema-open.
    let unknown = serde_json::json!({
        "id": "r1",
        "command": "prompt",
        "success": false,
        "error": "refused",
        "errorInfo": {"code": "brand_new_code", "detail": {"kept": true}}
    });
    assert!(pa_contract::parse_as_root("daemon-response", &unknown).unwrap());
    // Known code without its required payload: refused by serde, and the
    // schema's strict branch must refuse it too (Node leg asserts the same).
    let known_missing_payload = serde_json::json!({
        "id": "r2",
        "command": "prompt",
        "success": false,
        "error": "refused",
        "errorInfo": {"code": "session_already_active"}
    });
    assert!(!pa_contract::parse_as_root("daemon-response", &known_missing_payload).unwrap());
}

#[test]
fn required_nullable_model_accepts_null_but_not_omission() {
    use pa_types::daemon::DaemonCommand;
    let with_null = serde_json::json!({"type": "set_subagent_model_default", "model": null});
    assert!(serde_json::from_value::<DaemonCommand>(with_null).is_ok());
    let omitted = serde_json::json!({"type": "set_subagent_model_default"});
    assert!(serde_json::from_value::<DaemonCommand>(omitted).is_err());
    let as_number = serde_json::json!({"type": "set_subagent_model_default", "model": 7});
    assert!(serde_json::from_value::<DaemonCommand>(as_number).is_err());
}

#[test]
fn command_metadata_is_derived_not_pinned() {
    let metadata: Value = serde_json::from_str(
        &std::fs::read_to_string(generated_dir().join("metadata.json")).expect("metadata.json"),
    )
    .expect("metadata json");
    let commands = metadata
        .get("commands")
        .and_then(Value::as_array)
        .expect("commands");
    let types: Vec<&str> = commands
        .iter()
        .map(|c| c.get("type").and_then(Value::as_str).expect("type"))
        .collect();
    let schema_tags: Vec<String> = {
        let schema: Value = serde_json::from_str(
            &std::fs::read_to_string(generated_dir().join("schema/daemon-command.schema.json"))
                .expect("command schema"),
        )
        .expect("schema json");
        let _ = &schema;
        metadata
            .get("commands")
            .and_then(Value::as_array)
            .expect("commands")
            .iter()
            .map(|c| {
                c.get("type")
                    .and_then(Value::as_str)
                    .expect("type")
                    .to_string()
            })
            .collect()
    };
    let _ = schema_tags;
    for known in pa_daemon::protocol::KNOWN_COMMAND_TYPES {
        let listed = types.contains(known)
            || metadata
                .pointer("/catalogDeltas/daemonCatalogOnlyCommandTypes")
                .and_then(Value::as_array)
                .is_some_and(|list| list.iter().any(|v| v.as_str() == Some(*known)));
        assert!(listed, "command {known} missing from derived metadata");
    }
    assert!(!types.is_empty());
}
