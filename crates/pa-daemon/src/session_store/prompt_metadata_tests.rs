//! Canonical user-prompt metadata: role, timestamp and persisted cache semantics.
use super::*;

fn append(path: &Path, rows: &[Value]) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    for row in rows {
        writeln!(file, "{row}").unwrap();
    }
}

fn header() -> Value {
    json!({"type":"session", "id":"prompt", "timestamp":"2026-10-19T00:00:00.000Z", "cwd":"/tmp"})
}

fn message(id: &str, role: &str, timestamp: &Value) -> Value {
    json!({"type":"message", "id":id, "timestamp":"2026-10-19T00:00:00.000Z",
        "message":{"role":role, "content":"fixture", "timestamp":timestamp}})
}

#[test]
fn last_user_prompt_metadata_uses_user_role_and_attach_timestamp_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("roles.jsonl");
    append(&path, &[header(), message("a0", "assistant", &json!(9000))]);
    assert_eq!(read_session_info(&path).unwrap().last_user_prompt_ms, None);
    for (index, (timestamp, expected)) in [
        (json!(5000), Some(5000)),
        (json!(1200), Some(1200)), // Persisted order, not maximum timestamp.
        (json!("2026-10-19T00:00:01.000Z"), Some(1_792_368_001_000)),
        (json!(456.9), Some(456)),
        (json!("not a timestamp"), Some(456)),
        (Value::Null, Some(456)),
        (json!(-5), Some(0)), // Same finite-number clamp as attach.
    ]
    .into_iter()
    .enumerate()
    {
        append(
            &path,
            &[
                message(&format!("u{index}"), "user", &timestamp),
                message(&format!("a{index}"), "assistant", &json!(99_000)),
                message(&format!("t{index}"), "toolResult", &json!(100_000)),
                json!({"type":"custom_message", "id":format!("custom{index}"),
                "timestamp":"2026-10-19T02:00:00.000Z", "message":{"role":"user", "timestamp":200_000}}),
                json!({"type":"compaction", "id":format!("compact{index}"),
                "timestamp":"2026-10-19T03:00:00.000Z", "summary":"fixture"}),
                json!({"type":"session_info", "id":format!("name{index}"),
                "timestamp":"2026-10-19T04:00:00.000Z", "name":"renamed"}),
            ],
        );
        let info = read_session_info(&path).unwrap();
        assert_eq!(info.last_user_prompt_ms, expected);
        assert_eq!(info.modified, crate::util::iso_from_unix_ms(99_000));
    }
}

#[cfg(unix)]
#[test]
fn last_user_prompt_metadata_cache_sidecar_and_tail_resume_preserve_the_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resume.jsonl");
    append(&path, &[header(), message("u1", "user", &json!(1000))]);
    assert_eq!(
        read_session_info(&path).unwrap().last_user_prompt_ms,
        Some(1000)
    );
    persist_info_sidecar(&path);
    let sidecar = path.with_extension("info-cache.json");
    let mut state: Value = serde_json::from_slice(&fs::read(&sidecar).unwrap()).unwrap();
    // A certified prefix value absent from the transcript proves the next read did
    // not scan that prefix again. Appended messages must still fold normally.
    state["state"]["acc"]["last_user_prompt_ms"] = json!(777);
    fs::write(&sidecar, state.to_string()).unwrap();
    session_info_cache().lock().unwrap().drop_state(&path);
    assert_eq!(
        read_session_info(&path).unwrap().last_user_prompt_ms,
        Some(777)
    );
    assert_eq!(
        read_session_info(&path).unwrap().last_user_prompt_ms,
        Some(777)
    );
    append(&path, &[message("a1", "assistant", &json!(9000))]);
    assert_eq!(
        read_session_info(&path).unwrap().last_user_prompt_ms,
        Some(777)
    );
    append(&path, &[message("u2", "user", &json!(2000))]);
    assert_eq!(
        read_session_info(&path).unwrap().last_user_prompt_ms,
        Some(2000)
    );
    persist_info_sidecar(&path);
    session_info_cache().lock().unwrap().drop_state(&path);
    assert_eq!(
        read_session_info(&path).unwrap().last_user_prompt_ms,
        Some(2000)
    );
    // The pre-field sidecar version cannot certify a missing prefix timestamp.
    let mut old: Value = serde_json::from_slice(&fs::read(&sidecar).unwrap()).unwrap();
    old["version"] = json!(2);
    old["state"]["acc"]
        .as_object_mut()
        .unwrap()
        .remove("last_user_prompt_ms");
    fs::write(&sidecar, old.to_string()).unwrap();
    session_info_cache().lock().unwrap().drop_state(&path);
    assert_eq!(
        read_session_info(&path).unwrap().last_user_prompt_ms,
        Some(2000)
    );
    let tail = message("u3", "user", &json!(3000)).to_string();
    {
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(tail.as_bytes()).unwrap();
    }
    assert_eq!(
        read_session_info(&path).unwrap().last_user_prompt_ms,
        Some(3000)
    );
    {
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"\n").unwrap();
    }
    assert_eq!(
        read_session_info(&path).unwrap().last_user_prompt_ms,
        Some(3000)
    );
}

#[cfg(unix)]
#[test]
fn last_user_prompt_metadata_iso_and_epoch_sessions_reuse_cached_info() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("iso.jsonl");
    for timestamp in [json!("2026-10-19T00:00:01.000Z"), json!(0)] {
        fs::write(&path, "").unwrap();
        append(&path, &[header(), message("u", "user", &timestamp)]);
        read_session_info(&path).unwrap();
        persist_info_sidecar(&path);
        let sidecar = path.with_extension("info-cache.json");
        let mut state: Value = serde_json::from_slice(&fs::read(&sidecar).unwrap()).unwrap();
        state["state"]["acc"]["last_user_prompt_ms"] = json!(777);
        fs::write(&sidecar, state.to_string()).unwrap();
        session_info_cache().lock().unwrap().drop_state(&path);
        assert_eq!(
            read_session_info(&path).unwrap().last_user_prompt_ms,
            Some(777)
        );
    }
}
