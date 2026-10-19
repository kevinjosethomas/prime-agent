use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::BufReader;

fn request() -> Value {
    json!({"id": "route-a", "model": "google/gemini-3-flash-preview", "system": "Pick a task.", "user": "TARGET: ship it", "maxTokens": 200})
}

fn rows(output: Vec<u8>) -> Vec<Value> {
    String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn line(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    bytes
}

#[tokio::test]
async fn ready_probe_with_eof_never_completes() {
    let mut output = Vec::new();
    serve(BufReader::new(&b""[..]), &mut output, |_| async {
        panic!("ready probe must not complete")
    })
    .await
    .unwrap();
    assert_eq!(rows(output), vec![json!({"type": "ready", "protocolVersion": 1})]);
}

#[tokio::test]
async fn result_echoes_id_text_and_usage_and_completes_once() {
    let mut bytes = line(&request());
    bytes.extend(line(&request()));
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut output = Vec::new();
    serve(BufReader::new(bytes.as_slice()), &mut output, move |request| async move {
        count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.user, "TARGET: ship it");
        assert_eq!(request.max_tokens, 200);
        Ok(RouteResponse { text: "{\"task\":\"a\"}".to_string(), input: 12, output: 3 })
    })
    .await
    .unwrap();
    let rows = rows(output);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[1],
        json!({"type": "result", "id": "route-a", "text": "{\"task\":\"a\"}", "usage": {"input": 12, "output": 3}})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_requests_never_complete() {
    let mut extra = request();
    extra["tools"] = json!([]);
    let mut empty_user = request();
    empty_user["user"] = json!("  ");
    let mut no_tokens = request();
    no_tokens["maxTokens"] = json!(0);
    let mut too_many = request();
    too_many["maxTokens"] = json!(MAX_OUTPUT_TOKENS + 1);
    let mut long_user = request();
    long_user["user"] = json!("x".repeat(MAX_USER_BYTES + 1));
    for value in [extra, empty_user, no_tokens, too_many, long_user] {
        let mut output = Vec::new();
        serve(BufReader::new(line(&value).as_slice()), &mut output, |_| async {
            panic!("invalid request must not complete")
        })
        .await
        .unwrap();
        let last = rows(output).pop().unwrap();
        assert_eq!(last["type"], "error");
        assert_eq!(last["code"], "invalid_request");
        assert_eq!(last["id"], Value::Null);
    }
}

#[tokio::test]
async fn completion_errors_are_fixed_messages_with_the_request_id() {
    let mut output = Vec::new();
    serve(BufReader::new(line(&request()).as_slice()), &mut output, |_| async {
        Err("Task route completion failed")
    })
    .await
    .unwrap();
    assert_eq!(
        rows(output).pop().unwrap(),
        json!({"type": "error", "id": "route-a", "code": "unavailable", "message": "Task route completion failed"})
    );
}

#[test]
fn unknown_models_are_rejected_before_any_request() {
    let dir = tempfile::tempdir().unwrap();
    let error = model_and_auth(dir.path(), "not-a-model/anywhere").unwrap_err();
    assert_eq!(error, "Task route model is not a Prime Inference model");
}
