use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::BufReader;

fn request() -> Value {
    json!({
        "id": "route-a",
        "state": {"target": {"from": "kevin", "text": "ship it"}},
        "questions": {"task": {
            "type": "choice",
            "instructions": "Which task is the target about?",
            "criteria": {"t0": "Primer", "none": "General"}
        }}
    })
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
async fn ready_probe_with_eof_never_evaluates() {
    let mut output = Vec::new();
    serve(BufReader::new(&b""[..]), &mut output, |_| async {
        panic!("ready probe must not evaluate")
    })
    .await
    .unwrap();
    assert_eq!(
        rows(output),
        vec![json!({"type": "ready", "protocolVersion": 1})]
    );
}

#[tokio::test]
async fn result_echoes_id_and_answers_and_evaluates_once() {
    let mut bytes = line(&request());
    bytes.extend(line(&request()));
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut output = Vec::new();
    serve(BufReader::new(bytes.as_slice()), &mut output, move |request| async move {
        count.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.questions["task"].criteria.len(), 2);
        assert_eq!(request.state["target"]["text"], "ship it");
        Ok(json!({"task": {"choice": "t0", "confidence": 0.9, "probabilities": {"t0": 0.95, "none": 0.05}}}))
    })
    .await
    .unwrap();
    let rows = rows(output);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["type"], "result");
    assert_eq!(rows[1]["id"], "route-a");
    assert_eq!(rows[1]["answers"]["task"]["choice"], "t0");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_requests_never_evaluate() {
    let mut extra = request();
    extra["model"] = json!("other/model");
    let mut no_questions = request();
    no_questions["questions"] = json!({});
    let mut one_option = request();
    one_option["questions"]["task"]["criteria"] = json!({"t0": "Primer"});
    let mut scalar_state = request();
    scalar_state["state"] = json!("text");
    let mut long = request();
    long["state"]["pad"] = json!("x".repeat(MAX_REQUEST_BYTES));
    for value in [extra, no_questions, one_option, scalar_state, long] {
        let mut output = Vec::new();
        serve(
            BufReader::new(line(&value).as_slice()),
            &mut output,
            |_| async { panic!("invalid request must not evaluate") },
        )
        .await
        .unwrap();
        let last = rows(output).pop().unwrap();
        assert_eq!(last["type"], "error");
        assert_eq!(last["code"], "invalid_request");
        assert_eq!(last["id"], Value::Null);
    }
}

#[tokio::test]
async fn evaluation_errors_are_fixed_messages_with_the_request_id() {
    let mut output = Vec::new();
    serve(
        BufReader::new(line(&request()).as_slice()),
        &mut output,
        |_| async { Err("Task route evaluation is unavailable") },
    )
    .await
    .unwrap();
    assert_eq!(
        rows(output).pop().unwrap(),
        json!({"type": "error", "id": "route-a", "code": "unavailable", "message": "Task route evaluation is unavailable"})
    );
}

#[test]
fn missing_auth_is_reported_before_any_request() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        credentials(dir.path()).unwrap_err(),
        "Prime Inference authentication is unavailable"
    );
}
