use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn request() -> Value {
    json!({"id":"request-a","target":{"id":"user-new","role":"user","text":"Can you clarify that?"},"recentMessages":[],"candidates":[{"id":"thread-a","messages":[{"id":"assistant-old","role":"assistant","text":"The helper runs separately."}]}]})
}

#[tokio::test]
async fn ready_probe_with_eof_never_invokes_auth_or_evaluation() {
    let mut output = Vec::new();
    serve(BufReader::new(&b""[..]), &mut output, |_| async {
        panic!("ready probe must not evaluate")
    })
    .await
    .unwrap();
    let rows = String::from_utf8(output).unwrap();
    assert_eq!(rows.lines().count(), 1);
    let ready: Value = serde_json::from_str(rows.trim()).unwrap();
    assert_eq!(ready, json!({"type":"ready","protocolVersion":1}));
}
#[tokio::test]
async fn response_echoes_request_and_target_ids_and_evaluates_only_once() {
    let mut bytes = serde_json::to_vec(&request()).unwrap();
    bytes.push(b'\n');
    bytes.extend_from_slice(&serde_json::to_vec(&request()).unwrap());
    bytes.push(b'\n');
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut output = Vec::new();
    serve(
        BufReader::new(bytes.as_slice()),
        &mut output,
        move |request| async move {
            count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.target.id, "user-new");
            Ok(vec![ThreadScore {
                id: "thread-a".to_string(),
                related: 0.8,
                unrelated: 0.2,
                confidence: 0.9,
            }])
        },
    )
    .await
    .unwrap();
    let rows = String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["type"], "result");
    assert_eq!(rows[1]["id"], "request-a");
    assert_eq!(rows[1]["targetId"], "user-new");
    assert_eq!(rows[1]["scores"][0]["id"], "thread-a");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn invalid_or_empty_candidates_never_resolve_credentials() {
    for invalid in [false, true] {
        let mut value = request();
        if invalid {
            value["target"]["role"] = json!("system");
        } else {
            value["candidates"] = json!([]);
        }
        let mut bytes = serde_json::to_vec(&value).unwrap();
        bytes.push(b'\n');
        let mut output = Vec::new();
        serve(BufReader::new(bytes.as_slice()), &mut output, |_| async {
            panic!("must not evaluate")
        })
        .await
        .unwrap();
        let last: Value =
            serde_json::from_str(String::from_utf8(output).unwrap().lines().last().unwrap())
                .unwrap();
        assert_eq!(last["type"], if invalid { "error" } else { "result" });
        if !invalid {
            assert_eq!(last["scores"], json!([]));
        }
    }
}
#[tokio::test(start_paused = true)]
async fn helper_deadline_returns_sanitized_timeout_without_waiting_for_model() {
    let (server, client) = tokio::io::duplex(65536);
    let (reader, writer) = tokio::io::split(server);
    let task = tokio::spawn(serve(BufReader::new(reader), writer, |_| async {
        std::future::pending::<Result<Vec<ThreadScore>, String>>().await
    }));
    let (reader, mut writer) = tokio::io::split(client);
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let mut bytes = serde_json::to_vec(&request()).unwrap();
    bytes.push(b'\n');
    writer.write_all(&bytes).await.unwrap();
    line.clear();
    reader.read_line(&mut line).await.unwrap();
    let error: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(error["type"], "error");
    assert_eq!(error["code"], "timeout");
    assert_eq!(error["id"], "request-a");
    task.await.unwrap().unwrap();
}
