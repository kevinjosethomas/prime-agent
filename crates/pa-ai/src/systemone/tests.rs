use super::*;
use pa_types::decision::{ChoiceQuestion, ChoiceQuestionType};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn request() -> SystemOneRequest {
    SystemOneRequest {
        model: "cloudflare/clef".to_string(),
        rest: serde_json::Map::new(),
        state: json!({"messages": [{"role": "user", "text": "Prepare judging applications"}]}),
        questions: std::collections::BTreeMap::from([(
            "w0".to_string(),
            ChoiceQuestion {
                kind: ChoiceQuestionType::Choice,
                rest: serde_json::Map::new(),
                instructions: "Is this part of the O1 case?".to_string(),
                criteria: std::collections::BTreeMap::from([
                    ("relevant".to_string(), "Part of the case".to_string()),
                    ("unrelated".to_string(), "Unrelated to the case".to_string()),
                ]),
            },
        )]),
    }
}

fn reply() -> Value {
    json!({"model": "cloudflare/clef", "answers": {
        "w0": {"type": "choice", "choice": "relevant", "confidence": 0.9,
            "probabilities": {"relevant": 0.9, "unrelated": 0.1}}
    }})
}

async fn server(status: u16, body: String) -> (String, tokio::sync::oneshot::Receiver<Value>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/api/v1/", listener.local_addr().unwrap());
    let (sender, receiver) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut raw = Vec::new();
        let mut chunk = [0u8; 4_096];
        let (head, data) = loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0);
            raw.extend_from_slice(&chunk[..read]);
            let text = String::from_utf8_lossy(&raw);
            let Some((head, data)) = text.split_once("\r\n\r\n") else {
                continue;
            };
            let length = head
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")?
                        .trim()
                        .parse::<usize>()
                        .ok()
                })
                .unwrap();
            if data.len() >= length {
                break (head.to_string(), data.to_string());
            }
        };
        let headers: std::collections::BTreeMap<String, String> = head
            .lines()
            .skip(1)
            .filter_map(|line| {
                let (key, value) = line.split_once(':')?;
                Some((key.to_ascii_lowercase(), value.trim().to_string()))
            })
            .collect();
        let _ = sender.send(json!({
            "path": head.lines().next().unwrap(), "headers": headers,
            "body": serde_json::from_str::<Value>(&data).unwrap()
        }));
        let response = format!(
            "HTTP/1.1 {status} Status\r\ncontent-type: application/json\r\nlocation: http://127.0.0.1:1/blocked\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    (base, receiver)
}

#[tokio::test]
async fn posts_typed_evaluation_with_merged_auth_and_team_headers() {
    let expected = reply();
    let (base, seen) = server(200, expected.to_string()).await;
    let options = StreamOptions {
        api_key: Some("fixture-key".to_string()),
        headers: Some(std::collections::HashMap::from([(
            "X-Prime-Team-ID".to_string(),
            "fixture-team".to_string(),
        )])),
        timeout_ms: Some(2_000),
        ..Default::default()
    };
    let request = request();
    let result = evaluate(&base, &request, &options).await.unwrap();
    assert_eq!(result, serde_json::from_value(expected).unwrap());
    let seen = seen.await.unwrap();
    assert_eq!(seen["path"], "POST /api/v1/systemone HTTP/1.1");
    assert_eq!(seen["headers"]["authorization"], "Bearer fixture-key");
    assert_eq!(seen["headers"]["x-prime-team-id"], "fixture-team");
    assert_eq!(seen["body"], serde_json::to_value(request).unwrap());
    assert!(seen["body"].get("messages").is_none());
    assert!(seen["body"].get("tools").is_none());
}

#[tokio::test]
async fn provider_errors_never_reflect_body_or_credentials_and_redirects_are_not_followed() {
    for status in [401, 404, 500, 302] {
        let (base, _) = server(status, "fixture-secret echoed by upstream".to_string()).await;
        let error = evaluate(&base, &request(), &StreamOptions::default())
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("System One evaluation returned HTTP {status}")
        );
    }
}

#[tokio::test]
async fn malformed_and_oversized_responses_are_rejected_without_reflection() {
    for body in [
        "not JSON fixture-secret".to_string(),
        "x".repeat(MAX_RESPONSE_BYTES + 1),
    ] {
        let (base, _) = server(200, body).await;
        let error = evaluate(&base, &request(), &StreamOptions::default())
            .await
            .unwrap_err();
        assert_eq!(error.to_string(), "Invalid System One evaluation response");
    }
}

#[tokio::test]
async fn cancelled_evaluation_returns_without_contacting_a_provider() {
    let signal = tokio_util::sync::CancellationToken::new();
    signal.cancel();
    let options = StreamOptions {
        signal: Some(signal),
        ..Default::default()
    };
    let result = evaluate("http://127.0.0.1:1", &request(), &options).await;
    assert!(matches!(result, Err(SystemOneError::Cancelled)));
}

#[tokio::test]
async fn rejects_invalid_question_count_before_network() {
    let mut request = request();
    request.questions = (0..33)
        .map(|index| (format!("w{index}"), request.questions["w0"].clone()))
        .collect();
    assert!(matches!(
        evaluate("http://127.0.0.1:1", &request, &StreamOptions::default()).await,
        Err(SystemOneError::InvalidRequest)
    ));
}

#[tokio::test]
async fn rejects_oversized_evaluation_state_before_network() {
    let mut request = request();
    request.state = json!({"text": "x".repeat(MAX_REQUEST_BYTES)});
    assert!(matches!(
        evaluate("http://127.0.0.1:1", &request, &StreamOptions::default()).await,
        Err(SystemOneError::InvalidRequest)
    ));
}

#[tokio::test]
async fn a_hanging_provider_is_bounded_by_the_request_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (ready, accepted) = tokio::sync::oneshot::channel();
    let provider = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 4_096];
        assert_ne!(socket.read(&mut buffer).await.unwrap(), 0);
        let _ = ready.send(());
        std::future::pending::<()>().await;
        drop(socket);
    });
    let evaluation = tokio::spawn(async move {
        evaluate(
            &base,
            &request(),
            &StreamOptions {
                timeout_ms: Some(100),
                ..Default::default()
            },
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), accepted)
        .await
        .unwrap()
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), evaluation)
        .await
        .unwrap()
        .unwrap();
    provider.abort();
    assert!(matches!(result, Err(SystemOneError::Timeout)));
}

#[tokio::test]
async fn a_hanging_response_body_preserves_the_timeout_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let provider = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 4_096];
        assert_ne!(socket.read(&mut buffer).await.unwrap(), 0);
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1024\r\n\r\n").await.unwrap();
        std::future::pending::<()>().await;
        drop(socket);
    });
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        evaluate(
            &base,
            &request(),
            &StreamOptions {
                timeout_ms: Some(100),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    provider.abort();
    assert!(matches!(result, Err(SystemOneError::Timeout)));
}

#[tokio::test]
async fn merged_authorization_header_overrides_the_bearer_default() {
    let (base, seen) = server(200, reply().to_string()).await;
    let options = StreamOptions {
        api_key: Some("default-fixture-key".to_string()),
        headers: Some(std::collections::HashMap::from([(
            "Authorization".to_string(),
            "Bearer merged-fixture-key".to_string(),
        )])),
        ..Default::default()
    };
    evaluate(&base, &request(), &options).await.unwrap();
    assert_eq!(
        seen.await.unwrap()["headers"]["authorization"],
        "Bearer merged-fixture-key"
    );
}
