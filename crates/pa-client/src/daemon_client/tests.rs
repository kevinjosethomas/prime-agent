use super::*;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

/// Minimal scripted supervisor used by client tests: hello on connect,
/// canned responses keyed by command type.
async fn spawn_mock_daemon(listener: UnixListener) {
    let (stream, _) = listener.accept().await.expect("accept");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let hello = json!({
        "type": "daemon_hello",
        "protocol": { "name": "prime-agent.daemon", "version": 7 },
        "clientId": "srv",
        "serverCapabilities": [],
    });
    let mut line = serde_json::to_string(&hello).unwrap();
    line.push('\n');
    writer.write_all(line.as_bytes()).await.unwrap();
    let mut seen = String::new();
    loop {
        seen.clear();
        match reader.read_line(&mut seen).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let envelope: Value = serde_json::from_str(seen.trim()).unwrap();
        let id = envelope["id"].as_str().unwrap().to_string();
        let command_type = envelope["command"]["type"].as_str().unwrap();
        if command_type == "prompt" {
            // Stream an event before the response, like the real worker.
            let event = json!({
                "type": "session_event",
                "activeSessionId": "s1",
                "event": { "type": "turn_end" },
            });
            let mut payload = serde_json::to_string(&event).unwrap();
            payload.push('\n');
            writer.write_all(payload.as_bytes()).await.unwrap();
        }
        let response = json!({
            "type": "response",
            "id": id,
            "command": command_type,
            "success": true,
            "data": { "ok": true },
        });
        let mut payload = serde_json::to_string(&response).unwrap();
        payload.push('\n');
        writer.write_all(payload.as_bytes()).await.unwrap();
    }
}

fn empty_prompt_input() -> pa_types::daemon::PromptInput {
    pa_types::daemon::PromptInput {
        content: None,
        images: None,
        streaming_behavior: None,
        queue_if_busy: None,
        expand_prompt_templates: None,
        source: None,
        agent_message_id: None,
        custom_message: None,
        queue_key: None,
        prefix_messages: None,
        client_input_id: None,
        reply_to_message_id: None,
        reply_context: None,
        admission_id: None,
        rlm_notice_nonce: None,
    }
}

#[tokio::test]
async fn handshake_and_request_round_trip() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move { spawn_mock_daemon(listener).await });

    let (client, mut events) = DaemonClient::connect(&socket).await.unwrap();
    assert_eq!(client.protocol().version, 7);
    let data = client
        .request_ok(DaemonCommand::List {
            id: None,
            all: None,
            cwd: None,
            session_dir: None,
            include_client_owned: None,
            rest: Map::default(),
        })
        .await
        .unwrap();
    assert_eq!(data["ok"], true);

    client
        .request_ok(DaemonCommand::Prompt {
            id: None,
            active_session_id: "s1".to_string(),
            message: "hi".to_string(),
            input: empty_prompt_input(),
            rest: Map::default(),
        })
        .await
        .unwrap();
    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(
            event,
            DaemonClientEvent::SessionEvent { ref event, .. } if event["type"] == "turn_end"
        ),
        "unexpected event: {event:?}"
    );
    client.close();
}

#[tokio::test]
async fn request_timeout_reports_socket() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    // A daemon that sends hello but never answers commands.
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut writer = stream;
        let mut hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": [],
        })
        .to_string();
        hello.push('\n');
        writer.write_all(hello.as_bytes()).await.unwrap();
        std::future::pending::<()>().await;
    });
    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    let error = client
        .request_with_timeout(
            DaemonCommand::List {
                id: None,
                all: None,
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: Map::default(),
            },
            100,
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Timed out after"),
        "unexpected error: {error}"
    );
    assert!(error
        .to_string()
        .contains(socket.display().to_string().as_str()));
    assert!(!is_daemon_rejection(&error));
    // The envelope was queued before the timeout elapsed: a post-write
    // timeout is an uncertain delivery.
    assert!(is_command_result_uncertain(&error));
}

#[tokio::test]
async fn a_request_after_the_reader_died_refuses_instead_of_riding_the_budget() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    // A daemon that greets, then drops the socket.
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut writer = stream;
        let mut hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": [],
        })
        .to_string();
        hello.push('\n');
        writer.write_all(hello.as_bytes()).await.unwrap();
        drop(writer);
    });
    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    // Observable readiness: wait for the reader's death watch before
    // sending (the failure pass has run by then, so the request would
    // register after it — the exact race the refusal closes).
    let mut reader_dead = client.reader_dead();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !*reader_dead.borrow_and_update() {
            reader_dead
                .changed()
                .await
                .expect("the death watch stays live");
        }
    })
    .await
    .expect("the reader death watch fires when the socket closes");
    // A request budget far beyond the refusal bound: without the
    // refusal the send would ride it out and this await would outlive
    // the one-second failure bound.
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        client.request_with_timeout(
            DaemonCommand::List {
                id: None,
                all: None,
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: Map::default(),
            },
            30_000,
        ),
    )
    .await
    .expect("a dead reader refuses the send instead of riding the budget")
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("the daemon connection is closed"),
        "unexpected error: {error}"
    );
    assert!(error
        .to_string()
        .contains(socket.display().to_string().as_str()));
    // The refusal is a transport failure: transient for the submit path
    // (the pane stays mounted for the reconnect driver), never a daemon
    // rejection.
    assert!(is_daemon_unreachable(&error));
    assert!(!is_daemon_rejection(&error));
    // The request never queued to the writer: a provable non-delivery.
    assert!(is_command_not_sent(&error));
    assert!(!is_command_result_uncertain(&error));
}

#[test]
fn failed_response_is_a_typed_rejection() {
    let response = DaemonResponse {
        id: None,
        command: "prompt".to_string(),
        success: false,
        data: None,
        error: Some("Prompt cannot be empty".to_string()),
        error_info: None,
    };
    let error = response_data_or_error("prompt", response).unwrap_err();
    assert!(is_daemon_rejection(&error));
    let rejection = error
        .downcast_ref::<RequestRejected>()
        .expect("typed rejection");
    assert_eq!(rejection.command, "prompt");
    assert_eq!(rejection.message, "Prompt cannot be empty");
    assert_eq!(
        rejection.to_string(),
        "the daemon rejected the prompt request: Prompt cannot be empty"
    );
}

#[test]
fn model_catalog_changed_parses_to_the_refresh_event() {
    // The worker's broadcast frame parses into the refresh event:
    // unknown payloads stay None, this one never drops silently.
    let value = json!({"type": "model_catalog_changed"});
    assert!(matches!(
        client_event_from_value(&value),
        Some(DaemonClientEvent::ModelCatalogChanged)
    ));
}

/// The layout handoff keys the stash on the LATEST sequence the worker
/// reported, so `meta.sequence` must ride the parsed session event.
#[test]
fn session_event_parses_the_meta_sequence_for_the_handoff_tracker() {
    let with_sequence = json!({
        "type": "session_event",
        "activeSessionId": "sess-1",
        "event": { "type": "message_end" },
        "meta": {
            "id": "sess-1:41",
            "sequence": 41,
            "cursor": { "generation": "g-1", "sequence": 41 }
        }
    });
    match client_event_from_value(&with_sequence) {
        Some(DaemonClientEvent::SessionEvent { meta_sequence, .. }) => {
            assert_eq!(meta_sequence, 41);
        }
        other => panic!("the frame must parse as a session event: {other:?}"),
    }
    // The cursor's sequence is the fallback shape; a frame without
    // either collapses to zero (the tracker's monotonic max ignores
    // it - an unkeyed event never lowers the tracked sequence).
    let cursor_only = json!({
        "type": "session_event",
        "activeSessionId": "sess-1",
        "event": { "type": "message_end" },
        "meta": { "cursor": { "generation": "g-1", "sequence": 12 } }
    });
    match client_event_from_value(&cursor_only) {
        Some(DaemonClientEvent::SessionEvent { meta_sequence, .. }) => {
            assert_eq!(meta_sequence, 12);
        }
        other => panic!("the frame must parse as a session event: {other:?}"),
    }
    let no_meta = json!({
        "type": "session_event",
        "activeSessionId": "sess-1",
        "event": { "type": "message_end" }
    });
    match client_event_from_value(&no_meta) {
        Some(DaemonClientEvent::SessionEvent { meta_sequence, .. }) => {
            assert_eq!(meta_sequence, 0);
        }
        other => panic!("the frame must parse as a session event: {other:?}"),
    }
}

#[test]
fn plain_errors_are_not_rejections() {
    let error = anyhow!("the daemon connection is closed");
    assert!(!is_daemon_rejection(&error));
}

/// TS #2391 `update_restarting` wire round-trip: the typed info
/// survives the wire to a `RequestRejected`, and an unrelated refusal
/// never classifies as the update-restart transient state.
#[test]
fn update_restarting_rejection_round_trips_the_wire() {
    let line = serde_json::from_str::<DaemonResponse>(
        r#"{"type":"response","command":"create","success":false,"error":"Daemon is preparing an update restart","errorInfo":{"code":"update_restarting"}}"#,
    )
    .expect("typed refusal parses");
    let error = response_data_or_error("create", line).unwrap_err();
    assert!(is_update_restarting_rejection(&error));
    let rejection = error
        .downcast_ref::<RequestRejected>()
        .expect("typed rejection");
    assert_eq!(
        rejection.error_info,
        Some(pa_types::daemon::DaemonErrorInfo::UpdateRestarting)
    );
    let legacy = serde_json::from_str::<DaemonResponse>(
        r#"{"type":"response","command":"create","success":false,"error":"Daemon is preparing an update restart"}"#,
    )
    .expect("legacy refusal parses");
    let error = response_data_or_error("create", legacy).unwrap_err();
    assert!(is_update_restarting_rejection(&error));
    let other = serde_json::from_str::<DaemonResponse>(
        r#"{"type":"response","command":"create","success":false,"error":"Unknown active session: active-gap"}"#,
    )
    .expect("unrelated refusal parses");
    let error = response_data_or_error("create", other).unwrap_err();
    assert!(!is_update_restarting_rejection(&error));
}

/// The session-addressed lanes' kernel-not-running refusal (the
/// `/factory off` guard's definitive-zero class): the exact wire string
/// classifies through the plain-message predicate, and no other refusal
/// does — the guard's zero read must not fire on an unknown-command or
/// initializing refusal, which stay unreadable and fail closed.
#[test]
fn kernel_not_running_refusal_round_trips_the_wire() {
    let line = serde_json::from_str::<DaemonResponse>(
        r#"{"type":"response","command":"factory_activity","success":false,"error":"Kernel is not running"}"#,
    )
    .expect("refusal parses");
    let error = response_data_or_error("factory_activity", line).unwrap_err();
    assert!(is_kernel_not_running(&error));
    // A refusal that merely embeds the phrase is not the class.
    let wrapped = serde_json::from_str::<DaemonResponse>(
        r#"{"type":"response","command":"factory_activity","success":false,"error":"the session reports: Kernel is not running for this lane"}"#,
    )
    .expect("unrelated refusal parses");
    let error = response_data_or_error("factory_activity", wrapped).unwrap_err();
    assert!(!is_kernel_not_running(&error));
    // The adjacent unreadable classes never classify as the zero read.
    for message in [
        "Session is still initializing",
        "Unknown worker command: factory_activity",
    ] {
        let unrelated = serde_json::from_str::<DaemonResponse>(&format!(
            r#"{{"type":"response","command":"factory_activity","success":false,"error":"{message}"}}"#
        ))
        .expect("unrelated refusal parses");
        let error = response_data_or_error("factory_activity", unrelated).unwrap_err();
        assert!(!is_kernel_not_running(&error), "{message}");
    }
    // A transport failure is not a refusal at all.
    let plain = anyhow!("the daemon connection is closed");
    assert!(!is_kernel_not_running(&plain));
}

#[test]
fn fail_pending_resolves_a_transport_failure_not_a_refusal() {
    // The reader task's dead-connection failure must stay on the
    // transport half of the pending channel: a closed socket can never
    // masquerade as a daemon refusal and keep the UI alive.
    let shared = Shared::default();
    let (tx, rx) = oneshot::channel();
    shared.register_pending("daemon_1".to_string(), tx, None);
    shared.fail_pending("daemon_", "the daemon connection closed");
    let error = rx.blocking_recv().unwrap().unwrap_err();
    assert!(!is_daemon_rejection(&error));
    assert_eq!(error.to_string(), "the daemon connection closed");
    // The sweep is always conservatively uncertain.
    assert!(is_command_result_uncertain(&error));
}

/// A scripted worker socket for the direct-link tests: hello frame,
/// `peer_auth` acceptance, one attach response, then one streamed event.
async fn spawn_mock_worker(listener: tokio::net::UnixListener) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (stream, _) = listener.accept().await.expect("worker accept");
    let (mut reader, mut writer) = stream.into_split();
    let hello = pa_types::daemon::framing::encode_private_frame(
        &serde_json::json!({ "kind": "outbound", "outboundType": "daemon_hello" }),
        br#"{"type":"daemon_hello","protocol":{"name":"prime-agent.daemon","version":7}}"#,
        pa_types::daemon::framing::DEFAULT_PRIVATE_FRAME_LIMITS,
    )
    .unwrap();
    writer.write_all(&hello).await.unwrap();

    let mut pending = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut attached = false;
    loop {
        let read = reader.read(&mut chunk).await.expect("worker read");
        if read == 0 {
            break;
        }
        pending.extend_from_slice(&chunk[..read]);
        let mut decoder = pa_types::daemon::framing::PrivateFrameDecoder::new(
            pa_types::daemon::framing::DEFAULT_PRIVATE_FRAME_LIMITS,
        );
        let frames = decoder.push(&pending).expect("decode frames");
        pending.clear();
        for frame in frames {
            let command_type = frame.header["commandType"].as_str().unwrap_or_default();
            let request_id = frame.header["requestId"].as_str().unwrap_or_default();
            let (success, data) = match command_type {
                "peer_auth" => (
                    true,
                    Some(serde_json::json!({
                        "workerInstanceId": "inst-1",
                        "activeSessionId": "s1",
                        "purpose": "session_client",
                    })),
                ),
                "attach" => {
                    attached = true;
                    (
                        true,
                        Some(serde_json::json!({
                            "activeSessionId": "s1",
                            "snapshot": { "messages": [], "summary": {} },
                        })),
                    )
                }
                "get_state" => (
                    true,
                    Some(serde_json::json!({ "id": "s1", "activity": "idle" })),
                ),
                _ => (false, None),
            };
            let response = serde_json::json!({
                "id": request_id,
                "type": "response",
                "command": command_type,
                "success": success,
                "data": data,
            });
            let frame = pa_types::daemon::framing::encode_private_frame(
                &serde_json::json!({
                    "kind": "outbound",
                    "requestId": request_id,
                    "outboundType": "response",
                }),
                &serde_json::to_vec(&response).unwrap(),
                pa_types::daemon::framing::DEFAULT_PRIVATE_FRAME_LIMITS,
            )
            .unwrap();
            writer.write_all(&frame).await.unwrap();
        }
        if attached {
            // Stream one event, then wait for the client to close.
            let event = serde_json::json!({
                "type": "session_event",
                "activeSessionId": "s1",
                "event": { "type": "turn_end" },
            });
            let frame = pa_types::daemon::framing::encode_private_frame(
                &serde_json::json!({
                    "kind": "outbound",
                    "outboundType": "session_event",
                    "activeSessionId": "s1",
                }),
                &serde_json::to_vec(&event).unwrap(),
                pa_types::daemon::framing::DEFAULT_PRIVATE_FRAME_LIMITS,
            )
            .unwrap();
            writer.write_all(&frame).await.unwrap();
            attached = false;
        }
    }
}

// One scripted end-to-end upgrade scenario (supervisor + worker + event flow);
// splitting would add indirection without changing the flow.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn direct_upgrade_routes_attach_and_streams_events() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let worker_socket = dir.path().join("w.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let worker_listener = tokio::net::UnixListener::bind(&worker_socket).unwrap();
    tokio::spawn(async move { spawn_mock_worker(worker_listener).await });

    // Scripted supervisor: advertises direct_peer_transport, answers the
    // ticket request with a ticket for the mock worker socket.
    let ticket_socket = worker_socket.clone();
    let supervisor = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("supervisor accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = tokio::io::BufReader::new(reader);
        let mut line = String::new();
        let hello = serde_json::json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "serverCapabilities": ["direct_peer_transport"],
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
            let envelope: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
            let id = envelope["id"].as_str().unwrap().to_string();
            let command_type = envelope["command"]["type"].as_str().unwrap();
            let response = if command_type == "get_direct_worker_transport" {
                let identity = pa_types::platform::socket_identity(&ticket_socket).unwrap();
                serde_json::json!({
                    "type": "response",
                    "id": id,
                    "command": command_type,
                    "success": true,
                    "data": {
                        "purpose": "session_client",
                        "socketPath": ticket_socket.to_string_lossy(),
                        "socketIdentity": { "dev": identity.dev, "ino": identity.ino },
                        "workerInstanceId": "inst-1",
                        "activeSessionId": "s1",
                        "grantId": "g1",
                        "token": "t",
                        "expiresAt": "2999-01-01T00:00:00.000Z",
                    },
                })
            } else {
                serde_json::json!({
                    "type": "response",
                    "id": id,
                    "command": command_type,
                    "success": true,
                    "data": { "ok": true },
                })
            };
            writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        }
    });

    let (client, mut events) = DaemonClient::connect(&socket).await.unwrap();
    assert!(
        crate::direct_transport::supervisor_supports_direct(client.hello()),
        "capability advertised"
    );
    assert!(client.upgrade_direct("s1").await.unwrap());
    assert_eq!(client.direct_session_id().as_deref(), Some("s1"));

    let data = client
        .request_ok(DaemonCommand::Attach {
            id: None,
            active_session_id: "s1".to_string(),
            client_id: None,
            capabilities: None,
            resume_cursor: None,
            telemetry_disabled: None,
            recovery_config: None,
            env: None,
            launch_env: None,
            rest: Map::default(),
        })
        .await
        .unwrap();
    assert_eq!(data["activeSessionId"], "s1");
    let state = client
        .request_ok(DaemonCommand::GetState {
            id: None,
            active_session_id: "s1".to_string(),
            rest: Map::default(),
        })
        .await
        .unwrap();
    assert_eq!(state["id"], "s1");
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(
            event,
            DaemonClientEvent::SessionEvent { ref event, .. } if event["type"] == "turn_end"
        ),
        "unexpected event: {event:?}"
    );
    client.close();
    let _ = supervisor.await;
}

#[tokio::test]
async fn dead_connection_fails_pending_requests_immediately() {
    // The supervisor dies after the handshake while a request is in flight:
    // the reader task must fail the pending request at once (the exit-hang
    // class).
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut writer = stream;
        let mut hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": [],
        })
        .to_string();
        hello.push('\n');
        writer.write_all(hello.as_bytes()).await.unwrap();
        // Hold the accept open until the request is in flight, then
        // close the socket (the supervisor process dies).
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(writer);
    });
    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    // The default timeout is 30s; the failure must land in well under a
    // second because the socket died, not because a timer fired.
    let started = std::time::Instant::now();
    let error = client
        .request_ok(DaemonCommand::List {
            id: None,
            all: None,
            cwd: None,
            session_dir: None,
            include_client_owned: None,
            rest: Map::default(),
        })
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(
        error.to_string().contains("the daemon connection closed"),
        "unexpected error: {error}"
    );
    // The dead-connection failure is transport, never a daemon refusal.
    assert!(!is_daemon_rejection(&error));
    // The line was queued to the writer before the socket died: the sweep
    // is conservatively an uncertain delivery, never a provable non-delivery.
    assert!(is_command_result_uncertain(&error));
    assert!(!is_command_not_sent(&error));
    client.close();
    let _ = handle.await;
}

#[tokio::test]
async fn without_capability_upgrade_is_a_no_op() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move { spawn_mock_daemon(listener).await });
    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    assert!(!crate::direct_transport::supervisor_supports_direct(
        client.hello()
    ));
    assert!(!client.upgrade_direct("s1").await.unwrap());
    assert_eq!(client.direct_session_id(), None);
    client.close();
}

/// The fully-meta'd streamed event and the unknown future frame the tap
/// tests assert on, verbatim.
fn tap_test_frames() -> (Value, Value) {
    let event = json!({
        "type": "session_event",
        "activeSessionId": "s1",
        "event": { "type": "turn_end" },
        "meta": {
            "id": "s1:41",
            "emittedAt": "2026-10-20T00:00:00.000Z",
            "sequence": 41,
            "cursor": { "generation": "g-1", "sequence": 41 },
            "replayed": true,
        },
    });
    let future = json!({ "type": "future_frame", "novelty": { "deep": [1, 2, 3] } });
    (event, future)
}

/// A supervisor that greets, streams the tap tests' two application
/// frames, then answers commands.
async fn spawn_tap_daemon(listener: UnixListener) {
    let (stream, _) = listener.accept().await.expect("accept");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let (event, future) = tap_test_frames();
    let hello = json!({
        "type": "daemon_hello",
        "protocol": { "name": "prime-agent.daemon", "version": 7 },
        "clientId": "srv",
        "serverCapabilities": [],
    });
    writer
        .write_all(format!("{hello}\n").as_bytes())
        .await
        .unwrap();
    writer
        .write_all(format!("{event}\n").as_bytes())
        .await
        .unwrap();
    writer
        .write_all(format!("{future}\n").as_bytes())
        .await
        .unwrap();
    let mut seen = String::new();
    loop {
        seen.clear();
        match reader.read_line(&mut seen).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let envelope: Value = serde_json::from_str(seen.trim()).unwrap();
        let id = envelope["id"].as_str().unwrap().to_string();
        let response = json!({
            "type": "response",
            "id": id,
            "command": envelope["command"]["type"].as_str().unwrap_or_default(),
            "success": true,
            "data": { "ok": true },
        });
        writer
            .write_all(format!("{response}\n").as_bytes())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn raw_tap_receives_every_application_frame_verbatim() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move { spawn_tap_daemon(listener).await });

    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel::<Value>();
    let (client, mut events) = DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions {
            raw_tap: Some(tap_tx),
            ..DaemonClientOptions::default()
        },
    )
    .await
    .unwrap();
    let data = client
        .request_ok(DaemonCommand::List {
            id: None,
            all: None,
            cwd: None,
            session_dir: None,
            include_client_owned: None,
            rest: Map::default(),
        })
        .await
        .unwrap();
    assert_eq!(data["ok"], true);

    // The tap carries both application frames verbatim — full event meta and
    // the unknown frame type, absent fields untouched — and nothing else:
    // handshake and id-correlated response frames never appear.
    let (event, future) = tap_test_frames();
    let mut tapped = Vec::new();
    while let Ok(Some(frame)) =
        tokio::time::timeout(Duration::from_millis(200), tap_rx.recv()).await
    {
        tapped.push(frame);
    }
    assert_eq!(tapped, vec![event, future]);

    // The typed channel is unchanged: the known event parses, the unknown
    // frame type stays silent (its fidelity lives on the tap).
    let mut typed = Vec::new();
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(200), events.recv()).await
    {
        typed.push(event);
    }
    assert_eq!(typed.len(), 1, "unexpected events: {typed:?}");
    assert!(
        matches!(
            &typed[0],
            DaemonClientEvent::SessionEvent { ref event, .. } if event["type"] == "turn_end"
        ),
        "unexpected event: {:?}",
        typed[0]
    );
    client.close();
}

#[tokio::test]
async fn default_options_keep_the_reader_unbounded() {
    // A one-megabyte frame sails through the default options: no frame cap
    // was added to the historical transport.
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": [],
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        let event = json!({
            "type": "session_event",
            "activeSessionId": "s1",
            "event": { "type": "turn_end", "padding": "x".repeat(1024 * 1024) },
        });
        writer
            .write_all(format!("{event}\n").as_bytes())
            .await
            .unwrap();
        let mut seen = String::new();
        loop {
            seen.clear();
            match reader.read_line(&mut seen).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let envelope: Value = serde_json::from_str(seen.trim()).unwrap();
            let response = json!({
                "type": "response",
                "id": envelope["id"].as_str().unwrap_or_default(),
                "command": envelope["command"]["type"].as_str().unwrap_or_default(),
                "success": true,
                "data": { "ok": true },
            });
            writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        }
    });

    let (client, mut events) = DaemonClient::connect(&socket).await.unwrap();
    let data = client
        .request_ok(DaemonCommand::List {
            id: None,
            all: None,
            cwd: None,
            session_dir: None,
            include_client_owned: None,
            rest: Map::default(),
        })
        .await
        .unwrap();
    assert_eq!(data["ok"], true);
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("the oversized-by-old-standards frame still streams")
        .unwrap();
    assert!(
        matches!(
            event,
            DaemonClientEvent::SessionEvent { ref event, .. } if event["padding"].as_str().map(str::len) == Some(1024 * 1024)
        ),
        "unexpected event: {event:?}"
    );
    client.close();
}

#[tokio::test]
async fn frame_cap_tears_down_the_transport_and_fails_pending_as_uncertain() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    // A supervisor that greets, then writes one oversized line as soon as a
    // command arrives.
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": [],
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        let mut seen = String::new();
        match reader.read_line(&mut seen).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        writer.write_all("x".repeat(700).as_bytes()).await.unwrap();
        std::future::pending::<()>().await;
    });

    let (client, _events) = DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions {
            max_frame_bytes: Some(512),
            ..DaemonClientOptions::default()
        },
    )
    .await
    .unwrap();
    let started = std::time::Instant::now();
    let error = client
        .request_with_timeout(
            DaemonCommand::List {
                id: None,
                all: None,
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: Map::default(),
            },
            30_000,
        )
        .await
        .unwrap_err();
    // The teardown's failure sweep resolves the in-flight request: the line
    // was queued, so the outcome is conservatively uncertain.
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(is_command_result_uncertain(&error));
    assert!(!is_command_not_sent(&error));
    assert!(
        error.to_string().contains("the daemon connection closed"),
        "unexpected error: {error}"
    );
    // The reader is dead: a follow-up request is a provable non-delivery.
    let error = client
        .request_with_timeout(
            DaemonCommand::List {
                id: None,
                all: None,
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: Map::default(),
            },
            30_000,
        )
        .await
        .unwrap_err();
    assert!(is_command_not_sent(&error));
    client.close();
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn pending_cap_refuses_the_send_before_queuing() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    // A supervisor that greets, counts incoming lines, and never answers.
    let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&received);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": [],
        });
        let mut writer = writer;
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        let mut seen = String::new();
        loop {
            seen.clear();
            match reader.read_line(&mut seen).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });

    let (client, _events) = DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions {
            max_pending: Some(2),
            ..DaemonClientOptions::default()
        },
    )
    .await
    .unwrap();
    // Two requests register and queue (their lines reach the daemon).
    let first = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .request_with_timeout(
                    DaemonCommand::List {
                        id: None,
                        all: None,
                        cwd: None,
                        session_dir: None,
                        include_client_owned: None,
                        rest: Map::default(),
                    },
                    500,
                )
                .await
        })
    };
    let second = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .request_with_timeout(
                    DaemonCommand::GetState {
                        id: None,
                        active_session_id: "s1".to_string(),
                        rest: Map::default(),
                    },
                    500,
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while received.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first two requests reach the daemon");
    // The third never queues: an immediate, provable non-delivery.
    let started = std::time::Instant::now();
    let error = client
        .request_with_timeout(
            DaemonCommand::List {
                id: None,
                all: None,
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: Map::default(),
            },
            30_000,
        )
        .await
        .unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(is_command_not_sent(&error));
    assert!(!is_command_result_uncertain(&error));
    assert!(
        error
            .to_string()
            .contains("too many pending daemon requests"),
        "unexpected error: {error}"
    );
    // The queued pair times out as an uncertain delivery each.
    for result in [first.await.unwrap(), second.await.unwrap()] {
        let error = result.unwrap_err();
        assert!(is_command_result_uncertain(&error));
        assert!(
            error.to_string().contains("Timed out after"),
            "unexpected: {error}"
        );
    }
    client.close();
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn direct_reader_streams_application_frames_to_the_tap() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let worker_socket = dir.path().join("w.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let worker_listener = UnixListener::bind(&worker_socket).unwrap();
    tokio::spawn(async move { spawn_mock_worker(worker_listener).await });

    // Scripted supervisor: advertises direct_peer_transport, answers the
    // ticket request with a ticket for the mock worker socket.
    let ticket_socket = worker_socket.clone();
    let supervisor = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("supervisor accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        let hello = serde_json::json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "serverCapabilities": ["direct_peer_transport"],
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
            let envelope: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
            let id = envelope["id"].as_str().unwrap().to_string();
            let command_type = envelope["command"]["type"].as_str().unwrap();
            let response = if command_type == "get_direct_worker_transport" {
                let identity = pa_types::platform::socket_identity(&ticket_socket).unwrap();
                serde_json::json!({
                    "type": "response",
                    "id": id,
                    "command": command_type,
                    "success": true,
                    "data": {
                        "purpose": "session_client",
                        "socketPath": ticket_socket.to_string_lossy(),
                        "socketIdentity": { "dev": identity.dev, "ino": identity.ino },
                        "workerInstanceId": "inst-1",
                        "activeSessionId": "s1",
                        "grantId": "g1",
                        "token": "t",
                        "expiresAt": "2999-01-01T00:00:00.000Z",
                    },
                })
            } else {
                serde_json::json!({
                    "type": "response",
                    "id": id,
                    "command": command_type,
                    "success": true,
                    "data": { "ok": true },
                })
            };
            writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        }
    });

    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel::<Value>();
    let (client, _events) = DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions {
            raw_tap: Some(tap_tx),
            ..DaemonClientOptions::default()
        },
    )
    .await
    .unwrap();
    assert!(client.upgrade_direct("s1").await.unwrap());
    let data = client
        .request_ok(DaemonCommand::Attach {
            id: None,
            active_session_id: "s1".to_string(),
            client_id: None,
            capabilities: None,
            resume_cursor: None,
            telemetry_disabled: None,
            recovery_config: None,
            env: None,
            launch_env: None,
            rest: Map::default(),
        })
        .await
        .unwrap();
    assert_eq!(data["activeSessionId"], "s1");

    // The tap carries the worker's streamed application frame verbatim and
    // nothing else: the direct handshake, peer_auth, and id-correlated
    // responses never appear.
    let event = json!({
        "type": "session_event",
        "activeSessionId": "s1",
        "event": { "type": "turn_end" },
    });
    let mut tapped = Vec::new();
    while let Ok(Some(frame)) =
        tokio::time::timeout(Duration::from_millis(200), tap_rx.recv()).await
    {
        tapped.push(frame);
    }
    assert_eq!(tapped, vec![event]);
    client.close();
    let _ = supervisor.await;
}

#[tokio::test]
async fn writer_send_failure_cleans_its_pending_slot() {
    // `hard_close` swaps the writer for a dead channel while the reader is
    // still mounted: every send fails as a provable non-delivery, and the
    // registered slot must not linger — a ghost entry would fill the
    // pending cap and turn later refusals into cap refusals.
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move { spawn_mock_daemon(listener).await });

    let (mut client, _events) = DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions {
            max_pending: Some(1),
            ..DaemonClientOptions::default()
        },
    )
    .await
    .unwrap();
    client.hard_close();
    for _ in 0..2 {
        let error = client
            .request_with_timeout(
                DaemonCommand::List {
                    id: None,
                    all: None,
                    cwd: None,
                    session_dir: None,
                    include_client_owned: None,
                    rest: Map::default(),
                },
                30_000,
            )
            .await
            .unwrap_err();
        assert!(is_command_not_sent(&error), "unexpected error: {error}");
        assert!(!is_command_result_uncertain(&error));
        // The writer refusal, never the pending-cap refusal a leaked slot
        // would produce.
        assert!(
            error
                .to_string()
                .contains("the daemon connection is closed"),
            "unexpected error: {error}"
        );
    }
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn pre_handshake_frames_stay_off_the_tap() {
    // A daemon that misbehaves by streaming an application frame before its
    // hello: the tap (ordering guaranteed only post-handshake) stays empty
    // of it, while the typed channel keeps its legacy behavior of
    // dispatching it.
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let early = json!({
            "type": "session_event",
            "activeSessionId": "s1",
            "event": { "type": "early" },
        });
        writer
            .write_all(format!("{early}\n").as_bytes())
            .await
            .unwrap();
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": [],
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        let late = json!({
            "type": "session_event",
            "activeSessionId": "s1",
            "event": { "type": "late" },
        });
        writer
            .write_all(format!("{late}\n").as_bytes())
            .await
            .unwrap();
        let mut seen = String::new();
        loop {
            seen.clear();
            match reader.read_line(&mut seen).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            let envelope: Value = serde_json::from_str(seen.trim()).unwrap();
            let response = json!({
                "type": "response",
                "id": envelope["id"].as_str().unwrap_or_default(),
                "command": envelope["command"]["type"].as_str().unwrap_or_default(),
                "success": true,
                "data": { "ok": true },
            });
            writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        }
    });

    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel::<Value>();
    let (client, mut events) = DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions {
            raw_tap: Some(tap_tx),
            ..DaemonClientOptions::default()
        },
    )
    .await
    .unwrap();
    let data = client
        .request_ok(DaemonCommand::List {
            id: None,
            all: None,
            cwd: None,
            session_dir: None,
            include_client_owned: None,
            rest: Map::default(),
        })
        .await
        .unwrap();
    assert_eq!(data["ok"], true);

    // Only the post-handshake frame reaches the tap.
    let late = json!({
        "type": "session_event",
        "activeSessionId": "s1",
        "event": { "type": "late" },
    });
    let mut tapped = Vec::new();
    while let Ok(Some(frame)) =
        tokio::time::timeout(Duration::from_millis(200), tap_rx.recv()).await
    {
        tapped.push(frame);
    }
    assert_eq!(tapped, vec![late]);

    // The typed channel keeps its legacy pre-handshake dispatch: both
    // frames arrive as session events.
    let mut early_and_late = Vec::new();
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(200), events.recv()).await
    {
        early_and_late.push(event);
    }
    let kinds: Vec<&str> = early_and_late
        .iter()
        .filter_map(|event| match event {
            DaemonClientEvent::SessionEvent { event, .. } => event["type"].as_str(),
            _ => None,
        })
        .collect();
    assert_eq!(kinds, vec!["early", "late"]);
    client.close();
}

#[tokio::test]
async fn frame_cap_binds_partial_lines_across_fills() {
    // The oversized frame arrives in paced chunks without a newline: the
    // partial accumulation trips the cap before any complete line forms.
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": [],
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        let mut seen = String::new();
        match reader.read_line(&mut seen).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        for _ in 0..3 {
            writer.write_all("x".repeat(200).as_bytes()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        std::future::pending::<()>().await;
    });

    let (client, _events) = DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions {
            max_frame_bytes: Some(300),
            ..DaemonClientOptions::default()
        },
    )
    .await
    .unwrap();
    let started = std::time::Instant::now();
    let error = client
        .request_with_timeout(
            DaemonCommand::List {
                id: None,
                all: None,
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: Map::default(),
            },
            30_000,
        )
        .await
        .unwrap_err();
    // The teardown's sweep resolves the in-flight request as uncertain.
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(
        is_command_result_uncertain(&error),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("the daemon connection closed"),
        "unexpected error: {error}"
    );
    client.close();
}

#[tokio::test]
async fn frame_cap_binds_split_lines_spilling_over_the_cap() {
    // A line split across fills: its head is inside the cap alone, the
    // final tail carries the newline — the total must still tear the
    // transport down (the newline branch counts the spill, not just the
    // tail).
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": [],
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        let mut seen = String::new();
        match reader.read_line(&mut seen).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        // The head: 250 bytes, inside the 300-byte cap alone.
        writer.write_all("x".repeat(250).as_bytes()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        // The tail: 100 bytes plus the newline — 350 total, over the cap.
        let mut tail = "x".repeat(100);
        tail.push('\n');
        writer.write_all(tail.as_bytes()).await.unwrap();
        std::future::pending::<()>().await;
    });

    let (client, _events) = DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions {
            max_frame_bytes: Some(300),
            ..DaemonClientOptions::default()
        },
    )
    .await
    .unwrap();
    let started = std::time::Instant::now();
    let error = client
        .request_with_timeout(
            DaemonCommand::List {
                id: None,
                all: None,
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: Map::default(),
            },
            30_000,
        )
        .await
        .unwrap_err();
    // The teardown's sweep resolves the in-flight request as uncertain —
    // without the spill counted, the oversized line would be accepted and
    // this request would ride its 30s budget out.
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(
        is_command_result_uncertain(&error),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("the daemon connection closed"),
        "unexpected error: {error}"
    );
    client.close();
}

#[tokio::test]
async fn shutdown_resolves_pending_promptly_in_any_race_position() {
    // The B3 production-ordering property: a request racing shutdown never
    // rides its budget out. Pre-flip registrations are swept as uncertain
    // deliveries; post-flip registrations are refused at their post-insert
    // re-check as provable non-deliveries. Either class lands immediately —
    // and the test takes no receipt barrier: both interleavings are
    // acceptable, a 30s ride is not.
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut writer = stream;
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": [],
        })
        .to_string();
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });

    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    let in_flight = client.clone();
    let request = tokio::spawn(async move {
        in_flight
            .request_with_timeout(
                DaemonCommand::List {
                    id: None,
                    all: None,
                    cwd: None,
                    session_dir: None,
                    include_client_owned: None,
                    rest: Map::default(),
                },
                30_000,
            )
            .await
    });
    // No receipt barrier: wherever the registration lands against the
    // shutdown, the resolution must be prompt and truthful.
    tokio::time::sleep(Duration::from_millis(50)).await;
    client.shutdown();
    let started = std::time::Instant::now();
    let error = request.await.unwrap().unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(
        is_command_result_uncertain(&error) || is_command_not_sent(&error),
        "unexpected error: {error}"
    );
    assert!(!is_daemon_rejection(&error));
    assert!(*client.reader_dead().borrow());
    let clone = client.clone();
    let error = clone
        .request_with_timeout(
            DaemonCommand::List {
                id: None,
                all: None,
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: Map::default(),
            },
            30_000,
        )
        .await
        .unwrap_err();
    assert!(is_command_not_sent(&error));
    let drained = tokio::time::timeout(Duration::from_secs(5), client.join_tasks())
        .await
        .expect("tasks join after shutdown");
    assert_eq!(drained, 2, "the reader and writer pumps");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), client.join_tasks())
            .await
            .unwrap(),
        0,
        "a repeat join waits behind the lock and drains nothing"
    );
}

#[tokio::test]
async fn shutdown_marks_every_clone_dead() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move { spawn_mock_daemon(listener).await });

    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    let other = client.clone();
    client.shutdown();
    let error = other
        .request_with_timeout(
            DaemonCommand::List {
                id: None,
                all: None,
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: Map::default(),
            },
            30_000,
        )
        .await
        .unwrap_err();
    assert!(is_command_not_sent(&error), "unexpected error: {error}");
    assert!(
        error
            .to_string()
            .contains("the daemon connection is closed"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn shutdown_aborts_direct_link_pumps() {
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let worker_socket = dir.path().join("w.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let worker_listener = UnixListener::bind(&worker_socket).unwrap();
    tokio::spawn(async move { spawn_mock_worker(worker_listener).await });
    let ticket_socket = worker_socket.clone();
    let supervisor = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("supervisor accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        let hello = serde_json::json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "serverCapabilities": ["direct_peer_transport"],
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
            let envelope: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
            let id = envelope["id"].as_str().unwrap().to_string();
            let command_type = envelope["command"]["type"].as_str().unwrap();
            let response = if command_type == "get_direct_worker_transport" {
                let identity = pa_types::platform::socket_identity(&ticket_socket).unwrap();
                serde_json::json!({
                    "type": "response",
                    "id": id,
                    "command": command_type,
                    "success": true,
                    "data": {
                        "purpose": "session_client",
                        "socketPath": ticket_socket.to_string_lossy(),
                        "socketIdentity": { "dev": identity.dev, "ino": identity.ino },
                        "workerInstanceId": "inst-1",
                        "activeSessionId": "s1",
                        "grantId": "g1",
                        "token": "t",
                        "expiresAt": "2999-01-01T00:00:00.000Z",
                    },
                })
            } else {
                serde_json::json!({
                    "type": "response",
                    "id": id,
                    "command": command_type,
                    "success": true,
                    "data": { "ok": true },
                })
            };
            writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        }
    });

    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    assert!(client.upgrade_direct("s1").await.unwrap());
    assert_eq!(client.direct_session_id().as_deref(), Some("s1"));
    // The worker stays alive: its reader pump only ends by abort.
    client.shutdown();
    assert_eq!(client.direct_session_id(), None);
    let drained = tokio::time::timeout(Duration::from_secs(5), client.join_tasks())
        .await
        .expect("direct pumps join after shutdown");
    assert_eq!(drained, 4, "reader + writer + the direct pump pair");
    let _ = supervisor.await;
}

#[tokio::test]
async fn task_set_registration_after_death_aborts_and_retains_new_tasks() {
    // The shutdown-vs-registration race, deterministically: the death mark
    // is taken under the same lock the registrations check, so a task that
    // registers after the sweep can never escape it — it is aborted AT
    // registration and RETAINED, and the join still drains it.
    let tasks = std::sync::Arc::new(TaskSet::default());
    assert!(tasks.register_connection(
        tokio::spawn(std::future::pending::<()>()),
        tokio::spawn(std::future::pending::<()>())
    ));
    assert!(tasks.abort_all());
    // One-shot across every clone: a second shutdown reports already-dead.
    assert!(!tasks.abort_all());

    let (reader_tx, reader_rx) = oneshot::channel::<()>();
    let reader = tokio::spawn(async move {
        let _held = reader_tx;
        std::future::pending::<()>().await;
    });
    let (writer_tx, writer_rx) = oneshot::channel::<()>();
    let writer = tokio::spawn(async move {
        let _held = writer_tx;
        std::future::pending::<()>().await;
    });
    assert!(!tasks.register_connection(reader, writer));
    for receiver in [reader_rx, writer_rx] {
        tokio::time::timeout(Duration::from_secs(5), async move {
            assert!(
                receiver.await.is_err(),
                "the escaped task must end at registration"
            );
        })
        .await
        .expect("the escaped tasks end at registration");
    }

    let (direct_tx, direct_rx) = oneshot::channel::<()>();
    let direct = tokio::spawn(async move {
        let _held = direct_tx;
        std::future::pending::<()>().await;
    });
    assert!(!tasks.register_direct(vec![direct]));
    tokio::time::timeout(Duration::from_secs(5), async move {
        assert!(
            direct_rx.await.is_err(),
            "the escaped direct pump must end at registration"
        );
    })
    .await
    .expect("the escaped direct pump ends at registration");

    // Everything registered — including the escaped, aborted-at-registration
    // tasks — is retained and joined exactly once.
    let drained = tokio::time::timeout(Duration::from_secs(5), tasks.join_drain())
        .await
        .expect("the join drains every retained handle");
    assert_eq!(drained, 5, "initial pair + escaped pair + escaped direct");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), tasks.join_drain())
            .await
            .unwrap(),
        0,
        "a repeat join drains nothing"
    );
}

/// A guard whose Drop flips a flag when its owning task's future is truly
/// gone — the actual-completion barrier the join proofs assert against.
struct CompletionFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl Drop for CompletionFlag {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn join_awaits_actual_completion_before_returning() {
    // B1's proof: the join is only done when the tasks' futures actually
    // unwound — a Drop guard inside the task flips the flag, and the flag
    // must be up before join_drain returns.
    let tasks = std::sync::Arc::new(TaskSet::default());
    let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let first = {
        let flag = std::sync::Arc::clone(&flag);
        tokio::spawn(async move {
            let _completed = CompletionFlag(flag);
            std::future::pending::<()>().await;
        })
    };
    let second = {
        let flag = std::sync::Arc::clone(&flag);
        tokio::spawn(async move {
            let _completed = CompletionFlag(flag);
            std::future::pending::<()>().await;
        })
    };
    assert!(tasks.register_connection(first, second));
    // Let the runtime poll the tasks to their pending awaits BEFORE the
    // abort: an unpolled task never constructs its locals, and unwinding a
    // started task is what the barrier observes.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(tasks.abort_all());
    let drained = tokio::time::timeout(Duration::from_secs(5), tasks.join_drain())
        .await
        .expect("the join drains the aborted pair");
    assert_eq!(drained, 2);
    assert!(
        flag.load(std::sync::atomic::Ordering::SeqCst),
        "task completion observed before the join returned"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_join_retains_ownership_and_the_next_join_waits() {
    // B4's proof: a join future cancelled mid-drain loses NOTHING — the
    // in-flight handle is re-inserted by the guard's Drop, and the next
    // join still awaits the blocker's real completion. Multi-threaded:
    // the hot loop genuinely runs while the cancellation lands on the
    // other worker.
    let tasks = std::sync::Arc::new(TaskSet::default());
    let (held_tx, held_rx) = oneshot::channel::<()>();
    // A hot loop between awaits resists abort until it finishes: the drain
    // genuinely blocks on it.
    let blocker = tokio::spawn(async move {
        let _held = held_tx;
        let mut acc = 0u64;
        for i in 0..200_000_000u64 {
            acc = acc.wrapping_add(i);
        }
        std::hint::black_box(acc);
        std::future::pending::<()>().await;
    });
    let peer = tokio::spawn(std::future::pending::<()>());
    assert!(tasks.register_connection(blocker, peer));
    // The blocker is mid-loop (aborts cannot land there); mark dead.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(tasks.abort_all());

    // The first drain blocks on the looping blocker; cancel it mid-await.
    let first = {
        let tasks = std::sync::Arc::clone(&tasks);
        tokio::spawn(async move { tasks.join_drain().await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    first.abort();
    let _ = first.await;

    // The second join still OWNS everything: it waits out the blocker and
    // returns only after real completion. The cancelled first drain had
    // already consumed the instantly-ending peer; the re-inserted blocker
    // is the one handle left, drained exactly once here.
    let drained = tokio::time::timeout(Duration::from_secs(60), tasks.join_drain())
        .await
        .expect("the next join still awaits the retained tasks");
    assert_eq!(drained, 1, "the re-inserted blocker, exactly once");
    // Real completion observed: the blocker's held sender dropped only when
    // its future unwound.
    tokio::time::timeout(Duration::from_secs(5), async move {
        assert!(held_rx.await.is_err(), "the blocker truly ended");
    })
    .await
    .expect("the blocker's unwind is observed");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), tasks.join_drain())
            .await
            .unwrap(),
        0,
        "nothing is left owned"
    );
}

#[tokio::test]
async fn concurrent_joins_wait_for_each_other_behind_the_barrier() {
    // The concurrent-join proof against the completion barrier: both joins
    // return strictly after the tasks' real completion, and each retained
    // handle is drained exactly once across them.
    let tasks = std::sync::Arc::new(TaskSet::default());
    let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let first = {
        let flag = std::sync::Arc::clone(&flag);
        tokio::spawn(async move {
            let _completed = CompletionFlag(flag);
            std::future::pending::<()>().await;
        })
    };
    let second = {
        let flag = std::sync::Arc::clone(&flag);
        tokio::spawn(async move {
            let _completed = CompletionFlag(flag);
            std::future::pending::<()>().await;
        })
    };
    assert!(tasks.register_connection(first, second));
    // Polled to their pending awaits before the abort, so their unwind is
    // observable.
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(tasks.abort_all());
    let one = {
        let tasks = std::sync::Arc::clone(&tasks);
        tokio::spawn(async move { tasks.join_drain().await })
    };
    let two = {
        let tasks = std::sync::Arc::clone(&tasks);
        tokio::spawn(async move { tasks.join_drain().await })
    };
    let (drained_one, drained_two) =
        tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(one, two) })
            .await
            .expect("both joins complete");
    assert_eq!(
        drained_one.unwrap() + drained_two.unwrap(),
        2,
        "each handle joined once"
    );
    assert!(
        flag.load(std::sync::atomic::Ordering::SeqCst),
        "completion observed before the joins returned"
    );
}

#[tokio::test]
async fn upgrade_after_shutdown_refuses() {
    // The supervisor advertises direct peer transport, but the connection is
    // dead: the upgrade's entry request is a provable non-delivery and the
    // link never forms.
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut writer = stream;
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "clientId": "srv",
            "serverCapabilities": ["direct_peer_transport"],
        })
        .to_string();
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });

    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    client.shutdown();
    assert!(!client.upgrade_direct("s1").await.unwrap());
    assert_eq!(client.direct_session_id(), None);
    let drained = tokio::time::timeout(Duration::from_secs(5), client.join_tasks())
        .await
        .expect("nothing was left to join");
    assert_eq!(drained, 2, "only the supervisor pair ever registered");
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn shutdown_during_in_flight_upgrade_loses_the_registration_race() {
    // B2's proof: shutdown on another thread while the upgrade's worker
    // handshake is pending. The worker gates its hello on a release signal:
    // shutdown lands mid-handshake, the hello is then released, the
    // handshake completes on the live socket, the pumps spawn — and the
    // done-guard aborts them at registration. The upgrade settles
    // Ok(false), no link is installed, and the join drains the aborted
    // pumps to real completion.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let worker_socket = dir.path().join("w.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let worker_listener = UnixListener::bind(&worker_socket).unwrap();
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let worker_release = std::sync::Arc::clone(&release);
    let (accepted_tx, accepted_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let (stream, _) = worker_listener.accept().await.expect("worker accept");
        let (mut reader, mut writer) = stream.into_split();
        let _ = accepted_tx.send(());
        // notify_one stores a permit: no lost wakeup even if this arm runs
        // before the release lands.
        worker_release.notified().await;
        let hello = pa_types::daemon::framing::encode_private_frame(
            &serde_json::json!({ "kind": "outbound", "outboundType": "daemon_hello" }),
            br#"{"type":"daemon_hello","protocol":{"name":"prime-agent.daemon","version":7}}"#,
            pa_types::daemon::framing::DEFAULT_PRIVATE_FRAME_LIMITS,
        )
        .unwrap();
        writer.write_all(&hello).await.unwrap();
        let mut pending = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let read = reader.read(&mut chunk).await.expect("worker read");
            if read == 0 {
                break;
            }
            pending.extend_from_slice(&chunk[..read]);
            let mut decoder = pa_types::daemon::framing::PrivateFrameDecoder::new(
                pa_types::daemon::framing::DEFAULT_PRIVATE_FRAME_LIMITS,
            );
            let frames = decoder.push(&pending).expect("decode frames");
            pending.clear();
            for frame in frames {
                let command_type = frame.header["commandType"].as_str().unwrap_or_default();
                let request_id = frame.header["requestId"].as_str().unwrap_or_default();
                let (success, data) = if command_type == "peer_auth" {
                    (
                        true,
                        Some(serde_json::json!({
                            "workerInstanceId": "inst-1",
                            "activeSessionId": "s1",
                            "purpose": "session_client",
                        })),
                    )
                } else {
                    (false, None)
                };
                let response = serde_json::json!({
                    "id": request_id,
                    "type": "response",
                    "command": command_type,
                    "success": success,
                    "data": data,
                });
                let frame = pa_types::daemon::framing::encode_private_frame(
                    &serde_json::json!({
                        "kind": "outbound",
                        "requestId": request_id,
                        "outboundType": "response",
                    }),
                    &serde_json::to_vec(&response).unwrap(),
                    pa_types::daemon::framing::DEFAULT_PRIVATE_FRAME_LIMITS,
                )
                .unwrap();
                writer.write_all(&frame).await.unwrap();
            }
        }
    });

    let ticket_socket = worker_socket.clone();
    let supervisor = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("supervisor accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        let hello = serde_json::json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "serverCapabilities": ["direct_peer_transport"],
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
            let envelope: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
            let id = envelope["id"].as_str().unwrap().to_string();
            let command_type = envelope["command"]["type"].as_str().unwrap();
            let response = if command_type == "get_direct_worker_transport" {
                let identity = pa_types::platform::socket_identity(&ticket_socket).unwrap();
                serde_json::json!({
                    "type": "response",
                    "id": id,
                    "command": command_type,
                    "success": true,
                    "data": {
                        "purpose": "session_client",
                        "socketPath": ticket_socket.to_string_lossy(),
                        "socketIdentity": { "dev": identity.dev, "ino": identity.ino },
                        "workerInstanceId": "inst-1",
                        "activeSessionId": "s1",
                        "grantId": "g1",
                        "token": "t",
                        "expiresAt": "2999-01-01T00:00:00.000Z",
                    },
                })
            } else {
                serde_json::json!({
                    "type": "response",
                    "id": id,
                    "command": command_type,
                    "success": true,
                    "data": { "ok": true },
                })
            };
            writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        }
    });

    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    let upgrade = {
        let client = client.clone();
        tokio::spawn(async move { client.upgrade_direct("s1").await })
    };
    // The deterministic handshake barrier: the worker's accepted signal
    // proves the upgrade holds inside connect_direct's worker-hello wait —
    // no sleep, no lost notification.
    tokio::time::timeout(Duration::from_secs(5), accepted_rx)
        .await
        .expect("the worker accepted the upgrade's connection")
        .expect("the accepted barrier fired");
    client.shutdown();
    release.notify_one();
    let upgraded = tokio::time::timeout(Duration::from_secs(5), upgrade)
        .await
        .expect("the upgrade settles")
        .unwrap()
        .unwrap();
    assert!(!upgraded, "the lost registration race reports no upgrade");
    assert_eq!(client.direct_session_id(), None, "no link installed");
    // The supervisor pair plus the racing upgrade's aborted pumps join to
    // real completion (the same or the next call — a handshake completing
    // after a drain registers late by contract).
    let mut joined = 0;
    for _ in 0..2 {
        joined += tokio::time::timeout(Duration::from_secs(5), client.join_tasks())
            .await
            .expect("tasks join after shutdown");
    }
    assert!(
        joined >= 4,
        "supervisor pair + the aborted pump pair (joined: {joined})"
    );
    let _ = supervisor.await;
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn dropped_mid_flight_upgrade_leaves_no_residual_tasks() {
    // §5's caller-owned proof, deterministic: the worker accepts and HOLDS
    // the stream (accepted barrier), the upgrade parks in connect_direct's
    // hello wait carrying the worker socket; dropping the upgrade future
    // closes that socket (the worker observes EOF — its own eof barrier),
    // and shutdown+join then drains exactly the supervisor pair and
    // nothing residual.
    use tokio::io::AsyncReadExt;

    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let worker_socket = dir.path().join("w.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let worker_listener = UnixListener::bind(&worker_socket).unwrap();
    let (accepted_tx, accepted_rx) = oneshot::channel::<()>();
    let (eof_tx, eof_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let (stream, _) = worker_listener.accept().await.expect("worker accept");
        let (mut reader, _writer) = stream.into_split();
        let _ = accepted_tx.send(());
        // Hold the hello and the stream: the upgrade sits in its hello wait
        // holding this socket until its future is dropped.
        let mut chunk = [0u8; 64];
        loop {
            match reader.read(&mut chunk).await {
                Ok(0) | Err(_) => {
                    let _ = eof_tx.send(());
                    break;
                }
                Ok(_) => {}
            }
        }
    });
    let ticket_socket = worker_socket.clone();
    let supervisor = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("supervisor accept");
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        let hello = serde_json::json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "serverCapabilities": ["direct_peer_transport"],
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                break;
            }
            let envelope: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
            let id = envelope["id"].as_str().unwrap().to_string();
            let command_type = envelope["command"]["type"].as_str().unwrap();
            let response = if command_type == "get_direct_worker_transport" {
                let identity = pa_types::platform::socket_identity(&ticket_socket).unwrap();
                serde_json::json!({
                    "type": "response",
                    "id": id,
                    "command": command_type,
                    "success": true,
                    "data": {
                        "purpose": "session_client",
                        "socketPath": ticket_socket.to_string_lossy(),
                        "socketIdentity": { "dev": identity.dev, "ino": identity.ino },
                        "workerInstanceId": "inst-1",
                        "activeSessionId": "s1",
                        "grantId": "g1",
                        "token": "t",
                        "expiresAt": "2999-01-01T00:00:00.000Z",
                    },
                })
            } else {
                serde_json::json!({
                    "type": "response",
                    "id": id,
                    "command": command_type,
                    "success": true,
                    "data": { "ok": true },
                })
            };
            writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        }
    });

    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    let upgrade = {
        let client = client.clone();
        tokio::spawn(async move { client.upgrade_direct("s1").await })
    };
    // The accepted barrier: the upgrade is genuinely mid-handshake, holding
    // the worker socket in its future.
    tokio::time::timeout(Duration::from_secs(5), accepted_rx)
        .await
        .expect("the worker accepted the upgrade's connection")
        .expect("the accepted barrier fired");
    // Drop the in-flight upgrade: no pumps were spawned (the handshake never
    // completed), and the worker socket dies with the future.
    upgrade.abort();
    let _ = upgrade.await;
    // The worker observed the EOF — the caller-owned socket truly closed.
    tokio::time::timeout(Duration::from_secs(5), eof_rx)
        .await
        .expect("the worker sees the dropped upgrade's EOF")
        .expect("the EOF barrier fired");
    client.shutdown();
    assert_eq!(client.direct_session_id(), None);
    let drained = tokio::time::timeout(Duration::from_secs(5), client.join_tasks())
        .await
        .expect("tasks join after shutdown");
    assert_eq!(drained, 2, "exactly the supervisor pair — zero residual");
    let _ = supervisor.await;
}

#[tokio::test]
async fn a_cancelled_connect_aborts_its_spawned_tasks() {
    // The handshake guard's proof: the reader and writer tasks are spawned
    // BEFORE the hello await; cancelling the connect future there drops
    // their handles — the guard ABORTS both, and BOTH socket halves close.
    // The mock observes the write half's EOF (its read) AND the read
    // half's closure (its write erroring).
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    let write_failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_ended = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let failed = std::sync::Arc::clone(&write_failed);
    let ended = std::sync::Arc::clone(&read_ended);
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let (mut reader, mut writer) = stream.into_split();
        let mut chunk = [0u8; 64];
        // The write half closes first (the writer pump aborts): EOF here.
        match reader.read(&mut chunk).await {
            Ok(0) | Err(_) => {
                ended.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(_) => {}
        }
        // The read half closes too (the reader task aborts): this write
        // must error — a live reader would just buffer it.
        writer
            .write_all(b"x")
            .await
            .expect_err("the client's read half is gone");
        failed.store(true, std::sync::atomic::Ordering::SeqCst);
    });

    // Drive the connect into its hello wait, then cancel the future: the
    // guard must abort the spawned tasks (the pre-fix code detached them,
    // and this write below would have buffered forever).
    let mut connect = Box::pin(DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions::default(),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), connect.as_mut())
            .await
            .is_err(),
        "the never-greeting daemon keeps the connect pending"
    );
    // Drop the OWNED connect future NOW (a select arm drops it in the
    // driver; a pinned binding would keep the socket halves alive here).
    drop(connect);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !(read_ended.load(std::sync::atomic::Ordering::SeqCst)
            && write_failed.load(std::sync::atomic::Ordering::SeqCst))
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both halves closed: reader AND writer were aborted");
}

#[tokio::test]
async fn an_oversized_hello_line_fails_under_the_cap() {
    // Item 1: the cap covers EVERY decoded line — a COMPLETE hello line
    // (newline included in the read) beyond the cap must refuse the connect.
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut writer = stream;
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "padding": "x".repeat(700),
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let error = DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions {
            max_frame_bytes: Some(512),
            ..DaemonClientOptions::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("exceeded the limit"),
        "the oversized hello line is refused: {error}"
    );
}

#[tokio::test]
async fn an_oversized_pre_hello_frame_fails_under_the_cap() {
    // Item 1: a complete pre-hello event line beyond the cap also refuses.
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut writer = stream;
        let early = json!({
            "type": "session_event",
            "activeSessionId": "s1",
            "event": { "type": "delta", "padding": "x".repeat(700) },
        });
        writer
            .write_all(format!("{early}\n").as_bytes())
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let error = DaemonClient::connect_with_options(
        &socket,
        DaemonClientOptions {
            max_frame_bytes: Some(512),
            ..DaemonClientOptions::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("exceeded the limit"),
        "the oversized pre-hello line is refused: {error}"
    );
}

#[tokio::test]
async fn an_invalid_utf8_seed_line_terminates_the_reader() {
    // Item 2: a coalesced post-hello seed containing an invalid-UTF-8 line
    // tears the transport down — the in-flight request resolves as the
    // uncertain sweep class and the death watch flips, asserted BEFORE any
    // further frame delivers.
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut writer = stream;
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
        });
        writer
            .write_all(format!("{hello}\n").as_bytes())
            .await
            .unwrap();
        // The invalid-UTF-8 seed line, coalesced with the hello.
        writer.write_all(&[0xff, 0xfe, b'\n']).await.unwrap();
        std::future::pending::<()>().await;
    });
    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    let request = client.request_with_timeout(
        DaemonCommand::List {
            id: None,
            all: None,
            cwd: None,
            session_dir: None,
            include_client_owned: None,
            rest: Map::default(),
        },
        30_000,
    );
    let error = tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .expect("the seed failure resolves the pending request")
        .unwrap_err();
    assert!(is_command_result_uncertain(&error), "unexpected: {error}");
    let mut dead = client.reader_dead();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !*dead.borrow_and_update() {
            dead.changed().await.expect("the watch lives");
        }
    })
    .await
    .expect("the death watch flipped");
}

#[tokio::test]
async fn an_eof_unterminated_valid_hello_resolves_the_handshake() {
    // Item 3: base parity — a COMPLETE hello WITHOUT its trailing newline
    // at EOF dispatches once and the handshake RESOLVES (the connect
    // succeeds; the EOF then becomes the ordinary first loss).
    let dir = tempfile::TempDir::new().unwrap();
    let socket = dir.path().join("d.sock");
    let listener = UnixListener::bind(&socket).unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let mut writer = stream;
        let hello = json!({
            "type": "daemon_hello",
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
        });
        // No trailing newline: EOF right after the hello bytes.
        writer
            .write_all(serde_json::to_string(&hello).unwrap().as_bytes())
            .await
            .unwrap();
        drop(writer);
    });
    let (client, _events) = DaemonClient::connect(&socket).await.unwrap();
    assert_eq!(client.protocol().version, 7);
    // The EOF becomes the ordinary first loss: the death watch flips.
    let mut dead = client.reader_dead();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !*dead.borrow_and_update() {
            dead.changed().await.expect("the watch lives");
        }
    })
    .await
    .expect("the EOF-after-hello-accept became the ordinary first loss");
}
