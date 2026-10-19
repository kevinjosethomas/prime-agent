use super::*;
use pa_tui::auth_panel::{PastePromptTone, PasteStyle, PrimeTeamOption};
use pa_tui::provider_auth::ProviderAuthOutcome;
use serde_json::{json, Value};
use std::future::Future;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, ReadHalf, WriteHalf};

type ClientReader = BufReader<ReadHalf<DuplexStream>>;
type ClientWriter = WriteHalf<DuplexStream>;
async fn event(reader: &mut ClientReader) -> Value {
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    serde_json::from_str(&line).unwrap()
}
async fn send(writer: &mut ClientWriter, value: Value) {
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.push(b'\n');
    writer.write_all(&bytes).await.unwrap();
    writer.flush().await.unwrap();
}
fn scripted<F, Make>(
    make: Make,
) -> (
    ClientReader,
    ClientWriter,
    tokio::task::JoinHandle<std::io::Result<()>>,
)
where
    F: Future<Output = ProviderAuthOutcome> + Send + 'static,
    Make: FnOnce(AuthPanelHandle) -> F,
{
    let (server, client) = tokio::io::duplex(65536);
    let (reader, mut writer) = tokio::io::split(server);
    let (tx, mut input) = mpsc::channel(8);
    let read_task = tokio::spawn(protocol::pump(BufReader::new(reader), tx));
    let (tx, events) = mpsc::unbounded_channel();
    let panel = AuthPanelHandle::new(tx);
    let flow = make(panel.clone());
    let task = tokio::spawn(async move {
        let result = driver::drive(&mut input, &mut writer, "flow", panel, events, flow).await;
        read_task.abort();
        result
    });
    let (reader, writer) = tokio::io::split(client);
    (BufReader::new(reader), writer, task)
}
#[test]
fn bounded_decode_rejects_oversized_secrets_unknown_actions_and_missing_reply_value() {
    for value in [
        json!({"type":"begin","id":"flow","target":"provider","key":"openai","action":"paste"}),
        json!({"type":"begin","id":"flow","target":"provider","key":"https://remote","action":"login"}),
        json!({"type":"reply","id":"flow","requestId":"input:1","value":"x".repeat(8193)}),
        json!({"type":"reply","id":"flow","requestId":"input:1"}),
        json!({"type":"cancel","id":"flow","token":"fixture-secret"}),
    ] {
        assert!(protocol::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    assert!(protocol::decode(&vec![b'x'; 16385]).is_err());
    assert!(matches!(
        protocol::decode(br#"{"type":"reply","id":"flow","requestId":"team:1","value":null}"#),
        Ok(Request::Reply { value: None, .. })
    ));
}
#[test]
fn explicit_method_is_optional_and_only_valid_for_provider_login() {
    for method_id in ["oauth", "api_key"] {
        let request = json!({"type":"begin","id":"flow","target":"provider","key":"anthropic","action":"login","methodId":method_id});
        assert!(matches!(
            protocol::decode(&serde_json::to_vec(&request).unwrap()),
            Ok(Request::Begin {
                method_id: Some(_),
                ..
            })
        ));
    }
    for (target, action, method_id) in [
        ("provider", "login", "anthropic:api_key"),
        ("provider", "logout", "oauth"),
        ("mcp", "login", "oauth"),
    ] {
        let request = json!({"type":"begin","id":"flow","target":target,"key":"anthropic","action":action,"methodId":method_id});
        assert!(protocol::decode(&serde_json::to_vec(&request).unwrap()).is_err());
    }
    assert!(matches!(protocol::decode(br#"{"type":"begin","id":"flow","target":"provider","key":"anthropic","action":"login"}"#), Ok(Request::Begin { method_id: None, .. })));
}

#[tokio::test]
async fn readiness_probe_and_eof_never_begin_an_authentication_flow() {
    let (server, client) = tokio::io::duplex(65536);
    let (reader, writer) = tokio::io::split(server);
    let temp = tempfile::tempdir().unwrap();
    let cwd = temp.path().to_path_buf();
    let dir = cwd.clone();
    let task = tokio::spawn(serve(BufReader::new(reader), writer, cwd, dir));
    let (reader, mut writer) = tokio::io::split(client);
    let mut reader = BufReader::new(reader);
    let ready = event(&mut reader).await;
    assert_eq!(ready["type"], "ready");
    assert_eq!(ready["protocolVersion"], 1);
    writer.shutdown().await.unwrap();
    task.await.unwrap().unwrap();
    assert!(!temp.path().join("auth.json").exists());
}
#[tokio::test]
async fn stale_reply_cannot_answer_the_prompt_and_secret_never_reaches_stdout() {
    let (mut reader, mut writer, task) = scripted(|panel| async move {
        panel.progress("fixture-secret must not be echoed");
        let answer = panel
            .paste_prompt("Enter API key", PastePromptTone::Text, PasteStyle::Masked)
            .await;
        if answer.as_deref() == Some("fixture-secret") {
            ProviderAuthOutcome::Status("fixture-secret".to_string())
        } else {
            ProviderAuthOutcome::Error("wrong answer".to_string())
        }
    });
    let progress = event(&mut reader).await;
    assert_eq!(progress["type"], "progress");
    assert!(!progress.to_string().contains("fixture-secret"));
    let prompt = event(&mut reader).await;
    assert_eq!(prompt["secret"], true);
    send(
        &mut writer,
        json!({"type":"reply","id":"flow","requestId":"expired","value":"fixture-secret"}),
    )
    .await;
    assert_eq!(event(&mut reader).await["code"], "stale_reply");
    send(&mut writer,json!({"type":"reply","id":"flow","requestId":prompt["requestId"],"value":"fixture-secret"})).await;
    let settled = event(&mut reader).await;
    assert_eq!(settled["status"], "success");
    assert!(!settled.to_string().contains("fixture-secret"));
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn cancel_ends_the_flow_without_submitting_a_secret() {
    let (mut reader, mut writer, task) = scripted(|panel| async move {
        let value = panel
            .paste_prompt("Secret", PastePromptTone::Text, PasteStyle::Masked)
            .await;
        assert!(value.is_none());
        ProviderAuthOutcome::Cancelled
    });
    assert_eq!(event(&mut reader).await["type"], "input");
    send(&mut writer, json!({"type":"cancel","id":"flow"})).await;
    assert_eq!(event(&mut reader).await["status"], "cancelled");
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn team_reply_uses_only_a_listed_id_or_personal_account() {
    let (mut reader, mut writer, task) = scripted(|panel| async move {
        let pick = panel
            .select_team(
                vec![PrimeTeamOption {
                    team_id: "team-a".to_string(),
                    name: "A".to_string(),
                    slug: None,
                    role: None,
                    created_at: None,
                }],
                None,
            )
            .await;
        assert_eq!(pick, pa_tui::auth_panel::PrimeTeamPick::PersonalAccount);
        ProviderAuthOutcome::Status("selected".to_string())
    });
    let prompt = event(&mut reader).await;
    assert_eq!(prompt["type"], "teams");
    send(
        &mut writer,
        json!({"type":"reply","id":"flow","requestId":prompt["requestId"],"value":"unlisted"}),
    )
    .await;
    assert_eq!(event(&mut reader).await["code"], "stale_reply");
    send(
        &mut writer,
        json!({"type":"reply","id":"flow","requestId":prompt["requestId"],"value":null}),
    )
    .await;
    assert_eq!(event(&mut reader).await["status"], "success");
    task.await.unwrap().unwrap();
}
#[tokio::test(start_paused = true)]
async fn unanswered_input_times_out_and_cancels_the_underlying_flow() {
    let (mut reader, _writer, task) = scripted(|panel| async move {
        let _ = panel
            .paste_prompt("Secret", PastePromptTone::Text, PasteStyle::Masked)
            .await;
        ProviderAuthOutcome::Cancelled
    });
    assert_eq!(event(&mut reader).await["type"], "input");
    tokio::time::advance(Duration::from_secs(301)).await;
    assert_eq!(event(&mut reader).await["code"], "timeout");
    assert_eq!(event(&mut reader).await["status"], "error");
    task.await.unwrap().unwrap();
}
#[tokio::test]
async fn broker_api_key_login_and_logout_reuse_the_native_store_in_a_temporary_home() {
    let temp = tempfile::tempdir().unwrap();
    for action in ["login", "logout"] {
        let (server, client) = tokio::io::duplex(65536);
        let (reader, writer) = tokio::io::split(server);
        let cwd = temp.path().to_path_buf();
        let dir = cwd.clone();
        let task = tokio::spawn(serve(BufReader::new(reader), writer, cwd, dir));
        let (reader, mut writer) = tokio::io::split(client);
        let mut reader = BufReader::new(reader);
        assert_eq!(event(&mut reader).await["type"], "ready");
        send(
            &mut writer,
            json!({"type":"begin","id":"flow","target":"provider","key":"openai","action":action}),
        )
        .await;
        if action == "login" {
            let prompt = event(&mut reader).await;
            assert_eq!(prompt["type"], "input");
            send(&mut writer,json!({"type":"reply","id":"flow","requestId":prompt["requestId"],"value":"fixture-api-key"})).await;
        }
        assert_eq!(event(&mut reader).await["status"], "success");
        task.await.unwrap().unwrap();
        let auth = pa_core::auth::AuthStorage::create(temp.path());
        assert_eq!(
            auth.get_all().credential("openai").is_some(),
            action == "login"
        );
    }
}

#[tokio::test]
async fn external_panel_cancellation_wakes_a_pending_native_input() {
    let (tx, mut events) = mpsc::unbounded_channel();
    let panel = AuthPanelHandle::new(tx);
    let waiting = panel.clone();
    let task = tokio::spawn(async move {
        waiting
            .paste_prompt("Secret", PastePromptTone::Text, PasteStyle::Masked)
            .await
    });
    assert!(matches!(
        events.recv().await,
        Some(pa_tui::auth_panel::AuthPanelRequest::PastePrompt { .. })
    ));
    panel.cancel();
    assert_eq!(task.await.unwrap(), None);
    assert!(panel.cancelled());
}

#[test]
fn traces_target_accepts_only_login_and_logout_without_a_method() {
    for action in ["login", "logout"] {
        let request = json!({"type":"begin","id":"flow","target":"traces","key":"prime-agent-traces","action":action});
        assert!(matches!(
            protocol::decode(&serde_json::to_vec(&request).unwrap()),
            Ok(Request::Begin {
                target: protocol::Target::Traces,
                ..
            })
        ));
    }
    for request in [
        json!({"type":"begin","id":"flow","target":"traces","key":"prime-agent-traces","action":"paste"}),
        json!({"type":"begin","id":"flow","target":"traces","key":"prime-agent-traces","action":"key"}),
        json!({"type":"begin","id":"flow","target":"traces","key":"prime-agent-traces","action":"login","methodId":"oauth"}),
    ] {
        assert!(protocol::decode(&serde_json::to_vec(&request).unwrap()).is_err());
    }
}

#[tokio::test]
async fn auth_url_without_instructions_still_emits_a_string() {
    let (mut reader, mut writer, task) = scripted(|panel| async move {
        panel.auth_url("https://example.com/device", None);
        let _ = panel
            .paste_prompt("Code", PastePromptTone::Text, PasteStyle::Visible)
            .await;
        ProviderAuthOutcome::Cancelled
    });
    let url = event(&mut reader).await;
    assert_eq!(url["type"], "auth_url");
    assert_eq!(url["instructions"], "");
    assert_eq!(event(&mut reader).await["type"], "input");
    send(&mut writer, json!({"type":"cancel","id":"flow"})).await;
    assert_eq!(event(&mut reader).await["status"], "cancelled");
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn traces_catalog_state_and_logout_use_the_native_store_in_a_temporary_home() {
    let temp = tempfile::tempdir().unwrap();
    async fn exchange(dir: &std::path::Path, request: Value) -> Value {
        let (server, client) = tokio::io::duplex(65536);
        let (reader, writer) = tokio::io::split(server);
        let task = tokio::spawn(serve(
            BufReader::new(reader),
            writer,
            dir.to_path_buf(),
            dir.to_path_buf(),
        ));
        let (reader, mut writer) = tokio::io::split(client);
        let mut reader = BufReader::new(reader);
        let ready = event(&mut reader).await;
        assert!(ready["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "traces_login"));
        send(&mut writer, request).await;
        let reply = event(&mut reader).await;
        task.await.unwrap().unwrap();
        reply
    }
    let list = json!({"type":"list","id":"flow"});
    assert_eq!(exchange(temp.path(), list.clone()).await["traces"]["configured"], false);
    let mut auth = pa_core::auth::AuthStorage::create(temp.path());
    auth.set(
        pa_core::auth::PRIME_AGENT_TRACES_PROVIDER_ID,
        pa_core::auth::AuthCredential::ApiKey {
            key: "fixture-traces-key".to_string(),
            prime_team: None,
        },
    );
    assert_eq!(exchange(temp.path(), list.clone()).await["traces"]["configured"], true);
    let settled = exchange(
        temp.path(),
        json!({"type":"begin","id":"flow","target":"traces","key":"prime-agent-traces","action":"logout"}),
    )
    .await;
    assert_eq!(settled["status"], "success");
    assert_eq!(exchange(temp.path(), list).await["traces"]["configured"], false);
}
