//! One stateless, authenticated completion for Primer task routing.
//! The desktop builds the prompt; this helper owns model lookup, auth and inference.
//! No daemon, session, tools or history are involved.
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pa_types::ai::{AssistantContentBlock, Context, Message, StopReason, UserContent, UserMessage};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_REQUEST_BYTES: usize = 256 * 1024;
const MAX_SYSTEM_BYTES: usize = 200_000;
const MAX_USER_BYTES: usize = 64_000;
const MAX_OUTPUT_TOKENS: u64 = 1_000;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RouteRequest {
    id: String,
    model: String,
    system: String,
    user: String,
    max_tokens: u64,
}

pub(crate) struct RouteResponse {
    text: String,
    input: u64,
    output: u64,
}

fn decode(line: &[u8]) -> Result<RouteRequest, &'static str> {
    if line.len() > MAX_REQUEST_BYTES {
        return Err("Task route request exceeds 256 KiB");
    }
    let request: RouteRequest =
        serde_json::from_slice(line).map_err(|_| "Invalid task route request")?;
    if request.id.trim().is_empty()
        || request.id.len() > 128
        || request.model.trim().is_empty()
        || request.model.len() > 256
        || request.system.len() > MAX_SYSTEM_BYTES
        || request.user.trim().is_empty()
        || request.user.len() > MAX_USER_BYTES
        || !(1..=MAX_OUTPUT_TOKENS).contains(&request.max_tokens)
    {
        return Err("Invalid task route request bounds");
    }
    Ok(request)
}

pub(crate) fn run() -> Result<i32, String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|_| "Could not start task router".to_string())?;
    let agent_dir = crate::config::get_agent_dir();
    let result = runtime.block_on(serve(
        tokio::io::BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
        move |request| complete(agent_dir, request),
    ));
    runtime.shutdown_timeout(Duration::from_secs(2));
    result
        .map(|()| 0)
        .map_err(|_| "Task router connection closed".to_string())
}

async fn emit<W: AsyncWrite + Unpin>(writer: &mut W, value: Value) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}

fn error(id: Option<&str>, code: &str, message: &str) -> Value {
    json!({"type": "error", "id": id, "code": code, "message": message})
}

async fn serve<R, W, F, Fut>(mut reader: R, mut writer: W, completer: F) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    F: FnOnce(RouteRequest) -> Fut,
    Fut: Future<Output = Result<RouteResponse, &'static str>>,
{
    emit(&mut writer, json!({"type": "ready", "protocolVersion": 1})).await?;
    let mut line = Vec::new();
    let read = tokio::time::timeout(
        Duration::from_secs(30),
        (&mut reader)
            .take((MAX_REQUEST_BYTES + 1) as u64)
            .read_until(b'\n', &mut line),
    )
    .await;
    match read {
        Ok(Ok(0)) => return Ok(()),
        Ok(Ok(_)) => {}
        Err(_) => {
            return emit(&mut writer, error(None, "timeout", "Task route request timed out")).await
        }
        Ok(Err(_)) => return Err(std::io::Error::other("Task route connection closed")),
    }
    let request = match decode(&line) {
        Ok(request) => request,
        Err(message) => return emit(&mut writer, error(None, "invalid_request", message)).await,
    };
    let id = request.id.clone();
    let response = match tokio::time::timeout(Duration::from_secs(25), completer(request)).await {
        Ok(Ok(response)) => json!({
            "type": "result",
            "id": id,
            "text": response.text,
            "usage": {"input": response.input, "output": response.output},
        }),
        Ok(Err(message)) => error(Some(&id), "unavailable", message),
        Err(_) => error(Some(&id), "timeout", "Task route timed out"),
    };
    emit(&mut writer, response).await
}

fn model_and_auth(
    agent_dir: &Path,
    model_id: &str,
) -> Result<(pa_types::ai::Model, pa_ai::types::StreamOptions), &'static str> {
    let mut registry = pa_core::models::ModelRegistry::create(
        pa_core::auth::AuthStorage::create(agent_dir),
        agent_dir.join("models.json"),
    );
    let model = registry
        .get_all()
        .iter()
        .find(|model| model.provider == "prime-inference" && model.id == model_id)
        .cloned()
        .ok_or("Task route model is not a Prime Inference model")?;
    let credentials = registry.get_api_key_and_headers(&model, model.headers.as_ref());
    let header = credentials.headers.as_ref().is_some_and(|headers| {
        headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("authorization"))
    });
    if !credentials.ok || (credentials.api_key.is_none() && !header) {
        return Err("Prime Inference authentication is unavailable");
    }
    Ok((
        model,
        pa_ai::types::StreamOptions {
            api_key: credentials.api_key,
            headers: credentials
                .headers
                .map(|headers| headers.into_iter().collect()),
            timeout_ms: Some(20_000),
            ..Default::default()
        },
    ))
}

async fn complete(agent_dir: PathBuf, request: RouteRequest) -> Result<RouteResponse, &'static str> {
    let (model, mut base) = model_and_auth(&agent_dir, &request.model)?;
    base.max_tokens = Some(request.max_tokens);
    let mut options = pa_ai::types::SimpleStreamOptions::from_base(base);
    options.reasoning = pa_types::ai::minimum_reasoning(&model);
    let context = Context {
        system_prompt: Some(request.system),
        messages: vec![Message::User(UserMessage {
            content: UserContent::Text(request.user),
            timestamp: 0,
            rest: serde_json::Map::default(),
        })],
        tools: None,
    };
    // Provider errors can quote request text; never forward them.
    let message = pa_ai::complete_simple(&model, &context, Some(options))
        .await
        .map_err(|_| "Task route completion is unavailable")?;
    if matches!(message.stop_reason, StopReason::Error | StopReason::Aborted) {
        return Err("Task route completion failed");
    }
    let text = message
        .content
        .iter()
        .filter_map(|block| match block {
            AssistantContentBlock::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect::<String>();
    Ok(RouteResponse {
        text,
        input: message.usage.input,
        output: message.usage.output,
    })
}

#[cfg(test)]
mod tests;
