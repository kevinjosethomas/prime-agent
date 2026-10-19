//! One stateless Clef (System One) decision for Primer task routing.
//! The desktop builds the questions; this helper owns auth and the pinned evaluation model.
//! No daemon, session, tools or history are involved.
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pa_ai::types::StreamOptions;
use pa_types::decision::{ChoiceQuestion, SystemOneRequest};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const CLEF: &str = "cloudflare/clef";
const MAX_REQUEST_BYTES: usize = 256 * 1024;
const MAX_QUESTIONS: usize = 32;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RouteRequest {
    id: String,
    state: Value,
    questions: std::collections::BTreeMap<String, ChoiceQuestion>,
}

fn decode(line: &[u8]) -> Result<RouteRequest, &'static str> {
    if line.len() > MAX_REQUEST_BYTES {
        return Err("Task route request exceeds 256 KiB");
    }
    let request: RouteRequest =
        serde_json::from_slice(line).map_err(|_| "Invalid task route request")?;
    if request.id.trim().is_empty()
        || request.id.len() > 128
        || !request.state.is_object()
        || request.questions.is_empty()
        || request.questions.len() > MAX_QUESTIONS
        || request.questions.iter().any(|(name, question)| {
            name.trim().is_empty()
                || question.instructions.trim().is_empty()
                || question.criteria.len() < 2
                || question.criteria.len() > MAX_QUESTIONS
        })
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
        move |request| evaluate(agent_dir, request),
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

async fn serve<R, W, F, Fut>(mut reader: R, mut writer: W, evaluator: F) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    F: FnOnce(RouteRequest) -> Fut,
    Fut: Future<Output = Result<Value, &'static str>>,
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
    let response = match tokio::time::timeout(Duration::from_secs(25), evaluator(request)).await {
        Ok(Ok(answers)) => json!({"type": "result", "id": id, "answers": answers}),
        Ok(Err(message)) => error(Some(&id), "unavailable", message),
        Err(_) => error(Some(&id), "timeout", "Task route timed out"),
    };
    emit(&mut writer, response).await
}

fn credentials(agent_dir: &Path) -> Result<(String, StreamOptions), &'static str> {
    let mut registry = pa_core::models::ModelRegistry::create(
        pa_core::auth::AuthStorage::create(agent_dir),
        agent_dir.join("models.json"),
    );
    // Evaluation-only descriptor: reuse native auth/header merging, never register a chat model.
    let configured = registry
        .get_all()
        .iter()
        .find(|model| model.provider == "prime-inference" && model.id == CLEF);
    let base_url = configured
        .or_else(|| {
            registry
                .get_all()
                .iter()
                .find(|model| model.provider == "prime-inference")
        })
        .map_or_else(
            || pa_core::models::PRIME_INFERENCE_BASE_URL.to_string(),
            |model| model.base_url.clone(),
        );
    let target: pa_types::ai::Model = serde_json::from_value(json!({
        "id": CLEF, "name": "Clef evaluation", "api": "systemone",
        "provider": "prime-inference", "baseUrl": base_url,
        "reasoning": false, "input": ["text"],
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
        "contextWindow": 0, "maxTokens": 0,
        "headers": configured.and_then(|model| model.headers.clone()),
    }))
    .map_err(|_| "Cannot prepare task route authentication")?;
    let credentials = registry.get_api_key_and_headers(&target, target.headers.as_ref());
    let header = credentials.headers.as_ref().is_some_and(|headers| {
        headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("authorization"))
    });
    if !credentials.ok || (credentials.api_key.is_none() && !header) {
        return Err("Prime Inference authentication is unavailable");
    }
    Ok((
        base_url,
        StreamOptions {
            api_key: credentials.api_key,
            headers: credentials
                .headers
                .map(|headers| headers.into_iter().collect()),
            timeout_ms: Some(20_000),
            ..Default::default()
        },
    ))
}

async fn evaluate(agent_dir: PathBuf, request: RouteRequest) -> Result<Value, &'static str> {
    let (base_url, options) = credentials(&agent_dir)?;
    let evaluation = SystemOneRequest {
        model: CLEF.to_string(),
        state: request.state,
        questions: request.questions,
        rest: serde_json::Map::new(),
    };
    // The transport already sanitizes provider errors; keep a fixed message regardless.
    let response = pa_ai::systemone::evaluate(&base_url, &evaluation, &options)
        .await
        .map_err(|_| "Task route evaluation is unavailable")?;
    serde_json::to_value(&response.answers).map_err(|_| "Invalid task route evaluation")
}

#[cfg(test)]
mod tests;
