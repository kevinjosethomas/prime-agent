//! One-request JSONL composition of semantic-thread evaluation and existing native auth.
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use pa_ai::thread_classification::{
    contrastive_thread_evaluation, contrastive_thread_scores, decode_thread_request,
    thread_evaluation, thread_scores, CLEF_THREAD_MODEL, MAX_THREAD_REQUEST_BYTES,
};
use pa_ai::types::StreamOptions;
use pa_types::thread_classification::{ThreadClassificationRequest, ThreadScore};
use serde_json::{json, Value};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Clone, Copy)]
pub(crate) enum EvaluationMode {
    Binary,
    Contrastive,
}

pub(crate) fn run(mode: EvaluationMode) -> Result<i32, String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|_| "Could not start thread evaluator".to_string())?;
    let agent_dir = crate::config::get_agent_dir();
    let result = runtime.block_on(serve(
        tokio::io::BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
        move |request| evaluate(agent_dir, request, mode),
    ));
    runtime.shutdown_timeout(Duration::from_secs(2));
    result
        .map(|()| 0)
        .map_err(|_| "Thread evaluator connection closed".to_string())
}
async fn emit<W: AsyncWrite + Unpin>(writer: &mut W, value: Value) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}
async fn serve<R, W, F, Fut>(mut reader: R, mut writer: W, evaluator: F) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    F: FnOnce(ThreadClassificationRequest) -> Fut,
    Fut: Future<Output = Result<Vec<ThreadScore>, String>>,
{
    emit(&mut writer, json!({"type":"ready","protocolVersion":1})).await?;
    let mut line = Vec::new();
    let read = tokio::time::timeout(
        Duration::from_secs(30),
        (&mut reader)
            .take((MAX_THREAD_REQUEST_BYTES + 1) as u64)
            .read_until(b'\n', &mut line),
    )
    .await;
    match read {
        Ok(Ok(0)) => return Ok(()),
        Ok(Ok(_)) => {}
        Err(_) => return emit(
            &mut writer,
            json!({"type":"error","id":null,"code":"timeout","message":"Thread request timed out"}),
        )
        .await,
        Ok(Err(_)) => return Err(std::io::Error::other("Thread request connection closed")),
    }
    let Ok(request) = decode_thread_request(&line) else {
        return emit(
            &mut writer,
            json!({"type":"error","id":null,"code":"invalid_request","message":"Invalid thread context"}),
        )
        .await;
    };
    let id = request.id.clone();
    let target_id = request.target.id.clone();
    if request.candidates.is_empty() {
        return emit(
            &mut writer,
            json!({"type":"result","id":id,"targetId":target_id,"scores":[]}),
        )
        .await;
    }
    match tokio::time::timeout(Duration::from_secs(25),evaluator(request)).await {
        Ok(Ok(scores))=>emit(&mut writer,json!({"type":"result","id":id,"targetId":target_id,"scores":scores})).await,
        Ok(Err(message))=>emit(&mut writer,json!({"type":"error","id":id,"code":"unavailable","message":message})).await,
        Err(_)=>emit(&mut writer,json!({"type":"error","id":id,"code":"timeout","message":"Thread evaluation timed out"})).await,
    }
}

fn credentials(agent_dir: &Path) -> Result<(String, StreamOptions), String> {
    let mut registry = pa_core::models::ModelRegistry::create(
        pa_core::auth::AuthStorage::create(agent_dir),
        agent_dir.join("models.json"),
    );
    // Evaluation-only descriptor: reuse native auth/header merging, never register a chat model.
    let configured = registry
        .get_all()
        .iter()
        .find(|model| model.provider == "prime-inference" && model.id == CLEF_THREAD_MODEL);
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
    let target:pa_types::ai::Model=serde_json::from_value(json!({"id":CLEF_THREAD_MODEL,"name":"Clef evaluation","api":"systemone","provider":"prime-inference","baseUrl":base_url,"reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":0,"maxTokens":0,"headers":configured.and_then(|model|model.headers.clone())})).map_err(|_|"Cannot prepare thread evaluation authentication".to_string())?;
    let credentials = registry.get_api_key_and_headers(&target, target.headers.as_ref());
    let header = credentials.headers.as_ref().is_some_and(|headers| {
        headers
            .keys()
            .any(|name| name.eq_ignore_ascii_case("authorization"))
    });
    if !credentials.ok || (credentials.api_key.is_none() && !header) {
        return Err("Prime Inference authentication is unavailable".to_string());
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
async fn evaluate(
    agent_dir: PathBuf,
    request: ThreadClassificationRequest,
    mode: EvaluationMode,
) -> Result<Vec<ThreadScore>, String> {
    let (base_url, options) = credentials(&agent_dir)?;
    let evaluation = match mode {
        EvaluationMode::Binary => thread_evaluation(&request),
        EvaluationMode::Contrastive => contrastive_thread_evaluation(&request),
    };
    let response = pa_ai::systemone::evaluate(&base_url, &evaluation, &options)
        .await
        .map_err(|error| error.to_string())?;
    match mode {
        EvaluationMode::Binary => thread_scores(&request, &response),
        EvaluationMode::Contrastive => contrastive_thread_scores(&request, &response),
    }
    .map_err(str::to_string)
}

#[cfg(test)]
mod tests;
