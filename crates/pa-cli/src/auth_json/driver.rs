//! One bounded, cancellation-aware event/reply loop over the existing auth panel channel.
use super::protocol::{emit, Input, Request};
use pa_tui::auth_panel::{AuthPanelHandle, AuthPanelRequest, PrimeTeamOption, PrimeTeamPick};
use pa_tui::provider_auth::ProviderAuthOutcome;
use serde_json::json;
use std::future::Future;
use std::time::Duration;
use tokio::io::AsyncWrite;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep_until, Instant};

const FLOW_TIMEOUT: Duration = Duration::from_secs(600);
const INPUT_TIMEOUT: Duration = Duration::from_secs(300);
enum Prompt {
    Text {
        id: String,
        allow_empty: bool,
        reply: oneshot::Sender<Option<String>>,
    },
    Team {
        id: String,
        teams: Vec<PrimeTeamOption>,
        reply: oneshot::Sender<PrimeTeamPick>,
    },
}
struct CancelOnDrop(AuthPanelHandle);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) async fn drive<W, F>(
    input: &mut mpsc::Receiver<Input>,
    writer: &mut W,
    id: &str,
    panel: AuthPanelHandle,
    mut events: mpsc::UnboundedReceiver<AuthPanelRequest>,
    flow: F,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    F: Future<Output = ProviderAuthOutcome>,
{
    let _guard = CancelOnDrop(panel.clone());
    tokio::pin!(flow);
    let deadline = Instant::now() + FLOW_TIMEOUT;
    let mut prompt_deadline = deadline;
    let mut pending: Option<Prompt> = None;
    let mut sequence = 0_u32;
    loop {
        tokio::select! {
            outcome = &mut flow => {
                let (status,message) = if panel.cancelled() { ("cancelled","Sign-in cancelled.") } else { match outcome { ProviderAuthOutcome::Status(_) => ("success","Authentication updated."), ProviderAuthOutcome::Error(_) => ("error","Authentication could not be completed."), ProviderAuthOutcome::Cancelled => ("cancelled","Sign-in cancelled.") } };
                return emit(writer,json!({"type":"settled","id":id,"status":status,"message":message})).await;
            }
            event = events.recv() => {
                match event {
                    Some(AuthPanelRequest::Progress { .. } | AuthPanelRequest::Waiting { .. }) => emit(writer,json!({"type":"progress","id":id,"message":"Waiting for sign-in…"})).await?,
                    Some(AuthPanelRequest::AuthUrl { url, instructions }) => {
                        if !(url.starts_with("https://") || url.starts_with("http://localhost:") || url.starts_with("http://127.0.0.1:")) || url.len() > 8192 { return emit(writer,json!({"type":"settled","id":id,"status":"error","message":"The sign-in URL is unavailable."})).await; }
                        emit(writer,json!({"type":"auth_url","id":id,"url":url,"instructions":instructions.unwrap_or_default(),"openedByNative":true})).await?;
                    }
                    Some(AuthPanelRequest::PastePrompt { prompt, allow_empty, reply, .. }) => {
                        sequence += 1; let request_id=format!("input:{sequence}");
                        emit(writer,json!({"type":"input","id":id,"requestId":request_id,"prompt":prompt.chars().take(400).collect::<String>(),"secret":true,"allowEmpty":allow_empty})).await?;
                        pending=Some(Prompt::Text {id:request_id,allow_empty,reply});prompt_deadline=(Instant::now()+INPUT_TIMEOUT).min(deadline);
                    }
                    Some(AuthPanelRequest::SelectTeam { teams,current,reply }) => {
                        sequence += 1; let request_id=format!("team:{sequence}");
                        emit(writer,json!({"type":"teams","id":id,"requestId":request_id,"teams":teams.iter().map(|team|json!({"id":team.team_id,"name":team.name})).collect::<Vec<_>>(),"current":current})).await?;
                        pending=Some(Prompt::Team {id:request_id,teams,reply});prompt_deadline=(Instant::now()+INPUT_TIMEOUT).min(deadline);
                    }
                    Some(AuthPanelRequest::ProviderSettled { .. } | AuthPanelRequest::McpSettled { .. } | AuthPanelRequest::TracesSettled { .. }) | None => {}
                }
            }
            request = input.recv() => {
                match request {
                    Some(Input::Request(Request::Cancel {id:request_id})) if request_id==id => { panel.cancel();return emit(writer,json!({"type":"settled","id":id,"status":"cancelled","message":"Sign-in cancelled."})).await; }
                    Some(Input::Request(Request::Reply {id:flow_id,request_id,value})) if flow_id==id => {
                        let matches = match &pending {Some(Prompt::Text {id,allow_empty,..})=> id==&request_id && value.as_ref().is_some_and(|value| *allow_empty || !value.trim().is_empty()),Some(Prompt::Team {id,teams,..})=>id==&request_id && value.as_ref().is_none_or(|value|teams.iter().any(|team| &team.team_id==value)),None=>false};
                        if !matches {emit(writer,json!({"type":"error","id":id,"code":"stale_reply","message":"The sign-in prompt is no longer available or the answer is invalid."})).await?;continue;}
                        match pending.take() {
                            Some(Prompt::Text {reply,..}) => {let _=reply.send(value.map(|value|value.trim().to_string()));}
                            Some(Prompt::Team {teams,reply,..}) => {let choice=match value {Some(value)=>teams.into_iter().find(|team|team.team_id==value).map_or(PrimeTeamPick::Cancelled,PrimeTeamPick::Team),None=>PrimeTeamPick::PersonalAccount};let _=reply.send(choice);}
                            None=>{}
                        }
                        prompt_deadline=deadline;
                    }
                    Some(Input::End) | None => {panel.cancel();return emit(writer,json!({"type":"settled","id":id,"status":"cancelled","message":"Sign-in cancelled."})).await;}
                    Some(Input::Invalid) => {panel.cancel();emit(writer,json!({"type":"error","id":id,"code":"invalid_request","message":"Invalid auth request."})).await?;return emit(writer,json!({"type":"settled","id":id,"status":"error","message":"Invalid auth request."})).await;}
                    Some(Input::Request(_)) => emit(writer,json!({"type":"error","id":id,"code":"busy","message":"Only the current sign-in prompt can receive a reply."})).await?,
                }
            }
            () = sleep_until(prompt_deadline) => {panel.cancel();emit(writer,json!({"type":"error","id":id,"code":"timeout","message":"Sign-in timed out."})).await?;return emit(writer,json!({"type":"settled","id":id,"status":"error","message":"Sign-in timed out."})).await;}
        }
    }
}
