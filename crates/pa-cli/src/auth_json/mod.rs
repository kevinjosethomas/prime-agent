//! Local JSONL adapter for existing provider and MCP auth services. No agent or daemon is started.
mod driver;
mod native;
mod protocol;
#[cfg(test)]
mod tests;
use pa_tui::auth_panel::AuthPanelHandle;
use protocol::{emit, Input, Request};
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncWrite};
use tokio::sync::mpsc;

pub(crate) fn run() -> Result<i32, String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .map_err(|_| "Could not start auth helper.".to_string())?;
    let cwd = std::env::current_dir().map_err(|_| "Working directory unavailable.".to_string())?;
    let result = runtime.block_on(serve(
        tokio::io::BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
        cwd,
        crate::config::get_agent_dir(),
    ));
    runtime.shutdown_timeout(Duration::from_secs(2));
    result
        .map(|()| 0)
        .map_err(|_| "Auth helper connection closed.".to_string())
}
async fn serve<R, W>(
    reader: R,
    mut writer: W,
    cwd: PathBuf,
    agent_dir: PathBuf,
) -> std::io::Result<()>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin,
{
    emit(&mut writer,json!({"type":"ready","protocolVersion":1,"capabilities":["providers","provider_login","provider_logout","mcp_login","mcp_logout","mcp_paste","mcp_key","reply","cancel"]})).await?;
    let (tx, mut input) = mpsc::channel(8);
    let reader_task = tokio::spawn(protocol::pump(reader, tx));
    let request = tokio::time::timeout(Duration::from_secs(30), input.recv()).await;
    let result=match request {
        Ok(Some(Input::Request(Request::List {id})))=>{
            let mut value=native::options(cwd,agent_dir).await;
            value["type"]=json!("options");value["id"]=json!(id);emit(&mut writer,value).await
        }
        Ok(Some(Input::Request(Request::Begin {id,target,key,action,add_account})))=>{
            let (tx,events)=mpsc::unbounded_channel();let panel=AuthPanelHandle::new(tx);
            let flow=native::flow(cwd,agent_dir,target,action,key,add_account.unwrap_or(false),panel.clone());
            driver::drive(&mut input,&mut writer,&id,panel,events,flow).await
        }
        Ok(Some(Input::End)|None)=>Ok(()),
        Ok(Some(Input::Invalid|Input::Request(_)))=>emit(&mut writer,json!({"type":"error","id":null,"code":"invalid_request","message":"Expected a listing or sign-in request."})).await,
        Err(_)=>emit(&mut writer,json!({"type":"error","id":null,"code":"timeout","message":"Auth request timed out."})).await,
    };
    reader_task.abort();
    result
}
