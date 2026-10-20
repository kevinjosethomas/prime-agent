//! Bounded wire input for the local, one-flow auth broker. Never derive Debug on secret input.
use serde::Deserialize;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(super) const MAX_LINE: usize = 16 * 1024;
const MAX_VALUE: usize = 8 * 1024;

#[derive(Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(super) enum Request {
    List {
        id: String,
    },
    Begin {
        id: String,
        target: Target,
        key: String,
        action: Action,
        method_id: Option<Method>,
        /// `mcp` + `login`: connect one more account of the key's service
        /// under the next free per-account id.
        #[serde(default)]
        add_account: Option<bool>,
    },
    Reply {
        id: String,
        request_id: String,
        value: Option<String>,
    },
    Cancel {
        id: String,
    },
}
#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Target {
    Provider,
    Mcp,
    /// The Prime Agent Traces credential: login and logout only.
    Traces,
}
#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Action {
    Login,
    Logout,
    Paste,
    Key,
}

/// Stable method selectors are scoped to a provider, never encoded into its id.
#[derive(Clone, Copy, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Method {
    Oauth,
    ApiKey,
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
}
impl Request {
    pub(super) fn id(&self) -> &str {
        match self {
            Self::List { id }
            | Self::Begin { id, .. }
            | Self::Reply { id, .. }
            | Self::Cancel { id } => id,
        }
    }
    fn valid(&self) -> bool {
        valid_id(self.id())
            && match self {
                Self::List { .. } | Self::Cancel { .. } => true,
                Self::Begin {
                    id: _,
                    target,
                    key,
                    action,
                    method_id,
                    add_account,
                } => {
                    valid_id(key)
                        && (*target == Target::Mcp
                            || matches!(action, Action::Login | Action::Logout))
                        && (method_id.is_none()
                            || (*target == Target::Provider && *action == Action::Login))
                        && (add_account.is_none()
                            || (*add_account == Some(true)
                                && *target == Target::Mcp
                                && *action == Action::Login))
                }
                Self::Reply {
                    request_id, value, ..
                } => {
                    valid_id(request_id)
                        && value
                            .as_ref()
                            .is_none_or(|value| value.len() <= MAX_VALUE && !value.contains('\0'))
                }
            }
    }
}

pub(super) fn decode(bytes: &[u8]) -> Result<Request, ()> {
    if bytes.len() > MAX_LINE {
        return Err(());
    }
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| ())?;
    if value.get("type").and_then(serde_json::Value::as_str) == Some("reply")
        && value.get("value").is_none()
    {
        return Err(());
    }
    let request: Request = serde_json::from_value(value).map_err(|_| ())?;
    if request.valid() {
        Ok(request)
    } else {
        Err(())
    }
}

pub(super) enum Input {
    Request(Request),
    Invalid,
    End,
}
pub(super) async fn pump<R: AsyncBufRead + Unpin>(
    mut reader: R,
    tx: tokio::sync::mpsc::Sender<Input>,
) {
    loop {
        let mut line = Vec::new();
        let read = (&mut reader)
            .take((MAX_LINE + 1) as u64)
            .read_until(b'\n', &mut line)
            .await;
        let event = match read {
            Ok(0) | Err(_) => Input::End,
            Ok(_) => match decode(&line) {
                Ok(request) => Input::Request(request),
                Err(()) => Input::Invalid,
            },
        };
        let finished = matches!(event, Input::End | Input::Invalid);
        if tx.send(event).await.is_err() || finished {
            return;
        }
    }
}
pub(super) async fn emit<W: AsyncWrite + Unpin>(
    writer: &mut W,
    value: serde_json::Value,
) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await
}
