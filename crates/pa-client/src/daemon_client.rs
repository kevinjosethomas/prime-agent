//! JSONL daemon client for the interactive UI: connect, `daemon_hello`
//! handshake, `type: "command"` envelopes matched by envelope id.
//! Non-response frames (session events, list progress, closing notices)
//! are forwarded through an event channel so the UI can render live state.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use pa_types::daemon::{
    is_session_plane_daemon_command, DaemonCommand, DaemonCommandEnvelope, DaemonCommandFrameType,
    DaemonProtocolInfo, DaemonResponse, DAEMON_PROTOCOL_NAME, DAEMON_PROTOCOL_VERSION,
};
use serde_json::{Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};

use crate::direct_transport::{
    connect_direct, direct_attach_capabilities, read_session_transport_ticket,
    supervisor_supports_direct, DirectLink, TICKET_TIMEOUT_MS,
};

mod errors;
#[cfg(all(test, unix))]
mod tests;

pub(crate) use errors::response_data_or_error;
use errors::{command_type_debug, DirectRequestError};
pub use errors::{
    is_command_not_sent, is_command_result_uncertain, is_daemon_rejection, is_daemon_timeout,
    is_daemon_unreachable, is_kernel_not_running, is_update_restarting_rejection,
    rejected_provider_unauthenticated, CommandNotSent, CommandResultUncertain, RequestRejected,
};

/// Optional transport configuration for
/// [`DaemonClient::connect_with_options`]: the lossless raw outbound tap
/// and the opt-in wire bounds. Defaults preserve the existing
/// [`DaemonClient::connect`] behavior exactly — no tap, unbounded frames,
/// unbounded pending requests.
#[derive(Clone, Debug, Default)]
pub struct DaemonClientOptions {
    /// The raw outbound tap: a separate channel carrying every decoded
    /// application frame after the `daemon_hello` handshake — supervisor
    /// reader and direct reader pumps alike — verbatim as the raw JSON
    /// value, in per-socket wire order (no cross-socket ordering is
    /// claimed). Handshake and auth frames (`daemon_hello`, direct
    /// `peer_auth`) and id-correlated `response` frames never appear;
    /// unknown frame types and the full event meta (including absent
    /// fields, exactly as the daemon sent them) always do. Pre-handshake
    /// frames keep their legacy typed-only behavior and never reach the
    /// tap. The channel is unbounded like the event channel: the receiver
    /// must drain it, or memory grows without bound.
    pub raw_tap: Option<mpsc::UnboundedSender<Value>>,
    /// The inbound supervisor line byte cap: a frame beyond it tears the
    /// transport down and fails every pending request as an uncertain
    /// delivery. `None` (the default) keeps the unbounded reader this
    /// crate has always shipped. Inbound only — the size of outbound
    /// command envelopes is not bounded here; that is future command-shape
    /// work. The cap is also not the desktop's invalid-frame policy:
    /// invalid UTF-8 still tears the transport down (legacy), while a
    /// malformed JSON line is still skipped (legacy tolerant behavior);
    /// strict invalid-frame teardown is a separate future policy.
    pub max_frame_bytes: Option<usize>,
    /// The pending-request cap: a request at or beyond it refuses the
    /// send before any bytes are queued (a provable non-delivery).
    /// `None` (the default) keeps the unbounded pending map.
    pub max_pending: Option<usize>,
}

pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
/// Requests whose completion is bounded by the turn itself
/// (`prompt_and_wait`, `wait_for_idle`) — a long turn cannot expire them.
pub const LONG_RUNNING_REQUEST_TIMEOUT_MS: u64 = 600_000;
/// The long-running request set: the commands served with
/// [`LONG_RUNNING_REQUEST_TIMEOUT_MS`]. Contract metadata reads this table.
pub const LONG_RUNNING_COMMAND_TYPES: &[&str] = &["prompt_and_wait", "wait_for_idle"];
const CONNECT_TIMEOUT_MS: u64 = 3_000;
/// Hello handshake budget. A daemon loading a very large session can take
/// well over the 3s TS default to greet.
const HELLO_TIMEOUT_MS: u64 = 15_000;
/// Connect attempts before a connect/handshake failure surfaces: the
/// caller's error path only runs after the last attempt.
pub(crate) const CONNECT_ATTEMPTS: u32 = 3;
/// The first retry's backoff; each further attempt doubles it.
const CONNECT_RETRY_BACKOFF_MS: u64 = 1_000;

/// The update resume contract of a `daemon_closing` frame (spec §10.1).
#[derive(Debug, Clone)]
pub struct DaemonClosingUpdate {
    pub update_id: String,
    pub est_seconds: u64,
    /// The stopped sessions `[{sessionId, name}]`: reattach targets after the restart.
    pub sessions: Vec<Value>,
}

#[derive(Debug, Clone)]
pub enum DaemonClientEvent {
    /// `session_event`: one streamed event, with the frame's
    /// `meta.sequence` (the worker's monotonic counter).
    SessionEvent {
        active_session_id: String,
        event: Value,
        meta_sequence: u64,
    },
    SessionClosed {
        active_session_id: String,
        reason: String,
    },
    /// The direct worker link died: never itself a session loss — the UI
    /// re-attaches through the supervisor, which respawns the worker.
    DirectLinkLost { active_session_id: String },
    /// `session_list_item` progress frame: one saved row as the scan streams it, tagged with its
    /// request id.
    SessionListItem { session: Value, request_id: String },
    /// `session_list_progress` progress frame of `list_saved_sessions`.
    SessionListProgress { loaded: u64, total: u64 },
    /// `daemon_closing`: the supervisor is going down — keep the UI mounted
    /// and reconnect (the update resume contract, spec §10.1).
    DaemonClosing {
        reason: String,
        update: Option<DaemonClosingUpdate>,
    },
    /// `side_question_event`: one streamed side-question run (the `/btw` pane tracks it by id).
    SideQuestionEvent {
        active_session_id: String,
        event: Value,
    },
    /// `roster_update`: live roster deltas — changed entries upsert by agent
    /// id, `removed` deletes, `resync` replaces the whole roster.
    RosterUpdate {
        changed: Vec<Value>,
        removed: Vec<String>,
        resync: bool,
    },
    /// `heartbeats_changed`: the daemon-global broadcast when the heartbeat catalog changes.
    HeartbeatsChanged,
    /// `model_catalog_changed`: a background daemon-side catalog refresh
    /// changed the served snapshot. Rust-only extension (no TS counterpart).
    ModelCatalogChanged,
    /// `session_binding`: the supervisor rebound a session to a new active id; the view
    /// re-attaches.
    SessionBinding {
        previous_active_session_id: String,
        active_session_id: String,
    },
}

/// One non-response frame, parsed from a supervisor JSONL line or a direct
/// worker frame payload (both carry the same `DaemonOutbound` shapes).
// A flat frame-type match; splitting would add indirection without changing the flow.
#[allow(clippy::too_many_lines)]
pub(crate) fn client_event_from_value(value: &Value) -> Option<DaemonClientEvent> {
    let frame_type = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match frame_type {
        "session_event" => Some(DaemonClientEvent::SessionEvent {
            active_session_id: value
                .get("activeSessionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            event: value.get("event").cloned().unwrap_or(Value::Null),
            meta_sequence: value
                .get("meta")
                .and_then(|meta| {
                    meta.get("sequence")
                        .or_else(|| meta.get("cursor").and_then(|cursor| cursor.get("sequence")))
                })
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        }),
        "session_closed" => Some(DaemonClientEvent::SessionClosed {
            active_session_id: value
                .get("activeSessionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            reason: value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "side_question_event" => Some(DaemonClientEvent::SideQuestionEvent {
            active_session_id: value
                .get("activeSessionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            event: value.get("event").cloned().unwrap_or(Value::Null),
        }),
        "session_list_item" => Some(DaemonClientEvent::SessionListItem {
            session: value.get("session").cloned().unwrap_or(Value::Null),
            request_id: value
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "session_list_progress" => Some(DaemonClientEvent::SessionListProgress {
            loaded: value.get("loaded").and_then(Value::as_u64).unwrap_or(0),
            total: value.get("total").and_then(Value::as_u64).unwrap_or(0),
        }),
        "daemon_closing" => Some(DaemonClientEvent::DaemonClosing {
            reason: value
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            update: value.get("payload").cloned().and_then(|payload| {
                (payload.get("resume").and_then(Value::as_bool) == Some(true)).then(|| {
                    DaemonClosingUpdate {
                        update_id: payload
                            .get("updateId")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        est_seconds: payload
                            .get("estSeconds")
                            .and_then(Value::as_u64)
                            .unwrap_or_default(),
                        sessions: payload
                            .get("sessions")
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default(),
                    }
                })
            }),
        }),
        "roster_update" => Some(DaemonClientEvent::RosterUpdate {
            changed: value
                .get("changed")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            removed: value
                .get("removed")
                .and_then(Value::as_array)
                .map(|ids| {
                    ids.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            resync: value.get("resync") == Some(&Value::Bool(true)),
        }),
        "heartbeats_changed" => Some(DaemonClientEvent::HeartbeatsChanged),
        "model_catalog_changed" => Some(DaemonClientEvent::ModelCatalogChanged),
        "session_binding" => Some(DaemonClientEvent::SessionBinding {
            previous_active_session_id: value
                .get("previousActiveSessionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            active_session_id: value
                .get("activeSessionId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        _ => None,
    }
}

struct PendingRequest {
    reply: oneshot::Sender<Result<DaemonResponse, anyhow::Error>>,
    direct_owner: Option<Arc<()>>,
}

/// Connection state shared between the request side and the reader task.
#[derive(Default)]
pub(crate) struct Shared {
    /// Pending requests keyed by envelope id. `Ok` is a daemon answer
    /// (including a refusal); `Err` is a transport failure — a refusal is
    /// recoverable UI data, a dead connection is fatal.
    pending: Mutex<HashMap<String, PendingRequest>>,
    /// The opt-in pending-request bound: registration at or beyond it
    /// refuses the send before any bytes are queued. `None` (the default)
    /// keeps the unbounded map this crate has always shipped.
    max_pending: Option<usize>,
}

impl Shared {
    /// Register one pending reply; `false` when the opt-in pending cap
    /// refused the send — the request provably never reached the wire.
    pub(crate) fn register_pending(
        &self,
        id: String,
        reply: oneshot::Sender<Result<DaemonResponse, anyhow::Error>>,
        direct_owner: Option<Arc<()>>,
    ) -> bool {
        let mut pending = self.pending.lock().unwrap();
        if let Some(max_pending) = self.max_pending {
            if pending.len() >= max_pending {
                return false;
            }
        }
        pending.insert(
            id,
            PendingRequest {
                reply,
                direct_owner,
            },
        );
        true
    }

    pub(crate) fn resolve(&self, id: &str, response: DaemonResponse) -> bool {
        let mut pending = self.pending.lock().unwrap();
        pending
            .remove(id)
            .is_some_and(|request| request.reply.send(Ok(response)).is_ok())
    }

    /// Fail every pending request whose id starts with `prefix` with a
    /// transport error: the connection that carried them died. The failure
    /// resolves on the `Err` half — never a synthetic response. Every sweep
    /// is conservatively uncertain: once a line was queued to the writer,
    /// delivery cannot be disproven from the reader side.
    pub(crate) fn fail_pending(&self, prefix: &str, error: &str) {
        let mut pending = self.pending.lock().unwrap();
        let dead: Vec<String> = pending
            .keys()
            .filter(|id| id.starts_with(prefix))
            .cloned()
            .collect();
        for id in dead {
            if let Some(request) = pending.remove(&id) {
                let _ = request
                    .reply
                    .send(Err(anyhow::Error::new(CommandResultUncertain {
                        message: error.to_string(),
                    })));
            }
        }
    }

    /// A replaced link's EOF must not cancel requests sent over its successor.
    pub(crate) fn fail_direct_pending(&self, owner: &Arc<()>, error: &str) {
        let mut pending = self.pending.lock().unwrap();
        let dead: Vec<String> = pending
            .iter()
            .filter(|(_, request)| {
                request
                    .direct_owner
                    .as_ref()
                    .is_some_and(|candidate| Arc::ptr_eq(candidate, owner))
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in dead {
            if let Some(request) = pending.remove(&id) {
                let _ = request
                    .reply
                    .send(Err(anyhow::Error::new(CommandResultUncertain {
                        message: error.to_string(),
                    })));
            }
        }
    }
}

/// Every task one connection owns, shared by all clones of the client:
/// abort/join handles live once per connection, so one clone can never
/// tear down tasks another clone still uses. `shutdown` is the single
/// explicit owner of this set; nothing runs on `Drop`.
#[derive(Default)]
pub(crate) struct TaskSet {
    /// One lock guards the death mark AND every handle: registration and
    /// teardown observe the same state, so a task registered concurrently
    /// with a shutdown can never escape it (it is aborted at registration
    /// and RETAINED — the join still owns it).
    state: Mutex<TaskState>,
    /// Serializes the join phase: concurrent join callers wait behind the
    /// in-flight drain instead of racing it.
    join_lock: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct TaskState {
    /// Set under `state`'s lock before the aborts run: the atomic death
    /// mark comes first, the sweep second.
    done: bool,
    /// Every owned task in insertion order (supervisor reader and writer
    /// first, direct pumps after). Aborted handles STAY here until a join
    /// drains them — dropping one detaches its task.
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl TaskSet {
    /// Whether shutdown already marked the set dead.
    fn is_dead(&self) -> bool {
        self.state.lock().unwrap().done
    }

    /// Register the supervisor reader/writer handles once. `false` when the
    /// connection is already dead: the tasks are aborted AND retained, so
    /// the join owner still drains them — nothing registered is ever
    /// dropped un-joined.
    pub(crate) fn register_connection(
        &self,
        reader: tokio::task::JoinHandle<()>,
        writer: tokio::task::JoinHandle<()>,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.done {
            reader.abort();
            writer.abort();
            state.handles.push(reader);
            state.handles.push(writer);
            return false;
        }
        state.handles.push(reader);
        state.handles.push(writer);
        true
    }

    /// Register one direct link's pumps, reaping finished handles first.
    /// `false` when the connection is already dead: the tasks are aborted
    /// AND retained, so the join owner still drains them.
    pub(crate) fn register_direct(&self, handles: Vec<tokio::task::JoinHandle<()>>) -> bool {
        let mut handles = handles;
        let mut state = self.state.lock().unwrap();
        if state.done {
            for handle in &handles {
                handle.abort();
            }
            state.handles.append(&mut handles);
            return false;
        }
        state.handles.retain(|handle| !handle.is_finished());
        state.handles.append(&mut handles);
        true
    }

    /// Mark dead and abort every owned task IN PLACE. `true` when this call
    /// won the one-shot race. The aborted handles STAY in the set — the
    /// join drains them; dropping them here would detach the tasks and
    /// leave `join_tasks` with nothing to await.
    pub(crate) fn abort_all(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.done {
            return false;
        }
        state.done = true;
        for handle in &state.handles {
            handle.abort();
        }
        true
    }

    /// Take one drainable handle out under the lock (nothing before
    /// shutdown — the tasks are live).
    fn take_one(&self) -> Option<tokio::task::JoinHandle<()>> {
        let mut state = self.state.lock().unwrap();
        if !state.done {
            return None;
        }
        state.handles.pop()
    }

    /// Push an unfinished handle back — a cancelled join's guard re-inserts
    /// its in-flight handle so a later join still owns it.
    fn push_back(&self, handle: tokio::task::JoinHandle<()>) {
        self.state.lock().unwrap().handles.push(handle);
    }

    /// The join phase, cancellation-safe: every handle is awaited through a
    /// [`JoinGuard`] that re-inserts it if this future is cancelled
    /// mid-await — ownership never leaves the set. Returns how many tasks
    /// this drain awaited to completion. `0` before shutdown; a repeat
    /// call after a complete drain also returns `0` (it waited behind the
    /// `join_lock` and found the set empty).
    pub(crate) async fn join_drain(self: &Arc<Self>) -> usize {
        if !self.is_dead() {
            return 0;
        }
        let _serial = self.join_lock.lock().await;
        let mut drained = 0;
        while let Some(handle) = self.take_one() {
            let mut guard = JoinGuard {
                set: self,
                handle: Some(handle),
            };
            // Awaits the task's actual completion; a cancellation of this
            // future re-inserts the unfinished handle via the guard's Drop.
            (&mut guard).await;
            drained += 1;
        }
        drained
    }
}

/// One join in flight: the awaited handle stays SET-OWNED — if the join
/// future is cancelled mid-await, `Drop` pushes the unfinished handle back
/// into the set; only a completed await (the `Ready` arm consumed the
/// handle) removes it.
struct JoinGuard<'a> {
    set: &'a Arc<TaskSet>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl std::future::Future for JoinGuard<'_> {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let Some(handle) = self.handle.as_mut() else {
            return std::task::Poll::Ready(());
        };
        match std::pin::Pin::new(handle).poll(cx) {
            // Completed: the handle is consumed here — Drop will not re-insert.
            std::task::Poll::Ready(_) => {
                self.handle = None;
                std::task::Poll::Ready(())
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl Drop for JoinGuard<'_> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.set.push_back(handle);
        }
    }
}

/// A live connection to the daemon supervisor socket, optionally upgraded
/// with a direct worker link (session-plane commands and events go straight
/// to the session process; the supervisor stays the control plane).
#[derive(Clone)]
pub struct DaemonClient {
    socket_path: PathBuf,
    client_id: String,
    protocol: DaemonProtocolInfo,
    /// Full `daemon_hello` frame (schema id, app version, capabilities).
    hello: Value,
    next_request_id: Arc<AtomicU64>,
    shared: Arc<Shared>,
    writer: mpsc::UnboundedSender<String>,
    /// Direct-transport state (retained event sender + live worker link).
    direct: std::sync::Arc<crate::direct_transport::DirectState>,
    /// The supervisor reader's death watch (see `reader_dead`).
    reader_dead_rx: tokio::sync::watch::Receiver<bool>,
    /// This client's own handle on the shared death watch sender: `shutdown`
    /// flips the watch for every clone without waiting for the reader's own
    /// exit.
    reader_dead_tx: tokio::sync::watch::Sender<bool>,
    /// The connection's tasks (see [`TaskSet`]).
    tasks: Arc<TaskSet>,
    /// The transport options this connection was established with (the
    /// raw outbound tap rides the direct reader pumps on upgrade).
    options: DaemonClientOptions,
}

impl DaemonClient {
    /// Connect to `socket_path`, complete the hello handshake, and return the
    /// client plus the event receiver (the receiver must be polled or the
    /// reader task stalls once the channel's buffer fills).
    ///
    /// # Errors
    ///
    /// Returns `Err` on a connect or hello failure, timeout, or unknown protocol.
    // One linear handshake/reader-setup sequence; splitting would add
    // indirection without changing the flow.
    #[allow(clippy::too_many_lines)]
    pub async fn connect(
        socket_path: &Path,
    ) -> Result<(Self, mpsc::UnboundedReceiver<DaemonClientEvent>)> {
        Self::connect_with_options(socket_path, DaemonClientOptions::default()).await
    }

    /// [`Self::connect`] with [`DaemonClientOptions`]: the raw outbound tap
    /// and the opt-in frame/pending caps. The defaults reproduce
    /// [`Self::connect`]'s behavior exactly.
    ///
    /// # Errors
    ///
    /// Returns `Err` on a connect or hello failure, timeout, or unknown protocol.
    #[allow(clippy::too_many_lines)]
    pub async fn connect_with_options(
        socket_path: &Path,
        options: DaemonClientOptions,
    ) -> Result<(Self, mpsc::UnboundedReceiver<DaemonClientEvent>)> {
        let connect = tokio::time::timeout(
            Duration::from_millis(CONNECT_TIMEOUT_MS),
            pa_types::platform::transport::connect_transport(socket_path),
        )
        .await
        .map_err(|_| {
            anyhow!(
                "Timed out after {CONNECT_TIMEOUT_MS}ms connecting to the Prime Agent daemon. Socket: {}.",
                socket_path.display()
            )
        })?
        .with_context(|| {
            format!(
                "Failed to connect to the Prime Agent daemon. Socket: {}.",
                socket_path.display()
            )
        })?;

        let (reader_half, writer_half) = connect.split();
        let (line_tx, mut line_rx) = mpsc::unbounded_channel::<String>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<DaemonClientEvent>();
        let retained_event_tx = event_tx.clone();
        // The supervisor reader's death signal: the retained event sender keeps
        // the event channel open after the reader exits, so a channel close can
        // never observe a supervisor socket loss.
        let (reader_dead_tx, reader_dead_rx) = tokio::sync::watch::channel(false);
        let client_dead_tx = reader_dead_tx.clone();
        let shared = Arc::new(Shared {
            pending: Mutex::new(HashMap::new()),
            max_pending: options.max_pending,
        });
        let reader_shared = Arc::clone(&shared);
        let tasks = Arc::new(TaskSet::default());
        let raw_tap = options.raw_tap.clone();
        let max_frame_bytes = options.max_frame_bytes;

        // The INLINE handshake: NOTHING spawns until a valid hello — a
        // cancelled connect future (a select arm, a dropped future) closes
        // both socket halves with the future itself, so no transport task can
        // ever be orphaned. The SAME BufReader and partial spill ownership
        // continue into the reader task below (any coalesced post-hello
        // buffered bytes ride along — no frame lost, no read-ahead dropped).
        let (reader, partial, hello) = {
            let mut reader = BufReader::with_capacity(64 * 1024, reader_half);
            let mut partial: Vec<u8> = Vec::new();
            let deadline = tokio::time::sleep(Duration::from_millis(HELLO_TIMEOUT_MS));
            tokio::pin!(deadline);
            loop {
                if let Some(newline) = partial.iter().position(|byte| *byte == b'\n') {
                    let line: Vec<u8> = partial.drain(..=newline).collect();
                    // The legacy teardown class: invalid UTF-8 before the
                    // handshake failed the connect, too.
                    let Ok(text) = std::str::from_utf8(&line) else {
                        return Err(anyhow!(
                            "the daemon sent an invalid frame before the handshake. Socket: {}.",
                            socket_path.display()
                        ));
                    };
                    let Ok(value) = serde_json::from_str::<Value>(text.trim()) else {
                        continue;
                    };
                    if value.get("type").and_then(Value::as_str) == Some("daemon_hello") {
                        break Some(value);
                    }
                    // The legacy pre-hello behavior, verbatim: a known frame
                    // dispatches to the typed channel only — the tap stays
                    // post-handshake-only, and unknown frames stay silent.
                    if let Some(event) = client_event_from_value(&value) {
                        let _ = event_tx.send(event);
                    }
                } else if max_frame_bytes.is_some_and(|cap| partial.len() > cap) {
                    return Err(anyhow!(
                        "a frame exceeded the limit before the Prime Agent daemon handshake. Socket: {}.",
                        socket_path.display()
                    ));
                }
                let resolved = tokio::select! {
                    // The socket closed before the hello: the teardown error.
                    resolved = reader.fill_buf() => resolved,
                    () = &mut deadline => {
                        return Err(anyhow!(
                            "Timed out after {HELLO_TIMEOUT_MS}ms waiting for the Prime Agent daemon handshake. Socket: {}.",
                            socket_path.display()
                        ));
                    }
                };
                let buffer = match resolved {
                    Ok([]) | Err(_) => {
                        return Err(anyhow!(
                            "the daemon connection closed before the handshake. Socket: {}.",
                            socket_path.display()
                        ));
                    }
                    Ok(buffer) => buffer,
                };
                let read = buffer.len();
                partial.extend_from_slice(buffer);
                reader.consume(read);
            }
        };
        let Some(hello) = hello else {
            return Err(anyhow!(
                "the daemon connection closed before the handshake. Socket: {}.",
                socket_path.display()
            ));
        };
        let protocol = hello
            .get("protocol")
            .cloned()
            .and_then(|p| serde_json::from_value::<DaemonProtocolInfo>(p).ok())
            .unwrap_or(DaemonProtocolInfo {
                name: DAEMON_PROTOCOL_NAME.to_string(),
                version: DAEMON_PROTOCOL_VERSION,
            });
        if protocol.name != DAEMON_PROTOCOL_NAME {
            return Err(anyhow!(
                "the daemon on {} speaks an unknown protocol \"{}\"",
                socket_path.display(),
                protocol.name
            ));
        }
        // Envelope protocol version: the shared minimum.
        let version = protocol.version.min(DAEMON_PROTOCOL_VERSION);

        // The SYNC TAIL: no await between these spawns and the registration —
        // a cancellation cannot land here (the structure PR7's review
        // verified for connect_direct). The reader owns the SAME BufReader
        // and partial spill the handshake used.
        let writer_task = tokio::spawn(async move {
            let mut writer = writer_half;
            while let Some(line) = line_rx.recv().await {
                let mut payload = line.into_bytes();
                payload.push(b'\n');
                if writer.write_all(&payload).await.is_err() {
                    break;
                }
            }
            let _ = writer.shutdown().await;
        });
        let reader_task = tokio::spawn(async move {
            let mut reader = reader;
            let mut partial = partial;
            let mut event_tx = event_tx;
            let raw_tap = raw_tap;
            let max_frame_bytes = max_frame_bytes;
            let reader_shared = reader_shared;
            let mut reader_dead_tx = reader_dead_tx;
            // One decoded line: `false` only on invalid UTF-8, the teardown
            // `read_line` always produced. A malformed JSON line is skipped,
            // exactly like before.
            let mut dispatch = |line: &[u8]| -> bool {
                let Ok(text) = std::str::from_utf8(line) else {
                    return false;
                };
                let Ok(value) = serde_json::from_str::<Value>(text.trim()) else {
                    return true;
                };
                let frame_type = value
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                match frame_type {
                    "response" => {
                        if let Ok(response) = serde_json::from_value::<DaemonResponse>(value) {
                            let id = response.id.clone().unwrap_or_default();
                            reader_shared.resolve(&id, response);
                        }
                    }
                    _ => {
                        // The lossless tap first: every decoded application
                        // frame AFTER the handshake, verbatim and in wire
                        // order, before any typed conversion.
                        if let Some(tap) = raw_tap.as_ref() {
                            let _ = tap.send(value.clone());
                        }
                        if let Some(event) = client_event_from_value(&value) {
                            let _ = event_tx.send(event);
                        }
                    }
                }
                true
            };
            loop {
                let buffer = match reader.fill_buf().await {
                    // The socket closed: a trailing unterminated line still
                    // dispatches once, like `read_line`'s last line at EOF,
                    // before the teardown.
                    Ok([]) => {
                        if !partial.is_empty() {
                            let line = std::mem::take(&mut partial);
                            dispatch(&line);
                        }
                        break;
                    }
                    Ok(buffer) => buffer,
                    Err(_) => break,
                };
                if let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
                    // The cap covers the line without its newline, like the
                    // bounded readers' byte accounting — the spill's already
                    // accumulated bytes count toward it too, so a split line
                    // whose head passed the cap alone still tears down.
                    if max_frame_bytes.is_some_and(|cap| partial.len() + newline > cap) {
                        break;
                    }
                    let line: Vec<u8> = if partial.is_empty() {
                        buffer[..newline].to_vec()
                    } else {
                        partial.extend_from_slice(&buffer[..newline]);
                        std::mem::take(&mut partial)
                    };
                    let consumed = newline + 1;
                    reader.consume(consumed);
                    if !dispatch(&line) {
                        break;
                    }
                } else {
                    // No newline in the buffered bytes: the cap also bounds
                    // the partial frame accumulating across fills.
                    if max_frame_bytes.is_some_and(|cap| partial.len() + buffer.len() > cap) {
                        break;
                    }
                    partial.extend_from_slice(buffer);
                    let read = buffer.len();
                    reader.consume(read);
                }
            }
            // The supervisor socket closed: pending requests fail now and the
            // death watch wakes the reconnect driver.
            let _ = reader_dead_tx.send(true);
            reader_shared.fail_pending("daemon_", "the daemon connection closed");
        });
        tasks.register_connection(reader_task, writer_task);
        Ok((
            DaemonClient {
                socket_path: socket_path.to_path_buf(),
                client_id: format!("daemon-tui:{}", std::process::id()),
                protocol: DaemonProtocolInfo {
                    name: DAEMON_PROTOCOL_NAME.to_string(),
                    version,
                },
                hello,
                next_request_id: Arc::new(AtomicU64::new(0)),
                shared,
                writer: line_tx,
                direct: std::sync::Arc::new(crate::direct_transport::DirectState::new(
                    retained_event_tx,
                )),
                reader_dead_rx,
                reader_dead_tx: client_dead_tx,
                tasks,
                options,
            },
            event_rx,
        ))
    }

    /// A fresh receiver for the supervisor reader's death watch: fires when
    /// the reader task ends. Poll it with `watch::Receiver::changed`.
    #[must_use]
    pub fn reader_dead(&self) -> tokio::sync::watch::Receiver<bool> {
        self.reader_dead_rx.clone()
    }

    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// [`Self::connect`] with bounded retries and doubling backoff: a missed
    /// hello is a hiccup, not a fatal condition.
    ///
    /// # Errors
    ///
    /// Returns the last attempt's [`Self::connect`] error.
    pub async fn connect_with_retry(
        socket_path: &Path,
    ) -> Result<(Self, mpsc::UnboundedReceiver<DaemonClientEvent>)> {
        Self::connect_with_retry_with_options(socket_path, DaemonClientOptions::default()).await
    }

    /// [`Self::connect_with_retry`] carrying [`DaemonClientOptions`]: the
    /// tap and caps apply to every attempt unchanged.
    ///
    /// # Errors
    ///
    /// Returns the last attempt's [`Self::connect_with_options`] error.
    pub async fn connect_with_retry_with_options(
        socket_path: &Path,
        options: DaemonClientOptions,
    ) -> Result<(Self, mpsc::UnboundedReceiver<DaemonClientEvent>)> {
        let mut delay = CONNECT_RETRY_BACKOFF_MS;
        for attempt in 1..=CONNECT_ATTEMPTS {
            match DaemonClient::connect_with_options(socket_path, options.clone()).await {
                Ok(pair) => return Ok(pair),
                Err(error) => {
                    if attempt == CONNECT_ATTEMPTS {
                        return Err(error);
                    }
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    delay = delay.saturating_mul(2);
                }
            }
        }
        unreachable!("the attempt range is non-empty")
    }

    /// Protocol identity negotiated in the hello handshake.
    #[must_use]
    pub fn protocol(&self) -> &DaemonProtocolInfo {
        &self.protocol
    }

    /// Whether the daemon advertised `capability` (TS
    /// `supportsServerCapability`): capability-gated commands fall back to
    /// their older shape without it.
    pub fn supports_server_capability(&self, capability: &str) -> bool {
        self.hello
            .get("serverCapabilities")
            .and_then(Value::as_array)
            .is_some_and(|capabilities| {
                capabilities
                    .iter()
                    .any(|entry| entry.as_str() == Some(capability))
            })
    }

    #[must_use]
    pub fn hello(&self) -> &Value {
        &self.hello
    }

    /// Send one command envelope and wait for the matching response, using
    /// the TS default timeout for the command class.
    ///
    /// # Errors
    ///
    /// Returns `Err` like [`Self::request_with_timeout`].
    pub async fn request(&self, command: DaemonCommand) -> Result<DaemonResponse> {
        let timeout_ms = match &command {
            DaemonCommand::PromptAndWait { .. } | DaemonCommand::WaitForIdle { .. } => {
                LONG_RUNNING_REQUEST_TIMEOUT_MS
            }
            _ => DEFAULT_REQUEST_TIMEOUT_MS,
        };
        self.request_with_timeout(command, timeout_ms).await
    }

    /// Session-plane commands for the direct link's session go straight to the
    /// session worker socket; everything else goes to the supervisor. A dead
    /// link before the write falls back to the supervisor; a sent request that
    /// times out surfaces the error instead of retrying, so a prompt can never
    /// execute twice.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the frame cannot be written, the reader dies, or
    /// the timeout elapses.
    pub async fn request_with_timeout(
        &self,
        command: DaemonCommand,
        timeout_ms: u64,
    ) -> Result<DaemonResponse> {
        if let Some(link) = self.direct_link_for(&command) {
            let id = format!(
                "direct_{}",
                self.next_request_id.fetch_add(1, Ordering::SeqCst) + 1
            );
            match self.request_direct(&link, &command, &id, timeout_ms).await {
                Ok(response) => return Ok(response),
                Err(DirectRequestError::NotSent) => {
                    // The link was dead before the frame was queued, so
                    // supervisor routing is safe.
                    self.direct.drop_link();
                }
                // The pending cap refused before queuing: supervisor routing
                // stays safe, and the live link keeps serving.
                Err(DirectRequestError::Capped) => {}
                Err(DirectRequestError::Wait(error)) => return Err(error),
            }
        }
        self.request_supervisor(command, timeout_ms).await
    }

    /// Send one command envelope to the supervisor, bypassing any direct
    /// worker link, and require `success: true`. The supervisor-owned arms
    /// (`abort_compaction`) must reach the supervisor even when a direct link
    /// serves the session: the direct link IS the wedged worker in that case.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the request fails or times out, or carries `success: false`.
    pub async fn request_ok_via_supervisor(&self, command: DaemonCommand) -> Result<Value> {
        let name = command_type_debug(&command);
        let response = self
            .request_supervisor(command, DEFAULT_REQUEST_TIMEOUT_MS)
            .await?;
        response_data_or_error(&name, response)
    }

    /// The supervisor leg of [`Self::request_with_timeout`]: one JSONL
    /// envelope on the supervisor connection.
    async fn request_supervisor(
        &self,
        command: DaemonCommand,
        timeout_ms: u64,
    ) -> Result<DaemonResponse> {
        let id = format!(
            "daemon_{}",
            self.next_request_id.fetch_add(1, Ordering::SeqCst) + 1
        );
        self.request_supervisor_with_id(command, &id, timeout_ms)
            .await
    }

    /// The refusal for a supervisor reader that already ended: nothing can
    /// ever answer a request registered now. A provable non-delivery — the
    /// request was never queued to the writer.
    fn dead_reader_error(&self) -> anyhow::Error {
        anyhow::Error::new(CommandNotSent {
            message: format!(
                "the daemon connection is closed. Socket: {}.",
                self.socket_path.display()
            ),
        })
    }

    /// One JSONL envelope on the supervisor connection, under the caller's
    /// own envelope id (the streamed `session_list_item` frames carry
    /// it). The id must start with `daemon_` — the socket-close failure
    /// sweep filters by that prefix.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the envelope cannot be serialized or sent, the
    /// reader has died, or the timeout elapses.
    ///
    /// # Panics
    ///
    /// Panics when the shared pending-request mutex is poisoned.
    pub async fn request_supervisor_with_id(
        &self,
        command: DaemonCommand,
        id: &str,
        timeout_ms: u64,
    ) -> Result<DaemonResponse> {
        if *self.reader_dead_rx.borrow() {
            return Err(self.dead_reader_error());
        }
        let id = id.to_string();
        let envelope = DaemonCommandEnvelope {
            frame_type: DaemonCommandFrameType::Command,
            id: id.clone(),
            protocol: self.protocol.clone(),
            client_id: Some(self.client_id.clone()),
            command,
        };
        let line = serde_json::to_string(&envelope)?;
        let (tx, rx) = oneshot::channel::<Result<DaemonResponse>>();
        // The opt-in pending cap refuses before any bytes are queued: a
        // provable non-delivery.
        if !self.shared.register_pending(id.clone(), tx, None) {
            return Err(anyhow::Error::new(CommandNotSent {
                message: "too many pending daemon requests".to_string(),
            }));
        }
        // The reader runs on another worker: it can die (and run its failure
        // sweep) between the entry check and this registration. Re-check after
        // inserting — a death it missed is caught here. The request was never
        // queued to the writer, so it stays a provable non-delivery.
        if *self.reader_dead_rx.borrow() {
            self.shared.pending.lock().unwrap().remove(&id);
            return Err(self.dead_reader_error());
        }
        if self.writer.send(line).is_err() {
            // The writer had already exited: the line was never queued, and
            // the registered slot must not linger for the death sweep.
            self.shared.pending.lock().unwrap().remove(&id);
            return Err(anyhow::Error::new(CommandNotSent {
                message: "the daemon connection is closed".to_string(),
            }));
        }
        match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(result)) => result,
            // The reader task died before resolving this request: the line
            // was queued, so delivery is uncertain — never a refusal.
            Ok(Err(_)) => Err(anyhow::Error::new(CommandResultUncertain {
                message: format!(
                    "Connection to the Prime Agent daemon closed. Socket: {}.",
                    self.socket_path.display()
                ),
            })),
            Err(_) => {
                self.shared.pending.lock().unwrap().remove(&id);
                // The line was queued and may have been written: the timeout
                // is an uncertain delivery, never a clean failure.
                Err(anyhow::Error::new(CommandResultUncertain {
                    message: format!(
                        "Timed out after {timeout_ms}ms waiting for the Prime Agent daemon response. Socket: {}.",
                        self.socket_path.display()
                    ),
                }))
            }
        }
    }

    /// Send a command and require `success: true`, surfacing the daemon error
    /// string otherwise.
    ///
    /// # Errors
    ///
    /// Returns `Err` when [`Self::request`] fails or the response carries `success: false`.
    pub async fn request_ok(&self, command: DaemonCommand) -> Result<Value> {
        let name = command_type_debug(&command);
        let response = self.request(command).await?;
        response_data_or_error(&name, response)
    }

    /// The live direct link that may serve `command` (TS `servesDirect`):
    /// the command must be session-plane and address the link's session.
    fn direct_link_for(&self, command: &DaemonCommand) -> Option<DirectLink> {
        let link = self.direct.live_link()?;
        let command_type = command_type_debug(command);
        if !is_session_plane_daemon_command(&command_type) {
            return None;
        }
        let session = serde_json::to_value(command).ok().and_then(|payload| {
            payload
                .get("activeSessionId")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        });
        (session.as_deref() == Some(link.active_session_id.as_str())).then_some(link)
    }

    /// One request over the direct worker link.
    async fn request_direct(
        &self,
        link: &DirectLink,
        command: &DaemonCommand,
        id: &str,
        timeout_ms: u64,
    ) -> std::result::Result<DaemonResponse, DirectRequestError> {
        let payload = self
            .direct_command_payload(command)
            .map_err(|_| DirectRequestError::NotSent)?;
        let frame = pa_types::daemon::framing::encode_private_frame(
            &serde_json::json!({
                "kind": "command",
                "requestId": id,
                "commandType": command_type_debug(command),
            }),
            &serde_json::to_vec(&payload).unwrap_or_default(),
            pa_types::daemon::framing::DEFAULT_PRIVATE_FRAME_LIMITS,
        )
        .map_err(|_| DirectRequestError::NotSent)?;
        let (reply_tx, reply_rx) = oneshot::channel::<Result<DaemonResponse>>();
        // The opt-in pending cap refuses before the frame is queued: a
        // provable non-delivery, safe for the supervisor fallback — the live
        // link itself stays untouched.
        if !self
            .shared
            .register_pending(id.to_string(), reply_tx, Some(Arc::clone(&link.owner)))
        {
            return Err(DirectRequestError::Capped);
        }
        // Post-insert death re-check (the supervisor leg's guard): a
        // shutdown that flipped the watch after this registration refuses
        // here before the queueing — the pending slot is freed, the
        // supervisor fallback refuses in turn, and no line is ever queued
        // to a dying pump.
        if *self.reader_dead_rx.borrow() {
            self.shared.pending.lock().unwrap().remove(id);
            return Err(DirectRequestError::NotSent);
        }
        if !link.send(frame) {
            self.shared.pending.lock().unwrap().remove(id);
            return Err(DirectRequestError::NotSent);
        }
        match tokio::time::timeout(Duration::from_millis(timeout_ms), reply_rx).await {
            Ok(Ok(Ok(response))) => Ok(response),
            // The worker link died: a transport failure, never a refusal.
            Ok(Ok(Err(error))) => Err(DirectRequestError::Wait(error)),
            Ok(Err(_)) => Err(DirectRequestError::Wait(anyhow::Error::new(
                CommandResultUncertain {
                    message: format!(
                        "the direct session connection closed. Socket: {}.",
                        link.socket_path
                    ),
                },
            ))),
            Err(_) => {
                self.shared.pending.lock().unwrap().remove(id);
                Err(DirectRequestError::Wait(anyhow::Error::new(
                    CommandResultUncertain {
                        message: format!(
                            "Timed out after {timeout_ms}ms waiting for the session response. Socket: {}.",
                            link.socket_path
                        ),
                    },
                )))
            }
        }
    }

    /// The wire payload for a direct session command: the command itself
    /// stamped with this client's id (the supervisor stamps it on the routed
    /// path) and, for attach, the same slim-snapshot capability set, so both
    /// paths return identical attach results.
    fn direct_command_payload(&self, command: &DaemonCommand) -> Result<Value> {
        let mut payload = serde_json::to_value(command)?;
        if let Some(object) = payload.as_object_mut() {
            object.insert("clientId".to_string(), serde_json::json!(self.client_id));
            if matches!(command, DaemonCommand::Attach { .. }) {
                object.insert(
                    "capabilities".to_string(),
                    serde_json::json!(direct_attach_capabilities()),
                );
            }
        }
        Ok(payload)
    }

    /// The active direct link's session, when this client upgraded.
    #[must_use]
    pub fn direct_session_id(&self) -> Option<String> {
        self.direct.session_id()
    }

    /// Drop the direct link and keep plain supervisor routing.
    pub fn drop_direct(&self) {
        self.direct.drop_link();
    }

    /// Upgrade this connection with a direct worker link for
    /// `active_session_id`.
    ///
    /// # Errors
    ///
    /// Never returns `Err`: every failure path returns `Ok(false)`.
    pub async fn upgrade_direct(&self, active_session_id: &str) -> Result<bool> {
        if !supervisor_supports_direct(&self.hello) {
            return Ok(false);
        }
        if self.direct_session_id().as_deref() == Some(active_session_id) {
            return Ok(true);
        }
        self.drop_direct();
        let Ok(ticket_response) = self
            .request_with_timeout(
                DaemonCommand::GetDirectWorkerTransport {
                    id: None,
                    active_session_id: active_session_id.to_string(),
                    rest: Map::default(),
                },
                TICKET_TIMEOUT_MS,
            )
            .await
        else {
            return Ok(false);
        };
        if !ticket_response.success {
            return Ok(false);
        }
        let Some(data) = ticket_response.data else {
            return Ok(false);
        };
        let Ok(ticket) = read_session_transport_ticket(&data, active_session_id) else {
            return Ok(false);
        };
        let Some(event_tx) = self.direct.event_sender() else {
            return Ok(false);
        };
        match connect_direct(
            &ticket,
            Arc::clone(&self.shared),
            event_tx,
            self.options.raw_tap.clone(),
        )
        .await
        {
            Ok((link, tasks)) => {
                if self.tasks.register_direct(tasks) {
                    self.direct.set_link(link);
                    Ok(true)
                } else {
                    // The connection died while the handshake ran: the pumps
                    // were aborted and retained at registration — the dead
                    // link is never installed and no upgrade is claimed.
                    link.close();
                    Ok(false)
                }
            }
            Err(_) => Ok(false),
        }
    }

    /// Close the connection; pending requests fail with the closed error.
    pub fn close(&self) {
        self.direct.take_event_sender();
        self.drop_direct();
        let _ = self.writer.send(String::new());
    }

    /// Dispose the connection outright: like [`Self::close`], but the writer
    /// sender is DROPPED too — a half-attached client never survives the
    /// reconnect window.
    pub fn hard_close(&mut self) {
        self.direct.take_event_sender();
        self.drop_direct();
        let (replacement, _) = mpsc::unbounded_channel::<String>();
        let writer = std::mem::replace(&mut self.writer, replacement);
        let _ = writer.send(String::new());
    }

    /// True teardown, idempotent, clone-correct: flip the reader-dead watch
    /// FIRST (the one shared death mark — every clone's request entry and
    /// post-insert re-check refuse from here on), then abort the supervisor
    /// reader and writer pump plus every direct-link pump this connection
    /// ever spawned (aborting drops each task's socket half, so the fd
    /// closes), and fail every pending request NOW as the uncertain delivery
    /// class (no daemon-EOF dependency). Unlike [`Self::close`], nothing
    /// rides on the daemon closing the socket: the tasks are aborted where
    /// they stand. Nothing runs on `Drop` — the caller owns the teardown
    /// explicitly.
    pub fn shutdown(&self) {
        // The death mark FIRST (the one shared truth — the reader-dead
        // watch): a request registering after this point fails its
        // post-insert re-check and never queues (a provable non-delivery),
        // while every request already registered precedes the sweeps below
        // and resolves as an uncertain delivery. No pending request can
        // slip between the mark and the sweep.
        let _ = self.reader_dead_tx.send(true);
        if !self.tasks.abort_all() {
            return;
        }
        self.direct.take_event_sender();
        self.drop_direct();
        // The sweeps the aborted reader and direct pumps would have run:
        // every in-flight request resolves as an uncertain delivery.
        self.shared
            .fail_pending("daemon_", "the daemon connection closed");
        self.shared
            .fail_pending("direct_", "the session connection closed");
    }

    /// Await the REGISTERED tasks' ends after [`Self::shutdown`] — the
    /// supervisor reader and writer pumps and every direct-link pump that
    /// was registered (including any that lost a race with shutdown and
    /// were aborted at registration). Returns how many this drain awaited
    /// to completion. Cancellation-safe: a join future dropped mid-drain
    /// re-inserts its in-flight handle, so a repeated call resumes and
    /// still owns every task; concurrent callers wait behind the in-flight
    /// drain. When a drain returns, the connection owns zero REGISTERED
    /// tasks, timers, or sockets — caller-owned futures (an in-flight
    /// upgrade handshake, an outstanding request) settle or drop
    /// separately on their own; a handle registered after a drain (a
    /// handshake that completed late) is aborted at registration and is
    /// joined by the NEXT call, so a caller wanting one-shot exactness
    /// settles its own operations before the final join.
    pub async fn join_tasks(&self) -> usize {
        self.tasks.join_drain().await
    }
}
