//! The managed daemon session: one driver task owning the connection
//! lifecycle (connect, announced-loss reconnect windows, restore-attach)
//! and the multi-attach registry, exposing a clone-safe handle whose
//! requests route directly through the live connection — never serialized
//! behind the driver loop or a restore.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use pa_types::daemon::{DaemonCommand, DaemonResponse};
use serde_json::{Map, Value};
use tokio::sync::{mpsc, watch};

use crate::daemon_client::{CommandNotSent, DaemonClient, DaemonClientOptions};
use crate::registry::FrameAction;

/// One closing kind's reconnect behavior: unbounded (`window: None`) with
/// doubling backoff capped at `max_delay` (`initial == max` is a fixed
/// poll cadence). All timers are core-owned; the host never schedules.
#[derive(Debug, Clone)]
pub struct ReconnectWindow {
    /// `None` = unbounded (desktop semantics).
    pub window: Option<Duration>,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

/// Which closing armed a reconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectKind {
    /// `daemon_closing` carrying the update-resume contract.
    Update,
    /// An announced non-update closing.
    Shutdown,
    /// Unannounced reader loss.
    Lost,
}

/// The reconnect policy per closing kind. A `None` slot never arms for that
/// kind (the loss falls back to `lost`).
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    pub update: Option<ReconnectWindow>,
    pub shutdown: Option<ReconnectWindow>,
    pub lost: ReconnectWindow,
}

impl ReconnectPolicy {
    /// Desktop semantics: every loss is an unbounded exponential backoff,
    /// 250ms doubling to a 10s cap.
    #[must_use]
    pub fn desktop() -> Self {
        let unbounded = ReconnectWindow {
            window: None,
            initial_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(10),
        };
        ReconnectPolicy {
            update: Some(unbounded.clone()),
            shutdown: Some(unbounded.clone()),
            lost: unbounded,
        }
    }

    fn window_for(&self, kind: ReconnectKind) -> &ReconnectWindow {
        match kind {
            ReconnectKind::Update => self.update.as_ref().unwrap_or(&self.lost),
            ReconnectKind::Shutdown => self.shutdown.as_ref().unwrap_or(&self.lost),
            ReconnectKind::Lost => &self.lost,
        }
    }
}

/// The published connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Disconnected,
    Expired,
}

/// A status snapshot (sync-readable via [`DaemonClientSession::status`],
/// published on every transition through [`ConnectionEvent::Status`]).
#[derive(Debug, Clone)]
pub struct ConnectionStatus {
    pub state: ConnectionState,
    pub socket_path: PathBuf,
    pub app_version: Option<String>,
    pub capabilities: Vec<String>,
    pub error: Option<String>,
}

/// Everything the consumer drains from one session.
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    Status(ConnectionStatus),
    /// One raw outbound frame, verbatim, per-socket wire order — except
    /// session events the driver deduped (when dedupe is on). Unbounded;
    /// the receiver must drain.
    Frame(Value),
    ReattachBegin {
        active_session_id: String,
    },
    Reattached {
        active_session_id: String,
        data: Value,
    },
    ReattachFailed {
        active_session_id: String,
        error: String,
    },
    /// A bounded window lapsed: the driver stopped (no exit, no resend).
    ReconnectExpired {
        kind: ReconnectKind,
    },
}

/// The managed session's configuration, validated at [`DaemonClientSession::start`]:
/// every duration must be nonzero where present (`initial_delay <= max_delay`;
/// a `Some(window)` of zero is rejected) — zero is an error, never a magic
/// default.
#[derive(Debug, Clone)]
pub struct LifecycleConfig {
    pub socket_path: PathBuf,
    pub reconnect: ReconnectPolicy,
    /// The restore-attach capability default
    /// (`["attach_snapshot", "event_sequence"]` on the desktop).
    pub attach_capabilities: Vec<String>,
    /// Registry dedupe of replayed session events before `Frame` emission.
    pub dedup_session_events: bool,
    /// One restore-attach attempt's budget. MUST be nonzero.
    pub restore_attempt_timeout: Duration,
    /// 3c1 options; the driver installs its own raw tap (the field must be
    /// `None` here).
    pub transport: DaemonClientOptions,
}

/// The clone-safe handle. Requests route directly through the live client;
/// the registry mutex is held only for synchronous bookkeeping.
pub struct DaemonClientSession {
    state: Arc<SessionState>,
}

struct SessionState {
    status: RwLock<ConnectionStatus>,
    registry: Mutex<crate::registry::AttachmentRegistry>,
    client: tokio::sync::RwLock<Option<DaemonClient>>,
    /// Bumped on every connection swap and at stop: driver-owned
    /// operations abandon when it moves.
    epoch: AtomicU64,
    /// Refuses new requests from the moment stop begins.
    stopping: AtomicBool,
    cancel: watch::Sender<bool>,
    events: mpsc::UnboundedSender<ConnectionEvent>,
    driver: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The driver's completion mark: every stop settles only after it
    /// flips (a concurrent stop that does not own the driver handle waits
    /// on it — no early return before the teardown finished).
    stopped: watch::Sender<bool>,
}

/// The request-path refusal for a stopping or disconnected session: a
/// provable non-delivery (nothing was queued).
fn not_connected() -> anyhow::Error {
    anyhow::Error::new(CommandNotSent {
        message: "the managed daemon session is not connected".to_string(),
    })
}

fn stopping_error() -> anyhow::Error {
    anyhow::Error::new(CommandNotSent {
        message: "the managed daemon session is stopping".to_string(),
    })
}

impl DaemonClientSession {
    /// Start the driver task and return the handle plus the single event
    /// receiver (drain it; the channel is unbounded).
    ///
    /// # Errors
    ///
    /// Rejects a zero duration in the config, a nonzero restore budget
    /// missing, or a `raw_tap` the caller must not set.
    pub async fn start(
        config: LifecycleConfig,
    ) -> Result<(Self, mpsc::UnboundedReceiver<ConnectionEvent>)> {
        validate(&config)?;
        let (event_tx, event_rx) = mpsc::unbounded_channel::<ConnectionEvent>();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (stopped_tx, _stopped_rx) = watch::channel(false);
        let status = ConnectionStatus {
            state: ConnectionState::Connecting,
            socket_path: config.socket_path.clone(),
            app_version: None,
            capabilities: Vec::new(),
            error: None,
        };
        let state = Arc::new(SessionState {
            status: RwLock::new(status),
            registry: Mutex::new(crate::registry::AttachmentRegistry::default()),
            client: tokio::sync::RwLock::new(None),
            epoch: AtomicU64::new(0),
            stopping: AtomicBool::new(false),
            cancel: cancel_tx,
            events: event_tx,
            driver: tokio::sync::Mutex::new(None),
            stopped: stopped_tx,
        });
        let driver = tokio::spawn(driver_run(Arc::downgrade(&state), config, cancel_rx));
        *state.driver.lock().await = Some(driver);
        Ok((DaemonClientSession { state }, event_rx))
    }

    /// The latest status snapshot — synchronous, never blocks on I/O.
    ///
    /// # Panics
    ///
    /// Panics when the status lock is poisoned.
    #[must_use]
    pub fn status(&self) -> ConnectionStatus {
        self.state.status.read().unwrap().clone()
    }

    /// Register an attachment (idempotent per id) for restore-attach and
    /// dedupe.
    ///
    /// # Panics
    ///
    /// Panics when the registry lock is poisoned.
    pub fn register_attachment(&self, id: &str, capabilities: Option<Vec<String>>) {
        self.state
            .registry
            .lock()
            .unwrap()
            .register(id, capabilities.unwrap_or_default());
    }

    /// Drop an attachment and every marker it owns.
    ///
    /// # Panics
    ///
    /// Panics when the registry lock is poisoned.
    pub fn forget_attachment(&self, id: &str) {
        self.state.registry.lock().unwrap().forget(id);
    }

    /// One request over the live connection. Attach/reattach responses adopt
    /// into the registry (revision-guarded); detach/kill drop the entry.
    ///
    /// # Errors
    ///
    /// [`CommandNotSent`] while stopping or disconnected; the client's own
    /// transport/refusal classes otherwise.
    pub async fn request(&self, command: DaemonCommand) -> Result<DaemonResponse> {
        if self.state.stopping.load(Ordering::SeqCst) {
            return Err(stopping_error());
        }
        let client = {
            let guard = self.state.client.read().await;
            guard.clone().ok_or_else(not_connected)?
        };
        // The connection epoch at send time: a response that arrives across
        // a swap (its connection died mid-flight) never adopts — a stale
        // attach response cannot mutate the registry.
        let epoch = self.state.epoch.load(Ordering::SeqCst);
        let bookkeeping = self.before(&command);
        let response = client.request(command).await?;
        if self.state.epoch.load(Ordering::SeqCst) == epoch {
            self.after(bookkeeping.as_ref(), &response);
        }
        Ok(response)
    }

    /// [`Self::request`] requiring `success: true`.
    ///
    /// # Errors
    ///
    /// Propagates the request error; the daemon refusal surfaces typed.
    pub async fn request_ok(&self, command: DaemonCommand) -> Result<Value> {
        let name = wire_name(&command);
        let response = self.request(command).await?;
        if !response.success {
            // The typed refusal surface: the daemon's error info rides
            // along for the callers' classification (is_daemon_rejection &c).
            return Err(anyhow::Error::new(crate::daemon_client::RequestRejected {
                command: name,
                message: response
                    .error
                    .unwrap_or_else(|| "unknown error".to_string()),
                error_info: response.error_info,
            }));
        }
        Ok(response.data.unwrap_or(Value::Null))
    }

    /// Explicit stop, in order (§10): refuse new requests + epoch bump,
    /// shutdown the live client PROMPTLY (the death signal resolves every
    /// pending request — no live pending is awaited), cancel the driver
    /// (which true-cancels its restore tasks and joins them), then the
    /// driver's final core join; stop awaits the driver's end. Idempotent.
    pub async fn stop(&self) {
        // The explicit order: refuse new requests + epoch bump, the death
        // signal on the live client (every pending request resolves), then
        // the driver cancel; every stop settles only after the driver's
        // completion mark flips — a concurrent caller never returns early.
        self.state.stopping.store(true, Ordering::SeqCst);
        self.state.epoch.fetch_add(1, Ordering::SeqCst);
        if let Some(client) = self.state.client.write().await.take() {
            client.shutdown();
        }
        let _ = self.state.cancel.send(true);
        if let Some(handle) = self.state.driver.lock().await.take() {
            let _ = handle.await;
        } else {
            let mut stopped = self.state.stopped.subscribe();
            loop {
                if *stopped.borrow() {
                    break;
                }
                if stopped.changed().await.is_err() {
                    break;
                }
            }
        }
    }

    /// Pre-request registry bookkeeping: attach/reattach bump the
    /// revision; detach/kill only need their id. Synchronous, under the
    /// mutex, never across an await.
    ///
    /// # Panics
    ///
    /// Panics when the registry lock is poisoned.
    fn before(&self, command: &DaemonCommand) -> Option<Bookkeeping> {
        let mut registry = self.state.registry.lock().unwrap();
        match command {
            DaemonCommand::Attach {
                active_session_id, ..
            } => Some(Bookkeeping::Attach {
                id: active_session_id.clone(),
                revision: registry.bump_revision(active_session_id),
            }),
            DaemonCommand::Reattach {
                target_active_session_id,
                ..
            } => Some(Bookkeeping::Attach {
                id: target_active_session_id.clone(),
                revision: registry.bump_revision(target_active_session_id),
            }),
            DaemonCommand::Detach {
                active_session_id, ..
            } => active_session_id
                .clone()
                .map(|id| Bookkeeping::Detach { id }),
            DaemonCommand::Kill {
                active_session_id, ..
            } => Some(Bookkeeping::Detach {
                id: active_session_id.clone(),
            }),
            _ => None,
        }
    }

    /// Post-response registry bookkeeping: an attach/reattach success adopts
    /// at its own revision (never resurrecting a detached or superseded
    /// entry); a completed detach/kill forgets.
    ///
    /// # Panics
    ///
    /// Panics when the registry lock is poisoned.
    fn after(&self, bookkeeping: Option<&Bookkeeping>, response: &DaemonResponse) {
        match bookkeeping {
            Some(Bookkeeping::Attach { id, revision }) => {
                if !response.success {
                    return;
                }
                let data = response.data.clone().unwrap_or(Value::Null);
                let mut registry = self.state.registry.lock().unwrap();
                match revision {
                    Some(revision) => {
                        registry.record_attach(id, &[], &data, Some(*revision));
                    }
                    None => {
                        registry.record_attach(id, &[], &data, None);
                    }
                }
            }
            Some(Bookkeeping::Detach { id }) => {
                self.state.registry.lock().unwrap().forget(id);
            }
            None => {}
        }
    }
}

/// What one request's registry bookkeeping is.
#[derive(Debug, Clone)]
enum Bookkeeping {
    Attach { id: String, revision: Option<u64> },
    Detach { id: String },
}

/// The wire `type` tag of a command (the typed refusal's command name).
fn wire_name(command: &DaemonCommand) -> String {
    serde_json::to_value(command)
        .ok()
        .and_then(|value| value.get("type").cloned())
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| "command".to_string())
}

/// Config validation: zero durations are errors, never magic defaults; the
/// tap belongs to the driver.
fn validate(config: &LifecycleConfig) -> Result<()> {
    if config.restore_attempt_timeout.is_zero() {
        bail!("restore_attempt_timeout must be nonzero");
    }
    if config.transport.raw_tap.is_some() {
        bail!("the driver installs its own raw tap; DaemonClientOptions::raw_tap must be None");
    }
    for window in config
        .reconnect
        .update
        .iter()
        .chain(config.reconnect.shutdown.iter())
        .chain(std::iter::once(&config.reconnect.lost))
    {
        if window.initial_delay.is_zero() || window.max_delay.is_zero() {
            bail!("reconnect delays must be nonzero");
        }
        if window.initial_delay > window.max_delay {
            bail!("reconnect initial_delay must not exceed max_delay");
        }
        if window.window.is_some_and(|window| window.is_zero()) {
            bail!("a reconnect window must be nonzero or None (unbounded)");
        }
    }
    Ok(())
}


/// One restore attempt's outcome, consumed by the driver loop.
enum RestoreOutcome {
    Done {
        id: String,
        data: Value,
    },
    Failed {
        id: String,
        error: String,
    },
    /// The attempt is moot (the epoch moved, or the adoption lost its
    /// revision race): nothing is emitted for it.
    Abandoned,
}

/// The driver: one task owning the connection lifecycle. Holds a WEAK
/// session reference (no Arc cycle) and its own clone of the live
/// connection — the clone is what the final join drains. Every state
/// (connect, backoff sleep, service) selects cancellation and the
/// consumer-closed signal; a future is never lost to a lost wake-up.
async fn driver_run(
    session: std::sync::Weak<SessionState>,
    config: LifecycleConfig,
    mut cancel: watch::Receiver<bool>,
) {
    let (restore_tx, mut restore_rx) = mpsc::unbounded_channel::<RestoreOutcome>();
    let mut restore_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    // The current recovery's state: the closing kind, its window deadline,
    // and the sleep before the next connect attempt. A ZERO delay means
    // "attempt a connection now"; a `None` deadline is unbounded.
    let mut kind = ReconnectKind::Lost;
    let mut deadline: Option<tokio::time::Instant> = None;
    let mut delay = Duration::ZERO;
    let mut client: Option<DaemonClient> = None;
    loop {
        let Some(state) = session.upgrade() else {
            break; // Handles and consumer gone: nothing to serve.
        };
        // A pending backoff sleeps FIRST — cancellation and consumer death
        // are select arms — then the window check and the next attempt's
        // delay happen before the connect below.
        if !delay.is_zero() {
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                _ = cancel.changed() => break,
                () = state.events.closed() => break,
            }
            if let Some(deadline) = deadline {
                if tokio::time::Instant::now() > deadline {
                    let _ = state
                        .events
                        .send(ConnectionEvent::ReconnectExpired { kind });
                    publish(&state, |status| {
                        status.state = ConnectionState::Expired;
                        status.error =
                            Some(format!("the {} reconnect window expired", window_name(kind)));
                    });
                    break; // The driver stops; the host decides. No exit.
                }
            }
            let window = config.reconnect.window_for(kind);
            delay = delay.saturating_mul(2).min(window.max_delay);
        }
        if client.is_none() {
            publish(&state, |status| status.state = ConnectionState::Connecting);
            let mut transport = config.transport.clone();
            let (tap_tx, tap_rx) = mpsc::unbounded_channel::<Value>();
            transport.raw_tap = Some(tap_tx);
            let connect = DaemonClient::connect_with_options(&config.socket_path, transport);
            tokio::pin!(connect);
            let result = tokio::select! {
                result = &mut connect => result,
                _ = cancel.changed() => break,
                () = state.events.closed() => break,
            };
            match result {
                Ok((fresh, _legacy_events)) => {
                    // A new connection is a new epoch: every in-flight
                    // operation from the prior connection abandons.
                    let epoch = state.epoch.fetch_add(1, Ordering::SeqCst) + 1;
                    let dead = fresh.reader_dead();
                    let hello = fresh.hello().clone();
                    *state.client.write().await = Some(fresh.clone());
                    client = Some(fresh);
                    publish(&state, |status| {
                        status.state = ConnectionState::Connected;
                        status.app_version = hello
                            .get("appVersion")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        status.capabilities = hello
                            .get("serverCapabilities")
                            .and_then(Value::as_array)
                            .map(|values| {
                                values
                                    .iter()
                                    .filter_map(Value::as_str)
                                    .map(str::to_string)
                                    .collect()
                            })
                            .unwrap_or_default();
                    });
                    spawn_restores(&state, &config, epoch, &restore_tx, &mut restore_handles);
                    match service(
                        &state,
                        &config,
                        tap_rx,
                        dead,
                        &mut restore_rx,
                        &mut restore_handles,
                        &mut cancel,
                    )
                    .await
                    {
                        ServiceEnd::Loss(armed) => {
                            kind = armed;
                            let window = config.reconnect.window_for(kind);
                            deadline = window
                                .window
                                .map(|window| tokio::time::Instant::now() + window);
                            delay = window.initial_delay;
                        }
                        ServiceEnd::Stop => break,
                    }
                }
                Err(error) => {
                    client = None;
                    publish(&state, |status| {
                        status.state = ConnectionState::Disconnected;
                        status.error = Some(error.to_string());
                    });
                    let window = config.reconnect.window_for(kind);
                    if deadline.is_none() {
                        deadline = window
                            .window
                            .map(|window| tokio::time::Instant::now() + window);
                    }
                    delay = if delay.is_zero() {
                        window.initial_delay
                    } else {
                        delay
                    };
                }
            }
        }
    }
    // The settle (stop or teardown): true-cancel the restore tasks (never a
    // wedged await), join them, then the final core join of the live
    // connection — the driver's own clone, so stop()'s slot-take can never
    // orphan the join. Every stop returns only after the mark below flips.
    if let Some(state) = session.upgrade() {
        let handles = std::mem::take(&mut restore_handles);
        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }
        let effective = client
            .take()
            .or_else(|| state.client.write().await.take());
        if let Some(client) = effective {
            client.shutdown();
            client.join_tasks().await;
        }
        if let Some(residue) = state.client.write().await.take() {
            residue.shutdown();
            residue.join_tasks().await;
        }
        let _ = state.stopped.send(true);
    }
}

fn window_name(kind: ReconnectKind) -> &'static str {
    match kind {
        ReconnectKind::Update => "update",
        ReconnectKind::Shutdown => "shutdown",
        ReconnectKind::Lost => "reconnect",
    }
}

/// How the connected service loop ended: a loss carries the closing kind
/// the frames announced (or unannounced `Lost`).
enum ServiceEnd {
    Loss(ReconnectKind),
    Stop,
}

/// The connected loop: frames flow while restores run; every state selects
/// cancellation and consumer death; a swap shuts down and joins the prior
/// connection before the loop returns.
#[allow(clippy::too_many_arguments)]
async fn service(
    state: &Arc<SessionState>,
    config: &LifecycleConfig,
    mut tap_rx: mpsc::UnboundedReceiver<Value>,
    mut dead: watch::Receiver<bool>,
    restore_rx: &mut mpsc::UnboundedReceiver<RestoreOutcome>,
    restore_handles: &mut Vec<tokio::task::JoinHandle<()>>,
    cancel: &mut watch::Receiver<bool>,
) -> ServiceEnd {
    let mut announced: Option<ReconnectKind> = None;
    loop {
        tokio::select! {
            _ = cancel.changed() => return ServiceEnd::Stop,
            () = state.events.closed() => return ServiceEnd::Stop,
            _ = dead.changed() => {
                if *dead.borrow() {
                    // The prior connection's registered tasks are joined
                    // NOW: no swap leaves a prior join behind.
                    if let Some(client) = state.client.write().await.take() {
                        client.shutdown();
                        client.join_tasks().await;
                    }
                    return ServiceEnd::Loss(announced.take().unwrap_or(ReconnectKind::Lost));
                }
            }
            frame = tap_rx.recv() => {
                let Some(frame) = frame else { continue };
                if let Some(crate::daemon_client::DaemonClientEvent::DaemonClosing {
                    update, ..
                }) = crate::daemon_client::client_event_from_value(&frame)
                {
                    announced = Some(if update.is_some() {
                        ReconnectKind::Update
                    } else {
                        ReconnectKind::Shutdown
                    });
                }
                let action = state.registry.lock().unwrap().observe_frame(&frame);
                if config.dedup_session_events && action == FrameAction::Dedupe {
                    continue;
                }
                if state.events.send(ConnectionEvent::Frame(frame)).is_err() {
                    return ServiceEnd::Stop;
                }
            }
            outcome = restore_rx.recv() => {
                match outcome {
                    Some(RestoreOutcome::Done { id, data }) => {
                        let _ = state.events.send(ConnectionEvent::Reattached {
                            active_session_id: id,
                            data,
                        });
                    }
                    Some(RestoreOutcome::Failed { id, error }) => {
                        let _ = state.events.send(ConnectionEvent::ReattachFailed {
                            active_session_id: id,
                            error,
                        });
                    }
                    Some(RestoreOutcome::Abandoned) => {}
                    // All restore senders dropped: reap finished handles.
                    None => {
                        restore_handles.retain(|handle| !handle.is_finished());
                    }
                }
            }
        }
    }
}

/// Publish a status transition: snapshot + event.
///
/// # Panics
///
/// Panics when the status lock is poisoned.
fn publish(state: &Arc<SessionState>, edit: impl FnOnce(&mut ConnectionStatus)) {
    let mut status = state.status.write().unwrap();
    edit(&mut status);
    let _ = state.events.send(ConnectionEvent::Status(status.clone()));
}

/// Spawn the restore tasks for every registry entry. Each restore bumps
/// the entry's revision (its own attempt token) and adopts ONLY at that
/// revision — a `Reattached` event means the adoption truly happened; a
/// lost race emits nothing.
fn spawn_restores(
    state: &Arc<SessionState>,
    config: &LifecycleConfig,
    epoch: u64,
    restore_tx: &mpsc::UnboundedSender<RestoreOutcome>,
    restore_handles: &mut Vec<tokio::task::JoinHandle<()>>,
) {
    let entries = state.registry.lock().unwrap().restore_entries();
    for (id, entry) in entries {
        let _ = state.events.send(ConnectionEvent::ReattachBegin {
            active_session_id: id.clone(),
        });
        let Some(revision) = state.registry.lock().unwrap().bump_revision(&id) else {
            continue;
        };
        let state = Arc::clone(state);
        let config = config.clone();
        let restore_tx = restore_tx.clone();
        let capabilities = if entry.capabilities.is_empty() {
            config.attach_capabilities.clone()
        } else {
            entry.capabilities.clone()
        };
        restore_handles.push(tokio::spawn(async move {
            let attempt = async {
                let client = {
                    let guard = state.client.read().await;
                    guard.clone().ok_or_else(|| anyhow!("the session is not connected"))?
                };
                let command = DaemonCommand::Attach {
                    id: None,
                    active_session_id: id.clone(),
                    client_id: None,
                    capabilities: Some(capabilities.clone()),
                    resume_cursor: entry.cursor.clone(),
                    telemetry_disabled: None,
                    recovery_config: None,
                    env: None,
                    launch_env: None,
                    rest: Map::new(),
                };
                let response = client
                    .request_with_timeout(
                        command,
                        u64::try_from(config.restore_attempt_timeout.as_millis())
                            .unwrap_or(600_000),
                    )
                    .await?;
                if !response.success {
                    bail!(
                        "{}",
                        response
                            .error
                            .unwrap_or_else(|| "the attach was refused".to_string())
                    );
                }
                Ok::<Value, anyhow::Error>(response.data.unwrap_or(Value::Null))
            }
            .await;
            if state.epoch.load(Ordering::SeqCst) != epoch {
                let _ = restore_tx.send(RestoreOutcome::Abandoned);
                return;
            }
            match attempt {
                Ok(data) => {
                    let adopted = state
                        .registry
                        .lock()
                        .unwrap()
                        .record_attach(&id, &[], &data, Some(revision));
                    if adopted {
                        let _ = restore_tx.send(RestoreOutcome::Done {
                            id,
                            data,
                        });
                    } else {
                        let _ = restore_tx.send(RestoreOutcome::Abandoned);
                    }
                }
                Err(error) => {
                    let _ = restore_tx.send(RestoreOutcome::Failed {
                        id,
                        error: error.to_string(),
                    });
                }
            }
        }));
    }
}#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    fn config(socket: &std::path::Path) -> LifecycleConfig {
        LifecycleConfig {
            socket_path: socket.to_path_buf(),
            reconnect: ReconnectPolicy::desktop(),
            attach_capabilities: vec!["attach_snapshot".to_string()],
            dedup_session_events: true,
            restore_attempt_timeout: Duration::from_secs(5),
            transport: DaemonClientOptions::default(),
        }
    }

    /// A scripted supervisor: greets on every accept, records every
    /// command, answers everything EXCEPT holds attach responses until a
    /// release signal (when `hold_attach` is set), streams frames after
    /// the hello (`frames`), and drops the first connection once released
    /// (`drop_first`).
    struct Supervisor {
        commands: Arc<Mutex<Vec<(u64, Value)>>>,
        connected: Arc<AtomicU64>,
        eof: Arc<AtomicU64>,
    }

    impl Supervisor {
        fn spawn(
            dir: &tempfile::TempDir,
            frames: Vec<Value>,
            hold_attach: bool,
            drop_first_after_release: bool,
        ) -> (Self, std::path::PathBuf, tokio::sync::Notify) {
            let socket = dir.path().join("d.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let commands = Arc::new(Mutex::new(Vec::new()));
            let connected = Arc::new(AtomicU64::new(0));
            let eof = Arc::new(AtomicU64::new(0));
            let release = tokio::sync::Notify::new();
            let release_waiter = release.notified();
            tokio::spawn(supervisor_serve(
                listener,
                Arc::clone(&commands),
                Arc::clone(&connected),
                Arc::clone(&eof),
                frames,
                hold_attach,
                drop_first_after_release,
                release_waiter,
            ));
            (
                Supervisor {
                    commands,
                    connected,
                    eof,
                },
                socket,
                release,
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn supervisor_serve(
        listener: UnixListener,
        commands: Arc<Mutex<Vec<(u64, Value)>>>,
        connected: Arc<AtomicU64>,
        eof: Arc<AtomicU64>,
        frames: Vec<Value>,
        hold_attach: bool,
        drop_first_after_release: bool,
        release: tokio::sync::Notify,
    ) {
        let mut release: Option<tokio::sync::Notify> = Some(release);
        let mut connections = 0u64;
        loop {
            let (stream, _) = listener.accept().await.expect("supervisor accept");
            connections += 1;
            connected.store(connections, Ordering::SeqCst);
            let connection = connections;
            let commands = Arc::clone(&commands);
            let eof = Arc::clone(&eof);
            let frames = frames.clone();
            let release_waiter = release
                .take()
                .map(|notify| notify.notified());
            tokio::spawn(async move {
                let (reader, mut writer) = stream.into_split();
                let mut reader = tokio::io::BufReader::new(reader);
                let mut line = String::new();
                let hello = json!({
                    "type": "daemon_hello",
                    "protocol": { "name": "prime-agent.daemon", "version": 7 },
                    "clientId": "srv",
                    "serverCapabilities": [],
                    "appVersion": "mock",
                });
                writer
                    .write_all(format!("{hello}\n").as_bytes())
                    .await
                    .unwrap();
                for frame in frames {
                    writer
                        .write_all(format!("{frame}\n").as_bytes())
                        .await
                        .unwrap();
                }
                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) | Err(_) => {
                            eof.fetch_add(1, Ordering::SeqCst);
                            break;
                        }
                        Ok(_) => {}
                    }
                    let Ok(envelope) = serde_json::from_str::<Value>(line.trim()) else {
                        continue;
                    };
                    let command = envelope["command"].clone();
                    commands.lock().unwrap().push((connection, command.clone()));
                    let is_attach = command["type"] == "attach";
                    if hold_attach && is_attach {
                        // The held restore: everything else flows first.
                        if let Some(waiter) = release_waiter.as_ref() {
                            waiter.await;
                        }
                    }
                    let data = if is_attach {
                        json!({ "lastEventCursor": { "generation": "g", "sequence": 4 } })
                    } else {
                        json!({ "ok": true })
                    };
                    let response = json!({
                        "type": "response",
                        "id": envelope["id"],
                        "command": command["type"],
                        "success": true,
                        "data": data,
                    });
                    writer
                        .write_all(format!("{response}\n").as_bytes())
                        .await
                        .unwrap();
                    // The scripted first-connection death after release.
                    if drop_first_after_release && connection == 1 {
                        if let Some(waiter) = release_waiter.as_ref() {
                            waiter.await;
                        }
                        break;
                    }
                }
            });
        }
    }

    async fn next_event(
        events: &mut mpsc::UnboundedReceiver<ConnectionEvent>,
    ) -> ConnectionEvent {
        tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("an event arrives")
            .expect("the session lives")
    }

    async fn wait_connected(events: &mut mpsc::UnboundedReceiver<ConnectionEvent>) {
        loop {
            match next_event(events).await {
                ConnectionEvent::Status(status) if status.state == ConnectionState::Connected => {
                    return;
                }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn validation_rejects_zero_durations_and_foreign_taps() {
        let mut zero_restore = config(std::path::Path::new("/dev/null"));
        zero_restore.restore_attempt_timeout = Duration::ZERO;
        assert!(DaemonClientSession::start(zero_restore).await.is_err());

        let mut foreign_tap = config(std::path::Path::new("/dev/null"));
        let (tap_tx, _tap_rx) = mpsc::unbounded_channel::<Value>();
        foreign_tap.transport.raw_tap = Some(tap_tx);
        assert!(DaemonClientSession::start(foreign_tap).await.is_err());

        let mut zero_delay = config(std::path::Path::new("/dev/null"));
        zero_delay.reconnect.lost.initial_delay = Duration::ZERO;
        assert!(DaemonClientSession::start(zero_delay).await.is_err());
    }

    #[tokio::test]
    async fn frames_stream_verbatim_to_the_consumer() {
        let dir = tempfile::TempDir::new().unwrap();
        let event = json!({
            "type": "session_event",
            "activeSessionId": "s1",
            "event": { "type": "delta" },
            "meta": { "cursor": { "generation": "g", "sequence": 4 } },
        });
        let (supervisor, socket, _release) =
            Supervisor::spawn(&dir, vec![event.clone()], false, false);
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        wait_connected(&mut events).await;
        // The streamed frame reaches the consumer verbatim, with its full
        // event meta (dedupe semantics are the registry's unit tests).
        let frame = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::Frame(frame) if frame == event => return frame,
                    _ => {}
                }
            }
        })
        .await
        .expect("the streamed frame arrives");
        assert_eq!(frame, event);
        session.stop().await;
        assert!(supervisor.connected.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn reconnect_restores_with_cursor_and_never_resends() {
        let dir = tempfile::TempDir::new().unwrap();
        let (supervisor, socket, release) =
            Supervisor::spawn(&dir, Vec::new(), false, true);
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        wait_connected(&mut events).await;
        // Attach through the request path: the cursor g/4 adopts.
        session.register_attachment("s1", None);
        session
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
                rest: Map::new(),
            })
            .await
            .unwrap();
        // The scripted death of the first connection; the driver reconnects.
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if supervisor.connected.load(Ordering::SeqCst) >= 2 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the driver reconnects");
        // The restore on the second connection: exactly one attach with the
        // adopted resume cursor; nothing else was ever re-sent.
        let mut reattached = false;
        let observed = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::ReattachBegin { active_session_id } => {
                        assert_eq!(active_session_id, "s1");
                    }
                    ConnectionEvent::Reattached { data, .. } => {
                        assert_eq!(data["lastEventCursor"]["sequence"], 4);
                        reattached = true;
                        return;
                    }
                    ConnectionEvent::Status(status)
                        if status.state == ConnectionState::Connected =>
                    {
                        // the second connection's status round
                    }
                    _ => {}
                }
            }
        })
        .await;
        assert!(observed.is_ok() && reattached, "the restore reattached");
        let commands = supervisor.commands.lock().unwrap();
        let second: Vec<&Value> = commands
            .iter()
            .filter(|(connection, _)| *connection == 2)
            .map(|(_, command)| command)
            .collect();
        assert_eq!(second.len(), 1, "only the restore attach was sent");
        assert_eq!(second[0]["type"], "attach");
        assert_eq!(
            second[0]["resumeCursor"],
            json!({ "activeSessionId": "s1", "generation": "g", "sequence": 4 }),
            "the adopted cursor rode the restore"
        );
        session.stop().await;
    }

    #[tokio::test]
    async fn frames_flow_while_a_restore_is_held_and_requests_never_wait() {
        let dir = tempfile::TempDir::new().unwrap();
        let event = json!({
            "type": "session_event",
            "activeSessionId": "s1",
            "event": { "type": "delta" },
            "meta": { "cursor": { "generation": "g", "sequence": 5 } },
        });
        let (supervisor, socket, release) =
            Supervisor::spawn(&dir, vec![event.clone()], true, false);
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        // The restore is held; the frame still streams, and a user request
        // completes without waiting for the restore.
        session.register_attachment("s1", None);
        let session_clone = session.clone();
        let list = tokio::spawn(async move {
            session_clone
                .request_ok(DaemonCommand::List {
                    id: None,
                    all: None,
                    cwd: None,
                    session_dir: None,
                    include_client_owned: None,
                    rest: Map::new(),
                })
                .await
        });
        // Frame delivery while the restore attach is held.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::Frame(frame) if frame == event => return,
                    _ => {}
                }
            }
        })
        .await
        .expect("frames flow while the restore is held");
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::Reattached { .. } => return,
                    _ => {}
                }
            }
        })
        .await
        .expect("the restore completes after release");
        let list = tokio::time::timeout(Duration::from_secs(10), list)
            .await
            .expect("the concurrent request settled")
            .unwrap();
        assert_eq!(list["ok"], true);
        session.stop().await;
        assert!(supervisor.connected.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn stop_settles_the_connection_and_concurrent_stops_all_wait() {
        let dir = tempfile::TempDir::new().unwrap();
        let (supervisor, socket, _release) =
            Supervisor::spawn(&dir, Vec::new(), false, false);
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        wait_connected(&mut events).await;
        // Two concurrent stops: BOTH return only after the mock saw the
        // connection end (its EOF barrier).
        let (one, two) = {
            let one = session.clone();
            let two = session.clone();
            tokio::join!(one.stop(), two.stop())
        };
        let _ = (one, two);
        tokio::time::timeout(Duration::from_secs(10), async {
            while supervisor.eof.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the mock observed the connection end");
        // Post-stop requests are provable non-deliveries.
        let error = session
            .request(DaemonCommand::List {
                id: None,
                all: None,
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: Map::new(),
            })
            .await
            .unwrap_err();
        assert!(crate::daemon_client::is_command_not_sent(&error), "unexpected: {error}");
    }

    #[tokio::test]
    async fn a_cancelled_stop_future_still_settles_the_driver() {
        let dir = tempfile::TempDir::new().unwrap();
        let (_supervisor, socket, release) = Supervisor::spawn(&dir, Vec::new(), true, false);
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        wait_connected(&mut events).await;
        session.register_attachment("s1", None);
        // A held restore makes the settle observable. Cancel the stop
        // mid-await (the timeout drops its future), then a fresh stop must
        // still see the completion — the driver is never detached by the
        // cancelled join.
        tokio::time::timeout(Duration::from_millis(100), session.stop())
            .await
            .ok();
        release.notify_one();
        let settle = tokio::time::timeout(Duration::from_secs(10), session.stop()).await;
        assert!(settle.is_ok(), "the driver settled after the cancelled stop");
    }

    #[tokio::test]
    async fn receiver_drop_during_a_hung_hello_tears_the_connection_down() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("hung.sock");
        // A server that accepts but never greets: the driver's connect
        // future hangs in the hello wait until the consumer dies.
        let listener = UnixListener::bind(&socket).unwrap();
        let accepted = Arc::new(AtomicBool::new(false));
        let eof = Arc::new(AtomicBool::new(false));
        {
            let accepted = Arc::clone(&accepted);
            let eof = Arc::clone(&eof);
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                accepted.store(true, Ordering::SeqCst);
                let (mut reader, _writer) = stream.into_split();
                let mut chunk = [0u8; 64];
                match reader.read(&mut chunk).await {
                    Ok(0) | Err(_) => {
                        eof.store(true, Ordering::SeqCst);
                    }
                    Ok(_) => {}
                }
            });
        }
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !accepted.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the hung connection was accepted");
        // Drop the consumer: the driver must tear the hung connection down.
        drop(events);
        tokio::time::timeout(Duration::from_secs(10), async {
            while !eof.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the hung connection ended when the consumer dropped");
        session.stop().await;
    }

    #[tokio::test]
    async fn bounded_window_expires_after_real_retries() {
        // No daemon: every connect fails; the bounded window must expire
        // only AFTER retries actually happened (the retry loop runs).
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("absent.sock");
        let mut config = config(&socket);
        config.reconnect.lost = ReconnectWindow {
            window: Some(Duration::from_millis(300)),
            initial_delay: Duration::from_millis(20),
            max_delay: Duration::from_millis(40),
        };
        let (session, mut events) = DaemonClientSession::start(config).await.unwrap();
        let mut attempts = 0u64;
        let expired = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::Status(status) => {
                        if status.state == ConnectionState::Disconnected {
                            attempts += 1;
                        }
                        if status.state == ConnectionState::Expired {
                            return;
                        }
                    }
                    ConnectionEvent::ReconnectExpired { .. } => {}
                    _ => {}
                }
            }
        })
        .await;
        assert!(expired.is_ok(), "the bounded window expired");
        assert!(
            attempts >= 2,
            "retries actually ran before expiry (attempts: {attempts})"
        );
        session.stop().await;
    }
}
