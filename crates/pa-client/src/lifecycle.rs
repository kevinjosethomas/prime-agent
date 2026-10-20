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
        });
        let driver = tokio::spawn(driver_run(
            Arc::downgrade(&state),
            config,
            cancel_rx,
        ));
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
    pub fn register_attachment(&self, id: &str, capabilities: Option<Vec<String>>) {
        self.state
            .registry
            .lock()
            .unwrap()
            .register(id, capabilities.unwrap_or_default());
    }

    /// Drop an attachment and every marker it owns.
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
        let bookkeeping = self.before(&command);
        let response = client.request(command).await?;
        self.after(bookkeeping.as_ref(), &response);
        Ok(response)
    }

    /// [`Self::request`] requiring `success: true`.
    ///
    /// # Errors
    ///
    /// Propagates the request error; the daemon refusal surfaces typed.
    pub async fn request_ok(&self, command: DaemonCommand) -> Result<Value> {
        let response = self.request(command).await?;
        if !response.success {
            return Err(anyhow!(
                "the daemon refused the request: {}",
                response
                    .error
                    .unwrap_or_else(|| "unknown error".to_string())
            ));
        }
        Ok(response.data.unwrap_or(Value::Null))
    }

    /// Explicit stop, in order (§10): refuse new requests + epoch bump,
    /// shutdown the live client PROMPTLY (the death signal resolves every
    /// pending request — no live pending is awaited), cancel the driver
    /// (which true-cancels its restore tasks and joins them), then the
    /// driver's final core join; stop awaits the driver's end. Idempotent.
    pub async fn stop(&self) {
        self.state.stopping.store(true, Ordering::SeqCst);
        self.state.epoch.fetch_add(1, Ordering::SeqCst);
        if let Some(client) = self.state.client.write().await.take() {
            client.shutdown();
        }
        let _ = self.state.cancel.send(true);
        let handle = self.state.driver.lock().await.take();
        if let Some(handle) = handle {
            let _ = handle.await;
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
                target_active_session_id, ..
            } => Some(Bookkeeping::Attach {
                id: target_active_session_id.clone(),
                revision: registry.bump_revision(target_active_session_id),
            }),
            DaemonCommand::Detach {
                active_session_id, ..
            } => active_session_id.clone().map(|id| Bookkeeping::Detach { id }),
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
    Done { id: String, data: Value },
    Failed { id: String, error: String },
    /// The connection epoch moved (a swap or a stop): the attempt is moot
    /// and nothing adopts.
    Abandoned,
}

/// One backoff's in-flight state.
struct BackoffState {
    kind: ReconnectKind,
    delay: Duration,
    /// `None` = unbounded.
    deadline: Option<tokio::time::Instant>,
}

/// The driver. Holds only a WEAK session reference: no Arc cycle — when the
/// handles and the consumer are gone, the driver tears itself down via the
/// receiver-closed signal; stop() is the explicit settle path.
async fn driver_run(
    session: std::sync::Weak<SessionState>,
    config: LifecycleConfig,
    mut cancel: watch::Receiver<bool>,
) {
    let (restore_tx, mut restore_rx) = mpsc::unbounded_channel::<RestoreOutcome>();
    let mut restore_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut armed: Option<ReconnectKind> = None;
    let mut backoff: Option<BackoffState> = None;
    loop {
        let Some(state) = session.upgrade() else {
            break; // Handles and consumer gone: nothing to serve.
        };
        if backoff.is_none() {
            publish(&state, |status| status.state = ConnectionState::Connecting);
            let mut transport = config.transport.clone();
            let (tap_tx, tap_rx) = mpsc::unbounded_channel::<Value>();
            transport.raw_tap = Some(tap_tx);
            match DaemonClient::connect_with_options(&config.socket_path, transport).await {
                Ok((client, _legacy_events)) => {
                    let dead = client.reader_dead();
                    let hello = client.hello().clone();
                    let epoch = state.epoch.load(Ordering::SeqCst);
                    *state.client.write().await = Some(client);
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
                    spawn_restores(
                        &state, &config, epoch, &restore_tx, &mut restore_handles,
                    );
                    match service(
                        &state,
                        &config,
                        tap_rx,
                        dead,
                        &mut restore_rx,
                        &mut restore_handles,
                        &mut cancel,
                        &mut armed,
                    )
                    .await
                    {
                        ServiceEnd::Loss => {
                            let kind = armed
                                .take()
                                .filter(|kind| match kind {
                                    ReconnectKind::Update => config.reconnect.update.is_some(),
                                    ReconnectKind::Shutdown => config.reconnect.shutdown.is_some(),
                                    ReconnectKind::Lost => true,
                                })
                                .unwrap_or(ReconnectKind::Lost);
                            let window = config.reconnect.window_for(kind);
                            backoff = Some(fresh_backoff(kind, window));
                        }
                        ServiceEnd::Stop => break,
                    }
                    continue;
                }
                Err(error) => {
                    publish(&state, |status| {
                        status.state = ConnectionState::Disconnected;
                        status.error = Some(error.to_string());
                    });
                    let kind = armed.take().unwrap_or(ReconnectKind::Lost);
                    backoff = Some(fresh_backoff(kind, config.reconnect.window_for(kind)));
                }
            }
        }
        // The backoff tick: sleep, expiry, and the next attempt's delay.
        if !backoff_tick(&state, &config, &mut backoff, &mut cancel).await {
            break;
        }
    }
    // The settle (stop or teardown): cancel restore tasks (TRUE
    // cancellation — never a wedged await), join them, then the final core
    // join of the (already shutdown) client. Nothing runs on Drop.
    if let Some(state) = session.upgrade() {
        let mut handles = std::mem::take(&mut restore_handles);
        for handle in &handles {
            handle.abort();
        }
        for handle in handles.drain(..) {
            let _ = handle.await;
        }
        if let Some(client) = state.client.write().await.take() {
            client.shutdown();
            client.join_tasks().await;
        }
    }
}


/// One backoff tick: sleep with cancellation/consumer-death arms, then
/// expiry (a typed event; the driver stops) and the doubling delay.
/// Returns `false` when the driver must stop.
async fn backoff_tick(
    state: &Arc<SessionState>,
    config: &LifecycleConfig,
    backoff: &mut Option<BackoffState>,
    cancel: &mut watch::Receiver<bool>,
) -> bool {
    let delay = backoff
        .as_ref()
        .map_or(config.reconnect.lost.initial_delay, |state| state.delay);
    let sleep = tokio::time::sleep(delay);
    let gone = consumer_gone(state);
    tokio::pin!(gone);
    tokio::select! {
        () = sleep => {}
        _ = cancel.changed() => return false,
        _ = &mut gone => return false,
    }
    // Window expiry: a typed event, then the driver stops. No exit.
    let expired = backoff
        .as_ref()
        .and_then(|state| state.deadline)
        .is_some_and(|deadline| tokio::time::Instant::now() > deadline);
    if expired {
        let kind = backoff.as_ref().map_or(ReconnectKind::Lost, |state| state.kind);
        let _ = state
            .events
            .send(ConnectionEvent::ReconnectExpired { kind });
        publish(state, |status| {
            status.state = ConnectionState::Expired;
            status.error = Some(format!("the {} reconnect window expired", window_name(kind)));
        });
        return false;
    }
    // The next attempt's delay: doubling, capped.
    if let Some(backoff) = backoff.as_mut() {
        let window = config.reconnect.window_for(backoff.kind);
        backoff.delay = backoff.delay.saturating_mul(2).min(window.max_delay);
    }
    true
}

/// Arm a fresh backoff for one closing kind.
fn fresh_backoff(kind: ReconnectKind, window: &ReconnectWindow) -> BackoffState {
    BackoffState {
        kind,
        delay: window.initial_delay,
        deadline: window
            .window
            .map(|window| tokio::time::Instant::now() + window),
    }
}

fn window_name(kind: ReconnectKind) -> &'static str {
    match kind {
        ReconnectKind::Update => "update",
        ReconnectKind::Shutdown => "shutdown",
        ReconnectKind::Lost => "reconnect",
    }
}

/// How the connected service loop ended.
enum ServiceEnd {
    Loss,
    Stop,
}

/// The connected loop: frames flow while restores run; every state selects
/// cancellation and consumer death; a swap joins its prior connection.
#[allow(clippy::too_many_arguments)]
async fn service(
    state: &Arc<SessionState>,
    config: &LifecycleConfig,
    mut tap_rx: mpsc::UnboundedReceiver<Value>,
    mut dead: watch::Receiver<bool>,
    restore_rx: &mut mpsc::UnboundedReceiver<RestoreOutcome>,
    restore_handles: &mut Vec<tokio::task::JoinHandle<()>>,
    cancel: &mut watch::Receiver<bool>,
    armed: &mut Option<ReconnectKind>,
) -> ServiceEnd {
    loop {
        let gone = consumer_gone(state);
        tokio::pin!(gone);
        tokio::select! {
            _ = cancel.changed() => return ServiceEnd::Stop,
            _ = &mut gone => return ServiceEnd::Stop,
            _ = dead.changed() => {
                if *dead.borrow() {
                    // The old connection's registered tasks are joined NOW:
                    // no swap leaves a prior join behind.
                    if let Some(client) = state.client.write().await.take() {
                        client.shutdown();
                        client.join_tasks().await;
                    }
                    return ServiceEnd::Loss;
                }
            }
            frame = tap_rx.recv() => {
                let Some(frame) = frame else { continue };
                if let Some(crate::daemon_client::DaemonClientEvent::DaemonClosing {
                    update, ..
                }) = crate::daemon_client::client_event_from_value(&frame)
                {
                    *armed = Some(if update.is_some() {
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
                    Some(RestoreOutcome::Abandoned) => (),
                    // All restore senders dropped: reap finished handles.
                    None => {
                        restore_handles.retain(|handle| !handle.is_finished());
                    }
                }
            }
        }
    }
}

/// Resolve when the consumer is gone (the event receiver dropped): the
/// unbounded channel's only closure signal besides send failure.
async fn consumer_gone(state: &Arc<SessionState>) {
    loop {
        if state.events.is_closed() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
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

/// Spawn the restore tasks for every registry entry (per-entry order,
/// concurrent across entries; frames never wait for them). The adoption
/// happens in the task BEFORE its `Done` event — the reattach ordering
/// guarantee the desktop machine pins.
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
        let state = Arc::clone(state);
        let config = config.clone();
        let restore_tx = restore_tx.clone();
        let capabilities = if entry.capabilities.is_empty() {
            config.attach_capabilities.clone()
        } else {
            entry.capabilities.clone()
        };
        let revision = entry.revision + 1;
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
                    state
                        .registry
                        .lock()
                        .unwrap()
                        .record_attach(&id, &[], &data, Some(revision));
                    let _ = restore_tx.send(RestoreOutcome::Done { id, data });
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
}
