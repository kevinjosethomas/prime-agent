//! The managed daemon session: one driver task owning the connection
//! lifecycle (connect, announced-loss reconnect windows, restore-attach)
//! and the multi-attach registry, exposing a clone-safe handle whose
//! requests route directly through the live connection — never serialized
//! behind the driver loop or a restore.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
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
    /// The STRUCTURAL error code of the last connect failure: the first
    /// `io::Error` in the preserved anyhow chain, as its stable
    /// snake-cased `io::ErrorKind` label (`not_found` when the socket
    /// path does not exist, `connection_refused` when nothing listens,
    /// `permission_denied`, ...). `None` on every other status —
    /// announced losses, stop, and handshake/timeout failures carry no
    /// OS code and must never authorize a spawn. Platform-aware through
    /// `ErrorKind` (windows ERROR_FILE_NOT_FOUND and WSAECONNREFUSED map
    /// to the same labels); never parsed from the message.
    pub error_code: Option<String>,
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
#[derive(Clone)]
pub struct DaemonClientSession {
    state: Arc<SessionState>,
}

pub(crate) struct SessionState {
    status: RwLock<ConnectionStatus>,
    /// The registry AND the connection epoch share this mutex: adoption
    /// validation and the epoch bump are one critical section (B5).
    registry: Mutex<crate::registry::AttachmentRegistry>,
    client: tokio::sync::RwLock<Option<DaemonClient>>,
    /// Refuses new requests from the moment stop begins.
    stopping: AtomicBool,
    cancel: watch::Sender<bool>,
    events: mpsc::UnboundedSender<ConnectionEvent>,
    /// The driver's handle (never taken by stop) and its completion memo:
    /// the first stop to observe the handle's end fills the memo exactly
    /// once (the honest Result — a panicked driver is an Err, never a
    /// faked clean completion); concurrent stops read the memo, never a
    /// re-poll of the finished handle.
    driver: tokio::sync::Mutex<DriverState>,
    /// The driver's completion mark: every stop settles only after it
    /// flips.
    stopped: watch::Sender<bool>,
}

/// The monitored driver task and its memoized completion. The memo is the
/// CLONEABLE honest outcome: filled once, READ (cloned) by every stop —
/// never consumed — so every concurrent and subsequent caller observes the
/// same truth.
struct DriverState {
    handle: Option<tokio::task::JoinHandle<()>>,
    memo: Option<std::result::Result<(), String>>,
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
            error_code: None,
        };
        let state = Arc::new(SessionState {
            status: RwLock::new(status),
            registry: Mutex::new(crate::registry::AttachmentRegistry::default()),
            client: tokio::sync::RwLock::new(None),
            stopping: AtomicBool::new(false),
            cancel: cancel_tx,
            events: event_tx,
            driver: tokio::sync::Mutex::new(DriverState {
                handle: None,
                memo: None,
            }),
            stopped: stopped_tx,
        });
        let driver = tokio::spawn(driver_run(Arc::downgrade(&state), config, cancel_rx));
        state.driver.lock().await.handle = Some(driver);
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
    ///
    /// # Panics
    ///
    /// Panics when the registry lock is poisoned.
    pub async fn request(&self, command: DaemonCommand) -> Result<DaemonResponse> {
        if self.state.stopping.load(Ordering::SeqCst) {
            return Err(stopping_error());
        }
        // The capture tie (item 10): the client READ guard is HELD while
        // the token mint and the epoch capture run under the registry lock —
        // the client and the epoch are ONE tied pair (a swap between
        // separate reads can never split them). The no-client refusal
        // happens BEFORE any registry mutation, so a request that never
        // sends leaves no orphaned intent. No await inside.
        let (client, bookkeeping) = {
            let guard = self.state.client.read().await;
            let client = guard.as_ref().ok_or_else(not_connected)?.clone();
            let bookkeeping = self.before(&command);
            (client, bookkeeping)
        };
        let guard = AttemptGuard {
            state: &self.state,
            bookkeeping: bookkeeping.clone(),
            armed: true,
        };
        let response = match client.request(command).await {
            Ok(response) => response,
            Err(error) => {
                // A failed fresh attach abandons its intent — the guard's
                // Drop below would do the same; disarm first so the error
                // path and the guard never double-clean.
                guard.disarm();
                self.cleanup_attempt(bookkeeping.as_ref());
                return Err(error);
            }
        };
        guard.disarm();
        self.after(bookkeeping.as_ref(), &response);
        Ok(response)
    }

    /// The failed/cancelled attempt's cleanup: the fresh intent dies (token-
    /// matched), and a REGISTERED attempt's pinned token dangles when it is
    /// still THIS attempt's (the carryover — a newer attempt is untouched).
    fn cleanup_attempt(&self, bookkeeping: Option<&Bookkeeping>) {
        if let Some(Bookkeeping::Attach { id, token, .. }) = bookkeeping {
            let mut registry = self.state.registry.lock().unwrap();
            registry.abandon(id, *token);
            registry.invalidate_matching(id, *token);
        }
    }

    /// [`Self::request`] requiring `success: true`.
    ///
    /// # Errors
    ///
    /// Propagates the request error; the daemon refusal surfaces typed.
    pub async fn request_ok(&self, command: DaemonCommand) -> Result<Value> {
        let name = wire_name(&command);
        let response = self.request(command).await?;
        // The crate's own typed refusal surface: `RequestRejected` with the
        // daemon's error info rides along — never reinvented here.
        crate::daemon_client::response_data_or_error(&name, response)
    }

    /// Explicit stop, in order (§10): refuse new requests + epoch bump,
    /// shutdown the live client PROMPTLY (the death signal resolves every
    /// pending request — no live pending is awaited), cancel the driver
    /// (which true-cancels its restore tasks and joins them), then the
    /// driver's final core join; stop awaits the driver's end. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns the driver's honest completion: an `Err` when the driver
    /// panicked or ended before completing its settle (recorded in the
    /// status, never faked as a clean completion).
    ///
    /// # Panics
    ///
    /// Panics when the registry or status locks are poisoned.
    pub async fn stop(&self) -> Result<()> {
        // The synchronous stop marks, ALL under the registry gate (item 7):
        // the bump, the global invalidation, and the stopping mark are one
        // linearized section — an install holding the same gate cannot
        // publish past it.
        {
            // The synchronous stop marks, ALL under the registry gate (F-1):
            // the stopping mark lands INSIDE the same critical section as
            // the bump and the invalidation — an install holding the same
            // gate can never observe the bump without the mark.
            let mut registry = self.state.registry.lock().unwrap();
            registry.bump_epoch();
            registry.invalidate_all();
            self.state.stopping.store(true, Ordering::SeqCst);
        }
        self.state.cancel.send_replace(true);
        // The death signal on the live client: every pending request
        // resolves now — no live pending is ever awaited. CLONE, not take
        // (item 5): the SLOT retains the owner for the whole cleanup, so a
        // cancelled stop mid-join never drops the last TaskSet Arc.
        let retained = {
            let slot = self.state.client.read().await;
            slot.as_ref().cloned()
        };
        if let Some(client) = retained.as_ref() {
            client.shutdown();
        }
        // The completion authority: the driver's mark, raced against the
        // MONITORED handle (borrowed in place — a cancelled stop retains
        // everything). The first observation of the handle's end memoizes
        // the CLONEABLE outcome exactly once and clears the handle in the
        // same critical section; concurrent and subsequent stops READ the
        // memo, never a re-poll, never a consume.
        let mut stopped = self.state.stopped.subscribe();
        loop {
            if *stopped.borrow_and_update() {
                break;
            }
            let mut driver = self.state.driver.lock().await;
            let Some(handle) = driver.handle.as_mut() else {
                break; // A prior stop observed and memoized the end.
            };
            tokio::select! {
                changed = stopped.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                result = &mut *handle => {
                    // The driver task ended: memoize the cloneable outcome
                    // once, clear the handle (no re-poll).
                    driver.handle = None;
                    driver
                        .memo
                        .get_or_insert(result.map_err(|error| error.to_string()));
                    drop(driver);
                    if *stopped.borrow_and_update() {
                        // The settle completed and the mark flipped before
                        // the task ended: the normal completion.
                                break;
                    }
                    // The task ended WITHOUT the mark: a panicked or failed
                    // settle — finish the teardown this stop owns (the join
                    // runs while the SLOT retains the owner) and return the
                    // honest Err; the mark is never faked.
                    if let Some(client) = retained.as_ref() {
                        client.join_tasks().await;
                    }
                    self.state.client.write().await.take();
                    let outcome = self.state.driver.lock().await.memo.clone();
                    {
                        let mut status = self.state.status.write().unwrap();
                        status.state = ConnectionState::Disconnected;
                        status.error = Some(
                            "the session driver failed before completing its settle"
                                .to_string(),
                        );
                    }
                    return match outcome {
                        // The driver's honest failure — never a faked clean
                        // completion; every stop reads the same memo.
                        Some(Err(message)) => Err(anyhow!("{message}")),
                        Some(Ok(())) => Err(anyhow!(
                            "the session driver ended without completing its settle"
                        )),
                        // Never memoized and never started: the honest answer
                        // for a session with no driver to fail.
                        None => Ok(()),
                    };
                }
            }
        }
        // The join completes while the SLOT retains the owner; only after
        // it does the slot clear (item 5's clone-not-take).
        if let Some(client) = retained.as_ref() {
            client.join_tasks().await;
        }
        self.state.client.write().await.take();
        // A prior stop observed the driver's honest failure and memoized it
        // (the handle is gone): EVERY subsequent stop returns the identical
        // Err — the memo is read, never consumed (item 4).
        let outcome = self.state.driver.lock().await.memo.clone();
        // The mark is the completion truth: a memoized clean END with the
        // mark flipped is the NORMAL completion (the handle arm observed
        // the task end that the mark already certified); a memoized clean
        // END WITHOUT the mark is the died-without-settle shape.
        let mark = *stopped.borrow_and_update();
        match outcome {
            // A prior stop observed the driver's honest failure — every
            // subsequent stop returns the identical Err (the memo is read,
            // never consumed).
            Some(Err(message)) => Err(anyhow!("{message}")),
            // A prior stop observed a driver end WITHOUT the settle: the
            // same honest Err for every later caller — never a faked clean
            // completion.
            Some(Ok(())) if !mark => Err(anyhow!(
                "the session driver ended without completing its settle"
            )),
            // The memoized clean end with the mark flipped, or never
            // memoized: the normal completion.
            _ => Ok(()),
        }
    }

    fn before(&self, command: &DaemonCommand) -> Option<Bookkeeping> {
        // The registry lock: the token mint, the epoch capture, and any
        // invalidation are ONE critical section (B5's lock rule).
        let mut registry = self.state.registry.lock().unwrap();
        let epoch = registry.current_epoch();
        match command {
            DaemonCommand::Attach {
                active_session_id,
                capabilities,
                ..
            }
            | DaemonCommand::Reattach {
                target_active_session_id: active_session_id,
                capabilities,
                ..
            } => Some(Bookkeeping::Attach {
                id: active_session_id.clone(),
                token: registry.attempt_token(active_session_id),
                capabilities: capabilities.clone().unwrap_or_default(),
                epoch,
            }),
            DaemonCommand::Detach {
                active_session_id, ..
            } => {
                let id = active_session_id.clone()?;
                // Invalidate any in-flight attach attempt BEFORE the await: a
                // racing attach response can never adopt past the detach.
                registry.invalidate(&id);
                Some(Bookkeeping::Detach { id, epoch })
            }
            DaemonCommand::Kill {
                active_session_id, ..
            } => {
                let id = active_session_id.clone();
                // The SAME send-time invalidation as Detach: a racing attach
                // response can never adopt past the kill.
                registry.invalidate(&id);
                Some(Bookkeeping::Detach { id, epoch })
            }
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
        // The registry lock: the epoch validation and the adoption are ONE
        // critical section (B5's lock rule — the install/loss bumps run
        // under the same lock, so no bump slips between check and mutation).
        match bookkeeping {
            Some(Bookkeeping::Attach {
                id,
                token,
                capabilities,
                epoch,
            }) => {
                if !response.success {
                    self.state.registry.lock().unwrap().abandon(id, *token);
                    return;
                }
                let data = response.data.clone().unwrap_or(Value::Null);
                let mut registry = self.state.registry.lock().unwrap();
                if registry.current_epoch() != *epoch {
                    return; // A swap intervened: the stale response never adopts.
                }
                registry.record_attach(id, capabilities, &data, *token);
            }
            // A refused detach keeps the attachment (the desktop machine
            // deletes only on a resolved request); a successful one drops
            // it under ONE registry lock (the epoch comparison and the
            // forget in the same critical section — the carryover), and the
            // pending attach was invalidated at SEND time, so nothing can
            // resurrect afterwards.
            Some(Bookkeeping::Detach { id, epoch }) if response.success => {
                self.state
                    .registry
                    .lock()
                    .unwrap()
                    .forget_at_epoch(id, *epoch);
            }
            Some(Bookkeeping::Detach { .. }) | None => {}
        }
    }
}

/// A CANCELLED request's cleanup: the caller can drop the request future at
/// any await — the guard's Drop abandons the attempt's FRESH intent (only
/// when it still holds THIS token — a newer attempt's intent is never
/// removed). Nothing accumulates for cancelled or failed fresh attaches.
struct AttemptGuard<'a> {
    state: &'a Arc<SessionState>,
    bookkeeping: Option<Bookkeeping>,
    armed: bool,
}

impl AttemptGuard<'_> {
    /// Disarm on the completing paths: the response bookkeeping takes over.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for AttemptGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // A CANCELLED request future: the fresh intent dies (token-matched)
        // AND a registered attempt's pinned token dangles when it is still
        // this attempt's — one lock, both cleanups.
        if let Some(Bookkeeping::Attach { id, token, .. }) = &self.bookkeeping {
            let mut registry = self.state.registry.lock().unwrap();
            registry.abandon(id, *token);
            registry.invalidate_matching(id, *token);
        }
    }
}

/// What one request's registry bookkeeping is.
#[derive(Debug, Clone)]
enum Bookkeeping {
    /// The attach's id, its globally unique attempt token, the command's
    /// own capabilities (recorded at adoption), and the connection epoch
    /// captured under the registry lock at send time.
    Attach {
        id: String,
        token: u64,
        capabilities: Vec<String>,
        epoch: u64,
    },
    Detach {
        id: String,
        epoch: u64,
    },
}

/// The wire `type` tag of a command (the typed refusal's command name).
fn wire_name(command: &DaemonCommand) -> String {
    serde_json::to_value(command)
        .ok()
        .and_then(|value| value.get("type").cloned())
        .and_then(|tag| tag.as_str().map(str::to_string))
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

/// The structural error code of a connect failure: the first `io::Error`
/// in the preserved anyhow chain, as its stable snake-cased `ErrorKind`
/// label (`not_found`, `connection_refused`, `permission_denied`, ...).
/// `None` when the chain carries no `io::Error` (handshake or timeout
/// failures — which must never authorize a spawn). Never parsed from the
/// message.
fn os_error_code(error: &anyhow::Error) -> Option<String> {
    let io = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<std::io::Error>())?;
    let name = format!("{:?}", io.kind());
    let mut coded = String::with_capacity(name.len());
    for character in name.chars() {
        if character.is_uppercase() {
            coded.push('_');
        }
        coded.push(character.to_ascii_lowercase());
    }
    Some(coded.trim_start_matches('_').to_string())
}

/// One restore attempt's outcome, consumed by the driver loop.
pub(crate) enum RestoreOutcome {
    Done {
        id: String,
        data: Value,
        epoch: u64,
    },
    Failed {
        id: String,
        error: String,
        epoch: u64,
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
#[allow(clippy::too_many_lines)]
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
        if stopped(&state, &cancel) {
            break;
        }
        // A pending backoff sleeps FIRST — cancellation and consumer death
        // are select arms — then the window check and the next attempt's
        // delay happen before the connect below.
        if !delay.is_zero() {
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                _ = cancel.changed() => break,
                () = state.events.closed() => break,
            }
            if let Some(expiry) = deadline {
                if tokio::time::Instant::now() > expiry {
                    expire(&state, kind);
                    break; // The driver stops; the host decides. No exit.
                }
            }
            let window = config.reconnect.window_for(kind);
            delay = delay.saturating_mul(2).min(window.max_delay);
        }
        if client.is_none() && !stopped(&state, &cancel) {
            publish(&state, |status| status.state = ConnectionState::Connecting);
            let mut transport = config.transport.clone();
            let (tap_tx, tap_rx) = mpsc::unbounded_channel::<Value>();
            transport.raw_tap = Some(tap_tx);
            let connect = DaemonClient::connect_with_options(&config.socket_path, transport);
            tokio::pin!(connect);
            // The window's deadline is armed against the connect itself: a
            // hung hello cannot outlive a bounded recovery.
            let result = if let Some(expiry) = deadline {
                tokio::select! {
                    result = &mut connect => result,
                    _ = cancel.changed() => break,
                    () = state.events.closed() => break,
                    () = tokio::time::sleep_until(expiry) => {
                        expire(&state, kind);
                        break;
                    }
                }
            } else {
                tokio::select! {
                    result = &mut connect => result,
                    _ = cancel.changed() => break,
                    () = state.events.closed() => break,
                }
            };
            match result {
                Ok((fresh, _legacy_events)) => {
                    // The install linearization (item 7), extracted as the
                    // natural private helper the driver uses — the client
                    // WRITE guard and the registry gate held together:
                    // check the stop truth, bump the epoch, assign the
                    // client, and publish Connected in ONE gate-held
                    // section with NO await inside (lock order client ->
                    // registry; stop's marks run under the same registry
                    // gate, so neither can pass the other mid-section).
                    let install = install_fresh(&state, &fresh, &cancel).await;
                    let Some(epoch) = install else {
                        // A stop landed: the fresh connection never
                        // installs; its settle runs outside the guards.
                        fresh.shutdown();
                        fresh.join_tasks().await;
                        break;
                    };
                    let dead = fresh.reader_dead();
                    client = Some(fresh);
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
                            client = None;
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
                    let error_code = os_error_code(&error);
                    publish(&state, |status| {
                        status.state = ConnectionState::Disconnected;
                        status.error = Some(error.to_string());
                        status.error_code = error_code;
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
    // The settle (stop, consumer gone, or weak death): refuse new requests
    // and mark the terminal state FIRST, true-cancel the restore tasks
    // (never a wedged await), join them, then the final core join — the
    // driver's retained clone AND the request-path slot are both taken, so
    // neither survives the settle. Every stop returns only after the mark
    // below flips.
    if let Some(state) = session.upgrade() {
        state.stopping.store(true, Ordering::SeqCst);
        state.registry.lock().unwrap().bump_epoch();
        let handles = std::mem::take(&mut restore_handles);
        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }
        let mut effective = client.take();
        let residue = state.client.write().await.take();
        if effective.is_none() {
            effective = residue;
        }
        if let Some(client) = effective {
            client.shutdown();
            client.join_tasks().await;
        }
        // The terminal snapshot: no request clones a client afterwards, and
        // the sync status() never says Connected past completion. The
        // EXPIRY state is retained (its window already expired — the
        // settle never overwrites it back to Disconnected).
        {
            let mut status = state.status.write().unwrap();
            if status.state != ConnectionState::Expired {
                status.state = ConnectionState::Disconnected;
                status
                    .error
                    .get_or_insert_with(|| "the session stopped".to_string());
            }
            status.error_code = None;
            let _ = state.events.send(ConnectionEvent::Status(status.clone()));
        }
        state.stopped.send_replace(true);
    }
}

/// The persistent stopping check: the watch's `changed()` only fires on a
/// transition, so every state — sleep, connect, install — reads the truth
/// directly (the flag, or the cancel watch's current value).
fn stopped(state: &Arc<SessionState>, cancel: &watch::Receiver<bool>) -> bool {
    state.stopping.load(Ordering::SeqCst) || *cancel.borrow()
}

/// Publish the window expiry: a typed event and the terminal status. The
/// driver stops (no exit, no resend); the host decides.
fn expire(state: &Arc<SessionState>, kind: ReconnectKind) {
    let _ = state
        .events
        .send(ConnectionEvent::ReconnectExpired { kind });
    publish(state, |status| {
        status.state = ConnectionState::Expired;
        status.error = Some(format!(
            "the {} reconnect window expired",
            window_name(kind)
        ));
        status.error_code = None;
    });
}

/// The install linearization (item 7) — the natural private helper the
/// driver calls after a fresh connect resolves: the client WRITE guard is
/// awaited, then the registry gate is taken (client -> registry order) and
/// the stop truth checked, the epoch bumped, the client assigned, and the
/// Connected status published — all inside the one gate-held section, no
/// await within it. `None` = a stop landed: the fresh connection installs
/// nothing and its settle runs at the caller, outside the guards.
pub(crate) async fn install_fresh(
    state: &Arc<SessionState>,
    fresh: &DaemonClient,
    cancel: &watch::Receiver<bool>,
) -> Option<u64> {
    // The hello snapshot is taken BEFORE the gate (a pure read of the
    // resolved connection; the publish itself stays inside).
    let hello = fresh.hello().clone();
    let mut slot = state.client.write().await;
    let mut registry = state.registry.lock().unwrap();
    if stopped(state, cancel) {
        return None;
    }
    let epoch = registry.bump_epoch();
    *slot = Some(fresh.clone());
    publish(state, |status| {
        status.state = ConnectionState::Connected;
        // A live connection invalidates any prior connect-failure code.
        status.error_code = None;
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
    Some(epoch)
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
    // The shared loss cleanup (F-2): the ONE path for every observed death —
    // the pre-entry value check and the changed arm both flow through it, so
    // the loss's full cleanup (epoch bump + token invalidation under the
    // registry gate, restore cancel/join, the prior client's shutdown+join,
    // the slot clear) runs exactly once per loss, no bypass anywhere.
    //
    // The pre-service death check (item 6): a watch already true at entry
    // never fires `changed()` — the value is read directly and the shared
    // cleanup runs at once.
    if *dead.borrow() {
        run_loss_cleanup(state, restore_handles).await;
        return ServiceEnd::Loss(announced.take().unwrap_or(ReconnectKind::Lost));
    }
    loop {
        tokio::select! {
            _ = cancel.changed() => return ServiceEnd::Stop,
            () = state.events.closed() => return ServiceEnd::Stop,
            _ = dead.changed() => {
                if *dead.borrow() {
                    run_loss_cleanup(state, restore_handles).await;
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
                // Finished restore handles are reaped on EVERY outcome: the
                // outcome channel never closes (the driver holds a sender),
                // so completed handles must not accumulate across
                // reconnects.
                restore_handles.retain(|handle| !handle.is_finished());
                // Every outcome carries its spawn epoch: delivery is
                // validated under the registry gate — a Done or Failed
                // queued before a loss/stop boundary can never surface as
                // a stale Reattached/ReattachFailed afterwards (item 9).
                deliver_outcome(state, outcome);
            }
        }
    }
}

/// Deliver one restore outcome: the epoch check and the emission run
/// under ONE registry guard held across the synchronous send (F-3 — a
/// match-guard temporary drops at the guard expression, never covering the
/// send). A stale-epoch outcome is suppressed. Crate-private so the proof
/// suite drives the REAL delivery path against queued outcomes.
pub(crate) fn deliver_outcome(state: &Arc<SessionState>, outcome: Option<RestoreOutcome>) {
    match outcome {
        Some(RestoreOutcome::Done { id, data, epoch }) => {
            let registry = state.registry.lock().unwrap();
            if registry.current_epoch() == epoch {
                let _ = state.events.send(ConnectionEvent::Reattached {
                    active_session_id: id,
                    data,
                });
            }
        }
        Some(RestoreOutcome::Failed { id, error, epoch }) => {
            let registry = state.registry.lock().unwrap();
            if registry.current_epoch() == epoch {
                let _ = state.events.send(ConnectionEvent::ReattachFailed {
                    active_session_id: id,
                    error,
                });
            }
        }
        _ => {}
    }
}

/// The ONE shared loss cleanup (F-2): the epoch and EVERY in-flight attempt
/// token are invalidated under the registry lock first; the loss's restore
/// tasks are cancelled and joined; the prior connection's registered tasks
/// are shut down and joined; the slot is cleared — nothing of the lost
/// connection survives into the next swap.
async fn run_loss_cleanup(
    state: &Arc<SessionState>,
    restore_handles: &mut Vec<tokio::task::JoinHandle<()>>,
) {
    {
        let mut registry = state.registry.lock().unwrap();
        registry.bump_epoch();
        registry.invalidate_all();
    }
    let cancelled = std::mem::take(restore_handles);
    for handle in &cancelled {
        handle.abort();
    }
    for handle in cancelled {
        let _ = handle.await;
    }
    if let Some(client) = state.client.write().await.take() {
        client.shutdown();
        client.join_tasks().await;
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
        let token = state.registry.lock().unwrap().attempt_token(&id);
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
                    guard
                        .clone()
                        .ok_or_else(|| anyhow!("the session is not connected"))?
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
            match attempt {
                Ok(data) => {
                    // The epoch comparison AND the adoption run in ONE
                    // registry critical section (item 8) — a stop's bump can
                    // never land between them.
                    let adopted = state.registry.lock().unwrap().adopt_at_epoch(
                        &id,
                        &capabilities,
                        &data,
                        token,
                        epoch,
                    );
                    if adopted {
                        let _ = restore_tx.send(RestoreOutcome::Done { id, data, epoch });
                    } else {
                        let _ = restore_tx.send(RestoreOutcome::Abandoned);
                    }
                }
                Err(error) => {
                    let _ = restore_tx.send(RestoreOutcome::Failed {
                        id,
                        error: error.to_string(),
                        epoch,
                    });
                }
            }
        }));
    }
}
#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::AtomicU64;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
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
        client_ids: Arc<Mutex<Vec<String>>>,
        connected: Arc<AtomicU64>,
        eof: Arc<AtomicU64>,
    }

    impl Supervisor {
        #[allow(clippy::too_many_arguments)]
        fn spawn(
            dir: &tempfile::TempDir,
            frames: Vec<Value>,
            hold_attach: bool,
            drop_first_after_release: bool,
        ) -> (
            Self,
            std::path::PathBuf,
            std::sync::Arc<tokio::sync::Notify>,
        ) {
            let socket = dir.path().join("d.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let commands = Arc::new(Mutex::new(Vec::new()));
            let client_ids = Arc::new(Mutex::new(Vec::new()));
            let connected = Arc::new(AtomicU64::new(0));
            let eof = Arc::new(AtomicU64::new(0));
            let release = std::sync::Arc::new(tokio::sync::Notify::new());
            tokio::spawn(supervisor_serve(
                listener,
                Arc::clone(&commands),
                Arc::clone(&client_ids),
                Arc::clone(&connected),
                Arc::clone(&eof),
                frames,
                hold_attach,
                drop_first_after_release,
                Arc::clone(&release),
            ));
            (
                Supervisor {
                    commands,
                    client_ids,
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
        client_ids: Arc<Mutex<Vec<String>>>,
        connected: Arc<AtomicU64>,
        eof: Arc<AtomicU64>,
        frames: Vec<Value>,
        hold_attach: bool,
        drop_first_after_release: bool,
        release: std::sync::Arc<tokio::sync::Notify>,
    ) {
        let mut connections = 0u64;
        loop {
            let (stream, _) = listener.accept().await.expect("supervisor accept");
            connections += 1;
            connected.store(connections, Ordering::SeqCst);
            let connection = connections;
            let commands = Arc::clone(&commands);
            let client_ids = Arc::clone(&client_ids);
            let eof = Arc::clone(&eof);
            let frames = frames.clone();
            let release = Arc::clone(&release);
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
                    if let Some(client_id) = envelope["clientId"].as_str() {
                        client_ids.lock().unwrap().push(client_id.to_string());
                    }
                    let is_attach = command["type"] == "attach";
                    if hold_attach && is_attach {
                        // The held restore: everything else flows first.
                        release.notified().await;
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
                        release.notified().await;
                        break;
                    }
                }
            });
        }
    }

    async fn next_event(events: &mut mpsc::UnboundedReceiver<ConnectionEvent>) -> ConnectionEvent {
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
        let _ = session.stop().await;
        assert!(supervisor.connected.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn reconnect_restores_with_cursor_and_never_resends() {
        let dir = tempfile::TempDir::new().unwrap();
        let (supervisor, socket, release) = Supervisor::spawn(&dir, Vec::new(), false, true);
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
        // The recorded commands, inspected under a scoped lock (never held
        // across the stop's await below).
        let second: Vec<Value> = {
            let commands = supervisor.commands.lock().unwrap();
            commands
                .iter()
                .filter(|(connection, _)| *connection == 2)
                .map(|(_, command)| command.clone())
                .collect()
        };
        assert_eq!(second.len(), 1, "only the restore attach was sent");
        assert_eq!(second[0]["type"], "attach");
        assert_eq!(
            second[0]["resumeCursor"],
            json!({ "activeSessionId": "s1", "generation": "g", "sequence": 4 }),
            "the adopted cursor rode the restore"
        );
        let _ = session.stop().await;
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn frames_flow_while_a_restore_is_held_and_requests_never_wait() {
        // Two scripted connections. The first adopts an attach cursor (g/4)
        // and is dropped on signal; the second holds the RESTORE attach while
        // streaming an event — the frame must flow, and a user request must
        // complete, all while the restore is held; the release completes it.
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("d.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let drop_first = std::sync::Arc::new(tokio::sync::Notify::new());
        let release_second = std::sync::Arc::new(tokio::sync::Notify::new());
        let event = json!({
            "type": "session_event",
            "activeSessionId": "s1",
            "event": { "type": "delta" },
            "meta": { "cursor": { "generation": "g", "sequence": 5 } },
        });
        {
            let drop_first = Arc::clone(&drop_first);
            let release_second = Arc::clone(&release_second);
            let event = event.clone();
            tokio::spawn(async move {
                // Connection 1: adopt, then drop on signal — the halves are SCOPED so
                // the drop is the EOF the driver observes.
                {
                    let (stream, _) = listener.accept().await.expect("accept one");
                    let (reader, mut writer) = stream.into_split();
                    let mut reader = tokio::io::BufReader::new(reader);
                    let mut line = String::new();
                    let hello = json!({
                        "type": "daemon_hello",
                        "protocol": { "name": "prime-agent.daemon", "version": 7 },
                        "serverCapabilities": [],
                    });
                    writer
                        .write_all(format!("{hello}\n").as_bytes())
                        .await
                        .unwrap();
                    loop {
                        line.clear();
                        // The scripted death of the first connection.
                        tokio::select! {
                            read = reader.read_line(&mut line) => {
                                if read.unwrap_or(0) == 0 {
                                    break;
                                }
                            }
                            () = drop_first.notified() => break,
                        }
                        let envelope: Value = serde_json::from_str(line.trim()).unwrap();
                        let command = envelope["command"]["type"].as_str().unwrap_or_default();
                        let data = if command == "attach" {
                            json!({ "lastEventCursor": { "generation": "g", "sequence": 4 } })
                        } else {
                            json!({ "ok": true })
                        };
                        let response = json!({
                            "type": "response",
                            "id": envelope["id"],
                            "command": command,
                            "success": true,
                            "data": data,
                        });
                        writer
                            .write_all(format!("{response}\n").as_bytes())
                            .await
                            .unwrap();
                    }
                }
                // Connection 2: the held restore and the streamed frame.
                // The attach response is HELD (a pending slot + the release
                // signal) while every other command flows immediately.
                let (stream, _) = listener.accept().await.expect("accept two");
                let (reader, mut writer) = stream.into_split();
                let mut reader = tokio::io::BufReader::new(reader);
                let mut line = String::new();
                let hello = json!({
                    "type": "daemon_hello",
                    "protocol": { "name": "prime-agent.daemon", "version": 7 },
                    "serverCapabilities": [],
                });
                writer
                    .write_all(format!("{hello}\n").as_bytes())
                    .await
                    .unwrap();
                writer
                    .write_all(format!("{event}\n").as_bytes())
                    .await
                    .unwrap();
                let mut pending_attach: Option<Value> = None;
                loop {
                    line.clear();
                    tokio::select! {
                        read = reader.read_line(&mut line) => {
                            if read.unwrap_or(0) == 0 {
                                break;
                            }
                            let envelope: Value =
                                serde_json::from_str(line.trim()).unwrap();
                            let command =
                                envelope["command"]["type"].as_str().unwrap_or_default();
                            if command == "attach" {
                                pending_attach = Some(envelope);
                            } else {
                                let response = json!({
                                    "type": "response",
                                    "id": envelope["id"],
                                    "command": command,
                                    "success": true,
                                    "data": { "ok": true },
                                });
                                writer
                                    .write_all(format!("{response}\n").as_bytes())
                                    .await
                                    .unwrap();
                            }
                        }
                        () = release_second.notified() => {
                            if let Some(envelope) = pending_attach.take() {
                                let response = json!({
                                    "type": "response",
                                    "id": envelope["id"],
                                    "command": "attach",
                                    "success": true,
                                    "data": {
                                        "lastEventCursor": { "generation": "g", "sequence": 6 }
                                    },
                                });
                                writer
                                    .write_all(format!("{response}\n").as_bytes())
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                }
            });
        }
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        wait_connected(&mut events).await;
        // The first connection adopts the cursor g/4 through the request
        // path (registration + attach).
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
        drop_first.notify_one();
        // The reconnect's restore is HELD; the frame streams meanwhile.
        let mut saw_begin = false;
        let saw_frame = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::ReattachBegin { .. } => {
                        saw_begin = true;
                    }
                    ConnectionEvent::Frame(frame) if frame == event => {
                        assert!(saw_begin, "the begin precedes the streamed frame");
                        return;
                    }
                    _ => {}
                }
            }
        })
        .await;
        assert!(
            saw_frame.is_ok(),
            "the frame flowed while the restore was held"
        );
        // A user request completes WITHOUT waiting for the held restore.
        let list = {
            let session = session.clone();
            tokio::spawn(async move {
                session
                    .request_ok(DaemonCommand::List {
                        id: None,
                        all: None,
                        cwd: None,
                        session_dir: None,
                        include_client_owned: None,
                        rest: Map::new(),
                    })
                    .await
            })
        };
        let list = tokio::time::timeout(Duration::from_secs(10), list)
            .await
            .expect("the concurrent request settled")
            .unwrap()
            .unwrap();
        assert_eq!(list["ok"], true);
        // The release completes the restore.
        release_second.notify_one();
        let reattached = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let ConnectionEvent::Reattached { data, .. } = next_event(&mut events).await {
                    assert_eq!(data["lastEventCursor"]["sequence"], 6);
                    return;
                }
            }
        })
        .await;
        assert!(reattached.is_ok(), "the restore completed after release");
        let _ = session.stop().await;
    }

    #[tokio::test]
    async fn stop_settles_the_connection_and_concurrent_stops_all_wait() {
        let dir = tempfile::TempDir::new().unwrap();
        let (supervisor, socket, _release) = Supervisor::spawn(&dir, Vec::new(), false, false);
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
        assert!(
            crate::daemon_client::is_command_not_sent(&error),
            "unexpected: {error}"
        );
    }

    #[tokio::test]
    async fn a_cancelled_stop_future_still_settles_the_driver() {
        // Item 12's corrected shape: a driver whose settle CANNOT complete
        // yet (a probe blocked on a release signal — the stop future is
        // provably pending), the stop is dropped MID-WAIT, the probe then
        // ends WITHOUT the mark (the died-driver shape), and the fresh stop
        // asserts the INNER honest Err — never an outer-timeout fiction, and
        // the memoized outcome is identical for every caller.
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let (session, _events) =
            DaemonClientSession::start(config(std::path::Path::new("/dev/null")))
                .await
                .unwrap();
        {
            let release = std::sync::Arc::clone(&release);
            let mut driver = session.state.driver.lock().await;
            if let Some(handle) = driver.handle.take() {
                handle.abort();
            }
            // A probe driver that blocks until released, then ends WITHOUT
            // the settle and WITHOUT the mark.
            driver.handle = Some(tokio::spawn(async move {
                release.notified().await;
            }));
        }
        // The stop is genuinely pending: the probe holds both the mark and
        // the handle, so the bounded timeout proves it never completed.
        let mut stop = Box::pin(session.stop());
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut stop)
                .await
                .is_err(),
            "the stop is genuinely pending on the blocked driver"
        );
        drop(stop);
        // The probe ends (no mark, no settle): the fresh stop must report
        // the INNER honest Err, and the memoized outcome is identical for
        // every later caller.
        release.notify_one();
        let first = tokio::time::timeout(Duration::from_secs(5), session.stop())
            .await
            .expect("the fresh stop settles")
            .unwrap_err();
        let second = tokio::time::timeout(Duration::from_secs(5), session.stop())
            .await
            .expect("a later stop settles")
            .unwrap_err();
        assert_eq!(
            first.to_string(),
            second.to_string(),
            "every stop reads the identical honest memo"
        );
        assert!(
            first.to_string().contains("without completing its settle"),
            "unexpected: {first}"
        );
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
        let reader_gone = Arc::new(AtomicBool::new(false));
        {
            let accepted = Arc::clone(&accepted);
            let eof = Arc::clone(&eof);
            let reader_gone = Arc::clone(&reader_gone);
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                accepted.store(true, Ordering::SeqCst);
                let (mut reader, mut writer) = stream.into_split();
                let mut chunk = [0u8; 64];
                match reader.read(&mut chunk).await {
                    Ok(0) | Err(_) => {
                        eof.store(true, Ordering::SeqCst);
                    }
                    Ok(_) => {}
                }
                // The write half closed above; the read half must close
                // too. On the no-task path this is SOCKET-OWNERSHIP DROP,
                // zero tasks by construction (no transport task exists
                // during a hung hello — no reader-task Drop claim); the
                // write errors only when the future that owned both halves
                // was cancelled and dropped them.
                if writer.write_all(b"x").await.is_err() {
                    reader_gone.store(true, Ordering::SeqCst);
                }
            });
        }
        let (session, events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !accepted.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the hung connection was accepted");
        // Drop the consumer: the driver must tear the hung connection down
        // — BOTH halves observed BEFORE the stop/teardown completes (the
        // proof gates the settle, not an after-eventual).
        drop(events);
        tokio::time::timeout(Duration::from_secs(10), async {
            while !(eof.load(Ordering::SeqCst) && reader_gone.load(Ordering::SeqCst)) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the hung connection's BOTH halves ended when the consumer dropped");
        session.stop().await.expect("the settle completed honestly");
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
                if let ConnectionEvent::Status(status) = next_event(&mut events).await {
                    if status.state == ConnectionState::Disconnected {
                        attempts += 1;
                    }
                    if status.state == ConnectionState::Expired {
                        return;
                    }
                }
            }
        })
        .await;
        assert!(expired.is_ok(), "the bounded window expired");
        assert!(
            attempts >= 2,
            "retries actually ran before expiry (attempts: {attempts})"
        );
        let _ = session.stop().await;
    }

    #[tokio::test]
    async fn a_driver_ending_without_the_mark_fails_every_stop_honestly() {
        // P-2: a REAL-OWNING driver failure. The REAL driver's handle stays
        // IN the state; the driver is aborted through it (no take, no
        // replacement) — the stop monitor observes the actual JoinError
        // (the honest cancellation-completion outcome), and every stop
        // returns the identical honest Err. The real handle's end is really
        // observed, never a swapped mock.
        let (session, _events) =
            DaemonClientSession::start(config(std::path::Path::new("/dev/null")))
                .await
                .unwrap();
        // Abort the REAL driver THROUGH its retained handle — the state
        // keeps ownership; the monitor will observe the genuine end.
        {
            let driver = session.state.driver.lock().await;
            if let Some(handle) = driver.handle.as_ref() {
                handle.abort();
            }
        }
        let first = tokio::time::timeout(Duration::from_secs(5), session.stop())
            .await
            .expect("the first stop settles")
            .unwrap_err();
        let message = first.to_string();
        let second = tokio::time::timeout(Duration::from_secs(5), session.stop())
            .await
            .expect("a later stop settles")
            .unwrap_err();
        assert_eq!(
            message,
            second.to_string(),
            "every stop reads the identical honest memo"
        );
        assert!(
            message.contains("panicked") || message.contains("cancelled"),
            "the honest JoinError outcome, never a faked clean completion: {message}"
        );
    }

    #[tokio::test]
    async fn a_loss_before_service_entry_is_detected_at_entry() {
        // Item 6: the daemon dies between the hello and the service entry —
        // the dead watch is ALREADY true at entry, so `changed()` never
        // fires; the entry VALUE check takes the loss at once (status
        // Disconnected; the reconnect policy arms), no tap-close spin.
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("d.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        {
            let dropped = Arc::clone(&dropped);
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                let mut writer = stream;
                let hello = json!({
                    "type": "daemon_hello",
                    "protocol": { "name": "prime-agent.daemon", "version": 7 },
                });
                writer
                    .write_all(format!("{hello}\n").as_bytes())
                    .await
                    .unwrap();
                drop(writer);
                dropped.store(true, Ordering::SeqCst);
            });
        }
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        wait_connected(&mut events).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the daemon closed right after the hello");
        // The entry check must take the loss: a Disconnected (or Expired)
        // status arrives — the driver does NOT spin forever.
        let took = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::Status(status)
                        if status.state == ConnectionState::Disconnected =>
                    {
                        return;
                    }
                    _ => {}
                }
            }
        })
        .await;
        assert!(took.is_ok(), "the pre-entry loss was taken at entry");
        session.stop().await.expect("the settle completed honestly");
    }

    #[tokio::test]
    async fn a_queued_old_epoch_outcome_is_suppressed_current_delivers() {
        // P-1 (channel-level): ACTUAL old-epoch Done AND Failed outcomes
        // queued into the REAL delivery path BEFORE the epoch boundary;
        // after the bump neither surfaces, while a current-epoch outcome
        // still delivers (the positive case). No network request path.
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("p1.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut writer = stream;
            let hello = json!({
                "type": "daemon_hello",
                "protocol": { "name": "prime-agent.daemon", "version": 7 },
            });
            writer
                .write_all(format!("{hello}\n").as_bytes())
                .await
                .unwrap();
            std::future::pending::<()>().await;
        });
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        wait_connected(&mut events).await;
        // The OLD epoch captured; the boundary moves BEFORE the outcomes
        // are processed (queued pre-loss, delivered post-boundary).
        let old_epoch = session.state.registry.lock().unwrap().current_epoch();
        session.state.registry.lock().unwrap().bump_epoch();
        deliver_outcome(
            &session.state,
            Some(RestoreOutcome::Done {
                id: "s1".to_string(),
                data: json!({ "source": "old" }),
                epoch: old_epoch,
            }),
        );
        deliver_outcome(
            &session.state,
            Some(RestoreOutcome::Failed {
                id: "s1".to_string(),
                error: "the old connection's failure".to_string(),
                epoch: old_epoch,
            }),
        );
        // A CURRENT-epoch outcome delivers (the positive case).
        let current_epoch = session.state.registry.lock().unwrap().current_epoch();
        deliver_outcome(
            &session.state,
            Some(RestoreOutcome::Done {
                id: "s2".to_string(),
                data: json!({ "source": "current" }),
                epoch: current_epoch,
            }),
        );
        // Exactly one event — the current restore; the old pair never
        // surfaces.
        let delivered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::Reattached { data, .. } => {
                        assert_eq!(data["source"], "current");
                        return;
                    }
                    ConnectionEvent::ReattachFailed { error, .. } => {
                        panic!("the stale failure surfaced: {error}");
                    }
                    _ => {}
                }
            }
        })
        .await;
        assert!(
            delivered.is_ok(),
            "only the current-epoch outcome delivered"
        );
        // Drain briefly: no second event may arrive (the suppressed pair
        // must stay silent, not merely ordered later).
        if let Ok(event) = tokio::time::timeout(Duration::from_millis(300), events.recv()).await {
            panic!("a stale outcome surfaced after the current one: {event:?}");
        }
        session.stop().await.expect("the settle completed honestly");
    }

    #[tokio::test]
    async fn a_refused_detach_keeps_the_attachment_restorable() {
        // B-2: a REFUSED detach response leaves the attachment in place —
        // still restorable, its cursor retained (the success-only forget).
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("b2.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let refused = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let refused = std::sync::Arc::clone(&refused);
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept");
                let (reader, mut writer) = stream.into_split();
                let mut reader = tokio::io::BufReader::new(reader);
                let mut line = String::new();
                let hello = json!({
                    "type": "daemon_hello",
                    "protocol": { "name": "prime-agent.daemon", "version": 7 },
                });
                writer
                    .write_all(format!("{hello}\n").as_bytes())
                    .await
                    .unwrap();
                loop {
                    line.clear();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        break;
                    }
                    let envelope: Value = serde_json::from_str(line.trim()).unwrap();
                    let command = envelope["command"]["type"].as_str().unwrap_or_default();
                    let (success, data) = if command == "attach" {
                        (
                            true,
                            json!({ "lastEventCursor": { "generation": "g", "sequence": 9 } }),
                        )
                    } else if command == "detach" {
                        refused.store(true, Ordering::SeqCst);
                        (false, Value::Null)
                    } else {
                        (true, json!({ "ok": true }))
                    };
                    let response = json!({
                        "type": "response",
                        "id": envelope["id"],
                        "command": command,
                        "success": success,
                        "data": data,
                        "error": if success { Value::Null } else { json!("session busy") },
                    });
                    writer
                        .write_all(format!("{response}\n").as_bytes())
                        .await
                        .unwrap();
                }
            });
        }
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        wait_connected(&mut events).await;
        // Attach (adopts the cursor), then a REFUSED detach.
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
        let detach = session.request(DaemonCommand::Detach {
            id: None,
            active_session_id: Some("s1".to_string()),
            rest: Map::new(),
        });
        let response = tokio::time::timeout(Duration::from_secs(10), detach)
            .await
            .expect("the detach settled");
        assert!(response.is_ok());
        assert!(refused.load(Ordering::SeqCst), "the refusal was delivered");
        // The attachment SURVIVES: still registered with its cursor.
        let entries = session.state.registry.lock().unwrap().restore_entries();
        assert_eq!(entries.len(), 1, "the refused detach kept the attachment");
        assert_eq!(
            entries[0].1.cursor.as_ref().unwrap().sequence,
            Some(9),
            "the cursor is retained"
        );
        session.stop().await.expect("the settle completed honestly");
    }

    #[tokio::test]
    async fn the_old_capture_after_a_boundary_never_adopts() {
        // Item-10, the direct bookkeeping unit: the OLD capture (through
        // the real before() path) invoked AFTER the registry bump — the
        // stale response is REJECTED; the CURRENT capture's after() with
        // the NEW response ADOPTS. No network pretend: this drives the
        // exact adoption boundary the wire cannot deterministically
        // deliver (the epoch tie is the gate; no client identity is
        // involved).
        let (session, _events) =
            DaemonClientSession::start(config(std::path::Path::new("/dev/null")))
                .await
                .unwrap();
        // The OLD capture through the REAL before() path (the registry-side
        // mint under the lock, exactly as a request takes it).
        session
            .state
            .registry
            .lock()
            .unwrap()
            .register("s1", vec![]);
        let attach = DaemonCommand::Attach {
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
        };
        let old_bookkeeping = session.before(&attach);
        // The old response (what the OLD connection would have answered).
        let old_response = DaemonResponse {
            id: None,
            command: "attach".to_string(),
            success: true,
            data: Some(json!({
                "lastEventCursor": { "generation": "gold", "sequence": 1 }
            })),
            error: None,
            error_info: None,
        };
        // THE BOUNDARY: the registry bump (the same bump the
        // loss/stop/install paths perform — the epoch tie is the gate).
        session.state.registry.lock().unwrap().bump_epoch();
        // The old capture's after() — REJECTED: no adoption of "gold".
        session.after(old_bookkeeping.as_ref(), &old_response);
        {
            let entries = session.state.registry.lock().unwrap().restore_entries();
            assert!(
                entries.iter().all(|(_, entry)| entry.cursor.is_none()),
                "the stale old capture never adopts"
            );
        }
        // The CURRENT capture through the SAME before() path, with the NEW
        // response — ADOPTS "gnew" coherently.
        let new_bookkeeping = session.before(&attach);
        let new_response = DaemonResponse {
            id: None,
            command: "attach".to_string(),
            success: true,
            data: Some(json!({
                "lastEventCursor": { "generation": "gnew", "sequence": 2 }
            })),
            error: None,
            error_info: None,
        };
        session.after(new_bookkeeping.as_ref(), &new_response);
        {
            let entries = session.state.registry.lock().unwrap().restore_entries();
            let cursor = entries[0]
                .1
                .cursor
                .as_ref()
                .expect("the current capture adopts");
            assert_eq!(cursor.generation.as_deref(), Some("gnew"));
            assert_eq!(cursor.sequence, Some(2));
        }
    }

    #[tokio::test]
    async fn the_install_refuses_a_stop_marked_while_the_client_guard_holds() {
        // P-4, the REAL contention — no scheduler assumptions: a two-accept
        // mock (the driver's own connection AND a fresh client with a
        // completed handshake, both streams held open), the test HOLDS the
        // client WRITE guard, the install helper's future is observed
        // PENDING under a real-waker bounded timeout (its receipt —
        // genuinely parked at the client-guard await), then session.stop()
        // is pinned DIRECTLY and observed PENDING under the held guard (the
        // bounded timeout polls its synchronous prefix — the marks LAND:
        // stopping is asserted true BEFORE the release), and after the
        // release the pinned stop and the install are joined together.
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("p4.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        tokio::spawn(async move {
            // Both greeted connections STAY OPEN (their streams held for
            // the task's life) — a greeting that instantly drops its
            // stream would fake a loss.
            let mut held = Vec::new();
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.expect("accept");
                let mut writer = stream;
                let hello = json!({
                    "type": "daemon_hello",
                    "protocol": { "name": "prime-agent.daemon", "version": 7 },
                });
                writer
                    .write_all(format!("{hello}\n").as_bytes())
                    .await
                    .unwrap();
                held.push(writer);
            }
            std::future::pending::<()>().await;
        });
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        // Consume the initial Connected (the driver's own install).
        wait_connected(&mut events).await;
        // A REAL fresh client with a completed handshake (its own accept).
        let (fresh, _fresh_events) = DaemonClient::connect(&socket).await.unwrap();
        // The test holds the client WRITE guard.
        let guard = session.state.client.write().await;
        // The install helper's future — parked at the client-guard await.
        let cancel = session.state.cancel.subscribe();
        let mut install = Box::pin(install_fresh(&session.state, &fresh, &cancel));
        // The PENDING receipt: a bounded timeout on the future (the runtime
        // drives it with a real waker; no sleep, no noop-waker loss).
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut install)
                .await
                .is_err(),
            "the install is genuinely parked at the client-guard await"
        );
        // session.stop() pinned DIRECTLY: the bounded timeout polls its
        // synchronous prefix — the marks LAND (stopping asserted true
        // BEFORE the release, observed not assumed).
        let mut stop = Box::pin(session.stop());
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut stop)
                .await
                .is_err(),
            "the pinned stop is genuinely pending under the held guard"
        );
        assert!(
            session.state.stopping.load(Ordering::SeqCst),
            "the stop's marks landed under the registry gate before the release"
        );
        // Release: the pinned stop and the install are joined together
        // (tokio's RwLock hands the lock to the first queued waiter — both
        // parked parties keep being polled, no orphaned waiter).
        drop(guard);
        let (stop_result, installed) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(stop, install)
        })
        .await
        .expect("the pinned stop and the install both settle");
        stop_result.expect("the stop completed honestly");
        assert!(
            installed.is_none(),
            "the install REFUSED on the stop mark — no epoch, no install"
        );
        // The refused fresh connection's settle: shutdown, then the JOIN
        // (the tasks' real end observed).
        fresh.shutdown();
        let joined = fresh.join_tasks().await;
        assert_eq!(joined, 2, "the fresh connection's tasks joined");
        // R-2: EVENT-sequence evidence — drain the remaining stream bounded
        // and assert NO Status(Connected) event appears after the stop's
        // boundary (the initial Connected was consumed by wait_connected).
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, events.recv()).await {
                Ok(Some(ConnectionEvent::Status(status))) => {
                    assert_ne!(
                        status.state,
                        ConnectionState::Connected,
                        "no Connected publish past the stop mark (event sequence)"
                    );
                }
                Ok(Some(_)) => {}
                // The channel closes or the bounded drain expires: the
                // sequence is proven.
                Ok(None) | Err(_) => break,
            }
        }
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
        assert!(
            crate::daemon_client::is_command_not_sent(&error),
            "unexpected: {error}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_join_keeps_the_shared_slot_owner() {
        // P-3 (the Drop-guard barrier, no time loop): a REAL task
        // registered on the live connection's TaskSet — it sets STARTED,
        // then spins on a shared AtomicBool released from the test
        // (non-yielding while held), so the join is GENUINELY blocked from
        // the moment STARTED is observed. The stop is proven Pending and
        // dropped mid-join (the caller-drop moment); the TaskSet owner
        // survives in the SLOT or the DRIVER (whichever holds it), the
        // task's Drop flag flips BEFORE the fresh stop returns, and the
        // fresh stop asserts the INNER Ok with the mock's EOF observed.
        let dir = tempfile::TempDir::new().unwrap();
        let (supervisor, socket, _release) = Supervisor::spawn(&dir, Vec::new(), false, false);
        let (session, mut events) = DaemonClientSession::start(config(&socket)).await.unwrap();
        wait_connected(&mut events).await;
        let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let client = session
                .state
                .client
                .read()
                .await
                .clone()
                .expect("the connection is live");
            let started = std::sync::Arc::clone(&started);
            let dropped = std::sync::Arc::clone(&dropped);
            let release = std::sync::Arc::clone(&release);
            let barrier = tokio::spawn(async move {
                struct DropFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);
                impl Drop for DropFlag {
                    fn drop(&mut self) {
                        self.0.store(true, Ordering::SeqCst);
                    }
                }
                let _dropper = DropFlag(dropped);
                started.store(true, Ordering::SeqCst);
                // Non-yielding spin until released (another worker flips
                // it): the join blocks deterministically.
                while !release.load(Ordering::SeqCst) {
                    std::hint::spin_loop();
                }
            });
            client.tasks.register_direct(vec![barrier]);
        }
        // STARTED observed BEFORE the stop is triggered.
        tokio::time::timeout(Duration::from_secs(5), async {
            while !started.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the barrier task started");
        // The stop is genuinely pending (the join blocks on the barrier)
        // and is dropped mid-flight — the caller-drop moment.
        let mut stop = Box::pin(session.stop());
        assert!(
            tokio::time::timeout(Duration::from_millis(300), &mut stop)
                .await
                .is_err(),
            "the stop is genuinely pending on the blocked join"
        );
        drop(stop);
        assert!(
            !dropped.load(Ordering::SeqCst),
            "the barrier is still held through the cancelled stop"
        );
        // Release the barrier (from this worker): the driver's join
        // completes, the settle flips the mark, and the fresh stop returns
        // the INNER Ok only after the REAL task's Drop is observed.
        release.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(30), session.stop())
            .await
            .expect("the fresh stop settles")
            .expect("the fresh stop completes the retained join honestly");
        assert!(
            dropped.load(Ordering::SeqCst),
            "the real task's Drop was observed before the fresh stop returned"
        );
        let _ = supervisor;
    }
    #[tokio::test]
    async fn a_missing_socket_path_surfaces_the_structural_not_found_code() {
        let dir = tempfile::TempDir::new().unwrap();
        let missing = dir.path().join("no-such-socket.sock");
        let (session, mut events) = DaemonClientSession::start(config(&missing))
            .await
            .unwrap();
        // The first connect failure publishes Disconnected WITH the
        // structural code; the unbounded desktop policy keeps retrying, so
        // assert on the first Disconnected status event and the sync
        // snapshot alike.
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::Status(status)
                        if status.state == ConnectionState::Disconnected =>
                    {
                        return status;
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("a disconnected status arrives");
        assert_eq!(status.error_code.as_deref(), Some("not_found"));
        assert_eq!(session.status().error_code.as_deref(), Some("not_found"));
        let _ = session.stop().await;
    }

    #[tokio::test]
    async fn a_dead_socket_surfaces_the_structural_refused_code() {
        let dir = tempfile::TempDir::new().unwrap();
        // A plain FILE at the socket path: the connect reaches the kernel
        // and is refused — the "socket file exists, nothing listens" shape.
        let dead = dir.path().join("dead.sock");
        std::fs::write(&dead, b"").unwrap();
        let (session, mut events) = DaemonClientSession::start(config(&dead))
            .await
            .unwrap();
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::Status(status)
                        if status.state == ConnectionState::Disconnected =>
                    {
                        return status;
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("a disconnected status arrives");
        assert_eq!(status.error_code.as_deref(), Some("connection_refused"));
        let _ = session.stop().await;
    }

    #[tokio::test]
    async fn a_protocol_mismatch_carries_no_error_code() {
        let dir = tempfile::TempDir::new().unwrap();
        let socket = dir.path().join("d.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        // A supervisor greeting with an unsupported protocol version: the
        // connect fails as a handshake refusal — an anyhow failure with NO
        // io source, so no code may authorize a spawn.
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut writer = stream;
            let hello = json!({
                "type": "daemon_hello",
                "protocol": { "name": "prime-agent.daemon", "version": 6 },
                "clientId": "srv",
                "serverCapabilities": [],
            });
            let mut line = serde_json::to_string(&hello).unwrap();
            line.push('\n');
            writer.write_all(line.as_bytes()).await.unwrap();
            std::future::pending::<()>().await;
        });
        let (session, mut events) = DaemonClientSession::start(config(&socket))
            .await
            .unwrap();
        let status = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                match next_event(&mut events).await {
                    ConnectionEvent::Status(status)
                        if status.state == ConnectionState::Disconnected =>
                    {
                        return status;
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("a disconnected status arrives");
        assert_eq!(status.error_code, None);
        let _ = session.stop().await;
    }

    #[tokio::test]
    async fn a_custom_client_id_rides_every_envelope_including_the_restore() {
        let dir = tempfile::TempDir::new().unwrap();
        let (supervisor, socket, release) =
            Supervisor::spawn(&dir, Vec::new(), false, true);
        let mut lifecycle = config(&socket);
        lifecycle.transport.client_id = Some("prime-desktop:7:uuid".to_string());
        let (session, mut events) = DaemonClientSession::start(lifecycle)
            .await
            .unwrap();
        wait_connected(&mut events).await;
        // Attach through the request path (the envelope adoption), then the
        // scripted death of the first connection: the driver reconnects and
        // RESTORES — the restore attach must carry the custom identity too.
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
        // The restore's attach envelope on the second connection.
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let commands = supervisor.commands.lock().unwrap();
                if commands
                    .iter()
                    .any(|(connection, command)| *connection == 2)
                {
                    return;
                }
                drop(commands);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the restore attach was sent");
        let ids = supervisor.client_ids.lock().unwrap().clone();
        assert!(
            ids.iter().all(|id| id == "prime-desktop:7:uuid"),
            "every envelope carries the custom identity: {ids:?}"
        );
        assert!(ids.len() >= 2, "at least the fresh attach and the restore");
        let _ = session.stop().await;
    }
}
