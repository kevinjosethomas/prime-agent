//! The managed daemon session binding: the shared Rust lifecycle
//! (`pa-client` `lifecycle.rs` + `registry.rs`) behind a thin napi surface,
//! so hosts delete their TypeScript reconnect/restore-attach/dedupe state
//! machines instead of re-implementing them.
//!
//! Ownership rules (verbatim from the crate docs, extended):
//!
//! - **All state machine stays Rust:** reconnect windows/backoff, connect,
//!   restore-attach, cursor adoption, replay dedupe, the attachment
//!   registry, epoch/token validation, and stop ordering live in
//!   `pa-client`. This wrapper owns only serialization across the napi
//!   boundary, the JS event queue, lifecycle scheduling of core calls,
//!   and GC-safe teardown. No timers, registry, or reconnect loop here.
//! - **`startSession` bridges `DaemonClientSession::start` onto the
//!   binding-owned runtime:** the driver task it spawns must never land on
//!   napi's private `tokio_rt`.
//! - **Event delivery is pull-based, single-consumer, and fail-closed:**
//!   ONE bounded queue (`eventBuffer`, default 1024 events — a COUNT cap;
//!   with the frame byte cap the worst-case queue memory is
//!   `eventBuffer * maxFrameBytes` because raw frames ride the queue). The
//!   pump draining the core's unbounded event channel is the single
//!   producer; a full queue tears the whole session down (never a silent
//!   drop) and every later `nextEvent` observes the sticky `Overflow`
//!   error, which precedes any buffered event. Events are never dropped,
//!   coalesced, or silently reordered.
//! - **End-of-stream contract:** an explicit `stop()` (or a GC drop) marks
//!   teardown, wakes every pending pull, and ends the stream —
//!   already-queued events drain first, then `nextEvent` resolves `null`
//!   (the sticky `Overflow` error wins over buffered events). `stop()`
//!   settles independently of the consumer draining.
//! - **Errors are structural and honest:** the raw binding's payload
//!   classes (`NotSent`/`Uncertain`/`Rejected`/`Overflow`/`Transport`)
//!   with the same worker mechanics — the pre-poll barrier settles a
//!   teardown observed before the core future's first poll as `NotSent`
//!   with the cause, and the BIASED select puts the core arm first so a
//!   settled response is never downgraded. The core's own refusals
//!   (`CommandNotSent` for stopping/not-connected) classify `NotSent`; a
//!   genuinely in-flight request torn down by the stop settles
//!   `Uncertain` with the cause.
//! - **`stop()` is the honest completion:** the core stop joins the
//!   driver, its restore tasks, and the connection; the binding then
//!   drains its own operation bridges. A driver failure surfaces as the
//!   core's honest `Err`, never a faked clean completion.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use pa_client::daemon_client::DaemonClientOptions;
use pa_client::lifecycle::{
    ConnectionEvent, ConnectionState, ConnectionStatus, DaemonClientSession, LifecycleConfig,
    ReconnectPolicy, ReconnectWindow,
};
use pa_types::daemon::DaemonCommand;
use serde_json::{json, Value};
use tokio::sync::{mpsc, watch, Notify};

use crate::errors::{
    classify, napi_error, not_sent, not_sent_cause, overflow, transport, uncertain, ErrorPayload,
};
use crate::runtime::bridge;

/// The default bounded event-queue capacity. Reported in the README.
pub(crate) const DEFAULT_EVENT_BUFFER: u32 = 1024;
/// The upper bound on a configured event-queue capacity.
pub(crate) const MAX_EVENT_BUFFER: u32 = 1 << 20;
/// The default inbound frame byte cap (the desktop transport cap).
pub(crate) const DEFAULT_MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;
/// The default pending-request cap (the desktop transport cap).
pub(crate) const DEFAULT_MAX_PENDING: u32 = 128;
/// The default restore-attach capabilities (the desktop default).
pub(crate) const DEFAULT_ATTACH_CAPABILITIES: [&str; 2] = ["attach_snapshot", "event_sequence"];

fn state_name(state: ConnectionState) -> &'static str {
    match state {
        ConnectionState::Connecting => "connecting",
        ConnectionState::Connected => "connected",
        ConnectionState::Disconnected => "disconnected",
        ConnectionState::Expired => "expired",
    }
}

/// One reconnect window of an explicit policy, as the core window.
impl From<ManagedReconnectWindow> for ReconnectWindow {
    fn from(window: ManagedReconnectWindow) -> Self {
        ReconnectWindow {
            window: window
                .window_ms
                .map(|ms| Duration::from_millis(u64::from(ms))),
            initial_delay: Duration::from_millis(u64::from(window.initial_delay_ms)),
            max_delay: Duration::from_millis(u64::from(window.max_delay_ms)),
        }
    }
}

/// The status snapshot crossing the boundary.
fn status_value(status: &ConnectionStatus) -> Value {
    // Absent optionals are OMITTED, not JSON null: the JS contract is
    // `appVersion?/error?/errorCode?: string` (undefined when absent).
    let mut value = json!({
        "state": state_name(status.state),
        "socketPath": status.socket_path.display().to_string(),
        "capabilities": status.capabilities,
    });
    let fields = value.as_object_mut().expect("status object");
    if let Some(app_version) = &status.app_version {
        fields.insert("appVersion".into(), json!(app_version));
    }
    if let Some(error) = &status.error {
        fields.insert("error".into(), json!(error));
    }
    if let Some(error_code) = &status.error_code {
        fields.insert("errorCode".into(), json!(error_code));
    }
    value
}

/// One core `ConnectionEvent` as the JS event object.
fn event_value(event: ConnectionEvent) -> Value {
    match event {
        ConnectionEvent::Status(status) => {
            json!({ "type": "status", "status": status_value(&status) })
        }
        ConnectionEvent::Frame(frame) => json!({ "type": "frame", "frame": frame }),
        ConnectionEvent::ReattachBegin { active_session_id } => {
            json!({ "type": "reattachBegin", "activeSessionId": active_session_id })
        }
        ConnectionEvent::Reattached {
            active_session_id,
            data,
        } => json!({ "type": "reattached", "activeSessionId": active_session_id, "data": data }),
        ConnectionEvent::ReattachFailed {
            active_session_id,
            error,
        } => {
            json!({ "type": "reattachFailed", "activeSessionId": active_session_id, "error": error })
        }
        ConnectionEvent::ReconnectExpired { kind } => json!({
            "type": "reconnectExpired",
            // The ACTUAL closing kind, even when a null slot's fallback to
            // the lost window expired.
            "kind": match kind {
                pa_client::lifecycle::ReconnectKind::Update => "update",
                pa_client::lifecycle::ReconnectKind::Shutdown => "shutdown",
                pa_client::lifecycle::ReconnectKind::Lost => "lost",
            },
        }),
    }
}

/// One reconnect window of an explicit policy.
#[napi(object)]
pub struct ManagedReconnectWindow {
    /// The bounded recovery window; omitted = unbounded. Zero is rejected.
    pub window_ms: Option<u32>,
    pub initial_delay_ms: u32,
    pub max_delay_ms: u32,
}

/// An explicit reconnect policy. A null/omitted `update`/`shutdown` slot
/// gives that loss kind no special window — the core falls back to the
/// `lost` policy (verbatim core semantics).
#[napi(object)]
pub struct ManagedReconnectPolicy {
    pub update: Option<ManagedReconnectWindow>,
    pub shutdown: Option<ManagedReconnectWindow>,
    pub lost: ManagedReconnectWindow,
}

/// Options for [`start_session`]. Every duration is explicit: a nonzero
/// `restoreAttemptTimeoutMs` is REQUIRED (no invented default — the
/// desktop's own attach rides its 30 s request class), and the reconnect
/// policy is either omitted (the existing Rust desktop policy) or fully
/// explicit.
#[napi(object)]
pub struct ManagedSessionOptions {
    pub socket_path: String,
    /// The bounded event-queue capacity (default 1024, must be 1..=1048576).
    pub event_buffer: Option<u32>,
    /// Omitted = the Rust desktop reconnect policy (250 ms doubling to a
    /// 10 s cap, unbounded, every loss kind).
    pub reconnect: Option<ManagedReconnectPolicy>,
    /// The restore-attach capability default (the desktop default).
    pub attach_capabilities: Option<Vec<String>>,
    /// Registry dedupe of replayed session events (the desktop default on).
    pub dedupe_session_events: Option<bool>,
    /// One restore-attach attempt's budget. REQUIRED, nonzero.
    pub restore_attempt_timeout_ms: u32,
    /// The inbound frame byte cap (default 64 MiB, the desktop cap).
    pub max_frame_bytes: Option<u32>,
    /// The pending-request cap (default 128, the desktop cap).
    pub max_pending: Option<u32>,
    /// The client identity on every command envelope. Default = the core's
    /// historical `daemon-tui:{pid}`.
    pub client_id: Option<String>,
}

/// The effective configuration of one managed session.
#[napi(object)]
pub struct ManagedCaps {
    pub event_buffer: u32,
    pub max_frame_bytes: Option<u32>,
    pub max_pending: Option<u32>,
    pub restore_attempt_timeout_ms: u32,
    pub dedupe_session_events: bool,
    pub attach_capabilities: Vec<String>,
    pub client_id: Option<String>,
    /// Whether the Rust desktop reconnect policy applied (an explicit
    /// policy reports false).
    pub desktop_reconnect_policy: bool,
}

/// The binding-owned operation bridges (the raw binding's registry pattern:
/// registered under the gate so the teardown drain can never miss one, a
/// std Mutex for its brief synchronous sections only — never held across
/// an await).
type BridgeRegistry = std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>;

/// The drain guard: awaiting one bridge handle at a time, retaining
/// ownership on cancellation (an unfinished handle re-enters the registry
/// so a later drain resumes) and removing it only on completion.
struct BridgeGuard {
    bridges: Arc<BridgeRegistry>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl std::future::Future for BridgeGuard {
    type Output = std::result::Result<(), tokio::task::JoinError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // SAFETY: the guard holds no self-referential data (a plain Arc and
        // an Unpin JoinHandle), so &mut through the Pin is sound.
        let this = std::pin::Pin::into_inner(self);
        let Some(handle) = this.handle.as_mut() else {
            return std::task::Poll::Ready(Ok(()));
        };
        match std::pin::Pin::new(handle).poll(cx) {
            std::task::Poll::Ready(outcome) => {
                this.handle = None;
                std::task::Poll::Ready(outcome)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl Drop for BridgeGuard {
    fn drop(&mut self) {
        // A cancelled drain retains ownership: the unfinished handle goes
        // back into the registry for the next drain pass.
        if let Some(handle) = self.handle.take() {
            self.bridges
                .lock()
                .expect("the bridge registry is not poisoned")
                .push(handle);
        }
    }
}

/// The stop lifecycle: one runner performs the stop chain, concurrent
/// callers await the same real completion (the `stop_done` watch), late
/// callers return the settled outcome.
enum StopState {
    Open,
    Closing,
    Done,
}

pub(crate) struct ManagedInner {
    /// The clone-safe session handle; every request routes through the
    /// live connection, never behind the driver loop.
    session: DaemonClientSession,
    caps: ManagedCaps,
    /// The bounded event-queue sender. `None` after teardown; taking it is
    /// part of the teardown mark.
    event_tx: Mutex<Option<mpsc::Sender<Value>>>,
    /// The receiving end: the try-lock doubles as the single-consumer
    /// enforcement for `nextEvent`.
    event_rx: tokio::sync::Mutex<mpsc::Receiver<Value>>,
    /// Wakes the pump and pending event pulls on teardown.
    teardown: Notify,
    /// The binding-owned operation bridges, drained during the stop chain.
    bridges: Arc<BridgeRegistry>,
    /// Idempotent stop coordination across concurrent callers.
    stop_state: std::sync::Mutex<StopState>,
    /// Broadcasts the real stop completion to concurrent callers.
    stop_done: watch::Sender<bool>,
    /// The stop's honest outcome, memoized once by the runner: a driver
    /// failure is an `Err` every stop caller reads — never faked clean.
    stop_outcome: Mutex<Option<std::result::Result<(), String>>>,
    overflow: AtomicBool,
    closed: AtomicBool,
}

impl ManagedInner {
    /// The teardown cause for in-flight operations: `None` while the
    /// session is live, `Some("overflow")` / `Some("closed")` after the
    /// stop chain marked the session.
    fn teardown_cause(&self) -> Option<&'static str> {
        if !self.closed.load(Ordering::SeqCst) {
            return None;
        }
        if self.overflow.load(Ordering::SeqCst) {
            Some("overflow")
        } else {
            Some("closed")
        }
    }

    /// The provable non-delivery for a post-mark operation.
    fn closed_refusal(&self) -> ErrorPayload {
        let cause = self.teardown_cause();
        if cause == Some("overflow") {
            let mut payload = not_sent(
                "the session was torn down after the event queue overflowed; the command was never queued".to_string(),
            );
            payload.cause = Some("overflow");
            payload
        } else {
            let mut payload =
                not_sent("the session is stopping; the command was never queued".to_string());
            payload.cause = Some("closed");
            payload
        }
    }

    /// The atomic registration gate: refuses new operations once the stop
    /// chain marked the session (checked under the registry lock, so no
    /// late op can escape the drain), else spawns the worker onto the
    /// owned runtime and retains its handle in the registry.
    fn spawn_tracked<F>(&self, worker: F) -> std::result::Result<(), Box<ErrorPayload>>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut registry = self
            .bridges
            .lock()
            .expect("the bridge registry is not poisoned");
        // Reap finished handles at registration so a long-lived session
        // does not retain one dead entry per completed operation.
        registry.retain(|handle| !handle.is_finished());
        if self.closed.load(Ordering::SeqCst) {
            return Err(Box::new(self.closed_refusal()));
        }
        registry.push(bridge(worker));
        Ok(())
    }

    /// The synchronous teardown mark: flags, the queue-severing wake, and
    /// the event-queue sender drop — every pending pull and the pump
    /// observe the mark. The flag section runs under the registry lock so
    /// it linearizes with `spawn_tracked`.
    fn mark_closed(&self, is_overflow: bool) {
        {
            let _registry = self
                .bridges
                .lock()
                .expect("the bridge registry is not poisoned");
            if self.closed.swap(true, Ordering::SeqCst) {
                return;
            }
            if is_overflow {
                self.overflow.store(true, Ordering::SeqCst);
            }
        }
        *self
            .event_tx
            .lock()
            .expect("the event queue sender is not poisoned") = None;
        self.teardown.notify_waiters();
    }

    /// Start the dedicated stop runner at most once; concurrent or late
    /// callers observe the completion watch instead. The runner is NOT
    /// registered in the operation registry (it drains that registry), and
    /// no caller awaits the runner's handle — a cancelled JS stop promise
    /// leaves the runner alive to completion.
    pub(crate) fn start_stop(self: &Arc<Self>, is_overflow: bool) {
        let mut gate = self
            .stop_state
            .lock()
            .expect("the stop gate is not poisoned");
        match *gate {
            StopState::Done | StopState::Closing => {}
            StopState::Open => {
                self.mark_closed(is_overflow);
                *gate = StopState::Closing;
                let inner = Arc::clone(self);
                bridge(async move {
                    inner.stop_chain().await;
                });
            }
        }
    }

    /// The stop chain, running entirely on the owned runtime: the CORE
    /// stop first (refuse new requests, epoch bump, client shutdown, joins
    /// — its own ordering, untouched), then the binding-owned operation
    /// registry drained one handle at a time behind the re-insertion
    /// guard. The honest outcome is memoized before completion.
    async fn stop_chain(self: Arc<Self>) {
        let outcome = self.session.stop().await;
        loop {
            let handle = {
                self.bridges
                    .lock()
                    .expect("the bridge registry is not poisoned")
                    .pop()
            };
            let Some(handle) = handle else { break };
            let mut guard = BridgeGuard {
                bridges: Arc::clone(&self.bridges),
                handle: Some(handle),
            };
            let _ = (&mut guard).await;
            guard.handle = None;
        }
        *self
            .stop_outcome
            .lock()
            .expect("the stop outcome is not poisoned") =
            Some(outcome.map_err(|error| error.to_string()));
        *self
            .stop_state
            .lock()
            .expect("the stop gate is not poisoned") = StopState::Done;
        // send_replace retains the completion value even with zero live
        // receivers, so every later subscriber observes Done.
        self.stop_done.send_replace(true);
    }
}

#[napi]
/// The managed daemon session: reconnect/restore/dedupe owned by the
/// shared Rust lifecycle; JS pulls serialized events, sends requests, and
/// stops explicitly. Dropping the handle schedules the same stop chain
/// (async-safe; nothing runs from the GC thread).
pub struct ManagedDaemonSession {
    inner: Arc<ManagedInner>,
}

impl Drop for ManagedDaemonSession {
    fn drop(&mut self) {
        // The napi finalizer (GC) path: async-safe ONLY — schedule the full
        // stop chain on the binding-owned runtime (core stop + bridge
        // drain). No core calls from the GC thread; an already-stopped
        // session settles instantly.
        ManagedInner::start_stop(&self.inner, false);
    }
}

#[napi]
impl ManagedDaemonSession {
    #[napi]
    /// The latest status snapshot — synchronous, never blocks on I/O.
    pub fn status(&self) -> Value {
        status_value(&self.inner.session.status())
    }

    #[napi]
    /// The effective configuration this session was built with.
    pub fn caps(&self) -> ManagedCaps {
        ManagedCaps {
            event_buffer: self.inner.caps.event_buffer,
            max_frame_bytes: self.inner.caps.max_frame_bytes,
            max_pending: self.inner.caps.max_pending,
            restore_attempt_timeout_ms: self.inner.caps.restore_attempt_timeout_ms,
            dedupe_session_events: self.inner.caps.dedupe_session_events,
            attach_capabilities: self.inner.caps.attach_capabilities.clone(),
            client_id: self.inner.caps.client_id.clone(),
            desktop_reconnect_policy: self.inner.caps.desktop_reconnect_policy,
        }
    }

    #[napi]
    /// Register an attachment (idempotent per id) for restore-attach and
    /// dedupe.
    pub fn register_attachment(&self, id: String, capabilities: Option<Vec<String>>) {
        self.inner.session.register_attachment(&id, capabilities);
    }

    #[napi]
    /// Drop an attachment and every marker it owns.
    pub fn forget_attachment(&self, id: String) {
        self.inner.session.forget_attachment(&id);
    }

    #[napi]
    /// Pull the next serialized event. Single-consumer: a concurrent pull
    /// rejects. The sticky `Overflow` error (after an event-queue
    /// overflow) precedes any buffered event; an explicit stop drains
    /// already-queued events and then resolves `null`. Never drops or
    /// coalesces an event.
    pub async fn next_event(&self) -> Result<Option<Value>> {
        let mut receiver = self.inner.event_rx.try_lock().map_err(|_| {
            napi_error(transport(
                "an event pull is already pending: nextEvent is single-consumer".to_string(),
            ))
        })?;
        // The wakeup is BINDING-OWNED: register on the teardown notify
        // BEFORE checking the flags, then race the queue against the wake.
        let notified = self.inner.teardown.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        loop {
            // The sticky overflow error precedes ANY buffered event: the
            // consumer never continues on a partial stream.
            if self.inner.overflow.load(Ordering::SeqCst) {
                return Err(napi_error(overflow()));
            }
            if self.inner.closed.load(Ordering::SeqCst) {
                // Teardown marked: drain the already-queued events, then end
                // the stream. The pump dropped its sender at the mark, so
                // this settles without the consumer's help.
                return match receiver.recv().await {
                    Some(event) => Ok(Some(event)),
                    None => Ok(None),
                };
            }
            tokio::select! {
                event = receiver.recv() => match event {
                    Some(event) => {
                        if self.inner.overflow.load(Ordering::SeqCst) {
                            return Err(napi_error(overflow()));
                        }
                        return Ok(Some(event));
                    }
                    None => return Ok(None),
                },
                _ = &mut notified => {
                    // The wake: loop head re-checks the flags — no missed
                    // window between the check and the select.
                }
            }
        }
    }

    #[napi]
    /// One request over the live connection. Class-based timeouts apply
    /// (30 s default, 600 s for `prompt_and_wait`/`wait_for_idle`). The
    /// honest `NotSent`/`Uncertain`/`Rejected` classes preserved; no
    /// auto-resend, ever.
    pub async fn request(&self, command: Value) -> Result<Value> {
        let command: DaemonCommand = serde_json::from_value(command).map_err(|error| {
            napi_error(not_sent(format!("command deserialize failed: {error}")))
        })?;
        let (result_tx, result_rx) =
            tokio::sync::oneshot::channel::<std::result::Result<Value, Box<ErrorPayload>>>();
        let inner = Arc::clone(&self.inner);
        let session = self.inner.session.clone();
        // ONE tracked worker carries the operation; the napi promise rides a
        // oneshot so a cancelled JS promise never detaches the work — the
        // stop drain joins the real handle. The worker ALWAYS settles the
        // channel (barrier and both select arms send), so a missing send is
        // an invariant violation surfaced as Transport, never masked.
        self.inner
            .spawn_tracked(async move {
                let notified = inner.teardown.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let fut = session.request(command);
                tokio::pin!(fut);
                // Deterministic pre-poll barrier: teardown observed here is
                // BEFORE the request's first poll, so the line was provably
                // never queued — NotSent with the cause, never Uncertain.
                if let Some(cause) = inner.teardown_cause() {
                    let _ = result_tx.send(Err(Box::new(not_sent_cause(Some(cause)))));
                    return;
                }
                // Biased with the core arm FIRST: when a settled response and
                // the teardown wake are both ready, the core's own class
                // (Success/Rejected/transport) must win — never downgraded
                // to Uncertain by the race.
                let outcome = tokio::select! {
                    biased;
                    response = &mut fut => {
                        let response = response.map_err(|error| {
                            Box::new(classify(error, inner.teardown_cause()))
                        });
                        response.and_then(|response| {
                            serde_json::to_value(response).map_err(|error| {
                                Box::new(transport(format!("serialize response: {error}")))
                            })
                        })
                    }
                    _ = &mut notified => {
                        let cause = inner.teardown_cause().unwrap_or("closed");
                        Err(Box::new(uncertain(
                            "the session was torn down before the response arrived".to_string(),
                            Some(cause),
                        )))
                    }
                };
                let _ = result_tx.send(outcome);
            })
            .map_err(|payload| napi_error(*payload))?;
        result_rx
            .await
            .map_err(|_| {
                napi_error(transport(
                    "the request worker failed to settle its channel".to_string(),
                ))
            })?
            .map_err(|payload| napi_error(*payload))
    }

    #[napi]
    /// [`Self::request`] requiring `success: true`; the daemon refusal
    /// surfaces typed with `errorInfo` preserved.
    pub async fn request_ok(&self, command: Value) -> Result<Value> {
        let command: DaemonCommand = serde_json::from_value(command).map_err(|error| {
            napi_error(not_sent(format!("command deserialize failed: {error}")))
        })?;
        let (result_tx, result_rx) =
            tokio::sync::oneshot::channel::<std::result::Result<Value, Box<ErrorPayload>>>();
        let inner = Arc::clone(&self.inner);
        let session = self.inner.session.clone();
        self.inner
            .spawn_tracked(async move {
                let notified = inner.teardown.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let fut = session.request_ok(command);
                tokio::pin!(fut);
                // Deterministic pre-poll barrier (see `request`).
                if let Some(cause) = inner.teardown_cause() {
                    let _ = result_tx.send(Err(Box::new(not_sent_cause(Some(cause)))));
                    return;
                }
                // Biased with the core arm first (see `request`).
                let outcome = tokio::select! {
                    biased;
                    data = &mut fut => {
                        data.map_err(|error| Box::new(classify(error, inner.teardown_cause())))
                    }
                    _ = &mut notified => {
                        let cause = inner.teardown_cause().unwrap_or("closed");
                        Err(Box::new(uncertain(
                            "the session was torn down before the response arrived".to_string(),
                            Some(cause),
                        )))
                    }
                };
                let _ = result_tx.send(outcome);
            })
            .map_err(|payload| napi_error(*payload))?;
        result_rx
            .await
            .map_err(|_| {
                napi_error(transport(
                    "the request worker failed to settle its channel".to_string(),
                ))
            })?
            .map_err(|payload| napi_error(*payload))
    }

    #[napi]
    /// The full stop, as a promise that resolves on REAL completion: the
    /// core stop (refuse new requests, epoch bump, client shutdown and
    /// joins, the driver's final join) then the binding-owned operation
    /// drain. Idempotent: concurrent callers await the same completion
    /// watch and the same memoized honest outcome — a driver failure is
    /// an `Err` for every caller, never a faked clean completion.
    /// Cancelling this promise does NOT cancel the runner. The event
    /// stream ends at the mark: drain to your quiescent point BEFORE
    /// stopping.
    pub async fn stop(&self) -> Result<()> {
        ManagedInner::start_stop(&self.inner, false);
        let mut done = self.inner.stop_done.subscribe();
        if *done.borrow() {
            return settled_outcome(&self.inner);
        }
        if done.changed().await.is_err() {
            return settled_outcome(&self.inner);
        }
        settled_outcome(&self.inner)
    }
}

/// The memoized honest stop outcome: read after completion, never consumed.
fn settled_outcome(inner: &ManagedInner) -> Result<()> {
    let outcome = inner
        .stop_outcome
        .lock()
        .expect("the stop outcome is not poisoned")
        .clone();
    match outcome {
        Some(Err(message)) => Err(napi_error(transport(message))),
        _ => Ok(()),
    }
}

#[napi]
/// Start the managed daemon session: the shared Rust lifecycle owns
/// connect/reconnect/restore-attach/dedupe from the first moment; JS
/// pulls serialized events and sends requests. The driver runs on the
/// binding-owned runtime.
pub async fn start_session(options: ManagedSessionOptions) -> Result<ManagedDaemonSession> {
    let event_buffer = options.event_buffer.unwrap_or(DEFAULT_EVENT_BUFFER);
    if event_buffer == 0 || event_buffer > MAX_EVENT_BUFFER {
        return Err(napi_error(transport(format!(
            "eventBuffer must be between 1 and {MAX_EVENT_BUFFER}"
        ))));
    }
    if options.restore_attempt_timeout_ms == 0 {
        return Err(napi_error(transport(
            "restoreAttemptTimeoutMs is required and must be nonzero".to_string(),
        )));
    }
    if options.max_frame_bytes.is_some_and(|cap| cap == 0) {
        return Err(napi_error(transport(
            "maxFrameBytes must be at least 1".to_string(),
        )));
    }
    if options.max_pending.is_some_and(|cap| cap == 0) {
        return Err(napi_error(transport(
            "maxPending must be at least 1".to_string(),
        )));
    }
    let desktop_reconnect_policy = options.reconnect.is_none();
    let reconnect = match options.reconnect {
        None => ReconnectPolicy::desktop(),
        Some(explicit) => ReconnectPolicy {
            update: explicit.update.map(ReconnectWindow::from),
            shutdown: explicit.shutdown.map(ReconnectWindow::from),
            lost: ReconnectWindow::from(explicit.lost),
        },
    };
    let attach_capabilities = options.attach_capabilities.unwrap_or_else(|| {
        DEFAULT_ATTACH_CAPABILITIES
            .iter()
            .map(ToString::to_string)
            .collect()
    });
    let config = LifecycleConfig {
        socket_path: PathBuf::from(options.socket_path),
        reconnect,
        attach_capabilities,
        dedup_session_events: options.dedupe_session_events.unwrap_or(true),
        restore_attempt_timeout: Duration::from_millis(u64::from(
            options.restore_attempt_timeout_ms,
        )),
        transport: DaemonClientOptions {
            // The driver installs its own raw tap; a caller's tap would be
            // rejected by the core's validation.
            raw_tap: None,
            max_frame_bytes: usize::try_from(
                options.max_frame_bytes.unwrap_or(DEFAULT_MAX_FRAME_BYTES),
            )
            .ok(),
            max_pending: usize::try_from(options.max_pending.unwrap_or(DEFAULT_MAX_PENDING)).ok(),
            client_id: options.client_id.clone(),
        },
    };
    let caps = ManagedCaps {
        event_buffer,
        max_frame_bytes: Some(options.max_frame_bytes.unwrap_or(DEFAULT_MAX_FRAME_BYTES)),
        max_pending: Some(options.max_pending.unwrap_or(DEFAULT_MAX_PENDING)),
        restore_attempt_timeout_ms: options.restore_attempt_timeout_ms,
        dedupe_session_events: config.dedup_session_events,
        attach_capabilities: config.attach_capabilities.clone(),
        client_id: options.client_id.clone(),
        desktop_reconnect_policy,
    };
    // The constructor is a core future too: `start` spawns the driver, and
    // it must land on the binding-owned runtime — never napi's private one.
    let (session, mut core_events) =
        bridge(async move { DaemonClientSession::start(config).await })
            .await
            .map_err(|join_error| {
                napi_error(transport(format!("the start task failed: {join_error}")))
            })?
            .map_err(|error| napi_error(classify(error, None)))?;

    let (event_tx, event_rx) =
        mpsc::channel::<Value>(usize::try_from(event_buffer).expect("a bounded capacity"));
    let (stop_done, _) = watch::channel(false);
    let inner = Arc::new(ManagedInner {
        session,
        caps,
        event_tx: Mutex::new(Some(event_tx.clone())),
        event_rx: tokio::sync::Mutex::new(event_rx),
        teardown: Notify::new(),
        bridges: Arc::new(BridgeRegistry::default()),
        stop_state: std::sync::Mutex::new(StopState::Open),
        stop_done,
        stop_outcome: Mutex::new(None),
        overflow: AtomicBool::new(false),
        closed: AtomicBool::new(false),
    });

    // The event pump: forwards serialized events from the core's
    // (unbounded) channel into the bounded delivery queue, drains
    // promptly, and fails closed the moment the bounded queue is full —
    // never dropping an event. Registered like every binding-owned task so
    // the stop drain joins it.
    {
        let pump_inner = Arc::clone(&inner);
        let event_tx = event_tx;
        inner
            .spawn_tracked(async move {
                // The wake (enabled above) and the flag check together
                // close the miss window: a stop marked before the pump's
                // first poll is caught here; one between the check and
                // the select wakes the enabled notify.
                let notified = pump_inner.teardown.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                loop {
                    if pump_inner.closed.load(Ordering::SeqCst) {
                        break;
                    }
                    tokio::select! {
                        event = core_events.recv() => match event {
                            Some(event) => {
                                if event_tx.try_send(event_value(event)).is_err() {
                                    // Fail closed: the full queue tears the
                                    // whole session down (sticky Overflow on
                                    // every pull) — never a silent drop. The
                                    // pump never awaits the stop chain that
                                    // joins it.
                                    ManagedInner::start_stop(&pump_inner, true);
                                    break;
                                }
                            }
                            None => break,
                        },
                        _ = &mut notified => break,
                    }
                }
            })
            .map_err(|payload| napi_error(*payload))?;
    }
    Ok(ManagedDaemonSession { inner })
}
