//! The Node-API daemon client: a thin pull-based binding over
//! `pa-client`'s transport.
//!
//! The binding owns no transport semantics of its own — connect/handshake,
//! envelope matching, retry classification, and the raw frame tap all live
//! in `pa-client`. The JS surface is deliberately minimal:
//!
//! - `nextFrame()` pulls raw application frames (the lossless raw tap)
//!   through ONE bounded queue, single-consumer. The queue is fail-closed:
//!   a full queue tears the connection down and every later pull rejects
//!   with the terminal `Overflow` error. Frames are never dropped or
//!   coalesced.
//! - `nextStatus()` reports connection state: initially `open`, then
//!   changes only.
//! - `request`/`requestOk` speak serde `DaemonCommand`/`DaemonResponse` at
//!   the boundary — never a hand-maintained TS wire mirror — and preserve
//!   the precise `NotSent`/`Uncertain`/`Rejected` classes with no
//!   auto-resend.
//! - `close()` stops the binding's pumps and releases the core client.
//!   Honest scope: synchronous teardown of the core's reader/writer tasks
//!   is the shutdown-core work owned elsewhere; this binding claims only
//!   that it holds no tasks, channels, or event-loop handles after `close`.
//!
//! The core tap channel is unbounded on the core side (documented there):
//! this binding's bounded queue bounds the FRAME DELIVERY backlog, not the
//! transient core-side channel backlog; the pump drains promptly and
//! fails closed when the bounded queue fills.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use napi::bindgen_prelude::*;
use napi_derive::napi;
use pa_client::daemon_client::{
    DaemonClient, DaemonClientOptions, DEFAULT_REQUEST_TIMEOUT_MS,
};
use pa_types::daemon::DaemonCommand;
use serde_json::Value;
use tokio::sync::{mpsc, watch, Notify};

use crate::errors::{
    classify, napi_error, not_sent, not_sent_cause, overflow, transport, uncertain, ErrorPayload,
};
use crate::runtime::bridge;

/// The default bounded frame-queue capacity. Reported in the README.
pub(crate) const DEFAULT_FRAME_BUFFER: u32 = 1024;
/// The upper bound on a configured frame-queue capacity.
pub(crate) const MAX_FRAME_BUFFER: u32 = 1 << 20;

/// The connection state reported through `nextStatus`.
#[derive(Clone)]
pub(crate) enum Status {
    Open,
    Overflow,
    Closed { reason: String },
}

fn status_value(status: &Status) -> Value {
    match status {
        Status::Open => serde_json::json!({ "type": "open" }),
        Status::Overflow => serde_json::json!({ "type": "overflow" }),
        Status::Closed { reason } => {
            serde_json::json!({ "type": "closed", "reason": reason })
        }
    }
}

/// The effective configuration of one connection, as built from
/// [`ConnectOptions`] — the values the fixed-artifact provenance check
/// verifies the shipped addon against.
#[napi(object)]
pub struct Caps {
    /// The bounded frame-queue capacity in FRAMES (not bytes): 1024
    /// default. With the 64 MiB opt-in frame cap the worst-case queue
    /// memory is `frameBuffer * maxFrameBytes`.
    pub frame_buffer: u32,
    /// The inbound frame byte cap actually applied (`None` = unbounded).
    pub max_frame_bytes: Option<u32>,
    /// The pending-request cap actually applied (`None` = unbounded).
    pub max_pending: Option<u32>,
}

/// The binding-owned operation bridges (each request/upgrade future
/// bridged onto the shared runtime). A std Mutex guards the registry for
/// its brief synchronous sections only — never held across an await.
type BridgeRegistry = std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>;

/// The drain guard: awaiting one bridge handle at a time, retaining
/// ownership on cancellation (an unfinished handle re-enters the registry
/// so a later drain resumes) and removing it only on completion.
struct BridgeGuard {
    inner: Arc<Inner>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl std::future::Future for BridgeGuard {
    type Output = std::result::Result<(), tokio::task::JoinError>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        // SAFETY: the guard holds no self-referential data (plain Arc and
        // an Unpin JoinHandle), so &mut through the Pin is sound.
        let this = std::pin::Pin::into_inner(self);
        let Some(handle) = this.handle.as_mut() else {
            return std::task::Poll::Ready(Ok(()));
        };
        match handle.poll(cx) {
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
            self.inner
                .bridges
                .lock()
                .expect("the bridge registry is not poisoned")
                .push(handle);
        }
    }
}

/// The close lifecycle: one runner performs the shutdown chain, concurrent
/// callers await the same real completion (the `close_done` watch), late
/// callers return settled.
enum CloseState {
    Open,
    Closing,
    Done,
}

pub(crate) struct Inner {
    client: Mutex<Option<Arc<DaemonClient>>>,
    /// The effective configuration this connection was built with —
    /// reported back to JS for provenance checks.
    caps: Caps,
    status_tx: watch::Sender<Status>,
    /// The bounded frame-queue sender. `None` after teardown; taking it
    /// wakes a pending `nextFrame` with end-of-stream.
    frame_tx: Mutex<Option<mpsc::Sender<Value>>>,
    /// The receiving end: the try-lock doubles as the single-consumer
    /// enforcement for `nextFrame`.
    frame_rx: tokio::sync::Mutex<mpsc::Receiver<Value>>,
    /// Wakes in-flight requests on overflow/close teardown.
    teardown: Arc<Notify>,
    /// The binding-owned operation bridges, aborted and joined during close.
    bridges: BridgeRegistry,
    /// Idempotent close coordination across concurrent callers. The gate
    /// is a std Mutex: checked and set in brief synchronous sections.
    close_state: std::sync::Mutex<CloseState>,
    /// Broadcasts the real close completion to concurrent callers.
    close_done: watch::Sender<bool>,
    overflow: AtomicBool,
    closed: AtomicBool,
    initial_status_delivered: AtomicBool,
}

impl Inner {
    /// The live client, or a provable `NotSent` refusal after the close
    /// chain started — the overflow cause is surfaced when the frame
    /// queue tore the connection down.
    fn client_arc(&self) -> Result<Arc<DaemonClient>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(napi_error(self.closed_refusal()));
        }
        self.client
            .lock()
            .expect("the client slot is not poisoned")
            .clone()
            .ok_or_else(|| napi_error(self.closed_refusal()))
    }

    fn closed_refusal(&self) -> ErrorPayload {
        if self.overflow.load(Ordering::SeqCst) {
            not_sent(
                "the connection was torn down after the frame queue overflowed; the command was never queued".to_string(),
            )
        } else {
            not_sent("the client is closed; the command was never queued".to_string())
        }
    }

    /// The teardown cause for in-flight operations: `None` while the
    /// connection is live, `Some("overflow")` / `Some("closed")` after
    /// the close chain marked the connection.
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

    /// The atomic registration gate: refuses new operations once the close
    /// chain marked the connection (checked under the registry lock, so no
    /// late op can escape the drain), else spawns the worker onto the
    /// owned runtime and retains its handle in the registry.
    fn spawn_tracked<F>(&self, worker: F) -> std::result::Result<(), ErrorPayload>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut registry = self
            .bridges
            .lock()
            .expect("the bridge registry is not poisoned");
        // Reap finished handles at registration so a long-lived connection
        // does not retain one dead entry per completed operation.
        registry.retain(|handle| !handle.is_finished());
        if self.closed.load(Ordering::SeqCst) {
            return Err(self.closed_refusal());
        }
        registry.push(bridge(worker));
        Ok(())
    }

    /// The binding-owned wake of step one: flags, status, and the notify —
    /// every pending pull and in-flight request observes the cause.
    fn mark_closed(&self, is_overflow: bool, reason: &str) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        if is_overflow {
            self.overflow.store(true, Ordering::SeqCst);
        }
        let status = if is_overflow {
            Status::Overflow
        } else {
            Status::Closed {
                reason: reason.to_string(),
            }
        };
        let _ = self.status_tx.send(status);
        self.teardown.notify_waiters();
        *self
            .frame_tx
            .lock()
            .expect("the frame queue sender is not poisoned") = None;
    }

    /// The full close chain (PR7): mark closed and refuse new operations;
    /// shut the core transport down; cancel/join the binding-owned
    /// operation bridges; then join the core's registered task set to a
    /// real completion. Idempotent: concurrent callers await the same
    /// completion, late callers return settled.
    /// Start the dedicated cleanup runner at most once; concurrent or late
    /// callers observe the completion watch instead. The runner is NOT
    /// registered in the operation registry (it drains that registry), and
    /// no caller awaits the runner's handle — a cancelled JS close promise
    /// leaves the runner alive to completion.
    pub(crate) fn start_close(self: &Arc<Self>, is_overflow: bool, reason: &'static str) {
        let mut gate = self
            .close_state
            .lock()
            .expect("the close gate is not poisoned");
        match *gate {
            CloseState::Done | CloseState::Closing => {}
            CloseState::Open => {
                *gate = CloseState::Closing;
                let inner = Arc::clone(self);
                bridge(async move {
                    inner.close_chain(is_overflow, reason).await;
                });
            }
        }
    }

    /// The full close chain, running entirely on the owned runtime: mark
    /// closed and refuse new operations, shut the core transport down, drain
    /// the binding-owned operation registry one handle at a time behind the
    /// re-insertion guard, then join the core's registered task set.
    async fn close_chain(self: Arc<Self>, is_overflow: bool, reason: &'static str) {
        self.mark_closed(is_overflow, reason);
        let client = self
            .client
            .lock()
            .expect("the client slot is not poisoned")
            .clone();
        if let Some(client) = &client {
            client.shutdown();
        }
        loop {
            let handle = {
                self.bridges
                    .lock()
                    .expect("the bridge registry is not poisoned")
                    .pop()
            };
            let Some(handle) = handle else { break };
            let mut guard = BridgeGuard {
                inner: Arc::clone(&self),
                handle: Some(handle),
            };
            let _ = (&mut guard).await;
            guard.handle = None;
        }
        if let Some(client) = &client {
            let _ = client.join_tasks().await;
            self.client
                .lock()
                .expect("the client slot is not poisoned")
                .take();
        }
        *self
            .close_state
            .lock()
            .expect("the close gate is not poisoned") = CloseState::Done;
        // send_replace retains the completion value even with zero live
        // receivers, so every later subscriber observes Done.
        self.close_done.send_replace(true);
    }
}

/// Options for [`connect`]. All derived from `pa-client`'s transport caps.
#[napi(object)]
#[derive(Default)]
pub struct ConnectOptions {
    /// The bounded frame-queue capacity (default 1024, must be 1..=1048576).
    /// Bounds the JS-side delivery backlog only — the core tap channel
    /// itself stays unbounded, as documented in `pa-client`.
    pub frame_buffer: Option<u32>,
    /// The opt-in inbound frame byte cap; a frame beyond it tears the
    /// transport down (fail every pending request as uncertain).
    pub max_frame_bytes: Option<u32>,
    /// The opt-in pending-request cap; a request at or beyond it refuses
    /// the send before any bytes are queued (a provable non-delivery).
    pub max_pending: Option<u32>,
}

#[napi]
/// The native daemon client handle. See the crate docs for the pull/fail
/// closed contract.
pub struct PrimeDaemonClient {
    inner: Arc<Inner>,
}

impl Drop for PrimeDaemonClient {
    fn drop(&mut self) {
        // The napi finalizer (GC) path: async-safe ONLY — schedule the full
        // close chain on the binding-owned runtime (wake + core shutdown +
        // bridge joins + core task join). No core calls from the GC thread;
        // an already-closed connection settles to the Done state instantly.
        Inner::start_close(&self.inner, false, "dropped without close");
    }
}

#[napi]
impl PrimeDaemonClient {
    #[napi]
    /// The current status value without waiting for a change.
    pub fn current_status(&self) -> Result<Value> {
        let status = self.inner.status_tx.borrow().clone();
        Ok(status_value(&status))
    }

    #[napi]
    /// The effective configuration this connection was built with.
    pub fn caps(&self) -> Caps {
        Caps {
            frame_buffer: self.inner.caps.frame_buffer,
            max_frame_bytes: self.inner.caps.max_frame_bytes,
            max_pending: self.inner.caps.max_pending,
        }
    }

    #[napi]
    pub fn hello(&self) -> Result<Value> {
        let client = self.inner.client_arc()?;
        Ok(client.hello().clone())
    }

    #[napi]
    pub fn protocol(&self) -> Result<Value> {
        let client = self.inner.client_arc()?;
        serde_json::to_value(client.protocol()).map_err(|error| {
            napi_error(transport(format!("serialize protocol info: {error}")))
        })
    }

    #[napi]
    pub fn supports_server_capability(&self, capability: String) -> Result<bool> {
        let client = self.inner.client_arc()?;
        Ok(client.supports_server_capability(&capability))
    }

    #[napi]
    pub fn socket_path(&self) -> Result<String> {
        let client = self.inner.client_arc()?;
        Ok(client.socket_path().display().to_string())
    }

    #[napi]
    pub fn direct_session_id(&self) -> Result<Option<String>> {
        let client = self.inner.client_arc()?;
        Ok(client.direct_session_id())
    }

    #[napi]
    pub fn drop_direct(&self) -> Result<()> {
        let client = self.inner.client_arc()?;
        client.drop_direct();
        Ok(())
    }

    #[napi]
    pub async fn upgrade_direct(&self, active_session_id: String) -> Result<bool> {
        let client = self.inner.client_arc()?;
        let (result_tx, result_rx) =
            tokio::sync::oneshot::channel::<std::result::Result<bool, Box<ErrorPayload>>>();
        let inner = Arc::clone(&self.inner);
        self.inner
            .spawn_tracked(async move {
                let notified = inner.teardown.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let fut = client.upgrade_direct(&active_session_id);
                tokio::pin!(fut);
                // Pre-poll barrier (see `request`).
                if let Some(cause) = inner.teardown_cause() {
                    let _ = result_tx.send(Err(Box::new(not_sent_cause(Some(cause)))));
                    return;
                }
                // The core arm first; a teardown wake settles the DEFINED
                // negative — the link provably never installs — dropping the
                // in-flight handshake future.
                let outcome = tokio::select! {
                    biased;
                    core = &mut fut => core.map_err(|error| Box::new(classify(error, None))),
                    _ = &mut notified => Ok(false),
                };
                let _ = result_tx.send(outcome);
            })
            .map_err(napi_error)?;
        result_rx
            .await
            .map_err(|_| {
                napi_error(transport(
                    "the upgrade worker failed to settle its channel".to_string(),
                ))
            })?
            .map_err(|payload| napi_error(*payload))
    }

    #[napi]
    pub async fn next_frame(&self) -> Result<Option<Value>> {
        let mut receiver = self.inner.frame_rx.try_lock().map_err(|_| {
            napi_error(transport(
                "a frame pull is already pending: nextFrame is single-consumer".to_string(),
            ))
        })?;
        // The wakeup is BINDING-OWNED: register on the teardown notify
        // BEFORE checking the flags, then race the queue against the wake —
        // the pull never depends on the core channel closing (whose chain
        // rides on socket EOF).
        let notified = self.inner.teardown.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.inner.overflow.load(Ordering::SeqCst) {
            return Err(napi_error(overflow()));
        }
        if self.inner.teardown_cause().is_some() {
            return Ok(None);
        }
        tokio::select! {
            frame = receiver.recv() => match frame {
                Some(frame) => {
                    // The sticky overflow error precedes ANY buffered frame:
                    // the consumer never continues on a partial stream.
                    if self.inner.overflow.load(Ordering::SeqCst) {
                        Err(napi_error(overflow()))
                    } else {
                        Ok(Some(frame))
                    }
                }
                None => {
                    if self.inner.overflow.load(Ordering::SeqCst) {
                        Err(napi_error(overflow()))
                    } else {
                        Ok(None)
                    }
                }
            },
            _ = &mut notified => {
                if self.inner.overflow.load(Ordering::SeqCst) {
                    Err(napi_error(overflow()))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Per-consumer status changes: every pending pull observes the next
    /// change (a genuine watch broadcast), and the initial `open` value is
    /// delivered exactly once per connection — not per caller. Terminal
    /// statuses resolve immediately for late callers.
    #[napi]
    pub async fn next_status(&self) -> Result<Value> {
        let mut receiver = self.inner.status_tx.subscribe();
        if !self
            .inner
            .initial_status_delivered
            .swap(true, Ordering::SeqCst)
        {
            return Ok(status_value(&receiver.borrow().clone()));
        }
        loop {
            let current = receiver.borrow_and_update().clone();
            if !matches!(current, Status::Open) {
                return Ok(status_value(&current));
            }
            if receiver.changed().await.is_err() {
                return Ok(status_value(&current));
            }
        }
    }

    #[napi]
    pub async fn request(
        &self,
        command: Value,
        timeout_ms: Option<u32>,
    ) -> Result<Value> {
        let client = self.inner.client_arc()?;
        let command: DaemonCommand = serde_json::from_value(command)
            .map_err(|error| {
                napi_error(not_sent(format!("command deserialize failed: {error}")))
            })?;
        let timeout = u64::from(
            timeout_ms.unwrap_or(u32::try_from(DEFAULT_REQUEST_TIMEOUT_MS).expect(
                "the default request timeout fits a u32",
            )),
        );
        let (result_tx, result_rx) = tokio::sync::oneshot::channel::<std::result::Result<Value, Box<ErrorPayload>>>();
        let inner = Arc::clone(&self.inner);
        // ONE tracked worker carries the operation; the napi promise rides a
        // oneshot so a cancelled JS promise never detaches the work — the
        // close drain joins the real handle. The worker ALWAYS settles the
        // channel (barrier and both select arms send), so a missing send is
        // an invariant violation surfaced as Transport, never masked.
        self.inner
            .spawn_tracked(async move {
                let notified = inner.teardown.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let fut = client.request_with_timeout(command, timeout);
                tokio::pin!(fut);
                // Deterministic pre-poll barrier: teardown observed here is
                // BEFORE the request's first poll, so the line was provably
                // never queued — NotSent with the cause, never Uncertain.
                if let Some(cause) = inner.teardown_cause() {
                    let _ = result_tx.send(Err(Box::new(not_sent_cause(Some(cause)))));
                    return;
                }
                // Biased with the core future FIRST: when a completed answer
                // and the teardown wake are both ready, the core's own class
                // (Success/Rejected/transport) must win — never downgraded to
                // Uncertain by the race.
                let outcome = tokio::select! {
                    biased;
                    response = &mut fut => {
                        let response = response.map_err(|error| Box::new(classify(error, None)));
                        response.and_then(|response| {
                            serde_json::to_value(response).map_err(|error| {
                                Box::new(transport(format!("serialize response: {error}")))
                            })
                        })
                    }
                    _ = &mut notified => {
                        let cause = inner.teardown_cause().unwrap_or("closed");
                        Err(Box::new(uncertain(
                            "the connection was torn down before the response arrived".to_string(),
                            Some(cause),
                        )))
                    }
                };
                let _ = result_tx.send(outcome);
            })
            .map_err(napi_error)?;
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
    pub async fn request_ok(&self, command: Value) -> Result<Value> {
        let client = self.inner.client_arc()?;
        let command: DaemonCommand = serde_json::from_value(command)
            .map_err(|error| {
                napi_error(not_sent(format!("command deserialize failed: {error}")))
            })?;
        let (result_tx, result_rx) = tokio::sync::oneshot::channel::<std::result::Result<Value, Box<ErrorPayload>>>();
        let inner = Arc::clone(&self.inner);
        self.inner
            .spawn_tracked(async move {
                let notified = inner.teardown.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let fut = client.request_ok(command);
                tokio::pin!(fut);
                // Deterministic pre-poll barrier (see `request`).
                if let Some(cause) = inner.teardown_cause() {
                    let _ = result_tx.send(Err(Box::new(not_sent_cause(Some(cause)))));
                    return;
                }
                // Biased with the core future first (see `request`).
                let outcome = tokio::select! {
                    biased;
                    data = &mut fut => data.map_err(|error| Box::new(classify(error, None))),
                    _ = &mut notified => {
                        let cause = inner.teardown_cause().unwrap_or("closed");
                        Err(Box::new(uncertain(
                            "the connection was torn down before the response arrived".to_string(),
                            Some(cause),
                        )))
                    }
                };
                let _ = result_tx.send(outcome);
            })
            .map_err(napi_error)?;
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
    /// The full close chain, as a promise that resolves on REAL completion:
    /// mark closed and refuse new operations, shut the core transport down,
    /// drain the binding-owned operation registry, then join the core's
    /// registered task set. Idempotent: concurrent callers await the same
    /// completion watch. Cancelling this promise does NOT cancel the
    /// cleanup runner; after completion the JS side holds no task, channel,
    /// or event-loop handle.
    pub async fn close(&self) -> Result<()> {
        Inner::start_close(&self.inner, false, "closed by the host");
        let mut done = self.inner.close_done.subscribe();
        if *done.borrow() {
            return Ok(());
        }
        let _ = done.changed().await;
        Ok(())
    }
}

#[napi]
/// Connect to the daemon at `socketPath`, complete the hello handshake, and
/// return a pull-based client. `options.frameBuffer` bounds the frame
/// delivery queue (default 1024); the queue is fail-closed — see the crate
/// docs.
pub async fn connect(
    socket_path: String,
    options: Option<ConnectOptions>,
) -> Result<PrimeDaemonClient> {
    let options = options.unwrap_or_default();
    let frame_buffer = options.frame_buffer.unwrap_or(DEFAULT_FRAME_BUFFER);
    if frame_buffer == 0 || frame_buffer > MAX_FRAME_BUFFER {
        return Err(napi_error(transport(format!(
            "frameBuffer must be between 1 and {MAX_FRAME_BUFFER}"
        ))));
    }
    if options.max_frame_bytes.is_some_and(|cap| cap == 0) {
        return Err(napi_error(transport("maxFrameBytes must be at least 1".to_string())));
    }
    if options.max_pending.is_some_and(|cap| cap == 0) {
        return Err(napi_error(transport("maxPending must be at least 1".to_string())));
    }
    let (tap_tx, tap_rx) = mpsc::unbounded_channel::<Value>();
    let core_options = DaemonClientOptions {
        raw_tap: Some(tap_tx),
        max_frame_bytes: options.max_frame_bytes.and_then(|cap| usize::try_from(cap).ok()),
        max_pending: options.max_pending.and_then(|cap| usize::try_from(cap).ok()),
    };
    let caps = Caps {
        frame_buffer,
        max_frame_bytes: options.max_frame_bytes,
        max_pending: options.max_pending,
    };
    let path = std::path::PathBuf::from(socket_path);

    let (client, event_rx) = bridge(async move {
        DaemonClient::connect_with_options(&path, core_options).await
    })
    .await
    .map_err(|join_error| {
        napi_error(transport(format!("the connect task failed: {join_error}")))
    })?
    .map_err(|error| napi_error(classify(error, None)))?;
    let client = Arc::new(client);

    let (frame_tx, frame_rx) =
        mpsc::channel::<Value>(usize::try_from(frame_buffer).expect("a bounded capacity"));
    let (status_tx, _) = watch::channel(Status::Open);

    let (close_done, _) = watch::channel(false);
    let inner = Arc::new(Inner {
        client: Mutex::new(Some(Arc::clone(&client))),
        caps,
        status_tx,
        frame_tx: Mutex::new(Some(frame_tx.clone())),
        frame_rx: tokio::sync::Mutex::new(frame_rx),
        teardown: Arc::new(Notify::new()),
        bridges: BridgeHandles::default(),
        close_state: std::sync::Mutex::new(CloseState::Open),
        close_done,
        overflow: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        initial_status_delivered: AtomicBool::new(false),
    });

    // The tap pump: forwards raw frames from the core's (unbounded) tap
    // channel into the bounded delivery queue, drains promptly, and fails
    // closed the moment the bounded queue is full — never dropping a frame.
    // Both pumps are REGISTERED operations: the cleanup runner drains them
    // alongside the request/upgrade workers.
    {
        let inner = Arc::clone(&inner);
        let frame_tx = frame_tx;
        let mut tap_rx = tap_rx;
        inner
            .spawn_tracked(async move {
            // The pump also exits on the binding-owned teardown wake (the
            // core tap closing is NOT a teardown trigger here: its chain
            // rides on socket EOF, which is the shutdown PR's to prove).
            let notified = inner.teardown.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            loop {
                tokio::select! {
                    maybe = tap_rx.recv() => match maybe {
                        Some(frame) => {
                            if frame_tx.try_send(frame).is_err() {
                                // Start the dedicated cleanup (which drains
                                // this pump's own registry entry) and exit:
                                // the pump never awaits the cleanup that
                                // joins it.
                                Inner::start_close(&inner, true, "the frame queue overflowed");
                                break;
                            }
                        }
                        None => break,
                    },
                    _ = &mut notified => break,
                }
            }
        })
        .expect("the connection is open when the tap pump registers");
    }
    // The typed event channel is unbound from JS (no TUI enum serde, no
    // hand-written event mirror) but must still be drained: the core keeps
    // it unbounded, and this discard pump owns exactly that. Registered
    // like every other binding-owned task.
    {
        let mut event_rx = event_rx;
        inner
            .spawn_tracked(async move {
                while event_rx.recv().await.is_some() {}
            })
            .expect("the connection is open when the event drain registers");
    }
    Ok(PrimeDaemonClient { inner })
}
