//! NAPI spike for the PR3b Node adapter. NOT the final API.
//!
//! Proves, on the real target toolchain: (1) the isolated workspace resolves
//! the pa-client/pa-types path dependencies and their workspace-inherited
//! deps from the root Cargo.toml; (2) the pinned napi-rs 3.x line builds;
//! (3) async fns ride the tokio_rt runtime; (4) ThreadsafeFunctions deliver
//! non-blocking and release cleanly (drop-release, no abort) so Node exits
//! naturally after teardown; (5) the `unsafe_code = "forbid"` lint question
//! for macro output.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use pa_client::daemon_client::{DaemonClientOptions, DEFAULT_REQUEST_TIMEOUT_MS};

#[napi(object)]
pub struct SpikeMeta {
    pub version: String,
    pub default_request_timeout_ms: u32,
    pub pa_client_options_linked: bool,
}

#[napi]
/// Build-boundary metadata plus a pa-client dependency-resolution proof.
pub fn spike_metadata() -> SpikeMeta {
    SpikeMeta {
        version: env!("CARGO_PKG_VERSION").to_string(),
        default_request_timeout_ms: u32::try_from(DEFAULT_REQUEST_TIMEOUT_MS)
            .expect("the default request timeout fits a u32"),
        pa_client_options_linked: DaemonClientOptions::default().raw_tap.is_none(),
    }
}

#[napi]
/// Proves async fns work on the napi-rs tokio runtime.
pub async fn spike_delay(ms: u32) -> u32 {
    tokio::time::sleep(Duration::from_millis(u64::from(ms))).await;
    ms
}

/// A periodic ThreadsafeFunction ticker whose handle stops it again. The
/// Node test asserts ticks arrive, and that after `stop()` the process ends
/// naturally: napi-rs 3 releases a ThreadsafeFunction when the last Rust
/// reference drops, so teardown is "drop every reference", not "abort".
#[napi]
pub struct SpikeTicker {
    stop: Arc<AtomicBool>,
    tsfn: Option<Arc<ThreadsafeFunction<u32>>>,
}

#[napi]
impl SpikeTicker {
    #[napi(constructor)]
    pub fn new(callback: Function) -> Result<Self> {
        let tsfn: ThreadsafeFunction<u32> = callback
            .build_threadsafe_function::<u32>()
            .callee_handled::<true>()
            .build_callback(|ctx| Ok(ctx.value))?;
        let stop = Arc::new(AtomicBool::new(false));
        let task_stop = Arc::clone(&stop);
        // napi-rs 3 ThreadsafeFunction is not Clone; the docs' idiom wraps it
        // in Arc — the last drop releases the underlying function.
        let tsfn = Arc::new(tsfn);
        let task_tsfn = Arc::clone(&tsfn);
        // Runs on the napi-rs tokio_rt runtime; the loop exits on the stop
        // flag and drops its TSFN clone, so a stopped ticker leaves nothing
        // holding the Node event loop.
        tokio::spawn(async move {
            let mut tick = 0_u32;
            while !task_stop.load(Ordering::SeqCst) {
                task_tsfn.call(Ok(tick), ThreadsafeFunctionCallMode::NonBlocking);
                tick = tick.saturating_add(1);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        Ok(Self {
            stop,
            tsfn: Some(tsfn),
        })
    }

    #[napi]
    pub fn stop(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        // Drop the binding's reference: with the task's clone gone one loop
        // iteration later, the TSFN releases and the event loop can drain.
        self.tsfn = None;
        Ok(())
    }
}
