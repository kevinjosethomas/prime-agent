//! NAPI spike for the PR3b Node adapter. NOT the final API.
//!
//! Proves, on the real target toolchain: (1) the isolated workspace resolves
//! the pa-client/pa-types path dependencies and their workspace-inherited
//! deps from the root Cargo.toml; (2) the pinned napi-rs major builds;
//! (3) async fns ride the tokio_rt runtime; (4) ThreadsafeFunctions deliver
//! non-blocking and release cleanly so Node exits naturally after teardown;
//! (5) the `unsafe_code = "forbid"` lint question for macro output.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::Env;
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
/// naturally (no un-released TSFN keeping the event loop alive).
#[napi]
pub struct SpikeTicker {
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
    tsfn: ThreadsafeFunction<u32>,
}

#[napi]
impl SpikeTicker {
    #[napi(constructor)]
    pub fn new(env: Env, callback: JsFunction) -> Result<Self> {
        let tsfn: ThreadsafeFunction<u32> =
            callback.create_threadsafe_function(0, |ctx| {
                ctx.env
                    .create_int32(i32::try_from(ctx.value).unwrap_or(0))
                    .map(|v| vec![v])
            })?;
        let stop = Arc::new(AtomicBool::new(false));
        let task_stop = Arc::clone(&stop);
        let task_tsfn = tsfn.clone();
        // Runs on the napi-rs tokio_rt runtime; the loop exits on the stop
        // flag and drops the TSFN clone, so a stopped ticker leaves nothing
        // holding the Node event loop.
        let task = tokio::spawn(async move {
            let mut tick = 0_u32;
            while !task_stop.load(Ordering::SeqCst) {
                task_tsfn.call(tick, ThreadsafeFunctionCallMode::NonBlocking);
                tick += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        Ok(Self {
            stop,
            task: Some(task),
            tsfn,
        })
    }

    #[napi]
    pub fn stop(&mut self, env: Env) -> Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        // Abort the JS-side queue too: releasing the remaining reference is
        // what lets the Node event loop drain.
        let _ = env;
        self.tsfn.abort();
        Ok(())
    }
}
