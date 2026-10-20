//! The binding's shared tokio runtime.
//!
//! A synchronous napi call thread has no ambient tokio reactor, and
//! napi-rs keeps its shared runtime module private. Every core future is
//! bridged through this ONE process-wide runtime, so `pa-client`'s
//! internal task spawns (reader/writer pumps) land on the same reactor as
//! the binding's own pump tasks. The runtime's parked worker threads never
//! hold the Node event loop: with no thread-safe function and no JS handle
//! outstanding, Node exits on its own.

use std::sync::OnceLock;

use tokio::runtime::Runtime;

/// Worker threads for the shared runtime: a client transport is I/O-bound;
/// two workers serve connect/request/pump concurrency without excess threads.
const WORKER_THREADS: usize = 2;

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKER_THREADS)
            .enable_all()
            .build()
            .expect("the client-node runtime builds")
    })
}

/// Run one core future to completion on the shared runtime.
pub(crate) fn bridge<T, F>(fut: F) -> tokio::task::JoinHandle<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    runtime().spawn(fut)
}
