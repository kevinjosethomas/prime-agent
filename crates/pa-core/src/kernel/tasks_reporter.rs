//! The trusted tasks-reporter env contract: the canonical JSON configuration the
//! desktop pushes to the daemon (`set_task_reporter`) and the daemon stamps on
//! every kernel environment it provisions.
//!
//! Two delivery paths converge on [`current`]:
//! - the daemon stamps [`TASKS_REPORTER_ENV`] on every spawned worker process
//!   (`worker_launch_env`), and the worker's kernels inherit it;
//! - the supervisor also forwards the accepted configuration to already-live
//!   workers, whose engines store it here ([`store_forwarded`]) so a kernel
//!   (re)start inside an existing worker picks the configuration up without a
//!   worker restart. A forwarded value also wins inside a freshly stamped
//!   worker (the two are the same trusted configuration).
//!
//! The value is trusted only because the daemon set it; it must never be
//! sourced from prompt or task content.

use std::sync::Mutex;

/// The env var carrying the canonical reporter JSON
/// `{"command": [...], "env": {...}, "timeout": seconds}`.
pub const TASKS_REPORTER_ENV: &str = "PRIME_AGENT_INTERNAL_TASKS_REPORTER";

/// The last configuration the supervisor forwarded to this worker process.
static FORWARDED: Mutex<Option<String>> = Mutex::new(None);

/// Store a configuration the supervisor forwarded to this live worker; every
/// kernel this worker starts from now on carries it.
///
/// # Panics
///
/// Panics only on mutex poisoning (the holder panicked mid-lock).
pub fn store_forwarded(value: &str) {
    *FORWARDED.lock().expect("tasks reporter forward lock") = Some(value.to_string());
}

/// The current reporter configuration: the forwarded value first, else this
/// process's own env var (a worker the supervisor stamped at spawn).
///
/// # Panics
///
/// Panics only on mutex poisoning (the holder panicked mid-lock).
#[must_use]
pub fn current() -> Option<String> {
    FORWARDED
        .lock()
        .expect("tasks reporter forward lock")
        .clone()
        .or_else(|| {
            std::env::var(TASKS_REPORTER_ENV)
                .ok()
                .filter(|v| !v.trim().is_empty())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The forwarded configuration wins over a stale process env value; an
    /// empty process value never surfaces as configured. The slot is reset
    /// after the test so it never leaks into other tests' kernel envs.
    #[test]
    fn forwarded_value_wins_over_the_process_env() {
        let prior = current();
        store_forwarded(r#"{"command":["x"],"env":{},"timeout":15}"#);
        assert_eq!(
            current().as_deref(),
            Some(r#"{"command":["x"],"env":{},"timeout":15}"#)
        );
        match prior {
            Some(value) => store_forwarded(&value),
            None => reset_forwarded_for_tests(),
        }
    }

    /// Test-only: drop a forwarded configuration again.
    pub(crate) fn reset_forwarded_for_tests() {
        *FORWARDED.lock().expect("tasks reporter forward lock") = None;
    }
}
