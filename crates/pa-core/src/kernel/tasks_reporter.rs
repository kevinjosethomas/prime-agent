//! The trusted tasks-reporter env contract: the canonical JSON the daemon
//! stamps on every kernel environment or forwards to a live worker, so
//! `import tasks` self-configures without any prompt. Trusted only because
//! the daemon set it; never sourced from prompt or task content.

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

    /// The forwarded value wins over the process env; reset so it never leaks.
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
