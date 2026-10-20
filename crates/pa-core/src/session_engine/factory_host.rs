//! The factory host bridge: the session seam that exposes the kernel's
//! factory surface (`factory.graph/status/watch/run/stop/resume`) to the
//! daemon and TUI, the `/factory` view's lane.
//!
//! The factory itself lives in the kernel (`rlm/factory.py`, the Rust port's
//! Python kernel architecture — the executor owns the run registry in kernel
//! memory), so this bridge is host->kernel: the session engine validates the
//! request and rides the out-of-band `factory_activity` kernel frame (the
//! `bash_activity` precedent — a running cell never delays the live view).
//! The kernel's `run` owns machine resolution and validation (the machine
//! library is the only spec source); the executor's own children ride the
//! existing `rlm.spawn` host path, where the daemon allowlist pin and
//! request auth are enforced per spawn.

use anyhow::anyhow;

use crate::kernel::shared::{FACTIVITY_WATCH_TIMEOUT_MS_CAP, FACTORY_ACTIVITY_ACTIONS};

/// The out-of-band request the bridge sends into the kernel, validated
/// host-side before the frame: one action with its target and the watch
/// bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactoryActivityRequest {
    pub action: &'static str,
    pub run_id: Option<String>,
    pub spec_id: Option<String>,
    pub timeout_ms: Option<u64>,
}

impl FactoryActivityRequest {
    /// Parse and validate one daemon command payload. Mirrors the kernel
    /// frame's own validation (the kernel re-validates): a known action,
    /// string targets when present, and a bounded watch timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when the action is unknown, a required target is
    /// missing, or the timeout is outside its bounds.
    pub fn parse(
        action: &str,
        run_id: Option<&str>,
        spec_id: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> anyhow::Result<Self> {
        let Some(action) = FACTORY_ACTIVITY_ACTIONS
            .iter()
            .find(|known| **known == action)
            .copied()
        else {
            return Err(anyhow!("unknown factory activity action"));
        };
        if action == "run" && spec_id.is_none_or(|id| id.trim().is_empty()) {
            return Err(anyhow!("factory activity run requires specId"));
        }
        if action != "graph" && action != "run" && run_id.is_none_or(|id| id.trim().is_empty()) {
            return Err(anyhow!("factory activity {action} requires runId"));
        }
        if let Some(timeout_ms) = timeout_ms {
            if timeout_ms > FACTIVITY_WATCH_TIMEOUT_MS_CAP {
                return Err(anyhow!(
                    "factory activity timeoutMs must be at most {FACTIVITY_WATCH_TIMEOUT_MS_CAP}"
                ));
            }
        }
        Ok(Self {
            action,
            run_id: run_id
                .filter(|id| !id.trim().is_empty())
                .map(str::to_string),
            spec_id: spec_id
                .filter(|id| !id.trim().is_empty())
                .map(str::to_string),
            timeout_ms,
        })
    }
}

// The unit battery lives in the child module (factory_host::tests).
#[cfg(test)]
mod tests;
