//! Atomic global Harness preference updates; live session state is untouched.

use anyhow::{anyhow, Result};
use pa_types::daemon::global_harness_defaults::{GlobalHarnessDefaultsPatch, HarnessQueueMode};

use super::{QueueModeSetting, SettingsManager, SettingsScope};

impl SettingsManager {
    /// Persist only the supplied global Harness fields in one locked write.
    ///
    /// # Errors
    /// Returns an error if global settings could not be read or saved.
    pub fn set_global_harness_defaults(
        &mut self,
        patch: &GlobalHarnessDefaultsPatch,
    ) -> Result<()> {
        if let Some(error) = self
            .errors()
            .iter()
            .find(|error| error.scope == SettingsScope::Global)
        {
            return Err(anyhow!(
                "Cannot update global Harness defaults: {}",
                error.message
            ));
        }
        let mut fields = Vec::new();
        let global = self.global_mut();
        if let Some(enabled) = patch.auto_compaction_enabled {
            global
                .compaction
                .get_or_insert_with(Default::default)
                .enabled = Some(enabled);
            fields.push("compaction.enabled");
        }
        if let Some(mode) = patch.steering_mode {
            global.steering_mode = Some(queue_mode_setting(mode));
            fields.push("steeringMode");
        }
        if let Some(mode) = patch.follow_up_mode {
            global.follow_up_mode = Some(queue_mode_setting(mode));
            fields.push("followUpMode");
        }
        if let Some(depth) = patch.max_depth {
            global.rlm_max_depth = Some(depth);
            fields.push("rlmMaxDepth");
        }
        if fields.is_empty() {
            return Ok(());
        }
        self.save_global_scope(&fields)
    }
}

fn queue_mode_setting(mode: HarnessQueueMode) -> QueueModeSetting {
    match mode {
        HarnessQueueMode::All => QueueModeSetting::All,
        HarnessQueueMode::OneAtATime => QueueModeSetting::OneAtATime,
    }
}
