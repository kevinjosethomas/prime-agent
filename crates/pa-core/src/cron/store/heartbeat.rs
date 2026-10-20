//! `/heartbeat` job management: create/pause/resume/clear lifecycle plus
//! the heartbeat management actions (pause/resume/stop).

use uuid::Uuid;

use super::state::normalize_optional_label;
use super::{
    iso_from_millis, now_millis, AgentCronJobStore, CreateAgentCronJobInput,
    HeartbeatManagementAction,
};
use crate::cron::{
    is_heartbeat_cron_job, next_run_at_for_schedule, parse_agent_cron_schedule, parse_iso_millis,
    AgentCronJob, HeartbeatJobUpdate, JobStatus, ScheduleKind, DEFAULT_HEARTBEAT_DELIVERY_MODE,
};

impl AgentCronJobStore {
    pub fn get_heartbeat(&self, active_session_id: &str) -> Option<AgentCronJob> {
        self.read_jobs()
            .into_iter()
            .filter(|job| {
                job.active_session_id == active_session_id
                    && job.source.as_deref() == Some("heartbeat")
                    && matches!(job.status, JobStatus::Active | JobStatus::Paused)
            })
            .max_by_key(|job| parse_iso_millis(&job.updated_at).unwrap_or(0))
    }

    pub fn get_latest_heartbeat(&self, active_session_id: &str) -> Option<AgentCronJob> {
        self.read_jobs()
            .into_iter()
            .filter(|job| {
                job.active_session_id == active_session_id
                    && job.source.as_deref() == Some("heartbeat")
            })
            .max_by_key(|job| parse_iso_millis(&job.updated_at).unwrap_or(0))
    }

    /// Create the heartbeat job for a session, cancelling its existing active/paused
    /// heartbeat first.
    ///
    /// # Errors
    ///
    /// Returns an error when the schedule is invalid or not recurring.
    pub fn create_heartbeat(
        &self,
        input: &CreateAgentCronJobInput,
    ) -> anyhow::Result<AgentCronJob> {
        let now = input.now.unwrap_or_else(now_millis);
        let parsed = parse_agent_cron_schedule(&input.schedule_text, now)?;
        if parsed.0.kind == ScheduleKind::Once {
            anyhow::bail!("Heartbeat schedule must be recurring");
        }
        let prompt = input.prompt.trim();
        if prompt.is_empty() {
            anyhow::bail!("Heartbeat instruction cannot be empty");
        }
        let now_iso = iso_from_millis(now);
        let existing: Vec<AgentCronJob> = self
            .read_jobs()
            .into_iter()
            .map(|job| {
                if job.active_session_id == input.active_session_id
                    && job.source.as_deref() == Some("heartbeat")
                    && matches!(job.status, JobStatus::Active | JobStatus::Paused)
                {
                    AgentCronJob {
                        status: JobStatus::Cancelled,
                        next_run_at: None,
                        updated_at: now_iso.clone(),
                        ..job
                    }
                } else {
                    job
                }
            })
            .collect();
        let job = AgentCronJob {
            id: Uuid::new_v4().to_string(),
            status: JobStatus::Active,
            source: Some("heartbeat".to_string()),
            runtime_kind: input.runtime_kind.clone(),
            delivery_mode: Some(
                input
                    .delivery_mode
                    .unwrap_or(DEFAULT_HEARTBEAT_DELIVERY_MODE),
            ),
            active_session_id: input.active_session_id.clone(),
            session_id: input.session_id.clone(),
            session_file: input.session_file.clone(),
            cwd: input.cwd.clone(),
            label: normalize_optional_label(input.label.as_deref()),
            prompt: prompt.to_string(),
            schedule: parsed.0,
            created_at: now_iso.clone(),
            updated_at: now_iso,
            next_run_at: Some(iso_from_millis(parsed.1)),
            last_run_at: None,
            last_skipped_at: None,
            last_error: None,
            run_count: 0,
        };
        let mut jobs = existing;
        jobs.push(job.clone());
        self.write_jobs(&jobs)?;
        Ok(job)
    }
    /// # Errors
    ///
    /// Returns an error when the state lock or write fails.
    pub fn pause_heartbeat(
        &self,
        active_session_id: &str,
        now: u64,
    ) -> anyhow::Result<Option<AgentCronJob>> {
        let Some(current) = self.get_heartbeat(active_session_id) else {
            return Ok(None);
        };
        let now_iso = iso_from_millis(now);
        let mut paused = None;
        let jobs: Vec<AgentCronJob> = self
            .read_jobs()
            .into_iter()
            .map(|job| {
                if job.id != current.id {
                    return job;
                }
                let paused_job = AgentCronJob {
                    status: JobStatus::Paused,
                    next_run_at: None,
                    updated_at: now_iso.clone(),
                    ..job
                };
                paused = Some(paused_job.clone());
                paused_job
            })
            .collect();
        self.write_jobs(&jobs)?;
        Ok(paused)
    }

    /// Resume the heartbeat for a session, recomputing its next run time. `Ok(None)`
    /// when the session has no heartbeat.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored schedule is invalid or not recurring.
    pub fn resume_heartbeat(
        &self,
        active_session_id: &str,
        now: u64,
    ) -> anyhow::Result<Option<AgentCronJob>> {
        let Some(current) = self.get_heartbeat(active_session_id) else {
            return Ok(None);
        };
        let Some(next_run_at) = next_run_at_for_schedule(&current.schedule, now)? else {
            anyhow::bail!("Heartbeat schedule must be recurring");
        };
        let now_iso = iso_from_millis(now);
        let mut resumed = None;
        let jobs: Vec<AgentCronJob> = self
            .read_jobs()
            .into_iter()
            .map(|job| {
                if job.id != current.id {
                    return job;
                }
                let resumed_job = AgentCronJob {
                    status: JobStatus::Active,
                    next_run_at: Some(iso_from_millis(next_run_at)),
                    updated_at: now_iso.clone(),
                    ..job
                };
                resumed = Some(resumed_job.clone());
                resumed_job
            })
            .collect();
        self.write_jobs(&jobs)?;
        Ok(resumed)
    }

    /// # Errors
    ///
    /// Returns an error when the state lock or write fails.
    pub fn clear_heartbeat(
        &self,
        active_session_id: &str,
        now: u64,
    ) -> anyhow::Result<Option<AgentCronJob>> {
        let Some(current) = self.get_heartbeat(active_session_id) else {
            return Ok(None);
        };
        let now_iso = iso_from_millis(now);
        let mut cleared = None;
        let jobs: Vec<AgentCronJob> = self
            .read_jobs()
            .into_iter()
            .map(|job| {
                if job.id != current.id {
                    return job;
                }
                let cleared_job = AgentCronJob {
                    status: JobStatus::Cancelled,
                    next_run_at: None,
                    updated_at: now_iso.clone(),
                    ..job
                };
                cleared = Some(cleared_job.clone());
                cleared_job
            })
            .collect();
        self.write_jobs(&jobs)?;
        Ok(cleared)
    }

    /// Apply a pause, stop, or resume management action to a heartbeat job. `Ok(None)`
    /// when no matching job was found (cancelled and completed jobs are left untouched).
    ///
    /// # Errors
    ///
    /// Returns an error when resuming a job whose stored schedule is invalid.
    pub fn manage_heartbeat(
        &self,
        active_session_id: &str,
        id: &str,
        action: HeartbeatManagementAction,
        now: u64,
    ) -> anyhow::Result<Option<AgentCronJob>> {
        let now_iso = iso_from_millis(now);
        let mut updated = None;
        let jobs: Vec<AgentCronJob> = self
            .read_jobs()
            .into_iter()
            .map(|job| {
                if job.id != id
                    || job.active_session_id != active_session_id
                    || !is_heartbeat_cron_job(&job)
                {
                    return job;
                }
                if matches!(job.status, JobStatus::Cancelled | JobStatus::Completed) {
                    return job;
                }
                let next_job = match action {
                    HeartbeatManagementAction::Pause => AgentCronJob {
                        status: JobStatus::Paused,
                        next_run_at: None,
                        updated_at: now_iso.clone(),
                        ..job
                    },
                    HeartbeatManagementAction::Stop => AgentCronJob {
                        status: JobStatus::Cancelled,
                        next_run_at: None,
                        updated_at: now_iso.clone(),
                        ..job
                    },
                    HeartbeatManagementAction::Resume => AgentCronJob {
                        status: JobStatus::Active,
                        updated_at: now_iso.clone(),
                        ..job
                    },
                };
                updated = Some(next_job.clone());
                next_job
            })
            .collect();
        let Some(mut updated_job) = updated else {
            return Ok(None);
        };
        if action == HeartbeatManagementAction::Resume {
            let Some(next_run_at) = next_run_at_for_schedule(&updated_job.schedule, now)? else {
                anyhow::bail!("Heartbeat schedule must be recurring");
            };
            updated_job.next_run_at = Some(iso_from_millis(next_run_at));
            let jobs: Vec<AgentCronJob> = jobs
                .into_iter()
                .map(|job| {
                    if job.id == updated_job.id {
                        updated_job.clone()
                    } else {
                        job
                    }
                })
                .collect();
            self.write_jobs(&jobs)?;
            return Ok(Some(updated_job));
        }
        self.write_jobs(&jobs)?;
        Ok(Some(updated_job))
    }

    /// Edit one heartbeat job (user or RLM) by job id: instruction, interval,
    /// delivery mode, and label. The job id and status are preserved, and a
    /// paused job keeps its null next run.
    ///
    /// # Errors
    ///
    /// Returns an error when the instruction is blank or the interval does not
    /// parse to a recurring schedule.
    pub fn update_heartbeat_job(
        &self,
        active_session_id: &str,
        id: &str,
        update: &HeartbeatJobUpdate,
        now: u64,
    ) -> anyhow::Result<Option<AgentCronJob>> {
        if let Some(instruction) = &update.instruction {
            anyhow::ensure!(
                !instruction.trim().is_empty(),
                "Heartbeat instruction cannot be empty"
            );
        }
        let parsed_schedule = match &update.interval {
            Some(interval) => {
                let parsed = parse_agent_cron_schedule(interval, now)?;
                anyhow::ensure!(
                    parsed.0.kind != ScheduleKind::Once,
                    "Heartbeat schedule must be recurring"
                );
                Some(parsed)
            }
            None => None,
        };
        let now_iso = iso_from_millis(now);
        let mut updated: Option<AgentCronJob> = None;
        let jobs: Vec<AgentCronJob> = self
            .read_jobs()
            .into_iter()
            .map(|job| {
                if job.id != id
                    || job.active_session_id != active_session_id
                    || !is_heartbeat_cron_job(&job)
                    || matches!(job.status, JobStatus::Cancelled | JobStatus::Completed)
                {
                    return job;
                }
                let mut next = job.clone();
                if let Some(label) = &update.label {
                    next.label = normalize_optional_label(Some(label));
                }
                if let Some(delivery_mode) = update.delivery_mode {
                    next.delivery_mode = Some(delivery_mode);
                }
                if let Some(instruction) = &update.instruction {
                    next.prompt = instruction.trim().to_string();
                }
                if let Some((schedule, next_run)) = &parsed_schedule {
                    next.schedule = schedule.clone();
                    next.next_run_at =
                        (next.status != JobStatus::Paused).then(|| iso_from_millis(*next_run));
                }
                next.updated_at.clone_from(&now_iso);
                updated = Some(next.clone());
                next
            })
            .collect();
        let Some(updated) = updated else {
            // No matching active job: cancelled and completed rows stay untouched.
            return Ok(None);
        };
        self.write_jobs(&jobs)?;
        Ok(Some(updated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cron::store::input;
    use crate::cron::DeliveryMode;

    #[test]
    fn pause_does_not_ack_when_state_lock_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.json");
        let store = AgentCronJobStore::new(path.clone());
        let now = 1_700_000_000_000;
        let heartbeat = store
            .create_heartbeat(&input("continue", "every 5m", now))
            .unwrap();
        let before = std::fs::read(&path).unwrap();
        let held =
            crate::platform::lock_dir::LockDir::acquire(&path, std::time::Duration::from_secs(30))
                .unwrap();
        assert!(store.pause_heartbeat("live-1", now + 1).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
        drop(held);
        assert_eq!(store.get_heartbeat("live-1"), Some(heartbeat));
    }

    #[test]
    fn heartbeat_lifecycle() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = AgentCronJobStore::new(dir.path().join("jobs.json"));
        let now = 1_700_000_000_000;
        let heartbeat = store
            .create_heartbeat(&input("continue the mission", "every 5m", now))
            .unwrap();
        assert_eq!(store.get_heartbeat("live-1").unwrap().id, heartbeat.id);
        let paused = store.pause_heartbeat("live-1", now + 1).unwrap().unwrap();
        assert_eq!(paused.status, JobStatus::Paused);
        assert_eq!(paused.next_run_at, None);
        let resumed = store.resume_heartbeat("live-1", now + 2).unwrap().unwrap();
        assert_eq!(resumed.status, JobStatus::Active);
        assert!(resumed.next_run_at.is_some());
        let second = store
            .create_heartbeat(&input("new instruction", "every 2m", now + 3))
            .unwrap();
        assert_eq!(store.get_heartbeat("live-1").unwrap().id, second.id);
        assert_eq!(store.list().len(), 2);
        let jobs = store.list();
        let cancelled_first = jobs.iter().find(|job| job.id != second.id).unwrap();
        assert_eq!(cancelled_first.status, JobStatus::Cancelled);
        let cleared = store.clear_heartbeat("live-1", now + 4).unwrap().unwrap();
        assert_eq!(cleared.status, JobStatus::Cancelled);
        assert!(store
            .create_heartbeat(&input("nope", "in 10m", now))
            .is_err());
    }

    fn rlm_input(prompt: &str, schedule_text: &str, now: u64) -> crate::cron::store::CreateAgentCronJobInput {
        let mut rlm_input = input(prompt, schedule_text, now);
        rlm_input.source = Some("rlm_heartbeat".to_string());
        rlm_input
    }

    #[test]
    fn heartbeat_update_job_edits_both_sources_and_preserves_the_job() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = AgentCronJobStore::new(dir.path().join("jobs.json"));
        let now = 1_700_000_000_000;
        let heartbeat = store
            .create_heartbeat(&input("continue the mission", "every 5m", now))
            .unwrap();
        let rlm = store.create_rlm_heartbeat(&rlm_input("watch pods", "every 10m", now)).unwrap();

        // The full edit set on the user heartbeat, addressed by job id.
        let edited = store
            .update_heartbeat_job(
                "live-1",
                &heartbeat.id,
                &HeartbeatJobUpdate {
                    instruction: Some("  revised instruction  ".to_string()),
                    interval: Some("every 2m".to_string()),
                    delivery_mode: Some(DeliveryMode::FollowUp),
                    label: Some("  ops  ".to_string()),
                },
                now + 1,
            )
            .unwrap()
            .unwrap();
        assert_eq!(edited.id, heartbeat.id, "the job id is preserved");
        assert_eq!(edited.status, JobStatus::Active, "the status is preserved");
        assert_eq!(edited.prompt, "revised instruction");
        assert_eq!(edited.schedule.expression, "every 2m");
        assert_eq!(edited.delivery_mode, Some(DeliveryMode::FollowUp));
        assert_eq!(edited.label.as_deref(), Some("ops"));
        assert!(edited.next_run_at.is_some());
        // The RLM job in the same session stays untouched by the other job's edit.
        let listed_rlm = store.list_rlm_heartbeats("live-1", false).remove(0);
        assert_eq!(listed_rlm.id, rlm.id);
        assert_eq!(listed_rlm.prompt, "watch pods");

        // The RLM job edits through the same by-job-id path.
        let edited_rlm = store
            .update_heartbeat_job(
                "live-1",
                &rlm.id,
                &HeartbeatJobUpdate {
                    instruction: Some("watch pods closer".to_string()),
                    ..Default::default()
                },
                now + 2,
            )
            .unwrap()
            .unwrap();
        assert_eq!(edited_rlm.id, rlm.id);
        assert_eq!(edited_rlm.prompt, "watch pods closer");
        assert_eq!(edited_rlm.status, JobStatus::Active);

        // A paused job keeps its status and null next run across an interval edit.
        store
            .manage_heartbeat(
                "live-1",
                &heartbeat.id,
                HeartbeatManagementAction::Pause,
                now + 3,
            )
            .unwrap()
            .unwrap();
        let paused_edit = store
            .update_heartbeat_job(
                "live-1",
                &heartbeat.id,
                &HeartbeatJobUpdate {
                    interval: Some("every 7m".to_string()),
                    ..Default::default()
                },
                now + 4,
            )
            .unwrap()
            .unwrap();
        assert_eq!(paused_edit.status, JobStatus::Paused);
        assert_eq!(paused_edit.next_run_at, None);
        assert_eq!(paused_edit.schedule.expression, "every 7m");
    }

    #[test]
    fn heartbeat_update_job_scopes_by_job_id_and_session() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = AgentCronJobStore::new(dir.path().join("jobs.json"));
        let now = 1_700_000_000_000;
        let heartbeat = store
            .create_heartbeat(&input("continue", "every 5m", now))
            .unwrap();
        // A second session's job: a same-session-scoped edit must not reach it, and
        // the wrong session selector must not match the first job either.
        let mut other_input = input("other session", "every 3m", now);
        other_input.active_session_id = "live-2".to_string();
        let other = store.create_heartbeat(&other_input).unwrap();
        let missing = store
            .update_heartbeat_job(
                "live-1",
                &other.id,
                &HeartbeatJobUpdate {
                    label: Some("hijack".to_string()),
                    ..Default::default()
                },
                now + 1,
            )
            .unwrap();
        assert!(missing.is_none(), "the other session's job is not addressable");
        let cross_session = store
            .update_heartbeat_job(
                "live-2",
                &heartbeat.id,
                &HeartbeatJobUpdate {
                    label: Some("hijack".to_string()),
                    ..Default::default()
                },
                now + 1,
            )
            .unwrap();
        assert!(cross_session.is_none(), "the job is not addressable from live-2");
        let untouched = store.get_heartbeat("live-1").unwrap();
        assert_eq!(untouched.label, None);

        // Unknown ids and terminal jobs are not editable.
        assert!(store
            .update_heartbeat_job(
                "live-1",
                "missing-id",
                &HeartbeatJobUpdate {
                    label: Some("nope".to_string()),
                    ..Default::default()
                },
                now + 1,
            )
            .unwrap()
            .is_none());
        store
            .manage_heartbeat(
                "live-1",
                &heartbeat.id,
                HeartbeatManagementAction::Stop,
                now + 2,
            )
            .unwrap()
            .unwrap();
        assert!(store
            .update_heartbeat_job(
                "live-1",
                &heartbeat.id,
                &HeartbeatJobUpdate {
                    label: Some("nope".to_string()),
                    ..Default::default()
                },
                now + 3,
            )
            .unwrap()
            .is_none());
    }

    #[test]
    fn heartbeat_update_job_rejects_bad_fields() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = AgentCronJobStore::new(dir.path().join("jobs.json"));
        let now = 1_700_000_000_000;
        let heartbeat = store
            .create_heartbeat(&input("continue", "every 5m", now))
            .unwrap();
        let blank = store
            .update_heartbeat_job(
                "live-1",
                &heartbeat.id,
                &HeartbeatJobUpdate {
                    instruction: Some("   ".to_string()),
                    ..Default::default()
                },
                now + 1,
            )
            .unwrap_err();
        assert_eq!(blank.to_string(), "Heartbeat instruction cannot be empty");
        let once = store
            .update_heartbeat_job(
                "live-1",
                &heartbeat.id,
                &HeartbeatJobUpdate {
                    interval: Some("in 10m".to_string()),
                    ..Default::default()
                },
                now + 1,
            )
            .unwrap_err();
        assert_eq!(once.to_string(), "Heartbeat schedule must be recurring");
        // The wire parser demands one field and the TS delivery-mode spelling.
        assert!(HeartbeatJobUpdate::from_wire(None, None, None, None).is_err());
        assert_eq!(
            HeartbeatJobUpdate::from_wire(None, None, Some("wrong"), None)
                .unwrap_err()
                .to_string(),
            "Heartbeat delivery mode must be \"steer\" or \"follow_up\""
        );
        let update = HeartbeatJobUpdate::from_wire(None, None, Some("follow_up"), None).unwrap();
        assert!(update.drops_queued_fire());
        assert!(!HeartbeatJobUpdate::from_wire(None, None, None, Some("l"))
            .unwrap()
            .drops_queued_fire());
    }
}
