use super::*;

#[tokio::test]
async fn restore_child_completed_worker_stays_dormant_even_with_old_busy_journal() {
    let dir = tempfile::tempdir().unwrap();
    let supervisor = Arc::new(
        Supervisor::new(crate::supervisor::SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: dir.path().join("agent"),
        })
        .unwrap(),
    );
    let child_file = dir.path().join("child.jsonl");
    std::fs::write(&child_file, "saved child bytes").unwrap();
    let journal = dir.path().join("child.recovery.jsonl");
    crate::journal::append_record(&journal,&json!({"activeSessionId":"passive-child","sessionId":"child","busy":true,"operation":"prompt","recordedAt":"2026-01-01T00:00:00Z"})).unwrap();
    assert!(crate::journal::WorkerRecoveryJournal::read_interrupted(
        &journal
    ));
    let before = std::fs::read(&journal).unwrap();
    let descriptor = pa_types::daemon::DaemonWorkerDescriptor {
        version: 1,
        worker_id: "passive-child".to_string(),
        pid: 4242,
        process_start_id: None,
        socket_path: dir
            .path()
            .join("missing.sock")
            .to_string_lossy()
            .into_owned(),
        recovery_journal_path: journal.to_string_lossy().into_owned(),
        orphan_process_journal_path: None,
        supervisor_socket_path: dir
            .path()
            .join("missing.sock")
            .to_string_lossy()
            .into_owned(),
        authentication_token: "root-token".to_string(),
        worker_instance_id: None,
        root_active_session_id: "passive-child".to_string(),
        owner_client_id: None,
        root_session_id: None,
        session_file: Some(child_file.to_string_lossy().to_string()),
        session_dir: Some(dir.path().to_string_lossy().to_string()),
        telemetry_disabled: Some(true),
        created_at: "t".to_string(),
        updated_at: "t".to_string(),
        lifecycle: DaemonWorkerLifecycle::Ready,
        create_command: pa_types::daemon::DurableDaemonCreateCommand {
            session_path: None,
            no_session: None,
            rest: serde_json::Map::default(),
        },
        consecutive_failures: 0,
        stop_requested_at: None,
        archive_on_stop: None,
        last_failure_at: None,
        last_error: None,
        rest: serde_json::Map::default(),
    };
    for _ in 0..2 {
        let outcome = supervisor
            .adopt_persisted_worker(
                dir.path().join("child.descriptor.json"),
                descriptor.clone(),
                AdoptionBoot::UpdateRoster {
                    kept: Arc::new(std::collections::HashSet::from(["passive-child".into()])),
                    passive: Arc::new(std::collections::HashSet::from(["passive-child".into()])),
                },
            )
            .await;
        assert!(matches!(outcome, AdoptionOutcome::SkippedIdle));
        assert!(supervisor.registry.get("passive-child").await.is_none());
        assert_eq!(std::fs::read(&journal).unwrap(), before);
        assert_eq!(std::fs::read(&child_file).unwrap(), b"saved child bytes");
    }
}
