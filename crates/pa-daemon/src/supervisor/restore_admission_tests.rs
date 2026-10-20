use super::*;

#[tokio::test]
async fn restore_child_existing_admission_preserves_ledger_and_completed_display_on_retry() {
    let dir = tempfile::tempdir().unwrap();
    let agent_dir = dir.path().join("agent");
    let sessions = agent_dir.join("sessions");
    let child_dir = sessions.join("child");
    std::fs::create_dir_all(&child_dir).unwrap();
    let parent = sessions.join("parent.jsonl");
    let child = child_dir.join("child.jsonl");
    std::fs::write(&parent, "{\"type\":\"session\",\"id\":\"parent\"}\n").unwrap();
    std::fs::write(&child, "{\"type\":\"session\",\"id\":\"child\"}\n").unwrap();
    let supervisor = Arc::new(
        Supervisor::new(SupervisorOptions {
            socket_path: dir.path().join("daemon.sock"),
            agent_dir: agent_dir.clone(),
        })
        .unwrap(),
    );
    let ledger = supervisor.rlm_spawn_ledger_for(None).await.unwrap();
    ledger
        .append_spawn(&crate::rlm_ledger::RlmSpawnInput {
            child_id: "sub-child".into(),
            parent: parent.to_string_lossy().into_owned(),
            child: child.to_string_lossy().into_owned(),
            depth: 1,
            name: "original".into(),
        })
        .unwrap();
    let display = json!({"type":"rlm_subagent","childId":"sub-child","sessionName":"original","sessionFile":child,"sessionDir":child_dir,"status":"completed","createdAt":42,"prompt":"fixture","model":{"id":"fixture-model"}});
    let display_file = child_dir.join("rlm-subagent.json");
    std::fs::write(&display_file, serde_json::to_vec_pretty(&display).unwrap()).unwrap();
    let ledger_file = crate::rlm_ledger::rlm_ledger_path(&agent_dir, &sessions);
    let ledger_before = std::fs::read(&ledger_file).unwrap();
    let display_before = std::fs::read(&display_file).unwrap();
    let command: DaemonCommand = serde_json::from_value(json!({"type":"create","sessionPath":child,"runtimeMetadata":{"kind":"subagent","rlmChildId":"sub-child","parentSessionFile":parent,"rlmDepth":1}})).unwrap();
    let summary = json!({"sessionFile":child,"sessionName":"different name"});
    for _ in 0..2 {
        supervisor
            .record_rlm_child_admission(&command, &summary)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&ledger_file).unwrap(), ledger_before);
        assert_eq!(std::fs::read(&display_file).unwrap(), display_before);
    }
}
