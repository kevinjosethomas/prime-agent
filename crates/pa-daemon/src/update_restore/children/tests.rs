use super::*;
use crate::rlm_ledger::{write_rlm_subagent_display, RlmSubagentDisplayEntry};

fn fixture() -> (tempfile::TempDir, UpdateRoster, RlmLedgerEdge) {
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().join("parent.jsonl");
    let child_dir = dir.path().join("child");
    std::fs::create_dir_all(&child_dir).unwrap();
    let child = child_dir.join("child.jsonl");
    std::fs::write(&parent, "{\"type\":\"session\",\"id\":\"parent\"}\n").unwrap();
    std::fs::write(&child, "{\"type\":\"session\",\"id\":\"child\"}\n").unwrap();
    let edge = RlmLedgerEdge {
        child_id: "sub-child".into(),
        parent: parent.to_string_lossy().into_owned(),
        child: child.to_string_lossy().into_owned(),
        depth: 1,
        name: "saved child".into(),
        deleted: None,
        deleted_usage: None,
    };
    write_rlm_subagent_display(&RlmSubagentDisplayEntry {
        type_tag: "rlm_subagent".into(),
        child_id: edge.child_id.clone(),
        session_name: edge.name.clone(),
        session_dir: child_dir.to_string_lossy().into_owned(),
        session_file: edge.child.clone(),
        rlm_parent_node_id: None,
        prompt: None,
        spawn_code: None,
        model: None,
        status: "completed".into(),
        created_at: 42,
    })
    .unwrap();
    let roster: UpdateRoster = serde_json::from_value(json!({
        "format_version":1, "update_id":"test-update", "socket_path":"/test/daemon.sock",
        "created_at":"2026-01-01T00:00:00Z", "supervisor":{"pid":1,"generation":"test"},
        "binary":{"from_version":"old","to_version":"new"},
        "sessions":[{
            "session_id":"child", "active_session_id":"active-child", "session_file":child,
            "kind":"subagent", "rlm_depth":1, "cwd":"/work",
            "runtime_config":{"create":{"sessionPath":child,"rlmChildId":"sub-child", "runtimeMetadata":{"kind":"subagent","rlmChildId":"sub-child","createdAt":42}}},
            "queue":{"next_turn":[],"actions":{"actions":{"activeInputOwnership":{"state":"unknown","inputIds":[],"replyToMessageIds":[]},"queuedCount":0,"steering":[],"followUps":[]},"steering":[],"followUps":[]}},
            "in_flight":{"streaming":false,"compacting":false,"bash_running":false,"rlm_children":false,"retrying":false,"prompt_in_flight":false},
            "should_resume":false
        }],
        "workers":[{"worker_id":"worker-child","sessions":["child"],"launch_env":{}}],
        "subagents":[{"child_id":"sub-child","session_id":"child","parent_session_id":"parent","name":"saved child","status":"completed","depth":1,"session_file":child,"display_file":child_dir.join("rlm-subagent.json")}]
    })).unwrap();
    (dir, roster, edge)
}

#[test]
fn restore_child_completed_stays_passive_without_touching_saved_files() {
    let (_dir, mut roster, edge) = fixture();
    let display = Path::new(&edge.child)
        .parent()
        .unwrap()
        .join("rlm-subagent.json");
    let before = std::fs::read(&display).unwrap();
    let topology = roster.subagents.clone();
    assert_eq!(
        prepare_roster(&mut roster),
        HashSet::from(["worker-child".into()])
    );
    assert!(roster.sessions.is_empty());
    assert!(roster.workers.is_empty());
    assert_eq!(roster.subagents, topology);
    assert_eq!(std::fs::read(&display).unwrap(), before);
    assert!(prepare_roster(&mut roster).is_empty());
}

#[test]
fn restore_child_busy_or_queued_completion_never_discards_work() {
    let (_dir, original, _edge) = fixture();
    for flag in [
        "streaming",
        "compacting",
        "bash_running",
        "rlm_children",
        "retrying",
        "prompt_in_flight",
    ] {
        let mut roster = original.clone();
        let mut flags = serde_json::to_value(roster.sessions[0].in_flight).unwrap();
        flags[flag] = json!(true);
        roster.sessions[0].in_flight = serde_json::from_value(flags).unwrap();
        let before = roster.clone();
        assert!(prepare_roster(&mut roster).is_empty(), "{flag}");
        assert_eq!(roster, before);
    }
    for actions in [
        json!({"steering":["human input"]}),
        json!({"followUps":["later"]}),
        json!({"actions":{"queuedCount":1,"steering":[],"followUps":[]}}),
        json!({"futureQueueFormat":["unknown input"]}),
        json!({"activeInputOwnership":{"state":"active","inputIds":["input-1"]}}),
    ] {
        let mut roster = original.clone();
        roster.sessions[0].queue.actions = actions;
        let before = roster.clone();
        assert!(prepare_roster(&mut roster).is_empty());
        assert_eq!(roster, before);
    }
    let mut roster = original.clone();
    roster.sessions[0].should_resume = true;
    let before = roster.clone();
    assert!(prepare_roster(&mut roster).is_empty());
    assert_eq!(roster, before);
}

#[test]
fn restore_child_nonterminal_and_unverified_displays_are_not_passivated() {
    let (_dir, original, edge) = fixture();
    for change in ["running", "wrong-child", "wrong-file", "missing"] {
        let mut roster = original.clone();
        let path = Path::new(&edge.child)
            .parent()
            .unwrap()
            .join("rlm-subagent.json");
        let mut display: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let before = std::fs::read(&path).unwrap();
        match change {
            "running" => display["status"] = json!("running"),
            "wrong-child" => display["childId"] = json!("another-child"),
            "wrong-file" => display["sessionFile"] = json!("/other.jsonl"),
            "missing" => {}
            _ => unreachable!(),
        }
        if change == "missing" {
            std::fs::remove_file(&path).unwrap();
        } else {
            std::fs::write(&path, serde_json::to_vec(&display).unwrap()).unwrap();
        }
        let expected = roster.clone();
        assert!(prepare_roster(&mut roster).is_empty(), "{change}");
        assert_eq!(roster, expected);
        std::fs::write(&path, before).unwrap();
    }
}

#[test]
fn restore_child_mixed_worker_keeps_its_unfinished_session_and_order() {
    let (_dir, mut roster, _edge) = fixture();
    let mut unfinished = roster.sessions[0].clone();
    unfinished.session_id = "unfinished".into();
    unfinished.should_resume = true;
    roster.sessions.push(unfinished.clone());
    roster.workers[0].sessions.push("unfinished".into());
    assert!(prepare_roster(&mut roster).is_empty());
    assert_eq!(roster.sessions, vec![unfinished]);
    assert_eq!(roster.workers[0].sessions, vec!["unfinished"]);
}

#[test]
fn restore_child_verified_parent_hydration_is_idempotent_and_keeps_queue_and_metadata() {
    let (_dir, roster, edge) = fixture();
    let row = &roster.sessions[0];
    let before = row.clone();
    let mut payload = row.runtime_config["create"].clone();
    let mut expected = payload.clone();
    expected["runtimeMetadata"]["parentSessionFile"] = json!(edge.parent);
    expected["runtimeMetadata"]["rlmDepth"] = json!(1);
    hydrate_create(&mut payload, row, std::slice::from_ref(&edge)).unwrap();
    assert_eq!(payload, expected);
    hydrate_create(&mut payload, row, std::slice::from_ref(&edge)).unwrap();
    assert_eq!(payload, expected);
    assert_eq!(*row, before);
}

#[test]
fn restore_child_missing_ambiguous_deleted_and_conflicting_parents_fail_closed() {
    let (_dir, roster, edge) = fixture();
    let row = &roster.sessions[0];
    let original = row.runtime_config["create"].clone();
    let mut wrong = edge.clone();
    wrong.parent = "/missing-parent.jsonl".into();
    let mut deleted = edge.clone();
    deleted.deleted = Some(crate::rlm_ledger::RlmLedgerDeleteReason::User);
    for edges in [
        vec![],
        vec![edge.clone(), edge.clone()],
        vec![wrong],
        vec![deleted],
    ] {
        let mut payload = original.clone();
        assert!(hydrate_create(&mut payload, row, &edges).is_err());
        assert_eq!(payload, original);
    }
    for (key, value) in [
        ("parentSessionFile", json!("/wrong-parent.jsonl")),
        ("rlmDepth", json!(9)),
        ("rlmChildId", json!("wrong-child")),
    ] {
        let mut payload = original.clone();
        payload["runtimeMetadata"][key] = value;
        let before = payload.clone();
        assert!(hydrate_create(&mut payload, row, std::slice::from_ref(&edge)).is_err());
        assert_eq!(payload, before);
    }
}

#[test]
fn restore_child_nested_parent_can_stay_dormant_while_its_child_recovers() {
    let (dir, mut roster, mut edge) = fixture();
    let mut row = roster.sessions[0].clone();
    let nested = dir.path().join("grandchild.jsonl");
    std::fs::write(&nested, "{\"type\":\"session\",\"id\":\"grandchild\"}\n").unwrap();
    edge.parent = edge.child.clone();
    edge.child = nested.to_string_lossy().into_owned();
    edge.child_id = "sub-grandchild".into();
    row.session_id = "grandchild".into();
    row.session_file = edge.child.clone();
    row.active_session_id = "active-grandchild".into();
    row.runtime_config["create"]["sessionPath"] = json!(nested);
    row.runtime_config["create"]["rlmChildId"] = json!(edge.child_id);
    row.runtime_config["create"]["runtimeMetadata"]["rlmChildId"] = json!(edge.child_id);
    row.rlm_depth = 2;
    edge.depth = 2;
    row.should_resume = true;
    let mut payload = row.runtime_config["create"].clone();
    hydrate_create(&mut payload, &row, std::slice::from_ref(&edge)).unwrap();
    assert_eq!(payload["runtimeMetadata"]["rlmDepth"], json!(2));
    assert_eq!(
        payload["runtimeMetadata"]["parentSessionFile"],
        json!(edge.parent)
    );
    roster.sessions.push(row.clone());
    roster.workers[0].sessions.push(row.session_id.clone());
    prepare_roster(&mut roster);
    assert_eq!(roster.sessions, vec![row]);
}
