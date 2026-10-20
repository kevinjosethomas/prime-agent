//! The `heartbeat_update_job` daemon protocol e2e: the real supervisor and a
//! real session worker over the socket, with the faux engine answering turns.
//! The edit command must reach the live owner's store, preserve the job id and
//! status, and answer the TS-style errors for missing jobs and empty payloads.
// Fn length is a style gate, not correctness (the readoption e2e convention).
#![allow(clippy::too_many_lines)]
#![allow(clippy::large_futures)]
#![allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

struct Daemon {
    child: Child,
    #[allow(dead_code)]
    socket: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[allow(clippy::zombie_processes)]
fn spawn_daemon(socket: &Path, agent_dir: &Path) -> Daemon {
    std::fs::create_dir_all(agent_dir).expect("agent dir");
    let mut command = Command::new(env!("CARGO_BIN_EXE_pa-daemon"));
    command
        .arg("supervisor")
        .arg("--socket")
        .arg(socket)
        .arg("--agent-dir")
        .arg(agent_dir)
        .env_remove("PRIME_API_KEY")
        .env("PRIME_AGENT_CODING_AGENT_DIR", agent_dir);
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn pa-daemon supervisor");
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if socket.exists() {
            return Daemon {
                child,
                socket: socket.to_path_buf(),
            };
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("supervisor socket never appeared");
}

struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

impl Client {
    fn connect(socket: &Path) -> (Client, Value) {
        let stream = UnixStream::connect(socket).expect("connect");
        let writer = stream.try_clone().expect("clone");
        let mut client = Client {
            reader: BufReader::new(stream),
            writer,
        };
        let hello = client.read_line();
        assert_eq!(hello["type"], "daemon_hello");
        (client, hello)
    }

    fn read_line(&mut self) -> Value {
        let mut line = String::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        self.reader
            .get_mut()
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("timeout");
        loop {
            match self.reader.read_line(&mut line) {
                Ok(0) => panic!("supervisor closed the connection"),
                Ok(_) if line.trim().is_empty() => line.clear(),
                Ok(_) => return serde_json::from_str(line.trim()).expect("parse line"),
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for supervisor line: {error}"
                    );
                }
            }
        }
    }

    fn request(&mut self, id: &str, command: &Value) -> Value {
        let envelope = json!({
            "type": "command",
            "id": id,
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "command": command,
        });
        let mut line = serde_json::to_string(&envelope).expect("serialize");
        line.push('\n');
        self.writer
            .write_all(line.as_bytes())
            .unwrap_or_else(|error| panic!("write command {id}: {error}"));
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            assert!(Instant::now() < deadline, "no response for id {id}");
            let line = self.read_line();
            if line.get("id").and_then(Value::as_str) == Some(id) {
                return line;
            }
        }
    }
}

/// The `heartbeat_update_job` protocol: the capability is advertised, the live
/// worker's heartbeat is edited by job id (id and status preserved), and the
/// unknown-job and empty-payload errors match the protocol family.
#[test]
fn heartbeat_update_job_edits_the_live_heartbeat_over_the_socket() {
    let root = tempfile::TempDir::new().expect("temp dir");
    let dir = root.path().to_path_buf();
    let agent_dir = dir.join("agent");
    let socket = dir.join("edit.sock");
    let script = dir.join("faux.json");
    std::fs::write(
        &script,
        json!({
            "engine": "faux",
            "responses": [
                { "text": "the lane works on" },
                { "text": "the lane works on" },
            ]
        })
        .to_string(),
    )
    .expect("write faux script");

    let _supervisor = spawn_daemon(&socket, &agent_dir);
    let (mut client, hello) = Client::connect(&socket);
    assert!(
        hello["serverCapabilities"]
            .as_array()
            .expect("server capabilities")
            .iter()
            .any(|capability| capability == "heartbeat_job_edit"),
        "the daemon advertises heartbeat_job_edit: {hello}"
    );

    let sessions = agent_dir.join("sessions");
    std::fs::create_dir_all(&sessions).expect("sessions dir");
    let created = client.request(
        "c1",
        &json!({
            "type": "create",
            "name": "edit-lane",
            "config": {
                "cwd": dir.to_string_lossy(),
                "sessionDir": sessions.to_string_lossy(),
                "script": script.to_string_lossy(),
            },
        }),
    );
    assert_eq!(created["success"], true, "create failed: {created}");
    let active_id = created["data"]["id"]
        .as_str()
        .or_else(|| created["data"]["activeSessionId"].as_str())
        .expect("active id")
        .to_string();

    let heartbeat = client.request(
        "hb",
        &json!({
            "type": "heartbeat_set",
            "activeSessionId": active_id,
            "schedule": "every 5m",
            "prompt": "liveness ping",
        }),
    );
    assert_eq!(
        heartbeat["success"], true,
        "heartbeat_set failed: {heartbeat}"
    );
    let job_id = heartbeat["data"]["heartbeat"]["id"]
        .as_str()
        .expect("job id")
        .to_string();

    // THE EDIT: the full field set over the socket, routed to the live owner.
    let edited = client.request(
        "edit",
        &json!({
            "type": "heartbeat_update_job",
            "activeSessionId": active_id,
            "jobId": job_id,
            "instruction": "  revised liveness ping  ",
            "interval": "every 2m",
            "deliveryMode": "follow_up",
            "label": "ops",
        }),
    );
    assert_eq!(
        edited["success"], true,
        "heartbeat_update_job failed: {edited}"
    );
    let edited_job = &edited["data"]["heartbeat"];
    assert_eq!(edited_job["id"], job_id.as_str(), "the job id is preserved");
    assert_eq!(edited_job["status"], "active", "the status is preserved");
    assert_eq!(edited_job["prompt"], "revised liveness ping");
    assert_eq!(edited_job["schedule"]["expression"], "every 2m");
    assert_eq!(edited_job["deliveryMode"], "follow_up");
    assert_eq!(edited_job["label"], "ops");
    assert!(
        edited_job["nextRunAt"].is_string(),
        "an active job keeps a next run"
    );

    // The store answers the edit: a fresh read serves the edited row.
    let reread = client.request(
        "get",
        &json!({ "type": "heartbeat_get", "activeSessionId": active_id }),
    );
    assert_eq!(reread["success"], true, "heartbeat_get failed: {reread}");
    assert_eq!(reread["data"]["heartbeat"]["id"], job_id.as_str());
    assert_eq!(
        reread["data"]["heartbeat"]["prompt"],
        "revised liveness ping"
    );

    // A label-only edit keeps a paused job paused with a null next run.
    let paused = client.request(
        "pause",
        &json!({
            "type": "heartbeat_manage", "activeSessionId": active_id,
            "jobId": job_id, "action": "pause",
        }),
    );
    assert_eq!(paused["success"], true, "heartbeat_manage failed: {paused}");
    let relabeled = client.request(
        "rel",
        &json!({
            "type": "heartbeat_update_job",
            "activeSessionId": active_id,
            "jobId": job_id,
            "label": "quiet ops",
        }),
    );
    assert_eq!(
        relabeled["success"], true,
        "label-only edit failed: {relabeled}"
    );
    assert_eq!(relabeled["data"]["heartbeat"]["status"], "paused");
    assert_eq!(relabeled["data"]["heartbeat"]["label"], "quiet ops");
    assert_eq!(relabeled["data"]["heartbeat"]["nextRunAt"], Value::Null);

    // The unknown-job and empty-payload errors.
    let missing = client.request(
        "miss",
        &json!({
            "type": "heartbeat_update_job",
            "activeSessionId": active_id,
            "jobId": "no-such-job",
            "instruction": "x",
        }),
    );
    assert_eq!(missing["success"], false);
    assert_eq!(
        missing["error"], "No active heartbeat found: no-such-job",
        "the missing-job error: {missing}"
    );
    let empty = client.request(
        "empty",
        &json!({
            "type": "heartbeat_update_job",
            "activeSessionId": active_id,
            "jobId": job_id,
        }),
    );
    assert_eq!(empty["success"], false);
    assert_eq!(
        empty["error"], "heartbeat_update_job requires at least one field to update",
        "the empty-payload error: {empty}"
    );

    client.request(
        "k1",
        &json!({ "type": "kill", "activeSessionId": active_id }),
    );
}
