//! Top-level agents as first-class registry members: `rlm.create_session`
//! sessions tracked by the starter like children, with the same messaging,
//! collect, delete and recovery semantics, surviving the starter's exit.
// Stack-resident futures by design on the daemon's hot paths.
#![allow(clippy::large_futures)]
// Style gate only, not correctness.
#![allow(clippy::too_many_lines)]
#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
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
fn spawn_supervisor(socket: &Path, agent_dir: &Path, kernel_python: &Path) -> Daemon {
    let binary = env!("CARGO_BIN_EXE_pa-daemon");
    let child = Command::new(binary)
        .arg("supervisor")
        .arg("--socket")
        .arg(socket)
        .arg("--agent-dir")
        .arg(agent_dir)
        .env("PRIME_AGENT_KERNEL_PYTHON", kernel_python)
        .env("PRIME_AGENT_CODING_AGENT_DIR", agent_dir)
        .env_remove("PRIME_API_KEY")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .env(
            pa_daemon::worker::WORKER_SUPERVISOR_LOST_EXIT_MS_ENV,
            "15000",
        )
        .spawn()
        .expect("spawn pa-daemon supervisor");
    Daemon {
        child,
        socket: socket.to_path_buf(),
    }
}

fn kernel_python() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("PA_E2E_KERNEL_PYTHON") {
        let explicit = PathBuf::from(explicit);
        assert!(
            explicit.exists(),
            "PA_E2E_KERNEL_PYTHON {} not found",
            explicit.display()
        );
        return Some(explicit);
    }
    let candidate = PathBuf::from(std::env::var("HOME").map_or_else(
        |_| "/home/ubuntu/.prime/agent/kernel-venv/bin/python".to_string(),
        |home| format!("{home}/.prime/agent/kernel-venv/bin/python"),
    ));
    if candidate.exists() {
        return Some(candidate);
    }
    eprintln!(
        "kernel python {} not found; skipping the top-level agents e2e",
        candidate.display()
    );
    None
}

fn wait_socket_ready(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if UnixStream::connect(socket).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "supervisor socket never came up");
        std::thread::sleep(Duration::from_millis(20));
    }
}

struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

impl Client {
    fn connect(socket: &Path) -> (Self, Value) {
        let stream = UnixStream::connect(socket).expect("connect supervisor");
        let write_stream = stream.try_clone().expect("clone socket");
        let mut client = Client {
            reader: BufReader::new(stream),
            writer: write_stream,
        };
        let hello = client.read_line();
        (client, hello)
    }

    fn send_command(&mut self, id: &str, command: &Value) {
        let envelope = json!({
            "type": "command",
            "id": id,
            "protocol": { "name": "prime-agent.daemon", "version": 7 },
            "command": command,
        });
        let mut line = serde_json::to_string(&envelope).expect("serialize command");
        line.push('\n');
        self.writer.write_all(line.as_bytes()).expect("send");
        self.writer.flush().expect("flush");
    }

    fn read_line(&mut self) -> Value {
        let mut line = String::new();
        let deadline = Instant::now() + Duration::from_secs(120);
        self.reader
            .get_mut()
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set timeout");
        loop {
            match self.reader.read_line(&mut line) {
                Ok(0) => panic!("supervisor closed the connection"),
                Ok(_) if line.trim().is_empty() => line.clear(),
                Ok(_) => return serde_json::from_str(line.trim()).expect("parse response line"),
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "timed out waiting for supervisor line: {error}"
                    );
                }
            }
        }
    }

    fn read_response(&mut self, id: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(240);
        loop {
            assert!(Instant::now() < deadline, "no response for id {id}");
            let line = self.read_line();
            if line.get("id").and_then(Value::as_str) == Some(id) {
                return line;
            }
        }
    }
}

fn wait_until<T>(
    client: &mut Client,
    budget: Duration,
    mut probe: impl FnMut(&mut Client) -> Option<T>,
) -> T {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(value) = probe(client) {
            return value;
        }
        assert!(Instant::now() < deadline, "condition never became true");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn roster_summaries(client: &mut Client, id: &str) -> Vec<Value> {
    client.send_command(id, &json!({ "type": "list" }));
    let list = client.read_response(id);
    assert_eq!(list["success"], true, "list failed: {list}");
    list["data"]["sessions"]
        .as_array()
        .cloned()
        .expect("sessions array")
}

fn run_turn(client: &mut Client, session_id: &str, message: &str, id: &str) {
    client.send_command(
        id,
        &json!({ "type": "prompt", "activeSessionId": session_id, "message": message }),
    );
    let prompted = client.read_response(id);
    assert_eq!(prompted["success"], true, "prompt failed: {prompted}");
    let idle_id = format!("{id}-idle");
    client.send_command(
        &idle_id,
        &json!({ "type": "wait_for_idle", "activeSessionId": session_id }),
    );
    let idle = client.read_response(&idle_id);
    assert_eq!(idle["success"], true, "wait_for_idle failed: {idle}");
}

fn await_receipt(receipt: &Path) -> String {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        if let Some(content) = std::fs::read_to_string(receipt).ok() {
            return content;
        }
        assert!(
            Instant::now() < deadline,
            "kernel cell receipt never appeared at {}",
            receipt.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn assert_no_error(receipt: &Path) {
    assert!(
        !receipt.exists(),
        "the kernel cell failed: {}",
        std::fs::read_to_string(receipt).unwrap_or_default()
    );
}

/// The created session's cell: report its starter through `agents.parent()`.
fn worker_parent_cell(receipt: &Path, error_receipt: &Path) -> String {
    format!(
        "import json, traceback\ntry:\n    from rlm import agents\n    parent = await agents.parent()\n    await parent.send(\"worker says done\")\n    open({receipt:?}, \"w\").write(json.dumps({{\"relationship\": parent.relationship}}))\nexcept Exception:\n    open({error:?}, \"w\").write(traceback.format_exc())\n    raise",
        receipt = receipt.display().to_string(),
        error = error_receipt.display().to_string(),
    )
}

fn create_cell(receipt: &Path, error_receipt: &Path, name: &str) -> String {
    format!(
        "import json, traceback\ntry:\n    from rlm import agents\n    agent = await agents.spawn(\"run the lane task\", name={name:?}, top_level=True)\n    open({receipt:?}, \"w\").write(json.dumps({{\"child_id\": agent.child_id, \"session_id\": agent.session_id, \"name\": agent.name, \"relationship\": agent.relationship, \"top_level\": agent.top_level, \"has_model\": agent.model is not None}}))\n    print(agent.child_id)\nexcept Exception:\n    open({error:?}, \"w\").write(traceback.format_exc())\n    raise",
        receipt = receipt.display().to_string(),
        error = error_receipt.display().to_string(),
    )
}

fn list_cell(receipt: &Path, error_receipt: &Path) -> String {
    format!(
        "import json, traceback\ntry:\n    from rlm import agents\n    rows = await agents.list()\n    open({receipt:?}, \"w\").write(json.dumps([[r.name, r.relationship, r.top_level, r.child_id is not None] for r in rows]))\nexcept Exception:\n    open({error:?}, \"w\").write(traceback.format_exc())\n    raise",
        receipt = receipt.display().to_string(),
        error = error_receipt.display().to_string(),
    )
}

fn send_cell(receipt: &Path, error_receipt: &Path, name: &str) -> String {
    format!(
        "import json, traceback\ntry:\n    from rlm import agents\n    rows = [r for r in await agents.list() if r.name == {name:?}]\n    receipt = await rows[0].send(\"hello from the starter\")\n    open({receipt:?}, \"w\").write(json.dumps({{\"message_id\": bool(receipt.message_id)}}))\nexcept Exception:\n    open({error:?}, \"w\").write(traceback.format_exc())\n    raise",
        receipt = receipt.display().to_string(),
        error = error_receipt.display().to_string(),
    )
}

fn wait_cell(receipt: &Path, error_receipt: &Path, name: &str) -> String {
    format!(
        "import json, traceback\ntry:\n    from rlm import agents\n    rows = [r for r in await agents.list() if r.name == {name:?}]\n    result = await rows[0].wait()\n    open({receipt:?}, \"w\").write(json.dumps({{\"status\": result.status, \"answer\": result.answer}}))\nexcept Exception:\n    open({error:?}, \"w\").write(traceback.format_exc())\n    raise",
        receipt = receipt.display().to_string(),
        error = error_receipt.display().to_string(),
    )
}

fn stop_cell(receipt: &Path, error_receipt: &Path, name: &str) -> String {
    format!(
        "import json, traceback\ntry:\n    from rlm import agents\n    rows = [r for r in await agents.list() if r.name == {name:?}]\n    await rows[0].stop()\n    remaining = await agents.list()\n    open({receipt:?}, \"w\").write(json.dumps(len(remaining)))\nexcept Exception:\n    open({error:?}, \"w\").write(traceback.format_exc())\n    raise",
        receipt = receipt.display().to_string(),
        error = error_receipt.display().to_string(),
    )
}

/// The starter script: one tool-call cell per turn, each followed by a plain
/// text turn that ends the scripted response.
fn write_parent_script(dir: &Path, cells: &[String]) -> PathBuf {
    let script = dir.join("parent.json");
    let mut responses = Vec::new();
    for (index, cell) in cells.iter().enumerate() {
        responses.push(json!({ "content": [
            { "type": "toolCall", "name": "python_repl", "arguments": { "code": cell } },
        ] }));
        responses.push(json!({ "text": format!("turn {index} done") }));
    }
    std::fs::write(
        &script,
        json!({ "engine": "faux", "responses": responses }).to_string(),
    )
    .expect("write parent script");
    script
}

/// The created session's script: report the starter, then answer the task.
fn write_worker_script(dir: &Path, parent_cell: &str) -> PathBuf {
    let script = dir.join("worker.json");
    std::fs::write(
        &script,
        json!({ "responses": [
            { "content": [
                { "type": "toolCall", "name": "python_repl", "arguments": { "code": parent_cell } },
            ] },
            { "text": "the top-level worker answer" },
        ] })
        .to_string(),
    )
    .expect("write worker script");
    script
}

fn create_parent(
    client: &mut Client,
    dir: &Path,
    parent_script: &Path,
    child_script: &Path,
    session_path: Option<&Path>,
    id: &str,
) -> Value {
    let sessions_dir = dir.join("sessions");
    std::fs::create_dir_all(&sessions_dir).expect("sessions dir");
    let mut create = json!({
        "type": "create",
        "name": "parent",
        "config": {
            "cwd": dir.to_string_lossy(),
            "sessionDir": sessions_dir.to_string_lossy(),
            "script": parent_script.to_string_lossy(),
            "childScript": child_script.to_string_lossy(),
        },
    });
    if let Some(path) = session_path {
        create["sessionPath"] = json!(path.to_string_lossy());
    }
    client.send_command(id, &create);
    let created = client.read_response(id);
    assert_eq!(created["success"], true, "create parent failed: {created}");
    created["data"].clone()
}

/// One supervisor restart, returning the new daemon handle.
fn restart_daemon(daemon: &mut Daemon, socket: &Path, agent_dir: &Path, kernel_python: &Path) {
    client_shutdown(socket);
    let deadline = Instant::now() + Duration::from_secs(30);
    while daemon
        .child
        .try_wait()
        .expect("wait for the supervisor exit")
        .is_none()
    {
        assert!(Instant::now() < deadline, "the supervisor never exited");
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = std::mem::replace(daemon, spawn_supervisor(socket, agent_dir, kernel_python));
    wait_socket_ready(socket);
}

fn client_shutdown(socket: &Path) {
    let (mut client, hello) = Client::connect(socket);
    assert_eq!(hello["type"], "daemon_hello");
    client.send_command("bye", &json!({ "type": "shutdown" }));
}

/// The full first-class flow: create, roster, child-role messaging, the
/// worker's parent view, collect, delete, survival past the starter's exit,
/// and registry recovery with a second delete.
#[test]
fn top_level_sessions_are_first_class_agents() {
    let Some(kernel_python) = kernel_python() else {
        return;
    };
    let dir = tempfile::TempDir::new().expect("temp dir");
    let agent_dir = dir.path().join("agent");
    std::fs::create_dir_all(&agent_dir).expect("agent dir");
    let socket = dir.path().join("supervisor.sock");
    let receipts = dir.path().join("receipts");
    std::fs::create_dir_all(&receipts).expect("receipts dir");
    let receipt = |name: &str| receipts.join(name);
    let error = |name: &str| receipts.join(format!("{name}.error"));

    let parent_seen = receipt("worker-parent.json");
    let worker_script = write_worker_script(
        dir.path(),
        &worker_parent_cell(&parent_seen, &error("worker-parent")),
    );
    let cells = [
        create_cell(&receipt("create.json"), &error("create"), "w1"),
        list_cell(&receipt("list.json"), &error("list")),
        send_cell(&receipt("send.json"), &error("send"), "w1"),
        wait_cell(&receipt("wait.json"), &error("wait"), "w1"),
        stop_cell(&receipt("stop.json"), &error("stop"), "w1"),
        create_cell(&receipt("create2.json"), &error("create2"), "w2"),
        list_cell(&receipt("list2.json"), &error("list2")),
    ];
    let parent_script = write_parent_script(dir.path(), &cells);
    let mut daemon = spawn_supervisor(&socket, &agent_dir, &kernel_python);
    wait_socket_ready(&socket);
    let (mut client, hello) = Client::connect(&socket);
    assert_eq!(hello["type"], "daemon_hello");
    let parent = create_parent(
        &mut client,
        dir.path(),
        &parent_script,
        &worker_script,
        None,
        "c1",
    );
    let parent_id = parent["activeSessionId"]
        .as_str()
        .or_else(|| parent["id"].as_str())
        .expect("parent active session id")
        .to_string();
    let parent_file = parent["sessionFile"]
        .as_str()
        .expect("parent session file")
        .to_string();

    // (1) create: the Agent carries the registry identity.
    run_turn(&mut client, &parent_id, "create the worker", "t1");
    let created: Value =
        serde_json::from_str(&await_receipt(&receipt("create.json"))).expect("create receipt");
    assert_no_error(&error("create"));
    assert_eq!(created["name"], "w1", "the create reply: {created}");
    assert_eq!(created["relationship"], "child");
    assert_eq!(created["top_level"], true);
    assert_eq!(created["has_model"], true);
    let child_id = created["child_id"].as_str().expect("child id").to_string();
    assert!(child_id.starts_with("sub-"));
    let worker_session_id = created["session_id"]
        .as_str()
        .expect("session id")
        .to_string();

    // (2) the roster: everything the session started, top-level flagged.
    run_turn(&mut client, &parent_id, "list the roster", "t2");
    let listed: Value =
        serde_json::from_str(&await_receipt(&receipt("list.json"))).expect("list receipt");
    assert_no_error(&error("list"));
    assert_eq!(
        listed,
        json!([["w1", "child", true, true]]),
        "the started session lists as a top-level child: {listed}"
    );

    // (3) the starter messages it as a child.
    run_turn(&mut client, &parent_id, "message the worker", "t3");
    let send: Value =
        serde_json::from_str(&await_receipt(&receipt("send.json"))).expect("send receipt");
    assert_no_error(&error("send"));
    assert_eq!(send["message_id"], true, "the child send receipt: {send}");

    // (4) the worker reports its starter as its parent.
    let parent_view: Value =
        serde_json::from_str(&await_receipt(&parent_seen)).expect("worker parent receipt");
    assert_no_error(&error("worker-parent"));
    assert_eq!(
        parent_view["relationship"], "parent",
        "the started session sees its starter as the parent"
    );

    // (5) collect returns its final answer.
    run_turn(&mut client, &parent_id, "collect the worker", "t4");
    let waited: Value =
        serde_json::from_str(&await_receipt(&receipt("wait.json"))).expect("wait receipt");
    assert_no_error(&error("wait"));
    assert_eq!(waited["status"], "done", "the collect result: {waited}");
    assert_eq!(waited["answer"], "the top-level worker answer");

    // (6) delete stops the worker; the session file stays resumable.
    run_turn(&mut client, &parent_id, "delete the worker", "t5");
    let stopped: String =
        serde_json::from_str(&await_receipt(&receipt("stop.json"))).expect("stop receipt");
    assert_no_error(&error("stop"));
    assert_eq!(stopped, "0", "the registry empties after the delete");
    let worker_file = dir
        .path()
        .join("sessions")
        .join(format!("{worker_session_id}.jsonl"));
    assert!(
        worker_file.is_file(),
        "the deleted top-level session keeps its file"
    );
    let worker_content = std::fs::read_to_string(&worker_file).expect("worker session file");
    assert!(
        !worker_content.contains("\"archived\""),
        "a deleted top-level session stays resumable: {worker_content}"
    );

    // (7) the starter's exit never stops a started session.
    run_turn(&mut client, &parent_id, "create the survivor", "t6");
    let created2: Value =
        serde_json::from_str(&await_receipt(&receipt("create2.json"))).expect("create2 receipt");
    assert_no_error(&error("create2"));
    let worker2_session_id = created2["session_id"]
        .as_str()
        .expect("session id 2")
        .to_string();
    run_turn(&mut client, &parent_id, "list the roster again", "t7");
    let listed2: Value =
        serde_json::from_str(&await_receipt(&receipt("list2.json"))).expect("list2 receipt");
    assert_eq!(
        listed2,
        json!([["w2", "child", true, true]]),
        "the second started session lists the same way: {listed2}"
    );
    client.send_command(
        "n1",
        &json!({ "type": "new_session", "activeSessionId": parent_id }),
    );
    let replaced = client.read_response("n1");
    assert_eq!(replaced["success"], true, "new_session failed: {replaced}");
    let survivor = wait_until(&mut client, Duration::from_secs(30), |client| {
        roster_summaries(client, "l1")
            .into_iter()
            .find(|summary| summary["sessionName"] == json!("w2"))
    });
    assert!(
        survivor["activeSessionId"].is_string(),
        "the started session outlives its starter: {survivor}"
    );

    // (8) registry recovery after a daemon restart: the resumed starter
    // relists its started session from the ledger, then deletes it.
    restart_daemon(&mut daemon, &socket, &agent_dir, &kernel_python);
    let (mut client, hello) = Client::connect(&socket);
    assert_eq!(hello["type"], "daemon_hello");
    let third_cells = [
        list_cell(&receipt("list3.json"), &error("list3")),
        stop_cell(&receipt("stop3.json"), &error("stop3"), "w2"),
    ];
    let parent_script3 = write_parent_script(dir.path(), &third_cells);
    let parent = create_parent(
        &mut client,
        dir.path(),
        &parent_script3,
        &worker_script,
        Some(Path::new(&parent_file)),
        "c3",
    );
    let parent_id = parent["activeSessionId"]
        .as_str()
        .or_else(|| parent["id"].as_str())
        .expect("parent active session id")
        .to_string();
    run_turn(&mut client, &parent_id, "relist the roster", "t8");
    let listed3: Value =
        serde_json::from_str(&await_receipt(&receipt("list3.json"))).expect("list3 receipt");
    assert_no_error(&error("list3"));
    assert_eq!(
        listed3,
        json!([["w2", "child", true, true]]),
        "the restarted starter relists its started session from the ledger: {listed3}"
    );
    run_turn(&mut client, &parent_id, "delete the survivor", "t9");
    let stopped: String =
        serde_json::from_str(&await_receipt(&receipt("stop3.json"))).expect("stop3 receipt");
    assert_no_error(&error("stop3"));
    assert_eq!(stopped, "0", "the registry empties after the delete");
    let worker2_file = dir
        .path()
        .join("sessions")
        .join(format!("{worker2_session_id}.jsonl"));
    assert!(
        worker2_file.is_file(),
        "the deleted top-level session keeps its file"
    );
    let content = std::fs::read_to_string(&worker2_file).expect("worker2 session file");
    assert!(
        !content.contains("\"archived\""),
        "a deleted top-level session stays resumable: {content}"
    );
}
