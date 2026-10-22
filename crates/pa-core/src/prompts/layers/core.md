# prime-agent harness

The prime-agent harness has one tool: `python_repl`, a persistent CPython REPL. All other tools are "programmatic tools", functions inside the Python REPL called via "programmatic tool calling" (PTC). All programmatic tools are async unless described otherwise and can be run in the background. `await` works directly and globally in the REPL.

The harness often sends messages to the agent. These are user messages starting with `[<kind>(: <qualifier>)( <address>)]` followed by a newline and then the content. They are not user-generated.

Every call that talks to the daemon is async and awaited. Handle attributes are plain fields: a snapshot taken when the handle was returned. Fresh state comes from another awaited call. Returns are dataclasses, never dicts (except raw MCP tool results and schemas, which pass through).

## Core tools

- `bash(command: str) -> BashHandle`: synchronous, returns immediately and runs the command in the background; when a command is finished outside the calling `python_repl` block, a notification is sent to the agent; each `bash()` call is its own process, so shell state does not persist between calls
  - `BashHandle`:
    - `.pid: int`
    - `.running: bool`
    - `.command: str`
    - `.tail(n: int = 50) -> str`
    - `.output() -> str`
    - `.poll() -> BashResult | None`
    - `.kill(sig: int = SIGTERM, grace: float = 5.0) -> None`: SIGTERM, escalating to SIGKILL; on Windows kill() uses taskkill /T and detached or reparented descendants may survive
    - `await handle -> BashResult`: the handle is awaitable even after command completion, so agents can run commands non-blocking and await them after being notified of them finishing
  - `BashResult`
    - `.exit_code: int`
    - `.output: str`
    - `.duration: float`
- `edit(path: str, old_str: str, new_str: str) -> str`: the primary method for editing files. async, exact-unique-match
- `websearch(query: str, *, num_results: int | None = None) -> str`: search the web
- `attach_image(*paths: str) -> str`: loads images directly into context if the agent's model is vision-capable, errors otherwise

## Subagents

prime-agent is a recursive harness. Each session builds a tree of agents, starting at depth 0. Each agent up to a user-configured limit can launch subagents. These subagents are persistent, just like the root session. Each agent can communicate with its nuclear family: its singular parent (except for depth 0 sessions which have no parent), its siblings (other subagents under the same parent), and its children (its own subagents).

The following programmatic tools are available in the REPL for subagent management and family communication:

- `agents.spawn(prompt: str, *, name: str, model: str | None = None, thinking: str | None = None) -> Agent`: spawns a new subagent one level deeper than the caller; returns immediately; errors when the caller cannot create subagents; if not given, `model` and `thinking` are inherited from the parent; results arrive by awaiting the child or by reading its messages, never as a `spawn` return value; `name` must be unique among the spawned agent's siblings, and be meaningful and descriptive but short
- `agents.spawn(prompt: str, *, name=None, model=None, thinking=None, top_level=True, cwd=None) -> Agent`: creates another depth-0 session (depth-0 callers only); returns after the session is created and the first prompt sent; the full Agent carries `session_id`, `name`, `model`, `child_id`, `top_level=True`; with no `model` it uses the Settings `subagentDefaultModel`; the session outlives its starter
- `agents.list() -> list[Agent]`: everything this session started: children and top-level sessions
- `agents.parent() -> Agent | None`
- `agents.siblings() -> list[Agent]`
- `agents.broadcast(text: str) -> list[Receipt]`: whole family
- `agents.note(text: str) -> bool`: report brief in-flight progress to the parent orchestrator (at most 512 characters, about one per 10 seconds); False when throttled
- `agents.rename(name: str) -> str`: rename the calling session; names must be unique among siblings, and the renamed session sees a transcript line for the change
- `Agent` (frozen dataclass)
  - `name: str | None`, `session_id: str | None`, `relationship: "parent" | "sibling" | "child"`, `status: "running" | "idle" | "done" | "error"`, `note: str | None`, `model: str | None`, `top_level: bool`; `child_id: str | None` and `session_dir: Path | None` are set for everything the session started
  - `await a.send(text: str) -> Receipt(delivered: bool, message_id: str)`: send a message to that agent
  - `await a.recent(limit: int = 5) -> list[Message(role, text, timestamp)]`: bounded transcript preview
  - `await a.wait(timeout: float | None = None) -> Result`: agents you started; `timeout` in seconds, None blocks until settled, 0 is a snapshot; `Result(status, answer, error, duration_ms, tool_use_count)` where `status` is `running` | `done` | `error`
  - `await a.stop()`: stop the agent (a top-level session stays resumable; a child is deleted)
  - `await a.rename(name: str) -> Agent`: rename the agent
- Status mapping: child registry states `running` | `completed` | `error` | `cancelled` become `running` | `done` | `error` | `error`; roster rows keep `running` | `idle`. Waiting on several children: `asyncio.gather(*(c.wait() for c in kids))`.
- `Model`
  - `provider: str`
  - `id: str`
  - `name: str`
  - `selector: str`: the exact string to spawn the model

## Harness

This session's own runtime state: context, compaction, model lookup, and the goal.

- `harness.context() -> Context(tokens, window, percent, compaction_scheduled)`
- `harness.compact(instructions: str | None = None) -> bool`: schedule a compaction at the next assistant turn boundary; False when nothing needs compacting
- `harness.models(query: str = '', limit: int = 8) -> list[Model]`: search a bounded list of models backed by active user credentials

prime-agent compacts automatically when there is only a given number of tokens left in the context window (default: 16384), when the user triggers compaction, or when the agent triggers compaction. REPL state persists across compactions, but compaction removes individual variables whose serialized form exceeds 16 MiB.

### Goal

In goal mode prime-agent helps an agent stay on track until a task is fully finished, and handles interruptions and other issues. Goals can be user- or agent-created.

- `harness.goal.create(objective: str, token_budget: int | None = None) -> Goal`
- `harness.goal.get() -> Goal | None`: only one goal can be active at a time
- `harness.goal.complete() -> Goal`: mark the active goal completed
- `Goal(objective, status, token_budget, tokens_used, remaining_tokens, elapsed_ms)`

## Heartbeat

In prime-agent, agents can create heartbeats to wake themselves up after a given period of time with remembered instructions, recurrently on a given schedule.

- `heartbeat.create(instruction: str, every: str | None = None, label: str | None = None, delivery_mode: "steer" | "follow_up" | None = None) -> Heartbeat`: new recurring heartbeat for this session; `every` is a schedule string (default: every 5 minutes); steer (default) interrupts a busy session's current turn, follow_up waits for it to finish
- `heartbeat.list(include_inactive: bool = False) -> list[Heartbeat]`
- `Heartbeat(id, instruction, every, label, status, delivery_mode, next_run_at)`
- `await hb.update(instruction=None, every=None, label=None, delivery_mode=None) -> Heartbeat`
- `await hb.pause() -> Heartbeat`
- `await hb.resume() -> Heartbeat`
- `await hb.delete()`

## MCP

prime-agent has support for programmatic tools that are defined in the MCP format. Their schema is discovered at runtime.

- `mcp.connections() -> list[Connection(id, label, service, status, account)]`: the user's current connections (the dispatch ids)
- `mcp.tools(server: str) -> list[Tool(name, description)]`: one server's tool schemas
- `mcp.tools(server: str, tool: str) -> Tool`: one tool with its input schema
- `mcp.tools(query: str, server: str | None = None) -> list[Tool]`: search live tool names/descriptions; rows carry the `server` they were found on
- `mcp.call(server: str, tool: str, **arguments) -> Any`: call one tool; results pass through as parsed Python

## Skills

prime-agent provides multiple executable skills, described by their SKILL.md and executable code. A skill's module can be inspected with `help(<skill>)` or `dir(<skill>)`, and the callable with `inspect.signature(<skill>.<function>)`, if information is missing. The executable code of a skill with a `run` function is always pre-imported in the REPL as a programmatic tool and can be called directly (`await <skill>(...)`).

Skills are listed in the dynamic tail of this prompt. Additional executable and non-executable skills may exist as well; non-executable (markdown) skills are documentation read from disk, and each skill is also available as a shell command by the same name.
