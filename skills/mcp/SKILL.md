---
name: mcp
description: Use external MCP services generically from Python - inspect the user's connections, discover live tool schemas, and call tools on any connection (Notion, Linear, Slack, and the rest of the catalog) without per-service packages.
---

# MCP Services

Every external service reachable over MCP uses the same pre-imported `mcp`
module. Adding a service is data (a catalog entry plus a user login), never a
new Python package, and there are no per-service skills to read.

## Inventory: what can I use?

```python
# The user's actual connections. Connection.id is the dispatch id everywhere below.
connections = await mcp.connections()
```

`Connection` rows carry `id`, `label`, `service`, `status`, and `account`.
The supported-service catalog itself is browsable in the client (`/plugins` or
`/mcp`), not through Python; the module only exposes what the user has
connected. `connected` means a verified MCP handshake, not just stored
credentials. If a service you need is absent, say so honestly instead of
promising one-click setup.

## Recommend, never connect

If a task would benefit from an unconnected service, name the real catalog
entry (e.g. "Connecting Notion would let me search your workspace directly")
and tell the user to open `/plugins` (or run `/mcp login <service>`) to connect.
Recommendations never install anything, open a browser, use credentials, or
request extra scopes. There is no agent-initiated connect call; if the user
connects mid-task, the connection activates on the next call without a
reload.

## Discover, then call

```python
# Live tool metadata is cached per connection; schemas come from the server.
tools = await mcp.tools("notion")                        # every tool: Tool(name, description)
tool = await mcp.tools("notion", tool="notion-search")   # one tool with input_schema
tools = await mcp.tools(query="search workspace documents")  # across connected servers

result = await mcp.call("notion", "notion-search", query="roadmap")
```

- Discover before calling: don't assume tool names or arguments; read the
  `input_schema` with `mcp.tools(server, tool=...)` and construct matching
  arguments.
- `mcp.tools(server)` returns the complete inventory or raises — never a
  silent partial list. Returned schemas are isolated copies; mutate them
  freely.
- `mcp.tools(query=...)` without a server searches the connections the host
  reports as connected and reports its scope: `searched`, `unavailable`
  (per-connection failures as fixed, redaction-safe summaries), and
  `truncated` (narrow the query or pick a server when it is set — the result
  never claims to be exhaustive).
- Prefer narrow searches over dumping all schemas into context.
- Results are already-parsed Python (a `dict` for structured output, otherwise
  a string). No `json.loads` needed.

## Errors and permissions

- `PermissionError` — the tool is excluded by the user's `enabledTools` /
  `disabledTools` policy. Don't retry; tell the user.
- `KeyError` — the tool or connection doesn't exist. Re-check with
  `mcp.tools` / `mcp.connections`; tools can change across sessions.
- `McpDiscoveryError` — the server's tool listing couldn't complete honestly
  (broken pagination). No partial inventory is published; re-run, and ask
  the user to reconnect if it persists.
- `RuntimeError` — credentials are not available (`McpCredentialsUnavailable`:
  ask the user to connect via `/plugins` or `/mcp login <service>`, never via
  environment variables), a refresh failed, or the host bridge is unavailable.
- `McpToolError` — the server flagged the call as an error. Surface the
  message; don't blindly retry a write that may have already happened.

Credentials never appear in inventory results, tool schemas, or errors.
