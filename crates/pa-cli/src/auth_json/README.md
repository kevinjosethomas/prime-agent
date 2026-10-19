# Local auth method selection

The JSONL service keeps protocol version 1. Its ready capabilities now include
`provider_auth_methods`. No daemon, agent session, new credential store, or OAuth
implementation is added.

When that capability is present, each provider catalog row carries a `methodId`:
`oauth` or `api_key`. The selector is stable within a provider and comes from the
native row's auth type, not its display name, order, configured status, or flow.
Prime Inference's API-key credential still uses its existing browser flow.

Clients can send `{ "type": "begin", "id": "flow", "target": "provider",
"key": "anthropic", "action": "login", "methodId": "api_key" }` to select
that exact native method. An unavailable or missing method returns an error;
there is no fallback to another method or interpretation of compound provider ids.
`methodId` is accepted only for provider login, never MCP or provider logout.
Logout remains provider-wide because native storage holds one credential per id.

Compatibility: absent `methodId` retains the first matching native row used by
older v1 clients. Existing fields and outcomes are unchanged. New clients must
not send a selector unless the current helper advertises the capability; an old
helper rejects unknown fields. Additive catalog fields are safe for v1 clients.

Regression source tests exercise both method orders for Anthropic and xAI, exact
selection, unavailable/missing methods, legacy first-row behavior, and bounded
request decoding without credentials, browser opening, or native invocation.

## Prime Agent Traces

Ready capabilities also include `traces_login` and `traces_logout`, still under
protocol version 1. `{ "type": "begin", "id": "flow", "target": "traces",
"key": "prime-agent-traces", "action": "login" }` runs the existing `/traces login`
flow (Prime CLI key reuse, then the browser challenge raced against a pasted key)
through the same event and reply loop as provider login. `logout` removes the
stored `prime-agent-traces` credential. Only `login` and `logout` are valid, and
`methodId` is rejected. The `options` reply adds `"traces": { "configured": bool }`
for the stored credential. Desktop clients use this instead of opening a terminal.

`auth_url.instructions` is always a string; a flow without instructions sends `""`.
