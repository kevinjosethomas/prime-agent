# pa-ai

Provider APIs and model registry.

## Scope
Provider trait + per-provider streaming clients (anthropic, openai-completions/responses, google, bedrock, mistral, azure, prime-inference), model registry/resolution, usage accounting, stream-failure retry, provider-error shapes (per-SDK user-facing texts, diagnostic error names, connection-error profiles), bedrock transport selection (h2c prior-knowledge HTTP/2 cleartext, h2-preferred TLS ALPN, the http1 AWS_BEDROCK_FORCE_HTTP1/proxy mode), overflow handling, JSON repair parsing, faux provider for tests.

## Non-goals
No agent loop, no tool execution, no session state, no UI. Receives/returns `pa-types` messages.

## Public API
`Provider` trait, `ProviderRegistry`, model lookup/resolution, faux provider. `utils::tls::http_client_builder` lets caller-owned HTTP transports apply the shared extra-CA trust configuration. Per-provider internals are `pub(crate)`.

## Depends on
pa-types (one-way).

The fork adds `systemone::evaluate`, a bounded typed evaluation HTTP client using host-resolved request auth. It does not implement chat completion, agent execution, catalog registration, or Decision API control loops. Shared request/answer DTOs live in pa-types.

`thread_classification::{decode_thread_request, thread_evaluation, thread_scores}`
map bounded public conversation context into independent Clef same-conversation
questions and validate complete per-candidate distributions. This is separate from
workstream outcome relevance. No threshold, state, credentials, or execution is owned
here; CLI consumers resolve auth and own continuity policy. Wire DTOs are pa-types.

Thread evaluation shares only the target and final six causal public messages. Each
question carries its own complete speaker-labelled candidate evidence, omitting
opaque IDs and index references from model input. Concise user/assistant-specific
rules reduce context and coreference burden, at the cost of older recent history;
frozen live-corpus evaluation, rather than policy fixtures, determines accuracy.
