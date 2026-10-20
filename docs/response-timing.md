# Daemon response timing

`get_state`'s session summary carries a `responseTiming` block — measured,
per-assistant-response timing folded cumulatively over the whole session
file. The daemon advertises the `response_timing` server capability
(schema revision 32); clients gate on it before reading the block.

## Wire shape

```json
"responseTiming": {
  "avgTtftMs": 234.5,
  "avgTokensPerSecond": 41.3,
  "llmTimeMs": 81234.5,
  "toolTimeMs": 4521.2,
  "lastResponse": {
    "ttftMs": 189.0,
    "generationMs": 2210.4,
    "tokensPerSecond": 33.5,
    "llmTimeMs": 2401.3
  }
}
```

Every field is omitted independently when unmeasured — no estimates, no
fabricated zeros. `lastResponse` is the latest measured assistant
response, and its own unmeasured fields stay omitted (a tool-only
response shows `llmTimeMs` without a stale TTFT).

Durations are milliseconds (one decimal); `tokensPerSecond` is tokens per
second (one decimal). `avgTtftMs` is the plain mean over the responses with
a measured TTFT; `avgTokensPerSecond` is overall generation throughput —
the summed output tokens of the responses with a measured generation
interval divided by their summed generation seconds (not a mean of
per-response rates). `llmTimeMs` and `toolTimeMs` are cumulative sums
(tool time is total tool work: parallel executions sum).

## Scope and exclusions

- Assistant responses of the agent loop only:
  - `ttftMs`: request sent (the provider fired the payload hook) to the
    first nonempty streamed text/thinking delta.
  - `generationMs`: that first delta to the terminal `done`/`error`
    event; the generation interval `tokensPerSecond` measures.
  - `llmTimeMs`: request sent to the terminal event.
- Per tool call, the toolResult row's `details.durationMs` carries the
  loop's measured full-call wall duration for every executed tool call — a
  documented fork deviation from the TS python rows, whose
  `details.durationMs` was the narrower kernel-execution value.
- Excluded by design: the compaction/branch summarizer calls (they never
  cross the stream seam this feature instruments) and user-aborted
  responses synthesized outside the seam (their rows carry no timing; a
  terminal provider `error` event is measured like `done`).

## Accounting

The fold is the timing twin of the whole-file usage scan behind the
summary's `usage` field: every in-file branch included, cumulative across
compactions (rows are never deleted), riding the same resumable scan
state with per-id replacement for retried durable writes. A cold reattach
reproduces the block from the session file alone.
