# Workstream labeling with Clef evaluation

The desktop sends `classify_workstream` to the local daemon. This fork adds a
small dedicated System One HTTP client. It does not restore the experimental
Decision API control loop, create an agent/session, or use chat completions.

Clef's explicit evaluation target is `prime-inference/cloudflare/clef`. It need
not appear in the chat model catalog. The daemon uses a transient descriptor only
to resolve existing provider/model headers, API key, and the stored team's
`X-Prime-Team-ID` through `ModelRegistry::get_api_key_and_headers`. It inserts no
catalog entry and changes no settings. The endpoint comes from the existing
Prime Inference provider configuration, falling back to the canonical
`https://api.pinference.ai/api/v1`.

The request is `POST <baseUrl>/systemone`:

```json
{
  "model": "cloudflare/clef",
  "state": {
    "turnId": "message-id",
    "workstreams": [{"id":"o1","title":"Prepare O1 case","summary":"Lighthouse and judging"}],
    "recentMessages": [{"role":"assistant","text":"The case includes judging."}],
    "messages": [{"role":"user","text":"Can we prepare judging applications?"}]
  },
  "questions": {
    "w0": {
      "type": "choice",
      "instructions": "Does the current message directly concern workstream o1?",
      "criteria": {"relevant":"Clearly part of this outcome","unrelated":"Another task or uncertain"}
    }
  }
}
```

The structured reply carries `model` and `answers.w0` with `choice`, `confidence`,
and `probabilities: {relevant: number, unrelated: number}`. No model-generated JSON
text is parsed. Every requested question must be answered with a valid
probability distribution. Candidate IDs stay in state; opaque question keys
avoid restrictions on workstream IDs.

Each call evaluates at most 32 questions. Up to 100 workstreams are evaluated in
sequential batches under one 20-second deadline; candidates are never silently
omitted. Both relevant probability and confidence must reach 0.75 to apply a
label. This is an initial conservative threshold, not an empirically calibrated
accuracy guarantee. Low confidence leaves the message in Inbox. Provider/auth
errors, malformed answers, and partial batches also leave prior labels intact.

Responses, errors, redirects, and request sizes are bounded. Error messages never
include provider response bodies, request state, credentials, or team identity.
The daemon's response remains `{turnId, uncertain, workstreamIds}`.

## Contract evidence

- [Upstream System One transport and synthetic fixtures at 56408e2a](https://github.com/PrimeIntellect-ai/prime-agent/blob/56408e2a83af8b84983e83670f1e485d71d71f3d/crates/pa-ai/src/providers/systemone.rs).
- [Historical Decision API documentation](https://github.com/PrimeIntellect-ai/prime-agent/blob/56408e2a83af8b84983e83670f1e485d71d71f3d/docs/decision-api.md).
  This experimental feature was subsequently reverted upstream by #3522; it is
  not present in the base checkout.
- [System One API and published Clef question limits](https://system-one.dev/en/api).

CI runs synthetic loopback HTTP tests for request shape, auth/team headers,
redaction, cancellation, timeout, and malformed/oversized responses, plus native
classification-policy and batching tests. These do not prove current live Prime
Inference availability or classification accuracy. No live model request is made
by these tests.
