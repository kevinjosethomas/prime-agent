# Last-user-prompt metadata

The daemon's active summary, saved summary, saved catalog and passive child summary
publish optional `lastUserPromptAt` as an ISO timestamp. It comes from the latest
valid persisted `message` with `message.role == "user"`, in append order. This is
user-role prompt time, including harness user messages, not human-only input time.

The field is absent when no valid user timestamp exists. There is no created,
modified, assistant activity, file mtime or scan-time fallback. Later assistant,
tool-result, custom, rename and compaction records do not reset it. The timestamp
parser is shared with attach's `lastUserPromptMs`: unsigned milliseconds, finite
numeric milliseconds truncated/clamped to zero, or a positive ISO timestamp.
Invalid/missing timestamps leave the last valid user timestamp intact.

The existing resumable `SessionScanAccumulator` owns the numeric scalar and
`SessionInfo` carries it. Unchanged files reuse cached metadata, appended files
fold only the tail, and certified persisted sidecars resume the same fold.
Sidecar version advances from 2 to 3, requiring one normal cold reconstruction
for old prefixes. No new desktop index, attach-all request or history query is
needed. Compacted-away prompts remain available through this whole-file metadata.
Active summary reuses its existing `read_session_info` result for usage and prompt
time; pathless stores derive the same scalar from their in-memory entries.

This is an additive optional daemon field. No protocol version changes, new
dependency, Python DTO or app-owned model were introduced. pa-daemon continues
to own session persistence, metadata folding and daemon summary projection.

## Validation boundary

The source candidate adds focused native tests for user-role/timestamp provenance,
cache/sidecar/append/torn-tail resume, old-version rejection, ISO-only cached
metadata, and equality across active/saved/catalog/passive serializers, including
prompts outside the attach compaction window. Fork CI includes these plus existing
session-info, summary and windowed-attach suites, before package assembly.

Local verification is source review and `cargo fmt --all --check` only. Native
Rust tests/builds are intentionally remote-only for this work; their addition is
not a claim they passed. Root owns any authorized exact-SHA fork CI push, artifact
activation and maintenance restart. Existing native sessions are not restarted by
this change's preparation, and live workers cannot emit a new field until safely
replaced. UI code must keep missing metadata honest, not use activity as a proxy.
