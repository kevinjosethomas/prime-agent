# Desktop runtime verification

## Native fork CI

`desktop-fork-runtime.yml` runs on pushes to `feat/root-desktop-runtime` and
`primer/runtime-followups-candidate`, or by explicit workflow dispatch.
The Python job runs complete `prime-agent-runtime/tests` discovery, including the
exact shipped readiness assertion and its stale-archive-API regressions. It also
runs the separate create-session and subagent-registry regression suites.

The native job retains the auth, provider-login, MCP-login, workstream identity,
bootstrap readiness, classifier, MCP, release-build and packaged-artifact checks.
Its non-authenticating `auth --json` EOF probe must advertise
`provider_auth_methods` on the v1 ready event. No login request is needed.

## Canonical desktop reporter boundary

This native repository does not stage the separately owned desktop reporter.
The three canonical SQLite tests therefore skip in its Python CI job. A green
source job is **not** full archive interoperability evidence. Wheel inspection
and the packaged binary's `--version` check do not cover that boundary either.

Before installation, run the same complete suite against the coordinated built
desktop reporter. Record the exact candidate commit, runtime tree, reporter
artifact SHA-256, and desktop source identity. Require zero skips:

```sh
ELECTRON_RUN_AS_NODE=1 \
WORKSTREAM_DESKTOP_REPORTER='["/path/to/Primer.app/Contents/MacOS/Prime Agent Desktop", "/path/to/Primer.app/Contents/Resources/app.asar/dist-electron/workstream-cli.cjs"]' \
PYTHONPATH=prime-agent-runtime/src \
python -m unittest discover -s prime-agent-runtime/tests -v
```

The tests add their own `--db` inside `TemporaryDirectory`. Do not provide a live
workstream DB, open Electron UI, or invoke a real session/provider. At the initial
archive candidate this discovers 26 tests: 19 fixture/API, three canonical desktop,
and four exact readiness-probe tests. Without a reporter only 23 pass, with three
explicit skips. With the canonical reporter all 26 must pass.

The canonical archive test covers status, milestone, timestamp and archive-state
preservation across archive/restore/update. The separate assignment test verifies
queued and observed native identities. It does not establish archive/restore
preservation of associated sessions or classified turns in the canonical host;
that remains an additional integration limit, not a property proven by 26 green
tests. Fixture coverage and the minimal `set-archived` request are source-side
evidence only for those associations.

The coordinated candidate's local run and immutable artifact/source identities
are recorded in [desktop-runtime-followups-evidence.md](desktop-runtime-followups-evidence.md).
This record does not claim remote CI passed, native gates ran locally, or the
candidate was installed. `.github/workflows/ci.yml` remains the workspace Rust
check/clippy/test authority. Full required gates must pass before merge, and root
owns remote runs, complete-runtime staging and installation.
