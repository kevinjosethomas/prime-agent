# Runtime follow-ups local evidence

Date: 2026-10-19. Combined source from base
`261698e1d725b1d3faa8594434dd6a84426a0ec7`, with auth selector
`b622d73244a75a5e38c81a22408d39fdb159ea24`, archive API
`cfdeda8c7fb15f83611ca4dfb82f7b2e64f1cf6e`, and mandatory readiness fix
`3e8de09061f94b5aef8b7931c758349d7bbdc784`.

The tested combined code commit is `b955eacce390b0e70d6a161d6bee6273296c4a0d`.
Runtime tree: `2f5080f8048fd67c9c4a5807b5a39d3d8016e31b`.
Readiness probe blob: `691d65d05470d4f8d9c6a2c33be6328790e7217d`.
The subsequent CI/docs commit changes no runtime, test or native source.

## Local results

Run from the candidate root using the existing project source interface with
`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=prime-agent-runtime/src` and
`/opt/homebrew/bin/python3` (Python 3.14.4), without installing dependencies:

- `python -m unittest discover -s prime-agent-runtime/tests -v`, reporter unset:
  **26 discovered; 23 passed, three canonical desktop tests skipped**.
- Same full discovery with `ELECTRON_RUN_AS_NODE=1` and reporter JSON argv below:
  **26 passed, zero skips**. Every canonical test uses its own temporary DB.
- From `prime-agent-runtime`, `PYTHONPATH=src python -m unittest discover -s test -p test_create_session.py -v`:
  **four passed**, host mocked.
- From the same directory, `PYTHONPATH=src python -m unittest discover -s test -p test_subagent_registry.py -v`:
  **27 passed**, host mocked.
- `cargo fmt --all --check`: **passed**. Formatting only; no native build/test.

All four exact-probe cases ran: current runtime ready, stale facade rejected,
stale client/snapshot handles rejected, and stale archived-list signatures rejected.
The full test command extracts the shipped Python assertion from Rust source;
it does not run a native bootstrap or install anything.

## Exact desktop artifact and source

Reporter argv:

```json
["/Users/kevin/Applications/Primer.app/Contents/MacOS/Prime Agent Desktop", "/Users/kevin/Applications/Primer.app/Contents/Resources/app.asar/dist-electron/workstream-cli.cjs"]
```

Existing installed Primer version: `0.1.2`. Artifact SHA-256:

- `Contents/Resources/app.asar`: `d65bfac2b46207708860646446e330c041c3e71761091d7f201977353456e537`
- `dist-electron/workstream-cli.cjs` inside that ASAR: `02e1c9ad2ec4f1911452bf3d89ab4f5132a438ae3460889f519c3c72ce12de0d`
- `Contents/MacOS/Prime Agent Desktop`: `5a86f6e4fb5a8cebce3ebfb1b11279a3cbeba8f1708c44d43c9f254bbfdaf071`

The reporter's bundled source map includes seven sources. All seven source
contents byte-match the tracked files under `packages/desktop` at desktop commit
`a876363190d707412779ec9ede9d89b2f59ae536` (branch
`task10/primer-ui-parallel-integration`), with no local edits to those files.
This identifies the reporter sources, not the entire installed desktop build.

| Bundled source | SHA-256 |
| --- | --- |
| `src/main/workstreams/cli.ts` | `a2f2dafc265bc464e0bfbf9dfdd026fbc491d59f54eb4086743bdd4d2e435e71` |
| `src/main/workstreams/database.ts` | `99dbb20a85202856cbe52d539ba373e5dffc4fe31779ec997fc5d50681333fa3` |
| `src/shared/workstreams.ts` | `ec41098534d873a544ffdce5cae353c5da2e9f8bc1b9cd06f2ddb2595e6585e6` |
| `src/main/workstreams/validation.ts` | `5c9da14d46d8e83cba715a739f7088193305b73ff9230f4111743fa6b253c5a0` |
| `src/shared/session-links.ts` | `bf513fa0562f7d9535f830a470734844db0491daf308492f36011cd457308664` |
| `src/main/workstreams/prompt.ts` | `ec3a2ac6e1a032dc8985d4484c1fcc688290f0df7fff5a25ab1ef6cf67a76f2d` |
| `src/main/workstreams/context.ts` | `61ecb9d112027e24392d43c921d7a0e11f481afbab55d4c915690eb2182bd98d` |

## Limits

These are local source and temp-DB interoperability results, not remote CI or
installed candidate verification. No native auth/bootstrap tests, workspace
check/clippy/test, release build, package assembly or installation ran here.
The local Python version is not CI's Python 3.12; remote CI must run the exact
candidate. The native fork workflow deliberately exposes its three skipped
canonical reporter tests; it cannot claim this local cross-repo evidence as its
own executed CI coverage. Archive preservation of canonical associated sessions
and classified turns remains outside these 26 tests; see
[verification boundaries](desktop-runtime-verification.md).

No Electron UI, daemon/session restart, live DB, real auth/provider call, dependency
installation, push or GitHub comment was used. Root owns any remote authorization,
full native gates, complete-runtime artifact staging and coordinated installation.

Assumptions: the existing built reporter is the coordinated canonical host; its
artifact and embedded sources are identified above rather than inferred from the
current desktop branch. Constants changed: none. Public archive filtering retains
`archived=False` by default and `None` for all entries. Native auth JSON remains v1,
with additive `provider_auth_methods`; kernel protocol remains v3. No Rust edition,
syntax, dependencies or ownership boundaries changed in the CI follow-up.
