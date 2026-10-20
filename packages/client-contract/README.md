# prime-agent-client-contract

The generated Prime Agent daemon client contract: canonical JSON Schemas,
TypeScript types, runtime validators, and derived wire metadata — all
generated from the Rust wire types (`crates/pa-types/src/daemon` and the
message types it references). **Nothing here is hand-maintained.**

## Scope

- **Canonical schemas** (`generated/schema/*.schema.json`): the
  deserialization-acceptance contract — what the supervisor socket may carry
  INTO a client — derived with `schemars` from the same serde derives the
  wire uses.
- **Types** (`generated/ts/`): TypeScript declarations generated from the same
  schemas (`npm run gen:ts`); committed and drift-gated.
- **Validators** (`src/validators.ts`): Ajv (draft 2020-12) compiled from the
  same schemas. Never mutates input.
- **Corpus** (`generated/corpus/`): schema-driven wire witnesses (accepted),
  mutation candidates with recorded serde verdicts (mutations), and real
  captured fixtures asserted lossless in the Rust tests.
- **Metadata** (`generated/metadata.json`): protocol identity, per-command
  plane/mutating/timeout classes, outbound kinds, error codes, capability
  catalogs — every list derived from the Rust source of truth. Catalog gaps
  are REPORTED (`catalogDeltas`), never silently reconciled.

## Direction and open/closed surfaces

- The canonical direction is **deserialization-acceptance**. Serialization
  asymmetries are documented in `metadata.json` under `direction`.
- **Closed on receive**: unknown command types (a client must not send or
  accept them).
- **Open on receive**: unknown fields inside a known command (the flatten
  catch-all), unknown outbound frame kinds (forward raw —
  `checkInboundFrame` distinguishes strict known kinds from pass-through
  unknown kinds), unknown `errorInfo` codes (strict known-code shapes, open
  unknown codes).
- Capability wire fields stay open strings; the metadata catalogs are
  informational.

## API

```ts
import { loadContract, checkInboundFrame, loadMetadata } from "prime-agent-client-contract";

const c = await loadContract();
if (c.validateCommand(frame)) { /* a known command type with strict shape */ }
const verdict = checkInboundFrame(c, frame);
if (verdict.unknownKind) { /* forward raw */ }
const metadata = loadMetadata(); // protocol, commands, kinds, errors, capabilities
```

## Non-goals

- No transport, no connection lifecycle, no reconnect/attachment policy.
- No Electron, preload, or renderer concerns.
- No platform packaging (native adapter work is separate).
- Not published to npm (consumed pinned from the harness source).
- Rust-side behavior is never changed by this package: it is a generated
  view of the wire, and disagreements are surfaced, not patched over.

## Regeneration

`cargo run -p pa-contract` (Rust side: schemas, corpus, metadata) and
`npm run gen:ts` (Node side: types + typecheck module). Both outputs are
committed; `cargo test -p pa-contract` and `npm run verify` fail on drift.
