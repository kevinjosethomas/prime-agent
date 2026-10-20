# pa-contract

The daemon client contract generator: the canonical JSON Schemas, wire
witness corpora, and client-facing metadata for the Prime Agent supervisor
socket protocol, derived single-source from the `pa-types` wire graph
(`SchemaSettings::draft2020_12().for_deserialize()` — the schemas state what
the wire may carry INTO a serde parse).

## Ownership

- Owns: schema tooling only. It reads `pa-types` (with the default-off
  `contract` feature), `pa-daemon`'s protocol catalogs, and `pa-client`'s
  timeout constants, and writes the generated artifacts into
  `packages/client-contract/generated/` (schema, corpus, metadata — the
  Rust-owned half of the committed output).
- Depends on `pa-types`, `pa-daemon`, `pa-client`; nothing depends on it.
- It is a workspace member in its own CI battery; it never enters the TUI or
  client dependency graphs.

## Usage

```sh
cargo run -p pa-contract            # export into packages/client-contract/generated
cargo test -p pa-contract            # drift gate + wire corpus conformance
```

Generated output is committed and byte-gated: regeneration must match. The
Node half (TS types, validators) is owned by `packages/client-contract`
(see its README). Non-goals: transport, lifecycle, platform packaging,
client adoption.
