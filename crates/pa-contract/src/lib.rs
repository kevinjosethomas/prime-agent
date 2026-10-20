//! The daemon client contract generator: the canonical JSON Schemas, wire
//! witnesses, and client-facing metadata, all derived from `pa_types::daemon`
//! (never a hand-maintained mirror).
//!
//! The canonical direction is DESERIALIZATION-ACCEPTANCE: a schema states what
//! the wire may carry INTO a client. Serialize-direction asymmetries (emit
//! null vs omit) are documented in `metadata.json` under `direction` and held
//! by the round-trip witnesses. Open-on-receive surfaces (unknown outbound
//! frame kinds, unknown error-info codes, unknown fields inside a known
//! command via the flatten catch-all) stay open; closed-by-design surfaces
//! (unknown command types) stay closed — mirroring serde exactly.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use schemars::generate::SchemaSettings;
use serde_json::{json, Map, Value};

use pa_types::daemon::{
    command_plane, is_daemon_mutating_command, is_session_plane_daemon_command, DaemonCommandPlane,
};

/// One canonical schema root exported for clients.
pub struct SchemaRoot {
    /// Kebab-case file stem (`daemon-command.schema.json`).
    pub stem: &'static str,
    /// The schema (a `schemars` root: `$schema` + `$defs` for the reachable
    /// graph) or a generator-synthesized open surface.
    pub schema: Value,
}

fn root_schema<T: schemars::JsonSchema>(stem: &'static str) -> SchemaRoot {
    // Explicit deserialization-acceptance contract: the schema states what the
    // wire may carry INTO a serde parse (defaults/omission direction), not
    // what serde emits.
    let generator = SchemaSettings::draft2020_12()
        .for_deserialize()
        .into_generator();
    let schema = generator.into_root_schema_for::<T>();
    SchemaRoot {
        stem,
        schema: serde_json::to_value(schema).expect("serialize schema"),
    }
}

/// The client-facing schema roots: the supervisor-socket wire only.
///
/// # Panics
///
/// Panics when a schema derivation fails to serialize (a build-time
/// contract-generation failure, never a runtime one).
pub fn schema_roots() -> Vec<SchemaRoot> {
    use pa_types::daemon::{
        DaemonCommand, DaemonCommandEnvelope, DaemonCommandWire, DaemonErrorInfo,
        DaemonEventEnvelope, DaemonEventMeta, DaemonOutbound, DaemonResponse, DaemonResumeCursor,
    };
    use pa_types::session::AgentMessage;
    let mut error_schema = root_schema::<DaemonErrorInfo>("daemon-error-info");
    drop_phantom_unknown_branch(&mut error_schema.schema);
    let codes = error_info_codes(&error_schema.schema);
    // Same strict-known/open-unknown policy at the ROOT: an unknown `code`
    // passes through (serde: `DaemonErrorInfo::Unknown`), a known code must
    // carry its branch shape.
    if let Some(branches) = error_schema
        .schema
        .as_object_mut()
        .and_then(|object| object.get_mut("oneOf"))
        .and_then(Value::as_array_mut)
    {
        branches.push(json!({
            "type": "object",
            "required": ["code"],
            "properties": {
                "code": { "type": "string", "not": { "enum": codes } }
            },
            "additionalProperties": true
        }));
    }
    let mut response = root_schema::<DaemonResponse>("daemon-response");
    patch_error_info_everywhere(&mut response.schema, &codes);
    let mut outbound = root_schema::<DaemonOutbound>("daemon-outbound");
    patch_error_info_everywhere(&mut outbound.schema, &codes);
    let mut envelope = root_schema::<DaemonEventEnvelope>("daemon-event-envelope");
    patch_error_info_everywhere(&mut envelope.schema, &codes);
    vec![
        root_schema::<DaemonCommand>("daemon-command"),
        root_schema::<DaemonCommandEnvelope>("daemon-command-envelope"),
        root_schema::<DaemonCommandWire>("daemon-command-wire"),
        outbound,
        response,
        error_schema,
        envelope,
        root_schema::<DaemonEventMeta>("daemon-event-meta"),
        root_schema::<DaemonResumeCursor>("daemon-resume-cursor"),
        root_schema::<AgentMessage>("agent-message"),
        // Synthesized (no Rust type): the open receive envelope. Any object
        // with a string `type` is a frame; known kinds get shape validation by
        // the `daemon-outbound` schema, unknown kinds pass through raw.
        SchemaRoot {
            stem: "client-inbound-frame",
            schema: json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$id": "client-inbound-frame.schema.json",
                "title": "ClientInboundFrame",
                "description": "Open receive envelope: every supervisor line is an object with a string `type`. Unknown kinds pass through raw; known kinds are additionally shape-validated against daemon-outbound.",
                "type": "object",
                "required": ["type"],
                "properties": {
                    "type": { "type": "string" }
                },
                "additionalProperties": true
            }),
        },
    ]
}

/// The receive-consistent errorInfo policy, used for EVERY response
/// occurrence (root and `$defs` copies): a KNOWN code must carry its strict
/// branch shape (missing required payloads still fail), an UNKNOWN code
/// passes through (serde: `DaemonErrorInfo::Unknown` via `serde(other)`).
fn strict_known_open_unknown_error_info(codes: &[String]) -> Value {
    json!({
        "anyOf": [
            { "type": "null" },
            { "$ref": "#/$defs/DaemonErrorInfo" },
            {
                "type": "object",
                "required": ["code"],
                "properties": {
                    "code": { "type": "string", "not": { "enum": codes } }
                },
                "additionalProperties": true
            }
        ]
    })
}

/// The real daemon error codes. `unknown` is the `serde(other)` catch-all's
/// serialization literal — a phantom on the wire (a real unknown code keeps
/// ITS OWN value): filtered from the catalog and from the strict branches.
fn error_info_codes(error_schema: &Value) -> Vec<String> {
    enum_tags(error_schema)
        .into_iter()
        .filter(|tag| tag != "unknown")
        .collect()
}

/// Drop the phantom `unknown` const branch from the closed error-info union.
fn drop_phantom_unknown_branch(schema: &mut Value) {
    let Some(object) = schema.as_object_mut() else {
        return;
    };
    let Some(branches) = object.get_mut("oneOf").and_then(Value::as_array_mut) else {
        return;
    };
    branches.retain(|branch| {
        branch
            .get("properties")
            .and_then(|props| props.get("code"))
            .and_then(|code| code.get("const"))
            .and_then(Value::as_str)
            != Some("unknown")
    });
}

/// Apply the strict-known/open-unknown errorInfo policy at EVERY occurrence:
/// the response root, the outbound union's flattened Response branch, and any
/// `$defs` copies (event envelopes nest the outbound union). A known code
/// must carry its branch shape; an unknown code passes through (serde:
/// `DaemonErrorInfo::Unknown`).
fn patch_error_info_everywhere(schema: &mut Value, codes: &[String]) {
    fn walk(schema: &mut Value, patch: &Value) {
        match schema {
            Value::Object(object) => {
                let mut owned = std::mem::take(object);
                if let Some(props) = owned.get_mut("properties").and_then(Value::as_object_mut) {
                    if props.contains_key("errorInfo") {
                        props.insert("errorInfo".to_string(), patch.clone());
                    }
                }
                for value in owned.values_mut() {
                    walk(value, patch);
                }
                *object = owned;
            }
            Value::Array(list) => {
                for value in list.iter_mut() {
                    walk(value, patch);
                }
            }
            _ => {}
        }
    }
    let patch = strict_known_open_unknown_error_info(codes);
    walk(schema, &patch);
}

/// The discriminant property name of a tagged enum's schema: the property
/// that is a `const` in every branch (`type` for commands/outbound frames,
/// `code` for error info, `role` for messages, `state` for reply ownership).
#[must_use]
pub fn discriminant_property(schema: &Value) -> Option<String> {
    let branches = schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))?
        .as_array()?;
    for branch in branches {
        let props = branch.get("properties")?.as_object()?;
        let consts: Vec<String> = props
            .keys()
            .filter(|name| props[*name].get("const").is_some())
            .cloned()
            .collect();
        if consts.len() == 1 {
            return consts.into_iter().next();
        }
        if consts.len() > 1 {
            // Prefer `type`, then `code`, then the first const property.
            return consts
                .iter()
                .find(|name| name.as_str() == "type")
                .or_else(|| consts.iter().find(|name| name.as_str() == "code"))
                .cloned();
        }
    }
    None
}

/// The wire tag consts of a tagged enum's schema (`oneOf`/`anyOf` branches),
/// read off the schema's own discriminant — never a hardcoded property name.
///
/// # Panics
///
/// Panics on a malformed branch list (generation-time input only).
pub fn enum_tags(schema: &Value) -> Vec<String> {
    let Some(tag_property) = discriminant_property(schema) else {
        return Vec::new();
    };
    let Some(branches) = schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    branches
        .iter()
        .filter_map(|branch| {
            branch
                .get("properties")
                .and_then(|props| props.get(&tag_property))
                .and_then(|tag| tag.get("const"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

/// Derive one minimal accepted witness per enum branch, plus per-optional-
/// property variants and an unknown-extras variant. All synthesized from the
/// schema itself — no hand-maintained samples.
///
/// # Panics
///
/// Panics when a schema branch is not synthesizable (a malformed schema
/// graph — a generation-time failure, never a runtime one).
#[must_use]
pub fn witnesses(stem: &str, schema: &Value) -> Vec<Value> {
    let defs = defs_of(schema);
    let mut out = Vec::new();
    if schema.get("oneOf").is_some() || schema.get("anyOf").is_some() {
        let branches = schema
            .get("oneOf")
            .or_else(|| schema.get("anyOf"))
            .and_then(Value::as_array)
            .expect("branches");
        for branch in branches {
            out.push(synth(branch, &defs, 0));
        }
    } else {
        let base = synth(schema, &defs, 0);
        // Optional-property variants: the schema's optional fields each get a
        // witness where only that field is added (cursor union forms, null vs
        // absent, defaults).
        let mut extra_variants = Vec::new();
        if let Some(props) = schema.get("properties").and_then(Value::as_object) {
            for (name, prop) in props {
                if required_of(schema).contains(name) {
                    continue;
                }
                let mut sample = base.clone();
                if let Some(obj) = sample.as_object_mut() {
                    obj.insert(name.clone(), synth(prop, &defs, 0));
                }
                extra_variants.push(sample);
            }
        }
        // Open-extras witness: unknown fields ride the flatten catch-all.
        let mut extras = base.clone();
        if let Some(obj) = extras.as_object_mut() {
            obj.insert("__contractUnknownExtra".to_string(), json!({"kept": true}));
        }
        out.push(base);
        out.extend(extra_variants);
        out.push(extras);
    }
    let _ = stem;
    out
}

fn defs_of(schema: &Value) -> Map<String, Value> {
    schema
        .get("$defs")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default()
}

fn required_of(schema: &Value) -> Vec<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Deterministic minimal value for a (sub)schema.
fn synth(schema: &Value, defs: &Map<String, Value>, depth: usize) -> Value {
    if depth > 24 {
        return json!({});
    }
    // A `$ref` may carry siblings (internally tagged enums express the tag as
    // `properties` + `required` next to a `$ref` of the variant payload):
    // resolve the target, then merge the sibling required properties over it.
    let resolved = if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let name = reference.rsplit('/').next().unwrap_or_default();
        defs.get(name).cloned().unwrap_or_else(|| schema.clone())
    } else {
        schema.clone()
    };
    if resolved != *schema
        && (schema.get("properties").is_some() || schema.get("required").is_some())
    {
        let mut merged = synth(&resolved, defs, depth + 1);
        let required = required_of(schema);
        if let (Some(target), Some(source)) = (merged.as_object_mut(), schema.get("properties")) {
            for name in required {
                if let Some(prop) = source.get(&name) {
                    target.insert(name.clone(), synth(prop, defs, depth + 1));
                }
            }
        }
        return merged;
    }
    let schema = &resolved;
    // A declared default is definitionally accepted: synthesize it.
    if let Some(default) = schema.get("default") {
        return default.clone();
    }
    if let Some(constant) = schema.get("const") {
        return constant.clone();
    }
    if let Some(enumeration) = schema.get("enum").and_then(Value::as_array) {
        return enumeration.first().cloned().unwrap_or(Value::Null);
    }
    for key in ["oneOf", "anyOf"] {
        if let Some(branches) = schema.get(key).and_then(Value::as_array) {
            return synth(branches.first().expect("non-empty branch"), defs, depth + 1);
        }
    }
    if let Some(all) = schema.get("allOf").and_then(Value::as_array) {
        // Merge allOf members (flatten shapes) into one minimal object.
        let mut merged = Map::new();
        for member in all {
            let value = synth(member, defs, depth + 1);
            if let Some(object) = value.as_object() {
                for (name, prop) in object {
                    merged.insert(name.clone(), prop.clone());
                }
            }
        }
        return Value::Object(merged);
    }
    // `type` may be a list (e.g. ["string", "null"]): synthesize the first
    // non-null member; a null-only type is a null.
    let member_type = match schema.get("type") {
        Some(Value::Array(list)) => list
            .iter()
            .filter_map(Value::as_str)
            .find(|name| *name != "null")
            .map(str::to_string),
        Some(Value::String(name)) => Some(name.clone()),
        _ => None,
    };
    if schema
        .get("type")
        .and_then(Value::as_array)
        .is_some_and(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .all(|name| name == "null")
        })
    {
        return Value::Null;
    }
    match member_type.as_deref() {
        Some("string") => json!("wire"),
        Some("integer" | "number") => json!(0),
        Some("boolean") => json!(false),
        Some("null") => Value::Null,
        Some("array") => match schema.get("items") {
            Some(items) => json!([synth(items, defs, depth + 1)]),
            None => json!([]),
        },
        Some("object") | None => {
            // A bare `true` schema or open object: minimal open object.
            if schema.get("properties").is_none() {
                return json!({});
            }
            let mut object = Map::new();
            let required = required_of(schema);
            if let Some(props) = schema.get("properties").and_then(Value::as_object) {
                for name in &required {
                    if let Some(prop) = props.get(name) {
                        object.insert(name.clone(), synth(prop, defs, depth + 1));
                    }
                }
            }
            Value::Object(object)
        }
        _ => json!({}),
    }
}

/// Mutation candidates derived from a witness by schema-driven rules (never
/// hand-written frames). Classification happens against the real serde parse.
pub fn mutation_candidates(stem: &str, schema: &Value, witness: &Value) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    let mut push = |name: String, value: Value| out.push((name, value));
    // The closed surface: only known discriminant values may enter. The
    // discriminant is derived from the schema (type / code / role / state).
    let tags = enum_tags(schema);
    let discriminant = discriminant_property(schema);
    if !tags.is_empty() && stem != "client-inbound-frame" {
        if let (Some(property), Some(mut object)) =
            (discriminant.as_deref(), witness.as_object().cloned())
        {
            object.insert(property.to_string(), json!("__contract_unknown_kind__"));
            push("unknown-kind".to_string(), Value::Object(object));
        }
        if let (Some(property), Some(mut object)) =
            (discriminant.as_deref(), witness.as_object().cloned())
        {
            let _ = object.remove(property);
            push("missing-tag".to_string(), Value::Object(object));
        }
    }
    if stem == "client-inbound-frame" {
        push("non-object".to_string(), json!("not-a-frame"));
        if let Some(mut object) = witness.as_object().cloned() {
            object.insert("type".to_string(), json!(7));
            push("non-string-type".to_string(), Value::Object(object));
        }
        if let Some(mut object) = witness.as_object().cloned() {
            object.remove("type");
            push("missing-type".to_string(), Value::Object(object));
        }
    }
    let required = required_of(schema);
    if let Some(object) = witness.as_object() {
        let required = required
            .iter()
            .map(String::as_str)
            .chain(object.keys().map(String::as_str));
        for name in required {
            let mut drop = object.clone();
            if drop.remove(name).is_some() {
                push(format!("drop-{name}"), Value::Object(drop));
            }
            let mut wrong = object.clone();
            let wrong_value = match object.get(name).and_then(Value::as_str) {
                Some("") => json!(false),
                _ => json!("wrong-type"),
            };
            wrong.insert(name.to_string(), wrong_value);
            push(format!("wrong-type-{name}"), Value::Object(wrong));
            let mut nulled = object.clone();
            nulled.insert(name.to_string(), Value::Null);
            push(format!("null-{name}"), Value::Object(nulled));
        }
    }
    out
}

/// Nested union branches: walk the schema and the witness in parallel; where
/// a property's subschema is a union (oneOf/anyOf), emit one FULL-ROOT
/// variant per branch with that property re-synthesized at its path (message
/// content as a bare string vs. typed block arrays) — all synthesized from
/// the schema, never hand-written.
#[must_use]
pub fn nested_branch_variants(
    schema: &Value,
    witness: &Value,
    defs: &Map<String, Value>,
) -> Vec<Value> {
    let mut replacements: Vec<(Vec<String>, Value)> = Vec::new();
    collect_union_replacements(schema, witness, defs, &[], 0, &mut replacements);
    let mut out = Vec::new();
    for (path, value) in replacements {
        if out.len() >= 32 {
            break;
        }
        let mut variant = witness.clone();
        let pointer = format!("/{}", path.join("/"));
        if let Some(slot) = variant.pointer_mut(&pointer) {
            *slot = value;
            out.push(variant);
        }
    }
    out
}

fn collect_union_replacements(
    schema: &Value,
    witness: &Value,
    defs: &Map<String, Value>,
    prefix: &[String],
    depth: usize,
    out: &mut Vec<(Vec<String>, Value)>,
) {
    if depth > 4 || out.len() >= 64 {
        return;
    }
    let schema = &resolve_ref(schema, defs);
    // A tagged union: descend into the branch that claims the witness (its
    // discriminant const matches) so the walk stays schema-driven.
    if let Some(branches) = schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(Value::as_array)
    {
        let Some(discriminant) = discriminant_property(schema) else {
            return;
        };
        if let Some(tag) = witness.get(&discriminant).and_then(Value::as_str) {
            for branch in branches {
                if branch
                    .get("properties")
                    .and_then(|props| props.get(&discriminant))
                    .and_then(|prop| prop.get("const"))
                    .and_then(Value::as_str)
                    == Some(tag)
                {
                    collect_union_replacements(branch, witness, defs, prefix, depth + 1, out);
                    return;
                }
            }
        }
        return;
    }
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return;
    };
    let Some(object) = witness.as_object() else {
        return;
    };
    for (name, prop) in properties {
        let mut path = prefix.to_vec();
        path.push(name.clone());
        let resolved = resolve_ref(prop, defs);
        if let Some(branches) = resolved
            .get("anyOf")
            .or_else(|| resolved.get("oneOf"))
            .and_then(Value::as_array)
        {
            for branch in branches {
                if out.len() >= 64 {
                    return;
                }
                out.push((path.clone(), synth(branch, defs, 0)));
            }
        }
        // Descend into the property's own value (arrays included).
        if let Some(child) = object.get(name) {
            match child {
                Value::Array(list) => {
                    if let Some(items) = resolved.get("items") {
                        for (index, element) in list.iter().enumerate() {
                            let mut element_path = path.clone();
                            element_path.push(index.to_string());
                            collect_union_replacements(
                                items,
                                element,
                                defs,
                                &element_path,
                                depth + 1,
                                out,
                            );
                        }
                    }
                }
                Value::Object(_) => {
                    collect_union_replacements(prop, child, defs, &path, depth + 1, out);
                }
                _ => {}
            }
        }
    }
}

fn resolve_ref(schema: &Value, defs: &Map<String, Value>) -> Value {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let name = reference.rsplit('/').next().unwrap_or_default();
        if let Some(def) = defs.get(name) {
            return def.clone();
        }
    }
    schema.clone()
}

/// Drop-each-field mutations of tagged objects at ANY depth (known-kind
/// nested blocks must stay strict: a malformed known tag is a serde error,
/// never a Raw fallback). Classification happens against the real parse.
pub fn tagged_object_field_drops(witness: &Value) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    if out.len() >= 64 {
        return out;
    }
    if let Some(object) = witness.as_object() {
        let is_tagged = object
            .get("type")
            .or_else(|| object.get("code"))
            .or_else(|| object.get("role"))
            .and_then(Value::as_str)
            .is_some();
        if is_tagged {
            let tag_property = ["type", "code", "role"]
                .into_iter()
                .find(|name| object.contains_key(*name))
                .unwrap_or("type");
            for name in object.keys() {
                if name == tag_property {
                    continue;
                }
                let mut dropped = object.clone();
                dropped.remove(name);
                out.push((format!("nested-drop-{name}"), Value::Object(dropped)));
                let mut mistyped = object.clone();
                let wrong_value = match object.get(name).and_then(Value::as_str) {
                    Some("") => json!(false),
                    _ => json!("wrong-type"),
                };
                mistyped.insert(name.clone(), wrong_value);
                out.push((format!("nested-wrong-type-{name}"), Value::Object(mistyped)));
                if out.len() >= 64 {
                    return out;
                }
            }
        }
    }
    let children: Vec<Value> = match witness {
        Value::Object(map) => map.values().cloned().collect(),
        Value::Array(list) => list.clone(),
        _ => Vec::new(),
    };
    for child in children {
        for (name, variant) in tagged_object_field_drops(&child) {
            out.push((format!("nested-{name}"), variant));
            if out.len() >= 64 {
                return out;
            }
        }
    }
    out
}

/// Client-facing metadata, every list derived from the canonical Rust sources.
///
/// # Panics
///
/// Panics on a malformed schema branch list (generation-time input only).
pub fn metadata(command_schema: &Value, outbound_schema: &Value, error_schema: &Value) -> Value {
    let command_tags = enum_tags(command_schema);
    let known: Vec<String> = pa_daemon::protocol::KNOWN_COMMAND_TYPES
        .iter()
        .map(ToString::to_string)
        .collect();
    let schema_only: Vec<String> = command_tags
        .iter()
        .filter(|tag| !known.contains(tag))
        .cloned()
        .collect();
    let daemon_only: Vec<String> = known
        .iter()
        .filter(|tag| !command_tags.contains(tag))
        .cloned()
        .collect();
    let commands = command_tags
        .iter()
        .map(|tag| {
            json!({
                "type": tag,
                "plane": match command_plane(tag) { DaemonCommandPlane::Session => "session", DaemonCommandPlane::Control => "control" },
                "mutating": is_daemon_mutating_command(tag),
                "sessionPlane": is_session_plane_daemon_command(tag),
                "longRunning": pa_client::daemon_client::LONG_RUNNING_COMMAND_TYPES.contains(&tag.as_str()),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "protocol": {
            "name": pa_types::daemon::DAEMON_PROTOCOL_NAME,
            "version": pa_types::daemon::DAEMON_PROTOCOL_VERSION,
            "schemaRevision": pa_types::daemon::DAEMON_SCHEMA_REVISION,
            "schemaId": pa_types::daemon::DAEMON_SCHEMA_ID,
            "negotiation": "envelope protocolVersion is exactly 7; the supervisor enforces 7. pa-client records min(server, client) per session as client behavior — that is not the wire contract."
        },
        "commands": commands,
        "outboundKinds": enum_tags(outbound_schema),
        "errorCodes": enum_tags(error_schema),
        "capabilities": {
            "client": pa_daemon::protocol::supported_client_capabilities(),
            "server": pa_daemon::protocol::default_server_capabilities(),
        },
        "timeouts": {
            "defaultRequestMs": pa_client::daemon_client::DEFAULT_REQUEST_TIMEOUT_MS,
            "longRunningRequestMs": pa_client::daemon_client::LONG_RUNNING_REQUEST_TIMEOUT_MS,
            "longRunningCommandTypes": pa_client::daemon_client::LONG_RUNNING_COMMAND_TYPES,
        },
        "constants": {
            "updateRestartPreparingMessage": pa_types::daemon::UPDATE_RESTART_PREPARING_MESSAGE,
            "kernelNotRunningMessage": pa_types::daemon::KERNEL_NOT_RUNNING_MESSAGE,
        },
        "direction": {
            "canonical": "deserialization-acceptance",
            "openOnReceive": [
                "unknown outbound frame kinds (serde: `DaemonOutbound` parse is stricter; clients forward raw)",
                "unknown errorInfo codes (serde: `DaemonErrorInfo::Unknown` via serde(other))",
                "unknown fields inside a known command (flatten catch-all)"
            ],
            "closedOnReceive": ["unknown command types"],
        },
        "catalogDeltas": {
            "schemaOnlyCommandTypes": schema_only,
            "daemonCatalogOnlyCommandTypes": daemon_only,
        }
    })
}

/// The generated-output directory inside the client package.
#[must_use]
pub fn generated_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../packages/client-contract/generated")
}

/// Export every canonical artifact into `dir`: `schema/`, `corpus/`,
/// `metadata.json`.
///
/// # Errors
///
/// Returns `Err` when the artifact directory cannot be created, a schema or
/// corpus line cannot be serialized, or any file cannot be written.
///
/// # Panics
///
/// Panics when a schema branch is not synthesizable (see [`witnesses`]).
#[allow(clippy::too_many_lines)]
pub fn export(dir: &Path) -> Result<Vec<PathBuf>> {
    let roots = schema_roots();
    let command_schema = roots
        .iter()
        .find(|root| root.stem == "daemon-command")
        .map(|root| root.schema.clone())
        .expect("daemon-command root");
    let outbound_schema = roots
        .iter()
        .find(|root| root.stem == "daemon-outbound")
        .map(|root| root.schema.clone())
        .expect("daemon-outbound root");
    let error_schema = roots
        .iter()
        .find(|root| root.stem == "daemon-error-info")
        .map(|root| root.schema.clone())
        .expect("daemon-error-info root");

    let mut written = Vec::new();
    std::fs::create_dir_all(dir.join("schema")).context("schema dir")?;
    std::fs::create_dir_all(dir.join("corpus/accepted")).context("accepted dir")?;
    std::fs::create_dir_all(dir.join("corpus/mutations")).context("mutations dir")?;

    for root in &roots {
        let path = dir
            .join("schema")
            .join(format!("{}.schema.json", root.stem));
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&root.schema).context("schema pretty")? + "\n",
        )
        .with_context(|| format!("write {}", path.display()))?;
        written.push(path);

        let mut accepted = String::new();
        let mut accepted_samples: Vec<Value> = witnesses(root.stem, &root.schema);
        let defs = defs_of(&root.schema);
        // Nested union branches: every property whose schema is a union gets
        // one accepted variant per branch (bare string vs. typed block arrays).
        for witness in accepted_samples.clone() {
            for variant in nested_branch_variants(&root.schema, &witness, &defs) {
                if accepted_samples.len() >= 512 {
                    break;
                }
                accepted_samples.push(variant);
            }
        }
        // Nested unknown-code coverage: every witness carrying an `errorInfo`
        // slot gets a same-shape variant with an unknown code — accepted by
        // serde (`DaemonErrorInfo::Unknown`) and by the patched open branch.
        for witness in accepted_samples.clone() {
            if let Some(error_info) = witness.get("errorInfo") {
                if error_info.is_null() || error_info.get("code").is_some() {
                    if let Some(mut nested) = witness.as_object().cloned() {
                        nested.insert(
                            "errorInfo".to_string(),
                            json!({"code": "__contract_unknown_code__", "detail": {"kept": true}}),
                        );
                        accepted_samples.push(Value::Object(nested));
                    }
                }
            }
        }
        for witness in accepted_samples {
            accepted.push_str(
                &(serde_json::to_string(&json!({"schemaRoot": root.stem, "sample": witness}))
                    .context("witness line")?
                    + "\n"),
            );
        }
        let accepted_path = dir
            .join("corpus/accepted")
            .join(format!("{}.jsonl", root.stem));
        std::fs::write(&accepted_path, accepted)
            .with_context(|| format!("write {}", accepted_path.display()))?;
        written.push(accepted_path);

        // Mutation candidates: never blindly labeled. Each candidate's
        // serde verdict is computed here (the generator runs the same serde
        // the tests run) and recorded; the Node leg must agree. Candidates
        // serde ACCEPTS are valid-mutation accepted coverage, not rejects.
        if root.stem != "client-inbound-frame" {
            let mut mutations = String::new();
            for witness in witnesses(root.stem, &root.schema) {
                let mut candidates: Vec<(String, Value)> =
                    mutation_candidates(root.stem, &root.schema, &witness);
                for (name, variant) in tagged_object_field_drops(&witness) {
                    candidates.push((name, variant));
                }
                for (name, variant) in candidates {
                    let rust_accepts = parse_as_root(root.stem, &variant)?;
                    mutations.push_str(
                        &(serde_json::to_string(&json!({
                            "schemaRoot": root.stem,
                            "case": name,
                            "sample": variant,
                            "rustAccepts": rust_accepts,
                        }))
                        .context("mutation line")?
                            + "\n"),
                    );
                }
            }
            let mutations_path = dir
                .join("corpus/mutations")
                .join(format!("{}.jsonl", root.stem));
            std::fs::write(&mutations_path, mutations)
                .with_context(|| format!("write {}", mutations_path.display()))?;
            written.push(mutations_path);
        }
    }

    let metadata_path = dir.join("metadata.json");
    std::fs::write(
        &metadata_path,
        serde_json::to_string_pretty(&metadata(&command_schema, &outbound_schema, &error_schema))
            .context("metadata pretty")?
            + "\n",
    )
    .with_context(|| format!("write {}", metadata_path.display()))?;
    written.push(metadata_path);
    Ok(written)
}

/// The roots' stem list (test helpers and package docs).
///
/// # Panics
///
/// Panics when a root schema cannot be serialized (see [`schema_roots`]).
#[must_use]
pub fn root_stems() -> Vec<&'static str> {
    schema_roots().into_iter().map(|root| root.stem).collect()
}

/// Map a schema-root stem to the Rust type that must (de)serialize its
/// accepted corpus and reject its rejected corpus.
///
/// # Errors
///
/// Returns `Err` only for an unknown schema-root stem or (in the lossless
/// round-trip helper) a re-serialization failure.
pub fn parse_as_root(stem: &str, sample: &Value) -> Result<bool> {
    use pa_types::daemon::{
        DaemonCommand, DaemonCommandEnvelope, DaemonCommandWire, DaemonErrorInfo,
        DaemonEventEnvelope, DaemonEventMeta, DaemonOutbound, DaemonResponse, DaemonResumeCursor,
    };
    use pa_types::session::AgentMessage;
    fn accepts<T: serde::de::DeserializeOwned>(sample: &Value) -> bool {
        // The accept verdict is the parse alone: deserialization may normalize
        // (defaults, `serde(other)` -> Unknown, deny-unknown-field drops).
        // Lossless round trips are asserted separately, on real fixtures.
        serde_json::from_value::<T>(sample.clone()).is_ok()
    }
    Ok(match stem {
        "daemon-command" => accepts::<DaemonCommand>(sample),
        "daemon-command-envelope" => accepts::<DaemonCommandEnvelope>(sample),
        "daemon-command-wire" => accepts::<DaemonCommandWire>(sample),
        "daemon-outbound" => accepts::<DaemonOutbound>(sample),
        "daemon-response" => accepts::<DaemonResponse>(sample),
        "daemon-error-info" => accepts::<DaemonErrorInfo>(sample),
        "daemon-event-envelope" => accepts::<DaemonEventEnvelope>(sample),
        "daemon-event-meta" => accepts::<DaemonEventMeta>(sample),
        "daemon-resume-cursor" => accepts::<DaemonResumeCursor>(sample),
        "agent-message" => accepts::<AgentMessage>(sample),
        "client-inbound-frame" => {
            // The open envelope: any object with a string type.
            sample.get("type").and_then(Value::as_str).is_some() && sample.is_object()
        }
        _ => anyhow::bail!("unknown schema root: {stem}"),
    })
}

/// Lossless re-emission: parsed and re-serialized back to the exact input
/// value. Guaranteed for flatten-preserving types, asserted on real goldens.
///
/// # Errors
///
/// Returns `Err` for an unknown schema-root stem or a re-serialization
/// failure.
pub fn roundtrips_as_root(stem: &str, sample: &Value) -> Result<bool> {
    use pa_types::daemon::{
        DaemonCommand, DaemonCommandEnvelope, DaemonCommandWire, DaemonErrorInfo,
        DaemonEventEnvelope, DaemonEventMeta, DaemonOutbound, DaemonResponse, DaemonResumeCursor,
    };
    fn roundtrips<T: serde::de::DeserializeOwned + serde::Serialize>(
        sample: &Value,
    ) -> Result<bool> {
        let parsed: T = serde_json::from_value(sample.clone())?;
        let out = serde_json::to_value(&parsed)?;
        Ok(out == *sample)
    }
    Ok(match stem {
        "daemon-command" => roundtrips::<DaemonCommand>(sample)?,
        "daemon-command-envelope" => roundtrips::<DaemonCommandEnvelope>(sample)?,
        "daemon-command-wire" => roundtrips::<DaemonCommandWire>(sample)?,
        "daemon-outbound" => roundtrips::<DaemonOutbound>(sample)?,
        "daemon-response" => roundtrips::<DaemonResponse>(sample)?,
        "daemon-error-info" => roundtrips::<DaemonErrorInfo>(sample)?,
        "daemon-event-envelope" => roundtrips::<DaemonEventEnvelope>(sample)?,
        "daemon-event-meta" => roundtrips::<DaemonEventMeta>(sample)?,
        "daemon-resume-cursor" => roundtrips::<DaemonResumeCursor>(sample)?,
        "agent-message" => roundtrips::<pa_types::session::AgentMessage>(sample)?,
        "client-inbound-frame" => sample.is_object() && sample.get("type").is_some(),
        _ => anyhow::bail!("unknown schema root: {stem}"),
    })
}

/// Corrupted variants must fail the same Rust parse that accepts the witness.
///
/// # Panics
///
/// Panics only via the schema-root helpers on malformed generation input.
///
/// # Errors
///
/// Returns `Err` for an unknown schema-root stem.
pub fn rejects_as_root(stem: &str, sample: &Value) -> Result<bool> {
    use pa_types::daemon::{
        DaemonCommand, DaemonCommandEnvelope, DaemonCommandWire, DaemonErrorInfo,
        DaemonEventEnvelope, DaemonEventMeta, DaemonOutbound, DaemonResponse, DaemonResumeCursor,
    };
    use pa_types::session::AgentMessage;
    fn rejects<T: serde::de::DeserializeOwned>(sample: &Value) -> bool {
        serde_json::from_value::<T>(sample.clone()).is_err()
    }
    Ok(match stem {
        "daemon-command" => rejects::<DaemonCommand>(sample),
        "daemon-command-envelope" => rejects::<DaemonCommandEnvelope>(sample),
        "daemon-command-wire" => rejects::<DaemonCommandWire>(sample),
        "daemon-outbound" => rejects::<DaemonOutbound>(sample),
        "daemon-response" => rejects::<DaemonResponse>(sample),
        "daemon-error-info" => rejects::<DaemonErrorInfo>(sample),
        "daemon-event-envelope" => rejects::<DaemonEventEnvelope>(sample),
        "daemon-event-meta" => rejects::<DaemonEventMeta>(sample),
        "daemon-resume-cursor" => rejects::<DaemonResumeCursor>(sample),
        "agent-message" => rejects::<AgentMessage>(sample),
        "client-inbound-frame" => {
            // The open envelope: only its own rejects-file cases run here.
            !(sample.get("type").and_then(Value::as_str).is_some() && sample.is_object())
        }
        _ => anyhow::bail!("unknown schema root: {stem}"),
    })
}

/// File-name -> content map of the canonical export (drift gate input).
///
/// # Panics
///
/// Panics when a schema branch is not synthesizable (see [`export`]).
///
/// # Errors
///
/// Returns `Err` when the export or a read fails. The temporary export
/// directory is best-effort removed afterwards.
pub fn export_map() -> Result<BTreeMap<String, String>> {
    let dir = std::env::temp_dir().join(format!("pa-contract-export-{}", std::process::id()));
    export(&dir)?;
    let mut map = BTreeMap::new();
    for entry in walk(&dir) {
        map.insert(
            entry
                .strip_prefix(&dir)
                .unwrap()
                .to_string_lossy()
                .to_string(),
            std::fs::read_to_string(&entry).context("read exported")?,
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
    Ok(map)
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}
