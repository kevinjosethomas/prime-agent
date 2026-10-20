//! Restore existing children without turning historical completion into a new spawn.
use std::collections::HashSet;
use std::path::Path;

use anyhow::{bail, Context, Result};
use pa_types::daemon::update_flow::{
    UpdateRoster, UpdateRosterInFlight, UpdateRosterSession, UpdateRosterSessionKind,
    UpdateRosterSubagentStatus,
};
use serde_json::{json, Value};

use crate::lease::canonical_session_path;
use crate::rlm_ledger::{read_rlm_subagent_display, RlmLedgerEdge};

/// Unknown queue formats retain their worker rather than risking a lost input.
fn empty_actions(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Array(items) => items.is_empty(),
        Value::Object(fields) => fields.iter().all(|(key, value)| match key.as_str() {
            "actions" => empty_actions(value),
            "steering" | "followUps" => value.as_array().is_some_and(Vec::is_empty),
            "queuedCount" => value.as_u64() == Some(0),
            "formatVersion" => value.as_u64() == Some(1),
            "activeInputOwnership" => value.as_object().is_some_and(|ownership| {
                ownership.iter().all(|(key, value)| match key.as_str() {
                    "state" => value.as_str() == Some("unknown"),
                    "inputIds" | "replyToMessageIds" => value.as_array().is_some_and(Vec::is_empty),
                    _ => false,
                })
            }),
            _ => false,
        }),
        Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

/// Prune only proven completed, quiescent children from this in-memory restart plan.
/// Their topology rows and every persisted file remain untouched. Return cold workers
/// that must stay passive even if their old recovery journal still says interrupted.
/// A still-live process is always adopted by the normal adoption path.
pub(crate) fn prepare_roster(roster: &mut UpdateRoster) -> HashSet<String> {
    let passive: HashSet<String> = roster
        .sessions
        .iter()
        .filter(|row| {
            row.kind == UpdateRosterSessionKind::Subagent
                && !row.should_resume
                && row.in_flight == UpdateRosterInFlight::default()
                && row.queue.next_turn.is_empty()
                && empty_actions(&row.queue.actions)
                && roster.subagents.iter().any(|child| {
                    child.session_id == row.session_id
                        && child.status == UpdateRosterSubagentStatus::Completed
                        && canonical_session_path(Path::new(&child.session_file))
                            == canonical_session_path(Path::new(&row.session_file))
                        && Path::new(&row.session_file).parent().is_some_and(|dir| {
                            read_rlm_subagent_display(dir).is_some_and(|display| {
                                display.status == "completed"
                                    && display.child_id == child.child_id
                                    && canonical_session_path(Path::new(&display.session_file))
                                        == canonical_session_path(Path::new(&row.session_file))
                            })
                        })
                })
        })
        .map(|row| row.session_id.clone())
        .collect();
    roster
        .sessions
        .retain(|row| !passive.contains(&row.session_id));
    let mut passive_workers = HashSet::new();
    for worker in &mut roster.workers {
        let had_sessions = !worker.sessions.is_empty();
        worker.sessions.retain(|id| !passive.contains(id));
        if had_sessions && worker.sessions.is_empty() {
            passive_workers.insert(worker.worker_id.clone());
        }
    }
    roster
        .workers
        .retain(|worker| !passive_workers.contains(&worker.worker_id));
    passive_workers
}

/// Resolve one existing edge by its durable child file, with no parent-handle or
/// directory-name inference. Missing, ambiguous, deleted and mismatched edges fail closed.
pub(crate) fn verified_edge<'a>(
    edges: &'a [RlmLedgerEdge],
    child: &Path,
    child_id: Option<&str>,
    depth: Option<u32>,
) -> Result<&'a RlmLedgerEdge> {
    let path = canonical_session_path(child);
    let mut matching = edges.iter().filter(|edge| {
        edge.deleted.is_none() && canonical_session_path(Path::new(&edge.child)) == path
    });
    let edge = matching
        .next()
        .context("saved child has no verified live ledger edge")?;
    if matching.next().is_some() {
        bail!("saved child has ambiguous ledger parents");
    }
    if child_id.is_some_and(|id| id != edge.child_id)
        || depth.is_some_and(|depth| depth != edge.depth)
    {
        bail!("saved child identity or depth differs from its ledger edge");
    }
    if !path.is_file() || !Path::new(&edge.parent).is_file() {
        bail!("saved child or its ledger parent file is missing");
    }
    Ok(edge)
}

/// Restore payloads are clones: recover legacy metadata from the existing edge without
/// rewriting the snapshot, transcript, graph, display entry, timestamps or queued inputs.
pub(super) fn hydrate_create(
    payload: &mut Value,
    row: &UpdateRosterSession,
    edges: &[RlmLedgerEdge],
) -> Result<()> {
    if row.kind != UpdateRosterSessionKind::Subagent {
        return Ok(());
    }
    let metadata = payload.get("runtimeMetadata");
    let child_id = metadata
        .and_then(|value| value.get("rlmChildId"))
        .and_then(Value::as_str)
        .or_else(|| payload.get("rlmChildId").and_then(Value::as_str));
    // Legacy snapshots used zero when runtime metadata carried no depth.
    // Only the unique durable ledger edge may supply that missing value.
    let depth = (row.rlm_depth > 0).then_some(row.rlm_depth);
    let edge = verified_edge(edges, Path::new(&row.session_file), child_id, depth)?;
    if payload
        .get("rlmChildId")
        .and_then(Value::as_str)
        .is_some_and(|id| id != edge.child_id)
        || payload
            .get("rlmDepth")
            .and_then(Value::as_u64)
            .is_some_and(|depth| depth != u64::from(edge.depth))
    {
        bail!("saved child create identity conflicts with its ledger edge");
    }
    if metadata
        .and_then(|value| value.get("parentSessionFile"))
        .and_then(Value::as_str)
        .is_some_and(|parent| {
            canonical_session_path(Path::new(parent))
                != canonical_session_path(Path::new(&edge.parent))
        })
    {
        bail!("saved child parent metadata conflicts with its ledger edge");
    }
    if metadata
        .and_then(|value| value.get("rlmDepth"))
        .and_then(Value::as_u64)
        .is_some_and(|depth| depth != u64::from(edge.depth))
    {
        bail!("saved child depth metadata conflicts with its ledger edge");
    }
    let fields = payload
        .as_object_mut()
        .context("restore create payload is not an object")?;
    let metadata = fields.entry("runtimeMetadata").or_insert_with(|| json!({}));
    let metadata = metadata
        .as_object_mut()
        .context("saved child metadata is not an object")?;
    if metadata
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind != "subagent")
    {
        bail!("saved child metadata kind is not subagent");
    }
    metadata.insert("kind".into(), json!("subagent"));
    metadata.insert("rlmChildId".into(), json!(edge.child_id));
    metadata.insert("rlmDepth".into(), json!(edge.depth));
    metadata.insert("parentSessionFile".into(), json!(edge.parent));
    Ok(())
}

#[cfg(test)]
mod tests;
