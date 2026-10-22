//! Bounded, read-only pages of the durable active branch before compaction.
//! Legacy tree consumers keep their full-tree response; desktop readers opt in.

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};

use crate::session_store::{SessionEntry, SessionFile};

const MAX_PAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_PAGE_ENTRIES: usize = 100;

pub(crate) fn history_page(store: &SessionFile, request: &Value) -> Result<Value> {
    page_from_entries(store.entries(), store.leaf_id(), request)
}

fn page_from_entries(
    entries: &[SessionEntry],
    leaf: Option<&str>,
    request: &Value,
) -> Result<Value> {
    if !request.is_object() {
        bail!("Invalid history page request");
    }
    let by_id: HashMap<_, _> = entries
        .iter()
        .map(|entry| (entry.id.as_str(), entry))
        .collect();
    if by_id.len() != entries.len() {
        bail!("History branch has duplicate entry ids");
    }
    // This deliberately walks durable entries, not the model's compacted window.
    let leaf = leaf.ok_or_else(|| anyhow!("History has no active branch"))?;
    let mut id = Some(leaf);
    let mut branch = Vec::new();
    let mut seen = HashSet::new();
    while let Some(current) = id {
        if !seen.insert(current) {
            bail!("History branch contains a cycle");
        }
        let entry = by_id
            .get(current)
            .ok_or_else(|| anyhow!("History branch is incomplete"))?;
        branch.push(*entry);
        id = entry.parent_id.as_deref();
    }
    branch.reverse();
    let anchor = request.get("anchorLeafId").map_or(Ok(leaf), |value| {
        value
            .as_str()
            .ok_or_else(|| anyhow!("Invalid history branch anchor"))
    })?;
    if !seen.contains(anchor) {
        bail!("History branch changed; reopen the chat and retry");
    }
    let compaction_index = branch
        .iter()
        .rposition(|entry| entry.type_ == "compaction")
        .ok_or_else(|| anyhow!("History has no compaction boundary"))?;
    let compaction = branch[compaction_index];
    if let Some(expected) = request.get("boundaryId") {
        if expected.as_str() != Some(compaction.id.as_str()) {
            bail!("History compaction changed; reopen the chat and retry");
        }
    }
    let kept = compaction
        .fields
        .get("firstKeptEntryId")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("History compaction boundary is missing"))?;
    let boundary = branch
        .iter()
        .position(|entry| entry.id == kept)
        .filter(|index| *index < compaction_index)
        .ok_or_else(|| anyhow!("History compaction boundary is invalid"))?;
    let first_kept = branch[boundary..]
        .iter()
        .find(|entry| entry.type_ == "message" || entry.type_ == "custom_message")
        .ok_or_else(|| anyhow!("History retained message is missing"))?;
    let end = match request.get("beforeEntryId") {
        None => boundary,
        Some(value) => branch
            .iter()
            .position(|entry| Some(entry.id.as_str()) == value.as_str())
            .filter(|index| *index < boundary)
            .ok_or_else(|| anyhow!("History page cursor is invalid"))?,
    };
    let mut data = json!({
        "version": 1,
        "anchorLeafId": anchor,
        "boundaryId": compaction.id,
        "compaction": compaction,
        "firstKeptEntry": first_kept,
        "entries": [],
        "nextBeforeEntryId": null,
    });
    // Reserve envelope and cursor overhead as well as the actual proof bytes.
    let mut bytes = serde_json::to_vec(&data)?.len() + 4096;
    if bytes > MAX_PAGE_BYTES {
        bail!("History boundary exceeds the page limit; no messages were changed");
    }
    let mut rows = Vec::new();
    let mut start = end;
    for entry in branch[..end].iter().rev().take(MAX_PAGE_ENTRIES) {
        let row = serde_json::to_value(entry)?;
        let size = serde_json::to_vec(&row)?.len() + 1;
        if bytes + size > MAX_PAGE_BYTES {
            if rows.is_empty() {
                bail!("A history entry exceeds the page limit; no messages were changed");
            }
            break;
        }
        bytes += size;
        rows.push(row);
        start -= 1;
    }
    rows.reverse();
    data["entries"] = Value::Array(rows);
    if start > 0 {
        data["nextBeforeEntryId"] = json!(branch[start].id);
    }
    Ok(data)
}

#[cfg(test)]
#[path = "history_page_tests.rs"]
mod tests;
