use super::*;

fn entry(id: usize, type_: &str, fields: Value) -> SessionEntry {
    SessionEntry {
        type_: type_.to_string(),
        id: format!("e{id}"),
        parent_id: id.checked_sub(1).map(|parent| format!("e{parent}")),
        timestamp: "2026-10-22T00:00:00Z".to_string(),
        fields,
    }
}

fn fixture(count: usize, text_bytes: usize) -> Vec<SessionEntry> {
    let mut entries: Vec<_> = (0..count)
        .map(|id| entry(id, "message", json!({"message": {
            "role": "user", "timestamp": id, "content": [{"type": "text", "text": "x".repeat(text_bytes)}]
        }})))
        .collect();
    entries.push(entry(
        count,
        "compaction",
        json!({
            "firstKeptEntryId": format!("e{}", count - 1), "summary": "summary", "tokensBefore": 42
        }),
    ));
    entries
}

#[test]
fn pages_recover_a_tree_larger_than_transport_limit_without_losing_or_reordering_entries() {
    let entries = fixture(160, 450_000);
    assert!(serde_json::to_vec(&entries).unwrap().len() > 64 * 1024 * 1024);
    let original = serde_json::to_vec(&entries).unwrap();
    let mut request = json!({});
    let mut recovered = Vec::new();
    let mut pages = 0;
    loop {
        let page = page_from_entries(&entries, Some("e160"), &request).unwrap();
        assert!(serde_json::to_vec(&page).unwrap().len() < MAX_PAGE_BYTES);
        let rows = page["entries"].as_array().unwrap();
        assert!(rows.len() <= MAX_PAGE_ENTRIES);
        assert!(!rows.is_empty());
        recovered.splice(0..0, rows.iter().cloned());
        pages += 1;
        let Some(before) = page["nextBeforeEntryId"].as_str() else {
            break;
        };
        request = json!({"beforeEntryId": before, "anchorLeafId": page["anchorLeafId"], "boundaryId": page["boundaryId"]});
    }
    assert!(pages > 1);
    assert_eq!(
        recovered,
        entries[..159]
            .iter()
            .map(|entry| serde_json::to_value(entry).unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(original, serde_json::to_vec(&entries).unwrap());
}

#[test]
fn cursors_allow_appends_but_reject_branch_moves_and_new_compactions() {
    let mut entries = fixture(105, 0);
    let page = page_from_entries(&entries, Some("e105"), &json!({})).unwrap();
    let request = json!({"beforeEntryId": page["nextBeforeEntryId"], "anchorLeafId": page["anchorLeafId"], "boundaryId": page["boundaryId"]});
    entries.push(entry(
        106,
        "message",
        json!({"message": {"role":"assistant"}}),
    ));
    let continued = page_from_entries(&entries, Some("e106"), &request).unwrap();
    assert_eq!(continued["entries"].as_array().unwrap().len(), 4);
    assert!(page_from_entries(&entries, Some("e3"), &request).is_err());
    entries.push(entry(107, "compaction", json!({"firstKeptEntryId":"e103"})));
    assert!(page_from_entries(&entries, Some("e107"), &request).is_err());
}

#[test]
fn huge_entry_is_reported_instead_of_skipped_and_bad_ancestry_never_falls_back_to_file_order() {
    let entries = fixture(2, MAX_PAGE_BYTES);
    assert!(page_from_entries(&entries, Some("e2"), &json!({})).is_err());
    let mut entries = fixture(3, 0);
    entries[1].parent_id = Some("missing".to_string());
    assert!(page_from_entries(&entries, Some("e3"), &json!({})).is_err());
    entries[1].parent_id = Some("e2".to_string());
    assert!(page_from_entries(&entries, Some("e3"), &json!({})).is_err());
}

#[test]
fn empty_older_branch_finishes_and_abandoned_messages_are_excluded() {
    let mut entries = fixture(1, 0);
    let complete = page_from_entries(&entries, Some("e1"), &json!({})).unwrap();
    assert_eq!(complete["entries"], json!([]));
    assert_eq!(complete["nextBeforeEntryId"], Value::Null);
    entries.insert(
        1,
        SessionEntry {
            id: "abandoned".to_string(),
            ..entry(90, "message", json!({"message": {"role":"user"}}))
        },
    );
    assert_eq!(
        page_from_entries(&entries, Some("e1"), &json!({})).unwrap(),
        complete
    );
}
