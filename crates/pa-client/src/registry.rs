//! The multi-attach registry: per-session cursor/capability bookkeeping and
//! replay dedupe, the wire-contract half of the managed lifecycle (the
//! desktop machine's attachment map, ported verbatim and bounded). Pure
//! synchronous state behind the session's `std::sync::Mutex` — the mutex is
//! never held across an await.

use std::collections::HashMap;

use pa_types::daemon::DaemonResumeCursor;
use serde_json::Value;

/// What the driver does with one raw frame after registry observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameAction {
    /// Forward the frame to the consumer.
    Deliver,
    /// A replayed session event inside a registered attachment's current
    /// generation: suppressed (the driver forwards nothing for it).
    Dedupe,
}

/// One session attachment: the cursor to resume from, the capabilities to
/// re-attach with, and the revision that guards response adoption.
#[derive(Debug, Clone)]
pub(crate) struct Attachment {
    /// The resume cursor: `None` until the first attach reports one —
    /// missing (no `resumeCursor` sent on restore) is distinct from null
    /// adoption and both are distinct from no attachment.
    pub cursor: Option<DaemonResumeCursor>,
    /// The capabilities to re-attach with (the registry never invents
    /// them: recorded from the attach command or the session config).
    pub capabilities: Vec<String>,
    /// Bumped per attach/reattach attempt: a response adopts only at its
    /// own revision, so a canceled/detached/superseded attempt can never
    /// resurrect or roll back an attachment.
    pub revision: u64,
}

/// The ordered multi-attach registry. Dedupe and cursor adoption apply ONLY
/// to registered attachments; an unregistered session's frames always
/// deliver. The seen map holds markers only for live registered
/// attachments — detach/rebind drop them, so it is bounded by the
/// attachment set, never by session churn.
#[derive(Debug, Default)]
pub(crate) struct AttachmentRegistry {
    /// Insertion order (restore visits entries in it).
    order: Vec<String>,
    attachments: HashMap<String, Attachment>,
    /// `(active_session_id, generation)` -> the last delivered sequence.
    seen: HashMap<(String, String), u64>,
}

impl AttachmentRegistry {
    /// Register (or re-register) an attachment; idempotent per id — an
    /// existing cursor and a higher existing revision survive a re-register.
    pub(crate) fn register(&mut self, id: &str, capabilities: Vec<String>) {
        if let Some(entry) = self.attachments.get_mut(id) {
            if entry.capabilities.is_empty() {
                entry.capabilities = capabilities;
            }
            return;
        }
        if self
            .attachments
            .insert(
                id.to_string(),
                Attachment {
                    cursor: None,
                    capabilities,
                    revision: 0,
                },
            )
            .is_none()
        {
            self.order.push(id.to_string());
        }
    }

    /// Bump and return the attachment's revision for one attach/reattach
    /// attempt; `None` when the id is not registered (a fresh attach
    /// self-registers on response adoption).
    pub(crate) fn bump_revision(&mut self, id: &str) -> Option<u64> {
        self.attachments.get_mut(id).map(|entry| {
            entry.revision += 1;
            entry.revision
        })
    }

    /// Drop an attachment and every marker it owns (detach/kill/close).
    pub(crate) fn forget(&mut self, id: &str) {
        if self.attachments.remove(id).is_some() {
            self.order.retain(|registered| registered != id);
        }
        self.seen.retain(|(marker_id, _), _| marker_id != id);
    }

    /// Adopt an attach/reattach response into the attachment. The desktop
    /// machine's `updateCursorFromSnapshot`, verbatim in its guards:
    /// the generation cursor shape or the legacy `eventSequence` shape,
    /// monotonic-only (a snapshot never rolls a cursor or a seen marker
    /// back), capability refresh, and the seen marker catch-up. The
    /// `revision` guard refuses adoption when the attachment was detached,
    /// superseded by a concurrent attempt (`Some` mismatch), or created
    /// fresh in the meantime (`None` guard expects unregistered-at-send).
    pub(crate) fn record_attach(
        &mut self,
        id: &str,
        capabilities: &[String],
        data: &Value,
        revision: Option<u64>,
    ) -> bool {
        let entry = match (revision, self.attachments.get_mut(id)) {
            // The attempt's own revision: adopt into the live entry.
            (Some(expected), Some(entry)) if entry.revision == expected => entry,
            // An unguarded attach (no revision tracked): adopt, creating the
            // entry when the id was never registered.
            (None, Some(entry)) => entry,
            (None, None) => self
                .attachments
                .entry(id.to_string())
                .or_insert(Attachment {
                    cursor: None,
                    capabilities: capabilities.to_vec(),
                    revision: 0,
                }),
            // Superseded by a later attempt, or detached: never adopt.
            _ => return false,
        };
        if !capabilities.is_empty() {
            entry.capabilities = capabilities.to_vec();
        }
        let snapshot = data.get("snapshot").unwrap_or(data);
        let current = entry.cursor.clone();
        if let Some(cursor) = snapshot.get("lastEventCursor").and_then(parse_cursor) {
            if current
                .as_ref()
                .is_none_or(|c| c.generation != cursor.generation)
                || current.as_ref().and_then(|c| c.sequence).unwrap_or(0)
                    < cursor.sequence.unwrap_or(0)
            {
                entry.cursor = Some(cursor);
            }
        } else if let Some(sequence) = snapshot.get("lastEventSequence").and_then(Value::as_u64) {
            if current.as_ref().and_then(|c| c.event_sequence).unwrap_or(0) < sequence {
                entry.cursor = Some(DaemonResumeCursor {
                    active_session_id: Some(id.to_string()),
                    generation: None,
                    sequence: None,
                    event_sequence: Some(sequence),
                });
            }
        }
        if entry.cursor.is_some() && self.order.iter().all(|registered| registered != id) {
            self.order.push(id.to_string());
        }
        let (sequence, generation) = match entry.cursor.as_ref() {
            Some(cursor) if cursor.sequence.is_some() => {
                (cursor.sequence, cursor.generation.clone())
            }
            Some(cursor) if cursor.event_sequence.is_some() => (cursor.event_sequence, None),
            _ => (None, None),
        };
        if let Some(sequence) = sequence {
            let marker = (id.to_string(), generation.unwrap_or_default());
            let last = self.seen.entry(marker).or_insert(0);
            *last = (*last).max(sequence);
        }
        true
    }

    /// Observe one raw outbound frame for bookkeeping (never consumes it):
    /// session events adopt cursors and decide dedupe for REGISTERED
    /// attachments only, `session_binding` rekeys the entry and its
    /// markers, `session_closed` drops the entry and its markers.
    pub(crate) fn observe_frame(&mut self, frame: &Value) -> FrameAction {
        match frame
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "session_event" => self.observe_session_event(frame),
            "session_binding" => self.observe_binding(frame),
            "session_closed" => {
                if let Some(id) = frame.get("activeSessionId").and_then(Value::as_str) {
                    self.forget(id);
                }
                FrameAction::Deliver
            }
            _ => FrameAction::Deliver,
        }
    }

    fn observe_session_event(&mut self, frame: &Value) -> FrameAction {
        let Some(id) = frame.get("activeSessionId").and_then(Value::as_str) else {
            return FrameAction::Deliver;
        };
        let meta = frame.get("meta");
        let sequence = meta
            .and_then(|meta| {
                meta.get("cursor")
                    .and_then(|cursor| cursor.get("sequence"))
                    .or_else(|| meta.get("sequence"))
            })
            .and_then(Value::as_u64);
        let Some(sequence) = sequence else {
            return FrameAction::Deliver;
        };
        if !self.attachments.contains_key(id) {
            return FrameAction::Deliver;
        }
        let generation = meta
            .and_then(|meta| meta.get("cursor"))
            .and_then(|cursor| cursor.get("generation"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let marker = (id.to_string(), generation.clone());
        let last = self.seen.entry(marker).or_insert(0);
        if sequence <= *last {
            return FrameAction::Dedupe;
        }
        *last = sequence;
        let entry = self.attachments.get_mut(id).expect("registered above");
        if generation.is_empty() {
            let previous = entry
                .cursor
                .as_ref()
                .and_then(|cursor| cursor.event_sequence);
            if previous.unwrap_or(0) < sequence {
                entry.cursor = Some(DaemonResumeCursor {
                    active_session_id: Some(id.to_string()),
                    generation: None,
                    sequence: None,
                    event_sequence: Some(sequence),
                });
            }
        } else {
            entry.cursor = Some(DaemonResumeCursor {
                active_session_id: Some(id.to_string()),
                generation: Some(generation),
                sequence: Some(sequence),
                event_sequence: None,
            });
        }
        FrameAction::Deliver
    }

    fn observe_binding(&mut self, frame: &Value) -> FrameAction {
        let (Some(previous), Some(next)) = (
            frame.get("previousActiveSessionId").and_then(Value::as_str),
            frame.get("activeSessionId").and_then(Value::as_str),
        ) else {
            return FrameAction::Deliver;
        };
        if let Some(entry) = self.attachments.remove(previous) {
            self.attachments.insert(next.to_string(), entry);
            self.order.retain(|registered| registered != previous);
            self.order.push(next.to_string());
        }
        // The markers follow the attachment: the bound session's events
        // continue its sequence line under the new id.
        let stale: Vec<(String, String)> = self
            .seen
            .keys()
            .filter(|(marker_id, _)| marker_id == previous)
            .cloned()
            .collect();
        for (marker_id, generation) in stale {
            let last = self
                .seen
                .remove(&(marker_id, generation.clone()))
                .unwrap_or(0);
            self.seen.insert((next.to_string(), generation), last);
        }
        FrameAction::Deliver
    }

    /// The restore set, in registration order. `capabilities` falls back to
    /// the config default at the caller; the cursor rides only when one
    /// exists (missing-vs-null preserved).
    pub(crate) fn restore_entries(&self) -> Vec<(String, Attachment)> {
        self.order
            .iter()
            .filter_map(|id| {
                self.attachments
                    .get(id)
                    .map(|entry| (id.clone(), entry.clone()))
            })
            .collect()
    }

}

/// The wire shape of a snapshot's `lastEventCursor`.
fn parse_cursor(value: &Value) -> Option<DaemonResumeCursor> {
    let generation = value.get("generation").and_then(Value::as_str)?.to_string();
    let sequence = value.get("sequence").and_then(Value::as_u64)?;
    Some(DaemonResumeCursor {
        active_session_id: None,
        generation: Some(generation),
        sequence: Some(sequence),
        event_sequence: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(generation: &str, sequence: u64) -> Value {
        json!({
            "type": "session_event",
            "activeSessionId": "s1",
            "event": { "type": "delta" },
            "meta": { "cursor": { "generation": generation, "sequence": sequence } },
        })
    }

    #[test]
    fn unregistered_events_always_deliver() {
        let mut registry = AttachmentRegistry::default();
        assert_eq!(registry.observe_frame(&event("g", 1)), FrameAction::Deliver);
        assert_eq!(registry.observe_frame(&event("g", 1)), FrameAction::Deliver);
        assert!(registry.restore_entries().is_empty());
    }

    #[test]
    fn dedupe_within_generation_and_accept_new_generation() {
        let mut registry = AttachmentRegistry::default();
        registry.register("s1", vec!["attach_snapshot".to_string()]);
        let actions: Vec<FrameAction> = [("g1", 2), ("g1", 2), ("g1", 1), ("g2", 1)]
            .iter()
            .map(|(generation, sequence)| registry.observe_frame(&event(generation, *sequence)))
            .collect();
        assert_eq!(
            actions,
            vec![
                FrameAction::Deliver,
                FrameAction::Dedupe,
                FrameAction::Dedupe,
                FrameAction::Deliver,
            ]
        );
        let (_, entry) = registry.restore_entries().remove(0);
        let cursor = entry.cursor.unwrap();
        assert_eq!(cursor.generation.as_deref(), Some("g2"));
        assert_eq!(cursor.sequence, Some(1));
    }

    #[test]
    fn snapshot_adoption_is_monotonic_and_dual_shape() {
        let mut registry = AttachmentRegistry::default();
        registry.register("s1", vec![]);
        // The generation shape.
        assert!(registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 4 } }),
            None
        ));
        // A stale snapshot never rolls back.
        assert!(registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 2 } }),
            None
        ));
        let entry = registry.restore_entries()[0].1.clone();
        assert_eq!(entry.cursor.as_ref().unwrap().sequence, Some(4));
        // The legacy shape.
        assert!(registry.record_attach("s1", &[], &json!({ "lastEventSequence": 7 }), None,));
        let entry = registry.restore_entries()[0].1.clone();
        assert_eq!(entry.cursor.as_ref().unwrap().event_sequence, Some(7));
        // The seen marker rode along: a replayed sequence dedupes.
        let replay = json!({
            "type": "session_event",
            "activeSessionId": "s1",
            "event": { "type": "delta" },
            "meta": { "sequence": 7 },
        });
        assert_eq!(registry.observe_frame(&replay), FrameAction::Dedupe);
    }

    #[test]
    fn revision_guard_refuses_stale_adoption() {
        let mut registry = AttachmentRegistry::default();
        registry.register("s1", vec![]);
        let revision = registry.bump_revision("s1").unwrap();
        // The current attempt adopts.
        assert!(registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 5 } }),
            Some(revision)
        ));
        // A superseded attempt never adopts: the revision moved past it.
        let superseded = registry.bump_revision("s1").unwrap();
        assert!(superseded > revision);
        assert!(!registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 9 } }),
            Some(revision)
        ));
        // The current attempt still adopts at its own revision.
        assert!(registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 6 } }),
            Some(superseded)
        ));
        registry.forget("s1");
        assert!(!registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 9 } }),
            Some(revision)
        ));
        assert!(registry.record_attach(
            "s2",
            &["event_sequence".to_string()],
            &json!({ "lastEventSequence": 1 }),
            None
        ));
        assert_eq!(registry.restore_entries().len(), 1);
        assert!(registry.restore_entries()[0].1.cursor.is_some());
    }

    #[test]
    fn binding_rekeys_and_closed_drops_with_markers() {
        let mut registry = AttachmentRegistry::default();
        registry.register("s1", vec![]);
        registry.observe_frame(&event("g", 3));
        let binding = json!({
            "type": "session_binding",
            "previousActiveSessionId": "s1",
            "activeSessionId": "s2",
        });
        assert_eq!(registry.observe_frame(&binding), FrameAction::Deliver);
        assert!(registry.restore_entries().iter().any(|(id, _)| id == "s2"));
        assert!(registry.restore_entries().iter().all(|(id, _)| id != "s1"));
        // The marker moved with the attachment: the replay dedupes under the
        // new id.
        let replay = json!({
            "type": "session_event",
            "activeSessionId": "s2",
            "event": { "type": "delta" },
            "meta": { "cursor": { "generation": "g", "sequence": 3 } },
        });
        assert_eq!(registry.observe_frame(&replay), FrameAction::Dedupe);
        // A close drops the attachment and every marker it owned.
        let closed = json!({ "type": "session_closed", "activeSessionId": "s2" });
        assert_eq!(registry.observe_frame(&closed), FrameAction::Deliver);
        assert!(registry.restore_entries().is_empty());
        assert_eq!(registry.observe_frame(&replay), FrameAction::Deliver);
    }
}
