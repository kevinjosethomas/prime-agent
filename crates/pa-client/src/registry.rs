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
    /// A replayed session event within the attachment's CURRENT generation:
    /// suppressed (the driver forwards nothing for it).
    Dedupe,
}

/// One session attachment: the cursor to resume from, the capabilities to
/// re-attach with, and the in-flight attempt token that guards adoption.
#[derive(Debug, Clone)]
pub(crate) struct Attachment {
    /// The resume cursor: `None` until an adoption reports one — a missing
    /// cursor (no `resumeCursor` sent on restore) is distinct from no
    /// attachment.
    pub cursor: Option<DaemonResumeCursor>,
    /// The capabilities to re-attach with (the attach attempt's own
    /// capabilities, recorded at adoption).
    pub capabilities: Vec<String>,
    /// The token of the attempt currently in flight for this entry: an
    /// attach response adopts only at its own token.
    pub attempt: u64,
}

/// The ordered multi-attach registry. Dedupe and cursor adoption apply ONLY
/// to registered attachments; an unregistered session's frames always
/// deliver.
///
/// # The generation contract (desktop parity, documented honestly)
///
/// Sequences are monotonic WITHIN a generation; generations are OPAQUE and
/// sequences are NOT ordered across them — the registry never compares
/// sequences across generations. ONE CURRENT marker exists per attachment
/// (`(generation, last-sequence-within-it)`, the prior generation's marker
/// forgotten), so the map is bounded by the attachment set. Dedupe applies
/// only when an event's generation matches the current marker's. A frame
/// whose generation differs replaces the marker slot and the cursor — the
/// desktop machine's own generation replacement, verbatim. A LATE
/// OLD-GENERATION FRAME (out of order across a transition) is therefore
/// possible and REPLACES the marker/cursor the same way: a known parity
/// limitation, explicit here; no cross-generation ordering or authoritative
/// boundary semantics are claimed or implemented.
#[derive(Debug, Default)]
pub(crate) struct AttachmentRegistry {
    /// Insertion order (restore visits entries in it).
    order: Vec<String>,
    attachments: HashMap<String, Attachment>,
    /// id -> (generation, the last delivered sequence within it). ONE
    /// CURRENT marker per attachment: a transition replaces the slot, so
    /// the map is bounded by the attachment set, never by churn.
    seen: HashMap<String, (String, u64)>,
    /// Fresh (unregistered-id) attach intents: id -> the in-flight attempt
    /// token. `forget` drops the slot; a stale token can never match again.
    intents: HashMap<String, u64>,
    /// The global monotonic attempt counter: every token is unique for the
    /// registry's lifetime — no ABA across forget/reinsert.
    attempts: u64,
}

impl AttachmentRegistry {
    /// Register (or re-register) an attachment; idempotent per id — an
    /// existing cursor and in-flight attempt survive a re-register.
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
                    attempt: 0,
                },
            )
            .is_none()
        {
            self.order.push(id.to_string());
        }
    }

    /// Mint the attempt token for one attach/reattach: registered ids pin
    /// the token on the entry; unregistered ids hold it as a fresh intent.
    /// Globally monotonic, so a forget mid-flight invalidates the attempt
    /// forever (the dangling token can never match again) and a reinserted
    /// entry only ever adopts at its OWN later token.
    pub(crate) fn attempt_token(&mut self, id: &str) -> u64 {
        self.attempts += 1;
        let token = self.attempts;
        match self.attachments.get_mut(id) {
            Some(entry) => entry.attempt = token,
            None => {
                self.intents.insert(id.to_string(), token);
            }
        }
        token
    }

    /// Drop an attachment and every marker and intent it owns
    /// (detach/kill/close): an in-flight attempt's token dangles and can
    /// never adopt afterwards.
    pub(crate) fn forget(&mut self, id: &str) {
        if self.attachments.remove(id).is_some() {
            self.order.retain(|registered| registered != id);
        }
        self.seen.remove(id);
        self.intents.remove(id);
    }

    /// Adopt an attach/reattach response into the attachment, ONLY at the
    /// attempt's own token. The desktop machine's `updateCursorFromSnapshot`
    /// guards, verbatim: the generation-cursor shape or the legacy
    /// `eventSequence` shape, monotonic within the current marker (a
    /// snapshot never rolls a cursor back), the capabilities recorded from
    /// the attempt itself, the marker catch-up, and order membership on
    /// attachment EXISTENCE (a cursorless attach is as restored as one
    /// with a cursor). A fresh id adopts only through its live intent; a
    /// registered entry only at its pinned token.
    pub(crate) fn record_attach(
        &mut self,
        id: &str,
        capabilities: &[String],
        data: &Value,
        token: u64,
    ) -> bool {
        let entry = match self.attachments.get_mut(id) {
            // The live entry at the attempt's own token.
            Some(entry) if entry.attempt == token => entry,
            // A fresh id's live intent: create the entry.
            None if self.intents.get(id) == Some(&token) => {
                self.intents.remove(id);
                let entry = self
                    .attachments
                    .entry(id.to_string())
                    .or_insert(Attachment {
                        cursor: None,
                        capabilities: Vec::new(),
                        attempt: token,
                    });
                entry.attempt = token;
                entry
            }
            // Superseded, detached, or stale (a forget/reinsert cycle moved
            // the tokens): never adopt, never resurrect.
            _ => return false,
        };
        if !capabilities.is_empty() {
            entry.capabilities = capabilities.to_vec();
        }
        // A successful adoption restores the attachment regardless of
        // whether a cursor exists.
        if self.order.iter().all(|registered| registered != id) {
            self.order.push(id.to_string());
        }
        let snapshot = data.get("snapshot").unwrap_or(data);
        let current = entry.cursor.clone();
        if let Some(mut cursor) = snapshot.get("lastEventCursor").and_then(parse_cursor) {
            cursor.active_session_id = Some(id.to_string());
            if current
                .as_ref()
                .is_none_or(|c| c.generation != cursor.generation)
                || current
                    .as_ref()
                    .and_then(|c| c.sequence)
                    .unwrap_or(0)
                    < cursor.sequence.unwrap_or(0)
            {
                entry.cursor = Some(cursor);
            }
        } else if let Some(sequence) = snapshot.get("lastEventSequence").and_then(Value::as_u64) {
            if current
                .as_ref()
                .and_then(|c| c.event_sequence)
                .unwrap_or(0)
                < sequence
            {
                entry.cursor = Some(DaemonResumeCursor {
                    active_session_id: Some(id.to_string()),
                    generation: None,
                    sequence: None,
                    event_sequence: Some(sequence),
                });
            }
        }
        self.catch_up_marker(id, entry.cursor.as_ref());
        true
    }

    /// The snapshot's marker catch-up: within the current marker's
    /// generation the sequence only ever rises.
    fn catch_up_marker(&mut self, id: &str, cursor: Option<&DaemonResumeCursor>) {
        let (generation, sequence) = match cursor {
            Some(cursor) if cursor.sequence.is_some() => {
                (cursor.generation.clone().unwrap_or_default(), cursor.sequence)
            }
            Some(cursor) if cursor.event_sequence.is_some() => {
                (String::new(), cursor.event_sequence)
            }
            _ => return,
        };
        let Some(sequence) = sequence else { return };
        match self.seen.get(id) {
            Some((marker_generation, last)) if *marker_generation == generation => {
                if sequence > *last {
                    self.seen
                        .insert(id.to_string(), (generation, sequence));
                }
            }
            _ => {
                self.seen.insert(id.to_string(), (generation, sequence));
            }
        }
    }

    /// Observe one raw outbound frame for bookkeeping (never consumes it):
    /// session events adopt cursors and decide dedupe for REGISTERED
    /// attachments only, `session_binding` rekeys the entry and its
    /// marker, `session_closed` drops the entry and everything it owns.
    pub(crate) fn observe_frame(&mut self, frame: &Value) -> FrameAction {
        match frame.get("type").and_then(Value::as_str).unwrap_or_default() {
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
        // Dedupe ONLY within the current marker's generation (the contract
        // above): a same-generation replay is suppressed; a different
        // generation replaces the marker slot — desktop parity, late
        // old-generation frames included.
        if let Some((marker_generation, last)) = self.seen.get(id) {
            if *marker_generation == generation && sequence <= *last {
                return FrameAction::Dedupe;
            }
        }
        self.seen.insert(id.to_string(), (generation.clone(), sequence));
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
        // The marker and the fresh intent follow the attachment: the bound
        // session continues its sequence line under the new id.
        if let Some((_, marker)) = self.seen.remove(previous) {
            self.seen.insert(next.to_string(), marker);
        }
        if let Some(intent) = self.intents.remove(previous) {
            self.intents.insert(next.to_string(), intent);
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
    fn dedupe_within_generation_and_generation_replacement() {
        let mut registry = AttachmentRegistry::default();
        registry.register("s1", vec!["attach_snapshot".to_string()]);
        let actions: Vec<FrameAction> = [("g1", 2), ("g1", 2), ("g1", 1), ("g2", 1)]
            .iter()
            .map(|(generation, sequence)| registry.observe_frame(&event(generation, sequence)))
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
        // A late old-generation frame replaces the marker and the cursor —
        // the documented desktop-parity limitation, never a heuristic.
        assert_eq!(registry.observe_frame(&event("g1", 3)), FrameAction::Deliver);
        let (_, entry) = registry.restore_entries().remove(0);
        assert_eq!(entry.cursor.as_ref().unwrap().generation.as_deref(), Some("g1"));
    }

    #[test]
    fn generation_churn_keeps_one_current_marker_per_attachment() {
        let mut registry = AttachmentRegistry::default();
        registry.register("s1", vec![]);
        for index in 0..50 {
            registry.observe_frame(&event(&format!("g{index}"), 1));
        }
        assert_eq!(registry.seen.len(), 1, "one current marker, bounded");
        assert_eq!(registry.observe_frame(&event("g49", 1)), FrameAction::Dedupe);
    }

    #[test]
    fn snapshot_adoption_is_monotonic_and_dual_shape() {
        let mut registry = AttachmentRegistry::default();
        registry.register("s1", vec![]);
        let token = registry.attempt_token("s1");
        // The generation shape, carrying its session id.
        assert!(registry.record_attach(
            "s1",
            &["attach_snapshot".to_string()],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 4 } }),
            token
        ));
        // A stale snapshot never rolls back.
        assert!(registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 2 } }),
            token
        ));
        let (_, entry) = registry.restore_entries().remove(0);
        assert_eq!(entry.cursor.as_ref().unwrap().sequence, Some(4));
        assert_eq!(
            entry.cursor.as_ref().unwrap().active_session_id.as_deref(),
            Some("s1")
        );
        // The legacy shape.
        assert!(registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventSequence": 7 }),
            token
        ));
        let (_, entry) = registry.restore_entries().remove(0);
        assert_eq!(entry.cursor.as_ref().unwrap().event_sequence, Some(7));
        // The marker rode along: a replayed sequence dedupes.
        let replay = json!({
            "type": "session_event",
            "activeSessionId": "s1",
            "event": { "type": "delta" },
            "meta": { "sequence": 7 },
        });
        assert_eq!(registry.observe_frame(&replay), FrameAction::Dedupe);
        // The capabilities were recorded from the attempt.
        assert_eq!(entry.capabilities, vec!["attach_snapshot".to_string()]);
    }

    #[test]
    fn a_fresh_attach_with_no_cursor_is_still_restored() {
        let mut registry = AttachmentRegistry::default();
        let token = registry.attempt_token("s3");
        assert!(registry.record_attach("s3", &["event_sequence".to_string()], &json!({}), token));
        let entries = registry.restore_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "s3");
        assert!(entries[0].1.cursor.is_none());
        assert_eq!(entries[0].1.capabilities, vec!["event_sequence".to_string()]);
    }

    #[test]
    fn tokens_close_the_aba_and_resurrection_holes() {
        let mut registry = AttachmentRegistry::default();
        registry.register("s1", vec![]);
        let token = registry.attempt_token("s1");
        // The current attempt adopts at its own token.
        assert!(registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 5 } }),
            token
        ));
        // A forget mid-flight invalidates the token: the stale response
        // can never adopt or resurrect.
        registry.forget("s1");
        assert!(!registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 9 } }),
            token
        ));
        assert!(registry.restore_entries().is_empty());
        // A re-register mints a NEW token: the old one never matches again
        // (globally unique, no tombstones needed).
        registry.register("s1", vec![]);
        let renewed = registry.attempt_token("s1");
        assert_ne!(renewed, token);
        assert!(!registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 9 } }),
            token
        ));
        assert!(registry.record_attach(
            "s1",
            &[],
            &json!({ "lastEventCursor": { "generation": "g", "sequence": 6 } }),
            renewed
        ));
        // A fresh id's intent adopts exactly once.
        let fresh = registry.attempt_token("s4");
        assert!(registry.record_attach("s4", &[], &json!({ "lastEventSequence": 1 }), fresh));
        // Replaying the same token after adoption never adopts again.
        assert!(!registry.record_attach("s4", &[], &json!({ "lastEventSequence": 2 }), fresh));
    }

    #[test]
    fn binding_rekeys_and_closed_drops_with_markers_and_intents() {
        let mut registry = AttachmentRegistry::default();
        registry.register("s1", vec![]);
        registry.observe_frame(&event("g", 3));
        let binding = json!({
            "type": "session_binding",
            "previousActiveSessionId": "s1",
            "activeSessionId": "s2",
        });
        assert_eq!(registry.observe_frame(&binding), FrameAction::Deliver);
        let entries = registry.restore_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "s2");
        // The marker moved with the attachment: the replay dedupes under the
        // new id.
        let replay = json!({
            "type": "session_event",
            "activeSessionId": "s2",
            "event": { "type": "delta" },
            "meta": { "cursor": { "generation": "g", "sequence": 3 } },
        });
        assert_eq!(registry.observe_frame(&replay), FrameAction::Dedupe);
        // A close drops the attachment, marker, and intent it owned.
        let closed = json!({ "type": "session_closed", "activeSessionId": "s2" });
        assert_eq!(registry.observe_frame(&closed), FrameAction::Deliver);
        assert!(registry.restore_entries().is_empty());
        assert_eq!(registry.observe_frame(&replay), FrameAction::Deliver);
    }
}
