//! Whole-file response-timing scan: the resumable cumulative fold behind
//! `get_state`'s `summary.responseTiming`, the timing twin of the whole-file
//! usage scan in [`crate::session_usage`].
//!
//! Accounting class (matching `summary.usage` exactly): the fold walks the
//! session file's rows in persisted order — every in-file branch included,
//! cumulative across compactions because rows are never deleted. The
//! persisted scan state caches the consumed prefix (the info sidecar);
//! per-row contributions are keyed by row id (assistant and tool-result rows
//! alike) so a re-appended id (a retried durable append) replaces its earlier
//! contribution instead of double-counting, exactly like the usage scan's
//! assistant map.
//!
//! Scope: assistant responses of the agent loop (the row `timing` block
//! stamped by `pa_core::session_engine::response_timing`) and tool
//! executions (the toolResult row's `details.durationMs`: the loop's
//! uniform full-call wall duration for every executed tool call — a
//! documented fork deviation from the TS python rows' narrower
//! kernel-execution value, so the cumulative metric never mixes spans).
//! The compaction/branch summarizer calls are excluded by design. Measured
//! values only — a row without a timing block contributes nothing, and an
//! absent field is `None`, never a fabricated zero.
//!
//! Aggregates (each field omitted when its samples are all absent):
//!
//! * `avgTtftMs`: plain mean over the responses with a measured TTFT.
//! * `avgTokensPerSecond`: overall generation throughput — the summed
//!   output tokens of the responses with a measured generation interval,
//!   divided by their summed generation seconds (`SUM(tokens) /
//!   SUM(seconds)`, not a mean of per-response rates). A generation
//!   interval that rounds to zero is not a usable rate sample: its row is
//!   omitted from the numerator and denominator rather than inflating
//!   every other sample's apparent rate.
//! * `llmTimeMs` / `toolTimeMs`: cumulative sums (tool time is total tool
//!   work — parallel executions sum).
//! * `lastResponse`: the latest-written measured assistant row, with each
//!   of its own unmeasured fields omitted (a tool-only response shows
//!   `llmTimeMs` without a stale TTFT).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::session_store::SessionEntry;

/// The assistant-row timing block as persisted (one decimal, ms). The scan-side
/// parse of `message.timing`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseTimingRow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttft_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub llm_time_ms: Option<f64>,
}

impl ResponseTimingRow {
    /// At least one measured field.
    #[must_use]
    fn is_measured(self) -> bool {
        self.ttft_ms.is_some() || self.generation_ms.is_some() || self.llm_time_ms.is_some()
    }

    /// `message.timing` of one assistant row; absent/malformed blocks read as
    /// unmeasured (a foreign row must never fail the fold).
    #[must_use]
    pub fn of_message(message: &Value) -> Self {
        message
            .get("timing")
            .and_then(|timing| serde_json::from_value(timing.clone()).ok())
            .unwrap_or_default()
    }
}

/// The persisted form of one assistant row's contribution: the timing block's
/// floats as `to_bits()` integers (the workspace `serde_json` does not
/// round-trip every decimal float) plus the row's own output tokens.
#[derive(Serialize, Deserialize, Clone)]
struct PersistedRow {
    ttft_ms_bits: Option<u64>,
    generation_ms_bits: Option<u64>,
    llm_time_ms_bits: Option<u64>,
    output_tokens: u64,
}

/// One assistant row's whole fold contribution: its timing plus the row's
/// own output tokens (that response's TPS numerator).
#[derive(Debug, Clone, Copy, Default)]
struct MeasuredRow {
    timing: ResponseTimingRow,
    output_tokens: u64,
}

impl MeasuredRow {
    fn to_persisted(self) -> PersistedRow {
        PersistedRow {
            ttft_ms_bits: self.timing.ttft_ms.map(f64::to_bits),
            generation_ms_bits: self.timing.generation_ms.map(f64::to_bits),
            llm_time_ms_bits: self.timing.llm_time_ms.map(f64::to_bits),
            output_tokens: self.output_tokens,
        }
    }

    fn from_persisted(persisted: &PersistedRow) -> Self {
        Self {
            timing: ResponseTimingRow {
                ttft_ms: persisted.ttft_ms_bits.map(f64::from_bits),
                generation_ms: persisted.generation_ms_bits.map(f64::from_bits),
                llm_time_ms: persisted.llm_time_ms_bits.map(f64::from_bits),
            },
            output_tokens: persisted.output_tokens,
        }
    }
}

/// Id-keyed contributions in first-insertion order: a later row with the
/// same id replaces its contribution in place (the usage scan's
/// assistant-map semantics, applied to the timing sums). The displaced
/// contribution is returned so the caller can subtract it. Each entry also
/// carries its write ordinal (persisted with the state): the fold serves
/// rows in file order, so the highest measured ordinal is the latest
/// measured write.
#[derive(Default, Clone)]
struct MeasuredRowsById {
    entries: Vec<(String, MeasuredRow, u64)>,
    index: std::collections::HashMap<String, usize>,
    next_ordinal: u64,
}

impl MeasuredRowsById {
    /// Insert or replace by id; the displaced contribution is `Some` when the
    /// id existed. Returns (displaced, this write's ordinal).
    fn replace(&mut self, id: &str, row: MeasuredRow) -> (Option<MeasuredRow>, u64) {
        let ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        if let Some(at) = self.index.get(id) {
            let previous = self.entries[*at].1;
            self.entries[*at].1 = row;
            self.entries[*at].2 = ordinal;
            return (Some(previous), ordinal);
        }
        self.index.insert(id.to_string(), self.entries.len());
        self.entries.push((id.to_string(), row, ordinal));
        (None, ordinal)
    }

    /// The measured entry with the highest write ordinal as its owning
    /// `(id, ordinal, row)` triple: the `lastResponse` re-derivation after an
    /// eviction (rare: a re-appended row wrote over the latest measured one
    /// with an unmeasured row). The triple keeps the slot's id and ordinal
    /// truthful, so a later unmeasured replacement of THAT id evicts again.
    fn latest_measured(&self) -> Option<(String, u64, MeasuredRow)> {
        self.entries
            .iter()
            .filter(|(_, row, _)| row.timing.is_measured())
            .max_by_key(|(_, _, ordinal)| *ordinal)
            .map(|(id, row, ordinal)| (id.clone(), *ordinal, *row))
    }
}

impl Serialize for MeasuredRowsById {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(
            self.entries
                .iter()
                .map(|(id, row, ordinal)| (id.as_str(), (row.to_persisted(), *ordinal))),
        )
    }
}

impl<'de> Deserialize<'de> for MeasuredRowsById {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = Vec::<(String, (PersistedRow, u64))>::deserialize(deserializer)?;
        let mut map = MeasuredRowsById::default();
        for (id, (persisted, ordinal)) in entries {
            map.next_ordinal = map.next_ordinal.max(ordinal + 1);
            map.index.insert(id.clone(), map.entries.len());
            map.entries
                .push((id, MeasuredRow::from_persisted(&persisted), ordinal));
        }
        Ok(map)
    }
}

/// The persisted form of one tool-result row's contribution (see
/// [`PersistedRow`] for the bits encoding).
#[derive(Serialize, Deserialize, Clone)]
struct PersistedToolRow {
    duration_ms_bits: Option<u64>,
}

/// Tool-result durations keyed by id in first-insertion order (the same
/// insert-or-replace semantics as [`MeasuredRowsById`], over the persisted
/// bits form — see [`PersistedRow`] for the encoding). The id index keeps
/// the fold O(1) per row on cold whole-file scans.
#[derive(Default, Clone)]
struct ToolDurationsById {
    entries: Vec<(String, PersistedToolRow)>,
    index: std::collections::HashMap<String, usize>,
}

impl ToolDurationsById {
    /// Insert or replace by id; the displaced duration is `Some` when the id
    /// existed and carried one.
    fn replace(&mut self, id: &str, duration_ms_bits: Option<u64>) -> Option<Option<u64>> {
        if let Some(at) = self.index.get(id) {
            let previous = self.entries[*at].1.duration_ms_bits;
            self.entries[*at].1.duration_ms_bits = duration_ms_bits;
            return Some(previous);
        }
        self.index.insert(id.to_string(), self.entries.len());
        self.entries
            .push((id.to_string(), PersistedToolRow { duration_ms_bits }));
        None
    }
}

impl Serialize for ToolDurationsById {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(
            self.entries
                .iter()
                .map(|(id, row)| (id.as_str(), row.duration_ms_bits)),
        )
    }
}

impl<'de> Deserialize<'de> for ToolDurationsById {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let entries = Vec::<(String, Option<u64>)>::deserialize(deserializer)?;
        let mut map = ToolDurationsById::default();
        for (id, duration_ms_bits) in entries {
            map.replace(&id, duration_ms_bits);
        }
        Ok(map)
    }
}

/// The resumable timing fold: the persisted scan-state form. `Serialize`/
/// `Deserialize` are the sidecar's encoding; every float rides as
/// `to_bits()` integers.
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct ResponseTimingScan {
    rows: MeasuredRowsById,
    ttft_ms_sum_bits: u64,
    ttft_count: u64,
    generation_ms_sum_bits: u64,
    generation_output_tokens: u64,
    llm_time_ms_sum_bits: u64,
    llm_count: u64,
    /// Tool-result durations keyed by row id (the same replacement
    /// semantics as the assistant rows).
    tool_rows: ToolDurationsById,
    tool_time_ms_sum_bits: u64,
    tool_count: u64,
    /// The latest-written measured assistant row as `(id, write ordinal,
    /// contribution)`: the ordinal lets an unmeasured replacement of that id
    /// re-derive the slot instead of serving stale numbers.
    last: Option<(String, u64, PersistedRow)>,
}

impl ResponseTimingScan {
    /// Fold one assistant row: `timing` (absent on old or foreign rows) and
    /// the row's own `usage.output` tokens. A re-appended id replaces its
    /// earlier contribution.
    pub fn fold_assistant(&mut self, id: &str, timing: ResponseTimingRow, output_tokens: u64) {
        let row = MeasuredRow {
            timing,
            output_tokens,
        };
        let (previous, ordinal) = self.rows.replace(id, row);
        if let Some(previous) = previous {
            self.subtract_assistant_contribution(previous);
        }
        if timing.is_measured() {
            self.add_assistant_contribution(row);
            self.last = Some((id.to_string(), ordinal, row.to_persisted()));
        } else if self
            .last
            .as_ref()
            .is_some_and(|(last_id, _, _)| last_id == id)
        {
            // The id that held the latest measured write was re-written
            // unmeasured: the slot must not serve its stale values. Re-derive
            // the latest remaining measured write with its own id and ordinal
            // (rare: retried durable writes), so a later unmeasured
            // replacement of THAT id evicts again.
            self.last = self
                .rows
                .latest_measured()
                .map(|(id, ordinal, row)| (id, ordinal, row.to_persisted()));
        }
    }

    /// Fold one tool-result row's `details.durationMs` (absent on old rows
    /// and on calls that never executed — never fabricated). A re-appended
    /// id replaces its earlier contribution.
    pub fn fold_tool_result(&mut self, id: &str, duration_ms: Option<f64>) {
        let duration_ms_bits = duration_ms.map(f64::to_bits);
        if let Some(previous_bits) = self.tool_rows.replace(id, duration_ms_bits) {
            if let Some(bits) = previous_bits {
                self.tool_time_ms_sum_bits = f64::to_bits(
                    (f64::from_bits(self.tool_time_ms_sum_bits) - f64::from_bits(bits)).max(0.0),
                );
                self.tool_count = self.tool_count.saturating_sub(1);
            }
        }
        if let Some(duration_ms) = duration_ms {
            self.tool_time_ms_sum_bits =
                f64::to_bits(f64::from_bits(self.tool_time_ms_sum_bits) + duration_ms);
            self.tool_count += 1;
        }
    }

    /// The `responseTiming` summary block; `None` when nothing was ever
    /// measured (the wire field stays absent).
    #[must_use]
    pub fn summary(&self) -> Option<ResponseTimingSummary> {
        let ttft_count = self.ttft_count;
        let generation_ms_sum = f64::from_bits(self.generation_ms_sum_bits);
        let avg_ttft_ms = (ttft_count > 0)
            .then(|| round_ms(f64::from_bits(self.ttft_ms_sum_bits) / ttft_count as f64));
        let avg_tokens_per_second = (generation_ms_sum > 0.0).then(|| {
            round_tokens_per_second(
                self.generation_output_tokens as f64 / (generation_ms_sum / 1000.0),
            )
        });
        let llm_time_ms =
            (self.llm_count > 0).then(|| round_ms(f64::from_bits(self.llm_time_ms_sum_bits)));
        let tool_time_ms =
            (self.tool_count > 0).then(|| round_ms(f64::from_bits(self.tool_time_ms_sum_bits)));
        let last_response = self.last.as_ref().map(|(_, _, last)| {
            let row = MeasuredRow::from_persisted(last);
            let tokens_per_second = row
                .timing
                .generation_ms
                .filter(|generation_ms| *generation_ms > 0.0)
                .map(|generation_ms| {
                    round_tokens_per_second(row.output_tokens as f64 / (generation_ms / 1000.0))
                });
            LastResponseTiming {
                ttft_ms: row.timing.ttft_ms,
                generation_ms: row.timing.generation_ms,
                tokens_per_second,
                llm_time_ms: row.timing.llm_time_ms,
            }
        });
        if avg_ttft_ms.is_none()
            && avg_tokens_per_second.is_none()
            && llm_time_ms.is_none()
            && tool_time_ms.is_none()
            && last_response.is_none()
        {
            return None;
        }
        Some(ResponseTimingSummary {
            avg_ttft_ms,
            avg_tokens_per_second,
            llm_time_ms,
            tool_time_ms,
            last_response,
        })
    }

    /// A generation interval that rounds to zero is not a usable rate
    /// sample: the row's tokens would inflate every other sample's
    /// apparent rate, so only positive intervals enter the TPS aggregate.
    fn add_assistant_contribution(&mut self, row: MeasuredRow) {
        let timing = row.timing;
        if let Some(ttft_ms) = timing.ttft_ms {
            self.ttft_ms_sum_bits = f64::to_bits(f64::from_bits(self.ttft_ms_sum_bits) + ttft_ms);
            self.ttft_count += 1;
        }
        if let Some(generation_ms) = timing.generation_ms.filter(|ms| *ms > 0.0) {
            self.generation_ms_sum_bits =
                f64::to_bits(f64::from_bits(self.generation_ms_sum_bits) + generation_ms);
            self.generation_output_tokens += row.output_tokens;
        }
        if let Some(llm_time_ms) = timing.llm_time_ms {
            self.llm_time_ms_sum_bits =
                f64::to_bits(f64::from_bits(self.llm_time_ms_sum_bits) + llm_time_ms);
            self.llm_count += 1;
        }
    }

    fn subtract_assistant_contribution(&mut self, row: MeasuredRow) {
        let timing = row.timing;
        if timing.ttft_ms.is_some() {
            self.ttft_ms_sum_bits = f64::to_bits(
                (f64::from_bits(self.ttft_ms_sum_bits) - timing.ttft_ms.unwrap_or_default())
                    .max(0.0),
            );
            self.ttft_count = self.ttft_count.saturating_sub(1);
        }
        if timing.generation_ms.is_some_and(|ms| ms > 0.0) {
            self.generation_ms_sum_bits = f64::to_bits(
                (f64::from_bits(self.generation_ms_sum_bits)
                    - timing.generation_ms.unwrap_or_default())
                .max(0.0),
            );
            self.generation_output_tokens = self
                .generation_output_tokens
                .saturating_sub(row.output_tokens);
        }
        if timing.llm_time_ms.is_some() {
            self.llm_time_ms_sum_bits = f64::to_bits(
                (f64::from_bits(self.llm_time_ms_sum_bits)
                    - timing.llm_time_ms.unwrap_or_default())
                .max(0.0),
            );
            self.llm_count = self.llm_count.saturating_sub(1);
        }
    }
}

/// The `responseTiming` wire block of one session summary.
///
/// `Eq` is manual: the floats are finite (JSON numbers round-trip through
/// the one-decimal rounding and the bits-persisted sums, never NaN/inf), so
/// equality is total.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseTimingSummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_ttft_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_tokens_per_second: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_time_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_time_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_response: Option<LastResponseTiming>,
}

impl Eq for ResponseTimingSummary {}

/// The latest-written measured assistant row's own values; the fields it did
/// not measure stay absent.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LastResponseTiming {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttft_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens_per_second: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub llm_time_ms: Option<f64>,
}

impl Eq for LastResponseTiming {}

/// The tool-result row's `details.durationMs` (the TS wire convention);
/// absent on old rows and calls that never executed.
#[must_use]
pub fn tool_duration_of_message(message: &Value) -> Option<f64> {
    message
        .get("details")
        .and_then(|details| details.get("durationMs"))
        .and_then(Value::as_f64)
        .filter(|duration_ms| *duration_ms >= 0.0)
}

/// The in-memory fold (the summary's pathless-store fallback, mirroring
/// `session_usage::own_usage_summary_of`): one pass over the session's
/// entries in stored order.
#[must_use]
pub fn response_timing_summary_of(entries: &[SessionEntry]) -> Option<ResponseTimingSummary> {
    let mut scan = ResponseTimingScan::default();
    for entry in entries {
        if entry.type_ != "message" {
            continue;
        }
        let Some(message) = entry.fields.get("message") else {
            continue;
        };
        match message.get("role").and_then(Value::as_str) {
            Some("assistant") => {
                let output_tokens = message
                    .get("usage")
                    .and_then(|usage| usage.get("output"))
                    .and_then(Value::as_u64)
                    .unwrap_or_default();
                scan.fold_assistant(
                    &entry.id,
                    ResponseTimingRow::of_message(message),
                    output_tokens,
                );
            }
            Some("toolResult") => {
                scan.fold_tool_result(&entry.id, tool_duration_of_message(message));
            }
            _ => {}
        }
    }
    scan.summary()
}

/// One decimal of precision.
fn round_ms(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

/// One decimal of precision for token rates.
fn round_tokens_per_second(value: f64) -> f64 {
    (value * 10.0).round() / 10.0
}

impl ResponseTimingScan {
    /// The fold's retained per-id rows, counted toward the scan cache's
    /// bounded-entry budget alongside the usage scan's rows.
    #[must_use]
    pub(crate) fn retained_entries(&self) -> usize {
        self.rows.entries.len() + self.tool_rows.entries.len()
    }
}
