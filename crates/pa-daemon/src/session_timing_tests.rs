//! The response-timing scan fold: per-id replacement, last-write
//! `lastResponse`, tool-work sums, and the omission rules.

use serde_json::json;

use crate::session_store::SessionEntry;
use crate::session_timing::{
    response_timing_summary_of, tool_duration_of_message, ResponseTimingRow, ResponseTimingScan,
};

fn assistant_row(id: &str, timing: &serde_json::Value, output: u64) -> serde_json::Value {
    json!({
        "type": "message",
        "id": id,
        "timestamp": "2026-10-20T00:00:00.000Z",
        "message": {
            "role": "assistant",
            "usage": { "output": output },
            "timing": timing,
        }
    })
}

fn tool_row(id: &str, duration_ms: &serde_json::Value) -> serde_json::Value {
    json!({
        "type": "message",
        "id": id,
        "timestamp": "2026-10-20T00:00:00.000Z",
        "message": {
            "role": "toolResult",
            "details": { "durationMs": duration_ms },
        }
    })
}

fn entry(value: &serde_json::Value) -> SessionEntry {
    serde_json::from_value(value.clone()).expect("entry wire shape")
}

fn row(ttft: f64, generation: f64, llm: f64) -> serde_json::Value {
    json!({ "ttftMs": ttft, "generationMs": generation, "llmTimeMs": llm })
}

#[test]
fn the_scan_folds_whole_file_sums_and_last_response() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_assistant("a", timing(row(100.0, 900.0, 1000.0)), 90);
    scan.fold_assistant("b", timing(row(200.0, 800.0, 1000.0)), 80);
    scan.fold_tool_result("r", Some(5.0));

    let summary = scan.summary().expect("measured rows");
    // avgTtft = plain mean: (100 + 200) / 2.
    assert_eq!(summary.avg_ttft_ms, Some(150.0));
    // avgTps = SUM tokens / SUM seconds: (90 + 80) / (0.9 + 0.8) = 100 t/s.
    assert_eq!(summary.avg_tokens_per_second, Some(100.0));
    assert_eq!(summary.llm_time_ms, Some(2000.0));
    assert_eq!(summary.tool_time_ms, Some(5.0));
    let last = summary.last_response.expect("last measured row");
    assert_eq!(last.ttft_ms, Some(200.0));
    assert_eq!(last.generation_ms, Some(800.0));
    assert_eq!(last.tokens_per_second, Some(100.0));
    assert_eq!(last.llm_time_ms, Some(1000.0));
}

#[test]
fn avg_tps_is_overall_throughput_not_a_mean_of_rates() {
    // 100 tokens in 0.1s (1000 t/s) and 100 tokens in 9.9s (10.1 t/s):
    // overall throughput is 200/10 = 20 t/s, not 505 t/s.
    let mut scan = ResponseTimingScan::default();
    scan.fold_assistant("fast", timing(row(10.0, 100.0, 110.0)), 100);
    scan.fold_assistant("slow", timing(row(10.0, 9900.0, 9910.0)), 100);
    let summary = scan.summary().expect("measured rows");
    assert_eq!(summary.avg_tokens_per_second, Some(20.0));
}

#[test]
fn a_re_appended_id_replaces_its_contribution() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_assistant("a", timing(row(100.0, 900.0, 1000.0)), 90);
    // The same id re-appended (a retried durable write) replaces, not adds.
    scan.fold_assistant("a", timing(row(50.0, 450.0, 500.0)), 45);
    let summary = scan.summary().expect("measured rows");
    assert_eq!(summary.avg_ttft_ms, Some(50.0));
    assert_eq!(summary.avg_tokens_per_second, Some(100.0));
    assert_eq!(summary.llm_time_ms, Some(500.0));
    assert_eq!(summary.last_response.expect("last").ttft_ms, Some(50.0));
}

#[test]
fn a_replacement_without_timing_evicts_the_old_contribution() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_assistant("a", timing(row(100.0, 900.0, 1000.0)), 90);
    scan.fold_assistant("a", timing(json!({})), 0);
    assert_eq!(scan.summary(), None, "no measured row remains");
}

#[test]
fn a_tool_only_response_measures_llm_time_without_ttft() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_assistant("t", timing(json!({ "llmTimeMs": 700.0 })), 40);
    let summary = scan.summary().expect("measured row");
    assert_eq!(summary.avg_ttft_ms, None);
    assert_eq!(summary.avg_tokens_per_second, None);
    assert_eq!(summary.llm_time_ms, Some(700.0));
    let last = summary.last_response.expect("last measured row");
    assert_eq!(last.ttft_ms, None);
    assert_eq!(last.tokens_per_second, None);
    assert_eq!(last.llm_time_ms, Some(700.0));
}

#[test]
fn the_latest_write_wins_for_last_response_even_after_a_replacement() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_assistant("old", timing(row(100.0, 900.0, 1000.0)), 90);
    scan.fold_assistant("new", timing(json!({ "llmTimeMs": 42.0 })), 1);
    let last = scan
        .summary()
        .expect("measured")
        .last_response
        .expect("last");
    assert_eq!(last.llm_time_ms, Some(42.0));
    // And a later re-write of the FIRST id becomes the latest write again.
    scan.fold_assistant("old", timing(row(11.0, 0.1, 12.0)), 1);
    let last = scan
        .summary()
        .expect("measured")
        .last_response
        .expect("last");
    assert_eq!(last.ttft_ms, Some(11.0));
}

#[test]
fn parallel_tool_work_sums() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_tool_result("r1", Some(10.0));
    scan.fold_tool_result("r2", Some(15.0));
    scan.fold_tool_result("r3", None);
    let summary = scan.summary().expect("tool time measured");
    assert_eq!(summary.tool_time_ms, Some(25.0));
}

#[test]
fn an_empty_fold_answers_none() {
    let scan = ResponseTimingScan::default();
    assert_eq!(scan.summary(), None);
    let mut scan = ResponseTimingScan::default();
    scan.fold_assistant("unmeasured", timing(json!({})), 0);
    assert_eq!(scan.summary(), None);
}

#[test]
fn the_scan_state_round_trips_the_sidecar_bits_encoding() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_assistant("a", timing(row(100.0, 900.0, 1000.0)), 90);
    scan.fold_tool_result("r", Some(5.0));
    let persisted = serde_json::to_value(&scan).expect("sidecar form serializes");
    let reloaded: ResponseTimingScan =
        serde_json::from_value(persisted).expect("sidecar form parses");
    assert_eq!(reloaded.summary(), scan.summary());
}

#[test]
fn old_rows_and_foreign_timing_blocks_read_as_unmeasured() {
    let mut scan = ResponseTimingScan::default();
    // No timing block at all (a pre-feature row).
    scan.fold_assistant("old", ResponseTimingRow::of_message(&json!({})), 10);
    // A malformed timing block must not fail the fold.
    scan.fold_assistant(
        "odd",
        ResponseTimingRow::of_message(&json!({ "timing": "not-an-object" })),
        10,
    );
    assert_eq!(scan.summary(), None);
}

#[test]
fn tool_duration_reads_the_details_block() {
    assert_eq!(
        tool_duration_of_message(&json!({ "details": { "durationMs": 10 } })),
        Some(10.0)
    );
    assert_eq!(tool_duration_of_message(&json!({ "details": {} })), None);
    assert_eq!(tool_duration_of_message(&json!({})), None);
    assert_eq!(
        tool_duration_of_message(&json!({ "details": { "durationMs": "bad" } })),
        None
    );
}

#[test]
fn the_pathless_fold_walks_entries_in_stored_order() {
    let entries = vec![
        entry(&assistant_row("a", &row(100.0, 900.0, 1000.0), 90)),
        entry(&tool_row("r1", &json!(10.0))),
        entry(&assistant_row("b", &json!({ "llmTimeMs": 500.0 }), 40)),
        entry(&tool_row("r2", &json!(15.0))),
    ];
    let summary = response_timing_summary_of(&entries).expect("measured rows");
    assert_eq!(summary.avg_ttft_ms, Some(100.0));
    assert_eq!(summary.avg_tokens_per_second, Some(100.0));
    assert_eq!(summary.llm_time_ms, Some(1500.0));
    assert_eq!(summary.tool_time_ms, Some(25.0));
    // The latest write is the tool-only response: no stale TTFT.
    let last = summary.last_response.expect("last");
    assert_eq!(last.ttft_ms, None);
    assert_eq!(last.llm_time_ms, Some(500.0));
}

#[test]
fn the_pathless_fold_skips_non_message_rows() {
    let entries = vec![entry(&json!({
        "type": "model_change", "id": "m", "timestamp": "t",
        "provider": "p", "modelId": "m1"
    }))];
    assert_eq!(response_timing_summary_of(&entries), None);
}

/// The scan-side row parse used by the info fold.
fn timing(value: serde_json::Value) -> ResponseTimingRow {
    serde_json::from_value(value).expect("timing block shape")
}

#[test]
fn a_re_appended_tool_row_replaces_its_duration() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_tool_result("r", Some(10.0));
    scan.fold_tool_result("r", Some(25.0));
    let summary = scan.summary().expect("tool time measured");
    assert_eq!(summary.tool_time_ms, Some(25.0));
}

#[test]
fn a_replacement_without_duration_evicts_the_old_tool_time() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_tool_result("r", Some(10.0));
    scan.fold_tool_result("r", None);
    assert_eq!(scan.summary(), None);
}

#[test]
fn a_zero_generation_interval_is_not_a_rate_sample() {
    let mut scan = ResponseTimingScan::default();
    // A sub-decimal generation interval (rounds to 0.0) must not put its
    // tokens into the TPS aggregate.
    scan.fold_assistant(
        "fast",
        timing(json!({ "ttftMs": 100.0, "generationMs": 0.0, "llmTimeMs": 100.1 })),
        1000,
    );
    let summary = scan.summary().expect("measured row");
    assert_eq!(summary.avg_tokens_per_second, None);
    let last = summary.last_response.expect("last");
    assert_eq!(last.generation_ms, Some(0.0));
    assert_eq!(last.tokens_per_second, None);
}

#[test]
fn a_tools_only_file_reports_tool_time() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_tool_result("r", Some(12.5));
    let summary = scan.summary().expect("tool time measured");
    assert_eq!(summary.tool_time_ms, Some(12.5));
    assert_eq!(summary.avg_ttft_ms, None);
    assert_eq!(summary.llm_time_ms, None);
    assert_eq!(summary.last_response, None);
}

#[test]
fn a_measured_zero_tool_duration_is_present_not_absent() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_tool_result("r", Some(0.0));
    let summary = scan.summary().expect("tool time measured");
    assert_eq!(summary.tool_time_ms, Some(0.0));
}

#[test]
fn an_unmeasured_replacement_evicts_the_last_response_chain_to_the_true_latest() {
    let mut scan = ResponseTimingScan::default();
    scan.fold_assistant("a", timing(row(100.0, 900.0, 1000.0)), 90);
    scan.fold_assistant("b", timing(row(200.0, 800.0, 1000.0)), 80);
    // 'b' re-written unmeasured: the slot must fall back to 'a' (labelled 'a').
    scan.fold_assistant("b", timing(json!({})), 0);
    let summary = scan.summary().expect("a remains measured");
    let last = summary.last_response.expect("last is a's values");
    assert_eq!(last.ttft_ms, Some(100.0));
    // And 'a' re-written unmeasured afterwards: no measured row remains, so
    // the lastResponse must be gone entirely (not 'a' stale values).
    scan.fold_assistant("a", timing(json!({})), 0);
    let summary = scan.summary();
    assert!(summary.is_none(), "nothing measured remains: {summary:?}");
}
