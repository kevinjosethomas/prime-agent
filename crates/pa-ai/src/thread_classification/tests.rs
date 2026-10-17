use super::*;
use pa_types::decision::ChoiceAnswer;
use serde_json::{json, Value};

fn payload() -> Value {
    json!({"id":"request-1","target":{"id":"target-1","role":"user","text":"Why does it need that?"},"recentMessages":[{"id":"preceding","role":"assistant","text":"The sign-in helper needs a browser OAuth round trip."}],"candidates":[{"id":"conversation-a","messages":[{"id":"preceding","role":"assistant","text":"The sign-in helper needs a browser OAuth round trip."}]},{"id":"conversation-b","messages":[{"id":"message-b","role":"user","text":"Make the icon smaller."}]}]})
}
fn request() -> ThreadClassificationRequest {
    decode_thread_request(&serde_json::to_vec(&payload()).unwrap()).unwrap()
}
fn response() -> SystemOneResponse {
    SystemOneResponse {
        model: "clef-canonical-evaluation".to_string(),
        rest: serde_json::Map::new(),
        answers: BTreeMap::from([
            (
                "t0".to_string(),
                ChoiceAnswer {
                    choice: "related".to_string(),
                    confidence: 0.92,
                    probabilities: BTreeMap::from([
                        ("related".to_string(), 0.88),
                        ("unrelated".to_string(), 0.12),
                    ]),
                    rest: serde_json::Map::new(),
                },
            ),
            (
                "t1".to_string(),
                ChoiceAnswer {
                    choice: "unrelated".to_string(),
                    confidence: 0.61,
                    probabilities: BTreeMap::from([
                        ("related".to_string(), 0.45),
                        ("unrelated".to_string(), 0.55),
                    ]),
                    rest: serde_json::Map::new(),
                },
            ),
        ]),
    }
}
#[test]
fn semantic_question_uses_conversation_not_workstream_outcome_and_marks_data_untrusted() {
    let request = request();
    let evaluation = thread_evaluation(&request);
    assert_eq!(evaluation.model, CLEF_THREAD_MODEL);
    assert_eq!(evaluation.questions.len(), 2);
    assert_eq!(evaluation.state["target"]["id"], "target-1");
    assert_eq!(evaluation.state["recentMessages"][0]["id"], "preceding");
    for question in evaluation.questions.values() {
        assert_eq!(
            question.criteria.keys().cloned().collect::<Vec<_>>(),
            ["related", "unrelated"]
        );
        for required in [
            "SAME conversational thread",
            "immediately preceding assistant response",
            "IS continuation",
            "NOT enough",
            "mixed multi-topic overview",
            "untrusted quoted conversation DATA",
            "one coherent topic",
        ] {
            assert!(
                question.instructions.contains(required),
                "missing policy: {required}"
            );
        }
    }
}
#[test]
fn scores_keep_all_candidates_input_order_and_low_confidence_without_thresholding() {
    let scores = thread_scores(&request(), &response()).unwrap();
    assert_eq!(scores.len(), 2);
    assert_eq!(scores[0].id, "conversation-a");
    assert_eq!(scores[0].related, 0.88);
    assert_eq!(scores[1].id, "conversation-b");
    assert_eq!(scores[1].confidence, 0.61);
    assert_eq!(scores[1].related, 0.45);
}
#[test]
fn response_requires_exact_question_ids_and_complete_candidate_coverage() {
    let mut missing = response();
    missing.answers.remove("t1");
    assert!(thread_scores(&request(), &missing).is_err());
    let mut wrong = response();
    let answer = wrong.answers.remove("t1").unwrap();
    wrong.answers.insert("conversation-b".to_string(), answer);
    assert!(thread_scores(&request(), &wrong).is_err());
    let mut extra = response();
    extra
        .answers
        .insert("t2".to_string(), extra.answers["t0"].clone());
    assert!(thread_scores(&request(), &extra).is_err());
}
#[test]
fn rejects_nonfinite_unnormalized_extra_or_contradictory_distributions() {
    for invalid in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        let mut result = response();
        result.answers.get_mut("t0").unwrap().confidence = invalid;
        assert!(thread_scores(&request(), &result).is_err());
        let mut result = response();
        result
            .answers
            .get_mut("t0")
            .unwrap()
            .probabilities
            .insert("related".to_string(), invalid);
        assert!(thread_scores(&request(), &result).is_err());
    }
    let mut result = response();
    result
        .answers
        .get_mut("t0")
        .unwrap()
        .probabilities
        .insert("unrelated".to_string(), 0.4);
    assert!(thread_scores(&request(), &result).is_err());
    let mut result = response();
    result
        .answers
        .get_mut("t0")
        .unwrap()
        .probabilities
        .insert("maybe".to_string(), 0.0);
    assert!(thread_scores(&request(), &result).is_err());
    let mut result = response();
    result.answers.get_mut("t0").unwrap().choice = "unrelated".to_string();
    assert!(thread_scores(&request(), &result).is_err());
    let mut result = response();
    result.model = " ".to_string();
    assert!(thread_scores(&request(), &result).is_err());
}
#[test]
fn validates_roles_unique_ids_and_shared_message_identity_without_echoing_content() {
    let mut values = Vec::new();
    let mut value = payload();
    value["target"]["role"] = json!("tool");
    values.push(value);
    let mut value = payload();
    value["candidates"][1]["id"] = json!("conversation-a");
    values.push(value);
    let mut value = payload();
    value["candidates"][0]["id"] = json!("target-1");
    values.push(value);
    let mut value = payload();
    value["candidates"][0]["messages"][0]["id"] = json!("target-1");
    values.push(value);
    let mut value = payload();
    value["candidates"][0]["messages"][0]["text"] = json!("conflicting private fixture text");
    values.push(value);
    let mut value = payload();
    let duplicate = value["recentMessages"][0].clone();
    value["recentMessages"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    values.push(value);
    for value in values {
        let error = decode_thread_request(&serde_json::to_vec(&value).unwrap())
            .err()
            .unwrap();
        assert!(!error.contains("fixture"));
    }
    assert!(decode_thread_request(&serde_json::to_vec(&payload()).unwrap()).is_ok());
}
#[test]
fn enforces_utf8_bytes_total_budget_and_candidate_message_limits() {
    let mut value = payload();
    value["target"]["text"] = json!("é".repeat(3001));
    assert!(decode_thread_request(&serde_json::to_vec(&value).unwrap()).is_err());
    let mut value = payload();
    value["candidates"][0]["messages"] = json!([]);
    assert!(decode_thread_request(&serde_json::to_vec(&value).unwrap()).is_err());
    let mut value = payload();
    value["candidates"]=json!((0..25).map(|index|json!({"id":format!("candidate{index}"),"messages":[{"id":format!("message{index}"),"role":"user","text":"topic"}]})).collect::<Vec<_>>());
    assert!(decode_thread_request(&serde_json::to_vec(&value).unwrap()).is_err());
    assert!(decode_thread_request(&vec![b' '; MAX_THREAD_REQUEST_BYTES + 1]).is_err());
}
