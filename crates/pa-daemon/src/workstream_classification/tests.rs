use super::*;
use pa_types::decision::ChoiceAnswer;

fn payload() -> Value {
    json!({
        "provider":"prime-inference", "modelId":"cloudflare/clef", "turnId":"turn-1",
        "workstreams":[
            {"id":"o1", "title":"Prepare O1 case", "summary":"Lighthouse and judging"},
            {"id":"root", "title":"Build root orchestrator", "summary":"Persistent chat"}
        ],
        "recentMessages":[{"role":"assistant", "text":"The O1 case includes judging."}],
        "messages":[{"role":"user", "text":"Where do judging applications belong?"}]
    })
}

fn response(scores: &[f64]) -> SystemOneResponse {
    SystemOneResponse {
        model: CLEF.to_string(),
        rest: serde_json::Map::new(),
        answers: scores
            .iter()
            .enumerate()
            .map(|(index, score)| {
                (
                    format!("w{index}"),
                    ChoiceAnswer {
                        rest: serde_json::Map::new(),
                        choice: if *score >= 0.5 {
                            "relevant"
                        } else {
                            "unrelated"
                        }
                        .to_string(),
                        confidence: score.max(1.0 - score),
                        probabilities: BTreeMap::from([
                            ("relevant".to_string(), *score),
                            ("unrelated".to_string(), 1.0 - score),
                        ]),
                    },
                )
            })
            .collect(),
    }
}

#[test]
fn selects_only_confident_existing_ids_and_can_select_multiple_workstreams() {
    let request = validate_request(&payload()).unwrap();
    assert_eq!(
        selected_ids(&request.workstreams, &response(&[0.95, 0.8])).unwrap(),
        vec!["o1", "root"]
    );
    assert_eq!(
        selected_ids(&request.workstreams, &response(&[0.9, 0.1])).unwrap(),
        vec!["o1"]
    );
    assert_eq!(
        selected_ids(&request.workstreams, &response(&[0.6, 0.3])).unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn accepts_canonical_response_model_metadata_without_changing_the_requested_target() {
    let request = validate_request(&payload()).unwrap();
    let mut canonical = response(&[0.9, 0.1]);
    canonical.model = "clef-2026-10".to_string();
    assert_eq!(
        selected_ids(&request.workstreams, &canonical).unwrap(),
        vec!["o1"]
    );
    assert_eq!(evaluation_batch(&request, &request.workstreams).model, CLEF);
    for model in [String::new(), " ".to_string(), "x".repeat(2_049)] {
        canonical.model = model;
        assert!(selected_ids(&request.workstreams, &canonical).is_err());
    }
}

#[test]
fn reserved_inbox_is_never_an_evaluation_question_or_selected_outcome() {
    let mut context = payload();
    context["workstreams"].as_array_mut().unwrap().insert(
        0,
        json!({
            "id": "inbox", "title": "Inbox", "summary": "Awaiting classification"
        }),
    );
    let request = validate_request(&context).unwrap();
    assert_eq!(request.workstreams.len(), 2);
    let batch = evaluation_batch(&request, &request.workstreams);
    assert_eq!(batch.questions.len(), 2);
    assert!(!batch.state["workstreams"]
        .as_array()
        .unwrap()
        .iter()
        .any(|stream| stream["id"] == "inbox"));
    assert_eq!(
        selected_ids(&request.workstreams, &response(&[0.9, 0.8])).unwrap(),
        vec!["o1", "root"]
    );
}

#[tokio::test]
async fn inbox_only_abstains_without_resolving_auth_or_contacting_a_provider() {
    let mut context = payload();
    context["workstreams"] = json!([{
        "id": "inbox", "title": "Inbox", "summary": "Awaiting classification"
    }]);
    let result = classify(
        Path::new("/nonexistent-workstream-test-agent-dir"),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(
        result,
        json!({"turnId": "turn-1", "uncertain": true, "workstreamIds": []})
    );
}

#[test]
fn rejects_missing_extra_unknown_and_invalid_probability_answers() {
    let request = validate_request(&payload()).unwrap();
    assert!(selected_ids(&request.workstreams, &response(&[0.9])).is_err());
    assert!(selected_ids(&request.workstreams, &response(&[0.9, 0.1, 0.8])).is_err());
    let mut unknown = response(&[0.9, 0.1]);
    let answer = unknown.answers.remove("w0").unwrap();
    unknown.answers.insert("invented".to_string(), answer);
    assert!(selected_ids(&request.workstreams, &unknown).is_err());
    for probability in [-0.1, 1.1, f64::NAN] {
        let mut invalid = response(&[0.9, 0.1]);
        invalid
            .answers
            .get_mut("w0")
            .unwrap()
            .probabilities
            .insert("relevant".to_string(), probability);
        assert!(selected_ids(&request.workstreams, &invalid).is_err());
    }
}

#[test]
fn schema_contains_detailed_context_and_every_candidate_is_batched() {
    let mut context = payload();
    context["workstreams"] = Value::Array(
        (0..100)
            .map(|index| {
                json!({
                    "id": format!("task-{index}"), "title": format!("Task {index}"),
                    "summary": "Detailed outcome, stage, and actual milestones"
                })
            })
            .collect(),
    );
    let request = validate_request(&context).unwrap();
    let batches: Vec<_> = request
        .workstreams
        .chunks(QUESTION_LIMIT)
        .map(|candidates| evaluation_batch(&request, candidates))
        .collect();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.questions.len())
            .collect::<Vec<_>>(),
        vec![32, 32, 32, 4]
    );
    for batch in batches {
        let encoded = serde_json::to_value(batch).unwrap();
        assert_eq!(encoded["model"], "cloudflare/clef");
        assert_eq!(encoded["state"]["workstreams"], context["workstreams"]);
        assert_eq!(encoded["state"]["messages"], context["messages"]);
        assert_eq!(encoded["questions"]["w0"]["type"], "choice");
        assert!(encoded.get("tools").is_none());
        assert!(encoded.get("messages").is_none());
    }
}

#[test]
fn rejects_oversized_and_non_conversational_context_and_chat_models() {
    let mut oversized = payload();
    oversized["messages"][0]["text"] = json!("x".repeat(8_001));
    assert!(validate_request(&oversized).is_err());
    let mut tool = payload();
    tool["messages"][0]["role"] = json!("tool");
    assert!(validate_request(&tool).is_err());
    let mut chat_model = payload();
    chat_model["modelId"] = json!("other-chat-model");
    assert!(validate_request(&chat_model).is_err());
}

#[tokio::test]
async fn no_workstreams_returns_uncertain_without_auth_or_provider() {
    let mut empty = payload();
    empty["workstreams"] = json!([]);
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        classify(dir.path(), &empty).await.unwrap(),
        json!({
            "turnId":"turn-1", "uncertain":true, "workstreamIds":[]
        })
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn busy_classifier_refuses_before_auth_or_provider_work() {
    let slots = SLOTS.get_or_init(|| Semaphore::new(2));
    let _held = slots.try_acquire_many(2).unwrap();
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        classify(dir.path(), &payload()).await,
        Err("Workstream classifier is busy; try again later".to_string())
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn valid_unicode_workstream_ids_fit_the_desktop_contract() {
    let mut context = payload();
    let id = "📋".repeat(256);
    context["workstreams"][0]["id"] = json!(id);
    let request = validate_request(&context).unwrap();
    assert_eq!(
        selected_ids(&request.workstreams, &response(&[0.9, 0.1])).unwrap(),
        vec![id]
    );
}
