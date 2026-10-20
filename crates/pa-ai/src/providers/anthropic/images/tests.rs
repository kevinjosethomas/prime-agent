use super::*;
use serde_json::json;

fn picture(width: u32, height: u32) -> Value {
    let mut output = Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(width, height)
        .write_to(&mut output, ImageFormat::Png)
        .unwrap();
    json!({"type":"image","source":{"type":"base64","media_type":"image/png",
        "data":base64::engine::general_purpose::STANDARD.encode(output.into_inner())}})
}

fn size(block: &Value) -> (u32, u32) {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(block["source"]["data"].as_str().unwrap())
        .unwrap();
    ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .unwrap()
        .into_dimensions()
        .unwrap()
}

#[test]
fn historical_images_are_resized_when_a_follow_up_crosses_the_many_image_limit() {
    let large = picture(4000, 2000);
    let small = picture(2, 2);
    let mut blocks = vec![large];
    blocks.extend(std::iter::repeat_n(small.clone(), 19));
    let before = json!({"messages":[{"role":"user","content":blocks},
        {"role":"user","content":[small,{"type":"text","text":"A new follow-up"}]}]});
    let result = prepare(before.clone(), "claude-opus-5-5").unwrap();
    assert_eq!(image_count(&result), 21);
    assert_eq!(size(&result["messages"][0]["content"][0]), (2000, 1000));
    assert_eq!(size(&before["messages"][0]["content"][0]), (4000, 2000));
    assert_eq!(
        result["messages"][1]["content"][1]["text"],
        "A new follow-up"
    );
}

#[test]
fn twenty_user_images_preserve_valid_original_bytes() {
    let large = picture(4000, 20);
    let blocks = vec![large; 20];
    let params = json!({"messages":[{"role":"user","content":blocks}]});
    assert_eq!(prepare(params.clone(), "claude-opus-5-5").unwrap(), params);
}

#[test]
fn nested_tool_images_count_toward_the_request_limit() {
    let mut history = vec![picture(2, 2); 20];
    history.push(json!({"type":"tool_result","tool_use_id":"screenshot",
        "content":[picture(4000, 40)]}));
    let params = json!({"messages":[{"role":"user","content":history}]});
    let result = prepare(params, "claude-opus-5-5").unwrap();
    assert_eq!(
        size(&result["messages"][0]["content"][20]["content"][0]),
        (2000, 20)
    );
}

#[test]
fn tool_screenshots_obey_native_long_edge_and_patch_budget() {
    for (model, edge, patches) in [
        ("claude-opus-5-5", 2576, 4784),
        ("claude-sonnet-4-6", 1568, 1568),
    ] {
        let params = json!({"messages":[{"role":"user","content":[
            {"type":"tool_result","content":[picture(3000,3000)]}]}]});
        let result = prepare(params, model).unwrap();
        let (w, h) = size(&result["messages"][0]["content"][0]["content"][0]);
        assert!(w <= edge && h <= edge);
        assert!(u64::from(w.div_ceil(28)) * u64::from(h.div_ceil(28)) <= patches);
        assert_eq!(
            w, h,
            "resizing preserves the whole screenshot's proportions"
        );
    }
}

#[test]
fn a_single_oversized_image_is_fitted_without_cropping() {
    let result = prepare(
        json!({"messages":[{"role":"user","content":[picture(9000,90)]}]}),
        "claude-opus-5-5",
    )
    .unwrap();
    assert_eq!(size(&result["messages"][0]["content"][0]), (8000, 80));
}

#[test]
fn tool_inputs_and_text_are_never_reinterpreted_as_image_content() {
    let params = json!({"messages":[{"role":"assistant","content":[
        {"type":"tool_use","input":{"type":"image","source":{"type":"base64","data":"example"}}}]}]});
    assert!(!has_images(&params));
    assert_eq!(prepare(params.clone(), "claude-opus-5-5").unwrap(), params);
}

#[test]
fn older_date_suffixes_do_not_enable_high_resolution_bounds() {
    assert_eq!(tool_bounds("claude-opus-4-20250514"), (1568, 1568));
    assert_eq!(tool_bounds("claude-opus-4-7"), (2576, 4784));
    assert_eq!(tool_bounds("claude-opus-5-5"), (2576, 4784));
}
