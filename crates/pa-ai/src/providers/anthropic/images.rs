//! Request-local Claude image bounds. Saved messages and attachment originals stay unchanged.
use std::io::Cursor;

use base64::Engine;
use image::{imageops::FilterType, ImageFormat, ImageReader};
use serde_json::Value;

const MAX_ENCODED_BYTES: usize = 10 * 1024 * 1024;

fn count_content(content: &Value) -> usize {
    match content {
        Value::Array(blocks) => blocks.iter().map(count_content).sum(),
        Value::Object(block) if block.get("type").and_then(Value::as_str) == Some("image") => 1,
        Value::Object(block)
            if block.get("type").and_then(Value::as_str) == Some("tool_result") =>
        {
            block.get("content").map_or(0, count_content)
        }
        _ => 0,
    }
}

fn image_count(params: &Value) -> usize {
    params["messages"].as_array().map_or(0, |messages| {
        messages
            .iter()
            .map(|message| count_content(&message["content"]))
            .sum()
    })
}

pub(super) fn has_images(params: &Value) -> bool {
    image_count(params) > 0
}

fn tool_bounds(model: &str) -> (u32, u64) {
    let mut parts = model
        .split("claude-")
        .nth(1)
        .unwrap_or("")
        .split(['-', '.']);
    let family = parts.next().unwrap_or("");
    let major = parts
        .next()
        .and_then(|n| n.parse::<u32>().ok())
        .unwrap_or(0);
    let minor = parts
        .next()
        .and_then(|n| n.parse::<u32>().ok())
        .filter(|n| *n < 100)
        .unwrap_or(0);
    if matches!(family, "opus" | "sonnet" | "haiku") && (major > 4 || major == 4 && minor >= 7) {
        (2576, 4784)
    } else {
        (1568, 1568)
    }
}

fn dimensions(width: u32, height: u32, edge: u32, patches: Option<u64>) -> (u32, u32) {
    let longest = width.max(height).max(1);
    let numerator = edge.min(longest);
    let scaled = |dimension: u32| {
        u32::try_from(u64::from(dimension) * u64::from(numerator) / u64::from(longest))
            .unwrap_or(edge)
            .max(1)
    };
    let mut w = scaled(width);
    let mut h = scaled(height);
    while patches.is_some_and(|limit| u64::from(w.div_ceil(28)) * u64::from(h.div_ceil(28)) > limit)
    {
        if width >= height {
            w = w.saturating_sub(1).max(1);
            h = u32::try_from((u64::from(height) * u64::from(w)) / u64::from(width.max(1)))
                .unwrap_or(height)
                .max(1);
        } else {
            h = h.saturating_sub(1).max(1);
            w = u32::try_from((u64::from(width) * u64::from(h)) / u64::from(height.max(1)))
                .unwrap_or(width)
                .max(1);
        }
    }
    (w, h)
}

fn resize_source(source: &mut Value, edge: u32, patches: Option<u64>) -> Result<(), String> {
    if source["type"] != "base64" {
        return Ok(());
    }
    let Some(data) = source["data"].as_str() else {
        return Ok(());
    };
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(data) else {
        return Ok(());
    };
    // Unknown/malformed formats keep their existing provider validation behavior.
    let Ok(reader) = ImageReader::new(Cursor::new(&bytes)).with_guessed_format() else {
        return Ok(());
    };
    let Some(format) = reader.format() else {
        return Ok(());
    };
    let Ok((width, height)) = reader.into_dimensions() else {
        return Ok(());
    };
    if width == 0 || height == 0 {
        return Ok(());
    }
    let (mut target_w, mut target_h) = dimensions(width, height, edge, patches);
    if (target_w, target_h) == (width, height) && data.len() <= MAX_ENCODED_BYTES {
        return Ok(());
    }
    let mut reader = ImageReader::with_format(Cursor::new(&bytes), format);
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(256 * 1024 * 1024);
    reader.limits(limits);
    let original = reader.decode().map_err(|_| {
        "An image could not be prepared for Claude. The original attachment is unchanged."
            .to_string()
    })?;
    loop {
        let resized = original.resize_exact(target_w, target_h, FilterType::Lanczos3);
        let mut output = Cursor::new(Vec::new());
        resized
            .write_to(&mut output, ImageFormat::Png)
            .map_err(|_| "Could not encode an image for Claude".to_string())?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(output.into_inner());
        if encoded.len() <= MAX_ENCODED_BYTES {
            source["data"] = Value::String(encoded);
            source["media_type"] = Value::String("image/png".into());
            return Ok(());
        }
        target_w = (target_w * 4 / 5).max(1);
        target_h = (target_h * 4 / 5).max(1);
    }
}

fn prepare_content(
    content: &mut Value,
    edge: u32,
    tool: Option<(u32, u64)>,
    bounds: (u32, u64),
) -> Result<(), String> {
    match content {
        Value::Array(blocks) => {
            for block in blocks {
                prepare_content(block, edge, tool, bounds)?;
            }
        }
        Value::Object(block) if block.get("type").and_then(Value::as_str) == Some("image") => {
            if let Some(source) = block.get_mut("source") {
                resize_source(
                    source,
                    tool.map_or(edge, |b| edge.min(b.0)),
                    tool.map(|b| b.1),
                )?;
            }
        }
        Value::Object(block)
            if block.get("type").and_then(Value::as_str) == Some("tool_result") =>
        {
            if let Some(nested) = block.get_mut("content") {
                prepare_content(nested, edge, Some(bounds), bounds)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn prepare(mut params: Value, model: &str) -> Result<Value, String> {
    // Anthropic counts images across the entire request, including earlier turns and tools.
    // 2000px is the documented cross-platform safe cap once the count exceeds twenty.
    let edge = if image_count(&params) > 20 {
        2000
    } else {
        8000
    };
    let bounds = tool_bounds(model);
    if let Some(messages) = params["messages"].as_array_mut() {
        for message in messages {
            if let Some(content) = message.get_mut("content") {
                prepare_content(content, edge, None, bounds)?;
            }
        }
    }
    Ok(params)
}

#[cfg(test)]
mod tests;
