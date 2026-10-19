//! Fits outgoing images to the provider's request limits at send time.
//!
//! Sessions keep every image the user or a tool attached. Providers reject a whole request when one
//! image is too large, so one oversized screenshot in the history would fail every later turn. This
//! pass runs on the cloned history for each request: it downscales and re-encodes images that break
//! the limits of that request, and replaces the oldest images with a short text note when the
//! request carries too many images or too many image bytes. The stored transcript is never changed.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, LazyLock, Mutex};

use base64::Engine as _;
use image::{DynamicImage, ImageFormat, ImageReader};
use serde_json::Map;
use sha2::{Digest as _, Sha256};

use crate::types::{
    ImageContent, Message, Model, TextContent, UserMessageContent, UserOrToolContent,
};

/// Image limits for one request. Byte sizes count base64 characters, which is what providers
/// measure on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ImageLimits {
    pub max_dimension: u32,
    /// Lower per-image dimension limit once a request has more than `many_images_threshold` images.
    pub many_images_max_dimension: u32,
    pub many_images_threshold: usize,
    pub max_image_bytes: usize,
    pub max_images: usize,
    pub max_total_image_bytes: usize,
}

const MIB: usize = 1024 * 1024;

/// Anthropic Messages API, <https://platform.claude.com/docs/en/build-with-claude/vision#request-limits>:
/// 8000 px per image; a stricter limit for requests with more than 20 images, for which the docs
/// advise 2000 px "to stay under the limit on all platforms"; 10 MB (direct) or 5 MB (Bedrock,
/// Google Cloud) of base64 per image; 100 images on 200k-context models; 32 MB per request
/// (<https://platform.claude.com/docs/en/api/overview#request-size-limits>, 20 MB on Bedrock).
/// The smaller per-platform values are used so the same set works wherever Claude is served.
const ANTHROPIC: ImageLimits = ImageLimits {
    max_dimension: 8000,
    many_images_max_dimension: 2000,
    many_images_threshold: 20,
    max_image_bytes: 5 * MIB,
    max_images: 100,
    max_total_image_bytes: 16 * MIB,
};

/// Bedrock Converse, <https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Message.html>:
/// up to 20 images, each at most 3.75 MB (5,000,000 base64 characters), 8000 x 8000 px.
const BEDROCK: ImageLimits = ImageLimits {
    max_dimension: 8000,
    many_images_max_dimension: 8000,
    many_images_threshold: 20,
    max_image_bytes: 5_000_000,
    max_images: 20,
    max_total_image_bytes: 16 * MIB,
};

/// `OpenAI` APIs, <https://developers.openai.com/api/docs/guides/images-vision>: 1,500 images and 512 MB
/// per request, at most 30,000 patches per image; no per-image byte limit is documented. 4096 px
/// stays under the patch limit, and the byte caps keep requests reasonable.
const OPENAI: ImageLimits = ImageLimits {
    max_dimension: 4096,
    many_images_max_dimension: 4096,
    many_images_threshold: 1500,
    max_image_bytes: 20 * MIB,
    max_images: 1500,
    max_total_image_bytes: 48 * MIB,
};

/// Gemini, <https://ai.google.dev/gemini-api/docs/image-understanding>: up to 3,600 images; inline
/// data must keep the request under 20 MB (the lower of the two documented values).
const GOOGLE: ImageLimits = ImageLimits {
    max_dimension: 8000,
    many_images_max_dimension: 8000,
    many_images_threshold: 3600,
    max_image_bytes: 7 * MIB,
    max_images: 3600,
    max_total_image_bytes: 15 * MIB,
};

fn tighter(a: ImageLimits, b: ImageLimits) -> ImageLimits {
    ImageLimits {
        max_dimension: a.max_dimension.min(b.max_dimension),
        many_images_max_dimension: a.many_images_max_dimension.min(b.many_images_max_dimension),
        many_images_threshold: a.many_images_threshold.min(b.many_images_threshold),
        max_image_bytes: a.max_image_bytes.min(b.max_image_bytes),
        max_images: a.max_images.min(b.max_images),
        max_total_image_bytes: a.max_total_image_bytes.min(b.max_total_image_bytes),
    }
}

pub(crate) fn limits_for(model: &Model) -> ImageLimits {
    let transport = match model.api.as_str() {
        "bedrock-converse-stream" => BEDROCK,
        "openai-completions"
        | "openai-responses"
        | "azure-openai-responses"
        | "openai-codex-responses" => OPENAI,
        "google-generative-ai" | "google-vertex" => GOOGLE,
        // Anthropic Messages, and providers without documented limits (Mistral, custom).
        _ => ANTHROPIC,
    };
    // Claude behind another API (Copilot, OpenRouter, Bedrock) still enforces Anthropic's limits.
    if model.id.to_ascii_lowercase().contains("claude") {
        tighter(transport, ANTHROPIC)
    } else {
        transport
    }
}

#[derive(Debug, Clone)]
enum Fitted {
    /// Already within the limits; the original bytes are sent.
    Unchanged,
    Resized {
        data: Arc<str>,
        mime_type: &'static str,
    },
    /// Could not be decoded or brought under the limits.
    Unusable,
}

impl Fitted {
    fn sent_len(&self, original_len: usize) -> usize {
        match self {
            Self::Unchanged => original_len,
            Self::Resized { data, .. } => data.len(),
            Self::Unusable => 0,
        }
    }
}

type CacheKey = ([u8; 32], u32, usize);

/// Resized images by content hash and target, so a long session re-encodes each image once.
static CACHE: LazyLock<Mutex<HashMap<CacheKey, Fitted>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
const CACHE_MAX_BYTES: usize = 256 * MIB;

fn cached_fit(data: &str, max_dimension: u32, max_bytes: usize) -> Fitted {
    let key: CacheKey = (
        Sha256::digest(data.as_bytes()).into(),
        max_dimension,
        max_bytes,
    );
    if let Some(hit) = CACHE.lock().expect("image cache lock").get(&key) {
        return hit.clone();
    }
    let fitted = fit_image(data, max_dimension, max_bytes);
    let mut cache = CACHE.lock().expect("image cache lock");
    let held: usize = cache.values().map(|entry| entry.sent_len(0)).sum();
    if held + fitted.sent_len(0) > CACHE_MAX_BYTES {
        cache.clear();
    }
    cache.insert(key, fitted.clone());
    fitted
}

fn base64_len(raw_len: usize) -> usize {
    raw_len.div_ceil(3) * 4
}

fn fit_image(data: &str, max_dimension: u32, max_bytes: usize) -> Fitted {
    // An image this pass cannot read is sent as before unless it is over the byte limit.
    let unreadable = || {
        if data.len() <= max_bytes {
            Fitted::Unchanged
        } else {
            Fitted::Unusable
        }
    };
    let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(data.trim()) else {
        return unreadable();
    };
    let Ok(reader) = ImageReader::new(Cursor::new(&raw)).with_guessed_format() else {
        return unreadable();
    };
    let source_format = reader.format();
    let Ok((width, height)) = reader.into_dimensions() else {
        return unreadable();
    };
    if width.max(height) <= max_dimension && data.len() <= max_bytes {
        return Fitted::Unchanged;
    }
    let Ok(decoded) = image::load_from_memory(&raw) else {
        return Fitted::Unusable;
    };
    let keep_png = source_format == Some(ImageFormat::Png);
    let mut target = max_dimension.min(width.max(height));
    for _ in 0..8 {
        let resized = if width.max(height) > target {
            decoded.resize(target, target, image::imageops::FilterType::Triangle)
        } else {
            decoded.clone()
        };
        if let Some((encoded, mime_type)) = encode(&resized, keep_png, max_bytes) {
            return Fitted::Resized {
                data: encoded.into(),
                mime_type,
            };
        }
        target = target * 3 / 4;
    }
    Fitted::Unusable
}

/// PNG keeps text and UI crisp, so it stays PNG while small; anything larger becomes JPEG.
const KEEP_PNG_MAX_BYTES: usize = 2 * MIB;
const JPEG_QUALITY: u8 = 85;

fn encode(
    image: &DynamicImage,
    prefer_png: bool,
    max_bytes: usize,
) -> Option<(String, &'static str)> {
    let engine = base64::engine::general_purpose::STANDARD;
    if prefer_png {
        let mut png = Vec::new();
        if image
            .write_to(&mut Cursor::new(&mut png), ImageFormat::Png)
            .is_ok()
            && base64_len(png.len()) <= max_bytes.min(KEEP_PNG_MAX_BYTES)
        {
            return Some((engine.encode(png), "image/png"));
        }
    }
    let mut jpeg = Vec::new();
    let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, JPEG_QUALITY);
    image.to_rgb8().write_with_encoder(encoder).ok()?;
    (base64_len(jpeg.len()) <= max_bytes).then(|| (engine.encode(jpeg), "image/jpeg"))
}

fn image_blocks_mut(message: &mut Message) -> Option<&mut Vec<UserOrToolContent>> {
    match message {
        Message::User(user) => match &mut user.content {
            UserMessageContent::Blocks(blocks) => Some(blocks),
            UserMessageContent::Text(_) => None,
        },
        Message::ToolResult(result) => Some(&mut result.content),
        Message::Assistant(_) => None,
    }
}

/// Reads the image size from the header, which sits at the start of PNG, GIF, and WebP files and,
/// for JPEG, after any metadata segments.
fn image_size(data: &str) -> Option<(u32, u32)> {
    let prefix = &data.as_bytes()[..data.len().min(128 * 1024) / 4 * 4];
    let raw = base64::engine::general_purpose::STANDARD
        .decode(prefix)
        .ok()?;
    ImageReader::new(Cursor::new(raw))
        .with_guessed_format()
        .ok()?
        .into_dimensions()
        .ok()
}

/// Text sent in place of an image that does not fit the request.
fn placeholder(data: &str, mime_type: &str, reason: &str) -> UserOrToolContent {
    let text = match image_size(data) {
        Some((width, height)) => format!("[image omitted: {width}x{height} {mime_type}, {reason}]"),
        None => format!("[image omitted: {mime_type}, {reason}]"),
    };
    UserOrToolContent::Text(TextContent {
        text,
        text_signature: None,
        rest: Map::default(),
    })
}

/// Images dropped together when the request passes its image byte budget.
const BYTE_BUDGET_STEP: usize = 8;

/// Resizes or omits images in `messages` (an already-cloned history) so the request fits `limits`.
/// The most recent images are kept; the oldest are replaced first.
pub(crate) fn fit_images_to_limits(
    mut messages: Vec<Message>,
    limits: ImageLimits,
) -> Vec<Message> {
    let slots: Vec<(usize, usize)> = messages
        .iter_mut()
        .enumerate()
        .filter_map(|(index, message)| Some((index, image_blocks_mut(message)?)))
        .flat_map(|(index, blocks)| {
            blocks
                .iter()
                .enumerate()
                .filter(|(_, block)| block.image().is_some())
                .map(move |(block_index, _)| (index, block_index))
                .collect::<Vec<_>>()
        })
        .collect();
    if slots.is_empty() {
        return messages;
    }
    // Images are dropped in steps of a quarter of the limit, so the cutoff (and with it the cached
    // prompt prefix) moves only now and then instead of on every new image.
    let count_step = (limits.max_images / 4).max(1);
    let mut kept_from = slots
        .len()
        .saturating_sub(limits.max_images)
        .div_ceil(count_step)
        * count_step;
    let max_dimension = if slots.len() - kept_from > limits.many_images_threshold {
        limits.many_images_max_dimension
    } else {
        limits.max_dimension
    };

    let fitted: Vec<Option<Fitted>> = slots
        .iter()
        .enumerate()
        .map(|(position, &(message_index, block_index))| {
            (position >= kept_from).then(|| {
                let blocks =
                    image_blocks_mut(&mut messages[message_index]).expect("slot holds blocks");
                let (data, _) = blocks[block_index].image().expect("slot holds an image");
                cached_fit(data, max_dimension, limits.max_image_bytes)
            })
        })
        .collect();
    let mut total = 0usize;
    for position in (kept_from..slots.len()).rev() {
        let (message_index, block_index) = slots[position];
        let blocks = image_blocks_mut(&mut messages[message_index]).expect("slot holds blocks");
        let (data, _) = blocks[block_index].image().expect("slot holds an image");
        total += fitted[position]
            .as_ref()
            .map_or(0, |fitted| fitted.sent_len(data.len()));
        if total > limits.max_total_image_bytes {
            let cut = (position + 1).div_ceil(BYTE_BUDGET_STEP) * BYTE_BUDGET_STEP;
            kept_from = cut.min(slots.len() - 1);
            break;
        }
    }

    for (position, (&(message_index, block_index), fitted)) in slots.iter().zip(fitted).enumerate()
    {
        let blocks = image_blocks_mut(&mut messages[message_index]).expect("slot holds blocks");
        let block = &mut blocks[block_index];
        let (data, mime_type) = block.image().expect("slot holds an image");
        match fitted {
            _ if position < kept_from => {
                *block = placeholder(data, mime_type, "sent earlier in this conversation");
            }
            Some(Fitted::Resized { data, mime_type }) => match block {
                UserOrToolContent::Image(image) => {
                    image.data = data.to_string();
                    image.mime_type = mime_type.to_string();
                }
                _ => {
                    *block = UserOrToolContent::Image(ImageContent {
                        data: data.to_string(),
                        mime_type: mime_type.to_string(),
                        rest: Map::default(),
                    });
                }
            },
            Some(Fitted::Unusable) => {
                *block = placeholder(data, mime_type, "too large or unreadable to send");
            }
            Some(Fitted::Unchanged) | None => {}
        }
    }
    messages
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::types::{ModelInput, ToolResultMessage, UserMessage};
    use image::GenericImageView as _;

    pub(crate) fn png(width: u32, height: u32, noise: bool) -> String {
        let mut state = 0x2545_f491_u32;
        let image = image::RgbImage::from_fn(width, height, |x, y| {
            if noise {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                image::Rgb(state.to_le_bytes()[..3].try_into().expect("three bytes"))
            } else {
                #[allow(clippy::cast_possible_truncation)]
                image::Rgb([(x % 256) as u8, (y % 256) as u8, 128])
            }
        });
        let mut bytes = Vec::new();
        DynamicImage::ImageRgb8(image)
            .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
            .expect("encode png");
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    pub(crate) fn image_block(data: &str) -> UserOrToolContent {
        UserOrToolContent::Image(ImageContent {
            data: data.to_string(),
            mime_type: "image/png".into(),
            rest: Map::default(),
        })
    }

    pub(crate) fn user_with_image(data: &str) -> Message {
        Message::User(UserMessage {
            content: UserMessageContent::Blocks(vec![
                UserOrToolContent::Text(TextContent {
                    text: "look".into(),
                    text_signature: None,
                    rest: Map::default(),
                }),
                image_block(data),
            ]),
            timestamp: 0,
            rest: Map::default(),
        })
    }

    pub(crate) fn model(api: &str, id: &str) -> Model {
        Model {
            id: id.into(),
            name: id.into(),
            api: api.into(),
            provider: "test".into(),
            base_url: "http://localhost".into(),
            reasoning: false,
            thinking_level_map: None,
            input: vec![ModelInput::Text, ModelInput::Image],
            cost: crate::types::zero_model_cost(),
            context_window: 200_000,
            max_tokens: 8192,
            max_tokens_explicit: false,
            featured: None,
            headers: None,
            compat: None,
        }
    }

    /// (width, height, base64 length) of every image left in `messages`, oldest first.
    fn sent_images(messages: &[Message]) -> Vec<(u32, u32, usize)> {
        let mut messages = messages.to_vec();
        let mut found = Vec::new();
        for message in &mut messages {
            for block in image_blocks_mut(message).into_iter().flatten() {
                if let Some((data, _)) = block.image() {
                    let raw = base64::engine::general_purpose::STANDARD
                        .decode(data)
                        .expect("valid base64");
                    let (width, height) = image::load_from_memory(&raw)
                        .expect("decodable image")
                        .dimensions();
                    found.push((width, height, data.len()));
                }
            }
        }
        found
    }

    fn placeholders(messages: &[Message]) -> Vec<String> {
        let mut messages = messages.to_vec();
        let mut found = Vec::new();
        for message in &mut messages {
            for block in image_blocks_mut(message).into_iter().flatten() {
                if let Some(text) = block
                    .text()
                    .filter(|text| text.starts_with("[image omitted"))
                {
                    found.push(text.to_string());
                }
            }
        }
        found
    }

    #[test]
    fn oversized_single_image_is_downscaled_keeping_aspect_ratio() {
        let history = vec![user_with_image(&png(9000, 600, false))];
        let before = history.clone();
        let sent = fit_images_to_limits(
            history.clone(),
            limits_for(&model("anthropic-messages", "claude-x")),
        );
        assert_eq!(history, before);
        assert_eq!(
            sent_images(&sent),
            vec![(8000, 533, sent_images(&sent)[0].2)]
        );
    }

    #[test]
    fn many_image_request_caps_every_image_at_2000_px() {
        let screenshot = png(2576, 1419, false);
        let history: Vec<Message> = (0..25).map(|_| user_with_image(&screenshot)).collect();
        let sent = fit_images_to_limits(history, ANTHROPIC);
        let images = sent_images(&sent);
        assert_eq!(images.len(), 25);
        assert!(images
            .iter()
            .all(|&(width, height, _)| width == 2000 && height == 1102));
    }

    #[test]
    fn twenty_images_keep_full_size() {
        let screenshot = png(2576, 1419, false);
        let history: Vec<Message> = (0..20).map(|_| user_with_image(&screenshot)).collect();
        let sent = fit_images_to_limits(history.clone(), ANTHROPIC);
        assert_eq!(sent, history);
    }

    #[test]
    fn huge_image_bytes_are_reencoded_under_the_per_image_limit() {
        let noisy = png(1600, 1600, true);
        assert!(noisy.len() > ANTHROPIC.max_image_bytes);
        let sent = fit_images_to_limits(vec![user_with_image(&noisy)], ANTHROPIC);
        let images = sent_images(&sent);
        assert_eq!(images.len(), 1);
        assert!(images[0].2 <= ANTHROPIC.max_image_bytes);
    }

    #[test]
    fn too_many_images_omit_the_oldest() {
        let small = png(64, 48, false);
        let mut history: Vec<Message> = (0..BEDROCK.max_images + 5)
            .map(|_| user_with_image(&small))
            .collect();
        history.push(Message::ToolResult(ToolResultMessage {
            tool_call_id: "call-1".into(),
            tool_name: "attach_image".into(),
            content: vec![image_block(&small)],
            details: None,
            is_error: false,
            timestamp: 0,
            rest: Map::default(),
        }));
        // 26 images against a limit of 20: the oldest are dropped in steps of 5, so 10 go.
        let sent = fit_images_to_limits(history, BEDROCK);
        assert_eq!(sent_images(&sent).len(), 16);
        assert_eq!(
            placeholders(&sent),
            vec!["[image omitted: 64x48 image/png, sent earlier in this conversation]"; 10]
        );
        // The newest image (the tool result) is still sent.
        let Message::ToolResult(last) = sent.last().expect("tool result") else {
            panic!("expected tool result")
        };
        assert!(last.content[0].image().is_some());
        // Omitted images are the oldest ones.
        assert!(matches!(&sent[0], Message::User(user)
            if matches!(&user.content, UserMessageContent::Blocks(blocks) if blocks[1].text().is_some())));
    }

    #[test]
    fn total_image_bytes_stay_under_the_request_budget() {
        let limits = ImageLimits {
            max_total_image_bytes: 3 * MIB,
            ..ANTHROPIC
        };
        let noisy = png(700, 700, true);
        let history: Vec<Message> = (0..5).map(|_| user_with_image(&noisy)).collect();
        let sent = fit_images_to_limits(history, limits);
        let images = sent_images(&sent);
        assert!(!images.is_empty() && images.len() < 5);
        assert!(images.iter().map(|image| image.2).sum::<usize>() <= limits.max_total_image_bytes);
        assert_eq!(placeholders(&sent).len(), 5 - images.len());
    }

    #[test]
    fn claude_behind_other_apis_gets_anthropic_limits() {
        let limits = limits_for(&model("openai-completions", "claude-sonnet-4.5"));
        assert_eq!(limits.many_images_max_dimension, 2000);
        assert_eq!(limits_for(&model("openai-completions", "gpt-5")), OPENAI);
    }
}
