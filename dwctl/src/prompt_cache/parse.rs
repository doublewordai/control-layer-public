
use super::tokenizer::WirePrefix;
use sha2::{Digest, Sha256};

use super::index::{TierPolicy, TtlTier};

pub const MAX_BREAKPOINTS: usize = 4;
pub const WALK_BACK: usize = 20;

const TOOL_DEFINITION_ROLE: &str = "tool_definition";

const TOOL_CALL_ROLE: &str = "tool_call";

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("request body is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("too many cache_control breakpoints (max {MAX_BREAKPOINTS})")]
    TooManyBreakpoints,
    #[error("invalid cache_control ttl: {0:?}")]
    InvalidTtl(String),
    #[error("unsupported cache_control type: {0:?} (only \"ephemeral\")")]
    UnsupportedType(String),
    #[error("cache_control ttl tier '{}' is not currently available", .0.as_str())]
    DisabledTier(TtlTier),
    #[error("cache_control must be an object with a string \"type\": \"ephemeral\" (and an optional string \"ttl\")")]
    MalformedCacheControl,
    #[error("top-level cache_control ttl conflicts with the explicit cache_control on the last block")]
    AutomaticTtlConflict,
    #[error("no breakpoint slot left for automatic caching (all {MAX_BREAKPOINTS} used by explicit markers)")]
    NoAutomaticSlot,
}

#[derive(Debug, Clone)]
pub struct Block {
    pub role: String,
    pub text: String,
    pub message_index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Breakpoint {
    pub block_index: usize,
    pub ttl_tier: TtlTier,
    pub prefix: PrefixSpec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixSpec {
    pub tools_kept: usize,
    pub full_messages: usize,
    pub partial: Option<PartialMessage>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialMessage {
    pub content_parts: usize,
    pub tool_calls: usize,
}

impl PrefixSpec {
    pub fn to_wire(&self) -> WirePrefix {
        if let Some(partial) = &self.partial {
            if partial.tool_calls > 0 {
                WirePrefix::ToolCall {
                    message: self.full_messages,
                    tool_call: partial.tool_calls - 1,
                }
            } else {
                WirePrefix::Block {
                    message: self.full_messages,
                    block: partial.content_parts - 1,
                }
            }
        } else if self.full_messages > 0 {
            WirePrefix::Message {
                message: self.full_messages - 1,
            }
        } else {
            WirePrefix::Tools { tools: self.tools_kept }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParsedPrompt {
    pub blocks: Vec<Block>,
    pub cumulative_hashes: Vec<Vec<u8>>,
    pub breakpoints: Vec<Breakpoint>,
}

impl ParsedPrompt {
    pub fn read_candidates(&self, bp: &Breakpoint) -> Vec<Vec<u8>> {
        let i = bp.block_index;
        let start = i.saturating_sub(WALK_BACK - 1);
        (start..=i).rev().map(|j| self.cumulative_hashes[j].clone()).collect()
    }
}

pub fn parse_chat_completions(body: &[u8], policy: &TierPolicy, telemetry: &TelemetryPolicy) -> Result<ParsedPrompt, ParseError> {
    let v: serde_json::Value = serde_json::from_slice(body)?;

    let mut blocks: Vec<Block> = Vec::new();
    let mut breakpoints: Vec<Breakpoint> = Vec::new();
    let mut cumulative_hashes: Vec<Vec<u8>> = Vec::new();
    let mut hasher = Sha256::new();

    let tools_total = v.get("tools").and_then(|t| t.as_array()).map(|a| a.len()).unwrap_or(0);
    if let Some(tools) = v.get("tools").and_then(|t| t.as_array()) {
        for (tool_ordinal, tool) in tools.iter().enumerate() {
            let ttl = match tool.get("cache_control") {
                Some(cc) if !cc.is_null() => Some(parse_ttl(cc, policy)?),
                _ => None,
            };
            let stripped = strip_cache_control(tool);
            let text = serde_json::to_string(&stripped)?;

            let canonical = canonical_block_bytes(TOOL_DEFINITION_ROLE, stripped);
            hasher.update(&canonical);
            cumulative_hashes.push(hasher.clone().finalize().to_vec());

            let block_index = blocks.len();
            blocks.push(Block {
                role: TOOL_DEFINITION_ROLE.to_string(),
                text,
                message_index: None,
            });
            if let Some(ttl_tier) = ttl {
                breakpoints.push(Breakpoint {
                    block_index,
                    ttl_tier,
                    prefix: PrefixSpec {
                        tools_kept: tool_ordinal + 1,
                        full_messages: 0,
                        partial: None,
                    },
                });
            }
        }
    }

    if let Some(messages) = v.get("messages").and_then(|m| m.as_array()) {
        for (message_index, msg) in messages.iter().enumerate() {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("").to_string();
            let content_len = msg
                .get("content")
                .and_then(|c| c.as_array())
                .map(|a| {
                    a.iter()
                        .filter(|b| !(telemetry.strip_from_prompt && telemetry.excludes_block(&role, b)))
                        .count()
                })
                .unwrap_or(0);
            let calls_len = if role == "assistant" {
                msg.get("tool_calls").and_then(|t| t.as_array()).map(|a| a.len()).unwrap_or(0)
            } else {
                0
            };
            let bp_watermark = breakpoints.len();

            match msg.get("content") {
                Some(serde_json::Value::String(s)) => {
                    let canonical = canonical_block_bytes(&role, serde_json::json!({ "type": "text", "text": s }));
                    hasher.update(&canonical);
                    cumulative_hashes.push(hasher.clone().finalize().to_vec());
                    blocks.push(Block {
                        role: role.clone(),
                        text: s.clone(),
                        message_index: Some(message_index),
                    });
                }
                Some(serde_json::Value::Array(arr)) => {
                    let mut fwd_ordinal = 0usize;
                    for block in arr.iter() {
                        if telemetry.excludes_block(&role, block) {
                            if !telemetry.strip_from_prompt {
                                fwd_ordinal += 1;
                            }
                            continue;
                        }
                        let ttl = match block.get("cache_control") {
                            Some(cc) if !cc.is_null() => Some(parse_ttl(cc, policy)?),
                            _ => None,
                        };
                        let stripped = strip_cache_control(block);
                        let text = stripped.get("text").and_then(|t| t.as_str()).unwrap_or("").to_string();

                        let canonical = canonical_block_bytes(&role, stripped);
                        hasher.update(&canonical);
                        cumulative_hashes.push(hasher.clone().finalize().to_vec());

                        let block_index = blocks.len();
                        blocks.push(Block {
                            role: role.clone(),
                            text,
                            message_index: Some(message_index),
                        });
                        if let Some(ttl_tier) = ttl {
                            breakpoints.push(Breakpoint {
                                block_index,
                                ttl_tier,
                                prefix: PrefixSpec {
                                    tools_kept: tools_total,
                                    full_messages: message_index,
                                    partial: Some(PartialMessage {
                                        content_parts: fwd_ordinal + 1,
                                        tool_calls: 0,
                                    }),
                                },
                            });
                        }
                        fwd_ordinal += 1;
                    }
                }
                _ => {}
            }

            if role == "assistant"
                && let Some(tool_calls) = msg.get("tool_calls").and_then(|t| t.as_array())
            {
                for (call_ordinal, call) in tool_calls.iter().enumerate() {
                    let ttl = match call.get("cache_control") {
                        Some(cc) if !cc.is_null() => Some(parse_ttl(cc, policy)?),
                        _ => None,
                    };
                    let stripped = strip_cache_control(call);
                    let text = serde_json::to_string(&stripped)?;
                    let canonical = canonical_block_bytes(TOOL_CALL_ROLE, stripped);
                    hasher.update(&canonical);
                    cumulative_hashes.push(hasher.clone().finalize().to_vec());

                    let block_index = blocks.len();
                    blocks.push(Block {
                        role: TOOL_CALL_ROLE.to_string(),
                        text,
                        message_index: Some(message_index),
                    });
                    if let Some(ttl_tier) = ttl {
                        breakpoints.push(Breakpoint {
                            block_index,
                            ttl_tier,
                            prefix: PrefixSpec {
                                tools_kept: tools_total,
                                full_messages: message_index,
                                partial: Some(PartialMessage {
                                    content_parts: content_len,
                                    tool_calls: call_ordinal + 1,
                                }),
                            },
                        });
                    }
                }
            }

            for bp in breakpoints[bp_watermark..].iter_mut() {
                if let Some(partial) = &bp.prefix.partial
                    && partial.content_parts == content_len
                    && partial.tool_calls == calls_len
                {
                    bp.prefix.full_messages = message_index + 1;
                    bp.prefix.partial = None;
                }
            }
        }
    }

    if breakpoints.len() > MAX_BREAKPOINTS {
        return Err(ParseError::TooManyBreakpoints);
    }

    if let Some(auto_ttl) = top_level_auto_ttl(&v, policy)? {
        let last_index = blocks.len().saturating_sub(1);
        let last_marker = breakpoints.iter().find(|bp| bp.block_index == last_index).map(|bp| bp.ttl_tier);
        if let AutoAction::Synthesize(tier) = resolve_auto_action(auto_ttl, !blocks.is_empty(), last_marker, breakpoints.len())? {
            let full_messages = v.get("messages").and_then(|m| m.as_array()).map(|a| a.len()).unwrap_or(0);
            breakpoints.push(Breakpoint {
                block_index: last_index,
                ttl_tier: tier,
                prefix: PrefixSpec {
                    tools_kept: tools_total,
                    full_messages,
                    partial: None,
                },
            });
        }
    }

    Ok(ParsedPrompt {
        blocks,
        cumulative_hashes,
        breakpoints,
    })
}

fn top_level_auto_ttl(body: &serde_json::Value, policy: &TierPolicy) -> Result<Option<TtlTier>, ParseError> {
    match body.get("cache_control") {
        Some(cc) if !cc.is_null() => Ok(Some(parse_ttl(cc, policy)?)),
        _ => Ok(None),
    }
}

enum AutoAction {
    Synthesize(TtlTier),
    None,
}

fn resolve_auto_action(
    auto_ttl: TtlTier,
    saw_block: bool,
    last_block_marker: Option<TtlTier>,
    explicit_count: usize,
) -> Result<AutoAction, ParseError> {
    if !saw_block {
        return Ok(AutoAction::None);
    }
    match last_block_marker {
        Some(tier) if tier == auto_ttl => Ok(AutoAction::None),
        Some(_) => Err(ParseError::AutomaticTtlConflict),
        None if explicit_count >= MAX_BREAKPOINTS => Err(ParseError::NoAutomaticSlot),
        None => Ok(AutoAction::Synthesize(auto_ttl)),
    }
}

fn parse_ttl(cache_control: &serde_json::Value, policy: &TierPolicy) -> Result<TtlTier, ParseError> {
    use serde_json::Value;
    if !cache_control.is_object() {
        return Err(ParseError::MalformedCacheControl);
    }
    match cache_control.get("type") {
        Some(Value::String(t)) if t == "ephemeral" => {}
        Some(Value::String(t)) => return Err(ParseError::UnsupportedType(t.clone())),
        _ => return Err(ParseError::MalformedCacheControl),
    }
    let tier = match cache_control.get("ttl") {
        None => policy.default_ttl(),
        Some(Value::String(ttl)) => TtlTier::parse(ttl).ok_or_else(|| ParseError::InvalidTtl(ttl.clone()))?,
        Some(_) => return Err(ParseError::MalformedCacheControl),
    };
    if !policy.is_enabled(tier) {
        return Err(ParseError::DisabledTier(tier));
    }
    Ok(tier)
}

pub fn validate_markers(body: &serde_json::Value, policy: &TierPolicy, telemetry: &TelemetryPolicy) -> Result<(), ParseError> {
    let mut breakpoints = 0usize;
    let mut saw_block = false;
    let mut last_marker: Option<TtlTier> = None;

    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        for tool in tools {
            saw_block = true;
            match tool.get("cache_control") {
                Some(cc) if !cc.is_null() => {
                    let tier = parse_ttl(cc, policy)?;
                    breakpoints += 1;
                    if breakpoints > MAX_BREAKPOINTS {
                        return Err(ParseError::TooManyBreakpoints);
                    }
                    last_marker = Some(tier);
                }
                _ => last_marker = None,
            }
        }
    }
    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("");
            match msg.get("content") {
                Some(serde_json::Value::String(_)) => {
                    saw_block = true;
                    last_marker = None;
                }
                Some(serde_json::Value::Array(arr)) => {
                    for block in arr.iter() {
                        if telemetry.excludes_block(role, block) {
                            continue;
                        }
                        saw_block = true;
                        match block.get("cache_control") {
                            Some(cc) if !cc.is_null() => {
                                let tier = parse_ttl(cc, policy)?;
                                breakpoints += 1;
                                if breakpoints > MAX_BREAKPOINTS {
                                    return Err(ParseError::TooManyBreakpoints);
                                }
                                last_marker = Some(tier);
                            }
                            _ => last_marker = None,
                        }
                    }
                }
                _ => {}
            }

            if role == "assistant"
                && let Some(tool_calls) = msg.get("tool_calls").and_then(|t| t.as_array())
            {
                for call in tool_calls.iter() {
                    saw_block = true;
                    match call.get("cache_control") {
                        Some(cc) if !cc.is_null() => {
                            let tier = parse_ttl(cc, policy)?;
                            breakpoints += 1;
                            if breakpoints > MAX_BREAKPOINTS {
                                return Err(ParseError::TooManyBreakpoints);
                            }
                            last_marker = Some(tier);
                        }
                        _ => last_marker = None,
                    }
                }
            }
        }
    }

    if let Some(auto_ttl) = top_level_auto_ttl(body, policy)? {
        resolve_auto_action(auto_ttl, saw_block, last_marker, breakpoints)?;
    }
    Ok(())
}

fn strip_cache_control(block: &serde_json::Value) -> serde_json::Value {
    let mut b = block.clone();
    if let Some(obj) = b.as_object_mut() {
        obj.shift_remove("cache_control");
    }
    b
}

#[derive(Debug, Clone, Default)]
pub struct TelemetryPolicy {
    pub strip_from_prompt: bool,
    prefixes: Vec<String>,
}

impl TelemetryPolicy {
    pub fn from_config(strip_from_prompt: bool, prefixes: &[String]) -> Self {
        Self {
            strip_from_prompt,
            prefixes: prefixes.iter().filter(|p| !p.is_empty()).cloned().collect(),
        }
    }

    pub fn excludes_block(&self, role: &str, block: &serde_json::Value) -> bool {
        if self.prefixes.is_empty() || role != TELEMETRY_ROLE {
            return false;
        }
        let unmarked = block.get("cache_control").is_none_or(|cc| cc.is_null());
        if !unmarked {
            return false;
        }
        let text = block.get("text").and_then(|v| v.as_str()).unwrap_or("");
        let t = text.trim_start();
        self.prefixes.iter().any(|prefix| t.starts_with(prefix.as_str()))
    }
}

pub(crate) const TELEMETRY_ROLE: &str = "system";

fn canonical_block_bytes(role: &str, mut stripped_block: serde_json::Value) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(role.as_bytes());
    out.push(0x00);
    if let Some(content_only) = image_block_without_upload_id(&stripped_block) {
        stripped_block = content_only;
    }
    stripped_block.sort_all_objects();
    out.extend_from_slice(&serde_json::to_vec(&stripped_block).unwrap_or_default());
    out
}

fn image_block_without_upload_id(block: &serde_json::Value) -> Option<serde_json::Value> {
    use crate::image_normalizer::ImageToken;
    let pointer = match block.get("type")?.as_str()? {
        "image_url" => "/image_url/url",
        "input_image" => "/image_url",
        _ => return None,
    };
    let url = block.pointer(pointer)?.as_str()?;
    if !ImageToken::looks_like_token(url) {
        return None;
    }
    let token: ImageToken = url.parse().ok()?;
    let canonical = ImageToken(token.0, None).to_dw_img_uri();
    if canonical == url {
        return None;
    }
    let mut block = block.clone();
    *block.pointer_mut(pointer)? = canonical.into();
    Some(block)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_spec_end_of_message_is_whole_messages() {
        let body = serde_json::json!({
            "messages": [
                {"role": "system", "content": [{"type":"text","text":"sys","cache_control":{"type":"ephemeral"}}]},
                {"role": "user", "content": [
                    {"type":"text","text":"a"},
                    {"type":"text","text":"b","cache_control":{"type":"ephemeral"}}
                ]}
            ]
        })
        .to_string();
        let p = parse_chat_completions(
            body.as_bytes(),
            &TierPolicy::from_config(&["5m".to_string()], "5m"),
            &TelemetryPolicy::default(),
        )
        .unwrap();
        assert_eq!(
            p.breakpoints[0].prefix,
            PrefixSpec {
                tools_kept: 0,
                full_messages: 1,
                partial: None
            }
        );
        assert_eq!(
            p.breakpoints[1].prefix,
            PrefixSpec {
                tools_kept: 0,
                full_messages: 2,
                partial: None
            }
        );
    }

    #[test]
    fn prefix_spec_mid_message_keeps_partial_content() {
        let body = serde_json::json!({
            "messages": [
                {"role": "user", "content": [
                    {"type":"text","text":"c","cache_control":{"type":"ephemeral"}},
                    {"type":"text","text":"d"}
                ]}
            ]
        })
        .to_string();
        let p = parse_chat_completions(
            body.as_bytes(),
            &TierPolicy::from_config(&["5m".to_string()], "5m"),
            &TelemetryPolicy::default(),
        )
        .unwrap();
        assert_eq!(
            p.breakpoints[0].prefix,
            PrefixSpec {
                tools_kept: 0,
                full_messages: 0,
                partial: Some(PartialMessage {
                    content_parts: 1,
                    tool_calls: 0
                }),
            },
            "mid-message marker keeps exactly the marked parts"
        );
    }

    #[test]
    fn prefix_spec_tool_definition_and_tool_call_markers() {
        let body = serde_json::json!({
            "tools": [
                {"type":"function","function":{"name":"f"},"cache_control":{"type":"ephemeral"}},
                {"type":"function","function":{"name":"g"}}
            ],
            "messages": [
                {"role":"user","content":[{"type":"text","text":"hi"}]},
                {"role":"assistant","content":[{"type":"text","text":"t"}],
                 "tool_calls":[{"id":"c1","type":"function","function":{"name":"f","arguments":"{}"},
                                "cache_control":{"type":"ephemeral"}}]}
            ]
        })
        .to_string();
        let p = parse_chat_completions(
            body.as_bytes(),
            &TierPolicy::from_config(&["5m".to_string()], "5m"),
            &TelemetryPolicy::default(),
        )
        .unwrap();
        assert_eq!(
            p.breakpoints[0].prefix,
            PrefixSpec {
                tools_kept: 1,
                full_messages: 0,
                partial: None
            }
        );
        assert_eq!(
            p.breakpoints[1].prefix,
            PrefixSpec {
                tools_kept: 2,
                full_messages: 2,
                partial: None
            }
        );
    }

    #[test]
    fn wire_prefix_maps_every_marker_shape() {
        let cases = [
            (
                PrefixSpec {
                    tools_kept: 2,
                    full_messages: 0,
                    partial: None,
                },
                WirePrefix::Tools { tools: 2 },
                serde_json::json!({"tools": 2}),
            ),
            (
                PrefixSpec {
                    tools_kept: 2,
                    full_messages: 3,
                    partial: None,
                },
                WirePrefix::Message { message: 2 },
                serde_json::json!({"message": 2}),
            ),
            (
                PrefixSpec {
                    tools_kept: 0,
                    full_messages: 1,
                    partial: Some(PartialMessage {
                        content_parts: 2,
                        tool_calls: 0,
                    }),
                },
                WirePrefix::Block { message: 1, block: 1 },
                serde_json::json!({"message": 1, "block": 1}),
            ),
            (
                PrefixSpec {
                    tools_kept: 0,
                    full_messages: 1,
                    partial: Some(PartialMessage {
                        content_parts: 2,
                        tool_calls: 1,
                    }),
                },
                WirePrefix::ToolCall { message: 1, tool_call: 0 },
                serde_json::json!({"message": 1, "tool_call": 0}),
            ),
        ];
        for (spec, wire, json) in cases {
            assert_eq!(spec.to_wire(), wire);
            assert_eq!(serde_json::to_value(&wire).unwrap(), json, "wire serialization");
        }
    }

    #[test]
    fn telemetry_strip_mode_renumbers_wire_ordinals() {
        let body = serde_json::json!({
            "messages": [
                {"role": "system", "content": [
                    {"type":"text","text":"x-anthropic-billing-header: cch=1"},
                    {"type":"text","text":"kept","cache_control":{"type":"ephemeral"}},
                    {"type":"text","text":"tail"}
                ]}
            ]
        });
        let strip = parse_with(body.clone(), &telemetry());
        assert_eq!(
            strip.breakpoints[0].prefix.partial,
            Some(PartialMessage {
                content_parts: 1,
                tool_calls: 0
            })
        );
        let keep = parse_with(
            body,
            &TelemetryPolicy::from_config(false, &["x-anthropic-billing-header:".to_string()]),
        );
        assert_eq!(
            keep.breakpoints[0].prefix.partial,
            Some(PartialMessage {
                content_parts: 2,
                tool_calls: 0
            })
        );
    }

    fn all_tiers() -> TierPolicy {
        TierPolicy::from_config(&["5m".to_string(), "1h".to_string(), "24h".to_string()], "5m")
    }

    fn no_telemetry() -> TelemetryPolicy {
        TelemetryPolicy::default()
    }

    fn telemetry() -> TelemetryPolicy {
        TelemetryPolicy::from_config(true, &["x-anthropic-billing-header:".to_string()])
    }

    fn parse(body: serde_json::Value) -> ParsedPrompt {
        parse_chat_completions(body.to_string().as_bytes(), &all_tiers(), &no_telemetry()).unwrap()
    }

    fn parse_with(body: serde_json::Value, tele: &TelemetryPolicy) -> ParsedPrompt {
        parse_chat_completions(body.to_string().as_bytes(), &all_tiers(), tele).unwrap()
    }

    #[test]
    fn no_markers_no_breakpoints() {
        let p = parse(serde_json::json!({
            "model": "m",
            "messages": [
                {"role": "system", "content": "you are helpful"},
                {"role": "user", "content": "hi"}
            ]
        }));
        assert_eq!(p.blocks.len(), 2);
        assert_eq!(p.cumulative_hashes.len(), 2);
        assert!(p.breakpoints.is_empty());
        assert_ne!(p.cumulative_hashes[0], p.cumulative_hashes[1]);
    }

    #[test]
    fn single_marker_with_default_ttl() {
        let p = parse(serde_json::json!({
            "messages": [
                {"role": "system", "content": [
                    {"type": "text", "text": "long ctx", "cache_control": {"type": "ephemeral"}}
                ]},
                {"role": "user", "content": "q"}
            ]
        }));
        assert_eq!(p.breakpoints.len(), 1);
        assert_eq!(p.breakpoints[0].block_index, 0);
        assert_eq!(p.breakpoints[0].ttl_tier, TtlTier::FiveMinutes);
    }

    #[test]
    fn ttl_tiers_parse() {
        for (ttl, tier) in [
            ("5m", TtlTier::FiveMinutes),
            ("1h", TtlTier::OneHour),
            ("24h", TtlTier::TwentyFourHours),
        ] {
            let p = parse(serde_json::json!({
                "messages": [{"role": "system", "content": [
                    {"type": "text", "text": "x", "cache_control": {"type": "ephemeral", "ttl": ttl}}
                ]}]
            }));
            assert_eq!(p.breakpoints[0].ttl_tier, tier);
        }
    }

    #[test]
    fn invalid_ttl_errors() {
        let err = parse_chat_completions(
            serde_json::json!({
                "messages": [{"role": "system", "content": [
                    {"type": "text", "text": "x", "cache_control": {"type": "ephemeral", "ttl": "2h"}}
                ]}]
            })
            .to_string()
            .as_bytes(),
            &all_tiers(),
            &no_telemetry(),
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::InvalidTtl(t) if t == "2h"));
    }

    #[test]
    fn unsupported_cache_control_type_errors() {
        let err = parse_chat_completions(
            serde_json::json!({
                "messages": [{"role": "system", "content": [
                    {"type": "text", "text": "x", "cache_control": {"type": "persistent"}}
                ]}]
            })
            .to_string()
            .as_bytes(),
            &all_tiers(),
            &no_telemetry(),
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::UnsupportedType(t) if t == "persistent"));
    }

    #[test]
    fn more_than_four_breakpoints_errors() {
        let blocks: Vec<_> = (0..5)
            .map(|i| serde_json::json!({"type": "text", "text": format!("b{i}"), "cache_control": {"type": "ephemeral"}}))
            .collect();
        let err = parse_chat_completions(
            serde_json::json!({ "messages": [{"role": "user", "content": blocks}] })
                .to_string()
                .as_bytes(),
            &all_tiers(),
            &no_telemetry(),
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::TooManyBreakpoints));
    }

    #[test]
    fn disabled_tier_rejected_by_validate_and_parse() {
        let policy = TierPolicy::from_config(&["5m".to_string(), "1h".to_string()], "5m");
        let body = serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x", "cache_control": {"type": "ephemeral", "ttl": "24h"}}
            ]}]
        });
        let err = validate_markers(&body, &policy, &no_telemetry()).unwrap_err();
        assert!(matches!(err, ParseError::DisabledTier(TtlTier::TwentyFourHours)));
        let err2 = parse_chat_completions(body.to_string().as_bytes(), &policy, &no_telemetry()).unwrap_err();
        assert!(matches!(err2, ParseError::DisabledTier(TtlTier::TwentyFourHours)));
    }

    #[test]
    fn validate_markers_default_ttl_honours_policy() {
        let policy = TierPolicy::from_config(&["1h".to_string()], "1h");
        let body = serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x", "cache_control": {"type": "ephemeral"}}
            ]}]
        });
        assert!(validate_markers(&body, &policy, &no_telemetry()).is_ok());
        let p = parse_chat_completions(body.to_string().as_bytes(), &policy, &no_telemetry()).unwrap();
        assert_eq!(p.breakpoints[0].ttl_tier, TtlTier::OneHour);
    }

    #[test]
    fn validate_markers_ok_and_counts_breakpoints() {
        let ok = serde_json::json!({
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "a", "cache_control": {"type": "ephemeral", "ttl": "1h"}},
                {"type": "text", "text": "q"}
            ]}]
        });
        assert!(validate_markers(&ok, &all_tiers(), &no_telemetry()).is_ok());

        let blocks: Vec<_> = (0..5)
            .map(|i| serde_json::json!({"type": "text", "text": format!("b{i}"), "cache_control": {"type": "ephemeral"}}))
            .collect();
        let too_many = serde_json::json!({ "messages": [{"role": "user", "content": blocks}] });
        assert!(matches!(
            validate_markers(&too_many, &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::TooManyBreakpoints
        ));
    }

    #[test]
    fn non_object_cache_control_is_malformed() {
        let body = serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x", "cache_control": "persistent"}
            ]}]
        });
        assert!(matches!(
            validate_markers(&body, &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::MalformedCacheControl
        ));
        assert!(matches!(
            parse_chat_completions(body.to_string().as_bytes(), &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::MalformedCacheControl
        ));
    }

    #[test]
    fn non_string_type_or_ttl_is_malformed() {
        let bad_ttl = serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x", "cache_control": {"type": "ephemeral", "ttl": 123}}
            ]}]
        });
        assert!(matches!(
            validate_markers(&bad_ttl, &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::MalformedCacheControl
        ));

        let bad_type = serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x", "cache_control": {"type": true}}
            ]}]
        });
        assert!(matches!(
            validate_markers(&bad_type, &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::MalformedCacheControl
        ));
    }

    #[test]
    fn missing_type_is_malformed() {
        let no_type = serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x", "cache_control": {"ttl": "1h"}}
            ]}]
        });
        assert!(matches!(
            validate_markers(&no_type, &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::MalformedCacheControl
        ));
        let empty = serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x", "cache_control": {}}
            ]}]
        });
        assert!(matches!(
            parse_chat_completions(empty.to_string().as_bytes(), &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::MalformedCacheControl
        ));
    }

    #[test]
    fn null_cache_control_is_no_marker() {
        let body = serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x", "cache_control": null}
            ]}]
        });
        assert!(validate_markers(&body, &all_tiers(), &no_telemetry()).is_ok());
        let p = parse_chat_completions(body.to_string().as_bytes(), &all_tiers(), &no_telemetry()).unwrap();
        assert!(p.breakpoints.is_empty());
    }

    #[test]
    fn cache_control_excluded_from_hash() {
        let marked = parse(serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "shared prefix", "cache_control": {"type": "ephemeral", "ttl": "1h"}}
            ]}]
        }));
        let unmarked = parse(serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "shared prefix"}
            ]}]
        }));
        assert_eq!(marked.cumulative_hashes[0], unmarked.cumulative_hashes[0]);
        let other = parse(serde_json::json!({
            "messages": [{"role": "system", "content": [{"type": "text", "text": "different"}]}]
        }));
        assert_ne!(marked.cumulative_hashes[0], other.cumulative_hashes[0]);
    }

    #[test]
    fn walk_back_candidates_longest_first_bounded() {
        let blocks: Vec<_> = (0..25)
            .map(|i| {
                let mut b = serde_json::json!({"type": "text", "text": format!("b{i}")});
                if i == 24 {
                    b["cache_control"] = serde_json::json!({"type": "ephemeral"});
                }
                b
            })
            .collect();
        let p = parse(serde_json::json!({ "messages": [{"role": "user", "content": blocks}] }));
        let cands = p.read_candidates(&p.breakpoints[0]);
        assert_eq!(cands.len(), WALK_BACK);
        assert_eq!(cands[0], p.cumulative_hashes[24]);
        assert_eq!(cands[1], p.cumulative_hashes[23]);
        assert_eq!(cands[WALK_BACK - 1], p.cumulative_hashes[5]);
    }

    #[test]
    fn deterministic() {
        let body = serde_json::json!({
            "messages": [{"role": "system", "content": [{"type": "text", "text": "abc"}]}]
        });
        assert_eq!(parse(body.clone()).cumulative_hashes, parse(body).cumulative_hashes);
    }

    #[test]
    fn tool_definition_marker_creates_breakpoint() {
        let p = parse(serde_json::json!({
            "tools": [
                {"type": "function", "function": {"name": "lookup", "parameters": {}},
                 "cache_control": {"type": "ephemeral", "ttl": "1h"}}
            ],
            "messages": [{"role": "user", "content": "q"}]
        }));
        assert_eq!(p.blocks.len(), 2);
        assert_eq!(p.blocks[0].role, "tool_definition");
        assert_eq!(p.blocks[1].role, "user");
        assert_eq!(p.breakpoints.len(), 1);
        assert_eq!(p.breakpoints[0].block_index, 0);
        assert_eq!(p.breakpoints[0].ttl_tier, TtlTier::OneHour);
        assert!(p.blocks[0].text.contains("lookup"));
    }

    #[test]
    fn tools_hashed_before_messages() {
        let with_tool = parse(serde_json::json!({
            "tools": [{"type": "function", "function": {"name": "f", "parameters": {}}}],
            "messages": [{"role": "system", "content": "sys"}, {"role": "user", "content": "q"}]
        }));
        let without = parse(serde_json::json!({
            "messages": [{"role": "system", "content": "sys"}, {"role": "user", "content": "q"}]
        }));
        assert_eq!(with_tool.blocks.len(), 3);
        assert_eq!(with_tool.blocks[0].role, "tool_definition");
        assert_eq!(with_tool.blocks[1].role, "system");
        assert_ne!(with_tool.cumulative_hashes[1], without.cumulative_hashes[0]);
    }

    #[test]
    fn tool_marker_excluded_from_hash() {
        let marked = parse(serde_json::json!({
            "tools": [{"type": "function", "function": {"name": "f", "parameters": {}},
                       "cache_control": {"type": "ephemeral", "ttl": "1h"}}]
        }));
        let unmarked = parse(serde_json::json!({
            "tools": [{"type": "function", "function": {"name": "f", "parameters": {}}}]
        }));
        assert_eq!(marked.cumulative_hashes[0], unmarked.cumulative_hashes[0]);
    }

    #[test]
    fn tool_markers_count_toward_breakpoint_cap() {
        let tools: Vec<_> = (0..3)
            .map(|i| {
                serde_json::json!({
                "type": "function", "function": {"name": format!("f{i}"), "parameters": {}},
                "cache_control": {"type": "ephemeral"}})
            })
            .collect();
        let body = serde_json::json!({
            "tools": tools,
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "a", "cache_control": {"type": "ephemeral"}},
                {"type": "text", "text": "b", "cache_control": {"type": "ephemeral"}}
            ]}]
        });
        assert!(matches!(
            validate_markers(&body, &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::TooManyBreakpoints
        ));
        assert!(matches!(
            parse_chat_completions(body.to_string().as_bytes(), &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::TooManyBreakpoints
        ));
    }

    #[test]
    fn validate_markers_accepts_tool_marker() {
        let body = serde_json::json!({
            "tools": [{"type": "function", "function": {"name": "f", "parameters": {}},
                       "cache_control": {"type": "ephemeral", "ttl": "1h"}}],
            "messages": [{"role": "user", "content": "q"}]
        });
        assert!(validate_markers(&body, &all_tiers(), &no_telemetry()).is_ok());
    }

    #[test]
    fn provider_telemetry_block_excluded_from_cache_view() {
        let tele = telemetry();
        let with = parse_with(
            serde_json::json!({
                "messages": [{"role": "system", "content": [
                    {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1; cch=abc123;"},
                    {"type": "text", "text": "stable system prompt"}
                ]}]
            }),
            &tele,
        );
        let without = parse_with(
            serde_json::json!({
                "messages": [{"role": "system", "content": [
                    {"type": "text", "text": "stable system prompt"}
                ]}]
            }),
            &tele,
        );
        assert_eq!(with.blocks.len(), 1, "telemetry block is skipped");
        assert_eq!(with.blocks[0].text, "stable system prompt");
        assert_eq!(with.cumulative_hashes[0], without.cumulative_hashes[0]);
    }

    #[test]
    fn telemetry_kept_when_feature_disabled() {
        let p = parse(serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x-anthropic-billing-header: cch=abc;"},
                {"type": "text", "text": "stable"}
            ]}]
        }));
        assert_eq!(p.blocks.len(), 2, "feature off → telemetry block not excluded");
    }

    #[test]
    fn telemetry_only_excluded_from_system_role() {
        let p = parse_with(
            serde_json::json!({
                "messages": [{"role": "user", "content": [
                    {"type": "text", "text": "x-anthropic-billing-header: cch=abc;"},
                    {"type": "text", "text": "actual question"}
                ]}]
            }),
            &telemetry(),
        );
        assert_eq!(p.blocks.len(), 2, "user-role block starting with the prefix is not excluded");
    }

    #[test]
    fn empty_prefix_is_dropped_not_match_all() {
        let tele = TelemetryPolicy::from_config(true, &["".to_string()]);
        let p = parse_with(
            serde_json::json!({
                "messages": [{"role": "system", "content": [{"type": "text", "text": "anything at all"}]}]
            }),
            &tele,
        );
        assert_eq!(p.blocks.len(), 1, "empty prefix must not exclude blocks");
    }

    #[test]
    fn telemetry_nonce_does_not_change_prefix_hash() {
        let mk = |nonce: &str| {
            serde_json::json!({
                "messages": [
                    {"role": "system", "content": [
                        {"type": "text", "text": format!("x-anthropic-billing-header: cc_entrypoint=sdk-py; cch={nonce};")},
                        {"type": "text", "text": "long stable system prompt", "cache_control": {"type": "ephemeral", "ttl": "5m"}}
                    ]},
                    {"role": "user", "content": "hello"}
                ]
            })
        };
        let a = parse_with(mk("47d6f"), &telemetry());
        let b = parse_with(mk("b1b38"), &telemetry());
        assert_eq!(a.blocks.len(), 2);
        assert_eq!(a.breakpoints.len(), 1);
        assert_eq!(a.breakpoints[0].block_index, 0);
        assert_eq!(a.cumulative_hashes[0], b.cumulative_hashes[0]);
    }

    #[test]
    fn marked_block_is_not_treated_as_telemetry() {
        let p = parse_with(
            serde_json::json!({
                "messages": [{"role": "system", "content": [
                    {"type": "text", "text": "x-anthropic-billing-header: keep me", "cache_control": {"type": "ephemeral"}}
                ]}]
            }),
            &telemetry(),
        );
        assert_eq!(p.blocks.len(), 1);
        assert_eq!(p.breakpoints.len(), 1);
    }

    #[test]
    fn disabled_tier_on_tool_marker_rejected() {
        let policy = TierPolicy::from_config(&["5m".to_string(), "1h".to_string()], "5m");
        let body = serde_json::json!({
            "tools": [{"type": "function", "function": {"name": "f", "parameters": {}},
                       "cache_control": {"type": "ephemeral", "ttl": "24h"}}]
        });
        assert!(matches!(
            validate_markers(&body, &policy, &no_telemetry()).unwrap_err(),
            ParseError::DisabledTier(TtlTier::TwentyFourHours)
        ));
    }

    #[test]
    fn single_text_array_hashes_like_string_content() {
        let marked = parse(serde_json::json!({
            "messages": [{"role": "tool", "content": [
                {"type": "text", "text": "tool output", "cache_control": {"type": "ephemeral"}}
            ]}]
        }));
        let unmarked = parse(serde_json::json!({
            "messages": [{"role": "tool", "content": "tool output"}]
        }));
        assert_eq!(
            marked.cumulative_hashes[0], unmarked.cumulative_hashes[0],
            "marked single-part array must hash like the plain string"
        );
        assert_eq!(marked.breakpoints.len(), 1);
        assert!(unmarked.breakpoints.is_empty());
    }


    #[test]
    fn tool_call_is_hashed_and_marker_is_a_breakpoint() {
        let p = parse(serde_json::json!({
            "messages": [
                {"role": "user", "content": "q"},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"id": "tu1", "type": "function", "function": {"name": "lookup", "arguments": "{}"},
                     "cache_control": {"type": "ephemeral", "ttl": "1h"}}
                ]}
            ]
        }));
        assert_eq!(p.blocks.len(), 2, "user block + the tool_call block (null content adds none)");
        assert_eq!(p.blocks[1].role, "tool_call");
        assert_eq!(p.breakpoints.len(), 1);
        assert_eq!(p.breakpoints[0].block_index, 1);
        assert_eq!(p.breakpoints[0].ttl_tier, TtlTier::OneHour);
        assert!(p.blocks[1].text.contains("lookup"), "the call JSON is the write-side text");
    }

    #[test]
    fn tool_call_after_text_preserves_order() {
        let p = parse(serde_json::json!({
            "messages": [{"role": "assistant", "content": [{"type": "text", "text": "calling"}],
                          "tool_calls": [{"id": "t", "type": "function", "function": {"name": "f", "arguments": "{}"}}]}]
        }));
        assert_eq!(p.blocks.len(), 2);
        assert_eq!(p.blocks[0].role, "assistant");
        assert_eq!(p.blocks[1].role, "tool_call");
    }

    #[test]
    fn tool_call_marker_excluded_from_hash() {
        let marked = parse(serde_json::json!({
            "messages": [{"role": "assistant", "content": null, "tool_calls": [
                {"id": "tu1", "type": "function", "function": {"name": "f", "arguments": "{}"}, "cache_control": {"type": "ephemeral"}}
            ]}]
        }));
        let unmarked = parse(serde_json::json!({
            "messages": [{"role": "assistant", "content": null, "tool_calls": [
                {"id": "tu1", "type": "function", "function": {"name": "f", "arguments": "{}"}}
            ]}]
        }));
        assert_eq!(marked.cumulative_hashes[0], unmarked.cumulative_hashes[0]);
        assert_eq!(marked.breakpoints.len(), 1);
        assert!(unmarked.breakpoints.is_empty());
    }

    #[test]
    fn distinct_tool_calls_hash_differently() {
        let a = parse(serde_json::json!({
            "messages": [{"role": "assistant", "content": null, "tool_calls": [
                {"id": "t", "type": "function", "function": {"name": "f", "arguments": "{\"x\":1}"}}
            ]}]
        }));
        let b = parse(serde_json::json!({
            "messages": [{"role": "assistant", "content": null, "tool_calls": [
                {"id": "t", "type": "function", "function": {"name": "f", "arguments": "{\"x\":2}"}}
            ]}]
        }));
        assert_ne!(a.cumulative_hashes[0], b.cumulative_hashes[0]);
    }

    #[test]
    fn tool_call_markers_validate_and_count() {
        let policy = TierPolicy::from_config(&["5m".to_string(), "1h".to_string()], "5m");
        let body = serde_json::json!({
            "messages": [{"role": "assistant", "content": null, "tool_calls": [
                {"id": "t", "type": "function", "function": {"name": "f", "arguments": "{}"}, "cache_control": {"type": "ephemeral", "ttl": "24h"}}
            ]}]
        });
        assert!(matches!(
            validate_markers(&body, &policy, &no_telemetry()).unwrap_err(),
            ParseError::DisabledTier(TtlTier::TwentyFourHours)
        ));
    }

    #[test]
    fn tool_calls_on_non_assistant_message_are_ignored() {
        let p = parse(serde_json::json!({
            "messages": [{"role": "user", "content": "q", "tool_calls": [
                {"id": "t", "type": "function", "function": {"name": "f", "arguments": "{}"},
                 "cache_control": {"type": "ephemeral"}}
            ]}]
        }));
        assert_eq!(p.blocks.len(), 1, "only the user content block; the stray tool_calls is ignored");
        assert_eq!(p.blocks[0].role, "user");
        assert!(p.breakpoints.is_empty(), "the stray tool_call marker is not a breakpoint");

        let policy = TierPolicy::from_config(&["5m".to_string()], "5m");
        let body = serde_json::json!({
            "messages": [{"role": "user", "content": "q", "tool_calls": [
                {"id": "t", "type": "function", "function": {"name": "f", "arguments": "{}"}, "cache_control": {"type": "ephemeral", "ttl": "24h"}}
            ]}]
        });
        assert!(validate_markers(&body, &policy, &no_telemetry()).is_ok());
    }


    #[test]
    fn automatic_marker_synthesizes_breakpoint_on_last_block() {
        let p = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "system", "content": "static system"},
                {"role": "user", "content": "turn 1"},
                {"role": "assistant", "content": "reply"},
                {"role": "user", "content": "turn 2"}
            ]
        }));
        assert_eq!(p.blocks.len(), 4);
        assert_eq!(p.breakpoints.len(), 1);
        assert_eq!(p.breakpoints[0].block_index, 3, "on the last block");
        assert_eq!(p.breakpoints[0].ttl_tier, TtlTier::FiveMinutes);
        assert!(
            validate_markers(
                &serde_json::json!({
                    "cache_control": {"type": "ephemeral"},
                    "messages": [{"role": "user", "content": "q"}]
                }),
                &all_tiers(),
                &no_telemetry()
            )
            .is_ok()
        );
    }

    #[test]
    fn automatic_marker_honours_ttl() {
        let p = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral", "ttl": "1h"},
            "messages": [{"role": "user", "content": "q"}]
        }));
        assert_eq!(p.breakpoints.len(), 1);
        assert_eq!(p.breakpoints[0].ttl_tier, TtlTier::OneHour);
    }

    #[test]
    fn automatic_combines_with_explicit_breakpoint() {
        let p = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "system", "content": [
                    {"type": "text", "text": "sys", "cache_control": {"type": "ephemeral"}}
                ]},
                {"role": "user", "content": "q"}
            ]
        }));
        assert_eq!(p.breakpoints.len(), 2);
        assert_eq!(p.breakpoints[0].block_index, 0, "explicit system breakpoint");
        assert_eq!(p.breakpoints[1].block_index, 1, "synthesized on the last block");
    }

    #[test]
    fn automatic_noop_when_last_block_already_marked_same_ttl() {
        let body = serde_json::json!({
            "cache_control": {"type": "ephemeral", "ttl": "1h"},
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "q", "cache_control": {"type": "ephemeral", "ttl": "1h"}}
            ]}]
        });
        let p = parse(body.clone());
        assert_eq!(p.breakpoints.len(), 1, "no duplicate breakpoint");
        assert_eq!(p.breakpoints[0].block_index, 0);
        assert!(validate_markers(&body, &all_tiers(), &no_telemetry()).is_ok());
    }

    #[test]
    fn automatic_conflicting_ttl_on_last_block_errors() {
        let body = serde_json::json!({
            "cache_control": {"type": "ephemeral", "ttl": "1h"},
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "q", "cache_control": {"type": "ephemeral", "ttl": "5m"}}
            ]}]
        });
        assert!(matches!(
            parse_chat_completions(body.to_string().as_bytes(), &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::AutomaticTtlConflict
        ));
        assert!(matches!(
            validate_markers(&body, &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::AutomaticTtlConflict
        ));
    }

    #[test]
    fn automatic_no_slot_when_four_explicit_and_last_block_unmarked() {
        let mut content = vec![];
        for i in 0..4 {
            content.push(serde_json::json!({"type": "text", "text": format!("b{i}"), "cache_control": {"type": "ephemeral"}}));
        }
        content.push(serde_json::json!({"type": "text", "text": "last, unmarked"}));
        let body = serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [{"role": "user", "content": content}]
        });
        assert!(matches!(
            parse_chat_completions(body.to_string().as_bytes(), &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::NoAutomaticSlot
        ));
        assert!(matches!(
            validate_markers(&body, &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::NoAutomaticSlot
        ));
    }

    #[test]
    fn automatic_ok_when_four_explicit_but_last_block_is_one_of_them_same_ttl() {
        let mut content = vec![];
        for i in 0..4 {
            content.push(serde_json::json!({"type": "text", "text": format!("b{i}"), "cache_control": {"type": "ephemeral"}}));
        }
        let body = serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [{"role": "user", "content": content}]
        });
        let p = parse(body.clone());
        assert_eq!(p.breakpoints.len(), 4, "no 5th breakpoint; automatic is a no-op");
        assert!(validate_markers(&body, &all_tiers(), &no_telemetry()).is_ok());
    }

    #[test]
    fn automatic_skipped_when_no_blocks() {
        let body = serde_json::json!({ "cache_control": {"type": "ephemeral"}, "messages": [] });
        let p = parse(body.clone());
        assert!(p.breakpoints.is_empty());
        assert!(validate_markers(&body, &all_tiers(), &no_telemetry()).is_ok());
    }

    #[test]
    fn automatic_null_is_no_marker() {
        let body = serde_json::json!({
            "cache_control": null,
            "messages": [{"role": "user", "content": "q"}]
        });
        let p = parse(body.clone());
        assert!(p.breakpoints.is_empty());
        assert!(validate_markers(&body, &all_tiers(), &no_telemetry()).is_ok());
    }

    #[test]
    fn automatic_malformed_and_disabled_rejected() {
        let malformed = serde_json::json!({
            "cache_control": "ephemeral",
            "messages": [{"role": "user", "content": "q"}]
        });
        assert!(matches!(
            validate_markers(&malformed, &all_tiers(), &no_telemetry()).unwrap_err(),
            ParseError::MalformedCacheControl
        ));
        let policy = TierPolicy::from_config(&["5m".to_string(), "1h".to_string()], "5m");
        let disabled = serde_json::json!({
            "cache_control": {"type": "ephemeral", "ttl": "24h"},
            "messages": [{"role": "user", "content": "q"}]
        });
        assert!(matches!(
            validate_markers(&disabled, &policy, &no_telemetry()).unwrap_err(),
            ParseError::DisabledTier(TtlTier::TwentyFourHours)
        ));
    }

    #[test]
    fn automatic_advances_prefix_hash_grows_but_shares_prior() {
        let turn1 = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "turn 1"}
            ]
        }));
        let turn2 = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "turn 1"},
                {"role": "assistant", "content": "reply"},
                {"role": "user", "content": "turn 2"}
            ]
        }));
        assert_eq!(turn1.breakpoints[0].block_index, 1);
        assert_eq!(turn1.cumulative_hashes[1], turn2.cumulative_hashes[1]);
        assert_eq!(turn2.breakpoints[0].block_index, 3);
    }

    #[test]
    fn automatic_marker_lands_on_trailing_tool_call() {
        let p = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "user", "content": "q"},
                {"role": "assistant", "content": [{"type": "text", "text": "calling"}], "tool_calls": [
                    {"id": "c1", "type": "function", "function": {"name": "f", "arguments": "{}"}},
                    {"id": "c2", "type": "function", "function": {"name": "g", "arguments": "{}"}}
                ]}
            ]
        }));
        assert_eq!(p.blocks.len(), 4, "user + assistant text + 2 tool_calls");
        assert_eq!(p.breakpoints.len(), 1);
        assert_eq!(p.breakpoints[0].block_index, 3, "the LAST tool_call");
        assert_eq!(
            p.breakpoints[0].prefix,
            PrefixSpec {
                tools_kept: 0,
                full_messages: 2,
                partial: None
            },
            "the prefix is both messages whole"
        );
    }

    #[test]
    fn automatic_marker_lands_on_trailing_tool_result() {
        let p = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "user", "content": "q"},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"id": "c1", "type": "function", "function": {"name": "f", "arguments": "{}"}}
                ]},
                {"role": "tool", "tool_call_id": "c1", "content": "tool output"}
            ]
        }));
        assert_eq!(p.blocks.len(), 3, "user + tool_call + tool result");
        assert_eq!(p.blocks[2].role, "tool");
        assert_eq!(p.breakpoints[0].block_index, 2);
        assert_eq!(p.breakpoints[0].prefix.full_messages, 3);
    }

    #[test]
    fn automatic_marker_on_trailing_image_block() {
        let p = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "what is this?"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}}
                ]}
            ]
        }));
        assert_eq!(p.blocks.len(), 2);
        assert_eq!(p.blocks[1].text, "", "image blocks carry no write-side text");
        assert_eq!(p.breakpoints[0].block_index, 1, "marker on the image block");
        let other = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "what is this?"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,BBBB"}}
                ]}
            ]
        }));
        assert_ne!(p.cumulative_hashes[1], other.cumulative_hashes[1]);
    }

    #[test]
    fn stored_image_tokens_hash_by_content_not_by_upload_id() {
        use crate::image_normalizer::ImageToken;
        let body = |token: ImageToken| {
            parse(serde_json::json!({
                "cache_control": {"type": "ephemeral"},
                "messages": [
                    {"role": "user", "content": [
                        {"type": "text", "text": "what is this?"},
                        {"type": "image_url", "image_url": {"url": token.to_dw_img_uri()}}
                    ]}
                ]
            }))
        };
        let a = body(ImageToken::new_unique([7; 32]));
        let b = body(ImageToken::new_unique([7; 32]));
        let legacy = body(ImageToken([7; 32], None));
        let different = body(ImageToken::new_unique([8; 32]));
        assert_eq!(a.cumulative_hashes[1], b.cumulative_hashes[1]);
        assert_eq!(a.cumulative_hashes[1], legacy.cumulative_hashes[1]);
        let upper = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "what is this?"},
                    {"type": "image_url", "image_url": {"url": format!("dw-img://{}", hex::encode_upper([7u8; 32]))}}
                ]}
            ]
        }));
        assert_eq!(a.cumulative_hashes[1], upper.cumulative_hashes[1]);
        assert_ne!(a.cumulative_hashes[1], different.cumulative_hashes[1]);
    }

    #[test]
    fn token_shaped_text_outside_image_urls_hashes_as_written() {
        use crate::image_normalizer::ImageToken;
        let text_body = |text: String| {
            parse(serde_json::json!({
                "cache_control": {"type": "ephemeral"},
                "messages": [{"role": "user", "content": [{"type": "text", "text": text}]}]
            }))
        };
        let with_upload_id = ImageToken::new_unique([7; 32]);
        let a = text_body(format!("see {}", with_upload_id.to_dw_img_uri()));
        let b = text_body(format!("see {}", ImageToken(with_upload_id.0, None).to_dw_img_uri()));
        assert_ne!(a.cumulative_hashes[0], b.cumulative_hashes[0]);
    }

    #[test]
    fn automatic_with_trailing_blockless_message_marks_last_real_block() {
        let p = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "user", "content": "q"},
                {"role": "assistant", "content": null}
            ]
        }));
        assert_eq!(p.blocks.len(), 1, "the null-content assistant adds no block");
        assert_eq!(p.breakpoints[0].block_index, 0, "marker on the last REAL block");
        assert_eq!(p.breakpoints[0].prefix.full_messages, 2, "prefix spec counts the phantom message");

        let p = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "user", "content": "q"},
                {"role": "assistant", "content": []}
            ]
        }));
        assert_eq!(p.blocks.len(), 1);
        assert_eq!(p.breakpoints[0].block_index, 0);

        let p = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "user", "content": "q"},
                {"role": "assistant", "content": ""}
            ]
        }));
        assert_eq!(p.blocks.len(), 2, "empty-string prefill is a block");
        assert_eq!(p.breakpoints[0].block_index, 1);
    }

    #[test]
    fn automatic_with_telemetry_last_block_marks_prior_block() {
        let p = parse_with(
            serde_json::json!({
                "cache_control": {"type": "ephemeral"},
                "messages": [
                    {"role": "user", "content": "q"},
                    {"role": "system", "content": [
                        {"type": "text", "text": "x-anthropic-billing-header: cch=zzz"}
                    ]}
                ]
            }),
            &telemetry(),
        );
        assert_eq!(p.blocks.len(), 1, "telemetry block excluded from the cache view");
        assert_eq!(p.breakpoints[0].block_index, 0);
    }

    #[test]
    fn automatic_tool_loop_growth_reads_prior_turn() {
        let turn1 = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "q"}
            ]
        }));
        let turn2 = parse(serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "q"},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"id": "c1", "type": "function", "function": {"name": "f", "arguments": "{}"}}
                ]},
                {"role": "tool", "tool_call_id": "c1", "content": "result"},
                {"role": "user", "content": "next"}
            ]
        }));
        assert_eq!(turn1.breakpoints[0].block_index, 1);
        assert_eq!(
            turn1.cumulative_hashes[1], turn2.cumulative_hashes[1],
            "turn 2 reads turn 1's write"
        );
        assert_eq!(turn2.blocks.len(), 5, "sys + user + tool_call + tool + user");
        assert_eq!(turn2.breakpoints[0].block_index, 4, "marker moved to the new frontier");
        let candidates = turn2.read_candidates(&turn2.breakpoints[0]);
        assert!(
            candidates.contains(&turn1.cumulative_hashes[1]),
            "prior write reachable by walk-back"
        );
    }

    #[test]
    fn canonical_block_bytes_sorts_object_keys() {
        let block: serde_json::Value = serde_json::from_str(r#"{"type":"text","text":"hi","meta":{"z":1,"a":2}}"#).unwrap();
        let mut expected = b"user\0".to_vec();
        expected.extend_from_slice(br#"{"meta":{"a":2,"z":1},"text":"hi","type":"text"}"#);
        assert_eq!(canonical_block_bytes("user", block), expected);
    }

    #[test]
    fn key_order_does_not_change_hashes() {
        let a = r#"{"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object","properties":{"b":{"type":"string"},"a":{"type":"string"}}}}}],
            "messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral"}}]}]}"#;
        let b = r#"{"messages":[{"content":[{"cache_control":{"type":"ephemeral"},"text":"hi","type":"text"}],"role":"user"}],
            "tools":[{"function":{"parameters":{"properties":{"a":{"type":"string"},"b":{"type":"string"}},"type":"object"},"name":"f"},"type":"function"}]}"#;
        let policy = TierPolicy::from_config(&["5m".to_string()], "5m");
        let pa = parse_chat_completions(a.as_bytes(), &policy, &TelemetryPolicy::default()).unwrap();
        let pb = parse_chat_completions(b.as_bytes(), &policy, &TelemetryPolicy::default()).unwrap();
        assert_eq!(pa.cumulative_hashes.len(), 2);
        assert_eq!(pa.cumulative_hashes, pb.cumulative_hashes);
    }
}
