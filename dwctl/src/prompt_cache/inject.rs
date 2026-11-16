
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use serde_json::Value;
use std::sync::{Arc, OnceLock};
use tracing::error;

use super::parse::{TELEMETRY_ROLE, TelemetryPolicy};
use super::stats::{BilledCache, CacheBilling, CacheReadSource, CacheStats};

#[derive(Clone, Debug, Default)]
pub struct UpstreamCachedTokens(Arc<OnceLock<u64>>);

impl UpstreamCachedTokens {
    pub fn get(&self) -> Option<u64> {
        self.0.get().copied()
    }

    pub(crate) fn set(&self, value: u64) {
        let _ = self.0.set(value);
    }
}

fn upstream_cached_tokens(usage: &serde_json::Map<String, Value>) -> Option<u64> {
    usage
        .get("prompt_tokens_details")
        .and_then(Value::as_object)
        .and_then(|d| d.get("cached_tokens"))
        .and_then(Value::as_u64)
}

pub(crate) fn read_upstream_cached_tokens_json(body: &[u8]) -> Option<u64> {
    let json: Value = serde_json::from_slice(body).ok()?;
    upstream_cached_tokens(json.get("usage")?.as_object()?)
}

fn remove_cache_control(body: &mut Value, telemetry: &TelemetryPolicy) -> (bool, bool) {
    let mut rewrote = false;
    let mut had_marker = false;
    let Some(obj) = body.as_object_mut() else {
        return (false, false);
    };

    if let Some(removed) = obj.shift_remove("cache_control") {
        rewrote = true;
        had_marker |= !removed.is_null();
    }

    if let Some(messages) = obj.get_mut("messages").and_then(Value::as_array_mut) {
        for msg in messages.iter_mut() {
            let strip_system_telemetry = telemetry.strip_from_prompt && msg.get("role").and_then(Value::as_str) == Some(TELEMETRY_ROLE);
            if let Some(content) = msg.get_mut("content").and_then(Value::as_array_mut) {
                if strip_system_telemetry {
                    let before = content.len();
                    content.retain(|block| !telemetry.excludes_block(TELEMETRY_ROLE, block));
                    rewrote |= content.len() != before;
                }
                for block in content.iter_mut() {
                    strip_block_marker(block, &mut rewrote, &mut had_marker);
                }
            }
            if let Some(tool_calls) = msg.get_mut("tool_calls").and_then(Value::as_array_mut) {
                for call in tool_calls.iter_mut() {
                    strip_block_marker(call, &mut rewrote, &mut had_marker);
                }
            }
        }
    }

    if let Some(tools) = obj.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools.iter_mut() {
            strip_block_marker(tool, &mut rewrote, &mut had_marker);
        }
    }

    (rewrote, had_marker)
}

fn strip_block_marker(value: &mut Value, rewrote: &mut bool, had_marker: &mut bool) {
    if let Some(obj) = value.as_object_mut()
        && let Some(removed) = obj.shift_remove("cache_control")
    {
        *rewrote = true;
        *had_marker |= !removed.is_null();
    }
}

pub fn strip_cache_control(body: &[u8], telemetry: &TelemetryPolicy) -> (Option<Bytes>, bool) {
    let Ok(mut json) = serde_json::from_slice::<Value>(body) else {
        return (None, false);
    };
    let (rewrote, had_markers) = remove_cache_control(&mut json, telemetry);

    let mut usage_set = false;
    if let Some(obj) = json.as_object_mut() {
        let is_streaming = obj.get("stream").and_then(Value::as_bool) == Some(true);
        if is_streaming {
            let opts = obj.entry("stream_options").or_insert_with(|| serde_json::json!({}));
            if !opts.is_object() {
                *opts = serde_json::json!({});
            }
            if let Some(opts_obj) = opts.as_object_mut() {
                let already = opts_obj.get("include_usage").and_then(Value::as_bool) == Some(true);
                if !already {
                    opts_obj.insert("include_usage".to_string(), serde_json::json!(true));
                    usage_set = true;
                }
            }
        }
    }

    let body = if rewrote || usage_set {
        serde_json::to_vec(&json).ok().map(Bytes::from)
    } else {
        None
    };
    (body, had_markers)
}

const PROVIDER_CACHE_FIELDS: [&str; 6] = [
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "cache_creation",
    "prompt_cache_hit_tokens",
    "prompt_cache_miss_tokens",
    "cache_discount",
];

pub(crate) fn scrub_provider_cache_fields(usage: &mut serde_json::Map<String, Value>) -> bool {
    let mut changed = false;
    for key in PROVIDER_CACHE_FIELDS {
        changed |= usage.shift_remove(key).is_some();
    }
    if let Some(cached) = usage
        .get_mut("prompt_tokens_details")
        .and_then(Value::as_object_mut)
        .and_then(|d| d.get_mut("cached_tokens"))
        && cached.as_u64() != Some(0)
    {
        *cached = serde_json::json!(0);
        changed = true;
    }
    changed
}

pub fn scrub_usage_json(body: &[u8]) -> Option<Bytes> {
    let mut json: Value = serde_json::from_slice(body).ok()?;
    let usage = json.as_object_mut()?.get_mut("usage")?.as_object_mut()?;
    if !scrub_provider_cache_fields(usage) {
        return None;
    }
    serde_json::to_vec(&json).ok().map(Bytes::from)
}

fn splice_cache_fields(
    usage: &mut serde_json::Map<String, Value>,
    stats: &CacheStats,
    model: Option<&str>,
    allow_implicit: bool,
) -> BilledCache {
    let engine_reported = upstream_cached_tokens(usage);
    scrub_provider_cache_fields(usage);

    if let (Some(render_total), Some(prompt)) = (stats.render_total, usage.get("prompt_tokens").and_then(Value::as_u64)) {
        let drift = render_total as i64 - prompt as i64;
        let model_label = model.unwrap_or("unknown").to_string();
        metrics::histogram!("dwctl_cache_render_drift_tokens", "model" => model_label.clone()).record(drift as f64);
        if prompt > 0 && drift.unsigned_abs() > 20 && drift.unsigned_abs() * 100 > prompt {
            metrics::counter!("dwctl_cache_render_drift_exceeded_total", "model" => model_label).increment(1);
        }
    }
    let creations = stats.creation_total();
    let mut read = stats.read;
    let (mut c5, mut c1, mut c24) = (stats.creation_5m, stats.creation_1h, stats.creation_24h);
    if let Some(prompt) = usage.get("prompt_tokens").and_then(Value::as_u64) {
        if creations > prompt {
            (read, c5, c1, c24) = (0, 0, 0, 0);
        } else if read + creations > prompt {
            read = prompt - creations;
        }
    }

    let mut read_source = (read > 0).then_some(CacheReadSource::Module);
    if allow_implicit && let (Some(engine), Some(prompt)) = (engine_reported, usage.get("prompt_tokens").and_then(Value::as_u64)) {
        let engine_capped = engine.min(prompt);
        if engine_capped > 0 {
            read = engine_capped;
            read_source = Some(CacheReadSource::Engine);
        }
    }

    let details = usage.entry("prompt_tokens_details").or_insert_with(|| serde_json::json!({}));
    if let Some(details_obj) = details.as_object_mut() {
        details_obj.insert("cached_tokens".to_string(), serde_json::json!(read));
    }
    usage.insert("cache_read_input_tokens".to_string(), serde_json::json!(read));
    usage.insert("cache_creation_input_tokens".to_string(), serde_json::json!(c5 + c1 + c24));
    usage.insert(
        "cache_creation".to_string(),
        serde_json::json!({
            "ephemeral_5m_input_tokens": c5,
            "ephemeral_1h_input_tokens": c1,
            "ephemeral_24h_input_tokens": c24,
        }),
    );
    BilledCache {
        stats: CacheStats {
            read,
            creation_5m: c5,
            creation_1h: c1,
            creation_24h: c24,
            ..*stats
        },
        read_source,
    }
}

pub fn inject_into_usage_json(body: &[u8], stats: &CacheStats, allow_implicit: bool) -> Option<(Bytes, BilledCache)> {
    let mut json: Value = serde_json::from_slice(body).ok()?;
    let obj = json.as_object_mut()?;
    let model = obj.get("model").and_then(Value::as_str).map(String::from);
    let usage = obj.get_mut("usage")?.as_object_mut()?;
    let billed = splice_cache_fields(usage, stats, model.as_deref(), allow_implicit);
    serde_json::to_vec(&json).ok().map(|body| (Bytes::from(body), billed))
}

pub(crate) struct SseScan {
    pub rewritten: Option<Bytes>,
    pub saw_error: bool,
    pub saw_usage: bool,
    pub upstream_cached_tokens: Option<u64>,
    pub billing_stats: Option<BilledCache>,
}

#[derive(Clone, Copy)]
pub(crate) enum UsageEdit<'a> {
    Probe,
    Inject { stats: &'a CacheStats, allow_implicit: bool },
    Scrub,
}

pub(crate) fn scan_edit_sse(body: &[u8], edit: UsageEdit) -> SseScan {
    let Ok(body_str) = std::str::from_utf8(body) else {
        return SseScan {
            rewritten: None,
            saw_error: false,
            saw_usage: false,
            upstream_cached_tokens: None,
            billing_stats: None,
        };
    };

    if matches!(edit, UsageEdit::Probe) {
        let mut saw_error = false;
        let mut saw_usage = false;
        let mut upstream = None;
        for line in body_str.split('\n') {
            if let Some(chunk) = sse_data_json(line) {
                saw_error |= chunk.get("error").is_some();
                if let Some(usage) = chunk.get("usage").and_then(Value::as_object) {
                    saw_usage = true;
                    upstream = upstream.or_else(|| upstream_cached_tokens(usage));
                }
            }
        }
        return SseScan {
            rewritten: None,
            saw_error,
            saw_usage,
            upstream_cached_tokens: upstream,
            billing_stats: None,
        };
    }

    let mut out = String::with_capacity(body_str.len() + 256);
    let mut edited = false;
    let mut saw_error = false;
    let mut saw_usage = false;
    let mut upstream = None;
    let mut billing_stats = None;

    let mut first = true;
    for line in body_str.split('\n') {
        if !first {
            out.push('\n');
        }
        first = false;

        if let Some(mut chunk) = sse_data_json(line) {
            let chunk_obj = chunk.as_object_mut().expect("sse_data_json returns only objects");
            if chunk_obj.contains_key("error") {
                saw_error = true;
            }
            let model = chunk_obj.get("model").and_then(Value::as_str).map(String::from);
            if let Some(usage) = chunk_obj.get_mut("usage")
                && let Some(usage_obj) = usage.as_object_mut()
            {
                saw_usage = true;
                upstream = upstream.or_else(|| upstream_cached_tokens(usage_obj));
                if !edited {
                    let has_cr = line.ends_with('\r');
                    let mut billed = None;
                    let changed = match edit {
                        UsageEdit::Probe => false,
                        UsageEdit::Inject { stats, allow_implicit } => {
                            billed = Some(splice_cache_fields(usage_obj, stats, model.as_deref(), allow_implicit));
                            true
                        }
                        UsageEdit::Scrub => scrub_provider_cache_fields(usage_obj),
                    };
                    if changed && let Ok(reserialized) = serde_json::to_string(&chunk) {
                        billing_stats = billed;
                        out.push_str("data: ");
                        out.push_str(&reserialized);
                        if has_cr {
                            out.push('\r');
                        }
                        edited = true;
                        continue;
                    }
                }
            }
        }
        out.push_str(line);
    }

    SseScan {
        rewritten: if edited { Some(Bytes::from(out)) } else { None },
        saw_error,
        saw_usage,
        upstream_cached_tokens: upstream,
        billing_stats,
    }
}

fn sse_data_json(line: &str) -> Option<Value> {
    let data = line.strip_prefix("data:")?;
    let trimmed = data.strip_prefix(' ').unwrap_or(data).trim();
    if trimmed == "[DONE]" {
        return None;
    }
    serde_json::from_str::<Value>(trimmed).ok().filter(Value::is_object)
}

pub fn inject_into_sse_body(body: &[u8], stats: &CacheStats, allow_implicit: bool) -> Option<Bytes> {
    scan_edit_sse(body, UsageEdit::Inject { stats, allow_implicit }).rewritten
}

pub fn scrub_sse_body(body: &[u8]) -> Option<Bytes> {
    scan_edit_sse(body, UsageEdit::Scrub).rewritten
}

pub async fn inject_into_response_nonstreaming(
    response: Response,
    stats: &CacheStats,
    capture: &UpstreamCachedTokens,
    allow_implicit: bool,
) -> (Response, bool) {
    let status_ok = response.status().is_success();

    let is_json = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(';')
                .next()
                .map(str::trim)
                .is_some_and(|ct| ct.eq_ignore_ascii_case("application/json"))
        })
        .unwrap_or(true);
    if !is_json {
        return (response, false);
    }
    let (mut parts, body) = response.into_parts();
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to buffer response body for cache injection: {}", e);
            let err_body = serde_json::json!({
                "error": {
                    "message": format!("failed to read upstream response body: {e}"),
                    "type": "internal_error",
                    "code": "response_body_read_failed",
                }
            });
            return ((StatusCode::INTERNAL_SERVER_ERROR, axum::Json(err_body)).into_response(), false);
        }
    };

    if let Some(v) = read_upstream_cached_tokens_json(&body_bytes) {
        capture.set(v);
    }
    match inject_into_usage_json(&body_bytes, stats, allow_implicit) {
        Some((rewritten, billed)) => {
            if let Some(capture) = parts.extensions.get::<CacheBilling>() {
                capture.set(billed);
            }
            let len = rewritten.len();
            parts.headers.remove(axum::http::header::TRANSFER_ENCODING);
            parts.headers.remove(axum::http::header::CONTENT_ENCODING);
            parts
                .headers
                .insert(axum::http::header::CONTENT_LENGTH, axum::http::HeaderValue::from(len as u64));
            (Response::from_parts(parts, axum::body::Body::from(rewritten)), status_ok)
        }
        None => {
            let len = body_bytes.len();
            parts.headers.remove(axum::http::header::TRANSFER_ENCODING);
            parts
                .headers
                .insert(axum::http::header::CONTENT_LENGTH, axum::http::HeaderValue::from(len as u64));
            (Response::from_parts(parts, axum::body::Body::from(body_bytes)), false)
        }
    }
}

pub async fn scrub_response_nonstreaming(response: Response, capture: &UpstreamCachedTokens) -> Response {
    if !response.status().is_success() {
        return response;
    }
    let is_json = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(';')
                .next()
                .map(str::trim)
                .is_some_and(|ct| ct.eq_ignore_ascii_case("application/json"))
        })
        .unwrap_or(true);
    if !is_json {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to buffer response body for cache scrub: {}", e);
            let err_body = serde_json::json!({
                "error": {
                    "message": format!("failed to read upstream response body: {e}"),
                    "type": "internal_error",
                    "code": "response_body_read_failed",
                }
            });
            return (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(err_body)).into_response();
        }
    };

    if let Some(v) = read_upstream_cached_tokens_json(&body_bytes) {
        capture.set(v);
    }
    let rewritten = scrub_usage_json(&body_bytes);
    if rewritten.is_some() {
        parts.headers.remove(axum::http::header::CONTENT_ENCODING);
    }
    let body = rewritten.unwrap_or(body_bytes);
    parts.headers.remove(axum::http::header::TRANSFER_ENCODING);
    parts
        .headers
        .insert(axum::http::header::CONTENT_LENGTH, axum::http::HeaderValue::from(body.len() as u64));
    Response::from_parts(parts, axum::body::Body::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_cached_tokens_are_read_before_the_rewrite() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n\
                   data: {\"choices\":[],\"usage\":{\"prompt_tokens\":985,\"prompt_tokens_details\":{\"cached_tokens\":687}}}\n\n\
                   data: [DONE]\n\n";
        let probe = scan_edit_sse(sse.as_bytes(), UsageEdit::Probe);
        assert!(probe.saw_usage);
        assert_eq!(probe.upstream_cached_tokens, Some(687));
        let scrub = scan_edit_sse(sse.as_bytes(), UsageEdit::Scrub);
        assert_eq!(scrub.upstream_cached_tokens, Some(687), "read before zeroing");
        let out = String::from_utf8(scrub.rewritten.expect("rewritten").to_vec()).unwrap();
        assert!(out.contains("\"cached_tokens\":0"), "customer-visible value still zeroed: {out}");
        let no_usage = scan_edit_sse(b"data: {\"choices\":[]}\n\n", UsageEdit::Probe);
        assert_eq!(no_usage.upstream_cached_tokens, None);

        let body = br#"{"usage":{"prompt_tokens":10,"prompt_tokens_details":{"cached_tokens":4}}}"#;
        assert_eq!(read_upstream_cached_tokens_json(body), Some(4));
        assert_eq!(read_upstream_cached_tokens_json(br#"{"usage":{"prompt_tokens":10}}"#), None);
        assert_eq!(read_upstream_cached_tokens_json(b"nope"), None);
    }

    #[test]
    fn upstream_cached_tokens_cell_is_first_write_wins() {
        let cell = UpstreamCachedTokens::default();
        assert_eq!(cell.get(), None);
        cell.set(5);
        cell.set(9);
        assert_eq!(cell.get(), Some(5));
        assert_eq!(cell.clone().get(), Some(5), "clones share the cell");
    }

    fn module_billed(stats: CacheStats) -> BilledCache {
        BilledCache {
            read_source: (stats.read > 0).then_some(CacheReadSource::Module),
            stats,
        }
    }

    fn stats() -> CacheStats {
        CacheStats {
            read: 1024,
            render_total: None,
            creation_5m: 10,
            creation_1h: 20,
            creation_24h: 30,
        }
    }

    #[test]
    fn strip_removes_nested_cache_control_and_sets_include_usage() {
        let body = serde_json::json!({
            "stream": true,
            "messages": [{"role":"system","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral"}}]}]
        })
        .to_string();
        let (out, had_markers) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
        assert!(had_markers, "body had cache_control");
        let out = out.expect("changed");
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert!(!out.windows(13).any(|w| w == b"cache_control"));
        assert_eq!(v["stream_options"]["include_usage"], true);
    }

    #[test]
    fn strip_keeps_key_order() {
        let body = r#"{"tools":[{"type":"function","cache_control":{"type":"ephemeral"},"function":{"name":"f","parameters":{"type":"object","properties":{"z":{"type":"string"},"a":{"type":"string"}}}}}],"stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
        let (out, had_markers) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
        assert!(had_markers);
        assert_eq!(
            String::from_utf8(out.expect("changed").to_vec()).unwrap(),
            r#"{"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object","properties":{"z":{"type":"string"},"a":{"type":"string"}}}}}],"stream":true,"messages":[{"role":"user","content":"hi"}],"stream_options":{"include_usage":true}}"#
        );
    }

    #[test]
    fn strip_none_when_nothing_to_do() {
        let body = serde_json::json!({"messages":[{"role":"user","content":"hi"}]}).to_string();
        let (out, had_markers) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
        assert!(out.is_none());
        assert!(!had_markers);
    }

    #[test]
    fn strip_stream_without_markers_changes_body_but_not_marked() {
        let body = serde_json::json!({"stream": true, "messages":[{"role":"user","content":"hi"}]}).to_string();
        let (out, had_markers) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
        assert!(out.is_some(), "include_usage injected");
        assert!(!had_markers, "no markers present");
    }

    #[test]
    fn strip_stream_replaces_non_object_stream_options() {
        for stream_options in [Value::Null, serde_json::json!("x"), serde_json::json!([])] {
            let body = serde_json::json!({"stream": true, "stream_options": stream_options, "messages": []}).to_string();
            let (out, _) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
            let v: Value = serde_json::from_slice(&out.expect("include_usage injected")).unwrap();
            assert_eq!(v["stream_options"], serde_json::json!({"include_usage": true}));
        }
    }

    #[test]
    fn strip_null_cache_control_is_removed_but_not_marked() {
        let body = serde_json::json!({
            "messages": [{"role":"system","content":[{"type":"text","text":"x","cache_control":null}]}]
        })
        .to_string();
        let (out, had_markers) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
        assert!(!had_markers, "null cache_control is not a marker");
        let out = out.expect("body rewritten to drop the null cache_control key");
        assert!(!out.windows(13).any(|w| w == b"cache_control"), "cache_control key removed");
    }

    #[test]
    fn strip_removes_tool_marker_but_preserves_schema_field_named_cache_control() {
        let body = serde_json::json!({
            "tools": [{
                "type": "function",
                "function": {
                    "name": "set_config",
                    "parameters": {"type": "object", "properties": {
                        "cache_control": {"type": "string", "description": "a real argument"}
                    }}
                },
                "cache_control": {"type": "ephemeral", "ttl": "1h"}
            }],
            "messages": [{"role": "user", "content": "hi"}]
        })
        .to_string();
        let (out, had_markers) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
        assert!(had_markers, "the top-level tool marker is a marker");
        let v: Value = serde_json::from_slice(&out.expect("body rewritten")).unwrap();
        assert!(v["tools"][0].get("cache_control").is_none(), "tool marker stripped");
        assert!(
            v["tools"][0]["function"]["parameters"]["properties"]["cache_control"].is_object(),
            "legitimate schema field preserved"
        );
    }

    #[test]
    fn strip_schema_field_named_cache_control_alone_is_not_a_marker() {
        let body = serde_json::json!({
            "tools": [{"type": "function", "function": {"name": "f", "parameters": {
                "type": "object", "properties": {"cache_control": {"type": "string"}}}}}],
            "messages": [{"role": "user", "content": "hi"}]
        })
        .to_string();
        let (out, had_markers) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
        assert!(!had_markers, "a schema field is not a marker");
        assert!(out.is_none(), "nothing stripped, body unchanged");
    }

    #[test]
    fn strip_removes_top_level_automatic_marker() {
        let body = serde_json::json!({
            "cache_control": {"type": "ephemeral"},
            "messages": [{"role": "user", "content": "hi"}]
        })
        .to_string();
        let (out, had_markers) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
        assert!(had_markers, "top-level marker is adoption");
        let v: Value = serde_json::from_slice(&out.expect("body rewritten")).unwrap();
        assert!(v.get("cache_control").is_none(), "top-level marker stripped");
    }

    #[test]
    fn strip_top_level_null_marker_removed_not_adoption() {
        let body = serde_json::json!({
            "cache_control": null,
            "messages": [{"role": "user", "content": "hi"}]
        })
        .to_string();
        let (out, had_markers) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
        assert!(!had_markers, "null top-level cache_control is not a marker");
        let v: Value = serde_json::from_slice(&out.expect("body rewritten to drop the null field")).unwrap();
        assert!(v.get("cache_control").is_none(), "top-level field removed");
    }

    #[test]
    fn strip_removes_tool_call_marker_but_keeps_arguments() {
        let body = serde_json::json!({
            "messages": [{"role": "assistant", "content": null, "tool_calls": [
                {"id": "tu1", "type": "function",
                 "function": {"name": "f", "arguments": "{\"x\":1}"},
                 "cache_control": {"type": "ephemeral"}}
            ]}]
        })
        .to_string();
        let (out, had) = strip_cache_control(body.as_bytes(), &TelemetryPolicy::default());
        assert!(had, "a tool_call marker is a marker");
        let v: Value = serde_json::from_slice(&out.expect("rewritten")).unwrap();
        let call = &v["messages"][0]["tool_calls"][0];
        assert!(call.get("cache_control").is_none(), "marker stripped");
        assert_eq!(call["function"]["name"], "f", "call preserved");
        assert_eq!(call["function"]["arguments"], "{\"x\":1}", "arguments untouched");
    }

    #[test]
    fn strip_mode_removes_telemetry_block_from_forwarded_body() {
        let body = serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x-anthropic-billing-header: cch=abc;"},
                {"type": "text", "text": "real system", "cache_control": {"type": "ephemeral"}}
            ]}]
        })
        .to_string();
        let tele = TelemetryPolicy::from_config(true, &["x-anthropic-billing-header:".to_string()]);
        let (out, _) = strip_cache_control(body.as_bytes(), &tele);
        let v: Value = serde_json::from_slice(&out.expect("body rewritten")).unwrap();
        let content = v["messages"][0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 1, "telemetry block removed from the forwarded prompt");
        assert_eq!(content[0]["text"], "real system");
        assert!(content[0].get("cache_control").is_none(), "marker stripped from survivor");
    }

    #[test]
    fn strip_mode_leaves_non_system_blocks_untouched() {
        let body = serde_json::json!({
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "x-anthropic-billing-header: cch=abc;"},
                {"type": "text", "text": "actual question"}
            ]}]
        })
        .to_string();
        let tele = TelemetryPolicy::from_config(true, &["x-anthropic-billing-header:".to_string()]);
        let (out, _) = strip_cache_control(body.as_bytes(), &tele);
        assert!(out.is_none(), "non-system block is not stripped from the forwarded prompt");
    }

    #[test]
    fn ignore_mode_keeps_telemetry_block_in_forwarded_body() {
        let body = serde_json::json!({
            "messages": [{"role": "system", "content": [
                {"type": "text", "text": "x-anthropic-billing-header: cch=abc;"},
                {"type": "text", "text": "real system"}
            ]}]
        })
        .to_string();
        let tele = TelemetryPolicy::from_config(false, &["x-anthropic-billing-header:".to_string()]);
        let (out, _) = strip_cache_control(body.as_bytes(), &tele);
        assert!(out.is_none(), "ignore mode leaves the forwarded body untouched");
    }

    #[test]
    fn inject_non_streaming_adds_cache_fields() {
        let body = serde_json::json!({"usage":{"prompt_tokens":2000,"completion_tokens":5}}).to_string();
        let (out, billed) = inject_into_usage_json(body.as_bytes(), &stats(), false).unwrap();
        assert_eq!(billed, module_billed(stats()));
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["usage"]["prompt_tokens"], 2000, "total preserved");
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 1024);
        assert_eq!(v["usage"]["cache_read_input_tokens"], 1024);
        assert_eq!(v["usage"]["cache_creation_input_tokens"], 60);
        assert_eq!(v["usage"]["cache_creation"]["ephemeral_1h_input_tokens"], 20);
    }

    #[test]
    fn drifted_split_is_capped_to_prompt_in_reported_usage() {
        let body = serde_json::json!({"usage":{"prompt_tokens":1000,"completion_tokens":5}}).to_string();
        let (out, billed) = inject_into_usage_json(body.as_bytes(), &stats(), false).unwrap();
        assert_eq!(billed, module_billed(CacheStats { read: 940, ..stats() }));
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["usage"]["prompt_tokens"], 1000, "engine total preserved");
        assert_eq!(v["usage"]["cache_read_input_tokens"], 940, "read capped to prompt - creations");
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 940);
        assert_eq!(v["usage"]["cache_creation_input_tokens"], 60, "creations untouched by drift cap");
    }

    #[test]
    fn corrupt_writes_report_no_cache_activity() {
        let body = serde_json::json!({"usage":{"prompt_tokens":50,"completion_tokens":5}}).to_string();
        let (out, billed) = inject_into_usage_json(body.as_bytes(), &stats(), false).unwrap();
        assert_eq!(billed, BilledCache::default());
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["usage"]["cache_read_input_tokens"], 0);
        assert_eq!(v["usage"]["cache_creation_input_tokens"], 0);
        assert_eq!(v["usage"]["cache_creation"]["ephemeral_1h_input_tokens"], 0);
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 0);
    }

    #[test]
    fn inject_non_streaming_none_when_no_usage() {
        let body = serde_json::json!({"choices":[]}).to_string();
        assert!(inject_into_usage_json(body.as_bytes(), &stats(), false).is_none());
    }

    #[test]
    fn streaming_billing_metadata_matches_emitted_capped_usage() {
        for (prompt, expected) in [
            (2000, module_billed(stats())),
            (1000, module_billed(CacheStats { read: 940, ..stats() })),
            (50, BilledCache::default()),
        ] {
            let sse = format!("data: {{\"usage\":{{\"prompt_tokens\":{prompt}}}}}\n\ndata: [DONE]\n\n");
            let scan = scan_edit_sse(
                sse.as_bytes(),
                UsageEdit::Inject {
                    stats: &stats(),
                    allow_implicit: false,
                },
            );
            assert_eq!(scan.billing_stats, Some(expected));
            let body = String::from_utf8(scan.rewritten.unwrap().to_vec()).unwrap();
            let usage = sse_data_json(body.lines().next().unwrap()).unwrap()["usage"].clone();
            assert_eq!(usage["cache_read_input_tokens"], expected.stats.read);
            assert_eq!(usage["cache_creation_input_tokens"], expected.stats.creation_total());
            assert!(scan_edit_sse(sse.as_bytes(), UsageEdit::Scrub).billing_stats.is_none());
        }
        assert!(
            scan_edit_sse(
                b"data: {\"choices\":[]}\n\n",
                UsageEdit::Inject {
                    stats: &stats(),
                    allow_implicit: false
                }
            )
            .billing_stats
            .is_none()
        );
    }

    #[test]
    fn engine_passthrough_bills_the_upstream_hit_on_unmarked_requests() {
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 1000,
            "prompt_tokens_details": {"cached_tokens": 687}
        }})
        .to_string();
        let (out, billed) = inject_into_usage_json(body.as_bytes(), &CacheStats::default(), true).unwrap();
        assert_eq!(billed.stats.read, 687);
        assert_eq!(billed.stats.creation_total(), 0);
        assert_eq!(billed.read_source, Some(CacheReadSource::Engine));
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 687);
        assert_eq!(v["usage"]["cache_read_input_tokens"], 687);
        assert_eq!(v["usage"]["cache_creation_input_tokens"], 0);
    }

    #[test]
    fn armed_requests_stay_on_module_stats_never_mixed_with_engine() {
        let module = CacheStats {
            read: 200,
            ..Default::default()
        };
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 1000,
            "prompt_tokens_details": {"cached_tokens": 800}
        }})
        .to_string();
        let (out, billed) = inject_into_usage_json(body.as_bytes(), &module, false).unwrap();
        assert_eq!((billed.stats.read, billed.read_source), (200, Some(CacheReadSource::Module)));
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["usage"]["cache_read_input_tokens"], 200, "customer sees the module read");

        let module = CacheStats {
            read: 500,
            ..Default::default()
        };
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 1000,
            "prompt_tokens_details": {"cached_tokens": 200}
        }})
        .to_string();
        let (_, billed) = inject_into_usage_json(body.as_bytes(), &module, false).unwrap();
        assert_eq!((billed.stats.read, billed.read_source), (500, Some(CacheReadSource::Module)));
    }

    #[test]
    fn write_legs_are_explicit_and_never_mixed_with_engine_reads() {
        let module = CacheStats {
            creation_1h: 300,
            ..Default::default()
        };
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 1000,
            "prompt_tokens_details": {"cached_tokens": 900}
        }})
        .to_string();
        let (out, billed) = inject_into_usage_json(body.as_bytes(), &module, false).unwrap();
        assert_eq!(billed.stats.read, 0, "no engine read on an explicit write leg");
        assert_eq!(billed.stats.creation_1h, 300, "the write bills as normal");
        assert_eq!(billed.read_source, None);
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["usage"]["cache_read_input_tokens"], 0);
        assert_eq!(v["usage"]["cache_creation_input_tokens"], 300);
        assert_eq!(
            v["usage"]["prompt_tokens_details"]["cached_tokens"], 0,
            "engine hit not shown either"
        );
    }

    #[test]
    fn engine_passthrough_distrusts_nonsense_and_needs_a_prompt_count() {
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 100,
            "prompt_tokens_details": {"cached_tokens": 5000}
        }})
        .to_string();
        let (_, billed) = inject_into_usage_json(body.as_bytes(), &CacheStats::default(), true).unwrap();
        assert_eq!((billed.stats.read, billed.read_source), (100, Some(CacheReadSource::Engine)));

        let body = serde_json::json!({"usage":{
            "completion_tokens": 5,
            "prompt_tokens_details": {"cached_tokens": 687}
        }})
        .to_string();
        let (_, billed) = inject_into_usage_json(body.as_bytes(), &CacheStats::default(), true).unwrap();
        assert_eq!((billed.stats.read, billed.read_source), (0, None));
    }

    #[test]
    fn provider_dialect_cache_fields_are_scrubbed_and_never_read() {
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 1000,
            "prompt_cache_hit_tokens": 400
        }})
        .to_string();
        let (out, billed) = inject_into_usage_json(body.as_bytes(), &CacheStats::default(), true).unwrap();
        assert_eq!((billed.stats.read, billed.read_source), (0, None));
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert!(v["usage"].get("prompt_cache_hit_tokens").is_none(), "dialect field scrubbed");
        assert_eq!(v["usage"]["cache_read_input_tokens"], 0);
    }

    #[test]
    fn armed_requests_with_nothing_cacheable_stay_deterministically_zero() {
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 1000,
            "prompt_tokens_details": {"cached_tokens": 687}
        }})
        .to_string();
        let (out, billed) = inject_into_usage_json(body.as_bytes(), &CacheStats::default(), false).unwrap();
        assert_eq!((billed.stats.read, billed.read_source), (0, None));
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 0);
        assert_eq!(v["usage"]["cache_read_input_tokens"], 0);
    }

    #[test]
    fn corrupt_module_split_stays_on_explicit_list_price_semantics() {
        let module = CacheStats {
            read: 10,
            creation_1h: 200,
            ..Default::default()
        };
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 50,
            "prompt_tokens_details": {"cached_tokens": 40}
        }})
        .to_string();
        let (_, billed) = inject_into_usage_json(body.as_bytes(), &module, false).unwrap();
        assert_eq!(billed.stats.creation_total(), 0, "corrupt module writes zeroed");
        assert_eq!((billed.stats.read, billed.read_source), (0, None));
    }

    #[test]
    fn streaming_engine_passthrough_edits_the_terminal_frame() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":1000,\"prompt_tokens_details\":{\"cached_tokens\":687}}}\n\ndata: [DONE]\n\n";
        let scan = scan_edit_sse(
            sse.as_bytes(),
            UsageEdit::Inject {
                stats: &CacheStats::default(),
                allow_implicit: true,
            },
        );
        let billed = scan.billing_stats.unwrap();
        assert_eq!((billed.stats.read, billed.read_source), (687, Some(CacheReadSource::Engine)));
        let body = String::from_utf8(scan.rewritten.unwrap().to_vec()).unwrap();
        assert!(body.contains("\"cache_read_input_tokens\":687"), "got: {body}");
        assert!(body.contains("\"cached_tokens\":687"), "got: {body}");
        assert!(body.contains("data: [DONE]"));
    }

    #[test]
    fn inject_sse_preserves_crlf_on_edited_frame() {
        let sse = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2000}}\r\n\r\ndata: [DONE]\r\n\r\n";
        let out = inject_into_sse_body(sse.as_bytes(), &stats(), false).unwrap();
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"cache_read_input_tokens\":1024"), "got: {s}");
        assert!(s.contains("}\r\n\r\n"), "edited frame must keep CRLF framing, got: {s}");
        assert!(!s.contains("}\n\r"), "must not produce a malformed \\n\\r, got: {s}");
    }

    #[test]
    fn inject_sse_edits_only_terminal_usage_frame() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":2000}}\n\ndata: [DONE]\n\n";
        let out = inject_into_sse_body(sse.as_bytes(), &stats(), false).unwrap();
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"cached_tokens\":1024"));
        assert!(s.contains("\"cache_read_input_tokens\":1024"));
        assert_eq!(s.matches("cached_tokens").count(), 1);
        assert!(s.contains("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}"));
        assert!(s.contains("data: [DONE]"));
    }

    #[test]
    fn inject_sse_none_when_no_usage_frame() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";
        assert!(inject_into_sse_body(sse.as_bytes(), &stats(), false).is_none());
    }

    #[test]
    fn inject_sse_handles_data_prefix_without_space() {
        let sse = "data:{\"choices\":[],\"usage\":{\"prompt_tokens\":2000}}\n\ndata:[DONE]\n\n";
        let out = inject_into_sse_body(sse.as_bytes(), &stats(), false).expect("no-space data: frame is injected");
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"cache_read_input_tokens\":1024"), "got: {s}");
    }

    #[test]
    fn inject_into_sse_body_edits_the_usage_frame() {
        let body = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":2000}}\n\ndata: [DONE]\n\n";
        let out = inject_into_sse_body(body, &stats(), false).expect("usage frame present → edited");
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"cached_tokens\":1024"), "got: {s}");
        assert!(s.contains("data: [DONE]"), "DONE preserved");
        assert!(s.contains("\"content\":\"hi\""), "delta preserved");
    }

    #[test]
    fn inject_into_sse_body_none_without_usage() {
        let body = b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";
        assert!(
            inject_into_sse_body(body, &stats(), false).is_none(),
            "no usage frame → nothing to edit"
        );
    }

    #[tokio::test]
    async fn inject_nonstreaming_error_body_vetoes_commit() {
        use axum::body::Body;
        let resp = Response::builder()
            .status(400)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::json!({"error":{"message":"bad request"}}).to_string()))
            .unwrap();
        let (_out, billing_ok) = inject_into_response_nonstreaming(resp, &stats(), &UpstreamCachedTokens::default(), false).await;
        assert!(!billing_ok, "error body → no commit");
    }

    #[tokio::test]
    async fn inject_nonstreaming_success_injects_and_allows_commit() {
        use axum::body::Body;
        let resp = Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::json!({"usage":{"prompt_tokens":2000}}).to_string()))
            .unwrap();
        let (out, billing_ok) = inject_into_response_nonstreaming(resp, &stats(), &UpstreamCachedTokens::default(), false).await;
        assert!(billing_ok, "2xx with usage → commit allowed");
        let collected = axum::body::to_bytes(out.into_body(), usize::MAX).await.unwrap();
        let s = std::str::from_utf8(&collected).unwrap();
        assert!(s.contains("\"cached_tokens\":1024"), "got: {s}");
    }

    #[test]
    fn scrub_removes_provider_fields_and_zeroes_cached_tokens() {
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 985,
            "prompt_tokens_details": {"cached_tokens": 687, "audio_tokens": 3},
            "prompt_cache_hit_tokens": 687,
            "prompt_cache_miss_tokens": 298,
            "cache_read_input_tokens": 687,
            "cache_creation": {"ephemeral_5m_input_tokens": 10},
            "cache_discount": 0.9
        }})
        .to_string();
        let out = scrub_usage_json(body.as_bytes()).expect("dirty usage → rewritten");
        let v: Value = serde_json::from_slice(&out).unwrap();
        let usage = v["usage"].as_object().unwrap();
        assert_eq!(usage["prompt_tokens"], 985, "token totals untouched");
        assert_eq!(usage["prompt_tokens_details"]["cached_tokens"], 0, "zeroed, not removed");
        assert_eq!(usage["prompt_tokens_details"]["audio_tokens"], 3, "non-cache detail preserved");
        for key in PROVIDER_CACHE_FIELDS {
            assert!(!usage.contains_key(key), "{key} must be removed");
        }
    }

    #[test]
    fn scrub_none_when_usage_already_clean() {
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 100,
            "prompt_tokens_details": {"cached_tokens": 0}
        }})
        .to_string();
        assert!(scrub_usage_json(body.as_bytes()).is_none());
        assert!(scrub_usage_json(br#"{"choices":[]}"#).is_none(), "no usage object → no-op");
        assert!(scrub_usage_json(b"not json").is_none());
    }

    #[test]
    fn inject_also_scrubs_provider_only_fields() {
        let body = serde_json::json!({"usage":{
            "prompt_tokens": 2000,
            "prompt_cache_hit_tokens": 500,
            "cache_discount": 0.5
        }})
        .to_string();
        let (out, _) = inject_into_usage_json(body.as_bytes(), &stats(), false).unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        let usage = v["usage"].as_object().unwrap();
        assert!(!usage.contains_key("prompt_cache_hit_tokens"));
        assert!(!usage.contains_key("cache_discount"));
        assert_eq!(usage["cache_read_input_tokens"], 1024, "our fields written");
    }

    #[test]
    fn scrub_sse_zeroes_cached_tokens_in_terminal_frame() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":985,\"prompt_tokens_details\":{\"cached_tokens\":687}}}\n\ndata: [DONE]\n\n";
        let out = scrub_sse_body(sse.as_bytes()).expect("dirty usage frame → rewritten");
        let s = std::str::from_utf8(&out).unwrap();
        assert!(s.contains("\"cached_tokens\":0"), "got: {s}");
        assert!(!s.contains("687"), "provider value gone, got: {s}");
        assert!(
            s.contains("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}"),
            "delta byte-identical"
        );
        assert!(s.contains("data: [DONE]"));
    }

    #[test]
    fn scrub_sse_none_when_clean() {
        let clean = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100}}\n\ndata: [DONE]\n\n";
        assert!(scrub_sse_body(clean.as_bytes()).is_none());
        let no_usage = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n";
        assert!(scrub_sse_body(no_usage.as_bytes()).is_none());
    }

    #[test]
    fn scrub_zeroes_non_numeric_cached_tokens() {
        for weird in [serde_json::json!("687"), serde_json::json!(687.5)] {
            let body = serde_json::json!({"usage":{"prompt_tokens_details":{"cached_tokens": weird}}}).to_string();
            let out = scrub_usage_json(body.as_bytes()).expect("non-zero value → rewritten");
            let v: Value = serde_json::from_slice(&out).unwrap();
            assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 0);
        }
    }

    #[tokio::test]
    async fn scrub_nonstreaming_passes_error_responses_through() {
        use axum::body::Body;
        let body = serde_json::json!({"error":{"message":"upstream failed"}}).to_string();
        let resp = Response::builder()
            .status(502)
            .header("content-type", "application/json")
            .body(Body::from(body.clone()))
            .unwrap();
        let out = scrub_response_nonstreaming(resp, &UpstreamCachedTokens::default()).await;
        assert_eq!(out.status(), 502, "status preserved");
        let collected = axum::body::to_bytes(out.into_body(), usize::MAX).await.unwrap();
        assert_eq!(collected, body.as_bytes(), "error body untouched");
    }

    #[tokio::test]
    async fn scrub_nonstreaming_response_removes_provider_cache_fields() {
        use axum::body::Body;
        let resp = Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"usage":{
                    "prompt_tokens": 985,
                    "prompt_tokens_details": {"cached_tokens": 687},
                    "cache_read_input_tokens": 687
                }})
                .to_string(),
            ))
            .unwrap();
        let out = scrub_response_nonstreaming(resp, &UpstreamCachedTokens::default()).await;
        assert_eq!(out.status(), 200);
        let collected = axum::body::to_bytes(out.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(v["usage"]["prompt_tokens_details"]["cached_tokens"], 0);
        assert!(!v["usage"].as_object().unwrap().contains_key("cache_read_input_tokens"));
    }
}
