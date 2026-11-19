
use serde_json::{Value, json};

use super::model::{Content, ContentBlock, ImageSource, InputMessage, MessagesRequest, Tool, ToolResultContent};
use crate::inference::translation::TranslationError;

pub fn to_chat_completions(req: MessagesRequest, cache_enabled: bool) -> Result<Value, TranslationError> {
    let mut messages: Vec<Value> = Vec::new();

    if let Some(system) = &req.system
        && let Some(msg) = system_to_message(system)
    {
        messages.push(msg);
    }

    for m in &req.messages {
        convert_message(m, &mut messages)?;
    }

    let mut out = serde_json::Map::new();
    out.insert("model".into(), json!(req.model));
    out.insert("max_tokens".into(), json!(req.max_tokens));
    out.insert("messages".into(), Value::Array(messages));

    if req.stream {
        out.insert("stream".into(), json!(true));
        out.insert("stream_options".into(), json!({ "include_usage": true }));
    }
    if let Some(t) = req.temperature {
        out.insert("temperature".into(), json!(t));
    }
    if let Some(p) = req.top_p {
        out.insert("top_p".into(), json!(p));
    }
    if let Some(k) = req.top_k {
        out.insert("top_k".into(), json!(k));
    }
    if let Some(s) = &req.stop_sequences {
        out.insert("stop".into(), json!(s));
    }
    if let Some(tools) = &req.tools
        && !tools.is_empty()
    {
        out.insert("tools".into(), Value::Array(tools.iter().map(tool_to_openai).collect()));
    }
    if let Some(tc) = &req.tool_choice
        && let Some(v) = tool_choice_to_openai(tc)
    {
        out.insert("tool_choice".into(), v);
    }
    if let Some(effort) = reasoning_effort(req.thinking.as_ref(), req.output_config.as_ref())? {
        out.insert("reasoning_effort".into(), json!(effort));
    }
    if let Some(format) = response_format(req.output_config.as_ref())? {
        out.insert("response_format".into(), format);
    }
    if req.service_tier.as_deref() == Some("flex") {
        out.insert("service_tier".into(), json!("flex"));
    }
    if cache_enabled
        && let Some(cc) = &req.cache_control
        && !cc.is_null()
    {
        out.insert("cache_control".into(), cc.clone());
    }
    if let Some(nvext) = req.nvext.filter(Value::is_object) {
        out.insert("nvext".into(), nvext);
    }

    if !cache_enabled {
        strip_cache_control_markers(&mut out);
    }

    Ok(Value::Object(out))
}

fn strip_cache_control_markers(out: &mut serde_json::Map<String, Value>) {
    out.shift_remove("cache_control");
    if let Some(messages) = out.get_mut("messages").and_then(Value::as_array_mut) {
        for msg in messages.iter_mut() {
            if let Some(obj) = msg.as_object_mut() {
                obj.shift_remove("cache_control");
            }
            if let Some(content) = msg.get_mut("content").and_then(Value::as_array_mut) {
                for part in content.iter_mut() {
                    if let Some(part_obj) = part.as_object_mut() {
                        part_obj.shift_remove("cache_control");
                    }
                }
            }
            if let Some(tool_calls) = msg.get_mut("tool_calls").and_then(Value::as_array_mut) {
                for call in tool_calls.iter_mut() {
                    if let Some(call_obj) = call.as_object_mut() {
                        call_obj.shift_remove("cache_control");
                    }
                }
            }
        }
    }
    if let Some(tools) = out.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools.iter_mut() {
            if let Some(tool_obj) = tool.as_object_mut() {
                tool_obj.shift_remove("cache_control");
            }
        }
    }
}

const ANTHROPIC_EFFORTS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

fn reasoning_effort(thinking: Option<&Value>, output_config: Option<&Value>) -> Result<Option<&'static str>, TranslationError> {
    if let Some(config) = output_config
        && !config.is_object()
    {
        return Err(TranslationError::InvalidParameter {
            param: "output_config",
            message: format!("output_config: expected an object, got {config}"),
        });
    }
    let effort = match output_config.and_then(|config| config.get("effort")).filter(|v| !v.is_null()) {
        None => None,
        Some(value) => Some(
            value
                .as_str()
                .and_then(|effort| ANTHROPIC_EFFORTS.into_iter().find(|known| *known == effort))
                .ok_or_else(|| TranslationError::InvalidParameter {
                    param: "output_config.effort",
                    message: format!(
                        "output_config.effort: expected one of {}, got {value}",
                        ANTHROPIC_EFFORTS.join(", ")
                    ),
                })?,
        ),
    };
    let Some(thinking) = thinking else {
        return Ok(effort);
    };
    match thinking.get("type").and_then(Value::as_str) {
        Some("disabled" | "between_tools") => Ok(Some("none")),
        Some("adaptive") => Ok(effort),
        Some("enabled") => Ok(Some(effort.unwrap_or_else(|| {
            let budget = thinking.get("budget_tokens").and_then(Value::as_u64).unwrap_or(0);
            if budget <= 2048 {
                "low"
            } else if budget <= 8192 {
                "medium"
            } else {
                "high"
            }
        }))),
        _ => Err(TranslationError::InvalidParameter {
            param: "thinking.type",
            message: format!(
                "thinking.type: expected one of enabled, adaptive, disabled, between_tools, got {}",
                thinking.get("type").unwrap_or(&Value::Null)
            ),
        }),
    }
}

const RESPONSE_FORMAT_NAME: &str = "output";

fn response_format(output_config: Option<&Value>) -> Result<Option<Value>, TranslationError> {
    let Some(format) = output_config.and_then(|config| config.get("format")).filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    match format.get("type").and_then(Value::as_str) {
        Some("json_schema") => match format.get("schema") {
            Some(schema) if schema.is_object() => Ok(Some(json!({
                "type": "json_schema",
                "json_schema": { "name": RESPONSE_FORMAT_NAME, "schema": schema },
            }))),
            _ => Err(TranslationError::InvalidParameter {
                param: "output_config.format.schema",
                message: "output_config.format.schema: expected a JSON schema object".into(),
            }),
        },
        _ => Err(TranslationError::InvalidParameter {
            param: "output_config.format.type",
            message: format!(
                "output_config.format.type: expected json_schema, got {}",
                format.get("type").unwrap_or(&Value::Null)
            ),
        }),
    }
}

fn system_to_message(system: &Content) -> Option<Value> {
    match system {
        Content::Text(t) if !t.is_empty() => Some(json!({ "role": "system", "content": t })),
        Content::Text(_) => None,
        Content::Blocks(blocks) => {
            let parts: Vec<Value> = blocks.iter().filter_map(text_block_to_part).collect();
            if parts.is_empty() {
                None
            } else {
                Some(json!({ "role": "system", "content": parts }))
            }
        }
    }
}

fn convert_message(m: &InputMessage, out: &mut Vec<Value>) -> Result<(), TranslationError> {
    match m.role.as_str() {
        "assistant" => out.push(convert_assistant(&m.content)?),
        "user" => convert_user(&m.content, out),
        "system" => out.extend(system_to_message(&m.content)),
        other => {
            return Err(TranslationError::InvalidParameter {
                param: "messages[].role",
                message: format!("messages[].role: expected one of user, assistant, system, got {other}"),
            });
        }
    }
    Ok(())
}

fn convert_assistant(content: &Content) -> Result<Value, TranslationError> {
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut tool_calls: Vec<Value> = Vec::new();
    let mut marker: Option<Value> = None;

    match content {
        Content::Text(t) => text.push_str(t),
        Content::Blocks(blocks) => {
            for b in blocks {
                match b {
                    ContentBlock::Text { text: t, cache_control } => {
                        text.push_str(t);
                        if let Some(cc) = cache_control
                            && !cc.is_null()
                        {
                            marker = Some(cc.clone());
                        }
                    }
                    ContentBlock::ToolUse {
                        id,
                        name,
                        input,
                        cache_control,
                    } => {
                        let mut call = json!({
                            "id": id,
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": serde_json::to_string(input).unwrap_or_else(|_| "{}".into()),
                            }
                        });
                        if let Some(cc) = cache_control
                            && !cc.is_null()
                        {
                            call["cache_control"] = cc.clone();
                        }
                        tool_calls.push(call);
                    }
                    ContentBlock::Thinking { thinking } if !thinking.is_empty() => {
                        if !reasoning.is_empty() {
                            reasoning.push('\n');
                        }
                        reasoning.push_str(thinking);
                    }
                    _ => {}
                }
            }
        }
    }

    let mut msg = serde_json::Map::new();
    msg.insert("role".into(), json!("assistant"));
    if text.is_empty() {
        if marker.is_some() {
            return Err(TranslationError::InvalidParameter {
                param: "messages[].content",
                message: "messages[].content: cache_control cannot be set on an empty text block".to_string(),
            });
        }
        msg.insert("content".into(), Value::Null);
    } else if let Some(cc) = marker {
        msg.insert("content".into(), json!([{ "type": "text", "text": text, "cache_control": cc }]));
    } else {
        msg.insert("content".into(), json!(text));
    }
    if !reasoning.is_empty() {
        msg.insert("reasoning_content".into(), json!(reasoning));
    }
    if !tool_calls.is_empty() {
        msg.insert("tool_calls".into(), Value::Array(tool_calls));
    }
    Ok(Value::Object(msg))
}

fn convert_user(content: &Content, out: &mut Vec<Value>) {
    match content {
        Content::Text(t) => out.push(json!({ "role": "user", "content": t })),
        Content::Blocks(blocks) => {
            let mut tool_messages: Vec<Value> = Vec::new();
            let mut user_parts: Vec<Value> = Vec::new();

            for b in blocks {
                match b {
                    ContentBlock::Text { .. } => {
                        if let Some(part) = text_block_to_part(b) {
                            user_parts.push(part);
                        }
                    }
                    ContentBlock::Image { source, cache_control } => {
                        let mut part = json!({ "type": "image_url", "image_url": { "url": image_source_to_url(source) } });
                        if let Some(cc) = cache_control {
                            part["cache_control"] = cc.clone();
                        }
                        user_parts.push(part);
                    }
                    ContentBlock::ToolResult {
                        tool_use_id,
                        content,
                        cache_control,
                        ..
                    } => {
                        let result_text = tool_result_to_text(content);
                        let marker = cache_control
                            .as_ref()
                            .filter(|cc| !cc.is_null())
                            .cloned()
                            .or_else(|| nested_tool_result_marker(content));
                        let content_val = match marker {
                            Some(cc) => json!([{ "type": "text", "text": result_text, "cache_control": cc }]),
                            None => json!(result_text),
                        };
                        tool_messages.push(json!({
                            "role": "tool",
                            "tool_call_id": tool_use_id,
                            "content": content_val,
                        }));
                    }
                    _ => {}
                }
            }

            out.append(&mut tool_messages);
            if !user_parts.is_empty() {
                out.push(json!({ "role": "user", "content": user_parts }));
            }
        }
    }
}

fn text_block_to_part(block: &ContentBlock) -> Option<Value> {
    if let ContentBlock::Text { text, cache_control } = block {
        let mut part = json!({ "type": "text", "text": text });
        if let Some(cc) = cache_control {
            part["cache_control"] = cc.clone();
        }
        Some(part)
    } else {
        None
    }
}

fn image_source_to_url(source: &ImageSource) -> String {
    match source {
        ImageSource::Url { url } => url.clone(),
        ImageSource::Base64 { media_type, data } => format!("data:{media_type};base64,{data}"),
    }
}

fn nested_tool_result_marker(content: &Option<ToolResultContent>) -> Option<Value> {
    match content {
        Some(ToolResultContent::Blocks(blocks)) => blocks
            .iter()
            .rev()
            .filter_map(|b| b.get("cache_control"))
            .find(|cc| !cc.is_null())
            .cloned(),
        _ => None,
    }
}

fn tool_result_to_text(content: &Option<ToolResultContent>) -> String {
    match content {
        Some(ToolResultContent::Text(t)) => t.clone(),
        Some(ToolResultContent::Blocks(blocks)) => blocks
            .iter()
            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join(""),
        None => String::new(),
    }
}

fn tool_to_openai(t: &Tool) -> Value {
    let mut tool = json!({
        "type": "function",
        "function": {
            "name": t.name,
            "description": t.description,
            "parameters": t.input_schema.clone().unwrap_or_else(|| json!({ "type": "object" })),
        }
    });
    if let Some(cc) = &t.cache_control {
        tool["cache_control"] = cc.clone();
    }
    tool
}

fn tool_choice_to_openai(tc: &Value) -> Option<Value> {
    match tc.get("type").and_then(|v| v.as_str()) {
        Some("auto") => Some(json!("auto")),
        Some("any") => Some(json!("required")),
        Some("tool") => tc
            .get("name")
            .and_then(|n| n.as_str())
            .map(|name| json!({ "type": "function", "function": { "name": name } })),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn translate(v: Value) -> Value {
        to_chat_completions(serde_json::from_value::<MessagesRequest>(v).unwrap(), true).unwrap()
    }

    #[test]
    fn tool_input_schema_keeps_key_order() {
        let schema = r#"{"type":"object","properties":{"zeta":{"type":"string"},"alpha":{"type":"string"}},"required":["zeta","alpha"]}"#;
        let req: MessagesRequest = serde_json::from_str(&format!(
            r#"{{"model":"m","max_tokens":16,"messages":[{{"role":"user","content":"hi"}}],"tools":[{{"name":"lookup","input_schema":{schema}}}]}}"#
        ))
        .unwrap();
        let out = serde_json::to_string(&to_chat_completions(req, true).unwrap()).unwrap();
        assert!(out.contains(&format!(r#""parameters":{schema}"#)), "{out}");
    }

    #[test]
    fn top_level_cache_control_forwarded_for_automatic_caching() {
        let out = translate(json!({
            "model": "m",
            "max_tokens": 16,
            "cache_control": {"type": "ephemeral", "ttl": "1h"},
            "messages": [{"role": "user", "content": "hi"}]
        }));
        assert_eq!(out["cache_control"], json!({"type": "ephemeral", "ttl": "1h"}));
    }

    #[test]
    fn top_level_cache_control_absent_or_null_not_forwarded() {
        let absent = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [{"role": "user", "content": "hi"}]
        }));
        assert!(absent.get("cache_control").is_none(), "no marker → not emitted");

        let null = translate(json!({
            "model": "m", "max_tokens": 16,
            "cache_control": null,
            "messages": [{"role": "user", "content": "hi"}]
        }));
        assert!(null.get("cache_control").is_none(), "null marker → not emitted");
    }

    #[test]
    fn top_level_cache_control_not_emitted_when_caching_disabled() {
        let req = serde_json::from_value::<MessagesRequest>(json!({
            "model": "m", "max_tokens": 16,
            "cache_control": {"type": "ephemeral"},
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .unwrap();
        let out = to_chat_completions(req, false).unwrap();
        assert!(out.get("cache_control").is_none(), "caching disabled → top-level marker dropped");
    }

    fn any_cache_control(v: &Value) -> bool {
        match v {
            Value::Object(map) => map.contains_key("cache_control") || map.values().any(any_cache_control),
            Value::Array(arr) => arr.iter().any(any_cache_control),
            _ => false,
        }
    }

    #[test]
    fn all_block_markers_stripped_when_caching_disabled() {
        let body = json!({
            "model": "m", "max_tokens": 16,
            "system": [{"type": "text", "text": "sys", "cache_control": {"type": "ephemeral"}}],
            "tools": [{"name": "lookup", "description": "d", "input_schema": {"type": "object"},
                       "cache_control": {"type": "ephemeral"}}],
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "q", "cache_control": {"type": "ephemeral"}}
                ]},
                {"role": "assistant", "content": [
                    {"type": "text", "text": "thinking", "cache_control": {"type": "ephemeral"}},
                    {"type": "tool_use", "id": "tu1", "name": "lookup", "input": {"x": 1},
                     "cache_control": {"type": "ephemeral"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "tu1", "content": "res",
                     "cache_control": {"type": "ephemeral"}}
                ]}
            ]
        });

        assert!(any_cache_control(&translate(body.clone())), "caching on → markers preserved");

        let off = to_chat_completions(serde_json::from_value(body).unwrap(), false).unwrap();
        assert!(!any_cache_control(&off), "caching off → every block marker stripped");
    }

    #[test]
    fn assistant_cache_control_preserved_as_array_content() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [
                { "role": "user", "content": "hi" },
                { "role": "assistant", "content": [
                    { "type": "text", "text": "let me think", "cache_control": { "type": "ephemeral" } }
                ]}
            ]
        }));
        let asst = &out["messages"][1];
        assert_eq!(asst["role"], "assistant");
        assert_eq!(asst["content"][0]["type"], "text");
        assert_eq!(asst["content"][0]["text"], "let me think");
        assert_eq!(asst["content"][0]["cache_control"]["type"], "ephemeral");
    }

    #[test]
    fn assistant_without_marker_stays_string() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [ { "role": "assistant", "content": [ { "type": "text", "text": "plain" } ] } ]
        }));
        assert_eq!(out["messages"][0]["content"], "plain");
    }

    #[test]
    fn tool_result_cache_control_preserved_as_array_content() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [
                { "role": "assistant", "content": [ { "type": "tool_use", "id": "tu_1", "name": "wx", "input": {} } ] },
                { "role": "user", "content": [
                    { "type": "tool_result", "tool_use_id": "tu_1", "content": "sunny",
                      "cache_control": { "type": "ephemeral", "ttl": "1h" } }
                ]}
            ]
        }));
        let tool = &out["messages"][1];
        assert_eq!(tool["role"], "tool");
        assert_eq!(tool["tool_call_id"], "tu_1");
        assert_eq!(tool["content"][0]["text"], "sunny");
        assert_eq!(tool["content"][0]["cache_control"]["ttl"], "1h");
    }

    #[test]
    fn explicit_null_cache_control_is_not_a_marker() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [
                { "role": "assistant", "content": [ { "type": "text", "text": "hi", "cache_control": null } ] },
                { "role": "user", "content": [
                    { "type": "tool_result", "tool_use_id": "tu_1", "content": "sunny", "cache_control": null }
                ]}
            ]
        }));
        assert_eq!(out["messages"][0]["content"], "hi", "null marker → assistant stays a string");
        assert_eq!(out["messages"][1]["content"], "sunny", "null marker → tool_result stays a string");
    }

    #[test]
    fn tool_result_without_marker_stays_string() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [
                { "role": "assistant", "content": [ { "type": "tool_use", "id": "tu_1", "name": "wx", "input": {} } ] },
                { "role": "user", "content": [ { "type": "tool_result", "tool_use_id": "tu_1", "content": "sunny" } ] }
            ]
        }));
        assert_eq!(out["messages"][1]["content"], "sunny", "unmarked tool_result stays a plain string");
    }

    #[test]
    fn assistant_tool_use_cache_control_preserved_on_tool_call() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [{ "role": "assistant", "content": [
                { "type": "text", "text": "calling" },
                { "type": "tool_use", "id": "tu_1", "name": "lookup", "input": {"q": "x"},
                  "cache_control": { "type": "ephemeral", "ttl": "1h" } }
            ]}]
        }));
        let call = &out["messages"][0]["tool_calls"][0];
        assert_eq!(call["id"], "tu_1");
        assert_eq!(call["function"]["name"], "lookup");
        assert_eq!(call["cache_control"]["ttl"], "1h", "marker preserved on the tool_call");
    }

    #[test]
    fn assistant_tool_use_without_marker_has_no_cache_control() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [{ "role": "assistant", "content": [
                { "type": "tool_use", "id": "tu_1", "name": "lookup", "input": {} }
            ]}]
        }));
        assert!(
            out["messages"][0]["tool_calls"][0].get("cache_control").is_none(),
            "no marker → clean tool_call"
        );
    }

    #[test]
    fn system_role_in_messages_kept_in_place() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "system": "top-level system",
            "messages": [
                { "role": "user", "content": "hi" },
                { "role": "system", "content": [
                    { "type": "text", "text": "reminder", "cache_control": { "type": "ephemeral" } }
                ]},
                { "role": "user", "content": "go" }
            ]
        }));
        let msgs = out["messages"].as_array().unwrap();
        assert_eq!(msgs[0], json!({ "role": "system", "content": "top-level system" }));
        assert_eq!(msgs[1]["role"], "user");
        assert_eq!(msgs[2]["role"], "system");
        assert_eq!(
            msgs[2]["content"],
            json!([{ "type": "text", "text": "reminder", "cache_control": { "type": "ephemeral" } }]),
            "block-form system entry keeps its cache_control part"
        );
        assert_eq!(msgs[3]["role"], "user");
    }

    #[test]
    fn unknown_role_in_messages_still_rejected() {
        let req = serde_json::from_value::<MessagesRequest>(json!({
            "model": "m", "max_tokens": 16,
            "messages": [{ "role": "developer", "content": "hi" }]
        }))
        .unwrap();
        let err = to_chat_completions(req, true).unwrap_err();
        assert!(matches!(
            err,
            TranslationError::InvalidParameter { param: "messages[].role", message } if message.contains("developer")
        ));
    }

    fn effort_for(extra: Value) -> Result<Option<Value>, TranslationError> {
        let mut body = json!({"model": "m", "max_tokens": 16, "messages": [{"role": "user", "content": "hi"}]});
        body.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        let out = to_chat_completions(serde_json::from_value::<MessagesRequest>(body).unwrap(), true)?;
        Ok(out.get("reasoning_effort").cloned())
    }

    #[test]
    fn thinking_off_maps_to_effort_none_whatever_the_effort() {
        for kind in ["disabled", "between_tools"] {
            for extra in [
                json!({"thinking": {"type": kind}}),
                json!({"thinking": {"type": kind}, "output_config": {"effort": "high"}}),
            ] {
                assert_eq!(effort_for(extra.clone()).unwrap(), Some(json!("none")), "{extra}");
            }
        }
    }

    #[test]
    fn output_config_effort_maps_by_name() {
        for effort in ["low", "medium", "high", "xhigh", "max"] {
            for extra in [
                json!({"output_config": {"effort": effort}}),
                json!({"thinking": {"type": "adaptive"}, "output_config": {"effort": effort}}),
                json!({"thinking": {"type": "enabled", "budget_tokens": 32000}, "output_config": {"effort": effort}}),
            ] {
                assert_eq!(effort_for(extra.clone()).unwrap(), Some(json!(effort)), "{extra}");
            }
        }
    }

    #[test]
    fn adaptive_thinking_without_effort_leaves_the_model_default() {
        assert_eq!(
            effort_for(json!({"thinking": {"type": "adaptive", "display": "summarized"}})).unwrap(),
            None
        );
        assert_eq!(
            effort_for(json!({"thinking": null, "output_config": {"format": {"type": "json_schema", "schema": {}}}})).unwrap(),
            None
        );
    }

    #[test]
    fn enabled_thinking_without_effort_buckets_the_budget() {
        for (budget, effort) in [(1024, "low"), (4096, "medium"), (32000, "high")] {
            let extra = json!({"thinking": {"type": "enabled", "budget_tokens": budget}});
            assert_eq!(effort_for(extra).unwrap(), Some(json!(effort)), "{budget}");
        }
    }

    #[test]
    fn rejects_unknown_thinking_types_and_effort_values() {
        for (extra, field) in [
            (json!({"thinking": {"type": "sometimes"}}), "thinking.type"),
            (json!({"thinking": {}}), "thinking.type"),
            (json!({"output_config": {"effort": "none"}}), "output_config.effort"),
            (json!({"output_config": {"effort": 3}}), "output_config.effort"),
            (
                json!({"thinking": {"type": "disabled"}, "output_config": {"effort": "turbo"}}),
                "output_config.effort",
            ),
            (json!({"output_config": "high"}), "output_config"),
            (json!({"output_config": ["high"]}), "output_config"),
        ] {
            let err = effort_for(extra.clone()).unwrap_err();
            assert!(
                matches!(&err, TranslationError::InvalidParameter { param, message } if *param == field && message.contains(field)),
                "{extra}: {err}"
            );
        }
    }

    #[test]
    fn output_config_format_becomes_a_response_format() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16, "messages": [{"role": "user", "content": "hi"}],
            "output_config": {"effort": "low", "format": {"type": "json_schema", "schema": {
                "type": "object",
                "properties": {"name": {"type": "string"}, "note": {"type": "string"}},
                "required": ["name"],
                "additionalProperties": false
            }}}
        }));
        assert_eq!(
            out["response_format"],
            json!({"type": "json_schema", "json_schema": {"name": "output", "schema": {
                "type": "object",
                "properties": {"name": {"type": "string"}, "note": {"type": "string"}},
                "required": ["name"],
                "additionalProperties": false
            }}})
        );
        assert_eq!(out["reasoning_effort"], "low", "effort still applies next to a format");
    }

    #[test]
    fn output_config_format_keeps_schema_key_order() {
        let schema = r#"{"type":"object","properties":{"zeta":{"type":"string"},"alpha":{"type":"string"}}}"#;
        let req: MessagesRequest = serde_json::from_str(&format!(
            r#"{{"model":"m","max_tokens":16,"messages":[],"output_config":{{"format":{{"type":"json_schema","schema":{schema}}}}}}}"#
        ))
        .unwrap();
        let out = serde_json::to_string(&to_chat_completions(req, true).unwrap()).unwrap();
        assert!(out.contains(&format!(r#""schema":{schema}"#)), "{out}");
    }

    #[test]
    fn no_output_format_sets_no_response_format() {
        for extra in [
            json!({}),
            json!({"output_config": {"effort": "high"}}),
            json!({"output_config": {"format": null}}),
        ] {
            let mut body = json!({"model": "m", "max_tokens": 16, "messages": []});
            body.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            assert!(translate(body).get("response_format").is_none(), "{extra}");
        }
    }

    #[test]
    fn rejects_output_formats_it_cannot_enforce() {
        for (format, field) in [
            (json!({"type": "json_object"}), "output_config.format.type"),
            (json!({"schema": {"type": "object"}}), "output_config.format.type"),
            (json!("json_schema"), "output_config.format.type"),
            (json!({"type": "json_schema"}), "output_config.format.schema"),
            (json!({"type": "json_schema", "schema": "{}"}), "output_config.format.schema"),
        ] {
            let req = serde_json::from_value::<MessagesRequest>(json!({
                "model": "m", "max_tokens": 16, "messages": [], "output_config": {"format": format}
            }))
            .unwrap();
            let err = to_chat_completions(req, true).unwrap_err();
            assert!(
                matches!(&err, TranslationError::InvalidParameter { param, message } if *param == field && message.contains(field)),
                "{format}: {err}"
            );
        }
    }

    #[test]
    fn earlier_thinking_is_replayed_as_reasoning_content() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [
                {"role": "user", "content": "weather in SF?"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "I need the tool.", "signature": "sig-a"},
                    {"type": "text", "text": "Checking."},
                    {"type": "thinking", "thinking": "Call it for SF.", "signature": "sig-b"},
                    {"type": "tool_use", "id": "tu_1", "name": "wx", "input": {"city": "SF"}}
                ]},
                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "tu_1", "content": "sunny"}]}
            ]
        }));
        let assistant = &out["messages"][1];
        assert_eq!(assistant["reasoning_content"], "I need the tool.\nCall it for SF.");
        assert_eq!(assistant["content"], "Checking.");
        assert_eq!(assistant["tool_calls"][0]["id"], "tu_1");
        assert!(!out.to_string().contains("sig-"), "signatures are not forwarded");
    }

    #[test]
    fn thinking_only_turn_keeps_its_reasoning() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [{"role": "assistant", "content": [{"type": "thinking", "thinking": "hmm", "signature": "s"}]}]
        }));
        assert_eq!(
            out["messages"][0],
            json!({"role": "assistant", "content": null, "reasoning_content": "hmm"})
        );
    }

    #[test]
    fn empty_and_redacted_thinking_add_no_reasoning() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [{"role": "assistant", "content": [
                {"type": "thinking", "thinking": "", "signature": "s"},
                {"type": "thinking", "signature": "s"},
                {"type": "redacted_thinking", "data": "encrypted"},
                {"type": "text", "text": "answer"}
            ]}]
        }));
        assert_eq!(out["messages"][0], json!({"role": "assistant", "content": "answer"}));
    }

    #[test]
    fn thinking_in_a_user_turn_is_ignored() {
        let out = translate(json!({
            "model": "m", "max_tokens": 16,
            "messages": [{"role": "user", "content": [
                {"type": "thinking", "thinking": "not ours", "signature": "s"},
                {"type": "text", "text": "hi"}
            ]}]
        }));
        assert_eq!(
            out["messages"][0],
            json!({"role": "user", "content": [{"type": "text", "text": "hi"}]})
        );
    }
}
