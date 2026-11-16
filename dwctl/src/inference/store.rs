
use std::sync::Arc;

use async_trait::async_trait;
use fusillade::{RequestId, RetainedResponseWriteError, Storage};
use fusillade_arsenal::PostgresRequestManager;
use sqlx_pool_router::PoolProvider;
use uuid::Uuid;

use crate::crypto::hash_api_key;
use crate::inference::response_store::{ResponseContext, ResponseStore, StoreError};

pub const ONWARDS_RESPONSE_ID_HEADER: &str = "x-onwards-response-id";

#[derive(Debug, Clone, Copy)]
pub struct OnwardsDaemonId(pub Uuid);

pub struct FusilladeResponseStore<P: PoolProvider + Clone> {
    request_manager: Arc<PostgresRequestManager<P>>,
    keystore: Option<crate::keystore::Keystore>,
}

impl<P: PoolProvider + Clone> FusilladeResponseStore<P> {
    pub fn new(request_manager: Arc<PostgresRequestManager<P>>) -> Self {
        Self {
            request_manager,
            keystore: None,
        }
    }

    pub fn with_keystore(mut self, keystore: Option<crate::keystore::Keystore>) -> Self {
        self.keystore = keystore;
        self
    }

    pub async fn get_response(&self, response_id: &str) -> Result<ResponseLookup, StoreError> {
        let parsed_uuid = parse_response_id(response_id)?;

        match self.request_manager.get_request_detail(RequestId(parsed_uuid)).await {
            Ok(mut detail) => {
                if let Some(ks) = self.keystore.as_ref() {
                    match crate::inference::zdr::decrypt_response_body(ks, &detail.id, detail.response_body.as_deref())
                        .await
                        .map_err(|e| StoreError::StorageError(format!("ZDR response decrypt: {e}")))?
                    {
                        crate::inference::zdr::DecryptOutcome::Unchanged => {}
                        crate::inference::zdr::DecryptOutcome::Decrypted(plain) => detail.response_body = Some(plain),
                        crate::inference::zdr::DecryptOutcome::Gone => return Ok(ResponseLookup::Gone),
                    }
                }
                Ok(ResponseLookup::Found(detail_to_response_object(&detail)))
            }
            Err(fusillade::FusilladeError::RequestNotFound(_)) => Ok(ResponseLookup::NotFound),
            Err(e) => Err(StoreError::StorageError(format!("Failed to fetch request: {e}"))),
        }
    }
}

#[derive(Debug)]
pub enum ResponseLookup {
    Found(serde_json::Value),
    NotFound,
    Gone,
}

pub async fn fail_response<P: PoolProvider + Clone>(
    request_manager: &PostgresRequestManager<P>,
    response_id: &str,
    error: &str,
) -> Result<(), StoreError> {
    let id = parse_response_id(response_id)?;

    request_manager
        .fail_request(RequestId(id), error, 500)
        .await
        .map_err(|e| StoreError::StorageError(format!("Failed to fail request: {e}")))?;

    Ok(())
}

pub async fn request_exists<P: PoolProvider + Clone>(
    request_manager: &PostgresRequestManager<P>,
    request_id: Uuid,
) -> Result<bool, StoreError> {
    match request_manager.get_request_detail(RequestId(request_id)).await {
        Ok(_) => Ok(true),
        Err(fusillade::FusilladeError::RequestNotFound(_)) => Ok(false),
        Err(e) => Err(StoreError::StorageError(format!("Failed to check request existence: {e}"))),
    }
}

pub struct CreateContext<'a> {
    pub request_id: Uuid,
    pub request_body: &'a str,
    pub model: &'a str,
    pub endpoint: &'a str,
    pub base_url: &'a str,
    pub api_key: Option<&'a str>,
}

pub async fn complete_response_idempotent<P: PoolProvider + Clone>(
    request_manager: &PostgresRequestManager<P>,
    dwctl_pool: &sqlx::PgPool,
    response_id: &str,
    response_body: &str,
    status_code: u16,
    create_ctx: CreateContext<'_>,
) -> Result<(), StoreError> {
    let id = parse_response_id(response_id)?;

    match request_manager.complete_request(RequestId(id), response_body, status_code).await {
        Ok(()) => return Ok(()),
        Err(fusillade::FusilladeError::RequestStateConflict { current_state, .. }) if is_terminal(&current_state) => {
            tracing::info!("complete-response observed a terminal row");
            return Ok(());
        }
        Err(fusillade::FusilladeError::RequestStateConflict { .. }) => {
            return Err(StoreError::StorageError("Response is not writable in its current state".to_owned()));
        }
        Err(fusillade::FusilladeError::RequestNotFound(_)) => {}
        Err(error) => match RetainedResponseWriteError::from_fusillade_error(&error) {
            Some(RetainedResponseWriteError::AlreadyRetained) => return Ok(()),
            Some(RetainedResponseWriteError::NotFound) => return Err(response_unavailable()),
            None => {
                return Err(StoreError::StorageError("Failed to complete response".to_owned()));
            }
        },
    }

    tracing::info!("complete-response is synthesizing a missing row");
    if create_ctx.endpoint.is_empty() {
        return Err(StoreError::StorageError(
            "Cannot synthesize request row: empty endpoint in CreateContext (x-onwards-endpoint header missing upstream)".into(),
        ));
    }
    let created_by = lookup_created_by(dwctl_pool, create_ctx.api_key.map(hash_api_key).as_ref()).await;
    let realtime_input = fusillade::CreateRealtimeInput {
        request_id: create_ctx.request_id,
        body: create_ctx.request_body.to_string(),
        model: create_ctx.model.to_string(),
        endpoint: create_ctx.base_url.to_string(),
        method: "POST".to_string(),
        path: create_ctx.endpoint.to_string(),
        created_by: created_by.unwrap_or_default(),
    };
    let create_failed = match request_manager.create_realtime(realtime_input).await {
        Ok(_) => false,
        Err(error) => match RetainedResponseWriteError::from_fusillade_error(&error) {
            Some(RetainedResponseWriteError::AlreadyRetained) => return Ok(()),
            Some(RetainedResponseWriteError::NotFound) => return Err(response_unavailable()),
            None => true,
        },
    };

    match request_manager.complete_request(RequestId(id), response_body, status_code).await {
        Ok(()) => {
            tracing::info!("complete-response persisted the terminal row");
            Ok(())
        }
        Err(fusillade::FusilladeError::RequestStateConflict { current_state, .. }) if is_terminal(&current_state) => {
            tracing::info!("complete-response observed a terminal row after synthesis");
            Ok(())
        }
        Err(fusillade::FusilladeError::RequestNotFound(_)) if create_failed => {
            Err(StoreError::StorageError("Failed to synthesize response row".to_owned()))
        }
        Err(error) => match RetainedResponseWriteError::from_fusillade_error(&error) {
            Some(RetainedResponseWriteError::AlreadyRetained) => Ok(()),
            Some(RetainedResponseWriteError::NotFound) => Err(response_unavailable()),
            None => Err(StoreError::StorageError("Failed to complete synthesized response".to_owned())),
        },
    }
}

fn response_unavailable() -> StoreError {
    StoreError::NotFound("Response unavailable".to_owned())
}

fn is_terminal(state: &str) -> bool {
    matches!(state, "completed" | "failed" | "canceled")
}

pub async fn poll_until_terminal<P: PoolProvider + Clone>(
    request_manager: &PostgresRequestManager<P>,
    request_id: Uuid,
    poll_interval: std::time::Duration,
    timeout: std::time::Duration,
    keystore: Option<&crate::keystore::Keystore>,
) -> Result<fusillade::RequestDetail, StoreError> {
    let start = std::time::Instant::now();

    loop {
        match request_manager.get_terminal_request_detail(RequestId(request_id)).await {
            Ok(Some(mut detail)) => match detail.status.as_str() {
                "completed" | "failed" | "canceled" => {
                    if let Some(ks) = keystore {
                        match crate::inference::zdr::decrypt_response_body(ks, &detail.id, detail.response_body.as_deref())
                            .await
                            .map_err(|e| StoreError::StorageError(format!("ZDR response decrypt: {e}")))?
                        {
                            crate::inference::zdr::DecryptOutcome::Unchanged => {}
                            crate::inference::zdr::DecryptOutcome::Decrypted(plain) => detail.response_body = Some(plain),
                            crate::inference::zdr::DecryptOutcome::Gone => detail.response_body = None,
                        }
                    }
                    return Ok(detail);
                }
                _ => {}
            },
            Ok(None) => {}
            Err(fusillade::FusilladeError::RequestNotFound(_)) => {}
            Err(e) => {
                return Err(StoreError::StorageError(format!("Failed to poll request: {e}")));
            }
        }

        if start.elapsed() >= timeout {
            return Err(StoreError::StorageError(format!(
                "Timeout waiting for request {request_id} to complete after {timeout:?}"
            )));
        }

        tokio::time::sleep(poll_interval).await;
    }
}

pub async fn poll_until_complete<P: PoolProvider + Clone>(
    request_manager: &PostgresRequestManager<P>,
    response_id: &str,
    poll_interval: std::time::Duration,
    timeout: std::time::Duration,
    keystore: Option<&crate::keystore::Keystore>,
) -> Result<serde_json::Value, StoreError> {
    let id = parse_response_id(response_id)?;
    let detail = poll_until_terminal(request_manager, id, poll_interval, timeout, keystore).await?;
    Ok(detail_to_response_object(&detail))
}

pub async fn lookup_created_by(pool: &sqlx::PgPool, api_key: Option<&crate::crypto::ApiKeyHash>) -> Option<String> {
    let key = api_key?;
    match sqlx::query("SELECT user_id FROM public.api_keys WHERE secret_hash = $1 AND is_deleted = false LIMIT 1")
        .bind(&key[..])
        .fetch_optional(pool)
        .await
    {
        Ok(Some(row)) => {
            use sqlx::Row;
            let user_id: Uuid = row.get("user_id");
            Some(user_id.to_string())
        }
        Ok(None) => {
            tracing::warn!("API key was not found for response attribution");
            None
        }
        Err(_) => {
            tracing::error!("Failed to look up response attribution");
            None
        }
    }
}

fn extract_upstream_error(status: u16, body: &str) -> (&'static str, String) {
    if let Some(message) = parse_openai_error(body) {
        return (status_to_error_type(status), message);
    }
    (status_to_error_type(status), body.to_string())
}

fn parse_failure_error(err: &str) -> (&'static str, String, Option<u16>) {
    if let Ok(reason) = serde_json::from_str::<serde_json::Value>(err)
        && let Some(details) = reason.get("details")
    {
        let status = details.get("status").and_then(|s| s.as_u64()).and_then(|s| u16::try_from(s).ok());
        let error_type = status_to_error_type(status.unwrap_or(500));
        if let Some(body) = details.get("body").and_then(|b| b.as_str()) {
            if let Some(message) = parse_openai_error(body) {
                return (error_type, message, status);
            }
            return (error_type, body.to_string(), status);
        }
    }

    if let Some(rest) = err.strip_prefix("Upstream returned ")
        && let Some(colon_pos) = rest.find(": ")
        && let Ok(status) = rest[..colon_pos].parse::<u16>()
    {
        let body = &rest[colon_pos + 2..];
        if let Some(message) = parse_openai_error(body) {
            return (status_to_error_type(status), message, Some(status));
        }
        return (status_to_error_type(status), body.to_string(), Some(status));
    }

    if let Some(message) = parse_openai_error(err) {
        return ("server_error", message, None);
    }

    ("server_error", err.to_string(), None)
}

fn parse_openai_error(body: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(body).ok()?;
    let error = parsed.get("error")?;
    let message = error.get("message")?.as_str()?;
    Some(message.to_string())
}

fn status_to_error_type(status: u16) -> &'static str {
    match status {
        400 => "invalid_request_error",
        401 => "authentication_error",
        402 => "insufficient_credits",
        403 => "permission_error",
        404 => "not_found_error",
        429 => "rate_limit_error",
        499 => "client_disconnected",
        _ => "server_error",
    }
}

fn parse_response_id(response_id: &str) -> Result<Uuid, StoreError> {
    let uuid_str = response_id.strip_prefix("resp_").unwrap_or(response_id);
    Uuid::parse_str(uuid_str).map_err(|e| StoreError::NotFound(format!("Invalid response ID: {e}")))
}

fn state_to_status(state: &str) -> &'static str {
    match state {
        "pending" => "queued",
        "claimed" | "processing" => "in_progress",
        "completed" => "completed",
        "failed" => "failed",
        "canceled" => "cancelled",
        _ => "failed",
    }
}

pub fn detail_to_response_object(detail: &fusillade::RequestDetail) -> serde_json::Value {
    let status = state_to_status(&detail.status);

    let background = detail.service_tier.as_deref() == Some("background")
        || detail
            .body
            .as_deref()
            .and_then(|b| serde_json::from_str::<serde_json::Value>(b).ok())
            .and_then(|v| v.get("background")?.as_bool())
            .unwrap_or(false);

    let mut resp = serde_json::json!({
        "id": format!("resp_{}", detail.id),
        "object": "response",
        "created_at": detail.created_at.timestamp(),
        "status": status,
        "model": detail.model,
        "background": background,
        "output": [],
    });
    if let Some(service_tier) = detail.service_tier.as_deref() {
        resp["service_tier"] = serde_json::json!(service_tier);
    }

    if status == "completed" {
        let response_status = match detail.response_status {
            Some(s) => u16::try_from(s).unwrap_or(500),
            None => 200,
        };
        let is_error_response = response_status >= 400;

        if is_error_response {
            resp["status"] = serde_json::json!("failed");
            let (error_type, message) = if let Some(ref body) = detail.response_body {
                extract_upstream_error(response_status, body)
            } else {
                (
                    status_to_error_type(response_status),
                    format!("Upstream returned {response_status}"),
                )
            };
            resp["error"] = serde_json::json!({
                "type": error_type,
                "code": response_status,
                "message": message,
            });
        } else if let Some(ref body) = detail.response_body
            && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body)
        {
            if let Some(output) = parsed.get("output") {
                resp["output"] = output.clone();
            }
            if let Some(usage) = parsed.get("usage") {
                resp["usage"] = usage.clone();
            }
            if parsed.get("status").and_then(|s| s.as_str()) == Some("incomplete") {
                resp["status"] = serde_json::json!("incomplete");
                resp["incomplete_details"] = parsed.get("incomplete_details").cloned().unwrap_or(serde_json::Value::Null);
            }
            if parsed.get("choices").is_some() {
                resp["output"] = serde_json::json!([{
                    "type": "message",
                    "role": "assistant",
                    "content": parsed
                }]);
            }
        }
        resp["completed_at"] = if resp["status"] == "incomplete" {
            serde_json::Value::Null
        } else {
            serde_json::json!(detail.completed_at.map(|t| t.timestamp()))
        };
    }

    if status == "failed"
        && let Some(ref err) = detail.error
    {
        let (error_type, message, status_code) = parse_failure_error(err);
        let mut error_obj = serde_json::json!({
            "type": error_type,
            "message": message,
        });
        if let Some(code) = status_code {
            error_obj["code"] = serde_json::json!(code);
        }
        resp["error"] = error_obj;
    }

    resp
}

pub fn detail_to_chat_completion_object(detail: &fusillade::RequestDetail) -> (u16, serde_json::Value) {
    match detail.status.as_str() {
        "completed" => {
            let response_status = detail.response_status.and_then(|s| u16::try_from(s).ok()).unwrap_or(200);
            if response_status >= 400 {
                let (error_type, message) = match detail.response_body.as_deref() {
                    Some(body) => extract_upstream_error(response_status, body),
                    None => (
                        status_to_error_type(response_status),
                        format!("Upstream returned {response_status}"),
                    ),
                };
                (
                    response_status,
                    serde_json::json!({
                        "error": {
                            "message": message,
                            "type": error_type,
                            "code": response_status,
                        }
                    }),
                )
            } else {
                match detail
                    .response_body
                    .as_deref()
                    .and_then(|b| serde_json::from_str::<serde_json::Value>(b).ok())
                {
                    Some(mut parsed) => {
                        if let Some(service_tier) = detail.service_tier.as_deref()
                            && let Some(object) = parsed.as_object_mut()
                        {
                            object.insert("service_tier".to_string(), serde_json::Value::String(service_tier.to_string()));
                        }
                        (response_status, parsed)
                    }
                    None => (
                        500,
                        serde_json::json!({
                            "error": {
                                "message": "Empty or invalid response from upstream",
                                "type": "server_error",
                                "code": 500,
                            }
                        }),
                    ),
                }
            }
        }
        "failed" => {
            let (error_type, message, status_code) = match detail.error.as_deref() {
                Some(err) => parse_failure_error(err),
                None => ("server_error", "Request failed".to_string(), None),
            };
            let code = status_code.unwrap_or(500);
            (
                code,
                serde_json::json!({
                    "error": {
                        "message": message,
                        "type": error_type,
                        "code": code,
                    }
                }),
            )
        }
        "canceled" => (
            499,
            serde_json::json!({
                "error": {
                    "message": "Request was canceled",
                    "type": "request_canceled",
                    "code": 499,
                }
            }),
        ),
        other => {
            tracing::warn!(state = %other, "detail_to_chat_completion_object called on non-terminal state");
            (
                500,
                serde_json::json!({
                    "error": {
                        "message": format!("Unexpected request state: {other}"),
                        "type": "server_error",
                        "code": 500,
                    }
                }),
            )
        }
    }
}

pub fn chat_completion_to_stream_chunks(completion: &serde_json::Value, include_usage: bool) -> Vec<serde_json::Value> {
    let id = completion.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let created = completion.get("created").cloned().unwrap_or(serde_json::Value::Null);
    let model = completion.get("model").cloned().unwrap_or(serde_json::Value::Null);
    let system_fingerprint = completion.get("system_fingerprint").cloned();
    let service_tier = completion.get("service_tier").cloned();

    let base_chunk = |delta: serde_json::Value, index: i64, finish_reason: serde_json::Value| {
        let mut choice = serde_json::json!({
            "index": index,
            "delta": delta,
            "finish_reason": finish_reason,
        });
        choice["logprobs"] = serde_json::Value::Null;
        let mut chunk = serde_json::json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [choice],
        });
        if let Some(fp) = system_fingerprint.clone() {
            chunk["system_fingerprint"] = fp;
        }
        if let Some(tier) = service_tier.clone() {
            chunk["service_tier"] = tier;
        }
        if include_usage {
            chunk["usage"] = serde_json::Value::Null;
        }
        chunk
    };

    let mut chunks = Vec::new();

    if let Some(choices) = completion.get("choices").and_then(|c| c.as_array()) {
        for choice in choices {
            let index = choice.get("index").and_then(|i| i.as_i64()).unwrap_or(0);
            let message = choice.get("message").cloned().unwrap_or_else(|| serde_json::json!({}));
            let role = message
                .get("role")
                .cloned()
                .unwrap_or_else(|| serde_json::Value::String("assistant".to_string()));
            let finish_reason = choice.get("finish_reason").cloned().unwrap_or(serde_json::Value::Null);

            chunks.push(base_chunk(
                serde_json::json!({ "role": role, "content": "" }),
                index,
                serde_json::Value::Null,
            ));

            let mut delta = serde_json::Map::new();
            if let Some(content) = message.get("content")
                && !content.is_null()
            {
                delta.insert("content".to_string(), content.clone());
            }
            if let Some(tool_calls) = message.get("tool_calls")
                && !tool_calls.is_null()
            {
                delta.insert("tool_calls".to_string(), tool_calls.clone());
            }
            if !delta.is_empty() {
                chunks.push(base_chunk(serde_json::Value::Object(delta), index, serde_json::Value::Null));
            }

            chunks.push(base_chunk(serde_json::json!({}), index, finish_reason));
        }
    }

    if include_usage && let Some(usage) = completion.get("usage") {
        let mut chunk = serde_json::json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [],
            "usage": usage.clone(),
        });
        if let Some(fp) = system_fingerprint {
            chunk["system_fingerprint"] = fp;
        }
        if let Some(tier) = service_tier {
            chunk["service_tier"] = tier;
        }
        chunks.push(chunk);
    }

    chunks
}

pub fn response_object_to_stream_events(response: &serde_json::Value) -> Vec<(&'static str, serde_json::Value)> {
    let id = response.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let created_at = response.get("created_at").cloned().unwrap_or(serde_json::Value::Null);
    let model = response.get("model").cloned().unwrap_or(serde_json::Value::Null);

    let stub = serde_json::json!({
        "id": id, "object": "response", "status": "in_progress",
        "created_at": created_at, "model": model, "output": [],
    });

    let mut seq: i64 = 0;
    let mut mk = |name: &'static str, mut data: serde_json::Value| -> (&'static str, serde_json::Value) {
        data["type"] = serde_json::Value::String(name.to_string());
        data["sequence_number"] = serde_json::Value::from(seq);
        seq += 1;
        (name, data)
    };

    let mut events: Vec<(&'static str, serde_json::Value)> = Vec::new();
    events.push(mk("response.created", serde_json::json!({ "response": stub })));

    if let Some(output) = response.get("output").and_then(|o| o.as_array()) {
        for (output_index, item) in output.iter().enumerate() {
            if item.get("type").and_then(|t| t.as_str()) != Some("message") {
                continue;
            }
            let item_id = item.get("id").and_then(|i| i.as_str()).unwrap_or("msg_0").to_string();
            let text: String = item
                .get("content")
                .and_then(|c| c.as_array())
                .map(|parts| {
                    parts
                        .iter()
                        .filter(|p| p.get("type").and_then(|t| t.as_str()) == Some("output_text"))
                        .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                        .collect::<String>()
                })
                .unwrap_or_default();

            events.push(mk(
                "response.output_item.added",
                serde_json::json!({
                    "output_index": output_index,
                    "item": { "type": "message", "id": item_id, "status": "in_progress", "role": "assistant", "content": [] },
                }),
            ));
            events.push(mk(
                "response.output_text.delta",
                serde_json::json!({ "item_id": item_id, "output_index": output_index, "content_index": 0, "delta": text }),
            ));
            events.push(mk(
                "response.output_text.done",
                serde_json::json!({ "item_id": item_id, "output_index": output_index, "content_index": 0, "text": text }),
            ));
            events.push(mk(
                "response.output_item.done",
                serde_json::json!({ "output_index": output_index, "item": item.clone() }),
            ));
        }
    }

    let terminal = if response.get("status").and_then(|s| s.as_str()) == Some("incomplete") {
        "response.incomplete"
    } else {
        "response.completed"
    };
    events.push(mk(terminal, serde_json::json!({ "response": response.clone() })));
    events
}

#[async_trait]
impl<P: PoolProvider + Clone + Send + Sync + 'static> ResponseStore for FusilladeResponseStore<P> {
    async fn store(&self, response: &serde_json::Value) -> Result<String, StoreError> {
        let id = response.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        Ok(id)
    }

    async fn get_context(&self, response_id: &str, owner: &str) -> Result<Option<ResponseContext>, StoreError> {
        let parsed_uuid = parse_response_id(response_id)?;
        let detail = match self.request_manager.get_request_detail(RequestId(parsed_uuid)).await {
            Ok(detail) => detail,
            Err(fusillade::FusilladeError::RequestNotFound(_)) => return Ok(None),
            Err(e) => return Err(StoreError::StorageError(format!("Failed to fetch previous response: {e}"))),
        };
        if detail.created_by != owner {
            return Ok(None);
        }
        let Some(body) = detail.body.as_deref().filter(|body| !body.is_empty()) else {
            return Ok(None);
        };
        if crate::inference::zdr::is_zdr_body(body) {
            return Ok(None);
        }
        let request: serde_json::Value =
            serde_json::from_str(body).map_err(|e| StoreError::SerializationError(format!("Failed to parse previous request: {e}")))?;
        let input = request
            .get("input")
            .cloned()
            .ok_or_else(|| StoreError::SerializationError("Previous request has no input".to_string()))?;
        let response = detail_to_response_object(&detail);
        Ok(Some(ResponseContext { input, response }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_response_id_with_prefix() {
        let uuid = Uuid::new_v4();
        let id = format!("resp_{uuid}");
        let parsed = parse_response_id(&id).unwrap();
        assert_eq!(parsed, uuid);
    }

    #[test]
    fn test_chat_completion_to_stream_chunks_basic() {
        let completion = serde_json::json!({
            "id": "chatcmpl-abc",
            "object": "chat.completion",
            "created": 1_700_000_000,
            "model": "gpt-test",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": "hello world" },
                "finish_reason": "stop",
            }],
            "usage": { "prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7 },
        });

        let chunks = chat_completion_to_stream_chunks(&completion, false);
        assert_eq!(chunks.len(), 3);

        for c in &chunks {
            assert_eq!(c["id"], "chatcmpl-abc");
            assert_eq!(c["object"], "chat.completion.chunk");
            assert_eq!(c["created"], 1_700_000_000_i64);
            assert_eq!(c["model"], "gpt-test");
            assert_eq!(c["choices"][0]["index"], 0);
        }

        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        assert_eq!(chunks[0]["choices"][0]["delta"]["content"], "");
        assert!(chunks[0]["choices"][0]["finish_reason"].is_null());

        assert_eq!(chunks[1]["choices"][0]["delta"]["content"], "hello world");
        assert!(chunks[1]["choices"][0]["finish_reason"].is_null());

        assert_eq!(chunks[2]["choices"][0]["delta"], serde_json::json!({}));
        assert_eq!(chunks[2]["choices"][0]["finish_reason"], "stop");
    }

    #[test]
    fn test_chat_completion_to_stream_chunks_include_usage() {
        let completion = serde_json::json!({
            "id": "chatcmpl-abc",
            "model": "gpt-test",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": "hi" },
                "finish_reason": "stop",
            }],
            "usage": { "total_tokens": 7 },
        });

        let chunks = chat_completion_to_stream_chunks(&completion, true);
        assert_eq!(chunks.len(), 4);
        let usage_chunk = chunks.last().unwrap();
        assert_eq!(usage_chunk["choices"], serde_json::json!([]));
        assert_eq!(usage_chunk["usage"]["total_tokens"], 7);
        for chunk in &chunks[..3] {
            assert_eq!(chunk.get("usage"), Some(&serde_json::Value::Null), "{chunk}");
        }

        let chunks = chat_completion_to_stream_chunks(&completion, false);
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|chunk| chunk.get("usage").is_none()));
    }

    #[test]
    fn test_chat_completion_to_stream_chunks_preserves_service_tier() {
        let completion = serde_json::json!({
            "id": "chatcmpl-flex",
            "model": "gpt-test",
            "service_tier": "flex",
            "choices": [{
                "index": 0,
                "message": { "role": "assistant", "content": "hi" },
                "finish_reason": "stop",
            }],
            "usage": { "total_tokens": 7 },
        });

        let chunks = chat_completion_to_stream_chunks(&completion, true);

        assert!(!chunks.is_empty());
        assert!(chunks.iter().all(|chunk| chunk["service_tier"] == "flex"));
    }

    #[test]
    fn test_response_object_to_stream_events() {
        let response = serde_json::json!({
            "id": "resp_abc",
            "object": "response",
            "status": "completed",
            "model": "gpt-test",
            "created_at": 1_700_000_000_i64,
            "output": [{
                "type": "message",
                "id": "msg_1",
                "role": "assistant",
                "status": "completed",
                "content": [{ "type": "output_text", "text": "hi", "annotations": [] }],
            }],
            "usage": { "total_tokens": 4 },
        });

        let events = response_object_to_stream_events(&response);
        let names: Vec<&str> = events.iter().map(|(n, _)| *n).collect();
        assert_eq!(
            names,
            vec![
                "response.created",
                "response.output_item.added",
                "response.output_text.delta",
                "response.output_text.done",
                "response.output_item.done",
                "response.completed",
            ]
        );

        for (i, (name, data)) in events.iter().enumerate() {
            assert_eq!(data["type"], *name);
            assert_eq!(data["sequence_number"], i as i64);
        }

        assert_eq!(events[0].1["response"]["id"], "resp_abc");
        assert_eq!(events[0].1["response"]["status"], "in_progress");

        assert_eq!(events[1].1["item"]["type"], "message");
        assert_eq!(events[2].1["delta"], "hi");
        assert_eq!(events[2].1["item_id"], events[1].1["item"]["id"]);
        assert_eq!(events[3].1["text"], "hi");

        assert_eq!(events[5].1["response"], response);
        assert_eq!(events[5].1["response"]["usage"]["total_tokens"], 4);
    }

    #[test]
    fn test_chat_completion_to_stream_chunks_tool_calls() {
        let completion = serde_json::json!({
            "id": "chatcmpl-tc",
            "model": "gpt-test",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": serde_json::Value::Null,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": { "name": "get_weather", "arguments": "{}" },
                    }],
                },
                "finish_reason": "tool_calls",
            }],
        });

        let chunks = chat_completion_to_stream_chunks(&completion, false);
        assert_eq!(chunks.len(), 3);
        assert!(chunks[1]["choices"][0]["delta"].get("content").is_none());
        assert_eq!(chunks[1]["choices"][0]["delta"]["tool_calls"][0]["id"], "call_1");
        assert_eq!(chunks[2]["choices"][0]["finish_reason"], "tool_calls");
    }

    #[test]
    fn test_chat_completion_to_stream_chunks_multiple_choices() {
        let completion = serde_json::json!({
            "id": "chatcmpl-multi",
            "model": "gpt-test",
            "choices": [
                {
                    "index": 0,
                    "message": { "role": "assistant", "content": "first" },
                    "finish_reason": "stop",
                },
                {
                    "index": 1,
                    "message": { "role": "assistant", "content": "second" },
                    "finish_reason": "length",
                },
            ],
        });

        let chunks = chat_completion_to_stream_chunks(&completion, false);
        assert_eq!(chunks.len(), 6);

        assert_eq!(chunks[0]["choices"][0]["index"], 0);
        assert_eq!(chunks[0]["choices"][0]["delta"]["role"], "assistant");
        assert_eq!(chunks[1]["choices"][0]["index"], 0);
        assert_eq!(chunks[1]["choices"][0]["delta"]["content"], "first");
        assert_eq!(chunks[2]["choices"][0]["index"], 0);
        assert_eq!(chunks[2]["choices"][0]["finish_reason"], "stop");

        assert_eq!(chunks[3]["choices"][0]["index"], 1);
        assert_eq!(chunks[3]["choices"][0]["delta"]["role"], "assistant");
        assert_eq!(chunks[4]["choices"][0]["index"], 1);
        assert_eq!(chunks[4]["choices"][0]["delta"]["content"], "second");
        assert_eq!(chunks[5]["choices"][0]["index"], 1);
        assert_eq!(chunks[5]["choices"][0]["finish_reason"], "length");
    }

    #[test]
    fn test_response_object_to_stream_events_multiple_items() {
        let response = serde_json::json!({
            "id": "resp_multi",
            "object": "response",
            "status": "completed",
            "model": "gpt-test",
            "created_at": 1_700_000_000_i64,
            "output": [
                {
                    "type": "message",
                    "id": "msg_1",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{ "type": "output_text", "text": "alpha", "annotations": [] }],
                },
                {
                    "type": "message",
                    "id": "msg_2",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{ "type": "output_text", "text": "beta", "annotations": [] }],
                },
            ],
            "usage": { "total_tokens": 9 },
        });

        let events = response_object_to_stream_events(&response);
        let names: Vec<&str> = events.iter().map(|(n, _)| *n).collect();
        assert_eq!(
            names,
            vec![
                "response.created",
                "response.output_item.added",
                "response.output_text.delta",
                "response.output_text.done",
                "response.output_item.done",
                "response.output_item.added",
                "response.output_text.delta",
                "response.output_text.done",
                "response.output_item.done",
                "response.completed",
            ]
        );

        for (i, (_, data)) in events.iter().enumerate() {
            assert_eq!(data["sequence_number"], i as i64);
        }

        assert_eq!(events[1].1["item"]["id"], "msg_1");
        assert_eq!(events[2].1["delta"], "alpha");
        assert_eq!(events[2].1["output_index"], 0);
        assert_eq!(events[5].1["item"]["id"], "msg_2");
        assert_eq!(events[6].1["delta"], "beta");
        assert_eq!(events[6].1["output_index"], 1);
    }

    #[test]
    fn test_parse_response_id_without_prefix() {
        let uuid = Uuid::new_v4();
        let parsed = parse_response_id(&uuid.to_string()).unwrap();
        assert_eq!(parsed, uuid);
    }

    #[test]
    fn test_parse_response_id_invalid() {
        let result = parse_response_id("not-a-uuid");
        assert!(result.is_err());
        assert!(matches!(result, Err(StoreError::NotFound(_))));
    }

    #[test]
    fn test_state_to_status_mapping() {
        assert_eq!(state_to_status("pending"), "queued");
        assert_eq!(state_to_status("claimed"), "in_progress");
        assert_eq!(state_to_status("processing"), "in_progress");
        assert_eq!(state_to_status("completed"), "completed");
        assert_eq!(state_to_status("failed"), "failed");
        assert_eq!(state_to_status("canceled"), "cancelled");
        assert_eq!(state_to_status("unknown"), "failed");
    }

    #[test]
    fn test_store_extracts_id_from_response() {
        let response = serde_json::json!({
            "id": "resp_12345678-1234-1234-1234-123456789abc",
            "status": "completed",
        });
        let id = response.get("id").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(id, "resp_12345678-1234-1234-1234-123456789abc");
    }

    #[test]
    fn test_store_handles_missing_id() {
        let response = serde_json::json!({"status": "completed"});
        let id = response.get("id").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(id, "");
    }

    #[test]
    fn test_extract_upstream_error_openai_format() {
        let body = r#"{"error":{"message":"Forbidden","type":"invalid_request_error","param":null,"code":"forbidden"}}"#;
        let (error_type, message) = extract_upstream_error(403, body);
        assert_eq!(error_type, "permission_error");
        assert_eq!(message, "Forbidden");
    }

    #[test]
    fn test_extract_upstream_error_rate_limit() {
        let body = r#"{"error":{"message":"Rate limit exceeded","type":"rate_limit_error","code":"rate_limit"}}"#;
        let (error_type, message) = extract_upstream_error(429, body);
        assert_eq!(error_type, "rate_limit_error");
        assert_eq!(message, "Rate limit exceeded");
    }

    #[test]
    fn test_extract_upstream_error_plain_text() {
        let (error_type, message) = extract_upstream_error(402, "Account balance too low");
        assert_eq!(error_type, "insufficient_credits");
        assert_eq!(message, "Account balance too low");
    }

    #[test]
    fn test_extract_upstream_error_server_error() {
        let body = r#"{"error":{"message":"Internal error"}}"#;
        let (error_type, message) = extract_upstream_error(500, body);
        assert_eq!(error_type, "server_error");
        assert_eq!(message, "Internal error");
    }

    #[test]
    fn test_parse_failure_error_legacy_format() {
        let err = r#"{"type":"NonRetriableHttpStatus","details":{"status":403,"body":"{\"error\":{\"message\":\"Forbidden\",\"type\":\"invalid_request_error\",\"param\":null,\"code\":\"forbidden\"}}"}}"#;
        let (error_type, message, status_code) = parse_failure_error(err);
        assert_eq!(error_type, "permission_error");
        assert_eq!(message, "Forbidden");
        assert_eq!(status_code, Some(403));
    }

    #[test]
    fn test_parse_failure_error_plain_string() {
        let (error_type, message, status_code) = parse_failure_error("some unknown error");
        assert_eq!(error_type, "server_error");
        assert_eq!(message, "some unknown error");
        assert_eq!(status_code, None);
    }

    #[test]
    fn test_parse_failure_error_legacy_upstream_returned_format() {
        let err =
            r#"Upstream returned 403: {"error":{"message":"Forbidden","type":"invalid_request_error","param":null,"code":"forbidden"}}"#;
        let (error_type, message, status_code) = parse_failure_error(err);
        assert_eq!(error_type, "permission_error");
        assert_eq!(message, "Forbidden");
        assert_eq!(status_code, Some(403));
    }

    #[test]
    fn test_status_to_error_type_mapping() {
        assert_eq!(status_to_error_type(400), "invalid_request_error");
        assert_eq!(status_to_error_type(401), "authentication_error");
        assert_eq!(status_to_error_type(402), "insufficient_credits");
        assert_eq!(status_to_error_type(403), "permission_error");
        assert_eq!(status_to_error_type(404), "not_found_error");
        assert_eq!(status_to_error_type(429), "rate_limit_error");
        assert_eq!(status_to_error_type(500), "server_error");
        assert_eq!(status_to_error_type(503), "server_error");
    }

    fn make_detail(
        status: &str,
        response_status: Option<i16>,
        response_body: Option<&str>,
        error: Option<&str>,
    ) -> fusillade::RequestDetail {
        fusillade::RequestDetail {
            id: Uuid::new_v4(),
            batch_id: None,
            model: "test-model".to_string(),
            status: status.to_string(),
            created_at: chrono::Utc::now(),
            completed_at: None,
            failed_at: None,
            duration_ms: None,
            response_status,
            body: None,
            response_body: response_body.map(|s| s.to_string()),
            error: error.map(|s| s.to_string()),
            service_tier: None,
            created_by: "test-user".to_string(),
        }
    }

    #[test]
    fn truncated_flex_response_stays_incomplete() {
        let body = r#"{"object":"response","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[],"usage":{"total_tokens":4}}"#;
        let mut detail = make_detail("completed", Some(200), Some(body), None);
        detail.completed_at = Some(chrono::Utc::now());

        let response = detail_to_response_object(&detail);

        assert_eq!(response["status"], "incomplete");
        assert_eq!(response["incomplete_details"]["reason"], "max_output_tokens");
        assert!(response["completed_at"].is_null());
        let events = response_object_to_stream_events(&response);
        assert_eq!(events.last().map(|(name, _)| *name), Some("response.incomplete"));
        assert!(events.iter().all(|(name, _)| *name != "response.completed"));
    }

    #[test]
    fn background_request_detail_preserves_async_tier_in_response_object() {
        let mut detail = make_detail("pending", None, None, None);
        detail.body = Some(r#"{"model":"test-model","input":"hello"}"#.to_string());
        detail.service_tier = Some("background".to_string());

        let response = detail_to_response_object(&detail);

        assert_eq!(response["status"], "queued");
        assert_eq!(response["background"], true);
        assert_eq!(response["service_tier"], "background");
    }

    #[test]
    fn test_detail_to_chat_completion_completed_success_passes_through() {
        let body = r#"{"id":"chatcmpl-abc","object":"chat.completion","choices":[{"message":{"role":"assistant","content":"hi"}}]}"#;
        let detail = make_detail("completed", Some(200), Some(body), None);
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 200);
        assert_eq!(value["id"], "chatcmpl-abc");
        assert_eq!(value["object"], "chat.completion");
        assert!(value.get("error").is_none());
    }

    #[test]
    fn test_detail_to_chat_completion_completed_reports_recorded_service_tier() {
        let body = r#"{"id":"chatcmpl-flex","object":"chat.completion","service_tier":"default","choices":[]}"#;
        let mut detail = make_detail("completed", Some(200), Some(body), None);
        detail.service_tier = Some("flex".to_string());

        let (status, value) = detail_to_chat_completion_object(&detail);

        assert_eq!(status, 200);
        assert_eq!(value["service_tier"], "flex");
    }

    #[test]
    fn test_detail_to_chat_completion_completed_2xx_upstream_status_passes_through() {
        let body = r#"{"id":"chatcmpl-y","object":"chat.completion","choices":[]}"#;
        let detail = make_detail("completed", Some(201), Some(body), None);
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 201);
        assert_eq!(value["id"], "chatcmpl-y");
        assert!(value.get("error").is_none());
    }

    #[test]
    fn test_detail_to_chat_completion_completed_no_response_status_assumes_200() {
        let body = r#"{"id":"chatcmpl-x","object":"chat.completion","choices":[]}"#;
        let detail = make_detail("completed", None, Some(body), None);
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 200);
        assert_eq!(value["id"], "chatcmpl-x");
    }

    #[test]
    fn test_detail_to_chat_completion_completed_with_upstream_429_surfaces_status() {
        let body = r#"{"error":{"message":"Rate limit exceeded","type":"rate_limit_error"}}"#;
        let detail = make_detail("completed", Some(429), Some(body), None);
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 429);
        assert_eq!(value["error"]["message"], "Rate limit exceeded");
        assert_eq!(value["error"]["type"], "rate_limit_error");
        assert_eq!(value["error"]["code"], 429);
    }

    #[test]
    fn test_detail_to_chat_completion_completed_with_upstream_403_extracts_message() {
        let body = r#"{"error":{"message":"Forbidden","type":"invalid_request_error"}}"#;
        let detail = make_detail("completed", Some(403), Some(body), None);
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 403);
        assert_eq!(value["error"]["message"], "Forbidden");
        assert_eq!(value["error"]["type"], "permission_error");
        assert_eq!(value["error"]["code"], 403);
    }

    #[test]
    fn test_detail_to_chat_completion_completed_missing_body_returns_500() {
        let detail = make_detail("completed", Some(200), None, None);
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 500);
        assert_eq!(value["error"]["type"], "server_error");
    }

    #[test]
    fn test_detail_to_chat_completion_completed_invalid_json_body_returns_500() {
        let detail = make_detail("completed", Some(200), Some("not json"), None);
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 500);
        assert_eq!(value["error"]["type"], "server_error");
    }

    #[test]
    fn test_detail_to_chat_completion_failed_with_failure_reason_envelope() {
        let err = r#"{"type":"NonRetriableHttpStatus","details":{"status":403,"body":"{\"error\":{\"message\":\"Forbidden\",\"type\":\"invalid_request_error\"}}"}}"#;
        let detail = make_detail("failed", None, None, Some(err));
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 403);
        assert_eq!(value["error"]["message"], "Forbidden");
        assert_eq!(value["error"]["type"], "permission_error");
        assert_eq!(value["error"]["code"], 403);
    }

    #[test]
    fn test_detail_to_chat_completion_failed_with_legacy_upstream_returned_format() {
        let err = r#"Upstream returned 500: {"error":{"message":"Internal error"}}"#;
        let detail = make_detail("failed", None, None, Some(err));
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 500);
        assert_eq!(value["error"]["message"], "Internal error");
        assert_eq!(value["error"]["code"], 500);
    }

    #[test]
    fn test_detail_to_chat_completion_failed_with_no_error_field() {
        let detail = make_detail("failed", None, None, None);
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 500);
        assert_eq!(value["error"]["type"], "server_error");
        assert_eq!(value["error"]["message"], "Request failed");
    }

    #[test]
    fn test_detail_to_chat_completion_canceled_returns_499() {
        let detail = make_detail("canceled", None, None, None);
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 499);
        assert_eq!(value["error"]["type"], "request_canceled");
        assert_eq!(value["error"]["code"], 499);
    }

    #[test]
    fn test_detail_to_chat_completion_non_terminal_state_returns_500() {
        let detail = make_detail("processing", None, None, None);
        let (status, value) = detail_to_chat_completion_object(&detail);
        assert_eq!(status, 500);
        assert_eq!(value["error"]["type"], "server_error");
    }
}
