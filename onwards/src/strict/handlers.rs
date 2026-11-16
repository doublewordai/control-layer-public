
use super::schema_mismatch::{self, SchemaMismatch};
use super::schemas::chat_completions::{
    ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse,
    generated_chat_completion_id, normalize_chat_completion_chunk_value,
    normalize_chat_completion_response_value,
};
use super::schemas::completions::{
    CompletionChunk, CompletionRequest, CompletionResponse, generated_completion_id,
    normalize_completion_chunk_value, normalize_completion_response_value,
};
use super::schemas::embeddings::{EmbeddingsRequest, EmbeddingsResponse};
use super::schemas::responses::{
    ResponsesRequest, ResponsesResponse, ResponsesStreamingEvent, generated_response_id,
    normalize_responses_response_value, normalize_responses_streaming_event_value,
};
use super::stream_end;
use crate::AppState;
use crate::client::HttpClient;
use crate::errors::OnwardsErrorResponse;
use crate::extract_model_from_request;
use crate::handlers::{
    AuthenticatedApiKeyId, ResolvedTrust, UpstreamFailureLabels, target_message_handler,
};
use crate::rejections::RejectionContext;
use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequest, State};
use axum::http::{Extensions, HeaderMap, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use serde_json::json;
use tracing::{debug, error, warn};

const MAX_ERROR_BODY: usize = 64 * 1024;

struct ForwardResult {
    response: Response,
    trusted: bool,
    attribution: Attribution,
    internal_error: bool,
}

struct Attribution {
    authenticated_api_key_id: Option<AuthenticatedApiKeyId>,
    identity: Option<crate::RequestIdentity>,
    internal: bool,
}

impl Attribution {
    fn of(response: &Response, internal: bool) -> Self {
        Self {
            authenticated_api_key_id: response
                .extensions()
                .get::<AuthenticatedApiKeyId>()
                .copied(),
            identity: response
                .extensions()
                .get::<crate::RequestIdentity>()
                .cloned(),
            internal,
        }
    }

    fn apply(self, mut response: Response) -> Response {
        let extensions = response.extensions_mut();
        if let Some(api_key_id) = self.authenticated_api_key_id {
            extensions.insert(api_key_id);
        }
        if let Some(identity) = self.identity {
            extensions.insert(identity);
        }
        if !self.internal
            && response.status().as_u16() >= 400
            && response.extensions().get::<crate::ErrorOrigin>().is_none()
        {
            response
                .extensions_mut()
                .insert(crate::ErrorOrigin::Provider);
        }
        response
    }
}

fn is_sse_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .map(str::trim)
        .map(|value| value.eq_ignore_ascii_case("text/event-stream"))
        .unwrap_or(false)
}

fn response_is_sse(response: &Response) -> bool {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(is_sse_content_type)
        .unwrap_or(false)
}

pub async fn models_handler<T: HttpClient + Clone + Send + Sync + 'static>(
    State(state): State<AppState<T>>,
    req: Request<Body>,
) -> impl IntoResponse {
    crate::handlers::models(State(state), req).await
}

#[allow(clippy::result_large_err)]
fn parse_strict_request<T: serde::de::DeserializeOwned, C: HttpClient>(
    state: &AppState<C>,
    caller: Caller<'_>,
    body: &[u8],
) -> Result<T, Response> {
    serde_json::from_slice::<T>(body).map_err(|e| {
        let model = extract_model_from_request(caller.headers, body);
        let message = format!("Invalid request: {e}");
        if e.is_data() {
            refused_mismatch(
                state,
                caller,
                model.as_deref(),
                StatusCode::UNPROCESSABLE_ENTITY,
                schema_mismatch::locate::<T>(body),
                &message,
            )
        } else {
            refused_invalid(
                state,
                caller,
                model.as_deref(),
                StatusCode::BAD_REQUEST,
                "invalid_json",
                &message,
            )
        }
    })
}

#[derive(Clone, Copy)]
struct Caller<'a> {
    headers: &'a HeaderMap,
    key: Option<crate::auth::KeyHash>,
}

impl<'a> Caller<'a> {
    fn new(headers: &'a HeaderMap, extensions: &Extensions) -> Self {
        Self {
            headers,
            key: crate::auth::presented_key_from(headers, extensions),
        }
    }
}

fn rejection_context<C: HttpClient>(
    state: &AppState<C>,
    caller: Caller<'_>,
    model: Option<&str>,
) -> RejectionContext {
    let mut rejection = RejectionContext::from_request(state, caller.headers, caller.key);
    if let Some(model) = model {
        rejection.set_model_if_configured(&state.targets, model);
    }
    rejection
}

fn rejected<C: HttpClient>(
    state: &AppState<C>,
    caller: Caller<'_>,
    model: Option<&str>,
    error: OnwardsErrorResponse,
) -> Response {
    let rejection = rejection_context(state, caller, model);
    rejection.record_error(&error);
    error.with_identity(rejection.identity()).into_response()
}

fn refused_invalid<C: HttpClient>(
    state: &AppState<C>,
    caller: Caller<'_>,
    model: Option<&str>,
    status: StatusCode,
    code: &str,
    message: &str,
) -> Response {
    refused(state, caller, model, status, code, None, message)
}

fn refused_mismatch<C: HttpClient>(
    state: &AppState<C>,
    caller: Caller<'_>,
    model: Option<&str>,
    status: StatusCode,
    mismatch: Option<SchemaMismatch>,
    message: &str,
) -> Response {
    refused(
        state,
        caller,
        model,
        status,
        "schema_mismatch",
        mismatch.as_ref(),
        message,
    )
}

fn refused<C: HttpClient>(
    state: &AppState<C>,
    caller: Caller<'_>,
    model: Option<&str>,
    status: StatusCode,
    code: &str,
    mismatch: Option<&SchemaMismatch>,
    message: &str,
) -> Response {
    let param = mismatch.and_then(|mismatch| mismatch.path.as_deref());
    let rejection = rejection_context(state, caller, model);
    rejection.record_with_reason(
        status,
        code,
        param,
        mismatch.map(|mismatch| mismatch.reason),
    );
    let mut response = error_response_with_param(status, "invalid_request_error", message, param);
    if let Some(identity) = rejection.identity() {
        response.extensions_mut().insert(identity);
    }
    response
        .extensions_mut()
        .insert(crate::ErrorOrigin::Gateway {
            code: code.to_string(),
        });
    response
}

pub async fn chat_completions_handler<T: HttpClient + Clone + Send + Sync + 'static>(
    State(state): State<AppState<T>>,
    req: Request<Body>,
) -> Response {
    let headers = req.headers().clone();
    let extensions = req.extensions().clone();
    let caller = Caller::new(&headers, &extensions);
    let body_bytes = match axum::body::to_bytes(req.into_body(), state.body_limit).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return rejected(
                &state,
                caller,
                None,
                OnwardsErrorResponse::payload_too_large(state.body_limit),
            );
        }
    };

    let request: ChatCompletionRequest = match parse_strict_request(&state, caller, &body_bytes) {
        Ok(request) => request,
        Err(response) => return response,
    };

    let original_model = request.model.clone();
    let is_streaming = request.stream.unwrap_or(false);

    debug!(
        model = %original_model,
        messages_count = request.messages.len(),
        stream = is_streaming,
        "Chat completions request validated"
    );

    let resolved_model =
        extract_model_from_request(&headers, &body_bytes).unwrap_or(original_model.clone());
    let sse_buffer_limit = state.sse_buffer_limit;
    let response_id_override = caller_response_id(&state, &headers);
    let ForwardResult {
        response,
        trusted,
        attribution,
        internal_error,
    } = forward_request(
        state,
        headers,
        extensions,
        "/chat/completions",
        body_bytes.to_vec(),
    )
    .await;

    let response = if response.status().is_success() {
        let response_is_sse = response_is_sse(&response);

        if response_is_sse && !is_streaming {
            debug!(
                model = %resolved_model,
                "Using streaming chat sanitizer because upstream returned SSE"
            );
        }

        if is_streaming || response_is_sse {
            sanitize_streaming_chat_response(
                response,
                resolved_model,
                trusted,
                sse_buffer_limit,
                response_id_override,
                request.n.unwrap_or(1),
            )
            .await
        } else {
            sanitize_chat_response(response, resolved_model).await
        }
    } else if internal_error {
        response
    } else if trusted {
        debug!(model = %resolved_model, "Bypassing error sanitization for trusted provider");
        envelope_trusted_error(response).await
    } else {
        sanitize_error_response(response).await
    };
    attribution.apply(response)
}

pub async fn responses_handler<T: HttpClient + Clone + Send + Sync + 'static>(
    State(state): State<AppState<T>>,
    headers: HeaderMap,
    req: Request<Body>,
) -> Response {
    if req.uri().path().ends_with("/chat/completions") {
        return chat_completions_handler(State(state), req).await;
    }

    let extensions = req.extensions().clone();
    let caller = Caller::new(&headers, &extensions);
    let body_bytes = match Bytes::from_request(req, &state).await {
        Ok(bytes) => bytes,
        Err(e) => {
            error!(error = %e, "Failed to read responses request");
            return refused_invalid(
                &state,
                caller,
                None,
                StatusCode::BAD_REQUEST,
                "invalid_body",
                &format!("Invalid request: {}", e),
            );
        }
    };
    let mut json_request = Request::new(Body::from(body_bytes.clone()));
    *json_request.headers_mut() = headers.clone();
    *json_request.extensions_mut() = extensions.clone();
    let request: ResponsesRequest = match Json::from_request(json_request, &state).await {
        Ok(Json(r)) => r,
        Err(e) => {
            let model = extract_model_from_request(&headers, &body_bytes);
            let message = format!("Invalid request: {e}");
            let (status, code) = match e {
                JsonRejection::JsonDataError(_) => {
                    return refused_mismatch(
                        &state,
                        caller,
                        model.as_deref(),
                        StatusCode::BAD_REQUEST,
                        schema_mismatch::locate::<ResponsesRequest>(&body_bytes),
                        &message,
                    );
                }
                JsonRejection::MissingJsonContentType(_) => {
                    (StatusCode::UNSUPPORTED_MEDIA_TYPE, "invalid_content_type")
                }
                _ => (StatusCode::BAD_REQUEST, "invalid_json"),
            };
            return refused_invalid(&state, caller, model.as_deref(), status, code, &message);
        }
    };

    debug!(
        model = %request.model,
        has_previous_response_id = request.previous_response_id.is_some(),
        stream = ?request.stream,
        background = ?request.background,
        service_tier = ?request.service_tier,
        "Responses request validated"
    );

    let response_id_override = caller_response_id(&state, &headers);

    let request_value = match serde_json::to_value(&request) {
        Ok(v) => v,
        Err(e) => {
            error!(error = %e, "Failed to serialize responses request");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "Failed to process request",
            );
        }
    };
    if let Err(error) = crate::reasoning::validate_canonical_reasoning("/responses", &request_value)
    {
        return rejected(
            &state,
            caller,
            Some(&request.model),
            OnwardsErrorResponse::reasoning(&error),
        );
    }

    let original_model = request.model.clone();
    let is_streaming = request.stream.unwrap_or(false);

    let mut request = request;
    if let Some(ref mut tools) = request.tools {
        let (flattened, dropped) = flatten_tool_namespaces(std::mem::take(tools));
        *tools = flattened;
        if let Some(super::schemas::responses::ToolChoice::Specific {
            name: Some(target), ..
        }) = &request.tool_choice
            && dropped.contains(target)
        {
            warn!(
                tool = %target,
                "tool_choice named a freeform tool that was dropped; falling back to auto"
            );
            request.tool_choice = Some(super::schemas::responses::ToolChoice::Mode(
                "auto".to_string(),
            ));
        }
    }

    if let Some(ref mut tools) = request.tools {
        for tool in tools.iter_mut() {
            if let super::schemas::responses::Tool::Function { parameters, .. } = tool
                && let Some(obj) = parameters.as_object_mut()
                && !obj.contains_key("additionalProperties")
            {
                obj.insert(
                    "additionalProperties".to_string(),
                    serde_json::Value::Bool(false),
                );
            }
        }
    }

    let body_bytes = match serde_json::to_vec(&request) {
        Ok(bytes) => bytes,
        Err(e) => {
            error!(error = %e, "Failed to serialize responses request");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "Failed to process request",
            );
        }
    };

    let resolved_model =
        extract_model_from_request(&headers, &body_bytes).unwrap_or(original_model.clone());
    let ForwardResult {
        response,
        trusted,
        attribution,
        internal_error,
    } = forward_request(state.clone(), headers, extensions, "/responses", body_bytes).await;

    let response = if response.status().is_success() {
        let response_is_sse = response_is_sse(&response);

        if is_streaming || response_is_sse {
            sanitize_streaming_responses_response(
                response,
                resolved_model,
                trusted,
                response_id_override,
                state.sse_buffer_limit,
            )
            .await
        } else {
            sanitize_responses_response(response, resolved_model, response_id_override).await
        }
    } else if internal_error {
        response
    } else if trusted {
        debug!(model = %resolved_model, "Bypassing error sanitization for trusted provider");
        envelope_trusted_error(response).await
    } else {
        sanitize_error_response(response).await
    };
    attribution.apply(response)
}

pub async fn embeddings_handler<T: HttpClient + Clone + Send + Sync + 'static>(
    State(state): State<AppState<T>>,
    req: Request<Body>,
) -> Response {
    let headers = req.headers().clone();
    let extensions = req.extensions().clone();
    let caller = Caller::new(&headers, &extensions);
    let body_bytes = match axum::body::to_bytes(req.into_body(), state.body_limit).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return rejected(
                &state,
                caller,
                None,
                OnwardsErrorResponse::payload_too_large(state.body_limit),
            );
        }
    };

    let request: EmbeddingsRequest = match parse_strict_request(&state, caller, &body_bytes) {
        Ok(request) => request,
        Err(response) => return response,
    };

    let original_model = request.model.clone();

    debug!(
        model = %original_model,
        "Embeddings request validated"
    );

    let resolved_model =
        extract_model_from_request(&headers, &body_bytes).unwrap_or(original_model.clone());
    let ForwardResult {
        response,
        trusted,
        attribution,
        internal_error,
    } = forward_request(
        state,
        headers,
        extensions,
        "/embeddings",
        body_bytes.to_vec(),
    )
    .await;

    let response = if response.status().is_success() {
        sanitize_embeddings_response(response, resolved_model).await
    } else if internal_error {
        response
    } else if trusted {
        debug!(model = %resolved_model, "Bypassing error sanitization for trusted provider");
        envelope_trusted_error(response).await
    } else {
        sanitize_error_response(response).await
    };
    attribution.apply(response)
}

pub async fn completions_handler<T: HttpClient + Clone + Send + Sync + 'static>(
    State(state): State<AppState<T>>,
    req: Request<Body>,
) -> Response {
    let headers = req.headers().clone();
    let extensions = req.extensions().clone();
    let caller = Caller::new(&headers, &extensions);
    let body_bytes = match axum::body::to_bytes(req.into_body(), state.body_limit).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return rejected(
                &state,
                caller,
                None,
                OnwardsErrorResponse::payload_too_large(state.body_limit),
            );
        }
    };

    let request: CompletionRequest = match parse_strict_request(&state, caller, &body_bytes) {
        Ok(request) => request,
        Err(response) => return response,
    };

    let unsupported_reasoning_param = [
        (request.reasoning_effort.is_some(), "reasoning_effort"),
        (request.reasoning.is_some(), "reasoning"),
        (request.thinking.is_some(), "thinking"),
        (
            request.thinking_token_budget.is_some(),
            "thinking_token_budget",
        ),
        (
            request.chat_template_kwargs.is_some(),
            "chat_template_kwargs",
        ),
    ]
    .into_iter()
    .find_map(|(present, param)| present.then_some(param));
    if let Some(param) = unsupported_reasoning_param {
        let message = format!(
            "Parameter '{param}' is not supported by /v1/completions. Use /v1/chat/completions or /v1/responses."
        );
        return rejected(
            &state,
            caller,
            Some(&request.model),
            OnwardsErrorResponse::invalid_request(&message, Some(param), "unsupported_parameter"),
        );
    }

    let original_model = request.model.clone();
    let is_streaming = request.stream.unwrap_or(false);

    debug!(
        model = %original_model,
        stream = is_streaming,
        "Completions request validated"
    );

    let resolved_model =
        extract_model_from_request(&headers, &body_bytes).unwrap_or(original_model);
    let sse_buffer_limit = state.sse_buffer_limit;
    let response_id_override = caller_response_id(&state, &headers);
    let ForwardResult {
        response,
        trusted,
        attribution,
        internal_error,
    } = forward_request(
        state,
        headers,
        extensions,
        "/completions",
        body_bytes.to_vec(),
    )
    .await;

    let response = if response.status().is_success() {
        let response_is_sse = response_is_sse(&response);

        if is_streaming || response_is_sse {
            sanitize_streaming_completions_response(
                response,
                resolved_model,
                trusted,
                sse_buffer_limit,
                response_id_override,
                request.n.unwrap_or(1),
            )
            .await
        } else {
            sanitize_completions_response(response, resolved_model).await
        }
    } else if internal_error {
        response
    } else if trusted {
        debug!(model = %resolved_model, "Bypassing error sanitization for trusted provider");
        envelope_trusted_error(response).await
    } else {
        sanitize_error_response(response).await
    };
    attribution.apply(response)
}

async fn forward_request<T: HttpClient + Clone + Send + Sync + 'static>(
    state: AppState<T>,
    mut headers: HeaderMap,
    extensions: Extensions,
    path: &str,
    body_bytes: Vec<u8>,
) -> ForwardResult {
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );

    headers.remove(axum::http::header::ACCEPT_ENCODING);

    let mut request_builder = Request::builder().method("POST").uri(path);

    for (name, value) in headers.iter() {
        request_builder = request_builder.header(name, value);
    }

    let mut request = match request_builder.body(Body::from(body_bytes)) {
        Ok(req) => req,
        Err(e) => {
            error!(error = %e, "Failed to build request");
            let response = error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                "Failed to build request",
            );
            return ForwardResult {
                response,
                trusted: false,
                attribution: Attribution {
                    authenticated_api_key_id: None,
                    identity: None,
                    internal: true,
                },
                internal_error: true,
            };
        }
    };

    request.extensions_mut().extend(extensions);

    let (response, internal_error) = match target_message_handler(State(state), request).await {
        Ok(response) => (response, false),
        Err(err) => (err.into_response(), true),
    };

    let trusted = response
        .extensions()
        .get::<ResolvedTrust>()
        .map(|t| t.0)
        .unwrap_or(false);
    let attribution = Attribution::of(&response, internal_error);

    ForwardResult {
        response,
        trusted,
        attribution,
        internal_error,
    }
}

fn error_response(status: StatusCode, error_type: &str, message: &str) -> Response {
    error_response_with_param(status, error_type, message, None)
}

fn error_response_with_param(
    status: StatusCode,
    error_type: &str,
    message: &str,
    param: Option<&str>,
) -> Response {
    let body = json!({
        "error": {
            "message": message,
            "type": error_type,
            "param": param,
            "code": null
        }
    });

    (status, Json(body)).into_response()
}

fn caller_response_id<T: HttpClient + Clone + Send + Sync + 'static>(
    state: &AppState<T>,
    headers: &HeaderMap,
) -> Option<String> {
    state
        .response_id_header
        .as_deref()
        .and_then(|name| crate::response_id::extract_override_id(headers, name))
}

async fn sanitize_chat_response(mut response: Response, original_model: String) -> Response {
    let body_bytes =
        match axum::body::to_bytes(std::mem::take(response.body_mut()), usize::MAX).await {
            Ok(bytes) => {
                debug!(
                    bytes_read = bytes.len(),
                    "Read upstream response body for sanitization"
                );
                bytes
            }
            Err(e) => {
                error!(error = %e, "Failed to read response body for sanitization");
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "Failed to read upstream response",
                );
            }
        };

    let mut raw_response: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(resp) => resp,
        Err(e) => {
            error!(
                error = %e,
                response_len = body_bytes.len(),
                "Failed to deserialize chat response from provider, returning standard error"
            );
            return error_response(StatusCode::BAD_GATEWAY, "api_error", "Bad gateway");
        }
    };

    normalize_chat_completion_response_value(&mut raw_response, &original_model);

    let mut chat_response: ChatCompletionResponse = match serde_json::from_value(raw_response) {
        Ok(resp) => resp,
        Err(e) => {
            error!(
                error = %e,
                "Failed to coerce chat response into strict schema"
            );
            return error_response(StatusCode::BAD_GATEWAY, "api_error", "Bad gateway");
        }
    };

    chat_response.model = original_model;

    for choice in &mut chat_response.choices {
        choice.message.canonicalise_reasoning();
    }

    match serde_json::to_vec(&chat_response) {
        Ok(sanitized_bytes) => {
            let content_length = sanitized_bytes.len();
            debug!(
                content_length = content_length,
                "Setting sanitized response body"
            );
            *response.body_mut() = Body::from(sanitized_bytes);

            response
                .headers_mut()
                .remove(axum::http::header::TRANSFER_ENCODING);
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                header::HeaderValue::from(content_length),
            );
            debug!("Sanitized non-streaming chat completion response");
            response
        }
        Err(e) => {
            error!(
                error = %e,
                "Failed to serialize sanitized chat response, returning standard error"
            );
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                "Internal server error",
            )
        }
    }
}

async fn sanitize_streaming_chat_response(
    mut response: Response,
    original_model: String,
    trusted: bool,
    sse_buffer_limit: usize,
    response_id_override: Option<String>,
    expected_choices: u32,
) -> Response {
    let body_stream =
        http_body_util::BodyExt::into_data_stream(std::mem::take(response.body_mut()));
    let buffered_stream = crate::sse::SseBufferedStream::with_limit(body_stream, sse_buffer_limit);
    let stream_fallback_id = generated_chat_completion_id();

    let sanitized_stream = stream_end::end_explicitly(
        buffered_stream,
        stream_end::StreamProgress::new(expected_choices),
        move |chunk, progress| {
            let chunk_str = String::from_utf8_lossy(&chunk);
            let mut sanitized_lines = Vec::new();

            for line in chunk_str.lines() {
                if let Some(data_part) = line.strip_prefix("data: ") {

                    if data_part.trim() == "[DONE]" {
                        progress.record_end();
                        sanitized_lines.push(line.to_string());
                        continue;
                    }

                    let mut raw_chunk: serde_json::Value = match serde_json::from_str(data_part) {
                        Ok(chunk) => chunk,
                        Err(e) => {
                            error!(
                                error = %e,
                                data_len = data_part.len(),
                                "Failed to parse SSE data line from provider, terminating stream"
                            );
                            return Err(std::io::Error::other("Malformed SSE data from provider"));
                        }
                    };

                    if let Some(error_event) = try_format_sse_error(&raw_chunk, trusted) {
                        error!(
                            data_len = data_part.len(),
                            "Provider returned error envelope in SSE chunk, forwarding as error event"
                        );
                        progress.record_end();
                        sanitized_lines.push(error_event);
                        continue;
                    }

                    normalize_chat_completion_chunk_value(
                        &mut raw_chunk,
                        &original_model,
                        &stream_fallback_id,
                    );

                    match serde_json::from_value::<ChatCompletionChunk>(raw_chunk) {
                        Ok(mut chunk_data) => {
                            for choice in &chunk_data.choices {
                                progress
                                    .record_choice(choice.index, choice.finish_reason.is_some());
                            }
                            if chunk_data.usage.is_some() && chunk_data.choices.is_empty() {
                                progress.record_final_usage();
                            }

                            chunk_data.model = original_model.clone();
                            if let Some(id) = &response_id_override {
                                chunk_data.id.clone_from(id);
                            }

                            for choice in &mut chunk_data.choices {
                                choice.delta.canonicalise_reasoning();
                            }

                            match serde_json::to_string(&chunk_data) {
                                Ok(sanitized_json) => {
                                    sanitized_lines.push(format!("data: {}", sanitized_json));
                                }
                                Err(e) => {
                                    error!(error = %e, "Failed to serialize chunk, terminating stream");
                                    return Err(std::io::Error::other("Failed to serialize chunk"));
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                data_len = data_part.len(),
                                "Failed to parse SSE data line from provider, terminating stream"
                            );
                            return Err(std::io::Error::other("Malformed SSE data from provider"));
                        }
                    }
                } else if line.is_empty() {
                    sanitized_lines.push(String::new());
                }
            }

            let mut sanitized_chunk = sanitized_lines.join("\n");

            let input_trailing = chunk_str.chars().rev().take_while(|&c| c == '\n').count();
            let output_trailing = sanitized_chunk
                .chars()
                .rev()
                .take_while(|&c| c == '\n')
                .count();

            for _ in output_trailing..input_trailing {
                sanitized_chunk.push('\n');
            }

            Ok::<_, std::io::Error>(axum::body::Bytes::from(sanitized_chunk))
        },
    );

    *response.body_mut() = Body::from_stream(sanitized_stream);
    response.headers_mut().remove(header::CONTENT_LENGTH);
    debug!("Set up streaming chat completion response sanitization");
    response
}

async fn sanitize_completions_response(mut response: Response, original_model: String) -> Response {
    let body_bytes =
        match axum::body::to_bytes(std::mem::take(response.body_mut()), usize::MAX).await {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(error = %e, "Failed to read completions response body");
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "Failed to read upstream response",
                );
            }
        };

    let mut raw_response: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(resp) => resp,
        Err(e) => {
            error!(
                error = %e,
                response_len = body_bytes.len(),
                "Failed to deserialize completions response from provider, returning standard error"
            );
            return error_response(StatusCode::BAD_GATEWAY, "api_error", "Bad gateway");
        }
    };

    normalize_completion_response_value(&mut raw_response, &original_model);

    let mut completion_response: CompletionResponse = match serde_json::from_value(raw_response) {
        Ok(resp) => resp,
        Err(e) => {
            error!(
                error = %e,
                "Failed to coerce completions response into strict schema"
            );
            return error_response(StatusCode::BAD_GATEWAY, "api_error", "Bad gateway");
        }
    };

    completion_response.model = original_model;

    match serde_json::to_vec(&completion_response) {
        Ok(sanitized_bytes) => {
            let content_length = sanitized_bytes.len();
            *response.body_mut() = Body::from(sanitized_bytes);
            response
                .headers_mut()
                .remove(axum::http::header::TRANSFER_ENCODING);
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                header::HeaderValue::from(content_length),
            );
            debug!("Sanitized non-streaming completions response");
            response
        }
        Err(e) => {
            error!(error = %e, "Failed to serialize sanitized completions response");
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                "Internal server error",
            )
        }
    }
}

async fn sanitize_streaming_completions_response(
    mut response: Response,
    original_model: String,
    trusted: bool,
    sse_buffer_limit: usize,
    response_id_override: Option<String>,
    expected_choices: u32,
) -> Response {
    let body_stream =
        http_body_util::BodyExt::into_data_stream(std::mem::take(response.body_mut()));
    let buffered_stream = crate::sse::SseBufferedStream::with_limit(body_stream, sse_buffer_limit);
    let stream_fallback_id = generated_completion_id();

    let sanitized_stream = stream_end::end_explicitly(
        buffered_stream,
        stream_end::StreamProgress::new(expected_choices),
        move |chunk, progress| {
            let chunk_str = String::from_utf8_lossy(&chunk);
            let mut sanitized_lines = Vec::new();

            for line in chunk_str.lines() {
                if let Some(data_part) = line.strip_prefix("data: ") {
                    if data_part.trim() == "[DONE]" {
                        progress.record_end();
                        sanitized_lines.push(line.to_string());
                        continue;
                    }

                    let mut raw_chunk: serde_json::Value = match serde_json::from_str(data_part) {
                        Ok(chunk) => chunk,
                        Err(e) => {
                            error!(
                                error = %e,
                                data_len = data_part.len(),
                                "Failed to parse completions chunk, terminating stream"
                            );
                            return Err(std::io::Error::other("Malformed SSE data from provider"));
                        }
                    };

                    if let Some(error_event) = try_format_sse_error(&raw_chunk, trusted) {
                        error!(
                            data_len = data_part.len(),
                            "Provider returned error envelope in SSE chunk, forwarding as error event"
                        );
                        progress.record_end();
                        sanitized_lines.push(error_event);
                        continue;
                    }

                    normalize_completion_chunk_value(
                        &mut raw_chunk,
                        &original_model,
                        &stream_fallback_id,
                    );

                    match serde_json::from_value::<CompletionChunk>(raw_chunk) {
                        Ok(mut chunk_data) => {
                            for choice in &chunk_data.choices {
                                progress
                                    .record_choice(choice.index, choice.finish_reason.is_some());
                            }
                            if chunk_data.usage.is_some() && chunk_data.choices.is_empty() {
                                progress.record_final_usage();
                            }
                            chunk_data.model = original_model.clone();
                            if let Some(id) = &response_id_override {
                                chunk_data.id.clone_from(id);
                            }
                            match serde_json::to_string(&chunk_data) {
                                Ok(json) => sanitized_lines.push(format!("data: {json}")),
                                Err(e) => {
                                    error!(error = %e, "Failed to serialize completions chunk, terminating stream");
                                    return Err(std::io::Error::other(
                                        "Failed to serialize completions chunk",
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                data_len = data_part.len(),
                                "Failed to parse completions chunk, terminating stream"
                            );
                            return Err(std::io::Error::other("Malformed SSE data from provider"));
                        }
                    }
                } else if line.is_empty() {
                    sanitized_lines.push(String::new());
                }
            }

            let mut sanitized_chunk = sanitized_lines.join("\n");
            let input_trailing = chunk_str.chars().rev().take_while(|&c| c == '\n').count();
            let output_trailing = sanitized_chunk
                .chars()
                .rev()
                .take_while(|&c| c == '\n')
                .count();
            for _ in output_trailing..input_trailing {
                sanitized_chunk.push('\n');
            }

            Ok::<_, std::io::Error>(axum::body::Bytes::from(sanitized_chunk))
        },
    );

    *response.body_mut() = Body::from_stream(sanitized_stream);
    response.headers_mut().remove(header::CONTENT_LENGTH);
    debug!("Set up streaming completions response sanitization");
    response
}

async fn sanitize_embeddings_response(mut response: Response, original_model: String) -> Response {
    let body_bytes =
        match axum::body::to_bytes(std::mem::take(response.body_mut()), usize::MAX).await {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(error = %e, "Failed to read embeddings response body");
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "Failed to read upstream response",
                );
            }
        };

    let mut embeddings_response: EmbeddingsResponse = match serde_json::from_slice(&body_bytes) {
        Ok(resp) => resp,
        Err(e) => {
            error!(
                error = %e,
                response_len = body_bytes.len(),
                "Failed to deserialize embeddings response from provider, returning standard error"
            );
            return error_response(StatusCode::BAD_GATEWAY, "api_error", "Bad gateway");
        }
    };

    embeddings_response.model = original_model;

    match serde_json::to_vec(&embeddings_response) {
        Ok(sanitized_bytes) => {
            let content_length = sanitized_bytes.len();
            *response.body_mut() = Body::from(sanitized_bytes);

            response
                .headers_mut()
                .remove(axum::http::header::TRANSFER_ENCODING);
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                header::HeaderValue::from(content_length),
            );
            debug!("Sanitized embeddings response");
            response
        }
        Err(e) => {
            error!(
                error = %e,
                "Failed to serialize sanitized embeddings response, returning standard error"
            );
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                "Internal server error",
            )
        }
    }
}

fn flatten_tool_namespaces(
    tools: Vec<super::schemas::responses::Tool>,
) -> (
    Vec<super::schemas::responses::Tool>,
    std::collections::BTreeSet<String>,
) {
    use super::schemas::responses::{NamespaceTool, Tool};

    let mut flattened = Vec::with_capacity(tools.len());
    let mut dropped = std::collections::BTreeSet::new();
    for tool in tools {
        match tool {
            Tool::Namespace { tools: members, .. } => {
                for member in members {
                    match member {
                        NamespaceTool::Function {
                            name,
                            description,
                            parameters,
                            strict,
                            ..
                        } => flattened.push(Tool::Function {
                            name,
                            description,
                            parameters,
                            strict: strict.unwrap_or(true),
                        }),
                        NamespaceTool::Custom { name, .. } => {
                            debug!(tool = %name, "Skipping freeform tool in namespace group");
                            dropped.insert(name);
                        }
                    }
                }
            }
            other => flattened.push(other),
        }
    }
    (flattened, dropped)
}

async fn sanitize_responses_response(
    mut response: Response,
    original_model: String,
    response_id_override: Option<String>,
) -> Response {
    let body_bytes =
        match axum::body::to_bytes(std::mem::take(response.body_mut()), usize::MAX).await {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(error = %e, "Failed to read responses API response body");
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "Failed to read upstream response",
                );
            }
        };

    let mut raw_response: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(resp) => resp,
        Err(e) => {
            error!(
                error = %e,
                response_len = body_bytes.len(),
                "Failed to deserialize responses API response from provider, returning standard error"
            );
            return error_response(StatusCode::BAD_GATEWAY, "api_error", "Bad gateway");
        }
    };

    normalize_responses_response_value(&mut raw_response, &original_model);

    let mut responses_response: ResponsesResponse = match serde_json::from_value(raw_response) {
        Ok(resp) => resp,
        Err(e) => {
            error!(error = %e, "Failed to coerce responses API response into strict schema");
            return error_response(StatusCode::BAD_GATEWAY, "api_error", "Bad gateway");
        }
    };

    responses_response.model = original_model;
    if let Some(id) = response_id_override {
        responses_response.id = id;
    }

    match serde_json::to_vec(&responses_response) {
        Ok(sanitized_bytes) => {
            let content_length = sanitized_bytes.len();
            *response.body_mut() = Body::from(sanitized_bytes);

            response
                .headers_mut()
                .remove(axum::http::header::TRANSFER_ENCODING);
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                header::HeaderValue::from(content_length),
            );
            debug!("Sanitized responses API response");
            response
        }
        Err(e) => {
            error!(
                error = %e,
                "Failed to serialize sanitized responses API response, returning standard error"
            );
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "api_error",
                "Internal server error",
            )
        }
    }
}

async fn sanitize_streaming_responses_response(
    mut response: Response,
    original_model: String,
    trusted: bool,
    response_id_override: Option<String>,
    sse_buffer_limit: usize,
) -> Response {
    let body_stream =
        http_body_util::BodyExt::into_data_stream(std::mem::take(response.body_mut()));
    let buffered_stream = crate::sse::SseBufferedStream::with_limit(body_stream, sse_buffer_limit);
    let response_id_override = response_id_override.clone();
    let stream_fallback_response_id = response_id_override
        .clone()
        .unwrap_or_else(generated_response_id);

    let sanitized_stream = stream_end::end_explicitly(
        buffered_stream,
        stream_end::StreamProgress::new(1),
        move |chunk, progress| {
            let chunk_str = String::from_utf8_lossy(&chunk);
            let mut sanitized_lines = Vec::new();

            for line in chunk_str.lines() {
                if let Some(data_part) = line.strip_prefix("data: ") {

                    if data_part.trim() == "[DONE]" {
                        progress.record_end();
                        sanitized_lines.push(line.to_string());
                        continue;
                    }

                    let mut raw_event: serde_json::Value = match serde_json::from_str(data_part) {
                        Ok(event) => event,
                        Err(e) => {
                            error!(
                                error = %e,
                                data_len = data_part.len(),
                                "Failed to parse responses SSE data line from provider, terminating stream"
                            );
                            return Err(std::io::Error::other("Malformed SSE data from provider"));
                        }
                    };

                    if let Some(error_event) = try_format_sse_error(&raw_event, trusted) {
                        error!(
                            data_len = data_part.len(),
                            "Provider returned error envelope in SSE chunk, forwarding as error event"
                        );
                        progress.record_end();
                        sanitized_lines.push(error_event);
                        continue;
                    }

                    normalize_responses_streaming_event_value(
                        &mut raw_event,
                        &original_model,
                        &stream_fallback_response_id,
                    );

                    match serde_json::from_value::<ResponsesStreamingEvent>(raw_event) {
                        Ok(mut event) => {
                            if matches!(
                                event.event_type.as_str(),
                                "response.completed"
                                    | "response.failed"
                                    | "response.incomplete"
                                    | "error"
                            ) {
                                progress.record_end();
                            }

                            if let Some(ref mut response) = event.response {
                                response.model = original_model.clone();
                                if response_id_override.is_some() {
                                    response.id = stream_fallback_response_id.clone();
                                }
                            }

                            match serde_json::to_string(&event) {
                                Ok(sanitized_json) => {
                                    sanitized_lines.push(format!("data: {}", sanitized_json));
                                }
                                Err(e) => {
                                    error!(error = %e, "Failed to serialize responses chunk, terminating stream");
                                    return Err(std::io::Error::other("Failed to serialize chunk"));
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                data_len = data_part.len(),
                                "Failed to parse responses SSE data line from provider, terminating stream"
                            );
                            return Err(std::io::Error::other("Malformed SSE data from provider"));
                        }
                    }
                } else if line.is_empty() {
                    sanitized_lines.push(String::new());
                }
            }

            let mut sanitized_chunk = sanitized_lines.join("\n");

            let input_trailing = chunk_str.chars().rev().take_while(|&c| c == '\n').count();
            let output_trailing = sanitized_chunk
                .chars()
                .rev()
                .take_while(|&c| c == '\n')
                .count();

            for _ in output_trailing..input_trailing {
                sanitized_chunk.push('\n');
            }

            Ok::<_, std::io::Error>(axum::body::Bytes::from(sanitized_chunk))
        },
    );

    *response.body_mut() = Body::from_stream(sanitized_stream);
    response.headers_mut().remove(header::CONTENT_LENGTH);
    debug!("Set up streaming responses response sanitization");
    response
}

fn try_format_sse_error(value: &serde_json::Value, trusted: bool) -> Option<String> {
    let error_obj = value
        .get("error")
        .filter(|v| v.is_object() && (v.get("message").is_some() || v.get("code").is_some()))?;

    if trusted {
        let envelope = json!({ "error": error_obj });
        Some(format!("data: {envelope}"))
    } else {
        let provider_code = error_obj
            .get("code")
            .and_then(parse_provider_status_code)
            .unwrap_or_else(|| {
                warn!(
                    code = ?error_obj.get("code"),
                    "Provider error object has missing or unrecognized code, defaulting to 500"
                );
                500
            });
        let masked_code = mask_account_class_status(provider_code);
        let (error_type, message) = sanitized_error_for_status(masked_code);
        let envelope = json!({
            "error": {
                "message": message,
                "type": error_type,
                "param": null,
                "code": masked_code,
            }
        });
        Some(format!("data: {envelope}"))
    }
}

fn parse_provider_status_code(code: &serde_json::Value) -> Option<u16> {
    if let Some(n) = code.as_u64() {
        return u16::try_from(n).ok();
    }
    let s = code.as_str()?;
    if let Ok(n) = s.parse::<u16>() {
        return Some(n);
    }
    match s {
        "rate_limit" | "rate_limit_error" | "rate_limit_exceeded" => Some(429),
        _ => None,
    }
}

async fn sanitize_error_response(mut response: Response) -> Response {
    let status = response.status();

    if let Some(labels) = response.extensions().get::<UpstreamFailureLabels>() {
        metrics::counter!(
            "onwards_upstream_failed_total",
            "reason" => "third_party_error",
            "status" => status.as_str().to_string(),
            "model" => labels.model.clone(),
            "serving_class" => labels.serving_class.clone(),
            "traffic" => labels.traffic,
            "served_by" => labels.served_by.clone(),
        )
        .increment(1);
    }

    let body_bytes =
        match axum::body::to_bytes(std::mem::take(response.body_mut()), MAX_ERROR_BODY).await {
            Ok(bytes) => bytes,
            Err(e) => {
                error!(error = %e, "Failed to read error response body");
                return standard_error_response(status);
            }
        };

    error!(
        status = %status,
        body_len = body_bytes.len(),
        "Third-party error response (logged, not forwarded)"
    );

    standard_error_response(status)
}

async fn envelope_trusted_error(mut response: Response) -> Response {
    let encoded = response
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_some_and(|encoding| encoding != "identity");
    if encoded {
        return response;
    }

    let status = response.status();
    let body = match buffer_error_body(std::mem::take(response.body_mut())).await {
        BufferedErrorBody::Complete(bytes) => bytes,
        BufferedErrorBody::TooLarge(body) => {
            *response.body_mut() = body;
            return response;
        }
        BufferedErrorBody::Failed(e) => {
            error!(error = %e, status = %status, "Failed to read trusted error response body");
            Bytes::new()
        }
    };

    match openai_error_envelope(status, &body) {
        Some(envelope) => {
            let headers = response.headers_mut();
            headers.insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/json"),
            );
            headers.remove(header::CONTENT_LENGTH);
            *response.body_mut() = Body::from(envelope.to_string());
        }
        None => *response.body_mut() = Body::from(body),
    }
    response
}

enum BufferedErrorBody {
    Complete(Bytes),
    TooLarge(Body),
    Failed(axum::Error),
}

async fn buffer_error_body(body: Body) -> BufferedErrorBody {
    let mut stream = http_body_util::BodyExt::into_data_stream(body);
    let mut chunks: Vec<Bytes> = Vec::new();
    let mut len = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(e) => return BufferedErrorBody::Failed(e),
        };
        if chunk.is_empty() {
            continue;
        }
        len += chunk.len();
        chunks.push(chunk);
        if len > MAX_ERROR_BODY {
            let read = futures_util::stream::iter(chunks.into_iter().map(Ok::<_, axum::Error>));
            return BufferedErrorBody::TooLarge(Body::from_stream(read.chain(stream)));
        }
    }
    BufferedErrorBody::Complete(chunks.concat().into())
}

fn openai_error_envelope(status: StatusCode, body: &[u8]) -> Option<serde_json::Value> {
    let parsed: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    if parsed
        .get("error")
        .is_some_and(serde_json::Value::is_object)
    {
        return None;
    }

    let (error_type, generic_message) = sanitized_error_for_status(status.as_u16());
    let usable = |field: &str| {
        parsed
            .get(field)
            .and_then(serde_json::Value::as_str)
            .filter(|message| !message.trim().is_empty())
    };
    let message = usable("message")
        .or_else(|| usable("error"))
        .unwrap_or(generic_message);
    let context_length = parsed
        .pointer("/details/type")
        .and_then(serde_json::Value::as_str)
        == Some("context_length");

    Some(json!({
        "error": {
            "message": message,
            "type": error_type,
            "param": null,
            "code": context_length.then_some("context_length_exceeded"),
        }
    }))
}

fn sanitized_error_for_status(status: u16) -> (&'static str, &'static str) {
    match status {
        400 => ("invalid_request_error", "Invalid request"),
        401 => ("authentication_error", "Authentication failed"),
        402 => ("api_error", "Service unavailable"),
        403 => ("permission_error", "Permission denied"),
        404 => ("not_found_error", "Not found"),
        408 => ("api_error", "Gateway timeout"),
        413 => ("invalid_request_error", "Request too large"),
        422 => ("invalid_request_error", "Unprocessable request"),
        429 => ("rate_limit_error", "Rate limit exceeded"),
        500 => ("api_error", "Internal server error"),
        502 => ("api_error", "Bad gateway"),
        503 => ("api_error", "Service unavailable"),
        504 => ("api_error", "Gateway timeout"),
        _ => ("api_error", "An error occurred"),
    }
}

fn mask_account_class_status(status: u16) -> u16 {
    match status {
        401 | 402 | 403 | 451 => 502,
        408 => 504,
        _ => status,
    }
}

fn standard_error_response(status: StatusCode) -> Response {
    let masked_code = mask_account_class_status(status.as_u16());
    let masked_status = StatusCode::from_u16(masked_code).unwrap_or(status);
    let (error_type, message) = sanitized_error_for_status(masked_code);
    error_response(masked_status, error_type, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::{Target, Targets};
    use crate::test_utils::MockHttpClient;
    use axum::body::Body;
    use axum::http::Request;
    use dashmap::DashMap;
    use std::sync::Arc;
    use tower::ServiceExt;

    #[rstest::rstest]
    #[case(false, 65536, false)]
    #[case(false, 262144, true)]
    #[case(true, 65536, false)]
    #[case(true, 262144, true)]
    #[tokio::test]
    async fn test_router_applies_sse_buffer_limit(
        #[case] strict_mode: bool,
        #[case] limit: usize,
        #[case] succeeds: bool,
    ) {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.example.com/v1/".parse().unwrap())
                .sanitize_response(true)
                .build()
                .into_pool(),
        );
        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode,
            http_pool_config: None,
        };
        let content = "x".repeat(128 * 1024);
        let event = format!(
            "data: {}\n\n",
            serde_json::json!({
                "id": "chatcmpl-large", "object": "chat.completion.chunk", "created": 1,
                "model": "provider-model",
                "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": null}]
            })
        );
        let chunks = vec![
            event[..70 * 1024].to_string(),
            event[70 * 1024..].to_string(),
            "data: [DONE]\n\n".to_string(),
        ];
        let state = AppState::with_client(
            targets,
            MockHttpClient::new_streaming(StatusCode::OK, chunks),
        )
        .with_response_transform(crate::create_openai_sanitizer())
        .with_sse_buffer_limit(limit);
        let router = if strict_mode {
            crate::strict::build_strict_router(state)
        } else {
            crate::build_router(state)
        };
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}],"stream":true}"#,
            ))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await;
        if succeeds {
            assert_eq!(status, StatusCode::OK);
            let bytes = body.unwrap();
            let text = String::from_utf8_lossy(&bytes);
            assert!(text.contains(&content));
            assert!(text.contains("data: [DONE]"));
            assert!(text.contains("gpt-4"));
        } else {
            assert!(status.is_server_error() || body.is_err());
        }
    }


    #[derive(Clone, Default)]
    struct CaptureWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    async fn capture_logs<F, Fut>(f: F) -> String
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let buf = Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(CaptureWriter(buf.clone()))
            .finish();
        {
            let _guard = tracing::subscriber::set_default(subscriber);
            f().await;
        }
        let bytes = buf.lock().unwrap().clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zdr_sanitize_chat_response_does_not_log_body() {
        const SENTINEL: &str = "pikachu-used-thunderbolt-9f3a";
        let body = json!({
            "id": "chatcmpl-x",
            "object": "chat.completion",
            "created": 0,
            "model": "upstream-model",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": SENTINEL},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        })
        .to_string();

        let logs = capture_logs(|| async {
            let response = Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Body::from(body.clone()))
                .unwrap();
            let _ = sanitize_chat_response(response, "client-model".to_string()).await;
        })
        .await;

        assert!(
            !logs.contains(SENTINEL),
            "response body content leaked into logs:\n{logs}"
        );
        assert!(
            logs.contains("bytes_read") || logs.contains("content_length"),
            "expected length metadata in logs, got:\n{logs}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zdr_sanitize_error_response_does_not_log_provider_body() {
        const SENTINEL: &str = "pikachu-fainted-2b7c";
        let logs = capture_logs(|| async {
            let response = Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("{{\"error\":\"{SENTINEL}\"}}")))
                .unwrap();
            let _ = sanitize_error_response(response).await;
        })
        .await;

        assert!(
            !logs.contains(SENTINEL),
            "provider error body leaked into logs:\n{logs}"
        );
        assert!(
            logs.contains("body_len"),
            "expected body_len metadata in logs, got:\n{logs}"
        );
    }

    #[test]
    fn test_error_response_format() {
        let response = error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "Test error",
        );
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_error_response_matches_openai_format() {
        let response = error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "Invalid request",
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let our_error: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let openai_format = json!({
            "error": {
                "message": "Invalid request",
                "type": "invalid_request_error",
                "param": null,
                "code": null
            }
        });

        assert_eq!(our_error, openai_format);

        assert!(our_error["error"]["message"].is_string());
        assert!(our_error["error"]["type"].is_string());
        assert!(our_error["error"]["param"].is_null());
        assert!(our_error["error"]["code"].is_null());
    }

    #[tokio::test]
    async fn test_error_types_match_openai_conventions() {
        let test_cases = vec![
            (StatusCode::BAD_REQUEST, "invalid_request_error"),
            (StatusCode::NOT_FOUND, "not_found_error"),
            (StatusCode::TOO_MANY_REQUESTS, "rate_limit_error"),
            (StatusCode::INTERNAL_SERVER_ERROR, "api_error"),
            (StatusCode::BAD_GATEWAY, "api_error"),
            (StatusCode::SERVICE_UNAVAILABLE, "api_error"),
        ];

        for (status, expected_type) in test_cases {
            let response = standard_error_response(status);
            assert_eq!(
                response.status(),
                status,
                "Status {status} must pass through unmasked"
            );
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let error: serde_json::Value = serde_json::from_slice(&body).unwrap();

            assert_eq!(
                error["error"]["type"].as_str().unwrap(),
                expected_type,
                "Status {} should map to error type {}",
                status,
                expected_type
            );
        }

        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::PAYMENT_REQUIRED,
            StatusCode::FORBIDDEN,
            StatusCode::from_u16(451).unwrap(),
        ] {
            let response = standard_error_response(status);
            assert_eq!(
                response.status(),
                StatusCode::BAD_GATEWAY,
                "{status} must be masked to 502"
            );
        }
    }

    #[tokio::test]
    async fn test_strict_sanitize_non_streaming_removes_unknown_fields() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "gpt-4",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            },
            "provider": "custom-llm-provider",
            "cost": 0.00123,
            "internal_id": "xyz-123",
            "custom_field": "should_be_removed"
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"id\":\"chatcmpl-123\""));
        assert!(body_str.contains("\"model\":\"gpt-4\""));
        assert!(body_str.contains("\"choices\""));

        assert!(!body_str.contains("provider"));
        assert!(!body_str.contains("cost"));
        assert!(!body_str.contains("internal_id"));
        assert!(!body_str.contains("custom_field"));
    }

    #[tokio::test]
    async fn test_strict_chat_response_normalizes_null_like_tool_arguments() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id":"chatcmpl-tools",
            "object":"chat.completion",
            "created":1,
            "model":"provider-model",
            "choices":[{
                "index":0,
                "message":{
                    "role":"assistant",
                    "content":null,
                    "tool_calls":[
                        {"id":"missing","type":"function","function":{"name":"missing"}},
                        {"id":"null","type":"function","function":{"name":"null","arguments":null}},
                        {"id":"empty","type":"function","function":{"name":"empty","arguments":""}},
                        {"id":"string-null","type":"function","function":{"name":"string_null","arguments":"null"}},
                        {"id":"valid","type":"function","function":{"name":"valid","arguments":"{\"query\":\"x\"}"}}
                    ]
                },
                "finish_reason":"tool_calls"
            }]
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let calls = body["choices"][0]["message"]["tool_calls"]
            .as_array()
            .unwrap();

        assert_eq!(calls[0]["function"]["arguments"], "{}");
        assert_eq!(calls[1]["function"]["arguments"], "{}");
        assert_eq!(calls[2]["function"]["arguments"], "{}");
        assert_eq!(calls[3]["function"]["arguments"], "{}");
        assert_eq!(calls[4]["function"]["arguments"], r#"{"query":"x"}"#);
    }

    #[tokio::test]
    async fn test_strict_chat_request_forwards_body_unchanged() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "gpt-4",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!"
                },
                "finish_reason": "stop"
            }]
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client.clone());
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "id": "chatcmpl-client-supplied",
            "provider_extension": "preserve-me",
            "model": "gpt-4",
            "messages": [{"role": "user", "content": "Hello", "nested_unknown": "keep-me"}]
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);

        assert_eq!(
            &requests[0].body[..],
            request_body.as_bytes(),
            "onwards must forward the request body verbatim"
        );

        let forwarded_json: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(forwarded_json["id"], "chatcmpl-client-supplied");
        assert_eq!(forwarded_json["provider_extension"], "preserve-me");
        assert_eq!(forwarded_json["messages"][0]["nested_unknown"], "keep-me");
    }

    #[tokio::test]
    async fn test_strict_sanitize_rewrites_model_field() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .onwards_model("gpt-4-turbo-2024-04-09".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "gpt-4-turbo-2024-04-09",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!"
                },
                "finish_reason": "stop"
            }]
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"model\":\"gpt-4\""));
        assert!(!body_str.contains("gpt-4-turbo-2024-04-09"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_chat_backfills_missing_noncritical_fields() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "choices": [{
                "message": {
                    "content": "Hello from downstream"
                }
            }]
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["model"], "gpt-4");
        assert_eq!(json["object"], "chat.completion");
        let id = json["id"].as_str().expect("id should be a string");
        assert!(id.starts_with("chatcmpl-"));
        assert_eq!(json["choices"][0]["index"], 0);
        assert_eq!(json["choices"][0]["message"]["role"], "assistant");
        assert_eq!(
            json["choices"][0]["message"]["content"],
            "Hello from downstream"
        );
    }

    #[tokio::test]
    async fn test_strict_sanitize_chat_backfills_choice_indexes_from_position() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "choices": [
                {"message": {"content": "first"}},
                {"message": {"content": "second"}}
            ]
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["choices"][0]["index"], 0);
        assert_eq!(json["choices"][1]["index"], 1);
    }

    #[tokio::test]
    async fn test_strict_sanitize_streaming_removes_unknown_fields() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let streaming_chunks = vec![
            r#"data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1677652288,"model":"gpt-4","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}],"provider":"custom-provider","cost":0.001}

"#.to_string(),
            "data: [DONE]\n\n".to_string(),
        ];

        let mock_client = MockHttpClient::new_streaming(StatusCode::OK, streaming_chunks);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body =
            r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}],"stream":true}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(!body_str.contains("provider"));
        assert!(!body_str.contains("cost"));
        assert!(body_str.contains("[DONE]"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_streaming_rewrites_model() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .onwards_model("gpt-4-turbo".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let streaming_chunks = vec![
            r#"data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1677652288,"model":"gpt-4-turbo","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}

"#.to_string(),
            "data: [DONE]\n\n".to_string(),
        ];

        let mock_client = MockHttpClient::new_streaming(StatusCode::OK, streaming_chunks);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body =
            r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}],"stream":true}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"model\":\"gpt-4\""));
        assert!(!body_str.contains("gpt-4-turbo"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_streaming_chat_backfills_missing_noncritical_fields() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let streaming_chunks = vec![
            r#"data: {"choices":[{"delta":{"content":"Hello"}}]}

"#
            .to_string(),
            "data: [DONE]\n\n".to_string(),
        ];

        let mock_client = MockHttpClient::new_streaming(StatusCode::OK, streaming_chunks);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}],"stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"object\":\"chat.completion.chunk\""));
        assert!(body_str.contains("\"model\":\"gpt-4\""));
        assert!(body_str.contains("\"index\":0"));
        assert!(body_str.contains("\"content\":\"Hello\""));
        assert!(body_str.contains("\"id\":\"chatcmpl-"));
        assert!(body_str.contains("[DONE]"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_streaming_chat_backfills_chunk_indexes_from_position() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let streaming_chunks = vec![
            r#"data: {"choices":[{"delta":{"content":"first"}},{"delta":{"content":"second"}}]}

"#
            .to_string(),
            "data: [DONE]\n\n".to_string(),
        ];

        let mock_client = MockHttpClient::new_streaming(StatusCode::OK, streaming_chunks);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}],"stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"index\":0"));
        assert!(body_str.contains("\"index\":1"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_streaming_chat_reuses_fallback_id_across_chunks() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let streaming_chunks = vec![
            "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n".to_string(),
            "data: {\"choices\":[{\"delta\":{\"content\":\"second\"}}]}\n\n".to_string(),
            "data: [DONE]\n\n".to_string(),
        ];

        let mock_client = MockHttpClient::new_streaming(StatusCode::OK, streaming_chunks);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}],"stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        let ids: Vec<String> = body_str
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter(|data| *data != "[DONE]")
            .map(|data| serde_json::from_str::<serde_json::Value>(data).unwrap())
            .filter_map(|json| json.get("id").and_then(|v| v.as_str()).map(str::to_string))
            .collect();

        assert!(ids.len() >= 2);
        assert!(ids.iter().all(|id| id == &ids[0]));
    }

    #[tokio::test]
    async fn test_strict_sanitize_embeddings_removes_unknown_fields() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "text-embedding-3-small".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "object": "list",
            "data": [{
                "object": "embedding",
                "embedding": [0.1, 0.2, 0.3],
                "index": 0
            }],
            "model": "text-embedding-3-small",
            "usage": {
                "prompt_tokens": 5,
                "total_tokens": 5
            },
            "provider": "custom-embeddings",
            "cost": 0.0001,
            "cache_hit": true
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"text-embedding-3-small","input":"Hello world"}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/embeddings")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"object\":\"list\""));
        assert!(body_str.contains("\"model\":\"text-embedding-3-small\""));
        assert!(body_str.contains("\"data\""));

        assert!(!body_str.contains("provider"));
        assert!(!body_str.contains("cost"));
        assert!(!body_str.contains("cache_hit"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_embeddings_rewrites_model() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "text-embedding-3-small".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .onwards_model("text-embedding-3-small-internal".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "object": "list",
            "data": [{
                "object": "embedding",
                "embedding": [0.1, 0.2],
                "index": 0
            }],
            "model": "text-embedding-3-small-internal",
            "usage": {
                "prompt_tokens": 5,
                "total_tokens": 5
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"text-embedding-3-small","input":"Hello"}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/embeddings")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"model\":\"text-embedding-3-small\""));
        assert!(!body_str.contains("text-embedding-3-small-internal"));
    }

    #[tokio::test]
    async fn test_strict_error_returns_standard_message() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_error_response = r#"{
            "error": {
                "message": "Database connection failed at postgresql://internal-db:5432",
                "type": "internal_error",
                "provider": "custom-llm-backend",
                "internal_trace_id": "xyz-123-abc",
                "debug_info": "Stack trace: error at line 42"
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::BAD_REQUEST, mock_error_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"error\""));
        assert!(body_str.contains("\"type\":\"invalid_request_error\""));
        assert!(body_str.contains("\"message\":\"Invalid request\""));

        assert!(!body_str.contains("Database"));
        assert!(!body_str.contains("postgresql"));
        assert!(!body_str.contains("provider"));
        assert!(!body_str.contains("internal_trace_id"));
        assert!(!body_str.contains("debug_info"));
    }

    #[tokio::test]
    async fn test_strict_error_500_returns_standard_message() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_error_response = r#"{
            "error": {
                "message": "Contact support@provider.com with trace ID abc-123",
                "type": "server_error"
            }
        }"#;

        let mock_client =
            MockHttpClient::new(StatusCode::INTERNAL_SERVER_ERROR, mock_error_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"type\":\"api_error\""));
        assert!(body_str.contains("\"message\":\"Internal server error\""));

        assert!(!body_str.contains("support@provider.com"));
        assert!(!body_str.contains("trace ID abc-123"));
    }

    #[tokio::test]
    async fn test_strict_handle_malformed_error_response() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_error_response = "<html><body>Internal Server Error</body></html>";

        let mock_client =
            MockHttpClient::new(StatusCode::INTERNAL_SERVER_ERROR, mock_error_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(!body_str.contains("<html>"));
        assert!(!body_str.contains("<body>"));
        assert!(body_str.contains("\"error\""));

        let _: serde_json::Value = serde_json::from_str(&body_str).expect("Should be valid JSON");
    }

    #[tokio::test]
    async fn test_strict_sanitize_responses_removes_unknown_fields() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "resp_abc123",
            "object": "response",
            "created_at": 1677652288,
            "completed_at": 1677652290,
            "status": "completed",
            "incomplete_details": null,
            "model": "gpt-4o",
            "previous_response_id": null,
            "instructions": null,
            "output": [],
            "error": null,
            "tools": [],
            "tool_choice": "auto",
            "truncation": "auto",
            "parallel_tool_calls": true,
            "text": {},
            "top_p": 1.0,
            "presence_penalty": 0.0,
            "frequency_penalty": 0.0,
            "top_logprobs": 0,
            "temperature": 1.0,
            "reasoning": null,
            "usage": null,
            "max_output_tokens": null,
            "max_tool_calls": null,
            "store": false,
            "background": false,
            "service_tier": "default",
            "metadata": null,
            "safety_identifier": null,
            "prompt_cache_key": null,
            "provider": "custom-provider",
            "cost": 0.0123,
            "internal_trace_id": "xyz-789"
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4o","input":"Hello"}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"id\":\"resp_abc123\""));
        assert!(body_str.contains("\"model\":\"gpt-4o\""));
        assert!(body_str.contains("\"status\":\"completed\""));

        assert!(!body_str.contains("provider"));
        assert!(!body_str.contains("cost"));
        assert!(!body_str.contains("internal_trace_id"));
    }

    #[tokio::test]
    async fn test_responses_namespace_tools_are_flattened_before_forwarding() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "kimi-k2.5".to_string(),
            Target::builder()
                .url("https://dynamo.example.com/v1/".parse().unwrap())
                .build()
                .into_pool(),
        );
        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(
            StatusCode::OK,
            r#"{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"kimi-k2.5","output":[],"tools":[],"tool_choice":"auto","truncation":"disabled","parallel_tool_calls":true,"text":{"format":{"type":"text"}},"top_p":1.0,"presence_penalty":0.0,"frequency_penalty":0.0,"top_logprobs":0,"temperature":1.0,"reasoning":null,"usage":null,"store":false,"background":false,"service_tier":"default"}"#,
        );
        let state = AppState::with_client(targets, mock_client.clone());
        let router = crate::strict::build_strict_router(state);

        let request_body = serde_json::json!({
            "model": "kimi-k2.5",
            "input": "fix the build",
            "tools": [{
                "type": "namespace",
                "name": "functions",
                "description": "",
                "tools": [
                    {
                        "type": "function",
                        "name": "exec_command",
                        "description": "Runs a command.",
                        "strict": false,
                        "parameters": {"type": "object", "properties": {}}
                    },
                    {
                        "type": "custom",
                        "name": "apply_patch",
                        "description": "Edits files.",
                        "format": {"type": "grammar", "syntax": "lark", "definition": "start: /.+/"}
                    }
                ]
            }]
        });

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body.to_string()))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let forwarded = mock_client.requests.lock().unwrap();
        assert_eq!(forwarded.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&forwarded[0].body).unwrap();
        let tools = body["tools"]
            .as_array()
            .expect("tools should still be forwarded");
        assert_eq!(tools.len(), 1, "only the function member survives");
        assert_eq!(tools[0]["type"], "function");
        assert_eq!(tools[0]["name"], "exec_command");
    }

    #[tokio::test]
    async fn test_responses_dropped_freeform_tool_downgrades_pinned_tool_choice() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "kimi-k2.5".to_string(),
            Target::builder()
                .url("https://dynamo.example.com/v1/".parse().unwrap())
                .build()
                .into_pool(),
        );
        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(
            StatusCode::OK,
            r#"{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"kimi-k2.5","output":[],"tools":[],"tool_choice":"auto","truncation":"disabled","parallel_tool_calls":true,"text":{"format":{"type":"text"}},"top_p":1.0,"presence_penalty":0.0,"frequency_penalty":0.0,"top_logprobs":0,"temperature":1.0,"reasoning":null,"usage":null,"store":false,"background":false,"service_tier":"default"}"#,
        );
        let state = AppState::with_client(targets, mock_client.clone());
        let router = crate::strict::build_strict_router(state);

        let request_body = serde_json::json!({
            "model": "kimi-k2.5",
            "input": "fix the build",
            "tools": [{
                "type": "namespace",
                "name": "functions",
                "tools": [{
                    "type": "custom",
                    "name": "apply_patch",
                    "format": {"type": "grammar", "syntax": "lark", "definition": "start: /.+/"}
                }]
            }],
            "tool_choice": {"type": "function", "name": "apply_patch"}
        });

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body.to_string()))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let forwarded = mock_client.requests.lock().unwrap();
        assert_eq!(forwarded.len(), 1);
        let body: serde_json::Value = serde_json::from_slice(&forwarded[0].body).unwrap();
        assert!(
            body["tools"]
                .as_array()
                .is_none_or(|tools| tools.is_empty()),
            "the freeform tool is not forwarded"
        );
        assert_eq!(body["tool_choice"], "auto");
    }

    #[tokio::test]
    async fn test_strict_chat_invalid_reasoning_type_returns_parameter_specific_400() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .build()
                .into_pool(),
        );
        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };
        let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
        let state = AppState::with_client(targets, mock_client.clone());
        let router = crate::strict::build_strict_router(state);
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4o","messages":[],"reasoning_effort":false}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["param"], "reasoning_effort");
        assert_eq!(body["error"]["code"], "invalid_type");
        assert!(mock_client.get_requests().is_empty());
    }

    #[tokio::test]
    async fn test_strict_sanitize_responses_rewrites_model() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .onwards_model("gpt-4o-2024-05-13".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "resp_abc123",
            "object": "response",
            "created_at": 1677652288,
            "completed_at": 1677652290,
            "status": "completed",
            "incomplete_details": null,
            "model": "gpt-4o-2024-05-13",
            "previous_response_id": null,
            "instructions": null,
            "output": [],
            "error": null,
            "tools": [],
            "tool_choice": "auto",
            "truncation": "auto",
            "parallel_tool_calls": true,
            "text": {},
            "top_p": 1.0,
            "presence_penalty": 0.0,
            "frequency_penalty": 0.0,
            "top_logprobs": 0,
            "temperature": 1.0,
            "reasoning": null,
            "usage": null,
            "max_output_tokens": null,
            "max_tool_calls": null,
            "store": false,
            "background": false,
            "service_tier": "default",
            "metadata": null,
            "safety_identifier": null,
            "prompt_cache_key": null
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4o","input":"Hello"}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"model\":\"gpt-4o\""));
        assert!(!body_str.contains("gpt-4o-2024-05-13"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_responses_backfills_missing_noncritical_fields() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "resp_abc123",
            "created_at": 1677652288,
            "model": "provider-model",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": "Hello from downstream"
                }]
            }]
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"model":"gpt-4o","input":"Hello"}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["id"], "resp_abc123");
        assert_eq!(json["model"], "gpt-4o");
        assert_eq!(json["object"], "response");
        assert_eq!(json["status"], "completed");
        assert_eq!(json["tool_choice"], "auto");
        assert_eq!(json["parallel_tool_calls"], true);
        assert_eq!(json["usage"], serde_json::Value::Null);
        assert_eq!(
            json["output"][0]["content"][0]["text"],
            "Hello from downstream"
        );
    }

    #[tokio::test]
    async fn test_strict_sanitize_responses_error() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_error_response = r#"{
            "error": {
                "message": "Internal error in provider backend at server xyz-123.internal.com",
                "type": "server_error",
                "provider": "custom-provider",
                "trace_id": "abc-def-ghi"
            }
        }"#;

        let mock_client =
            MockHttpClient::new(StatusCode::INTERNAL_SERVER_ERROR, mock_error_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4o","input":"Hello"}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"type\":\"api_error\""));
        assert!(body_str.contains("\"message\":\"Internal server error\""));

        assert!(!body_str.contains("Internal error in provider backend"));
        assert!(!body_str.contains("xyz-123.internal.com"));
        assert!(!body_str.contains("provider"));
        assert!(!body_str.contains("trace_id"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_responses_streaming_removes_unknown_fields() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_stream = concat!(
            "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{",
            "\"id\":\"resp-123\",\"object\":\"response\",\"created_at\":1677652288,",
            "\"completed_at\":null,\"status\":\"in_progress\",\"incomplete_details\":null,",
            "\"model\":\"gpt-4o-2024-08-06\",\"previous_response_id\":null,\"instructions\":null,",
            "\"output\":[],\"error\":null,\"tools\":[],\"tool_choice\":\"auto\",",
            "\"truncation\":\"disabled\",\"parallel_tool_calls\":true,",
            "\"text\":{\"format\":{\"type\":\"text\"}},\"top_p\":1.0,\"presence_penalty\":0.0,",
            "\"frequency_penalty\":0.0,\"top_logprobs\":0,\"temperature\":1.0,",
            "\"reasoning\":{\"effort\":null,\"summary\":null},\"usage\":null,",
            "\"max_output_tokens\":null,\"max_tool_calls\":null,\"store\":false,",
            "\"background\":false,\"service_tier\":\"default\",\"metadata\":null,",
            "\"safety_identifier\":null,\"prompt_cache_key\":null}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,",
            "\"item_id\":\"msg_abc\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello\"}\n\n",
            "data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{",
            "\"id\":\"resp-123\",\"object\":\"response\",\"created_at\":1677652288,",
            "\"completed_at\":1677652290,\"status\":\"completed\",\"incomplete_details\":null,",
            "\"model\":\"gpt-4o-2024-08-06\",\"previous_response_id\":null,\"instructions\":null,",
            "\"output\":[],\"error\":null,\"tools\":[],\"tool_choice\":\"auto\",",
            "\"truncation\":\"disabled\",\"parallel_tool_calls\":true,",
            "\"text\":{\"format\":{\"type\":\"text\"}},\"top_p\":1.0,\"presence_penalty\":0.0,",
            "\"frequency_penalty\":0.0,\"top_logprobs\":0,\"temperature\":1.0,",
            "\"reasoning\":{\"effort\":null,\"summary\":null},\"usage\":null,",
            "\"max_output_tokens\":null,\"max_tool_calls\":null,\"store\":false,",
            "\"background\":false,\"service_tier\":\"default\",\"metadata\":null,",
            "\"safety_identifier\":null,\"prompt_cache_key\":null}}\n\n",
            "data: [DONE]\n\n"
        );

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_stream);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4o","input":"Hello","stream":true}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_str = String::from_utf8(body_bytes.to_vec()).unwrap();

        assert!(
            response_str.contains("\"type\":\"response.created\""),
            "response.created event should be present"
        );
        assert!(
            response_str.contains("\"type\":\"response.completed\""),
            "response.completed event should be present"
        );
        assert!(
            response_str.contains("\"type\":\"response.output_text.delta\""),
            "delta event should pass through"
        );

        assert!(
            !response_str.contains("gpt-4o-2024-08-06"),
            "Provider model name should be rewritten"
        );
        assert!(
            response_str.contains("\"model\":\"gpt-4o\""),
            "Model should be rewritten to the requested model name"
        );

        assert!(
            response_str.contains("\"sequence_number\":0"),
            "sequence_number should be preserved"
        );

        assert!(
            response_str.contains("\"reasoning\":{\"effort\":null,\"summary\":null}"),
            "reasoning null fields must not be collapsed to {{}}"
        );

        assert!(
            response_str.contains("\"delta\":\"Hello\""),
            "delta field should pass through on delta events"
        );
    }

    #[tokio::test]
    async fn test_strict_sanitize_responses_streaming_backfills_missing_noncritical_fields() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_stream = concat!(
            "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{",
            "\"id\":\"resp-123\",\"created_at\":1677652288,\"model\":\"provider-model\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,",
            "\"item_id\":\"msg_abc\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello\"}\n\n",
            "data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{",
            "\"id\":\"resp-123\",\"created_at\":1677652288,\"output\":[{",
            "\"type\":\"message\",\"role\":\"assistant\",\"content\":[{",
            "\"type\":\"output_text\",\"text\":\"Hello\"}]}]}}\n\n",
            "data: [DONE]\n\n"
        );

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_stream);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4o","input":"Hello","stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"type\":\"response.created\""));
        assert!(body_str.contains("\"type\":\"response.completed\""));
        assert!(body_str.contains("\"model\":\"gpt-4o\""));
        assert!(body_str.contains("\"id\":\"resp-123\""));
        assert!(body_str.contains("\"status\":\"in_progress\""));
        assert!(body_str.contains("\"status\":\"completed\""));
        assert!(body_str.contains("\"tool_choice\":\"auto\""));
        assert!(body_str.contains("\"delta\":\"Hello\""));
        assert!(
            !body_str.contains("[DONE]"),
            "nothing is forwarded after response.completed"
        );
    }

    #[tokio::test]
    async fn test_strict_sanitize_responses_streaming_backfills_incomplete_details() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_stream = concat!(
            "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{",
            "\"id\":\"resp-123\",\"created_at\":1677652288,\"model\":\"provider-model\"}}\n\n",
            "data: [DONE]\n\n"
        );

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_stream);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4o","input":"Hello","stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"incomplete_details\":null"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_responses_streaming_reuses_fallback_id_across_snapshots() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_stream = concat!(
            "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{",
            "\"created_at\":1677652288,\"model\":\"provider-model\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{",
            "\"created_at\":1677652288,\"output\":[]}}\n\n",
            "data: [DONE]\n\n"
        );

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_stream);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4o","input":"Hello","stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        let ids: Vec<String> = body_str
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter(|data| *data != "[DONE]")
            .map(|data| serde_json::from_str::<serde_json::Value>(data).unwrap())
            .filter_map(|json| {
                json.get("response")
                    .and_then(|response| response.get("id"))
                    .and_then(|id| id.as_str())
                    .map(str::to_string)
            })
            .collect();

        assert!(ids.len() >= 2);
        assert!(ids.iter().all(|id| id == &ids[0]));
    }

    #[tokio::test]
    async fn test_strict_sanitize_responses_streaming_forwards_error_object() {
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(
                "data: {\"error\":{\"message\":\"Input too long\",\"type\":\"invalid_request_error\",\"code\":\"context_length_exceeded\"}}\n\n",
            ))
            .unwrap();

        let result = sanitize_streaming_responses_response(
            response,
            "test-model".to_string(),
            true,
            None,
            crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
        )
        .await;
        assert_eq!(result.status(), StatusCode::OK);

        let body = axum::body::to_bytes(result.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body);
        assert!(body_str.contains("Input too long"));
        assert!(body_str.contains("\"error\""));
    }

    #[tokio::test]
    async fn test_chat_sanitization_updates_content_length_header() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1677652288,
            "model": "provider-model-name",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            },
            "provider": "custom-provider",
            "cost": 0.00123,
            "trace_id": "abc-xyz-123",
            "custom_metadata": "will be dropped"
        }"#;

        let mut mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        mock_client.set_header("content-length", mock_response.len().to_string());

        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4","messages":[{"role":"user","content":"test"}]}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        let body_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body_json["model"], "gpt-4");
        assert!(body_json.get("provider").is_none());
        assert!(body_json.get("cost").is_none());
        assert!(body_json.get("trace_id").is_none());
    }

    #[tokio::test]
    async fn test_embeddings_sanitization_updates_content_length_header() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "text-embedding-ada-002".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "object": "list",
            "data": [{
                "object": "embedding",
                "index": 0,
                "embedding": [0.1, 0.2, 0.3]
            }],
            "model": "provider-embedding-model",
            "usage": {
                "prompt_tokens": 5,
                "total_tokens": 5
            },
            "provider_metadata": "extra_data",
            "cost": 0.0001,
            "processing_time_ms": 123
        }"#;

        let mut mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        mock_client.set_header("content-length", mock_response.len().to_string());

        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"text-embedding-ada-002","input":"test"}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/embeddings")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        let body_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body_json["model"], "text-embedding-ada-002");
        assert!(body_json.get("provider_metadata").is_none());
        assert!(body_json.get("cost").is_none());
        assert!(body_json.get("processing_time_ms").is_none());
    }

    #[tokio::test]
    async fn test_trusted_target_sanitizes_success_responses() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("gpt-4".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1234567890,
            "model": "gpt-4-actual-provider-model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            },
            "provider_metadata": {
                "cost": 0.001,
                "trace_id": "trace-123"
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "gpt-4",
            "messages": [{"role": "user", "content": "Hello"}]
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();


        assert_eq!(
            response_json["model"], "gpt-4",
            "Trusted target should still sanitize success responses - model rewritten"
        );

        assert!(
            response_json.get("provider_metadata").is_none(),
            "Trusted target should still sanitize success responses - metadata removed"
        );

        assert_eq!(response_json["object"], "chat.completion");
        assert_eq!(response_json["choices"][0]["message"]["content"], "Hello!");

        assert_eq!(response_json["usage"]["total_tokens"], 15);
    }

    #[tokio::test]
    async fn test_trusted_target_bypasses_error_sanitization() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("gpt-4".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_error = r#"{
            "error": {
                "message": "Internal provider error: GPU cluster unavailable in eu-west-3",
                "type": "provider_error",
                "code": "internal_error",
                "metadata": {
                    "provider": "openai",
                    "region": "eu-west-3",
                    "trace_id": "trace-abc-123"
                }
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::INTERNAL_SERVER_ERROR, mock_error);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "gpt-4",
            "messages": [{"role": "user", "content": "Hello"}]
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(
            response_json["error"]["message"],
            "Internal provider error: GPU cluster unavailable in eu-west-3",
            "Trusted target should preserve original error message"
        );

        assert_eq!(response_json["error"]["code"], "internal_error");

        assert!(response_json["error"]["metadata"].is_object());
        assert_eq!(response_json["error"]["metadata"]["provider"], "openai");
        assert_eq!(response_json["error"]["metadata"]["region"], "eu-west-3");
        assert_eq!(
            response_json["error"]["metadata"]["trace_id"],
            "trace-abc-123"
        );
    }

    #[tokio::test]
    async fn test_trusted_target_account_class_error_not_masked() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("gpt-4".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_error = r#"{"error":{"message":"You have insufficient credits","type":"billing_error","code":"insufficient_quota"}}"#;
        let mock_client = MockHttpClient::new(StatusCode::PAYMENT_REQUIRED, mock_error);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::PAYMENT_REQUIRED,
            "trusted account-class error must keep its original status, not be masked to 502"
        );

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["error"]["message"], "You have insufficient credits");
    }

    fn upstream_failures_for_402(trusted: bool) -> (StatusCode, Vec<String>) {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use metrics_exporter_prometheus::PrometheusBuilder;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://openrouter.ai/api/v1".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            trusted,
            Vec::new(),
        );
        targets_map.insert("gpt-4".to_string(), pool.into());
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };
        let mock_error = r#"{"error":{"message":"Insufficient credits","code":402}}"#;
        let mock_client = MockHttpClient::new(StatusCode::PAYMENT_REQUIRED, mock_error);
        let router =
            crate::strict::build_strict_router(crate::AppState::with_client(targets, mock_client));
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}"#,
            ))
            .unwrap();

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let status = metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async { router.oneshot(request).await.unwrap().status() })
        });
        let lines = handle
            .render()
            .lines()
            .filter(|line| line.starts_with("onwards_upstream_failed_total{"))
            .map(str::to_string)
            .collect();
        (status, lines)
    }

    #[test]
    fn sanitized_third_party_error_counts_provider_status_and_host() {
        let (status, lines) = upstream_failures_for_402(false);

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(lines.len(), 1, "expected one counter line, got {lines:?}");
        let line = &lines[0];
        for label in [
            r#"reason="third_party_error""#,
            r#"status="402""#,
            r#"served_by="openrouter.ai""#,
            r#"model="gpt-4""#,
            r#"traffic="realtime""#,
        ] {
            assert!(line.contains(label), "{label} missing from {line}");
        }
        assert!(line.ends_with(" 1"), "expected a count of 1: {line}");
    }

    #[test]
    fn trusted_provider_error_is_not_counted_as_third_party() {
        let (status, lines) = upstream_failures_for_402(true);

        assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
        assert!(lines.is_empty(), "unexpected counter lines: {lines:?}");
    }

    #[tokio::test]
    async fn test_trusted_target_bypasses_streaming_error_sanitization() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("gpt-4".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_error = r#"{
            "error": {
                "message": "Rate limit exceeded in streaming mode: too many concurrent streams",
                "type": "provider_error",
                "code": "rate_limit_exceeded",
                "metadata": {
                    "provider": "openai",
                    "concurrent_streams": 150,
                    "max_streams": 100,
                    "trace_id": "trace-stream-456"
                }
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::TOO_MANY_REQUESTS, mock_error);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "gpt-4",
            "messages": [{"role": "user", "content": "Hello"}],
            "stream": true
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(
            response_json["error"]["message"],
            "Rate limit exceeded in streaming mode: too many concurrent streams",
            "Trusted target should bypass error sanitization for streaming requests"
        );

        assert_eq!(response_json["error"]["type"], "provider_error");

        assert!(response_json["error"]["metadata"].is_object());
        assert_eq!(response_json["error"]["metadata"]["provider"], "openai");
        assert_eq!(
            response_json["error"]["metadata"]["concurrent_streams"],
            150
        );
        assert_eq!(response_json["error"]["metadata"]["max_streams"], 100);
        assert_eq!(
            response_json["error"]["metadata"]["trace_id"],
            "trace-stream-456"
        );
    }

    #[tokio::test]
    async fn test_trusted_target_bypasses_error_sanitization_responses() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("gpt-4o-mini".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_error = r#"{
            "error": {
                "message": "Provider-specific error: vLLM out of memory",
                "type": "provider_error",
                "code": "oom_error",
                "metadata": {
                    "provider": "vllm",
                    "gpu_id": "3",
                    "trace_id": "vllm-trace-456"
                }
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::INTERNAL_SERVER_ERROR, mock_error);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4o-mini","input":"Test message"}"#;

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(
            response_json["error"]["message"], "Provider-specific error: vLLM out of memory",
            "Trusted target should preserve original error message for /v1/responses"
        );
        assert_eq!(response_json["error"]["code"], "oom_error");
        assert!(response_json["error"]["metadata"].is_object());
        assert_eq!(response_json["error"]["metadata"]["provider"], "vllm");
        assert_eq!(
            response_json["error"]["metadata"]["trace_id"],
            "vllm-trace-456"
        );
    }

    #[tokio::test]
    async fn test_trusted_target_bypasses_error_sanitization_embeddings() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("text-embedding-ada-002".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_error = r#"{
            "error": {
                "message": "Embedding service error: Token limit 8192 exceeded for input text",
                "type": "invalid_request_error",
                "code": "context_length_exceeded",
                "metadata": {
                    "provider": "openai",
                    "max_tokens": 8192,
                    "actual_tokens": 9500,
                    "trace_id": "emb-trace-789"
                }
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::BAD_REQUEST, mock_error);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "text-embedding-ada-002",
            "input": "Test text for embedding"
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/embeddings")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(
            response_json["error"]["message"],
            "Embedding service error: Token limit 8192 exceeded for input text",
            "Trusted target should preserve original error message for /v1/embeddings"
        );
        assert_eq!(response_json["error"]["code"], "context_length_exceeded");
        assert!(response_json["error"]["metadata"].is_object());
        assert_eq!(response_json["error"]["metadata"]["max_tokens"], 8192);
        assert_eq!(response_json["error"]["metadata"]["actual_tokens"], 9500);
        assert_eq!(
            response_json["error"]["metadata"]["trace_id"],
            "emb-trace-789"
        );
    }

    #[tokio::test]
    async fn test_trusted_target_sanitizes_success_responses_api() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("gpt-4o-mini".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "resp-123",
            "object": "response",
            "created_at": 1677652288,
            "completed_at": 1677652290,
            "status": "completed",
            "incomplete_details": null,
            "model": "gpt-4o-mini-actual-provider",
            "previous_response_id": null,
            "instructions": null,
            "output": [],
            "error": null,
            "tools": [],
            "tool_choice": "auto",
            "truncation": "auto",
            "parallel_tool_calls": true,
            "text": {},
            "top_p": 1.0,
            "presence_penalty": 0.0,
            "frequency_penalty": 0.0,
            "top_logprobs": 0,
            "temperature": 1.0,
            "reasoning": null,
            "usage": null,
            "max_output_tokens": null,
            "max_tool_calls": null,
            "store": false,
            "background": false,
            "service_tier": "default",
            "metadata": null,
            "safety_identifier": null,
            "prompt_cache_key": null,
            "provider_metadata": {
                "cost": 0.002,
                "trace_id": "trace-responses-456"
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4o-mini","input":"Test message"}"#;

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(
            response_json["model"], "gpt-4o-mini",
            "Trusted target should still sanitize /v1/responses success - model rewritten"
        );

        assert!(
            response_json.get("provider_metadata").is_none(),
            "Trusted target should still sanitize /v1/responses success - metadata removed"
        );

        assert_eq!(response_json["object"], "response");
        assert_eq!(response_json["status"], "completed");
        assert!(response_json["output"].is_array());
    }

    #[tokio::test]
    async fn test_trusted_target_sanitizes_success_embeddings() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("text-embedding-ada-002".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "object": "list",
            "data": [
                {
                    "object": "embedding",
                    "embedding": [0.1, 0.2, 0.3],
                    "index": 0
                }
            ],
            "model": "text-embedding-ada-002-actual-provider",
            "usage": {
                "prompt_tokens": 8,
                "total_tokens": 8
            },
            "provider_metadata": {
                "cost": 0.0001,
                "trace_id": "trace-emb-789",
                "region": "us-east-1"
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "text-embedding-ada-002",
            "input": "Test text for embedding"
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/embeddings")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(
            response_json["model"], "text-embedding-ada-002",
            "Trusted target should still sanitize /v1/embeddings success - model rewritten"
        );

        assert!(
            response_json.get("provider_metadata").is_none(),
            "Trusted target should still sanitize /v1/embeddings success - metadata removed"
        );

        assert_eq!(response_json["object"], "list");
        assert_eq!(response_json["data"][0]["embedding"][0], 0.1);
        assert_eq!(response_json["usage"]["total_tokens"], 8);
    }

    #[tokio::test]
    async fn test_responses_sanitization_updates_content_length_header() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "resp_123",
            "object": "response",
            "created_at": 1234567890,
            "completed_at": 1234567900,
            "status": "completed",
            "incomplete_details": null,
            "model": "provider-model",
            "previous_response_id": null,
            "instructions": null,
            "output": [],
            "error": null,
            "tools": [],
            "tool_choice": "auto",
            "truncation": "disabled",
            "parallel_tool_calls": true,
            "text": {
                "format": {
                    "type": "text"
                }
            },
            "top_p": 1.0,
            "presence_penalty": 0.0,
            "frequency_penalty": 0.0,
            "top_logprobs": 0,
            "temperature": 1.0,
            "reasoning": null,
            "usage": null,
            "max_output_tokens": null,
            "max_tool_calls": null,
            "store": false,
            "background": false,
            "service_tier": "default",
            "metadata": null,
            "safety_identifier": null,
            "prompt_cache_key": null,
            "provider_trace_id": "xyz-123",
            "internal_cost": 0.456,
            "custom_field": "should_be_removed"
        }"#;

        let mut mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        mock_client.set_header("content-length", mock_response.len().to_string());

        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model":"gpt-4o","input":"test"}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        let body_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body_json["model"], "gpt-4o");
        assert!(body_json.get("provider_trace_id").is_none());
        assert!(body_json.get("internal_cost").is_none());
        assert!(body_json.get("custom_field").is_none());
    }

    #[tokio::test]
    async fn test_streaming_multiline_sse_events_are_sanitized() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let chunk1 = r#"data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1234567890,"model":"provider-model","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}],"provider":"leaked-provider","cost":0.001}

"#;

        let mock_client = MockHttpClient::new_streaming(StatusCode::OK, vec![chunk1.to_string()]);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body =
            r#"{"model":"gpt-4","messages":[{"role":"user","content":"test"}],"stream":true}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"model\":\"gpt-4\""));

        assert!(
            !body_str.contains("provider"),
            "Provider field should be removed"
        );
        assert!(!body_str.contains("cost"), "Cost field should be removed");
        assert!(
            !body_str.contains("leaked-provider"),
            "Provider name should not leak"
        );
    }

    #[tokio::test]
    async fn test_streaming_sse_comments_dont_leak_metadata() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let chunks = vec![
            ": provider=custom-llm cost=0.001 trace_id=xyz-123\n".to_string(),
            r#"data: {"id":"chatcmpl-123","object":"chat.completion.chunk","created":1234567890,"model":"provider-model","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}

"#
            .to_string(),
        ];

        let mock_client = MockHttpClient::new_streaming(StatusCode::OK, chunks);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body =
            r#"{"model":"gpt-4","messages":[{"role":"user","content":"test"}],"stream":true}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(
            !body_str.contains("provider=custom-llm"),
            "Provider metadata in comments should be stripped"
        );
        assert!(
            !body_str.contains("trace_id=xyz-123"),
            "Trace ID should be stripped"
        );
        assert!(!body_str.contains("cost=0.001"), "Cost should be stripped");

        assert!(body_str.contains("\"model\":\"gpt-4\""));
    }

    #[tokio::test]
    async fn test_responses_api_omitted_fields_not_injected_as_null() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "resp_123",
            "object": "response",
            "created_at": 1234567890,
            "completed_at": 1234567900,
            "status": "completed",
            "incomplete_details": null,
            "model": "gpt-4o",
            "previous_response_id": null,
            "instructions": null,
            "output": [],
            "error": null,
            "tools": [],
            "tool_choice": "auto",
            "truncation": "disabled",
            "parallel_tool_calls": true,
            "text": { "format": { "type": "text" } },
            "top_p": 1.0,
            "presence_penalty": 0.0,
            "frequency_penalty": 0.0,
            "top_logprobs": 0,
            "temperature": 1.0,
            "reasoning": null,
            "usage": null,
            "max_output_tokens": null,
            "max_tool_calls": null,
            "store": false,
            "background": false,
            "service_tier": "default",
            "metadata": null,
            "safety_identifier": null,
            "prompt_cache_key": null
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client.clone());
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "gpt-4o",
            "input": [{
                "type": "message",
                "role": "user",
                "content": "Hello"
            }]
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let _response = router.oneshot(request).await.unwrap();

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);
        let forwarded_body = String::from_utf8(requests[0].body.clone()).unwrap();
        let forwarded_json: serde_json::Value = serde_json::from_str(&forwarded_body).unwrap();

        let input_items = forwarded_json["input"].as_array().unwrap();
        assert_eq!(input_items.len(), 1);

        let message_item = &input_items[0];

        assert!(
            !message_item.as_object().unwrap().contains_key("id"),
            "Optional 'id' field should not be present when omitted in request, found: {:?}",
            message_item
        );
        assert!(
            !message_item.as_object().unwrap().contains_key("status"),
            "Optional 'status' field should not be present when omitted in request, found: {:?}",
            message_item
        );
    }

    #[tokio::test]
    async fn test_responses_api_unknown_item_types_preserved() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-4o".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "resp_123",
            "object": "response",
            "created_at": 1234567890,
            "completed_at": 1234567900,
            "status": "completed",
            "incomplete_details": null,
            "model": "gpt-4o",
            "previous_response_id": null,
            "instructions": null,
            "output": [],
            "error": null,
            "tools": [],
            "tool_choice": "auto",
            "truncation": "disabled",
            "parallel_tool_calls": true,
            "text": { "format": { "type": "text" } },
            "top_p": 1.0,
            "presence_penalty": 0.0,
            "frequency_penalty": 0.0,
            "top_logprobs": 0,
            "temperature": 1.0,
            "reasoning": null,
            "usage": null,
            "max_output_tokens": null,
            "max_tool_calls": null,
            "store": false,
            "background": false,
            "service_tier": "default",
            "metadata": null,
            "safety_identifier": null,
            "prompt_cache_key": null
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client.clone());
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "gpt-4o",
            "input": [{
                "type": "web_search",
                "query": "latest news",
                "max_results": 10,
                "custom_field": "should be preserved"
            }]
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/responses")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let _response = router.oneshot(request).await.unwrap();

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);
        let forwarded_body = String::from_utf8(requests[0].body.clone()).unwrap();
        let forwarded_json: serde_json::Value = serde_json::from_str(&forwarded_body).unwrap();

        let input_items = forwarded_json["input"].as_array().unwrap();
        assert_eq!(input_items.len(), 1);

        let unknown_item = &input_items[0];

        assert_eq!(unknown_item["type"], "web_search");
        assert_eq!(unknown_item["query"], "latest news");
        assert_eq!(unknown_item["max_results"], 10);
        assert_eq!(
            unknown_item["custom_field"], "should be preserved",
            "Unknown item fields should be preserved, but got: {:?}",
            unknown_item
        );
    }

    #[tokio::test]
    async fn test_strict_mode_ignores_target_sanitize_response_flag() {

        use crate::target::{Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .sanitize_response(true)
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1234567890,
            "model": "gpt-4-actual-provider-model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            },
            "provider_metadata": {
                "cost": 0.001,
                "trace_id": "trace-123"
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = crate::AppState::with_client(targets, mock_client)
            .with_response_transform(crate::create_openai_sanitizer());

        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "gpt-4",
            "messages": [{"role": "user", "content": "Hello"}]
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(response_json["model"], "gpt-4");

        assert!(
            response_json.get("provider_metadata").is_none(),
            "Provider metadata should be removed by strict mode sanitization"
        );

        assert_eq!(response_json["object"], "chat.completion");
        assert_eq!(response_json["choices"][0]["message"]["content"], "Hello!");

    }

    #[tokio::test]
    async fn test_untrusted_target_still_sanitized() {
        use crate::target::{Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "third-party".to_string(),
            Target::builder()
                .url("https://third-party.com".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "chatcmpl-123",
            "object": "chat.completion",
            "created": 1234567890,
            "model": "provider-internal-model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello!"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            },
            "provider_metadata": {
                "should": "be removed"
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "third-party",
            "messages": [{"role": "user", "content": "Hello"}]
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

        assert_eq!(
            response_json["model"], "third-party",
            "Untrusted target should have model field rewritten"
        );

        assert!(
            response_json.get("provider_metadata").is_none(),
            "Untrusted target should have provider metadata removed"
        );

        assert_eq!(response_json["choices"][0]["message"]["content"], "Hello!");
    }

    #[tokio::test]
    async fn test_model_override_header_prevents_trust_bypass() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());

        let trusted_pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://trusted.com".parse().unwrap())
                    .onwards_key("sk-trusted".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("trusted-pool".to_string(), trusted_pool.into());

        let untrusted_pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://untrusted.com".parse().unwrap())
                    .onwards_key("sk-untrusted".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            false,
            Vec::new(),
        );
        targets_map.insert("untrusted-pool".to_string(), untrusted_pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "chatcmpl-bypass-attempt",
            "object": "chat.completion",
            "created": 1234567890,
            "model": "untrusted-internal-model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Response from untrusted provider"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            },
            "untrusted_metadata": {
                "should_be_removed": "yes",
                "cost": "$0.001",
                "trace_id": "leak-attempt-123"
            }
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "trusted-pool",
            "messages": [{"role": "user", "content": "Try to bypass"}]
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .header("model-override", "untrusted-pool")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();


        assert!(
            response_json.get("untrusted_metadata").is_none(),
            "SECURITY: Untrusted metadata MUST be removed - bypass attempt prevented"
        );

        assert_ne!(
            response_json["model"], "untrusted-internal-model",
            "Model should not be provider's internal model name"
        );

        assert_eq!(
            response_json["choices"][0]["message"]["content"],
            "Response from untrusted provider"
        );

    }

    #[tokio::test]
    async fn test_trusted_streaming_success_responses_still_sanitized() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("gpt-4".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_stream = "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1234567890,\"model\":\"gpt-4-provider\",\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"index\":0,\"finish_reason\":null}],\"provider_cost\":0.001}\n\ndata: [DONE]\n\n";

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_stream);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{
            "model": "gpt-4",
            "messages": [{"role": "user", "content": "Hello"}],
            "stream": true
        }"#;

        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let response_str = String::from_utf8(body_bytes.to_vec()).unwrap();

        assert!(
            response_str.contains("\"model\":\"gpt-4\""),
            "Trusted pool streaming success should still sanitize - model rewritten"
        );
        assert!(
            !response_str.contains("\"model\":\"gpt-4-provider\""),
            "Original provider model should be replaced"
        );

        assert!(
            !response_str.contains("\"provider_cost\""),
            "Trusted pool streaming success should still sanitize - metadata removed"
        );
    }

    #[tokio::test]
    async fn test_provider_trusted_overrides_untrusted_pool() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .trusted(true)
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            false,
            Vec::new(),
        );
        targets_map.insert("gpt-4".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let error_body = r#"{"error": {"message": "provider specific error", "type": "rate_limit_error", "provider_trace": "trace-xyz"}}"#;
        let mock_client = MockHttpClient::new(StatusCode::TOO_MANY_REQUESTS, error_body);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model": "gpt-4", "messages": [{"role": "user", "content": "Hi"}]}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = std::str::from_utf8(&body_bytes).unwrap();
        assert!(
            body_str.contains("provider_trace"),
            "Provider-specific fields should pass through for trusted provider (even with untrusted pool)"
        );
        assert!(
            body_str.contains("provider specific error"),
            "Original provider message should pass through for trusted provider"
        );
    }

    #[tokio::test]
    async fn test_provider_untrusted_overrides_trusted_pool() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::{LoadBalanceStrategy, Target, Targets};
        use crate::test_utils::MockHttpClient;
        use axum::body::Body;
        use axum::http::Request;
        use dashmap::DashMap;
        use std::sync::Arc;
        use tower::ServiceExt;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .trusted(false)
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("gpt-4".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let error_body = r#"{"error": {"message": "provider specific error", "type": "rate_limit_error", "provider_trace": "trace-xyz"}}"#;
        let mock_client = MockHttpClient::new(StatusCode::TOO_MANY_REQUESTS, error_body);
        let state = crate::AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request_body = r#"{"model": "gpt-4", "messages": [{"role": "user", "content": "Hi"}]}"#;
        let request = Request::builder()
            .method("POST")
            .uri("/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);

        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = std::str::from_utf8(&body_bytes).unwrap();
        assert!(
            !body_str.contains("provider_trace"),
            "Provider-specific fields should be stripped for explicitly untrusted provider (even with trusted pool)"
        );
        assert!(
            !body_str.contains("provider specific error"),
            "Original provider message should be stripped for explicitly untrusted provider"
        );
        assert!(
            body_str.contains("Rate limit exceeded"),
            "Sanitized standard message should be present"
        );
    }


    fn completions_mock_response(model: &str) -> String {
        format!(
            r#"{{
                "id": "cmpl-abc123",
                "object": "text_completion",
                "created": 1677652288,
                "model": "{model}",
                "choices": [{{
                    "text": "Hello, world!",
                    "index": 0,
                    "logprobs": null,
                    "finish_reason": "stop"
                }}],
                "usage": {{
                    "prompt_tokens": 5,
                    "completion_tokens": 7,
                    "total_tokens": 12
                }}
            }}"#
        )
    }

    fn completions_test_targets(model: &str) -> Targets {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            model.to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .build()
                .into_pool(),
        );
        Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        }
    }

    #[tokio::test]
    async fn test_completions_accepts_missing_prompt() {
        let mock_client = MockHttpClient::new(
            StatusCode::OK,
            &completions_mock_response("gpt-3.5-turbo-instruct"),
        );
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"model":"gpt-3.5-turbo-instruct"}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_completions_rejects_missing_model() {
        let mock_client = MockHttpClient::new(StatusCode::OK, "{}");
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"prompt":"Say hello"}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn test_completions_forwards_to_completions_endpoint() {
        let mock_client = MockHttpClient::new(
            StatusCode::OK,
            &completions_mock_response("gpt-3.5-turbo-instruct"),
        );
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client.clone(),
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Say hello"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let requests = mock_client.get_requests();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0].uri.contains("completions"),
            "Request should be forwarded to /completions, got: {}",
            requests[0].uri
        );
        assert!(
            !requests[0].uri.contains("chat"),
            "Request must NOT be forwarded to /chat/completions"
        );
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_removes_unknown_fields() {
        let mock_response = r#"{
            "id": "cmpl-abc123",
            "object": "text_completion",
            "created": 1677652288,
            "model": "gpt-3.5-turbo-instruct",
            "choices": [{"text": "Hello!", "index": 0, "logprobs": null, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 7, "total_tokens": 12},
            "provider": "custom-provider",
            "cost": 0.0001,
            "internal_id": "xyz-456"
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Say hello"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"object\":\"text_completion\""));
        assert!(body_str.contains("\"id\":\"cmpl-abc123\""));
        assert!(body_str.contains("\"choices\""));
        assert!(body_str.contains("\"text\":\"Hello!\""));

        assert!(!body_str.contains("\"provider\""));
        assert!(!body_str.contains("\"cost\""));
        assert!(!body_str.contains("\"internal_id\""));
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_rewrites_model() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-3.5-turbo-instruct".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .onwards_model("gpt-3.5-turbo-instruct-internal-v2".to_string())
                .build()
                .into_pool(),
        );
        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = completions_mock_response("gpt-3.5-turbo-instruct-internal-v2");
        let mock_client = MockHttpClient::new(StatusCode::OK, &mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"model\":\"gpt-3.5-turbo-instruct\""));
        assert!(!body_str.contains("internal-v2"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_backfills_missing_noncritical_fields() {
        let mock_response = r#"{
            "choices": [{"text": "Hello!", "finish_reason": "stop"}]
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Say hello"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["model"], "gpt-3.5-turbo-instruct");
        assert_eq!(json["object"], "text_completion");
        let id = json["id"].as_str().expect("id should be a string");
        assert!(id.starts_with("cmpl-"));
        assert_eq!(json["choices"][0]["index"], 0);
        assert_eq!(json["choices"][0]["text"], "Hello!");
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_backfills_choice_indexes_from_position() {
        let mock_response = r#"{
            "choices": [{"text": "first"}, {"text": "second"}]
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Say hello"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["choices"][0]["index"], 0);
        assert_eq!(json["choices"][1]["index"], 1);
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_does_not_backfill_generated_text() {
        let mock_response = r#"{
            "choices": [{"finish_reason": "stop"}]
        }"#;

        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Say hello"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"]["type"], "api_error");
    }

    #[tokio::test]
    async fn test_completions_sanitization_updates_content_length_header() {
        let mock_response = r#"{
            "id": "cmpl-abc123",
            "object": "text_completion",
            "created": 1677652288,
            "model": "gpt-3.5-turbo-instruct",
            "choices": [{"text": "Hi", "index": 0, "logprobs": null, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6},
            "provider_metadata": "extra_data_that_will_be_stripped",
            "cost": 0.0001,
            "processing_time_ms": 123
        }"#;

        let mut mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        mock_client.set_header("content-length", mock_response.len().to_string());

        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hi"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let content_length: usize = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .expect("content-length header should be present");

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(content_length, body.len());
        assert!(
            content_length < mock_response.len(),
            "sanitized body should be smaller"
        );

        assert_eq!(body_json["model"], "gpt-3.5-turbo-instruct");
        assert!(body_json.get("provider_metadata").is_none());
        assert!(body_json.get("cost").is_none());
    }

    #[tokio::test]
    async fn test_completions_array_prompt_accepted() {
        let mock_client = MockHttpClient::new(
            StatusCode::OK,
            &completions_mock_response("gpt-3.5-turbo-instruct"),
        );
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client.clone(),
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":["Hello","World"]}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let requests = mock_client.get_requests();
        let request_json: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert!(request_json["prompt"].is_array());
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_streaming_removes_unknown_fields() {
        let mock_client = MockHttpClient::new_streaming(
            StatusCode::OK,
            vec![
                "data: {\"id\":\"cmpl-abc\",\"object\":\"text_completion\",\"created\":1677652288,\"model\":\"gpt-3.5-turbo-instruct\",\"choices\":[{\"text\":\"Hello\",\"index\":0,\"logprobs\":null,\"finish_reason\":null}],\"provider\":\"custom\",\"cost\":0.001}\n\n".to_string(),
                "data: [DONE]\n\n".to_string(),
            ],
        );
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello","stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"object\":\"text_completion\""));
        assert!(body_str.contains("\"text\":\"Hello\""));
        assert!(body_str.contains("[DONE]"));

        assert!(!body_str.contains("\"provider\""));
        assert!(!body_str.contains("\"cost\""));
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_streaming_rewrites_model() {
        let targets = Arc::new(DashMap::new());
        targets.insert(
            "gpt-3.5-turbo-instruct".to_string(),
            Target::builder()
                .url("https://api.openai.com/v1/".parse().unwrap())
                .onwards_key("sk-test".to_string())
                .onwards_model("gpt-3.5-turbo-instruct-0914".to_string())
                .build()
                .into_pool(),
        );
        let targets = Targets {
            targets,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_client = MockHttpClient::new_streaming(
            StatusCode::OK,
            vec![
                "data: {\"id\":\"cmpl-abc\",\"object\":\"text_completion\",\"created\":1677652288,\"model\":\"gpt-3.5-turbo-instruct-0914\",\"choices\":[{\"text\":\"Hi\",\"index\":0,\"logprobs\":null,\"finish_reason\":null}]}\n\n".to_string(),
                "data: {\"id\":\"cmpl-abc\",\"object\":\"text_completion\",\"created\":1677652288,\"model\":\"gpt-3.5-turbo-instruct-0914\",\"choices\":[{\"text\":\"\",\"index\":0,\"logprobs\":null,\"finish_reason\":\"stop\"}]}\n\n".to_string(),
                "data: [DONE]\n\n".to_string(),
            ],
        );
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello","stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"model\":\"gpt-3.5-turbo-instruct\""));
        assert!(!body_str.contains("0914"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_streaming_backfills_missing_noncritical_fields() {
        let mock_client = MockHttpClient::new_streaming(
            StatusCode::OK,
            vec![
                "data: {\"choices\":[{\"text\":\"Hello\"}]}\n\n".to_string(),
                "data: [DONE]\n\n".to_string(),
            ],
        );
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello","stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"object\":\"text_completion\""));
        assert!(body_str.contains("\"model\":\"gpt-3.5-turbo-instruct\""));
        assert!(body_str.contains("\"index\":0"));
        assert!(body_str.contains("\"text\":\"Hello\""));
        assert!(body_str.contains("\"id\":\"cmpl-"));
        assert!(body_str.contains("[DONE]"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_streaming_backfills_missing_chunk_text() {
        let mock_client = MockHttpClient::new_streaming(
            StatusCode::OK,
            vec![
                "data: {\"choices\":[{\"finish_reason\":null}]}\n\n".to_string(),
                "data: [DONE]\n\n".to_string(),
            ],
        );
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello","stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"text\":\"\""));
        assert!(body_str.contains("\"index\":0"));
        assert!(body_str.contains("[DONE]"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_streaming_backfills_chunk_indexes_from_position() {
        let mock_client = MockHttpClient::new_streaming(
            StatusCode::OK,
            vec![
                "data: {\"choices\":[{\"text\":\"first\"},{\"text\":\"second\"}]}\n\n".to_string(),
                "data: [DONE]\n\n".to_string(),
            ],
        );
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello","stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(body_str.contains("\"index\":0"));
        assert!(body_str.contains("\"index\":1"));
    }

    #[tokio::test]
    async fn test_strict_sanitize_completions_streaming_reuses_fallback_id_across_chunks() {
        let mock_client = MockHttpClient::new_streaming(
            StatusCode::OK,
            vec![
                "data: {\"choices\":[{\"text\":\"first\"}]}\n\n".to_string(),
                "data: {\"choices\":[{\"text\":\"second\"}]}\n\n".to_string(),
                "data: [DONE]\n\n".to_string(),
            ],
        );
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello","stream":true}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        let ids: Vec<String> = body_str
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter(|data| *data != "[DONE]")
            .map(|data| serde_json::from_str::<serde_json::Value>(data).unwrap())
            .filter_map(|json| json.get("id").and_then(|v| v.as_str()).map(str::to_string))
            .collect();

        assert!(ids.len() >= 2);
        assert!(ids.iter().all(|id| id == &ids[0]));
    }

    #[tokio::test]
    async fn test_completions_untrusted_error_sanitized() {
        let mock_error = r#"{"error":{"message":"Provider internal error: OOM on GPU 3","code":"oom","provider":"custom-llm"}}"#;
        let mock_client = MockHttpClient::new(StatusCode::INTERNAL_SERVER_ERROR, mock_error);
        let state = AppState::with_client(
            completions_test_targets("gpt-3.5-turbo-instruct"),
            mock_client,
        );
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        assert!(!body_str.contains("OOM on GPU 3"));
        assert!(!body_str.contains("custom-llm"));
        assert!(body_str.contains("error"));
    }

    #[tokio::test]
    async fn test_completions_trusted_error_passed_through() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::LoadBalanceStrategy;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com/v1/".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("gpt-3.5-turbo-instruct".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_error = r#"{"error":{"message":"Context length exceeded","type":"invalid_request_error","code":"context_length_exceeded"}}"#;
        let mock_client = MockHttpClient::new(StatusCode::BAD_REQUEST, mock_error);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hello"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body_json["error"]["message"], "Context length exceeded");
        assert_eq!(body_json["error"]["code"], "context_length_exceeded");
    }

    #[tokio::test]
    async fn test_completions_trusted_success_still_sanitized() {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::LoadBalanceStrategy;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://api.openai.com/v1/".parse().unwrap())
                    .onwards_key("sk-test".to_string())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert("gpt-3.5-turbo-instruct".to_string(), pool.into());

        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };

        let mock_response = r#"{
            "id": "cmpl-abc",
            "object": "text_completion",
            "created": 1677652288,
            "model": "gpt-3.5-turbo-instruct-actual",
            "choices": [{"text": "Hi!", "index": 0, "logprobs": null, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
            "provider_metadata": {"cost": 0.0001}
        }"#;
        let mock_client = MockHttpClient::new(StatusCode::OK, mock_response);
        let state = AppState::with_client(targets, mock_client);
        let router = crate::strict::build_strict_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/completions")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"model":"gpt-3.5-turbo-instruct","prompt":"Hi"}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body_json["model"], "gpt-3.5-turbo-instruct");
        assert_ne!(body_json["model"], "gpt-3.5-turbo-instruct-actual");
        assert!(body_json.get("provider_metadata").is_none());
    }

    #[test]
    fn test_try_format_sse_error_trusted_forwards_verbatim() {
        let data_part =
            r#"{"error": {"message": "Input too long", "type": "BadRequestError", "code": 400}}"#;
        let value: serde_json::Value = serde_json::from_str(data_part).unwrap();
        let result = try_format_sse_error(&value, true);
        assert!(result.is_some());
        let line = result.unwrap();
        assert!(line.starts_with("data: "));
        assert!(line.contains("Input too long"));
    }

    #[test]
    fn test_try_format_sse_error_untrusted_sanitizes() {
        let data_part =
            r#"{"error": {"message": "OOM on GPU 3", "type": "BadRequestError", "code": 400}}"#;
        let value: serde_json::Value = serde_json::from_str(data_part).unwrap();
        let result = try_format_sse_error(&value, false);
        assert!(result.is_some());
        let line = result.unwrap();
        assert!(!line.contains("OOM on GPU 3"));
        assert!(line.contains("Invalid request"));
    }

    #[test]
    fn test_try_format_sse_error_ignores_valid_chunk() {
        let data_part = r#"{"id": "chatcmpl-123", "object": "chat.completion.chunk"}"#;
        let value: serde_json::Value = serde_json::from_str(data_part).unwrap();
        assert!(try_format_sse_error(&value, true).is_none());
    }

    #[test]
    fn test_try_format_sse_error_ignores_non_object_error() {
        let null_error = serde_json::json!({"error": null});
        assert!(try_format_sse_error(&null_error, true).is_none());
        let string_error = serde_json::json!({"error": "boom"});
        assert!(try_format_sse_error(&string_error, false).is_none());
    }

    #[test]
    fn test_try_format_sse_error_ignores_empty_error_object() {
        let empty_error = serde_json::json!({"error": {}});
        assert!(try_format_sse_error(&empty_error, false).is_none());
        assert!(try_format_sse_error(&empty_error, true).is_none());
    }

    #[tokio::test]
    async fn test_streaming_chat_forwards_trusted_error_in_sse() {
        let error_body = "data: {\"error\": {\"message\": \"Input too long\", \"type\": \"BadRequestError\", \"code\": 400}}\n\n";
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(error_body))
            .unwrap();

        let result = sanitize_streaming_chat_response(
            response,
            "test-model".to_string(),
            true,
            crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
            None,
            1,
        )
        .await;
        assert_eq!(result.status(), StatusCode::OK);

        let body = axum::body::to_bytes(result.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body);
        assert!(body_str.contains("Input too long"));
    }

    #[tokio::test]
    async fn test_streaming_chat_sanitizes_untrusted_error_in_sse() {
        let error_body = "data: {\"error\": {\"message\": \"OOM on GPU 3\", \"type\": \"server_error\", \"code\": 400}}\n\n";
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(error_body))
            .unwrap();

        let result = sanitize_streaming_chat_response(
            response,
            "test-model".to_string(),
            false,
            crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
            None,
            1,
        )
        .await;
        assert_eq!(result.status(), StatusCode::OK);

        let body = axum::body::to_bytes(result.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body);
        assert!(!body_str.contains("OOM on GPU 3"));
        assert!(body_str.contains("\"error\""));
    }

    #[tokio::test]
    async fn test_streaming_chat_surfaces_openrouter_shape_embedded_error() {
        let chunk_with_embedded_error = "data: {\"id\":\"gen-abc\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"openai/gpt-oss-20b\",\"choices\":[],\"error\":{\"message\":\"Provider returned error\",\"code\":429,\"metadata\":{\"raw\":\"You have been rate limited\"}}}\n\n";
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(chunk_with_embedded_error))
            .unwrap();

        let result = sanitize_streaming_chat_response(
            response,
            "test-model".to_string(),
            false,
            crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
            None,
            1,
        )
        .await;
        assert_eq!(result.status(), StatusCode::OK);

        let body = axum::body::to_bytes(result.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body);

        assert!(
            body_str.contains("data: {\"error\""),
            "expected a stand-alone error event in output, got: {body_str}"
        );
        assert!(
            body_str.contains("\"code\":429"),
            "expected embedded code 429 to be preserved, got: {body_str}"
        );
        assert!(!body_str.contains("You have been rate limited"));
        assert!(
            !body_str.contains("metadata"),
            "untrusted error must strip extra fields like metadata, got: {body_str}"
        );
    }

    #[tokio::test]
    async fn test_streaming_chat_trusted_emits_envelope_only_for_embedded_error() {
        let chunk_with_embedded_error = "data: {\"id\":\"chunk-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"qwen-internal\",\"choices\":[],\"error\":{\"message\":\"Engine was shut down during token generation\",\"type\":\"internal_server_error\",\"code\":500}}\n\n";
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(chunk_with_embedded_error))
            .unwrap();

        let result = sanitize_streaming_chat_response(
            response,
            "test-model".to_string(),
            true,
            crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
            None,
            1,
        )
        .await;
        assert_eq!(result.status(), StatusCode::OK);

        let body = axum::body::to_bytes(result.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8_lossy(&body);

        assert!(
            body_str.contains("data: {\"error\""),
            "expected envelope-only emission, got: {body_str}"
        );
        assert!(body_str.contains("Engine was shut down during token generation"));
        assert!(body_str.contains("\"code\":500"));
        assert!(
            !body_str.contains("\"object\":\"chat.completion.chunk\""),
            "chunk wrapper must be stripped from the emitted error event"
        );
    }

    #[test]
    fn test_mask_account_class_status_remaps_account_codes() {
        assert_eq!(mask_account_class_status(401), 502);
        assert_eq!(mask_account_class_status(402), 502);
        assert_eq!(mask_account_class_status(403), 502);
        assert_eq!(mask_account_class_status(451), 502);
        assert_eq!(mask_account_class_status(408), 504);
        assert_eq!(mask_account_class_status(400), 400);
        assert_eq!(mask_account_class_status(404), 404);
        assert_eq!(mask_account_class_status(413), 413);
        assert_eq!(mask_account_class_status(422), 422);
        assert_eq!(mask_account_class_status(429), 429);
        assert_eq!(mask_account_class_status(500), 500);
        assert_eq!(mask_account_class_status(502), 502);
        assert_eq!(mask_account_class_status(503), 503);
        assert_eq!(mask_account_class_status(504), 504);
    }

    fn upstream_sse(events: &[&str], fail: bool) -> Response {
        let mut items: Vec<Result<Bytes, std::io::Error>> = events
            .iter()
            .map(|e| Ok(Bytes::from(format!("data: {e}\n\n"))))
            .collect();
        if fail {
            items.push(Err(std::io::Error::other("connection reset")));
        }
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(futures_util::stream::iter(items)))
            .unwrap()
    }

    async fn last_data_line(response: Response) -> String {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("an interrupted stream still ends as a complete body");
        String::from_utf8_lossy(&body)
            .lines()
            .filter_map(|l| l.strip_prefix("data: "))
            .next_back()
            .expect("at least one data line")
            .to_string()
    }

    fn assert_interrupted(last: &str) {
        let value: serde_json::Value = serde_json::from_str(last).expect("an error envelope");
        assert_eq!(value["error"]["code"], stream_end::INTERRUPTED_STATUS);
        assert_eq!(value["error"]["type"], "api_error");
    }

    const CHAT_CONTENT: &str = r#"{"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"Hel"},"finish_reason":null}]}"#;
    const CHAT_FINISH: &str = r#"{"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#;

    async fn chat(response: Response) -> String {
        last_data_line(
            sanitize_streaming_chat_response(
                response,
                "test-model".to_string(),
                true,
                crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
                None,
                1,
            )
            .await,
        )
        .await
    }

    #[tokio::test]
    async fn test_streaming_chat_transport_error_mid_answer_ends_with_error_event() {
        assert_interrupted(&chat(upstream_sse(&[CHAT_CONTENT], true)).await);
    }

    #[tokio::test]
    async fn test_streaming_chat_early_end_mid_answer_ends_with_error_event() {
        assert_interrupted(&chat(upstream_sse(&[CHAT_CONTENT], false)).await);
    }

    #[tokio::test]
    async fn test_streaming_chat_malformed_event_ends_with_error_event() {
        assert_interrupted(&chat(upstream_sse(&[CHAT_CONTENT, "{not json"], false)).await);
    }

    #[tokio::test]
    async fn test_streaming_chat_lost_trailer_ends_with_done() {
        assert_eq!(
            chat(upstream_sse(&[CHAT_CONTENT, CHAT_FINISH], true)).await,
            "[DONE]"
        );
    }

    #[tokio::test]
    async fn test_streaming_chat_complete_stream_is_not_extended() {
        let response = upstream_sse(&[CHAT_CONTENT, CHAT_FINISH, "[DONE]"], false);
        let result = sanitize_streaming_chat_response(
            response,
            "test-model".to_string(),
            true,
            crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
            None,
            1,
        )
        .await;
        let body = axum::body::to_bytes(result.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert_eq!(body.matches("[DONE]").count(), 1);
        assert!(!body.contains("\"error\""));
    }

    #[tokio::test]
    async fn test_streaming_chat_usage_on_a_content_chunk_does_not_finish_the_answer() {
        let content_with_usage = r#"{"id":"c","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"Hel"},"finish_reason":null}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}"#;
        assert_interrupted(&chat(upstream_sse(&[content_with_usage], true)).await);
    }

    #[tokio::test]
    async fn test_streaming_chat_unterminated_last_event_is_kept_apart_from_the_error_event() {
        let body = format!("data: {CHAT_CONTENT}\n\ndata: {CHAT_CONTENT}");
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(body))
            .unwrap();
        let result = sanitize_streaming_chat_response(
            response,
            "test-model".to_string(),
            true,
            crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
            None,
            1,
        )
        .await;
        let body = axum::body::to_bytes(result.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        let events: Vec<&str> = body.split("\n\n").filter(|e| !e.is_empty()).collect();
        assert_eq!(events.len(), 3, "{body}");
        for event in &events {
            let data = event
                .strip_prefix("data: ")
                .expect("one data line per event");
            serde_json::from_str::<serde_json::Value>(data).expect("each event is valid JSON");
        }
        assert_interrupted(events[2].strip_prefix("data: ").unwrap());
    }

    #[tokio::test]
    async fn test_streaming_chat_embedded_error_is_not_followed_by_another() {
        let embedded = r#"{"error":{"message":"Internal server error","type":"internal_server_error","code":500}}"#;
        let last = chat(upstream_sse(&[CHAT_CONTENT, embedded], true)).await;
        let value: serde_json::Value = serde_json::from_str(&last).unwrap();
        assert_eq!(
            value["error"]["code"], 500,
            "the upstream's own error is the last event"
        );
    }

    #[tokio::test]
    async fn test_streaming_completions_early_end_mid_answer_ends_with_error_event() {
        let content = r#"{"id":"c","object":"text_completion","created":1,"model":"m","choices":[{"index":0,"text":"Hel","finish_reason":null}]}"#;
        let result = sanitize_streaming_completions_response(
            upstream_sse(&[content], false),
            "test-model".to_string(),
            true,
            crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
            None,
            1,
        )
        .await;
        assert_interrupted(&last_data_line(result).await);
    }

    #[tokio::test]
    async fn test_streaming_responses_transport_error_before_completion_ends_with_error_event() {
        let created = r#"{"type":"response.created","sequence_number":0,"response":{"id":"resp_1","object":"response","created_at":1,"status":"in_progress","model":"m","output":[]}}"#;
        let result = sanitize_streaming_responses_response(
            upstream_sse(&[created], true),
            "test-model".to_string(),
            true,
            None,
            crate::sse::DEFAULT_SSE_BUFFER_LIMIT,
        )
        .await;
        assert_interrupted(&last_data_line(result).await);
    }

    #[test]
    fn test_try_format_sse_error_masks_untrusted_402_to_502() {
        let data_part = r#"{"error": {"message": "Insufficient credits", "code": 402}}"#;
        let value: serde_json::Value = serde_json::from_str(data_part).unwrap();
        let line = try_format_sse_error(&value, false).expect("should emit");
        assert!(
            line.contains("\"code\":502"),
            "expected code masked to 502, got: {line}"
        );
        assert!(!line.contains("Insufficient credits"));
    }

    #[test]
    fn test_try_format_sse_error_masks_untrusted_408_to_504() {
        let value = serde_json::json!({"error": {"message": "upstream timed out", "code": 408}});
        let line = try_format_sse_error(&value, false).expect("should emit");
        assert!(
            line.contains("\"code\":504"),
            "expected 408 masked to 504, got: {line}"
        );
        assert!(line.contains("Gateway timeout"));
        assert!(!line.contains("upstream timed out"));
    }

    #[test]
    fn test_try_format_sse_error_untrusted_numeric_string_code() {
        let value = serde_json::json!({"error": {"message": "slow down", "code": "429"}});
        let line = try_format_sse_error(&value, false).expect("should emit");
        assert!(
            line.contains("\"code\":429"),
            "expected numeric string code parsed to 429, got: {line}"
        );
        assert!(line.contains("rate_limit_error"));
    }

    #[test]
    fn test_try_format_sse_error_untrusted_named_rate_limit_code() {
        let value = serde_json::json!({"error": {"message": "slow down", "code": "rate_limit"}});
        let line = try_format_sse_error(&value, false).expect("should emit");
        assert!(
            line.contains("\"code\":429"),
            "expected named rate-limit code mapped to 429, got: {line}"
        );
    }

    #[tokio::test]
    async fn test_standard_error_response_masks_402_to_502() {
        let response = standard_error_response(StatusCode::PAYMENT_REQUIRED);
        assert_eq!(
            response.status(),
            StatusCode::BAD_GATEWAY,
            "402 from an untrusted provider must surface as 502"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["type"], "api_error");
    }

    #[test]
    fn test_sanitized_error_for_status_explicit_402_408() {
        let (kind, _) = sanitized_error_for_status(402);
        assert_eq!(kind, "api_error");
        let (kind, _) = sanitized_error_for_status(408);
        assert_eq!(kind, "api_error");
    }

    const DYNAMO_REFUSED_FIELD: &str = r#"{"message":"Validation: Unsupported parameter(s): `verbosity`","type":"Bad Request","code":400}"#;

    #[rstest::rstest]
    #[case::dynamo_refused_field(
        400,
        DYNAMO_REFUSED_FIELD,
        "Validation: Unsupported parameter(s): `verbosity`",
        "invalid_request_error",
        None
    )]
    #[case::dynamo_context_length(
        400,
        r#"{"message":"This model configuration accepts at most 8192 combined input and output tokens.","type":"Bad Request","code":400,"details":{"type":"context_length","limit":8192,"actual":9000}}"#,
        "This model configuration accepts at most 8192 combined input and output tokens.",
        "invalid_request_error",
        Some("context_length_exceeded")
    )]
    #[case::dynamo_unsupported_media(
        422,
        r#"{"message":"Validation: image input is not supported by this model","type":"Unprocessable Entity","code":422}"#,
        "Validation: image input is not supported by this model",
        "invalid_request_error",
        None
    )]
    #[case::dynamo_internal(
        500,
        r#"{"message":"Internal server error","type":"Internal Server Error","code":500}"#,
        "Internal server error",
        "api_error",
        None
    )]
    #[case::string_error(
        400,
        r#"{"error":"prompt is empty"}"#,
        "prompt is empty",
        "invalid_request_error",
        None
    )]
    #[case::not_json(
        400,
        "Failed to parse the request body as JSON",
        "Invalid request",
        "invalid_request_error",
        None
    )]
    #[case::empty(503, "", "Service unavailable", "api_error", None)]
    #[case::blank_message(
        400,
        r#"{"message":"  ","code":400}"#,
        "Invalid request",
        "invalid_request_error",
        None
    )]
    #[case::null_message_string_error(
        400,
        r#"{"message":null,"error":"prompt is empty"}"#,
        "prompt is empty",
        "invalid_request_error",
        None
    )]
    #[case::blank_message_string_error(
        400,
        r#"{"message":" ","error":"prompt is empty"}"#,
        "prompt is empty",
        "invalid_request_error",
        None
    )]
    #[case::numeric_message_string_error(
        400,
        r#"{"message":7,"error":"prompt is empty"}"#,
        "prompt is empty",
        "invalid_request_error",
        None
    )]
    fn test_openai_error_envelope_wraps_other_shapes(
        #[case] status: u16,
        #[case] body: &str,
        #[case] message: &str,
        #[case] error_type: &str,
        #[case] code: Option<&str>,
    ) {
        let envelope =
            openai_error_envelope(StatusCode::from_u16(status).unwrap(), body.as_bytes()).unwrap();
        assert_eq!(
            envelope,
            json!({
                "error": {
                    "message": message,
                    "type": error_type,
                    "param": null,
                    "code": code,
                }
            })
        );
    }

    #[test]
    fn test_openai_error_envelope_keeps_existing_envelope() {
        let body = r#"{"error":{"message":"Context length exceeded","type":"invalid_request_error","code":"context_length_exceeded"}}"#;
        assert_eq!(
            openai_error_envelope(StatusCode::BAD_REQUEST, body.as_bytes()),
            None
        );
    }

    #[tokio::test]
    async fn test_envelope_trusted_error_leaves_encoded_body() {
        let compressed: &'static [u8] = &[0x1f, 0x8b, 0x08, 0x00];
        let response = Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from(compressed))
            .unwrap();

        let response = envelope_trusted_error(response).await;
        assert_eq!(
            response.headers().get(header::CONTENT_ENCODING).unwrap(),
            "gzip"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), compressed);
    }

    #[rstest::rstest]
    #[case::openai_envelope(json!({"error": {"message": "x".repeat(MAX_ERROR_BODY), "type": "invalid_request_error", "param": null, "code": "context_length_exceeded"}}))]
    #[case::dynamo_shape(json!({"message": "x".repeat(MAX_ERROR_BODY), "type": "Bad Request", "code": 400}))]
    #[tokio::test]
    async fn test_envelope_trusted_error_forwards_oversized_body(#[case] error: serde_json::Value) {
        let original = Bytes::from(error.to_string());
        let chunks: Vec<Result<Bytes, std::io::Error>> = original
            .chunks(10_000)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        let response = Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from_stream(futures_util::stream::iter(chunks)))
            .unwrap();

        let response = envelope_trusted_error(response).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, original);
    }

    fn trusted_error_router(model: &str, status: StatusCode, body: &str) -> axum::Router {
        use crate::load_balancer::{Provider, ProviderPool};
        use crate::target::LoadBalanceStrategy;

        let targets_map = Arc::new(DashMap::new());
        let pool = ProviderPool::with_config(
            vec![Provider::new(
                Target::builder()
                    .url("https://dynamo.example.com/v1/".parse().unwrap())
                    .build(),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            true,
            Vec::new(),
        );
        targets_map.insert(model.to_string(), pool.into());
        let targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: true,
            http_pool_config: None,
        };
        let state = AppState::with_client(targets, MockHttpClient::new(status, body));
        crate::strict::build_strict_router(state)
    }

    #[rstest::rstest]
    #[case(
        "/chat/completions",
        r#"{"model":"m","messages":[{"role":"user","content":"Hi"}]}"#
    )]
    #[case("/completions", r#"{"model":"m","prompt":"Hi"}"#)]
    #[case("/embeddings", r#"{"model":"m","input":"Hi"}"#)]
    #[case("/responses", r#"{"model":"m","input":"Hi"}"#)]
    #[tokio::test]
    async fn test_trusted_dynamo_error_gets_openai_envelope(
        #[case] uri: &str,
        #[case] request_body: &'static str,
    ) {
        let router = trusted_error_router("m", StatusCode::BAD_REQUEST, DYNAMO_REFUSED_FIELD);
        let request = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json,
            json!({
                "error": {
                    "message": "Validation: Unsupported parameter(s): `verbosity`",
                    "type": "invalid_request_error",
                    "param": null,
                    "code": null,
                }
            })
        );
    }
}
