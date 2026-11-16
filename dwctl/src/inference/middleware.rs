
use std::sync::Arc;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use fusillade::Storage as _;
use fusillade_arsenal::PostgresRequestManager;
use sqlx_pool_router::PoolProvider;

use super::image_normalizer_middleware::{normalize_error_response, normalize_value_to_tokens, refuse_normalize_error};
use super::rejections;
use super::serving_name::{ServingName, has_class_suffix};
use super::store::{self as response_store, ONWARDS_RESPONSE_ID_HEADER, OnwardsDaemonId};
use super::streaming::{ReplayFrame, flex_stream_response};
use crate::auth::internal_credential::presented_key_hash;
use crate::crypto::ApiKeyHash;
use crate::db::{errors::DbError, handlers::api_keys::ApiKeys, models::api_keys::ApiKeyPurpose};
use crate::image_normalizer::ImageNormalizer;
use crate::types::ApiKeyId;

#[derive(Clone)]
pub struct InferenceMiddlewareState<P: PoolProvider + Clone = sqlx_pool_router::DbPools> {
    pub request_manager: Arc<PostgresRequestManager<P>>,
    pub daemon_id: OnwardsDaemonId,
    pub loopback_base_url: String,
    pub dwctl_pool: sqlx_pool_router::DynPools,
    pub response_store: Arc<super::store::FusilladeResponseStore<P>>,
    pub image_normalizer: Arc<dyn ImageNormalizer>,
    pub image_normalizer_enabled: bool,
    pub unverified_requests_per_completion_hour: usize,
    pub flex_completion_window: String,
    pub keystore: Option<crate::keystore::Keystore>,
    pub key_policy_cache: crate::sync::key_policy::KeyPolicyCache,
    pub sse_buffer_limit: usize,
    pub api_key_usage: crate::api_key_usage::ApiKeyUsage,
}

#[tracing::instrument(skip_all)]
pub async fn inference_middleware<P: PoolProvider + Clone + Send + Sync + 'static>(
    State(state): State<InferenceMiddlewareState<P>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if req.headers().contains_key("model-override") {
        return invalid_request(
            &req,
            None,
            "Model-Override is not supported; select the model in the request body",
            "unsupported_header",
            "Model-Override",
        );
    }

    if !should_intercept(req.method(), req.uri().path()) {
        return next.run(req).await;
    }

    if let Some(request_id) = req.headers().get("x-fusillade-request-id") {
        if let Some(request_id) = request_id.to_str().ok().and_then(|s| s.parse::<uuid::Uuid>().ok()) {
            tracing::Span::current().set_attribute("doubleword.request_id", request_id.to_string());
        }
        return next.run(req).await;
    }

    let (mut parts, body) = req.into_parts();
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!(error = %e, "Failed to read request body in inference middleware");
            let response = Response::builder().status(StatusCode::BAD_REQUEST).body(Body::empty()).unwrap();
            return rejections::refuse(&parts, response, "body_read_failed", None, None);
        }
    };

    let mut request_value: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => {
            let category = match e.classify() {
                serde_json::error::Category::Io => "io",
                serde_json::error::Category::Syntax => "syntax",
                serde_json::error::Category::Data => "data",
                serde_json::error::Category::Eof => "eof",
            };
            tracing::error!(category, "Failed to parse request body in inference middleware");
            let response = Response::builder().status(StatusCode::BAD_REQUEST).body(Body::empty()).unwrap();
            return rejections::refuse(&parts, response, "invalid_json", None, None);
        }
    };

    let nested_path = parts.uri.path();
    let is_responses_api = nested_path.ends_with("/responses");
    let is_chat_completions_api = nested_path.ends_with("/chat/completions");
    let requested_tier = resolve_service_tier(request_value["service_tier"].as_str());
    let service_tier = if matches!(requested_tier, ServiceTier::Flex) && !is_responses_api && !is_chat_completions_api {
        tracing::warn!(endpoint = %nested_path, "service_tier:'flex' is not yet supported on this endpoint; falling back to realtime.");
        ServiceTier::Realtime
    } else {
        requested_tier
    };
    let is_daemon_processed = matches!(service_tier, ServiceTier::Flex | ServiceTier::Background);

    let scrubbed_ids = scrub_request_id_fields(&mut request_value);

    let scrubbed_scheduling = strip_scheduling_priority(&mut request_value);

    let api_key = rejections::api_key(nested_path, &parts.headers).map(str::to_string);
    let key_hash = presented_key_hash(&parts.extensions, api_key.as_deref());

    let serving_name = request_value["model"]
        .as_str()
        .map(|model| ServingName::resolve(model, parts.extensions.get::<rejections::Recorder>()));
    if matches!(requested_tier, ServiceTier::Flex | ServiceTier::Background)
        && let Some(name) = &serving_name
        && has_class_suffix(&name.requested)
    {
        let model = name.requested.clone();
        if name.class.is_some() {
            let message = format!(
                "Model '{model}' is available for realtime requests only; send flex and background requests to '{}'.",
                name.model
            );
            return invalid_request(&parts, Some(&model), &message, "serving_class_realtime_only", "model");
        }
        let mut response = invalid_request_response(
            &format!("The model '{model}' does not exist or you do not have access to it."),
            "model_not_found",
            "model",
        );
        *response.status_mut() = StatusCode::NOT_FOUND;
        return rejections::refuse(&parts, response, "model_not_found", Some("model"), Some(&model));
    }
    if let Some(name) = serving_name {
        parts.extensions.insert(name);
    }

    let scrubbed_pinned = match pinned_tolerations(
        &state.key_policy_cache,
        key_hash.as_ref(),
        request_value["model"].as_str().unwrap_or_default(),
    ) {
        Some(tolerations) => set_routing_tolerations(&mut request_value, &tolerations),
        None => false,
    };

    let mut body_bytes = if scrubbed_ids || scrubbed_scheduling || scrubbed_pinned {
        bytes::Bytes::from(request_value.to_string())
    } else {
        body_bytes
    };

    let model = request_value["model"].as_str().unwrap_or("unknown").to_string();
    let model = model.as_str();
    let refuse =
        |response: Response, code: &'static str, param: Option<&str>| rejections::refuse(&parts, response, code, param, Some(model));
    let invalid = |message: &str, code: &'static str, param: &'static str| invalid_request(&parts, Some(model), message, code, param);
    let endpoint = format!("/v1{nested_path}");

    let zdr = crate::inference::zdr::is_zdr_request(&state.key_policy_cache, key_hash.as_ref());

    if let Some(key) = key_hash.as_ref()
        && state
            .key_policy_cache
            .disabled_modalities(key)
            .contains(crate::modalities::Modality::Realtime)
    {
        return refuse(
            modality_disabled_response(crate::modalities::Modality::Realtime),
            "modality_disabled",
            None,
        );
    }
    if is_responses_api {
        if let Some(include) = request_value.get("include")
            && !include.is_null()
        {
            let Ok(_) = serde_json::from_value::<Vec<crate::inference::translation::responses::types::Include>>(include.clone()) else {
                return invalid(
                    "include must contain only supported Responses API projection values",
                    "invalid_parameter",
                    "include",
                );
            };
        }

        let has_encrypted_reasoning_replay = request_value
            .get("input")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|items| {
                items.iter().any(|item| {
                    item.get("type").and_then(serde_json::Value::as_str) == Some("reasoning")
                        && item.get("encrypted_content").is_some_and(|value| !value.is_null())
                })
            });
        if has_encrypted_reasoning_replay {
            return invalid(
                "encrypted reasoning item replay is not supported by this Responses API implementation",
                "unsupported_parameter",
                "input",
            );
        }

        if let Err(error) = onwards::reasoning::validate_canonical_reasoning(nested_path, &request_value) {
            let param = error.param().unwrap_or_default();
            return refuse(
                invalid_request_response(error.message(), error.code(), param),
                error.code(),
                error.param(),
            );
        }
    }
    if is_responses_api && zdr && request_value.get("previous_response_id").is_some_and(|id| !id.is_null()) {
        return invalid(
            "previous_response_id is not supported when zero-data retention is enabled",
            "zdr_state_not_supported",
            "previous_response_id",
        );
    }

    if is_responses_api && request_value.get("previous_response_id").is_some() {
        use crate::inference::translation::responses::hydrate::{HydrationError, hydrate_previous_response};
        let owner: Option<String> = match key_hash.as_ref() {
            Some(key_hash) => {
                sqlx::query_scalar("SELECT user_id::text FROM public.api_keys WHERE secret_hash = $1 AND is_deleted = false LIMIT 1")
                    .bind(&key_hash[..])
                    .fetch_optional(&state.dwctl_pool)
                    .await
                    .unwrap_or(None)
            }
            None => None,
        };
        let hydration = match owner.as_deref() {
            Some(owner) => hydrate_previous_response(&*state.response_store, owner, &mut request_value).await,
            None => Err(HydrationError::NotFound(
                request_value["previous_response_id"].as_str().unwrap_or_default().to_string(),
            )),
        };
        if let Err(e) = hydration {
            let (status, message) = match e {
                HydrationError::NotFound(id) => (StatusCode::BAD_REQUEST, format!("previous response not found: {id}")),
                HydrationError::Internal(msg) => {
                    tracing::error!(error = %msg, "responses hydration failed");
                    (StatusCode::INTERNAL_SERVER_ERROR, "failed to load previous response".to_string())
                }
            };
            let err_type = if status == StatusCode::BAD_REQUEST {
                "invalid_request_error"
            } else {
                "server_error"
            };
            let response = Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"error": {"message": message, "type": err_type}}).to_string(),
                ))
                .unwrap();
            if status.is_client_error() {
                return refuse(response, "previous_response_not_found", Some("previous_response_id"));
            }
            return response;
        }
        body_bytes = bytes::Bytes::from(request_value.to_string());
    }

    let background = if is_responses_api {
        request_value["background"].as_bool().unwrap_or(false)
    } else {
        false
    };
    if let Some(message) = background_contract_error(service_tier, is_responses_api, background) {
        let response = Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"error": {"message": message, "type": "invalid_request_error"}}).to_string(),
            ))
            .unwrap();
        let param = if is_responses_api { "background" } else { "service_tier" };
        return refuse(response, "invalid_parameter", Some(param));
    }
    tracing::debug!(
        model = %model,
        service_tier = %service_tier,
        background = background,
        endpoint = %endpoint,
        "Routing inference request"
    );

    let request_id = uuid::Uuid::new_v4();
    tracing::Span::current().set_attribute("doubleword.request_id", request_id.to_string());
    let resp_id = format!("resp_{request_id}");

    if is_daemon_processed {
        match key_hash.as_ref() {
            None => {
                let response = Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"error": {"message": "API key required", "type": "invalid_request_error"}}).to_string(),
                    ))
                    .unwrap();
                return refuse(response, "unauthenticated", None);
            }
            Some(key) => {
                match crate::error_enrichment::validate_api_key_model_access(state.dwctl_pool.write().into_inner(), key, model).await {
                    Ok(()) => {}
                    Err(crate::error_enrichment::ModelAccessRefusal::Retired(message)) => {
                        return refuse(crate::error_enrichment::retired_model_response(&message), "model_retired", None);
                    }
                    Err(crate::error_enrichment::ModelAccessRefusal::Denied(msg)) => {
                        let response = Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .header("content-type", "application/json")
                            .body(Body::from(
                                serde_json::json!({"error": {"message": msg, "type": "invalid_request_error"}}).to_string(),
                            ))
                            .unwrap();
                        return refuse(response, "forbidden", None);
                    }
                }
            }
        }
    }

    if matches!(service_tier, ServiceTier::Background) {
        match api_key_creator_can_run_background(&state.dwctl_pool.write(), key_hash.as_ref()).await {
            Ok(Some(true)) => {}
            Ok(Some(false)) => {
                let response = Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "error": {
                                "message": "Background inference requires the BackgroundInferenceUser role for the API key creator.",
                                "type": "invalid_request_error"
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap();
                return refuse(response, "forbidden", None);
            }
            Ok(None) => {
                let response = Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"error": {"message": "Invalid API key", "type": "invalid_request_error"}}).to_string(),
                    ))
                    .unwrap();
                return refuse(response, "invalid_api_key", None);
            }
            Err(e) => {
                tracing::error!(error = %e, "Failed to resolve background-inference role");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"error": {"message": "Failed to authorize background inference", "type": "server_error", "code": 500}}).to_string(),
                    ))
                    .unwrap();
            }
        }
    }

    let created_by = if background && matches!(service_tier, ServiceTier::Realtime) {
        response_store::lookup_created_by(&state.dwctl_pool.write(), key_hash.as_ref()).await
    } else {
        None
    };
    let queued_batch_key = if is_daemon_processed {
        match resolve_flex_batch_api_key(&state.dwctl_pool.write(), key_hash.as_ref()).await {
            Ok(Some(key)) => {
                state.api_key_usage.record(key.client_key_id);
                Some(key)
            }
            Ok(None) => {
                tracing::warn!(service_tier = %service_tier, "Queued API key disappeared before hidden batch-key resolution");
                let response = Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"error": {"message": "Invalid API key", "type": "invalid_request_error"}}).to_string(),
                    ))
                    .unwrap();
                return refuse(response, "invalid_api_key", None);
            }
            Err(e) => {
                tracing::error!(error = %e, service_tier = %service_tier, "Failed to resolve queued hidden batch key");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "error": {
                                "message": "Failed to prepare queued request",
                                "type": "server_error",
                                "code": 500,
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap();
            }
        }
    } else {
        None
    };

    if matches!(service_tier, ServiceTier::Flex)
        && let Some(key) = queued_batch_key.as_ref()
        && let Err(err) = crate::api::handlers::unverified_volume::enforce_unverified_volume_limit(
            &*state.request_manager,
            state.unverified_requests_per_completion_hour,
            key.key_owner_id,
            key.verified,
            &state.flex_completion_window,
            1,
            crate::api::handlers::unverified_volume::SubmissionKind::Flex,
        )
        .await
    {
        let response = Response::builder()
            .status(err.status_code())
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"error": {"message": err.user_message(), "type": "invalid_request_error"}}).to_string(),
            ))
            .unwrap();
        if response.status().is_client_error() {
            return refuse(response, "unverified_volume_limit", None);
        }
        return response;
    }

    match service_tier {
        ServiceTier::Realtime => {
            let realtime_input = fusillade::CreateRealtimeInput {
                request_id,
                body: if zdr { String::new() } else { request_value.to_string() },
                model: model.to_string(),
                endpoint: state.loopback_base_url.clone(),
                method: "POST".to_string(),
                path: endpoint.clone(),
                created_by: created_by.unwrap_or_default(),
            };
            let client_usage = super::stream_usage::requested_usage(&endpoint, &request_value);
            drop(request_value);
            let response = handle_realtime(&state, realtime_input, &resp_id, model, background, zdr, parts, body_bytes, next).await;
            match client_usage {
                Some(usage) => super::stream_usage::apply(response, usage, state.sse_buffer_limit),
                None => response,
            }
        }
        ServiceTier::Flex | ServiceTier::Background => {
            let is_background_tier = matches!(service_tier, ServiceTier::Background);
            if zdr && state.keystore.is_none() {
                tracing::error!("ZDR flex request but no keystore configured; refusing to store plaintext");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"error": {"message": "Zero-data-retention is enabled for this account but the server is not configured to store data securely; refusing to process the request.", "type": "server_error"}}).to_string(),
                    ))
                    .unwrap();
            }
            let zdr_keys = zdr.then(crate::inference::zdr::FlexKeys::generate);
            if state.image_normalizer_enabled
                && crate::image_normalizer::walker::has_inputs(&request_value, crate::image_normalizer::Mode::All)
            {
                let attribution = match key_hash.as_ref() {
                    Some(key) => match crate::api::handlers::images::try_resolve_caller(&state.dwctl_pool.write(), key).await {
                        Ok(caller) => caller.map(|c| c.attribution),
                        Err(e) => {
                            tracing::warn!(error = %e, "Image attribution lookup failed on flex enqueue");
                            return normalize_error_response(crate::image_normalizer::NormalizeError::AccessUnavailable);
                        }
                    },
                    None => None,
                };
                let access_pool = Some(state.dwctl_pool.write().into_inner());
                let seal_key = zdr_keys.as_ref().map(|keys| &keys.request[..]);
                match normalize_value_to_tokens(&mut request_value, &state.image_normalizer, access_pool, attribution, seal_key).await {
                    Ok(n) => {
                        if n > 0 {
                            tracing::debug!(substituted = n, service_tier = %service_tier, "Queued image normalisation replaced image inputs with tokens");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, service_tier = %service_tier, "Queued image normalisation failed");
                        return refuse_normalize_error(&parts, e, Some(model));
                    }
                }
            }
            let flex_stream = if is_chat_completions_api {
                request_value["stream"].as_bool().unwrap_or(false)
            } else if is_responses_api {
                !background && request_value["stream"].as_bool().unwrap_or(false)
            } else {
                false
            };
            let flex_stream_include_usage =
                flex_stream && is_chat_completions_api && request_value["stream_options"]["include_usage"].as_bool().unwrap_or(false);
            if flex_stream && let Some(obj) = request_value.as_object_mut() {
                obj.shift_remove("stream");
                obj.shift_remove("stream_options");
            }

            let flex_body = if let Some(keys) = zdr_keys.as_ref() {
                let zdr_store_error = || {
                    Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({"error": {"message": "Failed to securely store zero-data-retention request.", "type": "server_error"}}).to_string(),
                        ))
                        .unwrap()
                };
                let Some(keystore) = state.keystore.as_ref() else {
                    tracing::error!("ZDR enabled but keystore missing; refusing to store plaintext");
                    return zdr_store_error();
                };
                match crate::inference::zdr::prepare_flex_submit(keystore, &request_id, keys, &mut request_value).await {
                    Ok(body) => body,
                    Err(e) => {
                        tracing::error!(error = %e, "ZDR submit failed; refusing to store plaintext");
                        return zdr_store_error();
                    }
                }
            } else {
                request_value.to_string()
            };

            let Some(queued_batch_key) = queued_batch_key else {
                tracing::error!(service_tier = %service_tier, "Queued request has no execution key");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "error": {
                                "message": "Failed to prepare queued request",
                                "type": "server_error",
                                "code": 500,
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap();
            };
            let queued_created_by = queued_batch_key.key_owner_id.to_string();
            let queued_metadata = parts
                .headers
                .get(axum::http::header::USER_AGENT)
                .and_then(|value| value.to_str().ok())
                .map(|ua| ua.chars().take(256).collect::<String>())
                .filter(|ua| !ua.is_empty())
                .map(|ua| serde_json::json!({ "dw_user_agent": ua }));
            if is_background_tier {
                let background_input = fusillade::CreateBackgroundInput {
                    request_id,
                    body: flex_body,
                    model: model.to_string(),
                    endpoint: state.loopback_base_url.clone(),
                    method: "POST".to_string(),
                    path: endpoint.clone(),
                    api_key_id: queued_batch_key.id,
                    created_by: queued_created_by,
                    metadata: queued_metadata,
                };
                return handle_background(&state, background_input, &resp_id, model).await;
            }

            let flex_input = fusillade::CreateFlexInput {
                request_id,
                body: flex_body,
                model: model.to_string(),
                endpoint: state.loopback_base_url.clone(),
                method: "POST".to_string(),
                path: endpoint.clone(),
                api_key_id: queued_batch_key.id,
                created_by: queued_created_by,
                metadata: queued_metadata,
            };

            match (is_chat_completions_api, flex_stream) {
                (true, true) => handle_chat_completion_flex_streaming(&state, flex_input, request_id, flex_stream_include_usage).await,
                (true, false) => handle_chat_completion_flex(&state, flex_input, request_id).await,
                (false, true) => handle_responses_flex_streaming(&state, flex_input, request_id).await,
                (false, false) => handle_flex(&state, flex_input, &resp_id, model, background).await,
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ServiceTier {
    Realtime,
    Flex,
    Background,
}

#[derive(Debug, Clone)]
struct FlexBatchApiKey {
    id: ApiKeyId,
    client_key_id: ApiKeyId,
    key_owner_id: uuid::Uuid,
    verified: bool,
}

async fn resolve_flex_batch_api_key(pool: &sqlx::PgPool, api_key: Option<&ApiKeyHash>) -> Result<Option<FlexBatchApiKey>, DbError> {
    let Some(api_key) = api_key else {
        return Ok(None);
    };

    let row = sqlx::query(
        r#"
        SELECT ak.id, ak.user_id, ak.created_by, u.verified, child.id AS child_id
        FROM api_keys ak
        LEFT JOIN users u ON u.id = ak.user_id
        LEFT JOIN api_keys child
               ON child.parent_api_key_id = ak.id
              AND child.purpose = 'batch'
              AND child.hidden = true
              AND child.is_deleted = false
        WHERE ak.secret_hash = $1 AND ak.is_deleted = false
        LIMIT 1
        "#,
    )
    .bind(&api_key[..])
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };
    let client_key_id: ApiKeyId = sqlx::Row::get(&row, "id");
    let key_owner_id: uuid::Uuid = sqlx::Row::get(&row, "user_id");
    let created_by: uuid::Uuid = sqlx::Row::get(&row, "created_by");
    let verified: bool = sqlx::Row::try_get(&row, "verified").ok().flatten().unwrap_or(false);
    let child_id: Option<uuid::Uuid> = sqlx::Row::try_get(&row, "child_id")?;

    let mut conn = pool.acquire().await?;
    let mut api_keys_repo = ApiKeys::new(&mut conn);
    let key_id = match child_id {
        Some(id) => id,
        None => {
            api_keys_repo
                .get_or_create_hidden_key(key_owner_id, ApiKeyPurpose::Batch, created_by)
                .await?
        }
    };
    Ok(Some(FlexBatchApiKey {
        id: key_id,
        client_key_id,
        key_owner_id,
        verified,
    }))
}

async fn api_key_creator_can_run_background(pool: &sqlx::PgPool, api_key: Option<&ApiKeyHash>) -> Result<Option<bool>, DbError> {
    let Some(api_key) = api_key else {
        return Ok(None);
    };

    sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM user_roles ur
            WHERE ur.user_id = ak.created_by
              AND ur.role = 'BACKGROUNDINFERENCEUSER'
        )
        FROM api_keys ak
        WHERE ak.secret_hash = $1 AND ak.is_deleted = false
        LIMIT 1
        "#,
    )
    .bind(&api_key[..])
    .fetch_optional(pool)
    .await
    .map_err(Into::into)
}

impl std::fmt::Display for ServiceTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceTier::Realtime => write!(f, "realtime"),
            ServiceTier::Flex => write!(f, "flex"),
            ServiceTier::Background => write!(f, "background"),
        }
    }
}

fn resolve_service_tier(tier: Option<&str>) -> ServiceTier {
    match tier {
        Some("flex") => ServiceTier::Flex,
        Some("background") => ServiceTier::Background,
        _ => ServiceTier::Realtime,
    }
}

fn background_contract_error(service_tier: ServiceTier, is_responses_api: bool, background: bool) -> Option<&'static str> {
    if !matches!(service_tier, ServiceTier::Background) {
        return None;
    }
    if !is_responses_api {
        return Some(
            "service_tier: \"background\" is only supported by /v1/responses. Submit the request to /v1/responses with background: true, or choose a foreground service_tier.",
        );
    }
    if !background {
        return Some(
            "service_tier: \"background\" is asynchronous and requires background: true. Set background: true and poll GET /v1/responses/{id}, or choose a foreground service_tier.",
        );
    }
    None
}

#[allow(clippy::too_many_arguments)]
async fn handle_realtime<P: PoolProvider + Clone + Send + Sync + 'static>(
    state: &InferenceMiddlewareState<P>,
    realtime_input: fusillade::CreateRealtimeInput,
    resp_id: &str,
    model: &str,
    background: bool,
    zdr: bool,
    parts: axum::http::request::Parts,
    body_bytes: bytes::Bytes,
    next: Next,
) -> Response {
    let rm = state.request_manager.clone();

    let endpoint_for_header = realtime_input.path.clone();

    if background {
        if let Err(e) = fusillade::Storage::create_realtime(&*rm, realtime_input).await {
            tracing::warn!(error = %e, "Failed to create realtime tracking row");
        }
    }

    let raw_id = resp_id.strip_prefix("resp_").unwrap_or(resp_id);
    let mut req = Request::from_parts(parts, Body::from(body_bytes));
    req.headers_mut()
        .insert("x-fusillade-request-id", raw_id.parse().expect("response_id is valid header value"));
    req.headers_mut().insert(
        ONWARDS_RESPONSE_ID_HEADER,
        resp_id.parse().expect("response_id is valid header value"),
    );
    if let Ok(value) = endpoint_for_header.parse() {
        req.headers_mut().insert("x-onwards-endpoint", value);
    }
    if let Ok(value) = model.parse() {
        req.headers_mut().insert("x-onwards-model", value);
    }
    if zdr {
        req.headers_mut().insert(
            crate::inference::zdr::ZDR_MARKER_HEADER,
            "1".parse().expect("static ZDR marker is a valid header value"),
        );
    }

    if background {
        let response_body = serde_json::json!({
            "id": resp_id,
            "object": "response",
            "status": "in_progress",
            "model": model,
            "background": true,
            "output": [],
        });

        let api_key_usage = state.api_key_usage.clone();
        tokio::spawn(
            async move {
                let response = next.run(req).await;
                if let Some(onwards::AuthenticatedApiKeyId(api_key_id)) = response.extensions().get() {
                    api_key_usage.record(*api_key_id);
                }
                let (_parts, body) = response.into_parts();
                let _ = axum::body::to_bytes(body, usize::MAX).await;
            }
            .in_current_span(),
        );

        (StatusCode::ACCEPTED, Json(response_body)).into_response()
    } else {
        next.run(req).await
    }
}

async fn handle_flex<P: PoolProvider + Clone + Send + Sync + 'static>(
    state: &InferenceMiddlewareState<P>,
    flex_input: fusillade::CreateFlexInput,
    resp_id: &str,
    model: &str,
    background: bool,
) -> Response {
    if let Err(e) = state.request_manager.create_flex(flex_input).await {
        tracing::error!(error = %e, "Failed to create flex row in fusillade");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "error": {
                        "message": "Failed to enqueue request",
                        "type": "server_error",
                        "code": 500,
                    }
                })
                .to_string(),
            ))
            .unwrap();
    }

    if background {
        let response_body = serde_json::json!({
            "id": resp_id,
            "object": "response",
            "status": "queued",
            "model": model,
            "background": true,
            "service_tier": "flex",
            "output": [],
        });
        tracing::debug!(response_id = %resp_id, "Enqueued flex request");
        (StatusCode::ACCEPTED, Json(response_body)).into_response()
    } else {
        tracing::debug!(response_id = %resp_id, "Blocking flex — polling until daemon completes");

        let poll_interval = std::time::Duration::from_millis(500);
        let timeout = std::time::Duration::from_secs(3600);

        match response_store::poll_until_complete(&state.request_manager, resp_id, poll_interval, timeout, state.keystore.as_ref()).await {
            Ok(response_obj) => {
                let status_code = if matches!(response_obj["status"].as_str(), Some("completed" | "incomplete")) {
                    StatusCode::OK
                } else {
                    response_obj["error"]["code"]
                        .as_u64()
                        .and_then(|c| StatusCode::from_u16(c as u16).ok())
                        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
                };
                (status_code, Json(response_obj)).into_response()
            }
            Err(e) => {
                tracing::error!(error = %e, response_id = %resp_id, "Blocking flex poll failed");
                let response_body = serde_json::json!({
                    "error": {
                        "message": format!("Request timed out: {e}"),
                        "type": "server_error",
                    }
                });
                (StatusCode::GATEWAY_TIMEOUT, Json(response_body)).into_response()
            }
        }
    }
}

fn background_submission_body(resp_id: &str, model: &str) -> serde_json::Value {
    serde_json::json!({
        "id": resp_id,
        "object": "response",
        "status": "queued",
        "model": model,
        "background": true,
        "service_tier": "background",
        "output": [],
    })
}

async fn handle_background<P: PoolProvider + Clone + Send + Sync + 'static>(
    state: &InferenceMiddlewareState<P>,
    input: fusillade::CreateBackgroundInput,
    resp_id: &str,
    model: &str,
) -> Response {
    if let Err(e) = state.request_manager.create_background(input).await {
        tracing::error!(error = %e, "Failed to create background row in fusillade");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "error": {
                        "message": "Failed to enqueue background request",
                        "type": "server_error",
                        "code": 500,
                    }
                })
                .to_string(),
            ))
            .unwrap();
    }

    let response_body = background_submission_body(resp_id, model);
    tracing::debug!(response_id = %resp_id, "Enqueued background request");
    (StatusCode::ACCEPTED, Json(response_body)).into_response()
}

async fn handle_chat_completion_flex<P: PoolProvider + Clone + Send + Sync + 'static>(
    state: &InferenceMiddlewareState<P>,
    flex_input: fusillade::CreateFlexInput,
    request_id: uuid::Uuid,
) -> Response {
    if let Err(e) = state.request_manager.create_flex(flex_input).await {
        tracing::error!(error = %e, "Failed to create flex chat-completions batch in fusillade");
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "error": {
                        "message": "Failed to enqueue request",
                        "type": "server_error",
                        "code": 500,
                    }
                })
                .to_string(),
            ))
            .unwrap();
    }

    let poll_interval = std::time::Duration::from_millis(500);
    let timeout = std::time::Duration::from_secs(3600);

    match response_store::poll_until_terminal(&state.request_manager, request_id, poll_interval, timeout, state.keystore.as_ref()).await {
        Ok(detail) => {
            let (status, body) = response_store::detail_to_chat_completion_object(&detail);
            let status_code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            tracing::debug!(request_id = %request_id, %status_code, "Flex chat-completions terminal");
            (status_code, Json(body)).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, request_id = %request_id, "Blocking flex chat-completions poll failed");
            (
                StatusCode::GATEWAY_TIMEOUT,
                Json(serde_json::json!({
                    "error": {
                        "message": format!("Request timed out: {e}"),
                        "type": "server_error",
                        "code": 504,
                    }
                })),
            )
                .into_response()
        }
    }
}

async fn handle_chat_completion_flex_streaming<P: PoolProvider + Clone + Send + Sync + 'static>(
    state: &InferenceMiddlewareState<P>,
    flex_input: fusillade::CreateFlexInput,
    request_id: uuid::Uuid,
    include_usage: bool,
) -> Response {
    flex_stream_response(
        state.request_manager.clone(),
        flex_input,
        request_id,
        true,
        state.keystore.clone(),
        move |result| match result {
            Ok(detail) => {
                let (status, body) = response_store::detail_to_chat_completion_object(detail);
                if status < 400 {
                    response_store::chat_completion_to_stream_chunks(&body, include_usage)
                        .into_iter()
                        .map(ReplayFrame::unnamed)
                        .collect()
                } else {
                    vec![ReplayFrame::unnamed(body)]
                }
            }
            Err(msg) => vec![ReplayFrame::unnamed(serde_json::json!({
                "error": { "message": format!("Request failed: {msg}"), "type": "server_error", "code": 504 }
            }))],
        },
    )
    .await
}

async fn handle_responses_flex_streaming<P: PoolProvider + Clone + Send + Sync + 'static>(
    state: &InferenceMiddlewareState<P>,
    flex_input: fusillade::CreateFlexInput,
    request_id: uuid::Uuid,
) -> Response {
    flex_stream_response(
        state.request_manager.clone(),
        flex_input,
        request_id,
        false,
        state.keystore.clone(),
        move |result| match result {
            Ok(detail) => {
                let response = response_store::detail_to_response_object(detail);
                if matches!(response["status"].as_str(), Some("completed" | "incomplete")) {
                    response_store::response_object_to_stream_events(&response)
                        .into_iter()
                        .map(|(event, data)| ReplayFrame::named(event, data))
                        .collect()
                } else {
                    let stub = serde_json::json!({
                        "id": response.get("id").cloned().unwrap_or(serde_json::Value::Null),
                        "object": "response",
                        "status": "in_progress",
                    });
                    vec![
                        ReplayFrame::named("response.created", stub),
                        ReplayFrame::named("response.failed", response),
                    ]
                }
            }
            Err(msg) => {
                let stub = serde_json::json!({
                    "id": serde_json::Value::Null,
                    "object": "response",
                    "status": "in_progress",
                });
                let err = serde_json::json!({
                    "error": { "message": format!("Request failed: {msg}"), "type": "server_error", "code": 504 }
                });
                vec![
                    ReplayFrame::named("response.created", stub),
                    ReplayFrame::named("response.failed", err),
                ]
            }
        },
    )
    .await
}

pub(crate) fn should_intercept(method: &axum::http::Method, path: &str) -> bool {
    method == axum::http::Method::POST
        && (path.ends_with("/responses") || path.ends_with("/completions") || path.ends_with("/messages") || path.ends_with("/embeddings"))
}

fn modality_disabled_response(modality: crate::modalities::Modality) -> Response {
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "error": {
                    "message": modality.disabled_message(),
                    "type": "invalid_request_error",
                    "code": "modality_disabled",
                    "param": null
                }
            })
            .to_string(),
        ))
        .unwrap()
}

fn invalid_request<'a>(
    origin: impl Into<rejections::Origin<'a>>,
    model: Option<&str>,
    message: &str,
    code: &'static str,
    param: &'static str,
) -> Response {
    rejections::refuse(origin, invalid_request_response(message, code, param), code, Some(param), model)
}

fn invalid_request_response(message: &str, code: &str, param: &str) -> Response {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({
                "error": {
                    "message": message,
                    "type": "invalid_request_error",
                    "code": code,
                    "param": param,
                }
            })
            .to_string(),
        ))
        .unwrap()
}

const SCRUB_ID_KEYS: [&str; 5] = ["id", "completion_id", "completionId", "response_id", "responseId"];

fn scrub_request_id_fields(value: &mut serde_json::Value) -> bool {
    let mut scrubbed = false;
    if let Some(obj) = value.as_object_mut() {
        for key in SCRUB_ID_KEYS {
            scrubbed |= obj.shift_remove(key).is_some();
        }
    }
    scrubbed
}

pub(crate) fn strip_scheduling_priority(value: &mut serde_json::Value) -> bool {
    let mut scrubbed = false;
    if let Some(obj) = value.as_object_mut() {
        scrubbed |= obj.shift_remove("priority").is_some();
        if let Some(nvext) = obj.get_mut("nvext").and_then(|n| n.as_object_mut()) {
            scrubbed |= onwards::serving::scrub_router_targets(nvext);
            if let Some(hints) = nvext.get_mut("agent_hints").and_then(|h| h.as_object_mut()) {
                scrubbed |= hints.shift_remove("priority").is_some();
            }
            if let Some(routing) = nvext.get_mut("routing_constraints").and_then(|r| r.as_object_mut()) {
                scrubbed |= routing.shift_remove("tolerations").is_some();
            }
            scrubbed |= onwards::serving::scrub_billing(nvext);
        }
    }
    scrubbed
}

pub(crate) fn pinned_tolerations(
    cache: &crate::sync::key_policy::KeyPolicyCache,
    key: Option<&ApiKeyHash>,
    model: &str,
) -> Option<std::sync::Arc<serde_json::Value>> {
    key.and_then(|key| cache.tolerations(key, model))
}

pub(crate) fn set_routing_tolerations(value: &mut serde_json::Value, tolerations: &serde_json::Value) -> bool {
    let Some(obj) = value.as_object_mut() else {
        return false;
    };
    let nvext = obj.entry("nvext").or_insert_with(|| serde_json::json!({}));
    if !nvext.is_object() {
        *nvext = serde_json::json!({});
    }
    let Some(nvext) = nvext.as_object_mut() else {
        return false;
    };
    let routing = nvext.entry("routing_constraints").or_insert_with(|| serde_json::json!({}));
    if !routing.is_object() {
        *routing = serde_json::json!({});
    }
    let Some(routing) = routing.as_object_mut() else {
        return false;
    };
    if routing.get("tolerations") == Some(tolerations) {
        return false;
    }
    routing.insert("tolerations".to_string(), tolerations.clone());
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::hash_api_key;

    #[test]
    fn set_routing_tolerations_writes_the_preset_list_and_preserves_the_rest() {
        let mut body = serde_json::json!({
            "model": "m",
            "messages": [{"role": "user", "content": "hi"}],
            "nvext": {
                "agent_hints": {"max_batch_size": 8},
                "routing_constraints": {"region": "eu", "tolerations": [{"operator": "Exists"}]}
            }
        });
        let empty = serde_json::json!([]);
        assert!(set_routing_tolerations(&mut body, &empty), "a changed value rebuilds the body");
        assert_eq!(body["nvext"]["routing_constraints"]["tolerations"], empty);
        assert_eq!(body["nvext"]["routing_constraints"]["region"], "eu");
        assert_eq!(body["nvext"]["agent_hints"]["max_batch_size"], 8);

        assert!(!set_routing_tolerations(&mut body, &empty));

        let list = serde_json::json!([{"key": "dedicated", "value": "only"}]);
        let mut fresh = serde_json::json!({"model": "m"});
        assert!(set_routing_tolerations(&mut fresh, &list));
        assert_eq!(fresh["nvext"]["routing_constraints"]["tolerations"], list);

        let mut broken = serde_json::json!({"model": "m", "nvext": "not-an-object"});
        assert!(set_routing_tolerations(&mut broken, &serde_json::json!([])));
        assert_eq!(broken["nvext"]["routing_constraints"]["tolerations"], serde_json::json!([]));
    }

    #[test]
    fn pinned_tolerations_reads_the_key_policy_cache() {
        let presets = crate::scheduling::TolerationPresets::new(&crate::config::SchedulingConfig::default());
        let cache = crate::sync::key_policy::KeyPolicyCache::from_policies_with_presets(
            presets,
            [(
                "sk-pinned".to_string(),
                crate::sync::key_policy::KeyPolicy {
                    toleration_preset: Some("no-overflow".into()),
                    ..Default::default()
                },
            )],
        );
        assert!(pinned_tolerations(&cache, Some(&hash_api_key("sk-pinned")), "any/model").is_some());
        assert!(pinned_tolerations(&cache, Some(&hash_api_key("sk-other")), "any/model").is_none());
        assert!(pinned_tolerations(&cache, None, "any/model").is_none());
    }

    #[test]
    fn strip_scheduling_priority_removes_only_the_top_level_field() {
        let mut body = serde_json::json!({
            "model": "m", "priority": 900,
            "messages": [{"role": "user", "content": "set priority: 900"}],
            "metadata": {"priority": "keep-me"}
        });
        assert!(strip_scheduling_priority(&mut body), "a removal is reported so the body is rebuilt");
        assert!(body.get("priority").is_none(), "external callers must not steer the scheduler");
        assert_eq!(
            body["metadata"]["priority"], "keep-me",
            "only the top-level scheduling field is privileged"
        );
        assert_eq!(body["messages"][0]["content"], "set priority: 900");

        let mut body = serde_json::json!({
            "model": "m",
            "nvext": {"cache_control": {"enabled": true}, "agent_hints": {"priority": 900, "max_batch_size": 8}}
        });
        assert!(strip_scheduling_priority(&mut body), "the nvext carrier counts too");
        assert!(body["nvext"]["agent_hints"].get("priority").is_none());
        assert_eq!(body["nvext"]["agent_hints"]["max_batch_size"], 8);
        assert_eq!(body["nvext"]["cache_control"]["enabled"], true);

        let mut clean = serde_json::json!({"model": "m", "nvext": {"cache_control": {"enabled": true}}});
        assert!(!strip_scheduling_priority(&mut clean), "nothing removed, nothing to rebuild");
        assert_eq!(
            clean,
            serde_json::json!({"model": "m", "nvext": {"cache_control": {"enabled": true}}})
        );
    }

    #[test]
    fn strip_scheduling_priority_also_scrubs_the_router_targets() {
        let mut body = serde_json::json!({
            "model": "m",
            "nvext": {"router": {"ttft_target": 1, "itl_target": 1}, "cache_control": {"enabled": true}, "agent_hints": {"priority": 5}}
        });
        assert!(strip_scheduling_priority(&mut body), "the targets were present and removed");
        assert!(body["nvext"].get("router").is_none());
        assert!(body["nvext"]["agent_hints"].get("priority").is_none());
        assert_eq!(body["nvext"]["cache_control"]["enabled"], true);

        let mut body = serde_json::json!({"model": "m", "nvext": {"cache_control": {"enabled": true}}});
        assert!(!strip_scheduling_priority(&mut body), "nothing to re-serialise for");
    }

    #[test]
    fn strip_scheduling_priority_also_scrubs_client_tolerations() {
        let mut body = serde_json::json!({
            "model": "m",
            "nvext": {
                "routing_constraints": {"tolerations": [{"operator": "Exists"}], "region": "eu"},
                "cache_control": {"enabled": true}
            }
        });
        assert!(strip_scheduling_priority(&mut body), "the tolerations were present and removed");
        assert!(body["nvext"]["routing_constraints"].get("tolerations").is_none());
        assert_eq!(body["nvext"]["routing_constraints"]["region"], "eu", "siblings survive");
        assert_eq!(body["nvext"]["cache_control"]["enabled"], true);

        let mut clean = serde_json::json!({"model": "m", "nvext": {"routing_constraints": {"region": "eu"}}});
        assert!(!strip_scheduling_priority(&mut clean), "nothing removed, nothing to rebuild");
    }

    #[test]
    fn strip_scheduling_priority_also_scrubs_client_prices() {
        let mut body = serde_json::json!({
            "model": "m",
            "nvext": {"billing": {"prompt": 0, "cached": 0, "output": 0}, "cache_control": {"enabled": true}}
        });
        assert!(strip_scheduling_priority(&mut body), "the prices were present and removed");
        assert_eq!(body["nvext"], serde_json::json!({"cache_control": {"enabled": true}}));
    }

    #[test]
    fn test_should_intercept_responses() {
        assert!(should_intercept(&axum::http::Method::POST, "/v1/responses"));
        assert!(should_intercept(&axum::http::Method::POST, "/responses"));
    }

    #[test]
    fn test_should_intercept_chat_completions() {
        assert!(should_intercept(&axum::http::Method::POST, "/v1/chat/completions"));
    }

    #[test]
    fn test_should_intercept_embeddings() {
        assert!(should_intercept(&axum::http::Method::POST, "/v1/embeddings"));
    }

    #[test]
    fn test_should_intercept_completions() {
        assert!(should_intercept(&axum::http::Method::POST, "/v1/completions"));
        assert!(should_intercept(&axum::http::Method::POST, "/completions"));
    }

    #[test]
    fn test_should_not_intercept_get() {
        assert!(!should_intercept(&axum::http::Method::GET, "/v1/responses"));
    }

    #[test]
    fn test_should_not_intercept_models() {
        assert!(!should_intercept(&axum::http::Method::GET, "/v1/models"));
        assert!(!should_intercept(&axum::http::Method::POST, "/v1/models"));
    }

    #[test]
    fn test_should_not_intercept_batches() {
        assert!(!should_intercept(&axum::http::Method::POST, "/v1/batches"));
    }

    #[test]
    fn test_should_not_intercept_files() {
        assert!(!should_intercept(&axum::http::Method::POST, "/v1/files"));
    }

    #[test]
    fn scrub_strips_id_keys_and_preserves_extensions_and_previous_response_id() {
        let mut v = serde_json::json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "id": "gone",
            "completion_id": "gone",
            "completionId": "gone",
            "response_id": "gone",
            "responseId": "gone",
            "previous_response_id": "resp_keep",
            "x_custom_extension": "keep",
        });
        assert!(scrub_request_id_fields(&mut v), "a removal is reported so the body is rebuilt");
        assert!(!scrub_request_id_fields(&mut v), "idempotent: nothing left to remove");
        let obj = v.as_object().unwrap();
        for k in super::SCRUB_ID_KEYS {
            assert!(!obj.contains_key(k), "{k} should have been scrubbed");
        }
        assert_eq!(obj["previous_response_id"], "resp_keep");
        assert_eq!(obj["x_custom_extension"], "keep");
        assert_eq!(obj["model"], "gpt-4o");
    }

    #[test]
    fn test_resolve_service_tier_priority_is_realtime() {
        assert!(matches!(resolve_service_tier(Some("priority")), ServiceTier::Realtime));
    }

    #[test]
    fn test_resolve_service_tier_default_is_realtime() {
        assert!(matches!(resolve_service_tier(Some("default")), ServiceTier::Realtime));
    }

    #[test]
    fn test_resolve_service_tier_auto_is_realtime() {
        assert!(matches!(resolve_service_tier(Some("auto")), ServiceTier::Realtime));
    }

    #[test]
    fn test_resolve_service_tier_none_is_realtime() {
        assert!(matches!(resolve_service_tier(None), ServiceTier::Realtime));
    }

    #[test]
    fn test_resolve_service_tier_flex() {
        assert!(matches!(resolve_service_tier(Some("flex")), ServiceTier::Flex));
    }

    #[test]
    fn test_resolve_service_tier_background() {
        assert!(matches!(resolve_service_tier(Some("background")), ServiceTier::Background));
    }

    #[test]
    fn background_submission_is_queued_before_the_daemon_claims_it() {
        let response = background_submission_body("resp_background", "test-model");

        assert_eq!(
            response,
            serde_json::json!({
                "id": "resp_background",
                "object": "response",
                "status": "queued",
                "model": "test-model",
                "background": true,
                "service_tier": "background",
                "output": [],
            })
        );
    }

    #[test]
    fn background_service_tier_requires_async_responses() {
        assert_eq!(background_contract_error(ServiceTier::Background, true, true), None);

        let foreground_error =
            background_contract_error(ServiceTier::Background, true, false).expect("foreground background-tier request must be rejected");
        assert!(foreground_error.contains("background: true"));
        assert!(foreground_error.contains("service_tier"));

        let unsupported_endpoint_error = background_contract_error(ServiceTier::Background, false, false)
            .expect("non-Responses background-tier request must be rejected");
        assert!(unsupported_endpoint_error.contains("/v1/responses"));
        assert!(unsupported_endpoint_error.contains("background: true"));

        assert_eq!(background_contract_error(ServiceTier::Flex, true, false), None);
        assert_eq!(background_contract_error(ServiceTier::Realtime, false, false), None);
    }

    #[dwctl_test_macros::test]
    async fn resolve_flex_batch_key_uses_hidden_batch_key_for_key_owner(pool: sqlx::PgPool) {
        use crate::api::models::{api_keys::ApiKeyCreate, users::Role};
        use crate::db::{
            handlers::{Repository, api_keys::ApiKeys},
            models::api_keys::{ApiKeyCreateDBRequest, ApiKeyPurpose},
        };

        let user = crate::test::utils::create_test_user(&pool, Role::BatchAPIUser).await;
        let org = crate::test::utils::create_test_org(&pool, user.id).await;
        let realtime_key = {
            let mut conn = pool.acquire().await.expect("acquire connection");
            let mut repo = ApiKeys::new(&mut conn);
            repo.create(&ApiKeyCreateDBRequest::new(
                org.id,
                user.id,
                ApiKeyCreate {
                    name: "Org realtime key".to_string(),
                    description: None,
                    purpose: ApiKeyPurpose::Realtime,
                    member_id: None,
                    spend_limit: None,
                    spend_limit_interval: None,
                },
            ))
            .await
            .expect("create realtime key")
            .secret
        };

        let flex_key = resolve_flex_batch_api_key(&pool, Some(&hash_api_key(&realtime_key)))
            .await
            .expect("resolve flex key")
            .expect("known realtime key should resolve");

        assert_eq!(flex_key.key_owner_id, org.id);

        let row = sqlx::query(
            r#"
            SELECT user_id, created_by, purpose, hidden
            FROM api_keys
            WHERE id = $1
            "#,
        )
        .bind(flex_key.id)
        .fetch_one(&pool)
        .await
        .expect("hidden key row");

        let row_user_id: uuid::Uuid = sqlx::Row::get(&row, "user_id");
        let row_created_by: uuid::Uuid = sqlx::Row::get(&row, "created_by");
        let row_purpose: String = sqlx::Row::get(&row, "purpose");
        let row_hidden: bool = sqlx::Row::get(&row, "hidden");

        assert_eq!(row_user_id, org.id);
        assert_eq!(row_created_by, user.id);
        assert_eq!(row_purpose, "batch");
        assert!(row_hidden);
    }

    #[dwctl_test_macros::test]
    async fn background_role_is_resolved_from_organization_key_creator(pool: sqlx::PgPool) {
        use crate::api::models::{api_keys::ApiKeyCreate, users::Role};
        use crate::db::{
            handlers::{Repository, api_keys::ApiKeys},
            models::api_keys::{ApiKeyCreateDBRequest, ApiKeyPurpose},
        };

        let creator = crate::test::utils::create_test_user(&pool, Role::BackgroundInferenceUser).await;
        let org = crate::test::utils::create_test_org(&pool, creator.id).await;
        let org_key = {
            let mut conn = pool.acquire().await.expect("acquire connection");
            ApiKeys::new(&mut conn)
                .create(&ApiKeyCreateDBRequest::new(
                    org.id,
                    creator.id,
                    ApiKeyCreate {
                        name: "Org background key".to_string(),
                        description: None,
                        purpose: ApiKeyPurpose::Realtime,
                        member_id: None,
                        spend_limit: None,
                        spend_limit_interval: None,
                    },
                ))
                .await
                .expect("create organization key")
                .secret
        };

        assert_eq!(
            api_key_creator_can_run_background(&pool, Some(&hash_api_key(&org_key)))
                .await
                .expect("resolve creator role"),
            Some(true)
        );

        let org_has_background_role: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM user_roles WHERE user_id = $1 AND role = 'BACKGROUNDINFERENCEUSER')")
                .bind(org.id)
                .fetch_one(&pool)
                .await
                .expect("query organization roles");
        assert!(
            !org_has_background_role,
            "the organization owner row must not provide authorization"
        );
    }

    #[dwctl_test_macros::test]
    async fn background_role_rejects_api_key_creator_without_capability(pool: sqlx::PgPool) {
        use crate::api::models::{api_keys::ApiKeyCreate, users::Role};
        use crate::db::{
            handlers::{Repository, api_keys::ApiKeys},
            models::api_keys::{ApiKeyCreateDBRequest, ApiKeyPurpose},
        };

        let creator = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let key = {
            let mut conn = pool.acquire().await.expect("acquire connection");
            ApiKeys::new(&mut conn)
                .create(&ApiKeyCreateDBRequest::new(
                    creator.id,
                    creator.id,
                    ApiKeyCreate {
                        name: "Foreground-only key".to_string(),
                        description: None,
                        purpose: ApiKeyPurpose::Realtime,
                        member_id: None,
                        spend_limit: None,
                        spend_limit_interval: None,
                    },
                ))
                .await
                .expect("create key")
                .secret
        };

        assert_eq!(
            api_key_creator_can_run_background(&pool, Some(&hash_api_key(&key)))
                .await
                .expect("resolve creator role"),
            Some(false)
        );
        assert_eq!(
            api_key_creator_can_run_background(&pool, Some(&hash_api_key("unknown-key")))
                .await
                .expect("resolve unknown key"),
            None
        );
    }

    #[dwctl_test_macros::test]
    async fn responses_refuse_native_thinking_fields_before_acceptance(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::{create_test_api_key_for_user, create_test_app, create_test_user};

        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;

        let tiers = [
            (Some("flex"), false),
            (Some("flex"), true),
            (Some("background"), true),
            (None, true),
            (Some("auto"), true),
            (Some("priority"), true),
        ];
        for (tier, background) in tiers {
            let mut request = serde_json::json!({
                "model": "gpt-4",
                "input": "hi",
                "background": background,
                "chat_template_kwargs": {"enable_thinking": false}
            });
            if let Some(tier) = tier {
                request["service_tier"] = tier.into();
            }
            let response = server
                .post("/ai/v1/responses")
                .add_header("Authorization", &format!("Bearer {}", key.secret))
                .json(&request)
                .await;

            assert_eq!(response.status_code().as_u16(), 400, "{tier:?}/{background}: {}", response.text());
            let body: serde_json::Value = response.json();
            assert_eq!(body["error"]["type"], "invalid_request_error", "{body}");
            assert_eq!(body["error"]["code"], "unsupported_parameter", "{body}");
            assert_eq!(body["error"]["param"], "chat_template_kwargs.enable_thinking", "{body}");
            assert!(body["error"]["message"].as_str().unwrap().contains("reasoning.effort"), "{body}");
        }
    }
}
