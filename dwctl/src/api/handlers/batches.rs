
use crate::metrics::errors::component::BATCH_POPULATE;
use sqlx_pool_router::PoolProvider;

use crate::AppState;
use crate::api::models::batches::{
    BatchAnalytics, BatchErrors, BatchListResponse, BatchObjectType, BatchResponse, BatchResultsQuery, CreateBatchRequest,
    ListBatchesQuery, ListObjectType, RequestCounts, RetryRequestsRequest,
};
use crate::api::models::users::{CurrentUser, Role};
use crate::auth::permissions::{
    RequiresPermission, can_read_all_resources, can_run_background_inference, has_permission, operation, resolve_key_capabilities, resource,
};
use crate::db::handlers::deployments::BatchModelInfo;
use crate::db::handlers::{BatchTemplates, Connections, Credits, Deployments, Users, api_keys::ApiKeys, repository::Repository};
use crate::db::models::api_keys::ApiKeyPurpose;
use crate::errors::{Error, Result};
use crate::types::{Operation, Permission, Resource};
use axum::{
    Json,
    body::Body,
    extract::{Path, Query, State},
    http::StatusCode,
};
use bytes::Bytes;
use fusillade::Storage;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::pin::Pin;
use uuid::Uuid;


#[derive(Debug, Serialize, Deserialize)]
pub struct CreateBatchInput {
    pub batch_id: Uuid,
    pub file_id: Uuid,
}

pub async fn build_create_batch_job<P: sqlx_pool_router::PoolProvider + Clone + Send + Sync + 'static>(
    pool: sqlx::PgPool,
    state: crate::tasks::TaskState<P>,
) -> anyhow::Result<underway::Job<CreateBatchInput, crate::tasks::TaskState<P>>> {
    use underway::Job;
    use underway::job::To;
    use underway::task::Error as TaskError;

    Job::<CreateBatchInput, _>::builder()
        .state(state)
        .step(|cx, input: CreateBatchInput| async move {
            let batch_id = fusillade::BatchId(input.batch_id);

            if let Err(e) = cx
                .state
                .request_manager
                .populate_batch(batch_id, fusillade::FileId(input.file_id))
                .await
            {
                return match &e {
                    fusillade::FusilladeError::ValidationError(_) => {
                        crate::background_error!(
                            BATCH_POPULATE, "populate_failed", Critical,
                            batch_id = %input.batch_id,
                            error = %e,
                            "Failed to populate batch"
                        );
                        if let Err(mark_err) = cx.state.request_manager.mark_batch_failed(batch_id, &e.to_string()).await {
                            tracing::error!(
                                batch_id = %input.batch_id,
                                error = %mark_err,
                                "Failed to mark batch as failed after validation error"
                            );
                            Err(TaskError::Retryable(mark_err.to_string()))
                        } else {
                            Err(TaskError::Fatal(e.to_string()))
                        }
                    }
                    _ => {
                        crate::background_error!(
                            BATCH_POPULATE, "db_error", Error,
                            batch_id = %input.batch_id,
                            error = %e,
                            "Failed to populate batch, will retry"
                        );
                        Err(TaskError::Retryable(e.to_string()))
                    }
                };
            }

            tracing::info!(batch_id = %input.batch_id, "Batch populated");
            To::done()
        })
        .name("create-batch")
        .pool(pool)
        .build()
        .await
        .map_err(Into::into)
}


#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CascadeTarget {
    Canceled,
    Failed,
}

impl CascadeTarget {
    fn to_fusillade(self) -> fusillade::CascadeTargetState {
        match self {
            CascadeTarget::Canceled => fusillade::CascadeTargetState::Canceled,
            CascadeTarget::Failed => fusillade::CascadeTargetState::Failed,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            CascadeTarget::Canceled => "canceled",
            CascadeTarget::Failed => "failed",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CascadeBatchStateInput {
    pub batch_id: Uuid,
    pub target_state: CascadeTarget,
}

pub async fn build_cascade_batch_state_job<P: sqlx_pool_router::PoolProvider + Clone + Send + Sync + 'static>(
    pool: sqlx::PgPool,
    state: crate::tasks::TaskState<P>,
) -> anyhow::Result<underway::Job<CascadeBatchStateInput, crate::tasks::TaskState<P>>> {
    use underway::Job;
    use underway::job::To;
    use underway::task::Error as TaskError;

    Job::<CascadeBatchStateInput, _>::builder()
        .state(state)
        .step(|cx, input: CascadeBatchStateInput| async move {
            let batch_id = fusillade::BatchId(input.batch_id);

            match cx
                .state
                .request_manager
                .cascade_batch_state_to_requests(batch_id, input.target_state.to_fusillade())
                .await
            {
                Ok(updated) => {
                    if updated > 0 {
                        tracing::info!(
                            batch_id = %input.batch_id,
                            target_state = input.target_state.as_str(),
                            updated,
                            "Cascaded batch state to child requests"
                        );
                    }
                    To::done()
                }
                Err(e) => {
                    tracing::error!(
                        batch_id = %input.batch_id,
                        target_state = input.target_state.as_str(),
                        error = %e,
                        "Failed to cascade batch state to child requests"
                    );
                    Err(TaskError::Retryable(e.to_string()))
                }
            }
        })
        .name("cascade-batch-state")
        .pool(pool)
        .build()
        .await
        .map_err(Into::into)
}


fn parse_comma_separated_filter(raw: Option<&str>) -> Option<Vec<String>> {
    let raw = raw?;
    let values: Vec<String> = raw.split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect();
    if values.is_empty() { None } else { Some(values) }
}

fn parse_completion_window_filter(raw: Option<&str>) -> Option<Vec<String>> {
    parse_comma_separated_filter(raw)
}

#[derive(Debug)]
struct BatchClassFilter {
    completion_windows: Option<Vec<String>>,
    service_tiers: Option<Vec<String>>,
}

fn parse_batch_class_filter(raw: Option<&str>, is_platform_manager: bool) -> Result<BatchClassFilter> {
    let Some(values) = parse_completion_window_filter(raw) else {
        return Ok(BatchClassFilter {
            completion_windows: None,
            service_tiers: None,
        });
    };

    let includes_background = values.iter().any(|value| value == "background");
    if includes_background && !is_platform_manager {
        return Err(Error::BadRequest {
            message: "completion_window=background is only available to platform managers".to_string(),
        });
    }

    let completion_windows: Vec<_> = values.into_iter().filter(|value| value != "background").collect();
    Ok(BatchClassFilter {
        completion_windows: (!completion_windows.is_empty()).then_some(completion_windows),
        service_tiers: includes_background.then(|| vec!["background".to_string()]),
    })
}

fn is_batch_owner(current_user: &CurrentUser, created_by: &str) -> bool {
    let user_id = current_user.id.to_string();
    if created_by == user_id {
        return true;
    }
    if let Some(org_id) = current_user.active_organization
        && created_by == org_id.to_string()
    {
        return true;
    }
    false
}

async fn enqueue_cascade_batch_state<P: PoolProvider>(state: &AppState<P>, batch_id: Uuid, target_state: CascadeTarget) {
    let Some(ref job) = state.task_runner.cascade_batch_state_job else {
        tracing::debug!(
            batch_id = %batch_id,
            "cascade-batch-state job not configured, skipping enqueue"
        );
        return;
    };
    if let Err(e) = job.enqueue(&CascadeBatchStateInput { batch_id, target_state }).await {
        tracing::warn!(
            batch_id = %batch_id,
            target_state = target_state.as_str(),
            error = %e,
            "Failed to enqueue cascade-batch-state job (child requests will remain in old state)"
        );
    }
}

fn to_batch_response_with_email(batch: fusillade::Batch, creator_email: Option<&str>) -> BatchResponse {
    to_batch_response_enriched(batch, creator_email, None)
}

fn to_batch_response_enriched(batch: fusillade::Batch, creator_email: Option<&str>, source_name: Option<&str>) -> BatchResponse {
    use crate::api::models::dwext::BatchDwExtResponse;

    let raw_metadata: HashMap<String, String> = batch
        .metadata
        .and_then(|m| {
            m.as_object().map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
        })
        .unwrap_or_default();

    let request_source = raw_metadata.get("request_source").cloned();
    let dwext = BatchDwExtResponse {
        source: request_source.clone(),
        source_id: raw_metadata.get("dw_source_id").cloned(),
        source_name: source_name
            .map(|s| s.to_string())
            .or_else(|| raw_metadata.get("dw_source_name").cloned()),
        source_file: raw_metadata.get("dw_external_key").cloned(),
        sync_id: raw_metadata.get("dw_sync_id").cloned(),
    };

    let model = raw_metadata.get("dw_model").cloned();

    let internal_keys = [
        "dw_source_id",
        "dw_source_name",
        "dw_sync_id",
        "dw_external_key",
        "created_by",
        "dw_user_agent",
        "dw_model",
    ];
    let mut metadata: HashMap<String, String> = raw_metadata
        .into_iter()
        .filter(|(k, _)| !internal_keys.contains(&k.as_str()))
        .collect();

    if let Some(email) = creator_email {
        metadata.insert("created_by_email".to_string(), email.to_string());
    }

    let has_started = batch.requests_started_at.is_some();
    let is_finished = has_started && batch.pending_requests == 0 && batch.in_progress_requests == 0;
    let openai_status = if batch.cancelling_at.is_some() {
        if is_finished {
            "cancelled"
        } else {
            "cancelling"
        }
    } else if batch.failed_at.is_some() && !has_started {
        "failed"
    } else if !has_started {
        "validating"
    } else if is_finished && batch.failed_requests == batch.total_requests {
        "failed"
    } else if is_finished {
        if batch.completed_at.is_some() {
            "completed"
        } else if batch.failed_at.is_some() {
            "failed"
        } else {
            "finalizing"
        }
    } else {
        "in_progress"
    };

    let in_progress_at = if openai_status != "validating" {
        batch.requests_started_at.map(|dt| dt.timestamp())
    } else {
        None
    };

    let finalizing_at = if openai_status == "finalizing" || openai_status == "completed" {
        batch.finalizing_at.map(|dt| dt.timestamp())
    } else {
        None
    };
    let completed_at = batch.completed_at.map(|dt| dt.timestamp());
    let failed_at = batch.failed_at.map(|dt| dt.timestamp());
    let cancelled_at = batch.cancelled_at.map(|dt| dt.timestamp());

    let errors = batch.errors.and_then(|e| serde_json::from_value::<BatchErrors>(e).ok());

    let is_background = batch.service_tier.as_deref() == Some("background");
    let completion_window = if is_background {
        "background".to_string()
    } else {
        batch
            .completion_window
            .clone()
            .expect("non-background batch must have a completion window")
    };
    let expires_at = if is_background {
        None
    } else {
        Some(
            batch
                .expires_at
                .as_ref()
                .expect("non-background batch must have an expiry")
                .timestamp(),
        )
    };
    let expired_at = batch
        .expires_at
        .as_ref()
        .filter(|expires_at| !is_background && chrono::Utc::now() > **expires_at)
        .map(|expires_at| expires_at.timestamp());

    BatchResponse {
        id: batch.id.0.to_string(),
        object_type: BatchObjectType::Batch,
        endpoint: batch.endpoint.clone(),
        errors,
        input_file_id: batch.file_id.map(|id| id.0.to_string()).unwrap_or_default(),
        completion_window,
        status: openai_status.to_string(),
        output_file_id: batch.output_file_id.map(|id| id.0.to_string()),
        error_file_id: batch.error_file_id.map(|id| id.0.to_string()),
        created_at: batch.created_at.timestamp(),
        in_progress_at,
        expires_at,
        finalizing_at,
        completed_at,
        failed_at,
        expired_at,
        cancelling_at: batch.cancelling_at.map(|dt| dt.timestamp()),
        cancelled_at,
        request_counts: RequestCounts {
            total: batch.total_requests,
            completed: batch.completed_requests,
            failed: batch.failed_requests,
        },
        metadata: if metadata.is_empty() { None } else { Some(metadata) },
        model,
        analytics: None,
        dwext: if dwext.is_empty() { None } else { Some(dwext) },
    }
}

pub(crate) fn batch_model_label(models: &[String]) -> Option<String> {
    match models {
        [] => None,
        [only] => Some(only.clone()),
        _ => Some("mixed".to_string()),
    }
}

async fn fetch_creator_email(db: &sqlx::PgPool, batch: &fusillade::Batch) -> Option<String> {
    if batch.created_by.is_empty() {
        return None;
    }
    let user_id = Uuid::parse_str(&batch.created_by).ok()?;
    let mut conn = db.acquire().await.ok()?;
    Users::new(&mut conn).get_by_id(user_id).await.ok().flatten().map(|u| u.email)
}

const RESERVED_METADATA_KEYS: [&str; 7] = [
    "created_by_email",
    "context_name",
    "context_type",
    "request_source",
    "created_by",
    "dw_user_agent",
    "dw_model",
];

fn is_reserved_metadata_key(key: &str) -> bool {
    let normalised = key.to_ascii_lowercase().replace('-', "_");
    RESERVED_METADATA_KEYS.contains(&normalised.as_str())
}

#[utoipa::path(
    post,
    path = "/batches",
    tag = "batches",
    summary = "Create batch",
    description = "Create and start processing a batch from an uploaded file.

The batch will begin processing immediately. Use `GET /batches/{batch_id}` to monitor progress.",
    request_body = CreateBatchRequest,
    responses(
        (status = 201, description = "Batch created and queued for processing.", body = BatchResponse),
        (status = 400, description = "Invalid request — check that the endpoint and completion_window are valid."),
        (status = 402, description = "Insufficient credits — account balance is below zero."),
        (status = 404, description = "Input file not found or you don't have access to it."),
        (status = 422, description = "A reasoning effort maps to an absolute token budget but the request does not provide a sufficient output-token limit."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, input_file_id = %req.input_file_id))]
pub async fn create_batch<P: PoolProvider>(
    State(state): State<AppState<P>>,
    current_user: RequiresPermission<resource::Batches, operation::CreateOwn>,
    headers: axum::http::HeaderMap,
    Json(req): Json<CreateBatchRequest>,
) -> Result<(StatusCode, Json<BatchResponse>)> {
    let config = state.current_config();
    let is_background = req.completion_window == "background";
    if is_background && !can_run_background_inference(&current_user) {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Batches, Operation::CreateOwn),
            action: Operation::CreateOwn,
            resource: "background inference; assign the BackgroundInferenceUser role to submit this completion_window".to_string(),
        });
    }

    let can_use_any_window = current_user.can_manage_platform();
    if !is_background && !can_use_any_window && !config.batches.allowed_completion_windows.contains(&req.completion_window) {
        let allowed: Vec<&str> = config.batches.allowed_completion_windows.iter().map(|w| w.as_str()).collect();

        return Err(Error::BadRequest {
            message: format!("Unsupported completion_window. Allowed values: {}", allowed.join(", ")),
        });
    }

    let supported_endpoints = &config.batches.allowed_url_paths;
    if !supported_endpoints.iter().any(|endpoint| endpoint == &req.endpoint) {
        return Err(Error::BadRequest {
            message: format!(
                "Unsupported endpoint '{}'. Supported: {}",
                req.endpoint,
                supported_endpoints.join(", ")
            ),
        });
    }

    let file_id = Uuid::parse_str(&req.input_file_id).map_err(|_| Error::BadRequest {
        message: "Invalid input_file_id format".to_string(),
    })?;

    let balance_check_id = current_user.active_organization.unwrap_or(current_user.id);
    {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Internal {
            operation: format!("get db connection for credit check: {}", e),
        })?;
        crate::api::handlers::modalities::ensure_batch_enabled(&mut conn, balance_check_id).await?;
        let balance = Credits::new(&mut conn)
            .get_balance_for_admission(balance_check_id)
            .await
            .map_err(|e| Error::Internal {
                operation: format!("check credit balance: {}", e),
            })?;
        if let Some(balance) = balance
            && balance < rust_decimal::Decimal::ZERO
        {
            return Err(Error::InsufficientCredits {
                current_balance: balance,
                message: "Account balance too low. Please add credits to continue.".to_string(),
            });
        }
    }

    let file = state
        .request_manager
        .get_file_from_primary_pool(fusillade::FileId(file_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "File".to_string(),
            id: req.input_file_id.clone(),
        })?;

    use crate::types::Resource;
    let has_read_all = can_read_all_resources(&current_user, Resource::Files);
    if !has_read_all
        && !file
            .uploaded_by
            .as_deref()
            .is_some_and(|owner| is_batch_owner(&current_user, owner))
    {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Files, Operation::ReadAll),
            action: Operation::CreateOwn,
            resource: format!("batch using file {}", req.input_file_id),
        });
    }

    if file.status != fusillade::batch::FileStatus::Processed {
        return Err(Error::BadRequest {
            message: format!("Input file {} is {} and cannot be used for a batch", req.input_file_id, file.status),
        });
    }

    if let Some(ref uploaded_by) = file.uploaded_by
        && let Ok(file_owner_id) = uuid::Uuid::parse_str(uploaded_by)
    {
        let file_owner_id = crate::types::UserId::from(file_owner_id);
        if file_owner_id != balance_check_id {
            let mut conn = state.db.write().acquire().await.map_err(|e| Error::Internal {
                operation: format!("get db connection for file owner credit check: {}", e),
            })?;
            let owner_balance = Credits::new(&mut conn)
                .get_balance_for_admission(file_owner_id)
                .await
                .map_err(|e| Error::Internal {
                    operation: format!("check file owner credit balance: {}", e),
                })?;
            if let Some(owner_balance) = owner_balance
                && owner_balance < rust_decimal::Decimal::ZERO
            {
                let owner_name = {
                    let mut users_repo = Users::new(&mut conn);
                    users_repo
                        .get_by_id(file_owner_id)
                        .await
                        .ok()
                        .flatten()
                        .map(|u| u.display_name.unwrap_or(u.username))
                        .unwrap_or_else(|| file_owner_id.to_string())
                };
                return Err(Error::InsufficientCredits {
                    current_balance: owner_balance,
                    message: format!(
                        "File owner ({}) does not have enough balance. Please add credits to their account or upload a new file.",
                        owner_name
                    ),
                });
            }
        }
    }

    let (file_model_counts, batch_model_info) = load_and_validate_batch_models(&state, file_id).await?;

    let model_aliases: Vec<String> = file_model_counts.keys().cloned().collect();

    if !is_background && !can_use_any_window {
        for (alias, allowed_windows) in &batch_model_info.allowed_windows {
            if !allowed_windows.contains(&req.completion_window) {
                if allowed_windows.is_empty() {
                    return Err(Error::BadRequest {
                        message: format!("Model '{}' does not support batch processing.", alias),
                    });
                }
                return Err(Error::BadRequest {
                    message: format!(
                        "Model '{}' does not support completion window '{}'. Allowed: {}",
                        alias,
                        req.completion_window,
                        allowed_windows.join(", ")
                    ),
                });
            }
        }
    }

    let model_throughputs = batch_model_info.throughputs;
    let model_ids_by_alias = get_model_ids_by_aliases(&state, &model_aliases).await?;

    let request_source = if current_user.api_key_id.is_some() { "api" } else { "frontend" };

    let creator_user_agent = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(|ua| ua.chars().take(256).collect::<String>())
        .filter(|ua| !ua.is_empty());

    let target_user_id = current_user.active_organization.unwrap_or(current_user.id);

    let caps = {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        resolve_key_capabilities(&current_user, target_user_id, &mut conn)
            .await
            .map_err(Error::Database)?
    };
    let selected_key = if let Some(selected_id) = req.api_key_id {
        if current_user.active_organization.is_none() {
            return Err(Error::BadRequest {
                message: "api_key_id selection is only available in organization context".to_string(),
            });
        }
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let key = ApiKeys::new(&mut conn)
            .get_by_id(selected_id)
            .await
            .map_err(Error::Database)?
            .filter(|key| key.user_id == target_user_id)
            .filter(|key| !key.hidden)
            .filter(|key| key.parent_api_key_id.is_none())
            .filter(|key| matches!(key.purpose, ApiKeyPurpose::Realtime))
            .filter(|key| caps.is_org_manager || key.created_by == current_user.id)
            .ok_or_else(|| Error::NotFound {
                resource: "API key".to_string(),
                id: selected_id.to_string(),
            })?;
        Some(key)
    } else {
        None
    };
    if selected_key.is_none() && current_user.api_key_id.is_none() && caps.requires_batch_key_selection() {
        return Err(Error::BadRequest {
            message: "This organization manages API keys centrally: select one of your issued API keys (api_key_id) to create a batch."
                .to_string(),
        });
    }
    let (attribution_user_id, authenticating_key_id) = match &selected_key {
        Some(key) => (key.created_by, Some(key.id)),
        None => (current_user.id, current_user.api_key_id),
    };

    let (api_key_id, target_verified) = {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let key_id = ApiKeys::new(&mut conn)
            .resolve_batch_execution_key(target_user_id, attribution_user_id, authenticating_key_id)
            .await
            .map_err(Error::Database)?;
        let execution_account_id = ApiKeys::new(&mut conn)
            .get_by_id(key_id)
            .await
            .map_err(Error::Database)?
            .ok_or_else(|| Error::NotFound {
                resource: "API key".to_string(),
                id: key_id.to_string(),
            })?
            .user_id;
        if execution_account_id != balance_check_id {
            let balance = Credits::new(&mut conn)
                .get_balance_for_admission(execution_account_id)
                .await
                .map_err(Error::Database)?;
            if let Some(balance) = balance
                && balance < rust_decimal::Decimal::ZERO
            {
                return Err(Error::InsufficientCredits {
                    current_balance: balance,
                    message: "Execution key account balance too low. Please add credits to continue.".to_string(),
                });
            }
        }
        if ApiKeys::new(&mut conn).is_scope_exhausted(key_id).await.map_err(Error::Database)? {
            return Err(Error::SpendCapExceeded {
                message: "The API key used for this batch has reached its spending cap. Raise or remove the cap, or wait for the cap window to reset, then resubmit.".to_string(),
            });
        }
        let verified = Users::new(&mut conn).is_verified(target_user_id).await?;
        (key_id, verified)
    };

    let mut metadata_map = req.metadata.unwrap_or_default();
    metadata_map.retain(|key, _| !is_reserved_metadata_key(key));
    metadata_map.insert("request_source".to_string(), request_source.to_string());
    metadata_map.insert("created_by".to_string(), current_user.id.to_string());
    if let Some(user_agent) = creator_user_agent {
        metadata_map.insert("dw_user_agent".to_string(), user_agent);
    }
    let model_aliases_for_label: Vec<String> = file_model_counts.keys().cloned().collect();
    if let Some(label) = batch_model_label(&model_aliases_for_label) {
        metadata_map.insert("dw_model".to_string(), label);
    }
    let metadata = serde_json::to_value(metadata_map).ok();

    let total_requests: i64 = file_model_counts.values().sum();

    let batch = if is_background {
        state
            .request_manager
            .create_background_batch_record(fusillade::BackgroundBatchInput {
                file_id: fusillade::FileId(file_id),
                endpoint: req.endpoint.clone(),
                metadata,
                created_by: Some(target_user_id.to_string()),
                api_key_id: Some(api_key_id),
                total_requests: Some(total_requests),
            })
            .await
            .map_err(|e| match e {
                fusillade::FusilladeError::ValidationError(message) => Error::BadRequest { message },
                e => Error::Internal {
                    operation: format!("create background batch record: {e}"),
                },
            })?
    } else {
        crate::api::handlers::unverified_volume::enforce_unverified_volume_limit(
            &*state.request_manager,
            config.batches.unverified_requests_per_completion_hour,
            target_user_id,
            target_verified,
            &req.completion_window,
            total_requests,
            crate::api::handlers::unverified_volume::SubmissionKind::Batch,
        )
        .await?;

        let batch_input = fusillade::BatchInput {
            file_id: fusillade::FileId(file_id),
            endpoint: req.endpoint.clone(),
            completion_window: req.completion_window.clone(),
            metadata,
            created_by: Some(target_user_id.to_string()),
            api_key_id: Some(api_key_id),
            total_requests: Some(total_requests),
        };

        let reservation_ids = reserve_capacity_for_batch(
            &state,
            &req.completion_window,
            &file_model_counts,
            &model_throughputs,
            &model_ids_by_alias,
            config.batches.relaxation_factor(&req.completion_window),
        )
        .await?;

        let _release_guard = scopeguard::guard(reservation_ids.clone(), |ids| {
            let state = state.clone();
            tokio::runtime::Handle::current().spawn(async move {
                if let Err(e) = release_capacity_reservations(&state, &ids).await {
                    tracing::warn!(
                        reservation_ids = ?ids,
                        error = %e,
                        "Failed to release capacity reservations — will expire via TTL"
                    );
                }
            });
        });

        state.request_manager.create_batch_record(batch_input).await.map_err(|e| match e {
            fusillade::FusilladeError::ValidationError(message) => Error::BadRequest { message },
            e => Error::Internal {
                operation: format!("create batch record: {e}"),
            },
        })?
    };

    if let Err(e) = state
        .task_runner
        .create_batch_job
        .enqueue(&CreateBatchInput {
            batch_id: *batch.id,
            file_id,
        })
        .await
    {
        let _ = state
            .request_manager
            .mark_batch_failed(batch.id, &format!("Failed to enqueue population: {e}"))
            .await;
        return Err(Error::Internal {
            operation: format!("enqueue batch population: {}", e),
        });
    }

    tracing::debug!("Batch {} created, population enqueued", batch.id);


    Ok((
        StatusCode::CREATED,
        Json(to_batch_response_with_email(batch, Some(&current_user.email))),
    ))
}

async fn load_and_validate_batch_models<P: PoolProvider>(
    state: &AppState<P>,
    file_id: Uuid,
) -> Result<(HashMap<String, i64>, BatchModelInfo)> {
    let file_model_counts = {
        let mut templates_conn = state.request_manager.begin_write().await.map_err(|e| Error::Internal {
            operation: format!("begin primary Fusillade transaction for batch model counts: {e}"),
        })?;
        BatchTemplates::new(&mut templates_conn)
            .get_model_counts(file_id)
            .await
            .map_err(Error::Database)?
    };
    let model_aliases = file_model_counts.keys().cloned().collect::<Vec<_>>();

    let (batch_model_info, reasoning_policies) = {
        let mut control_conn = state.db.write().acquire().await.map_err(|e| Error::Internal {
            operation: format!("get primary control database connection for batch validation: {e}"),
        })?;
        let batch_model_info = Deployments::new(&mut control_conn)
            .get_batch_model_info(&model_aliases)
            .await
            .map_err(Error::Database)?;
        let reasoning_policies = Deployments::new(&mut control_conn)
            .get_reasoning_policies(&model_aliases)
            .await
            .map_err(Error::Database)?;
        (batch_model_info, reasoning_policies)
    };

    let mut templates_conn = state.request_manager.begin_write().await.map_err(|e| Error::Internal {
        operation: format!("begin primary Fusillade transaction for batch request validation: {e}"),
    })?;
    let mut templates = BatchTemplates::new(&mut templates_conn);
    let mut stream = templates.stream_reasoning_requests(file_id);
    while let Some(template) = stream.next().await {
        let template = template.map_err(Error::Database)?;
        let line = i64::from(template.line_number) + 1;
        let context = match template.custom_id.as_deref() {
            Some(custom_id) => format!("Line {line} (custom_id '{custom_id}')"),
            None => format!("Line {line}"),
        };
        let body: serde_json::Value = serde_json::from_str(&template.body).map_err(|error| Error::BadRequest {
            message: format!("{context}: stored request body is not valid JSON: {error}"),
        })?;
        let validation = reasoning_policies
            .get(&template.model)
            .cloned()
            .unwrap_or_default()
            .validate_request(&template.path, &body);
        if let Err(error) = validation {
            let message = format!("{context}: {}", error.message());
            return Err(match error.status_code() {
                422 => Error::UnprocessableEntity { message },
                _ => Error::BadRequest { message },
            });
        }
    }

    Ok((file_model_counts, batch_model_info))
}

async fn get_model_ids_by_aliases<P: PoolProvider>(state: &AppState<P>, model_aliases: &[String]) -> Result<HashMap<String, Uuid>> {
    if model_aliases.is_empty() {
        return Ok(HashMap::new());
    }

    let mut conn = state.db.read().acquire().await.map_err(|e| Error::Internal {
        operation: format!("get db connection: {}", e),
    })?;

    let result = crate::db::handlers::deployments::Deployments::new(&mut conn)
        .get_model_ids_by_aliases(model_aliases)
        .await
        .map_err(|e| Error::Internal {
            operation: format!("get model ids: {}", e),
        })?;

    let missing: Vec<&str> = model_aliases
        .iter()
        .filter(|alias| !result.contains_key(*alias))
        .map(|alias| alias.as_str())
        .collect();

    if !missing.is_empty() {
        return Err(Error::BadRequest {
            message: format!(
                "The following model(s) are no longer available: {}. \
                 The batch file references models that have been removed.",
                missing.join(", ")
            ),
        });
    }

    Ok(result)
}

async fn reserve_capacity_for_batch<P: PoolProvider>(
    state: &AppState<P>,
    completion_window: &str,
    file_model_counts: &HashMap<String, i64>,
    model_throughputs: &HashMap<String, f32>,
    model_ids_by_alias: &HashMap<String, Uuid>,
    relaxation_factor: f32,
) -> Result<Vec<Uuid>> {
    use super::sla_capacity::{CapacityError, CapacityReservationInput, reserve_capacity};

    let config = state.current_config();
    let input = CapacityReservationInput {
        completion_window,
        file_model_counts,
        model_throughputs,
        model_ids_by_alias,
        default_throughput: config.batches.default_throughput,
        relaxation_factor,
        reservation_ttl_secs: config.batches.reservation_ttl_secs,
        include_pending_counts: config.batches.pending_capacity_counts_enabled,
    };

    let pool = state.db.write();
    reserve_capacity(&pool, &*state.request_manager, &input).await.map_err(|e| match e {
        CapacityError::InsufficientCapacity { completion_window, models } => Error::TooManyRequests {
            message: format!(
                "Insufficient capacity for {} completion window. The following models are currently at capacity: {}. Try again later or use a longer completion window.",
                completion_window, models
            ),
        },
        CapacityError::Internal(msg) => Error::Internal { operation: msg },
    })
}

async fn release_capacity_reservations<P: PoolProvider>(state: &AppState<P>, reservation_ids: &[Uuid]) -> Result<()> {
    super::sla_capacity::release_reservations(&state.db.write(), reservation_ids)
        .await
        .map_err(|msg| Error::Internal { operation: msg })
}

#[utoipa::path(
    get,
    path = "/batches/{batch_id}",
    tag = "batches",
    summary = "Retrieve batch",
    description = "Retrieve the current status and details of a batch.

Poll this endpoint to monitor progress. Results are streamed to `output_file_id` as they complete — you can start downloading results before the batch finishes.",
    responses(
        (status = 200, description = "Batch details including status, progress counts, and output file IDs.", body = BatchResponse),
        (status = 404, description = "Batch not found or you don't have access to it."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("batch_id" = String, Path, description = "The batch ID returned when the batch was created.")
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, batch_id = %batch_id_str))]
pub async fn get_batch<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(batch_id_str): Path<String>,
    current_user: RequiresPermission<resource::Batches, operation::ReadOwn>,
) -> Result<Json<BatchResponse>> {
    let batch_id = Uuid::parse_str(&batch_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid batch ID format".to_string(),
    })?;

    let batch = state
        .request_manager
        .get_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        })?;

    let can_read_all = can_read_all_resources(&current_user, Resource::Batches);
    if !can_read_all && !is_batch_owner(&current_user, &batch.created_by) {
        return Err(Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str,
        });
    }

    let mut read_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let created_by_email = if let Some(api_key_id) = batch.api_key_id {
        let creator_map = ApiKeys::new(&mut read_conn)
            .get_creators_by_key_ids(vec![api_key_id])
            .await
            .map_err(Error::Database)?;
        if let Some(&creator_id) = creator_map.get(&api_key_id) {
            Users::new(&mut read_conn)
                .get_bulk(vec![creator_id])
                .await
                .map_err(|e| Error::Internal {
                    operation: format!("fetch creator user: {}", e),
                })?
                .get(&creator_id)
                .map(|u| u.email.clone())
        } else {
            None
        }
    } else {
        if let Ok(user_id) = Uuid::parse_str(&batch.created_by) {
            Users::new(&mut read_conn)
                .get_bulk(vec![user_id])
                .await
                .map_err(|e| Error::Internal {
                    operation: format!("fetch owner user: {}", e),
                })?
                .get(&user_id)
                .map(|u| u.email.clone())
        } else {
            None
        }
    };

    let (context_name, context_type) = if let Ok(owner_id) = Uuid::parse_str(&batch.created_by) {
        let user_map = Users::new(&mut read_conn)
            .get_bulk(vec![owner_id])
            .await
            .map_err(|e| Error::Internal {
                operation: format!("fetch owner user: {}", e),
            })?;
        match user_map.get(&owner_id) {
            Some(u) if u.user_type == "organization" => {
                let name = u.display_name.clone().unwrap_or_else(|| u.email.clone());
                (Some(name), Some("organization".to_string()))
            }
            Some(_) => (Some("Personal".to_string()), Some("personal".to_string())),
            None => (None, None),
        }
    } else {
        (None, None)
    };

    let is_sync = batch
        .metadata
        .as_ref()
        .and_then(|m| m.get("request_source"))
        .and_then(|v| v.as_str())
        == Some("sync");
    let source_name = if is_sync {
        if let Some(conn_id) = batch
            .metadata
            .as_ref()
            .and_then(|m| m.get("dw_source_id"))
            .and_then(|v| v.as_str())
            .and_then(|s| Uuid::parse_str(s).ok())
        {
            match Connections::new(&mut read_conn).get_by_id(conn_id).await {
                Ok(Some(conn)) if Uuid::parse_str(&batch.created_by).is_ok_and(|owner| conn.user_id == owner) => Some(conn.name),
                Ok(_) => None,
                Err(e) => {
                    tracing::warn!(error = %e, connection_id = %conn_id, "Failed to look up connection name");
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    let mut response = to_batch_response_enriched(batch, None, source_name.as_deref());

    if let Some(email) = created_by_email {
        response
            .metadata
            .get_or_insert_with(HashMap::new)
            .insert("created_by_email".to_string(), email);
    }
    if let Some(name) = context_name {
        response
            .metadata
            .get_or_insert_with(HashMap::new)
            .insert("context_name".to_string(), name);
    }
    if let Some(ctype) = context_type {
        response
            .metadata
            .get_or_insert_with(HashMap::new)
            .insert("context_type".to_string(), ctype);
    }

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/batches/{batch_id}/analytics",
    summary = "Get batch analytics",
    description = "Retrieve aggregated metrics for a batch including token usage, costs, and latency statistics.

Analytics update in real-time as requests complete.",
    responses(
        (status = 200, description = "Batch analytics with token counts, costs, and performance metrics.", body = BatchAnalytics),
        (status = 404, description = "Batch not found or you don't have access to it."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("batch_id" = String, Path, description = "The batch ID returned when the batch was created.")
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, batch_id = %batch_id_str))]
pub async fn get_batch_analytics<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(batch_id_str): Path<String>,
    current_user: RequiresPermission<resource::Batches, operation::ReadOwn>,
) -> Result<Json<BatchAnalytics>> {
    let batch_id = Uuid::parse_str(&batch_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid batch ID format".to_string(),
    })?;

    let batch = state
        .request_manager
        .get_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        })?;

    let can_read_all = can_read_all_resources(&current_user, Resource::Batches);
    if !can_read_all && !is_batch_owner(&current_user, &batch.created_by) {
        return Err(Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        });
    }

    let analytics = crate::db::handlers::analytics::get_batch_analytics(&state.db.read(), &batch_id)
        .await
        .map_err(|e| Error::Internal {
            operation: format!("fetch batch analytics: {}", e),
        })?
        .unwrap_or_default();

    Ok(Json(analytics))
}

#[utoipa::path(
    get,
    path = "/batches/{batch_id}/results",
    tag = "batches",
    summary = "Get batch results",
    description = "Stream batch results with merged input/output data as JSONL.

Each line contains the original input body, response body (for completed requests), error message (for failed requests), and current status. Results are filtered to show exactly one entry per input template (excluding superseded requests from escalation races).

Supports pagination via `limit` and `skip` query parameters, and filtering by `custom_id` via the `search` parameter.",
    responses(
        (status = 200, description = "Batch results as newline-delimited JSON. Check the `X-Incomplete` header to determine if more results exist.", content_type = "application/x-ndjson"),
        (status = 404, description = "Batch not found or you don't have access to it."),
        (status = 410, description = "The batch's output or error file has been deleted or has passed its `expires_at`, so its results are no longer available. Results whose input file was deleted or has expired are still returned, with `input_body` set to null."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("batch_id" = String, Path, description = "The batch ID returned when the batch was created."),
        BatchResultsQuery
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, batch_id = %batch_id_str))]
pub async fn get_batch_results<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(batch_id_str): Path<String>,
    Query(query): Query<BatchResultsQuery>,
    current_user: RequiresPermission<resource::Batches, operation::ReadOwn>,
) -> Result<axum::response::Response> {
    let batch_id = Uuid::parse_str(&batch_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid batch ID format".to_string(),
    })?;

    let batch = state
        .request_manager
        .get_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        })?;

    let can_read_all = can_read_all_resources(&current_user, Resource::Batches);
    if !can_read_all && !is_batch_owner(&current_user, &batch.created_by) {
        return Err(Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        });
    }

    let deleted_results_files: Vec<&str> = [
        (batch.output_file_id.is_none(), "output file"),
        (batch.error_file_id.is_none(), "error file"),
    ]
    .into_iter()
    .filter_map(|(deleted, name)| deleted.then_some(name))
    .collect();
    let (Some(output_file_id), Some(error_file_id)) = (batch.output_file_id, batch.error_file_id) else {
        return Err(Error::Gone {
            message: format!(
                "Results for batch {} are no longer available: its {} deleted.",
                batch_id_str,
                match deleted_results_files.as_slice() {
                    [one] => format!("{one} has been"),
                    _ => "output and error files have been".to_string(),
                }
            ),
        });
    };

    let (output_file, error_file) = futures::try_join!(
        state.request_manager.get_file(output_file_id),
        state.request_manager.get_file(error_file_id),
    )
    .map_err(|e| Error::Internal {
        operation: format!("fetch results files for batch {batch_id_str}: {e}"),
    })?;
    let now = chrono::Utc::now();
    let is_expired =
        |file: &fusillade::File| file.status == fusillade::batch::FileStatus::Expired || file.expires_at.is_some_and(|at| at <= now);
    if is_expired(&output_file) || is_expired(&error_file) {
        return Err(Error::Gone {
            message: format!("Results for batch {batch_id_str} are no longer available: its results files have expired."),
        });
    }

    let still_processing = batch.pending_requests > 0 || batch.in_progress_requests > 0;

    let offset = query.pagination.skip() as usize;
    let search = query.search.clone();
    let status = query.status.clone();
    let requested_limit = query.pagination.limit.map(|_| query.pagination.limit() as usize);

    if let Some(limit) = requested_limit {
        let results_stream = state
            .request_manager
            .get_batch_results_stream(fusillade::BatchId(batch_id), offset, search, status);

        let mut buffer: Vec<_> = results_stream.take(limit + 1).collect().await;
        let has_more_pages = buffer.len() > limit;
        buffer.truncate(limit);

        let line_count = buffer.len();
        let last_line = offset + line_count;
        let has_more = has_more_pages || still_processing;

        let mut jsonl_lines = Vec::new();
        for result in buffer {
            let json_line = result
                .and_then(|item| {
                    serde_json::to_string(&item)
                        .map(|json| format!("{}\n", json))
                        .map_err(|e| fusillade::FusilladeError::Other(anyhow::anyhow!("JSON serialization failed: {}", e)))
                })
                .map_err(|e| Error::Internal {
                    operation: format!("serialize batch result: {}", e),
                })?;
            jsonl_lines.push(json_line);
        }

        let jsonl_content = jsonl_lines.join("");

        let mut response = axum::response::Response::new(Body::from(jsonl_content));
        response
            .headers_mut()
            .insert("content-type", "application/x-ndjson".parse().unwrap());
        response.headers_mut().insert("X-Incomplete", has_more.to_string().parse().unwrap());
        response.headers_mut().insert("X-Last-Line", last_line.to_string().parse().unwrap());
        *response.status_mut() = StatusCode::OK;

        Ok(response)
    } else {
        let expected_count = if search.is_none() {
            let count = match status.as_deref() {
                Some("completed") => batch.completed_requests,
                Some("failed") => batch.failed_requests,
                Some("pending") => batch.pending_requests,
                Some("in_progress") => batch.in_progress_requests,
                _ => batch.total_requests,
            };
            Some((count as usize).saturating_sub(offset))
        } else {
            None
        };

        let results_stream = state
            .request_manager
            .get_batch_results_stream(fusillade::BatchId(batch_id), offset, search, status);

        let results_stream: Pin<Box<dyn futures::Stream<Item = fusillade::Result<fusillade::batch::BatchResultItem>> + Send>> =
            if let Some(count) = expected_count {
                Box::pin(results_stream.take(count))
            } else {
                Box::pin(results_stream)
            };

        let body_stream = results_stream.map(|result| {
            result
                .and_then(|item| {
                    serde_json::to_string(&item)
                        .map(|json| Bytes::from(format!("{}\n", json)))
                        .map_err(|e| fusillade::FusilladeError::Other(anyhow::anyhow!("JSON serialization failed: {}", e)))
                })
                .map_err(|e| std::io::Error::other(e.to_string()))
        });

        let body = Body::from_stream(body_stream);
        let mut response = axum::response::Response::new(body);
        response
            .headers_mut()
            .insert("content-type", "application/x-ndjson".parse().unwrap());
        response
            .headers_mut()
            .insert("X-Incomplete", still_processing.to_string().parse().unwrap());
        if let Some(count) = expected_count {
            let last_line = offset + count;
            response.headers_mut().insert("X-Last-Line", last_line.to_string().parse().unwrap());
        }
        *response.status_mut() = StatusCode::OK;

        Ok(response)
    }
}

#[utoipa::path(
    post,
    path = "/batches/{batch_id}/cancel",
    tag = "batches",
    summary = "Cancel batch",
    description = "Cancel an in-progress batch.

Pending requests will not be processed. Requests already in progress will complete. The batch status will transition to `cancelling` then `cancelled`.",
    responses(
        (status = 200, description = "Cancellation initiated. The batch will finish processing in-flight requests.", body = BatchResponse),
        (status = 404, description = "Batch not found or you don't have access to it."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("batch_id" = String, Path, description = "The batch ID returned when the batch was created.")
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, batch_id = %batch_id_str))]
pub async fn cancel_batch<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(batch_id_str): Path<String>,
    current_user: RequiresPermission<resource::Batches, operation::UpdateOwn>,
) -> Result<Json<BatchResponse>> {
    let batch_id = Uuid::parse_str(&batch_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid batch ID format".to_string(),
    })?;

    let batch = state
        .request_manager
        .get_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        })?;

    let can_update_all = has_permission(&current_user, Resource::Batches, Operation::UpdateAll);
    if !can_update_all && !is_batch_owner(&current_user, &batch.created_by) {
        return Err(Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        });
    }

    state
        .request_manager
        .cancel_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|e| Error::Internal {
            operation: format!("cancel batch: {}", e),
        })?;

    enqueue_cascade_batch_state(&state, batch_id, CascadeTarget::Canceled).await;

    let batch = state
        .request_manager
        .get_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        })?;

    tracing::debug!("Batch {} cancelled", batch_id);

    let creator_email = fetch_creator_email(&state.db.read(), &batch).await;
    Ok(Json(to_batch_response_with_email(batch, creator_email.as_deref())))
}

#[utoipa::path(
    delete,
    path = "/batches/{batch_id}",
    tag = "batches",
    summary = "Delete batch",
    description = "Permanently delete a batch and all its associated data.

This action cannot be undone. The input file is not deleted.",
    responses(
        (status = 204, description = "Batch deleted successfully."),
        (status = 400, description = "Invalid batch ID format."),
        (status = 404, description = "Batch not found or you don't have access to it."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("batch_id" = String, Path, description = "The batch ID returned when the batch was created.")
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, batch_id = %batch_id_str))]
pub async fn delete_batch<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(batch_id_str): Path<String>,
    current_user: RequiresPermission<resource::Batches, operation::DeleteOwn>,
) -> Result<StatusCode> {
    let batch_id = Uuid::parse_str(&batch_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid batch ID format".to_string(),
    })?;

    let batch = state
        .request_manager
        .get_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        })?;

    let can_delete_all = has_permission(&current_user, Resource::Batches, Operation::DeleteAll);
    if !can_delete_all && !is_batch_owner(&current_user, &batch.created_by) {
        return Err(Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        });
    }

    state
        .request_manager
        .delete_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|e| Error::Internal {
            operation: format!("delete batch: {}", e),
        })?;

    enqueue_cascade_batch_state(&state, batch_id, CascadeTarget::Canceled).await;

    tracing::debug!("Batch {} deleted", batch_id);

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/batches/{batch_id}/retry",
    tag = "batches",
    summary = "Retry failed and canceled requests",
    description = "Retry a batch: failed AND canceled requests are reset to pending and will be processed again; completed work is never redone. Retrying a cancelled batch overturns the cancellation (cancel-then-retry acts as pause/resume), so this also succeeds on a just-cancelled batch whose requests had not yet settled. Use it after fixing transient issues, increasing rate limits, or to resume a cancelled batch.",
    responses(
        (status = 200, description = "Requests queued for retry (or a cancellation was overturned and the batch resumed).", body = BatchResponse),
        (status = 400, description = "The batch cannot be retried: it has no failed or canceled requests and was not cancelled, it is past the retry age cut-off, or its input or results have been deleted by content retention."),
        (status = 404, description = "Batch not found or you don't have access to it."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("batch_id" = String, Path, description = "The batch ID returned when the batch was created.")
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, batch_id = %batch_id_str))]
pub async fn retry_failed_batch_requests<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(batch_id_str): Path<String>,
    current_user: RequiresPermission<resource::Batches, operation::UpdateOwn>,
) -> Result<Json<BatchResponse>> {
    let batch_id = Uuid::parse_str(&batch_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid batch ID format".to_string(),
    })?;

    let batch = state
        .request_manager
        .get_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        })?;

    let can_update_all = has_permission(&current_user, Resource::Batches, Operation::UpdateAll);
    if !can_update_all && !is_batch_owner(&current_user, &batch.created_by) {
        return Err(Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        });
    }

    let retried_count = state
        .request_manager
        .retry_failed_requests_for_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|e| match e {
            fusillade::FusilladeError::ValidationError(message) => Error::BadRequest { message },
            e => Error::Internal {
                operation: format!("retry failed requests: {}", e),
            },
        })?;

    if retried_count == 0 && batch.cancelling_at.is_none() {
        return Err(Error::BadRequest {
            message: "Nothing to retry: no failed or canceled requests, and the batch is not cancelled".to_string(),
        });
    }

    tracing::debug!(
        batch_id = %batch_id,
        retried_count,
        "Retried failed requests"
    );

    let batch = state
        .request_manager
        .get_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        })?;

    let creator_email = fetch_creator_email(&state.db.read(), &batch).await;
    Ok(Json(to_batch_response_with_email(batch, creator_email.as_deref())))
}

#[utoipa::path(
    post,
    path = "/batches/{batch_id}/retry-requests",
    tag = "batches",
    summary = "Retry specific requests",
    description = "Retry specific failed requests by their IDs.

Use this for fine-grained control over which requests to retry, rather than retrying all failures.",
    request_body = RetryRequestsRequest,
    responses(
        (status = 200, description = "Specified requests queued for retry.", body = BatchResponse),
        (status = 400, description = "No valid request IDs provided, or the batch is past the retry age cut-off. Requests whose input has been deleted by content retention are left as they are; the batch is returned with status 200."),
        (status = 404, description = "Batch not found or you don't have access to it."),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ("batch_id" = String, Path, description = "The batch ID returned when the batch was created.")
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id, batch_id = %batch_id_str))]
pub async fn retry_specific_requests<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(batch_id_str): Path<String>,
    current_user: RequiresPermission<resource::Batches, operation::UpdateOwn>,
    Json(req): Json<RetryRequestsRequest>,
) -> Result<Json<BatchResponse>> {
    let batch_id = Uuid::parse_str(&batch_id_str).map_err(|_| Error::BadRequest {
        message: "Invalid batch ID format".to_string(),
    })?;

    let batch = state
        .request_manager
        .get_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        })?;

    let can_update_all = has_permission(&current_user, Resource::Batches, Operation::UpdateAll);
    if !can_update_all && !is_batch_owner(&current_user, &batch.created_by) {
        return Err(Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        });
    }

    let request_ids: Vec<fusillade::RequestId> = req
        .request_ids
        .iter()
        .filter_map(|id_str| Uuid::parse_str(id_str).ok().map(fusillade::RequestId))
        .collect();

    if request_ids.is_empty() {
        return Err(Error::BadRequest {
            message: "No valid request IDs provided".to_string(),
        });
    }

    tracing::debug!(
        batch_id = %batch_id,
        request_count = request_ids.len(),
        "Retrying specific requests"
    );

    let results = state
        .request_manager
        .retry_failed_requests(fusillade::BatchId(batch_id), request_ids.clone())
        .await
        .map_err(|e| match e {
            fusillade::FusilladeError::ValidationError(message) => Error::BadRequest { message },
            e => Error::Internal {
                operation: format!("retry failed requests: {}", e),
            },
        })?;

    let failed_retries: Vec<_> = results
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.as_ref().err().map(|e| (i, e)))
        .collect();

    if !failed_retries.is_empty() {
        tracing::warn!(
            batch_id = %batch_id,
            failed_retry_count = failed_retries.len(),
            "Some requests failed to retry"
        );
    }

    let successful_retries = results.iter().filter(|r| r.is_ok()).count();
    tracing::debug!(
        batch_id = %batch_id,
        retried_count = successful_retries,
        "Successfully retried specific requests"
    );

    let batch = state
        .request_manager
        .get_batch(fusillade::BatchId(batch_id))
        .await
        .map_err(|_| Error::NotFound {
            resource: "Batch".to_string(),
            id: batch_id_str.clone(),
        })?;

    let creator_email = fetch_creator_email(&state.db.read(), &batch).await;
    Ok(Json(to_batch_response_with_email(batch, creator_email.as_deref())))
}

#[utoipa::path(
    get,
    path = "/batches",
    tag = "batches",
    summary = "List batches",
    description = "Returns a paginated list of your batches, newest first.

Use cursor-based pagination: pass `last_id` from the response as the `after` parameter to fetch the next page.",
    responses(
        (status = 200, description = "List of batches. Check `has_more` to determine if additional pages exist.", body = BatchListResponse),
        (status = 500, description = "An unexpected error occurred. Retry the request or contact support if the issue persists.")
    ),
    params(
        ListBatchesQuery
    )
)]
#[tracing::instrument(skip_all, fields(user_id = %current_user.id))]
pub async fn list_batches<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Query(query): Query<ListBatchesQuery>,
    current_user: RequiresPermission<resource::Batches, operation::ReadOwn>,
) -> Result<Json<BatchListResponse>> {
    let limit = query.pagination.limit();

    let after = query
        .pagination
        .after
        .as_ref()
        .and_then(|after_str| Uuid::parse_str(after_str).ok().map(fusillade::BatchId));

    let can_read_all = can_read_all_resources(&current_user, Resource::Batches);
    let created_by = if let Some(org_id) = current_user.active_organization {
        Some(org_id.to_string())
    } else if can_read_all {
        None
    } else {
        Some(current_user.id.to_string())
    };

    let api_key_ids_filter = if let Some(member_id) = query.member_id {
        let mut read_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let key_ids = match current_user.active_organization {
            Some(org_id) => {
                ApiKeys::new(&mut read_conn)
                    .find_hidden_key_ids(org_id, ApiKeyPurpose::Batch, member_id)
                    .await
                    .map_err(Error::Database)?
            }
            None if can_read_all => {
                ApiKeys::new(&mut read_conn)
                    .find_all_hidden_key_ids_by_creator(ApiKeyPurpose::Batch, member_id)
                    .await
                    .map_err(Error::Database)?
            }
            None => {
                return Err(Error::BadRequest {
                    message: "member_id filter is only available in organization context or for platform managers".to_string(),
                });
            }
        };
        if key_ids.is_empty() {
            return Ok(Json(BatchListResponse {
                object_type: ListObjectType::List,
                data: vec![],
                first_id: None,
                last_id: None,
                has_more: false,
            }));
        }
        Some(key_ids)
    } else {
        None
    };

    let is_platform_manager = current_user.roles.contains(&Role::PlatformManager);
    let batch_class_filter = parse_batch_class_filter(query.completion_window.as_deref(), is_platform_manager)?;

    let batches = state
        .request_manager
        .list_batches(fusillade::ListBatchesFilter {
            created_by,
            search: query.search.clone(),
            after,
            limit: Some(limit + 1),
            api_key_ids: api_key_ids_filter,
            status: query.status.clone(),
            created_after: query.created_after,
            created_before: query.created_before,
            active_first: query.active_first,
            completion_windows: batch_class_filter.completion_windows,
            service_tiers: batch_class_filter.service_tiers,
        })
        .await
        .map_err(|e| match &e {
            fusillade::FusilladeError::ValidationError(msg) => Error::BadRequest { message: msg.clone() },
            _ => Error::Internal {
                operation: format!("list batches: {}", e),
            },
        })?;

    let has_more = batches.len() > limit as usize;
    let batches: Vec<_> = batches.into_iter().take(limit as usize).collect();

    let first_id = batches.first().map(|b| b.id.0.to_string());
    let last_id = batches.last().map(|b| b.id.0.to_string());

    use crate::db::models::users::UserDBResponse;
    let mut read_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let api_key_ids: Vec<Uuid> = batches
        .iter()
        .filter_map(|b| b.api_key_id)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let api_key_creator_map: HashMap<Uuid, Uuid> = if !api_key_ids.is_empty() {
        ApiKeys::new(&mut read_conn)
            .get_creators_by_key_ids(api_key_ids)
            .await
            .map_err(Error::Database)?
    } else {
        HashMap::new()
    };

    let mut all_user_ids: std::collections::HashSet<Uuid> = std::collections::HashSet::new();
    for batch in &batches {
        if let Ok(owner_id) = Uuid::parse_str(&batch.created_by) {
            all_user_ids.insert(owner_id);
        }
        if let Some(api_key_id) = batch.api_key_id
            && let Some(&creator_id) = api_key_creator_map.get(&api_key_id)
        {
            all_user_ids.insert(creator_id);
        }
    }

    let user_map: HashMap<Uuid, UserDBResponse> = if !all_user_ids.is_empty() {
        Users::new(&mut read_conn)
            .get_bulk(all_user_ids.into_iter().collect())
            .await
            .map_err(|e| Error::Internal {
                operation: format!("fetch users: {}", e),
            })?
    } else {
        HashMap::new()
    };

    let includes: Vec<&str> = query
        .include
        .as_ref()
        .map(|s| s.split(',').map(|s| s.trim()).collect())
        .unwrap_or_default();
    let include_analytics = includes.contains(&"analytics");

    let batch_ids: Vec<Uuid> = batches.iter().map(|b| b.id.0).collect();

    let source_conn_ids: Vec<Uuid> = batches
        .iter()
        .filter(|b| b.metadata.as_ref().and_then(|m| m.get("request_source")).and_then(|v| v.as_str()) == Some("sync"))
        .filter_map(|b| {
            b.metadata
                .as_ref()
                .and_then(|m| m.get("dw_source_id"))
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok())
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let connection_info: HashMap<Uuid, (String, Uuid)> = if !source_conn_ids.is_empty() {
        Connections::new(&mut read_conn)
            .get_names_by_ids(&source_conn_ids)
            .await
            .map_err(Error::Database)?
    } else {
        HashMap::new()
    };

    let analytics_map: HashMap<Uuid, BatchAnalytics> = if include_analytics && !batches.is_empty() {
        crate::db::handlers::analytics::get_batches_analytics_bulk(&state.db.read(), &batch_ids)
            .await
            .map_err(|e| Error::Internal {
                operation: format!("fetch bulk batch analytics: {}", e),
            })?
    } else {
        HashMap::new()
    };

    let data: Vec<_> = batches
        .into_iter()
        .map(|batch| {
            let batch_id = batch.id;

            let individual_creator_id = batch.api_key_id.and_then(|key_id| api_key_creator_map.get(&key_id).copied());
            let created_by_email = individual_creator_id
                .and_then(|uid| user_map.get(&uid))
                .map(|u| u.email.clone())
                .or_else(|| {
                    if batch.api_key_id.is_none() {
                        Uuid::parse_str(&batch.created_by)
                            .ok()
                            .and_then(|uid| user_map.get(&uid))
                            .map(|u| u.email.clone())
                    } else {
                        None
                    }
                });

            let owner_id = Uuid::parse_str(&batch.created_by).ok();
            let owner = owner_id.and_then(|id| user_map.get(&id));
            let (context_name, context_type) = match owner {
                Some(u) if u.user_type == "organization" => {
                    let name = u.display_name.clone().unwrap_or_else(|| u.email.clone());
                    (Some(name), Some("organization".to_string()))
                }
                Some(_) => (Some("Personal".to_string()), Some("personal".to_string())),
                None => (None, None),
            };

            let is_sync = batch
                .metadata
                .as_ref()
                .and_then(|m| m.get("request_source"))
                .and_then(|v| v.as_str())
                == Some("sync");
            let source_name: Option<&str> = if is_sync {
                batch
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("dw_source_id"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .and_then(|conn_id| {
                        let batch_owner = Uuid::parse_str(&batch.created_by).ok()?;
                        let (name, owner) = connection_info.get(&conn_id)?;
                        if *owner == batch_owner { Some(name.as_str()) } else { None }
                    })
            } else {
                None
            };

            let mut response = to_batch_response_enriched(batch, None, source_name);

            if let Some(email) = created_by_email {
                response
                    .metadata
                    .get_or_insert_with(HashMap::new)
                    .insert("created_by_email".to_string(), email);
            }
            if let Some(name) = context_name {
                response
                    .metadata
                    .get_or_insert_with(HashMap::new)
                    .insert("context_name".to_string(), name);
            }
            if let Some(ctype) = context_type {
                response
                    .metadata
                    .get_or_insert_with(HashMap::new)
                    .insert("context_type".to_string(), ctype);
            }

            if include_analytics {
                response.analytics = analytics_map.get(&batch_id).cloned();
            }
            response
        })
        .collect();

    Ok(Json(BatchListResponse {
        object_type: ListObjectType::List,
        data,
        first_id,
        last_id,
        has_more,
    }))
}

#[cfg(test)]
mod tests {
    use super::{
        is_reserved_metadata_key, load_and_validate_batch_models, parse_batch_class_filter, parse_completion_window_filter,
        to_batch_response_with_email,
    };
    use crate::api::models::batches::CreateBatchRequest;
    use crate::api::models::users::Role;
    use crate::db::handlers::Credits;
    use crate::db::models::credits::{CreditTransactionCreateDBRequest, CreditTransactionType};
    use crate::errors::Error;
    use crate::test::utils::*;
    use axum::http::StatusCode;
    use fusillade::Storage;
    use rust_decimal::Decimal;
    use sqlx::PgPool;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use sqlx_pool_router::{PoolProvider, TestDbPools};
    use std::collections::HashMap;
    use std::time::Duration;
    use uuid::Uuid;

    fn background_batch() -> fusillade::Batch {
        let now = chrono::Utc::now();
        fusillade::Batch {
            id: fusillade::BatchId(Uuid::new_v4()),
            file_id: Some(fusillade::FileId(Uuid::new_v4())),
            created_at: now,
            metadata: None,
            service_tier: Some("background".to_string()),
            completion_window: None,
            endpoint: "/v1/chat/completions".to_string(),
            output_file_id: None,
            error_file_id: None,
            created_by: Uuid::new_v4().to_string(),
            expires_at: None,
            cancelling_at: None,
            errors: None,
            total_requests: 1,
            pending_requests: 1,
            in_progress_requests: 0,
            completed_requests: 0,
            failed_requests: 0,
            canceled_requests: 0,
            requests_started_at: Some(now),
            finalizing_at: None,
            completed_at: None,
            failed_at: None,
            cancelled_at: None,
            deleted_at: None,
            notification_sent_at: None,
            api_key_id: None,
        }
    }

    #[test]
    fn background_batch_response_has_a_tier_label_and_no_expiry() {
        let response = to_batch_response_with_email(background_batch(), None);

        assert_eq!(response.completion_window, "background");
        assert!(response.expires_at.is_none());
        assert!(response.expired_at.is_none());
    }

    #[test]
    #[should_panic(expected = "non-background batch must have a completion window")]
    fn sla_batch_response_requires_a_completion_window() {
        let mut batch = background_batch();
        batch.service_tier = None;

        let _ = to_batch_response_with_email(batch, None);
    }

    #[test]
    #[should_panic(expected = "non-background batch must have an expiry")]
    fn sla_batch_response_requires_an_expiry() {
        let mut batch = background_batch();
        batch.service_tier = None;
        batch.completion_window = Some("24h".to_string());

        let _ = to_batch_response_with_email(batch, None);
    }


    #[test]
    fn parse_completion_window_filter_returns_none_when_param_missing() {
        assert_eq!(parse_completion_window_filter(None), None);
    }

    #[test]
    fn parse_completion_window_filter_returns_none_when_param_empty() {
        assert_eq!(parse_completion_window_filter(Some("")), None);
    }

    #[test]
    fn parse_completion_window_filter_returns_none_when_param_whitespace_only() {
        assert_eq!(parse_completion_window_filter(Some("   \t  ")), None);
    }

    #[test]
    fn parse_completion_window_filter_parses_single_value() {
        assert_eq!(parse_completion_window_filter(Some("24h")), Some(vec!["24h".to_string()]));
    }

    #[test]
    fn parse_completion_window_filter_parses_comma_separated_values() {
        assert_eq!(
            parse_completion_window_filter(Some("24h,1h")),
            Some(vec!["24h".to_string(), "1h".to_string()])
        );
    }

    #[test]
    fn parse_completion_window_filter_trims_and_drops_empty_entries() {
        assert_eq!(
            parse_completion_window_filter(Some(" 24h , , 1h ,   ")),
            Some(vec!["24h".to_string(), "1h".to_string()])
        );
    }

    #[test]
    fn platform_manager_background_filter_maps_to_internal_service_tier() {
        let filter = parse_batch_class_filter(Some("24h,background"), true).expect("platform manager filter should be valid");

        assert_eq!(filter.completion_windows, Some(vec!["24h".to_string()]));
        assert_eq!(filter.service_tiers, Some(vec!["background".to_string()]));
    }

    #[test]
    fn non_platform_manager_background_filter_is_bad_request() {
        let error = parse_batch_class_filter(Some("background"), false).expect_err("background filter should be restricted");

        assert!(matches!(
            error,
            Error::BadRequest { message }
                if message == "completion_window=background is only available to platform managers"
        ));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn batch_validation_releases_fusillade_before_waiting_for_control(pool: PgPool) {
        let migrated_fusillade_pool = setup_fusillade_pool(&pool).await;
        migrated_fusillade_pool.close().await;

        let base_options: PgConnectOptions = pool.connect_options().as_ref().clone();
        let control_pool = PgPoolOptions::new()
            .max_connections(1)
            .min_connections(0)
            .connect_with(base_options.clone())
            .await
            .unwrap();
        let fusillade_pool = PgPoolOptions::new()
            .max_connections(1)
            .min_connections(0)
            .connect_with(base_options.options([("search_path", "fusillade")]))
            .await
            .unwrap();
        let state = create_test_app_state_with_database_pools(control_pool.clone(), fusillade_pool.clone(), create_test_config()).await;

        let held_control = state.db.write().acquire().await.unwrap();
        let held_fusillade = fusillade_pool.acquire().await.unwrap();
        let validation_state = state.clone();
        let validation = tokio::spawn(async move { load_and_validate_batch_models(&validation_state, Uuid::new_v4()).await });

        tokio::task::yield_now().await;
        drop(held_fusillade);
        let fusillade_is_available = tokio::time::timeout(Duration::from_millis(250), fusillade_pool.acquire()).await;
        let released_before_control_wait = fusillade_is_available.is_ok();

        drop(fusillade_is_available);
        drop(held_control);
        let validation_result = tokio::time::timeout(Duration::from_secs(1), validation)
            .await
            .expect("batch validation should finish after the control connection is released")
            .expect("batch validation task should not panic");
        validation_result.expect("empty batch validation should succeed");

        assert!(
            released_before_control_wait,
            "batch validation held the Fusillade connection while waiting for the control pool"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn revalidates_reasoning_against_current_mapping_before_batch_creation(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "reasoning-model", "reasoning-model").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let configure_effort = |effort: &'static str| {
            let pool = pool.clone();
            async move {
                let all_efforts = ["none", "minimal", "low", "medium", "high", "xhigh", "max"];
                let unsupported = all_efforts.into_iter().filter(|candidate| *candidate != effort).collect::<Vec<_>>();
                let values = serde_json::Map::from_iter([(effort.to_string(), serde_json::json!(effort))]);
                sqlx::query(
                    r#"
                    UPDATE deployed_models
                    SET reasoning_translation_overrides = $1
                    WHERE id = $2
                    "#,
                )
                .bind(serde_json::json!({
                    "chat_completions": {
                        "mode": "override",
                        "translation": {
                            "unsupported_efforts": unsupported,
                            "writes": [{
                                "target_path": "/reasoning_effort",
                                "values": values
                            }]
                        }
                    },
                    "responses": {"mode": "disabled"}
                }))
                .bind(deployment.id)
                .execute(&pool)
                .await
                .unwrap();
            }
        };

        configure_effort("high").await;
        let jsonl = r#"{"custom_id":"reasoning-request","method":"POST","url":"/v1/chat/completions","body":{"model":"reasoning-model","messages":[],"reasoning_effort":"high"}}"#;
        let upload = app
            .post("/ai/v1/files")
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_part(
                        "file",
                        axum_test::multipart::Part::bytes(jsonl.as_bytes()).file_name("reasoning-batch.jsonl"),
                    )
                    .add_part("purpose", axum_test::multipart::Part::text("batch")),
            )
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload.json();

        configure_effort("medium").await;
        let response = app
            .post("/ai/v1/batches")
            .json(&CreateBatchRequest {
                input_file_id: file["id"].as_str().unwrap().to_string(),
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: "24h".to_string(),
                metadata: None,
                api_key_id: None,
            })
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status(StatusCode::BAD_REQUEST);
        let body = response.text();
        assert!(
            body.contains("Line 1 (custom_id 'reasoning-request')"),
            "unexpected response: {body}"
        );
        assert!(body.contains("not supported by this provider"), "unexpected response: {body}");

        let batch_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM fusillade.batches")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(batch_count, 0, "validation must happen before creating a batch record");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_with_default_24h_sla(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_labels_model_when_templates_are_generation_two(pool: PgPool) {
        let config = create_test_config();
        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;

        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;
        let auth = add_auth_headers(&user);

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };
        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
        let batch: serde_json::Value = resp.json();
        assert_eq!(batch["model"], "gpt-4", "cutover batch must carry its model label: {batch}");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_in_org_context(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;

        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let org = create_test_org(&pool, user.id).await;

        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, org.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let auth = add_auth_headers(&user);
        let org_cookie = format!("dw_active_org={}", org.id);

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        resp.assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_with_unsupported_sla(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "1h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::BAD_REQUEST);
        let error_text = resp.text();
        assert!(error_text.contains("Unsupported completion_window"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn batch_creation_stores_the_submitters_user_agent_and_drops_a_supplied_one(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part(
                "file",
                axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl"),
            )
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap().to_string();

        let create_req = CreateBatchRequest {
            input_file_id: file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: Some(HashMap::from([
                ("dw_user_agent".to_string(), "spoofed/9.9.9".to_string()),
                ("dw-user-agent".to_string(), "spoofed-hyphenated/9.9.9".to_string()),
                ("DW_USER_AGENT".to_string(), "spoofed-shouty/9.9.9".to_string()),
                ("team".to_string(), "research".to_string()),
            ])),
            api_key_id: None,
        };
        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header("user-agent", "claude-cli/1.2.3")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
        let batch: crate::api::models::batches::BatchResponse = resp.json();

        let returned = batch.metadata.clone().unwrap_or_default();
        assert!(!returned.contains_key("dw_user_agent"), "dw_user_agent leaked into the response");
        assert_eq!(returned.get("team").map(String::as_str), Some("research"));

        let base_options: PgConnectOptions = pool.connect_options().as_ref().clone();
        let fusillade_pool = PgPoolOptions::new()
            .max_connections(1)
            .min_connections(0)
            .connect_with(base_options.options([("search_path", "fusillade")]))
            .await
            .unwrap();
        let metadata: serde_json::Value = sqlx::query_scalar("SELECT metadata FROM batches WHERE id = $1")
            .bind(Uuid::parse_str(&batch.id).unwrap())
            .fetch_one(&fusillade_pool)
            .await
            .expect("the batch should be stored with its metadata");
        fusillade_pool.close().await;

        assert_eq!(
            metadata["dw_user_agent"], "claude-cli/1.2.3",
            "the wire User-Agent should be stored for fusillade to replay on dispatch"
        );
        assert_eq!(metadata["team"], "research", "caller metadata should survive alongside it");
        for spelling in ["dw-user-agent", "DW_USER_AGENT"] {
            assert!(
                metadata.get(spelling).is_none(),
                "a reserved key must be stripped in every spelling that normalises to it, not just the exact one ({spelling} survived)"
            );
        }
    }

    #[test]
    fn reserved_metadata_keys_are_matched_on_their_normalised_form() {
        for reserved in [
            "dw_user_agent",
            "dw-user-agent",
            "DW_USER_AGENT",
            "Dw-User-Agent",
            "request_source",
            "created_by",
            "dw_model",
            "dw-model",
            "DW_MODEL",
        ] {
            assert!(is_reserved_metadata_key(reserved), "{reserved} should be reserved");
        }
        for allowed in ["team", "dw_user_agent_note", "user_agent", "dwuseragent", "model"] {
            assert!(!is_reserved_metadata_key(allowed), "{allowed} is the caller's to set");
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_with_custom_allowed_sla(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.allowed_completion_windows = vec!["1h".to_string(), "24h".to_string(), "48h".to_string()];

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "1h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);

        let upload_resp2 = app
            .post("/ai/v1/files")
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_part(
                        "file",
                        axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch-2.jsonl"),
                    )
                    .add_part("purpose", axum_test::multipart::Part::text("batch")),
            )
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp2.assert_status(StatusCode::CREATED);
        let file2: serde_json::Value = upload_resp2.json();
        let file_id2 = file2["id"].as_str().unwrap();

        let create_req2 = CreateBatchRequest {
            input_file_id: file_id2.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "48h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp2 = app
            .post("/ai/v1/batches")
            .json(&create_req2)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp2.assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_with_responses_endpoint(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/responses","body":{"model":"gpt-4","input":"Hello"}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/responses".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_sla_to_expiry_timestamp_24h(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let now = chrono::Utc::now();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
        let batch: serde_json::Value = resp.json();

        let expires_at = batch["expires_at"].as_i64().expect("expires_at should be present");

        let expires_at_dt = chrono::DateTime::from_timestamp(expires_at, 0).expect("Invalid timestamp");

        let expected_expiry = now + chrono::Duration::hours(24);

        let tolerance = chrono::Duration::minutes(1);
        let diff = (expires_at_dt - expected_expiry).abs();

        assert!(
            diff < tolerance,
            "Expiry timestamp should be ~24h from now. Expected: {}, Got: {}, Diff: {} seconds",
            expected_expiry,
            expires_at_dt,
            diff.num_seconds()
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_sla_to_expiry_timestamp_custom(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.allowed_completion_windows = vec!["1h".to_string(), "24h".to_string()];

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let now = chrono::Utc::now();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "1h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
        let batch: serde_json::Value = resp.json();

        let expires_at = batch["expires_at"].as_i64().expect("expires_at should be present");

        let expires_at_dt = chrono::DateTime::from_timestamp(expires_at, 0).expect("Invalid timestamp");

        let expected_expiry = now + chrono::Duration::hours(1);

        let tolerance = chrono::Duration::minutes(1);
        let diff = (expires_at_dt - expected_expiry).abs();

        assert!(
            diff < tolerance,
            "Expiry timestamp should be ~1h from now. Expected: {}, Got: {}, Diff: {} seconds",
            expected_expiry,
            expires_at_dt,
            diff.num_seconds()
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_batches_with_include_analytics(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let create_resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        create_resp.assert_status(StatusCode::CREATED);

        let list_resp = app
            .get("/ai/v1/batches")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        list_resp.assert_status_ok();
        let list_result: serde_json::Value = list_resp.json();
        assert!(!list_result["data"].as_array().unwrap().is_empty());
        let first_batch = &list_result["data"][0];
        assert!(first_batch["analytics"].is_null());

        let list_with_analytics_resp = app
            .get("/ai/v1/batches?include=analytics")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        list_with_analytics_resp.assert_status_ok();
        let list_with_analytics: serde_json::Value = list_with_analytics_resp.json();
        assert!(!list_with_analytics["data"].as_array().unwrap().is_empty());
        let first_batch_with_analytics = &list_with_analytics["data"][0];
        assert!(first_batch_with_analytics["analytics"].is_object());
        let analytics = &first_batch_with_analytics["analytics"];
        assert!(analytics["total_requests"].is_number());
        assert!(analytics["total_prompt_tokens"].is_number());
        assert!(analytics["total_completion_tokens"].is_number());
        assert!(analytics["total_tokens"].is_number());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_model_field(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment_a = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment_a.id, group.id, user.id).await;
        let deployment_b = create_test_deployment(&pool, user.id, "gpt-3half-model", "gpt-3.5").await;
        add_deployment_to_group(&pool, deployment_b.id, group.id, user.id).await;

        let upload_file = |jsonl_content: &'static str| {
            let app = &app;
            let user = &user;
            async move {
                let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
                let multipart = axum_test::multipart::MultipartForm::new()
                    .add_part("file", file_part)
                    .add_part("purpose", axum_test::multipart::Part::text("batch"));
                let upload_resp = app
                    .post("/ai/v1/files")
                    .multipart(multipart)
                    .add_header(&add_auth_headers(user)[0].0, &add_auth_headers(user)[0].1)
                    .add_header(&add_auth_headers(user)[1].0, &add_auth_headers(user)[1].1)
                    .await;
                upload_resp.assert_status(StatusCode::CREATED);
                let file: serde_json::Value = upload_resp.json();
                file["id"].as_str().unwrap().to_string()
            }
        };

        let create_batch = |file_id: String| {
            let app = &app;
            let user = &user;
            async move {
                let create_req = CreateBatchRequest {
                    input_file_id: file_id,
                    endpoint: "/v1/chat/completions".to_string(),
                    completion_window: "24h".to_string(),
                    metadata: None,
                    api_key_id: None,
                };
                let create_resp = app
                    .post("/ai/v1/batches")
                    .json(&create_req)
                    .add_header(&add_auth_headers(user)[0].0, &add_auth_headers(user)[0].1)
                    .add_header(&add_auth_headers(user)[1].0, &add_auth_headers(user)[1].1)
                    .await;
                create_resp.assert_status(StatusCode::CREATED);
                let batch: serde_json::Value = create_resp.json();
                batch
            }
        };

        let single_file_id = upload_file(
            r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#,
        )
        .await;
        let single_batch = create_batch(single_file_id).await;
        assert_eq!(single_batch["model"].as_str(), Some("gpt-4"));
        assert!(single_batch["metadata"]["dw_model"].is_null(), "dw_model leaked into metadata");
        let single_batch_id = single_batch["id"].as_str().unwrap().to_string();

        let mixed_file_id = upload_file(
            "{\"custom_id\":\"request-1\",\"method\":\"POST\",\"url\":\"/v1/chat/completions\",\"body\":{\"model\":\"gpt-4\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"}]}}\n{\"custom_id\":\"request-2\",\"method\":\"POST\",\"url\":\"/v1/chat/completions\",\"body\":{\"model\":\"gpt-3.5\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"}]}}",
        )
        .await;
        let mixed_batch = create_batch(mixed_file_id).await;
        assert_eq!(mixed_batch["model"].as_str(), Some("mixed"));
        let mixed_batch_id = mixed_batch["id"].as_str().unwrap().to_string();

        let list_resp = app
            .get("/ai/v1/batches?include=analytics")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        list_resp.assert_status_ok();
        let list_result: serde_json::Value = list_resp.json();
        let rows = list_result["data"].as_array().unwrap();
        let model_of = |id: &str| {
            rows.iter()
                .find(|row| row["id"].as_str() == Some(id))
                .expect("batch should be listed")["model"]
                .as_str()
                .map(str::to_string)
        };
        assert_eq!(model_of(&single_batch_id), Some("gpt-4".to_string()));
        assert_eq!(model_of(&mixed_batch_id), Some("mixed".to_string()));
        for row in rows {
            assert!(row["metadata"]["dw_model"].is_null(), "dw_model leaked into list metadata");
        }

        let get_resp = app
            .get(&format!("/ai/v1/batches/{single_batch_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        get_resp.assert_status_ok();
        let detail: serde_json::Value = get_resp.json();
        assert_eq!(detail["model"].as_str(), Some("gpt-4"));

        let get_mixed_resp = app
            .get(&format!("/ai/v1/batches/{mixed_batch_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        get_mixed_resp.assert_status_ok();
        let mixed_detail: serde_json::Value = get_mixed_resp.json();
        assert_eq!(mixed_detail["model"].as_str(), Some("mixed"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_errors_hidden_until_sla_expires(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "test-model", "test-model").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"test-model","messages":[{"role":"user","content":"Test"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
        let batch: serde_json::Value = resp.json();
        let batch_id = batch["id"].as_str().unwrap();
        let batch_uuid = Uuid::parse_str(batch_id).unwrap();

        sqlx::query(
            r#"
            UPDATE fusillade.batches
            SET errors = '{"object":"list","data":[{"code":"invalid_request","message":"Test error"}]}'::jsonb
            WHERE id = $1
            "#,
        )
        .bind(batch_uuid)
        .execute(&pool)
        .await
        .expect("Failed to set errors");

        let get_resp = app
            .get(&format!("/ai/v1/batches/{}", batch_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        get_resp.assert_status(StatusCode::OK);
        let batch_response: serde_json::Value = get_resp.json();

        assert!(!batch_response["errors"].is_null(), "Batch-level errors are now always shown");
        assert!(
            batch_response["failed_at"].is_null(),
            "failed_at should still be null since we didn't set it yet"
        );
        assert_eq!(
            batch_response["request_counts"]["failed"].as_i64().unwrap(),
            0,
            "Failed request count is 0 (no actual failed requests, only batch-level errors)"
        );

        sqlx::query(
            r#"
            UPDATE fusillade.batches
            SET failed_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(batch_uuid)
        .execute(&pool)
        .await
        .expect("Failed to set failed_at");

        let get_resp2 = app
            .get(&format!("/ai/v1/batches/{}", batch_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        get_resp2.assert_status(StatusCode::OK);
        let batch_response2: serde_json::Value = get_resp2.json();

        assert!(
            !batch_response2["errors"].is_null(),
            "Batch-level errors are now always shown (hiding logic removed)"
        );
        assert!(
            !batch_response2["failed_at"].is_null(),
            "failed_at is now shown (hiding logic removed)"
        );
        assert_eq!(
            batch_response2["request_counts"]["failed"].as_i64().unwrap(),
            0,
            "Failed request count is 0 (no actual failed requests)"
        );

        sqlx::query(
            r#"
            UPDATE fusillade.batches
            SET expires_at = NOW() - INTERVAL '1 hour'
            WHERE id = $1
            "#,
        )
        .bind(batch_uuid)
        .execute(&pool)
        .await
        .expect("Failed to expire batch");

        let get_resp3 = app
            .get(&format!("/ai/v1/batches/{}", batch_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        get_resp3.assert_status(StatusCode::OK);
        let batch_response3: serde_json::Value = get_resp3.json();

        assert!(
            !batch_response3["errors"].is_null(),
            "Errors should be visible when both failed_at is set AND SLA has expired"
        );
        assert_eq!(
            batch_response3["errors"]["data"][0]["message"].as_str().unwrap(),
            "Test error",
            "Error message should match what we set"
        );
        assert!(
            !batch_response3["failed_at"].is_null(),
            "failed_at should now be visible after SLA expires"
        );
    }

    async fn upload_input_file(app: &axum_test::TestServer, pool: &PgPool) -> (crate::api::models::users::UserResponse, String) {
        let user = create_test_user_with_roles(pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(pool).await;
        add_user_to_group(pool, user.id, group.id).await;
        let deployment = create_test_deployment(pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(pool, deployment.id, group.id, user.id).await;

        let jsonl = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let upload = app
            .post("/ai/v1/files")
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_text("purpose", "batch")
                    .add_part("file", axum_test::multipart::Part::bytes(jsonl.as_bytes()).file_name("batch.jsonl")),
            )
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload.json();
        (user, file["id"].as_str().unwrap().to_string())
    }

    fn create_batch_request(input_file_id: &str, completion_window: &str) -> CreateBatchRequest {
        CreateBatchRequest {
            input_file_id: input_file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: completion_window.to_string(),
            metadata: None,
            api_key_id: None,
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn create_batch_rejects_an_expired_input_with_400(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let (user, file_id) = upload_input_file(&app, &pool).await;
        sqlx::query("UPDATE fusillade.files SET expires_at = NOW() - INTERVAL '1 second' WHERE id = $1")
            .bind(Uuid::parse_str(&file_id).unwrap())
            .execute(&pool)
            .await
            .unwrap();

        let response = app
            .post("/ai/v1/batches")
            .json(&create_batch_request(&file_id, "24h"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        response.assert_status(StatusCode::BAD_REQUEST);
        assert!(response.text().contains("expired"), "unexpected response: {}", response.text());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn results_files_show_an_expiry(pool: PgPool) {
        let default_seconds = 2 * 24 * 60 * 60;
        let mut config = create_test_config();
        config.batches.files.default_expiry_seconds = default_seconds;
        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let (user, file_id) = upload_input_file(&app, &pool).await;

        let response = app
            .post("/ai/v1/batches")
            .json(&create_batch_request(&file_id, "24h"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        response.assert_status(StatusCode::CREATED);
        let batch: crate::api::models::batches::BatchResponse = response.json();

        for results_file_id in [batch.output_file_id.unwrap(), batch.error_file_id.unwrap()] {
            let file = app
                .get(&format!("/ai/v1/files/{results_file_id}"))
                .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
                .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
                .await;
            file.assert_status_ok();
            let file: serde_json::Value = file.json();
            let expires_at = file["expires_at"].as_i64().expect("results files carry an expiry");
            assert_eq!(expires_at, batch.created_at + default_seconds);
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn batch_results_are_gone_once_the_results_files_expire(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let (user, file_id) = upload_input_file(&app, &pool).await;
        let auth = add_auth_headers(&user);

        let response = app
            .post("/ai/v1/batches")
            .json(&create_batch_request(&file_id, "24h"))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status(StatusCode::CREATED);
        let batch: crate::api::models::batches::BatchResponse = response.json();
        let output_file_id = Uuid::parse_str(&batch.output_file_id.unwrap()).unwrap();
        let error_file_id = Uuid::parse_str(&batch.error_file_id.unwrap()).unwrap();
        let paths = [
            format!("/ai/v1/batches/{}/results", batch.id),
            format!("/ai/v1/batches/{}/results?limit=10", batch.id),
        ];
        let assert_results = |status: StatusCode| {
            let app = &app;
            let auth = &auth;
            let paths = &paths;
            async move {
                for path in paths {
                    let response = app
                        .get(path)
                        .add_header(&auth[0].0, &auth[0].1)
                        .add_header(&auth[1].0, &auth[1].1)
                        .await;
                    response.assert_status(status);
                    if status == StatusCode::GONE {
                        assert!(response.text().contains("expired"), "{path}: {}", response.text());
                    }
                }
            }
        };
        let restore = |id: Uuid| {
            let pool = &pool;
            async move {
                sqlx::query("UPDATE fusillade.files SET expires_at = NOW() + INTERVAL '1 day', status = 'processed' WHERE id = $1")
                    .bind(id)
                    .execute(pool)
                    .await
                    .unwrap();
            }
        };

        assert_results(StatusCode::OK).await;

        for id in [output_file_id, error_file_id] {
            sqlx::query("UPDATE fusillade.files SET expires_at = NOW() - INTERVAL '1 second' WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
            assert_results(StatusCode::GONE).await;
            restore(id).await;
            assert_results(StatusCode::OK).await;
        }

        let default_seconds = crate::config::FilesConfig::default().default_expiry_seconds;
        sqlx::query("UPDATE fusillade.files SET expires_at = NULL, created_at = NOW() - make_interval(secs => $2) WHERE id = $1")
            .bind(output_file_id)
            .bind((default_seconds + 1) as f64)
            .execute(&pool)
            .await
            .unwrap();
        assert_results(StatusCode::GONE).await;
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn batch_results_drop_input_bodies_once_the_input_is_gone(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let file_id = Uuid::new_v4();
        let batch_id = Uuid::new_v4();
        let num_requests = 3;
        sqlx::query(
            "INSERT INTO fusillade.files (id, name, status, expires_at, created_at, updated_at) VALUES ($1, 'input.jsonl', 'processed', NOW() + INTERVAL '1 day', NOW(), NOW())",
        )
        .bind(file_id)
        .execute(&pool)
        .await
        .expect("Failed to create file");
        sqlx::query(
            "INSERT INTO fusillade.batches (id, created_by, file_id, endpoint, completion_window, expires_at, created_at, total_requests, completed_at) VALUES ($1, $2, $3, '/v1/chat/completions', '24h', NOW() + interval '24 hours', NOW(), $4, NOW())",
        )
        .bind(batch_id)
        .bind(user.id.to_string())
        .bind(file_id)
        .bind(num_requests as i32)
        .execute(&pool)
        .await
        .expect("Failed to create batch");
        attach_results_files(&pool, batch_id).await;
        for i in 0..num_requests {
            let template_id = Uuid::new_v4();
            crate::test::utils::insert_fusillade_template(
                &pool,
                template_id,
                Some(file_id),
                "test-model",
                "http://test",
                "/v1/chat/completions",
                &serde_json::json!({"prompt": i}).to_string(),
                Some(&format!("req-{i}")),
            )
            .await;
            sqlx::query(
                "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, response_status, response_body, custom_id, created_at, completed_at) \
                 VALUES ($1, $2, $3, 'test-model', 'completed', 200, $4, $5, NOW() + ($6 || ' seconds')::interval, NOW())",
            )
            .bind(Uuid::new_v4())
            .bind(batch_id)
            .bind(template_id)
            .bind(format!(r#"{{"id":"chatcmpl-{i}"}}"#))
            .bind(format!("req-{i}"))
            .bind(i.to_string())
            .execute(&pool)
            .await
            .expect("Failed to create request");
        }

        let results = |input_present: bool| {
            let app = &app;
            let auth = &auth;
            async move {
                for path in [
                    format!("/ai/v1/batches/{batch_id}/results"),
                    format!("/ai/v1/batches/{batch_id}/results?limit=10"),
                ] {
                    let response = app
                        .get(&path)
                        .add_header(&auth[0].0, &auth[0].1)
                        .add_header(&auth[1].0, &auth[1].1)
                        .await;
                    response.assert_status(StatusCode::OK);
                    let lines: Vec<serde_json::Value> = response
                        .text()
                        .lines()
                        .map(|l| serde_json::from_str(l).expect("each line is JSON"))
                        .collect();
                    let custom_ids: Vec<&str> = lines.iter().map(|l| l["custom_id"].as_str().unwrap()).collect();
                    assert_eq!(custom_ids, ["req-0", "req-1", "req-2"], "{path}: every request, in order");
                    for (i, line) in lines.iter().enumerate() {
                        assert_eq!(line["response_body"]["id"], format!("chatcmpl-{i}"), "{path}");
                        if input_present {
                            assert_eq!(line["input_body"]["prompt"], i, "{path}");
                        } else {
                            assert!(line["input_body"].is_null(), "{path}: input body served after the input went");
                        }
                    }
                }
            }
        };

        results(true).await;

        sqlx::query("UPDATE fusillade.files SET expires_at = NOW() - INTERVAL '1 second' WHERE id = $1")
            .bind(file_id)
            .execute(&pool)
            .await
            .unwrap();
        results(false).await;
        sqlx::query("UPDATE fusillade.files SET expires_at = NOW() + INTERVAL '1 day' WHERE id = $1")
            .bind(file_id)
            .execute(&pool)
            .await
            .unwrap();
        results(true).await;

        sqlx::query(
            "UPDATE fusillade.request_template_buckets SET state = 'retired' \
             WHERE week_start = date_trunc('week', NOW() AT TIME ZONE 'UTC')::date",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("DELETE FROM fusillade.request_templates_g2 WHERE file_id = $1")
            .bind(file_id)
            .execute(&pool)
            .await
            .unwrap();
        results(false).await;
    }

    async fn attach_results_files(pool: &PgPool, batch_id: Uuid) -> (Uuid, Uuid) {
        let output_file_id = Uuid::new_v4();
        let error_file_id = Uuid::new_v4();
        for (id, name) in [(output_file_id, "output.jsonl"), (error_file_id, "error.jsonl")] {
            sqlx::query(
                "INSERT INTO fusillade.files (id, name, status, created_at, updated_at) VALUES ($1, $2, 'processed', NOW(), NOW())",
            )
            .bind(id)
            .bind(name)
            .execute(pool)
            .await
            .expect("Failed to create results file");
        }
        sqlx::query("UPDATE fusillade.batches SET output_file_id = $2, error_file_id = $3 WHERE id = $1")
            .bind(batch_id)
            .bind(output_file_id)
            .bind(error_file_id)
            .execute(pool)
            .await
            .expect("Failed to attach results files");
        (output_file_id, error_file_id)
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_results_served_after_input_file_deleted(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let batch_id = Uuid::new_v4();
        let num_requests = 3;
        sqlx::query(
            "INSERT INTO fusillade.batches (id, created_by, file_id, endpoint, completion_window, expires_at, created_at, total_requests, completed_at) VALUES ($1, $2, NULL, '/v1/chat/completions', '24h', NOW() + interval '24 hours', NOW(), $3, NOW())",
        )
        .bind(batch_id)
        .bind(user.id.to_string())
        .bind(num_requests as i32)
        .execute(&pool)
        .await
        .expect("Failed to create batch");
        attach_results_files(&pool, batch_id).await;

        for i in 0..num_requests {
            let (state, response_body, error) = if i == 1 {
                ("failed", None, Some("upstream said no"))
            } else {
                ("completed", Some(format!(r#"{{"id":"chatcmpl-{i}"}}"#)), None)
            };
            sqlx::query(
                "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, response_status, response_body, error, custom_id, created_at, completed_at, failed_at) \
                 VALUES ($1, $2, $3, 'test-model', $4, CASE WHEN $4 = 'completed' THEN 200 END, $5, $6, $7, NOW() + ($8 || ' seconds')::interval, \
                         CASE WHEN $4 = 'completed' THEN NOW() END, CASE WHEN $4 = 'failed' THEN NOW() END)",
            )
            .bind(Uuid::new_v4())
            .bind(batch_id)
            .bind(Uuid::new_v4())
            .bind(state)
            .bind(response_body)
            .bind(error)
            .bind(format!("req-{i}"))
            .bind(i.to_string())
            .execute(&pool)
            .await
            .expect("Failed to create request");
        }

        let auth = add_auth_headers(&user);

        for path in [
            format!("/ai/v1/batches/{}/results", batch_id),
            format!("/ai/v1/batches/{}/results?limit=10", batch_id),
        ] {
            let response = app
                .get(&path)
                .add_header(&auth[0].0, &auth[0].1)
                .add_header(&auth[1].0, &auth[1].1)
                .await;

            response.assert_status(StatusCode::OK);
            response.assert_header("X-Last-Line", &num_requests.to_string());
            let lines: Vec<serde_json::Value> = response
                .text()
                .lines()
                .map(|l| serde_json::from_str(l).expect("each line is JSON"))
                .collect();
            assert_eq!(lines.len(), num_requests, "{path}: every request is returned");
            let custom_ids: Vec<&str> = lines.iter().map(|l| l["custom_id"].as_str().unwrap()).collect();
            assert_eq!(custom_ids, ["req-0", "req-1", "req-2"], "{path}: creation order");
            for line in &lines {
                assert!(line["input_body"].is_null(), "{path}: input body is gone with the file");
            }
            assert_eq!(lines[0]["status"], "completed");
            assert_eq!(lines[0]["response_body"]["id"], "chatcmpl-0");
            assert_eq!(lines[1]["status"], "failed");
            assert_eq!(lines[1]["error"], "upstream said no");
        }

        let response = app
            .get(&format!("/ai/v1/batches/{}/results?status=failed", batch_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status(StatusCode::OK);
        assert_eq!(response.text().lines().count(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_results_gone_after_results_file_deleted(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let cases = [
            ("output_file_id", "output file has been deleted"),
            ("error_file_id", "error file has been deleted"),
        ];
        for (column, expected) in cases {
            let batch_id = Uuid::new_v4();
            sqlx::query(
                "INSERT INTO fusillade.batches (id, created_by, file_id, endpoint, completion_window, expires_at, created_at, total_requests, completed_at) VALUES ($1, $2, NULL, '/v1/chat/completions', '24h', NOW() + interval '24 hours', NOW(), 1, NOW())",
            )
            .bind(batch_id)
            .bind(user.id.to_string())
            .execute(&pool)
            .await
            .expect("Failed to create batch");
            attach_results_files(&pool, batch_id).await;
            sqlx::query(
                "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, response_status, response_body, custom_id, created_at, completed_at) VALUES ($1, $2, $3, 'test-model', 'completed', 200, '{}', 'req-0', NOW(), NOW())",
            )
            .bind(Uuid::new_v4())
            .bind(batch_id)
            .bind(Uuid::new_v4())
            .execute(&pool)
            .await
            .expect("Failed to create request");
            sqlx::query(&format!("UPDATE fusillade.batches SET {column} = NULL WHERE id = $1"))
                .bind(batch_id)
                .execute(&pool)
                .await
                .expect("Failed to unlink results file");

            for path in [
                format!("/ai/v1/batches/{}/results", batch_id),
                format!("/ai/v1/batches/{}/results?limit=10", batch_id),
            ] {
                let response = app
                    .get(&path)
                    .add_header(&auth[0].0, &auth[0].1)
                    .add_header(&auth[1].0, &auth[1].1)
                    .await;
                response.assert_status(StatusCode::GONE);
                let body = response.text();
                assert!(body.contains(expected), "{path}: expected {expected:?} in {body:?}");
            }

            let response = app
                .get(&format!("/ai/v1/batches/{}", batch_id))
                .add_header(&auth[0].0, &auth[0].1)
                .add_header(&auth[1].0, &auth[1].1)
                .await;
            response.assert_status(StatusCode::OK);
        }

        let batch_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO fusillade.batches (id, created_by, file_id, endpoint, completion_window, expires_at, created_at, total_requests, completed_at) VALUES ($1, $2, NULL, '/v1/chat/completions', '24h', NOW() + interval '24 hours', NOW(), 0, NOW())",
        )
        .bind(batch_id)
        .bind(user.id.to_string())
        .execute(&pool)
        .await
        .expect("Failed to create batch");
        let response = app
            .get(&format!("/ai/v1/batches/{}/results", batch_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        response.assert_status(StatusCode::GONE);
        assert!(response.text().contains("output and error files have been deleted"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_results_streaming(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "test-model-endpoint", "test-model").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let file_id = Uuid::new_v4();
        let batch_id = Uuid::new_v4();
        let num_requests = 50;

        sqlx::query(
            "INSERT INTO fusillade.files (id, name, status, created_at, updated_at) VALUES ($1, 'test.jsonl', 'processed', NOW(), NOW())",
        )
        .bind(file_id)
        .execute(&pool)
        .await
        .expect("Failed to create file");

        sqlx::query(
            "INSERT INTO fusillade.batches (id, created_by, file_id, endpoint, completion_window, expires_at, created_at, total_requests) VALUES ($1, $2, $3, '/v1/chat/completions', '24h', NOW() + interval '24 hours', NOW(), $4)",
        )
        .bind(batch_id)
        .bind(user.id.to_string())
        .bind(file_id)
        .bind(num_requests as i32)
        .execute(&pool)
        .await
        .expect("Failed to create batch");
        attach_results_files(&pool, batch_id).await;

        for i in 0..num_requests {
            let template_id = Uuid::new_v4();
            let request_id = Uuid::new_v4();
            let custom_id = format!("req-{}", i);
            let body = serde_json::json!({"model": "test-model", "messages": [{"role": "user", "content": format!("Test {}", i)}]});
            let response_body = serde_json::json!({
                "id": format!("chatcmpl-{}", i),
                "choices": [{"message": {"content": format!("Response {}", i)}}]
            });

            crate::test::utils::insert_fusillade_template(
                &pool,
                template_id,
                Some(file_id),
                "test-model",
                "http://test",
                "/v1/chat/completions",
                &serde_json::to_string(&body).unwrap(),
                Some(&custom_id),
            )
            .await;

            sqlx::query(
                "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, response_status, response_body, created_at, completed_at) VALUES ($1, $2, $3, 'test-model', 'completed', 200, $4, NOW(), NOW())",
            )
            .bind(request_id)
            .bind(batch_id)
            .bind(template_id)
            .bind(serde_json::to_string(&response_body).unwrap())
            .execute(&pool)
            .await
            .expect("Failed to create completed request");
        }

        let auth = add_auth_headers(&user);

        let response = app
            .get(&format!("/ai/v1/batches/{}/results", batch_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status(StatusCode::OK);
        response.assert_header("content-type", "application/x-ndjson");
        response.assert_header("X-Incomplete", "false");
        response.assert_header("X-Last-Line", &num_requests.to_string());
        assert!(
            response.headers().get("content-length").is_none(),
            "Unlimited download should be streamed without content-length"
        );

        let body = response.text();
        let lines: Vec<&str> = body.trim().lines().collect();
        assert_eq!(lines.len(), num_requests, "Should return all {} results", num_requests);

        for line in &lines {
            let item: serde_json::Value = serde_json::from_str(line).expect("Each line should be valid JSON");
            assert!(item.get("custom_id").is_some(), "Each result should have custom_id");
            assert!(item.get("status").is_some(), "Each result should have status");
        }

        let page_size = 10;
        let response = app
            .get(&format!("/ai/v1/batches/{}/results?limit={}", batch_id, page_size))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status(StatusCode::OK);
        response.assert_header("X-Incomplete", "true");
        response.assert_header("X-Last-Line", &page_size.to_string());

        let body = response.text();
        let lines: Vec<&str> = body.trim().lines().collect();
        assert_eq!(lines.len(), page_size, "Should return exactly {} results", page_size);

        let response = app
            .get(&format!(
                "/ai/v1/batches/{}/results?limit={}&skip={}",
                batch_id,
                page_size,
                num_requests - page_size
            ))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status(StatusCode::OK);
        response.assert_header("X-Incomplete", "false");
        response.assert_header("X-Last-Line", &num_requests.to_string());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_results_x_incomplete_while_still_processing(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "test-model-endpoint", "test-model").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let file_id = Uuid::new_v4();
        let batch_id = Uuid::new_v4();
        let num_completed = 5;
        let num_pending = 3;
        let total = num_completed + num_pending;

        sqlx::query(
            "INSERT INTO fusillade.files (id, name, status, created_at, updated_at) VALUES ($1, 'test.jsonl', 'processed', NOW(), NOW())",
        )
        .bind(file_id)
        .execute(&pool)
        .await
        .expect("Failed to create file");

        sqlx::query(
            "INSERT INTO fusillade.batches (id, created_by, file_id, endpoint, completion_window, expires_at, created_at, total_requests) VALUES ($1, $2, $3, '/v1/chat/completions', '24h', NOW() + interval '24 hours', NOW(), $4)",
        )
        .bind(batch_id)
        .bind(user.id.to_string())
        .bind(file_id)
        .bind(total as i32)
        .execute(&pool)
        .await
        .expect("Failed to create batch");
        attach_results_files(&pool, batch_id).await;

        for i in 0..num_completed {
            let template_id = Uuid::new_v4();
            let request_id = Uuid::new_v4();
            let custom_id = format!("req-{}", i);
            let body = serde_json::json!({"model": "test-model", "messages": [{"role": "user", "content": format!("Test {}", i)}]});
            let response_body = serde_json::json!({
                "id": format!("chatcmpl-{}", i),
                "choices": [{"message": {"content": format!("Response {}", i)}}]
            });

            crate::test::utils::insert_fusillade_template(
                &pool,
                template_id,
                Some(file_id),
                "test-model",
                "http://test",
                "/v1/chat/completions",
                &serde_json::to_string(&body).unwrap(),
                Some(&custom_id),
            )
            .await;

            sqlx::query(
                "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, response_status, response_body, created_at, completed_at) VALUES ($1, $2, $3, 'test-model', 'completed', 200, $4, NOW(), NOW())",
            )
            .bind(request_id)
            .bind(batch_id)
            .bind(template_id)
            .bind(serde_json::to_string(&response_body).unwrap())
            .execute(&pool)
            .await
            .expect("Failed to create completed request");
        }

        for i in num_completed..total {
            let template_id = Uuid::new_v4();
            let request_id = Uuid::new_v4();
            let custom_id = format!("req-{}", i);
            let body = serde_json::json!({"model": "test-model", "messages": [{"role": "user", "content": format!("Test {}", i)}]});

            crate::test::utils::insert_fusillade_template(
                &pool,
                template_id,
                Some(file_id),
                "test-model",
                "http://test",
                "/v1/chat/completions",
                &serde_json::to_string(&body).unwrap(),
                Some(&custom_id),
            )
            .await;

            sqlx::query(
                "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, created_at) VALUES ($1, $2, $3, 'test-model', 'pending', NOW())",
            )
            .bind(request_id)
            .bind(batch_id)
            .bind(template_id)
            .execute(&pool)
            .await
            .expect("Failed to create pending request");
        }

        let auth = add_auth_headers(&user);

        let response = app
            .get(&format!("/ai/v1/batches/{}/results", batch_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status(StatusCode::OK);
        response.assert_header("X-Incomplete", "true");

        let body = response.text();
        let lines: Vec<&str> = body.trim().lines().collect();
        assert_eq!(lines.len(), total, "Should return all request results");

        let response = app
            .get(&format!("/ai/v1/batches/{}/results?limit={}", batch_id, total))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status(StatusCode::OK);
        response.assert_header("X-Incomplete", "true");

        let body = response.text();
        let lines: Vec<&str> = body.trim().lines().collect();
        assert_eq!(lines.len(), total, "Should return all request results");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_reserve_capacity_for_batch_inserts_and_releases(pool: PgPool) {
        let config = create_test_config();
        let state = create_test_app_state_with_fusillade(pool.clone(), config).await;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let endpoint_id = create_test_endpoint(&pool, &format!("test-{}", Uuid::new_v4()), user.id).await;

        let alias = format!("alias-{}", Uuid::new_v4());
        let model_id = create_test_model(&pool, "model-a", &alias, endpoint_id, user.id).await;

        let file_model_counts: HashMap<String, i64> = HashMap::from([(alias.clone(), 5_i64)]);
        let model_throughputs = HashMap::from([(alias.clone(), 1000.0_f32)]);
        let model_ids_by_alias = HashMap::from([(alias.clone(), model_id)]);

        let reservation_ids =
            super::reserve_capacity_for_batch(&state, "24h", &file_model_counts, &model_throughputs, &model_ids_by_alias, 1.0)
                .await
                .unwrap();

        assert_eq!(reservation_ids.len(), 1);

        let row = sqlx::query!(
            "SELECT reserved_requests, released_at FROM batch_capacity_reservations WHERE id = $1",
            reservation_ids[0]
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.reserved_requests, 5);
        assert!(row.released_at.is_none());

        super::release_capacity_reservations(&state, &reservation_ids).await.unwrap();

        let row = sqlx::query!(
            "SELECT released_at FROM batch_capacity_reservations WHERE id = $1",
            reservation_ids[0]
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(row.released_at.is_some());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_reserve_capacity_for_batch_rejects_over_capacity(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.default_throughput = 0.0;

        let state = create_test_app_state_with_fusillade(pool.clone(), config).await;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let endpoint_id = create_test_endpoint(&pool, &format!("test-{}", Uuid::new_v4()), user.id).await;

        let alias = format!("alias-{}", Uuid::new_v4());
        let model_id = create_test_model(&pool, "model-a", &alias, endpoint_id, user.id).await;

        let file_model_counts: HashMap<String, i64> = HashMap::from([(alias.clone(), 1_i64)]);
        let model_throughputs = HashMap::from([(alias.clone(), 0.0_f32)]);
        let model_ids_by_alias = HashMap::from([(alias.clone(), model_id)]);

        let err = super::reserve_capacity_for_batch(&state, "1h", &file_model_counts, &model_throughputs, &model_ids_by_alias, 1.0)
            .await
            .unwrap_err();

        match err {
            Error::TooManyRequests { .. } => {}
            other => panic!("expected TooManyRequests, got {other:?}"),
        }

        let count = sqlx::query_scalar!(
            "SELECT COUNT(*) as count FROM batch_capacity_reservations WHERE model_id = $1 AND completion_window = $2",
            model_id,
            "1h"
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(count.unwrap_or(0), 0);
    }

    async fn seed_pending_capacity_batch(state: &crate::AppState<TestDbPools>, alias: &str, completion_window: &str) {
        let pending_templates = (0..3)
            .map(|idx| fusillade::RequestTemplateInput {
                custom_id: Some(format!("pending-{idx}")),
                endpoint: "https://api.example.com".to_string(),
                method: "POST".to_string(),
                path: "/v1/chat/completions".to_string(),
                body: format!(r#"{{"model":"{alias}","messages":[{{"role":"user","content":"hello"}}]}}"#),
                model: alias.to_string(),
            })
            .collect();

        let file_id = state
            .request_manager
            .create_file("pending-capacity-test".to_string(), None, pending_templates)
            .await
            .expect("create pending file");
        let pending_batch = state
            .request_manager
            .create_batch(fusillade::BatchInput {
                file_id,
                endpoint: "/v1/chat/completions".to_string(),
                completion_window: completion_window.to_string(),
                metadata: None,
                created_by: None,
                api_key_id: None,
                total_requests: None,
            })
            .await
            .expect("create pending batch");

        sqlx::query("UPDATE batches SET expires_at = NOW() + interval '30 minutes' WHERE id = $1")
            .bind(pending_batch.id.0)
            .execute(state.request_manager.pool())
            .await
            .expect("pin pending batch deadline");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_reserve_capacity_for_batch_ignores_pending_counts_when_disabled(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.pending_capacity_counts_enabled = false;
        let state = create_test_app_state_with_fusillade(pool.clone(), config).await;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let endpoint_id = create_test_endpoint(&pool, &format!("test-{}", Uuid::new_v4()), user.id).await;

        let alias = format!("alias-{}", Uuid::new_v4());
        let model_id = create_test_model(&pool, "model-a", &alias, endpoint_id, user.id).await;

        seed_pending_capacity_batch(&state, &alias, "24h").await;

        let file_model_counts = HashMap::from([(alias.clone(), 1_i64)]);
        let model_throughputs = HashMap::from([(alias.clone(), 0.001_f32)]);
        let model_ids_by_alias = HashMap::from([(alias.clone(), model_id)]);

        let reservation_ids =
            super::reserve_capacity_for_batch(&state, "1h", &file_model_counts, &model_throughputs, &model_ids_by_alias, 1.0)
                .await
                .expect("pending counts should be ignored when disabled");

        assert_eq!(reservation_ids.len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_reserve_capacity_for_batch_rejects_pending_counts_when_enabled(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.pending_capacity_counts_enabled = true;
        let state = create_test_app_state_with_fusillade(pool.clone(), config).await;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let endpoint_id = create_test_endpoint(&pool, &format!("test-{}", Uuid::new_v4()), user.id).await;

        let alias = format!("alias-{}", Uuid::new_v4());
        let model_id = create_test_model(&pool, "model-a", &alias, endpoint_id, user.id).await;

        seed_pending_capacity_batch(&state, &alias, "24h").await;

        let file_model_counts = HashMap::from([(alias.clone(), 1_i64)]);
        let model_throughputs = HashMap::from([(alias.clone(), 0.001_f32)]);
        let model_ids_by_alias = HashMap::from([(alias.clone(), model_id)]);

        let err = super::reserve_capacity_for_batch(&state, "1h", &file_model_counts, &model_throughputs, &model_ids_by_alias, 1.0)
            .await
            .expect_err("pending counts should reject when enabled");

        assert!(matches!(err, Error::TooManyRequests { .. }));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_reserve_capacity_for_batch_fails_open_when_pending_counts_query_fails(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.pending_capacity_counts_enabled = true;
        let state = create_test_app_state_with_fusillade(pool.clone(), config).await;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let endpoint_id = create_test_endpoint(&pool, &format!("test-{}", Uuid::new_v4()), user.id).await;

        let alias = format!("alias-{}", Uuid::new_v4());
        let model_id = create_test_model(&pool, "model-a", &alias, endpoint_id, user.id).await;

        sqlx::query("DROP TABLE fusillade.requests CASCADE").execute(&pool).await.unwrap();

        let file_model_counts = HashMap::from([(alias.clone(), 1_i64)]);
        let model_throughputs = HashMap::from([(alias.clone(), 0.001_f32)]);
        let model_ids_by_alias = HashMap::from([(alias.clone(), model_id)]);

        let reservation_ids =
            super::reserve_capacity_for_batch(&state, "1h", &file_model_counts, &model_throughputs, &model_ids_by_alias, 1.0)
                .await
                .expect("a failing pending-count query must fail open, not reject the submission");

        assert_eq!(reservation_ids.len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_reserve_capacity_for_batch_ignores_flex_pending_counts_when_enabled(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.pending_capacity_counts_enabled = true;
        let state = create_test_app_state_with_fusillade(pool.clone(), config).await;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let endpoint_id = create_test_endpoint(&pool, &format!("test-{}", Uuid::new_v4()), user.id).await;

        let alias = format!("alias-{}", Uuid::new_v4());
        let model_id = create_test_model(&pool, "model-a", &alias, endpoint_id, user.id).await;

        seed_pending_capacity_batch(&state, &alias, "1h").await;

        let file_model_counts = HashMap::from([(alias.clone(), 1_i64)]);
        let model_throughputs = HashMap::from([(alias.clone(), 0.001_f32)]);
        let model_ids_by_alias = HashMap::from([(alias.clone(), model_id)]);

        let reservation_ids =
            super::reserve_capacity_for_batch(&state, "1h", &file_model_counts, &model_throughputs, &model_ids_by_alias, 1.0)
                .await
                .expect("flex pending counts should be ignored for batch capacity");

        assert_eq!(reservation_ids.len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_with_high_priority(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.allowed_completion_windows = vec!["1h".to_string(), "24h".to_string()];

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "1h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
        let batch: serde_json::Value = resp.json();

        assert_eq!(batch["completion_window"].as_str().unwrap(), "1h");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_with_standard_priority(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
        let batch: serde_json::Value = resp.json();

        assert_eq!(batch["completion_window"].as_str().unwrap(), "24h");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_with_legacy_1h_format(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.allowed_completion_windows = vec!["1h".to_string(), "24h".to_string()];

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "1h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
        let batch: serde_json::Value = resp.json();

        assert_eq!(batch["completion_window"].as_str().unwrap(), "1h");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_rejects_invalid_completion_window(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "invalid".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        resp.assert_status(StatusCode::BAD_REQUEST);
        let error_text = resp.text();
        assert!(error_text.contains("Unsupported completion_window"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_can_use_any_completion_window(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::PlatformManager]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "0s".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        assert_ne!(
            resp.status_code(),
            StatusCode::BAD_REQUEST,
            "PlatformManager should be able to use arbitrary completion windows, got: {}",
            resp.text()
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_blocked_by_zero_relaxation_factor(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.allowed_completion_windows = vec!["24h".to_string()];
        config.batches.window_relaxation_factors = std::collections::HashMap::from([("24h".to_string(), 0.0)]);

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        resp.assert_status(StatusCode::TOO_MANY_REQUESTS);
        let error_text = resp.text();
        assert!(error_text.contains("completion window"), "Error should mention completion window");
        assert!(error_text.contains("gpt-4"), "Error should name the overloaded model");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_reserve_capacity_relaxation_factor_expands_acceptance(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.default_throughput = 0.001;
        config.batches.allowed_completion_windows = vec!["1h".to_string(), "24h".to_string()];
        config.batches.window_relaxation_factors = std::collections::HashMap::from([("1h".to_string(), 2.0)]);

        let state = create_test_app_state_with_fusillade(pool.clone(), config).await;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let endpoint_id = create_test_endpoint(&pool, &format!("test-{}", Uuid::new_v4()), user.id).await;
        let alias = format!("alias-{}", Uuid::new_v4());
        let model_id = create_test_model(&pool, "model-a", &alias, endpoint_id, user.id).await;

        let file_model_counts = HashMap::from([(alias.clone(), 5_i64)]);
        let model_throughputs = HashMap::from([(alias.clone(), 0.001_f32)]);
        let model_ids_by_alias = HashMap::from([(alias.clone(), model_id)]);

        let strict_err = super::reserve_capacity_for_batch(&state, "1h", &file_model_counts, &model_throughputs, &model_ids_by_alias, 1.0)
            .await
            .unwrap_err();
        assert!(
            matches!(strict_err, Error::TooManyRequests { .. }),
            "Should be rejected at strict capacity"
        );

        let reservation_ids =
            super::reserve_capacity_for_batch(&state, "1h", &file_model_counts, &model_throughputs, &model_ids_by_alias, 2.0)
                .await
                .expect("Should be accepted with 2× relaxation factor");
        assert_eq!(reservation_ids.len(), 1);

        super::release_capacity_reservations(&state, &reservation_ids).await.unwrap();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_reserve_capacity_relaxation_factor_is_window_specific(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.default_throughput = 0.001;
        config.batches.allowed_completion_windows = vec!["1h".to_string(), "24h".to_string()];
        config.batches.window_relaxation_factors = std::collections::HashMap::from([("24h".to_string(), 10.0)]);

        let state = create_test_app_state_with_fusillade(pool.clone(), config).await;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let endpoint_id = create_test_endpoint(&pool, &format!("test-{}", Uuid::new_v4()), user.id).await;
        let alias = format!("alias-{}", Uuid::new_v4());
        let model_id = create_test_model(&pool, "model-a", &alias, endpoint_id, user.id).await;

        let file_model_counts = HashMap::from([(alias.clone(), 5_i64)]);
        let model_throughputs = HashMap::from([(alias.clone(), 0.001_f32)]);
        let model_ids_by_alias = HashMap::from([(alias.clone(), model_id)]);

        let err = super::reserve_capacity_for_batch(&state, "1h", &file_model_counts, &model_throughputs, &model_ids_by_alias, 1.0)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::TooManyRequests { .. }), "1h should be rejected — not relaxed");

        let reservation_ids =
            super::reserve_capacity_for_batch(&state, "24h", &file_model_counts, &model_throughputs, &model_ids_by_alias, 10.0)
                .await
                .expect("24h should be accepted with 10× relaxation");
        assert_eq!(reservation_ids.len(), 1);

        super::release_capacity_reservations(&state, &reservation_ids).await.unwrap();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_relaxation_factor_from_config(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.default_throughput = 0.0001;
        config.batches.allowed_completion_windows = vec!["24h".to_string()];
        config.batches.window_relaxation_factors = std::collections::HashMap::from([("24h".to_string(), 2.0)]);

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        resp.assert_status(StatusCode::CREATED);
    }

    async fn setup_batch_user(pool: &PgPool) -> crate::api::models::users::UserResponse {
        let user = create_test_user_with_roles(pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(pool).await;
        add_user_to_group(pool, user.id, group.id).await;
        let deployment = create_test_deployment(pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(pool, deployment.id, group.id, user.id).await;
        user
    }

    async fn submit_one_request_batch(
        app: &axum_test::TestServer,
        user: &crate::api::models::users::UserResponse,
        completion_window: &str,
    ) -> axum_test::TestResponse {
        let jsonl = r#"{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl.as_bytes()).file_name("test.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let auth = add_auth_headers(user);
        let upload = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        upload.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload.json();
        let file_id = file["id"].as_str().unwrap().to_string();

        let create_req = CreateBatchRequest {
            input_file_id: file_id,
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: completion_window.to_string(),
            metadata: None,
            api_key_id: None,
        };
        app.post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn background_batch_requires_background_inference_role(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = setup_batch_user(&pool).await;

        let response = submit_one_request_batch(&app, &user, "background").await;

        response.assert_status(StatusCode::FORBIDDEN);
        let body = response.text();
        assert!(body.contains("BackgroundInferenceUser"), "unexpected response: {body}");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn background_batch_maps_completion_window_and_skips_sla_admission(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.allowed_completion_windows = vec!["24h".to_string()];
        config.batches.unverified_requests_per_completion_hour = 1;
        config.batches.default_throughput = 0.0;
        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;

        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser, Role::BackgroundInferenceUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;
        sqlx::query("UPDATE deployed_models SET throughput = 0, batch_capacity = 0 WHERE id = $1")
            .bind(deployment.id)
            .execute(&pool)
            .await
            .expect("remove SLA capacity");

        let first = submit_one_request_batch(&app, &user, "background").await;
        first.assert_status(StatusCode::CREATED);
        let body: serde_json::Value = first.json();
        assert_eq!(body["completion_window"], "background");

        let batch_id = Uuid::parse_str(body["id"].as_str().expect("batch id")).expect("UUID batch id");
        let stored: (Option<String>, Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as("SELECT service_tier, completion_window, expires_at FROM fusillade.batches WHERE id = $1")
                .bind(batch_id)
                .fetch_one(&pool)
                .await
                .expect("stored background batch");
        assert_eq!(stored.0.as_deref(), Some("background"));
        assert_eq!(stored.1, None);
        assert_eq!(stored.2, None);

        submit_one_request_batch(&app, &user, "background")
            .await
            .assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_unverified_volume_limit_rejects_over_cap(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.unverified_requests_per_completion_hour = 1;
        config.batches.allowed_completion_windows = vec!["1h".to_string(), "24h".to_string()];
        config.batches.default_throughput = 100.0;

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = setup_batch_user(&pool).await;

        submit_one_request_batch(&app, &user, "1h").await.assert_status(StatusCode::CREATED);

        let resp = submit_one_request_batch(&app, &user, "1h").await;
        resp.assert_status(StatusCode::TOO_MANY_REQUESTS);
        let body = resp.text();
        assert!(
            body.contains("Unverified") && body.to_lowercase().contains("verify"),
            "expected an actionable verify message, got: {body}"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_rejected_when_spend_cap_exhausted(pool: PgPool) {
        use crate::db::handlers::Repository as _;
        use crate::db::handlers::api_keys::ApiKeys;
        use crate::db::models::api_keys::{ApiKeyCreateDBRequest, ApiKeyPurpose};

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = setup_batch_user(&pool).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = ApiKeys::new(&mut conn);
        let key = repo
            .create(&ApiKeyCreateDBRequest {
                user_id: user.id,
                name: "capped-batch-key".to_string(),
                description: None,
                purpose: ApiKeyPurpose::Realtime,
                created_by: user.id,
                spend_limit: Some(rust_decimal::Decimal::from(10)),
                spend_limit_interval: None,
            })
            .await
            .unwrap();
        repo.get_or_create_child_hidden_key(key.id).await.unwrap();
        repo.reset_spend_window(key.id).await.unwrap();
        sqlx::query("UPDATE api_key_spend_checkpoints SET window_spend = 10, total_spend = 10 WHERE api_key_id = $1")
            .bind(key.id)
            .execute(&pool)
            .await
            .unwrap();
        drop(conn);

        let jsonl = r#"{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl.as_bytes()).file_name("test.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let auth = add_auth_headers(&user);
        let upload = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        upload.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload.json();
        let file_id = file["id"].as_str().unwrap().to_string();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.clone(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };
        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header("authorization", &format!("Bearer {}", key.secret))
            .await;
        resp.assert_status(StatusCode::PAYMENT_REQUIRED);
        let body = resp.text();
        assert!(
            body.to_lowercase().contains("spending cap"),
            "expected an explicit cap message, got: {body}"
        );

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        resp.assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_verified_user_bypasses_volume_limit(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.unverified_requests_per_completion_hour = 1;
        config.batches.allowed_completion_windows = vec!["1h".to_string(), "24h".to_string()];
        config.batches.default_throughput = 100.0;

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = setup_batch_user(&pool).await;
        sqlx::query!("UPDATE users SET verified = true WHERE id = $1", user.id)
            .execute(&pool)
            .await
            .unwrap();

        submit_one_request_batch(&app, &user, "1h").await.assert_status(StatusCode::CREATED);
        submit_one_request_batch(&app, &user, "1h").await.assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_unverified_limit_disabled_when_zero(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.unverified_requests_per_completion_hour = 0;
        config.batches.allowed_completion_windows = vec!["1h".to_string(), "24h".to_string()];
        config.batches.default_throughput = 100.0;

        let (app, _bg_services) = create_test_app_with_config(pool.clone(), config, false).await;
        let user = setup_batch_user(&pool).await;

        submit_one_request_batch(&app, &user, "1h").await.assert_status(StatusCode::CREATED);
        submit_one_request_batch(&app, &user, "1h").await.assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_rejected_with_negative_balance(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut conn);
        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::AdminGrant,
                amount: Decimal::new(100, 2),
                source_id: Uuid::new_v4().to_string(),
                description: Some("Initial credits".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();
        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::new(200, 2),
                source_id: Uuid::new_v4().to_string(),
                description: Some("Usage".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();
        drop(conn);

        let create_req = CreateBatchRequest {
            input_file_id: Uuid::new_v4().to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let auth = add_auth_headers(&user);
        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        resp.assert_status(StatusCode::PAYMENT_REQUIRED);
        let body = resp.text();
        assert!(body.contains("balance too low"), "Expected balance too low message, got: {}", body);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_allowed_with_zero_balance(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;


        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        resp.assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_allowed_with_negative_balance_flag(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', true) ON CONFLICT (user_id, feature_flag) DO UPDATE SET enabled = EXCLUDED.enabled")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE user_balance_checkpoints SET balance = -100 WHERE user_id = $1")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();

        submit_one_request_batch(&app, &user, "24h")
            .await
            .assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_negative_balance_org_flag_does_not_cover_personal_file(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let org = create_test_org(&pool, user.id).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;
        add_user_to_group(&pool, org.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;
        let auth = add_auth_headers(&user);
        let org_cookie = format!("dw_active_org={}", org.id);

        let jsonl = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part(
                "file",
                axum_test::multipart::Part::bytes(jsonl.as_bytes()).file_name("personal.jsonl"),
            )
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        upload.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload.json();
        let request = CreateBatchRequest {
            input_file_id: file["id"].as_str().unwrap().to_owned(),
            endpoint: "/v1/chat/completions".to_owned(),
            completion_window: "24h".to_owned(),
            metadata: None,
            api_key_id: None,
        };
        sqlx::query("UPDATE user_balance_checkpoints SET balance = -100 WHERE user_id = $1 OR user_id = $2")
            .bind(user.id)
            .bind(org.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', true)")
            .bind(org.id)
            .execute(&pool)
            .await
            .unwrap();

        for enabled in [false, true, false] {
            sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', $2) ON CONFLICT (user_id, feature_flag) DO UPDATE SET enabled = EXCLUDED.enabled")
                .bind(user.id).bind(enabled).execute(&pool).await.unwrap();
            let response = app
                .post("/ai/v1/batches")
                .json(&request)
                .add_header(&auth[0].0, &auth[0].1)
                .add_header(&auth[1].0, &auth[1].1)
                .add_header("cookie", &org_cookie)
                .await;
            response.assert_status(if enabled {
                StatusCode::CREATED
            } else {
                StatusCode::PAYMENT_REQUIRED
            });
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_in_org_context_checks_org_balance_not_user(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;

        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let org = create_test_org(&pool, user.id).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut credits_repo = Credits::new(&mut conn);

        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::new(500, 2),
                source_id: Uuid::new_v4().to_string(),
                description: Some("Usage".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();

        credits_repo
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: org.id,
                transaction_type: CreditTransactionType::AdminGrant,
                amount: Decimal::new(1000, 2),
                source_id: Uuid::new_v4().to_string(),
                description: Some("Org credits".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();
        drop(conn);

        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, org.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let auth = add_auth_headers(&user);
        let org_cookie = format!("dw_active_org={}", org.id);

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;

        resp.assert_status(StatusCode::CREATED);
        sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', true) ON CONFLICT (user_id, feature_flag) DO UPDATE SET enabled = EXCLUDED.enabled")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE user_balance_checkpoints SET balance = -100 WHERE user_id = $1")
            .bind(org.id)
            .execute(&pool)
            .await
            .unwrap();
        for enabled in [false, true, false] {
            sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', $2) ON CONFLICT (user_id, feature_flag) DO UPDATE SET enabled = EXCLUDED.enabled")
                .bind(org.id)
                .bind(enabled)
                .execute(&pool)
                .await
                .unwrap();
            let resp = app
                .post("/ai/v1/batches")
                .json(&create_req)
                .add_header(&auth[0].0, &auth[0].1)
                .add_header(&auth[1].0, &auth[1].1)
                .add_header("cookie", &org_cookie)
                .await;
            resp.assert_status(if enabled {
                StatusCode::CREATED
            } else {
                StatusCode::PAYMENT_REQUIRED
            });
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_batches_member_id_rejected_outside_org_context(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let resp = app
            .get(&format!("/ai/v1/batches?member_id={}", Uuid::new_v4()))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        resp.assert_status(StatusCode::BAD_REQUEST);
        let body = resp.text();
        assert!(
            body.contains("organization context"),
            "Expected error about org context, got: {}",
            body
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_batches_member_id_no_key_returns_empty(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let org = create_test_org(&pool, user.id).await;
        let auth = add_auth_headers(&user);
        let org_cookie = format!("dw_active_org={}", org.id);

        let unknown_member = Uuid::new_v4();
        let resp = app
            .get(&format!("/ai/v1/batches?member_id={}", unknown_member))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        resp.assert_status_ok();
        let body: serde_json::Value = resp.json();
        assert_eq!(body["data"].as_array().unwrap().len(), 0);
        assert_eq!(body["has_more"], false);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_batches_enrichment_in_org_context(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;

        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let org = create_test_org(&pool, user.id).await;

        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, org.id, group.id).await;
        add_user_to_group(&pool, user.id, group.id).await;
        let deployment = create_test_deployment(&pool, user.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let auth = add_auth_headers(&user);
        let org_cookie = format!("dw_active_org={}", org.id);

        let jsonl_content = r#"{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("enrich-test.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };
        let create_resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        create_resp.assert_status(StatusCode::CREATED);

        let list_resp = app
            .get("/ai/v1/batches")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        list_resp.assert_status_ok();
        let list_body: serde_json::Value = list_resp.json();
        let batch = &list_body["data"][0];
        let metadata = &batch["metadata"];
        assert!(
            metadata["created_by_email"].is_string(),
            "Expected enriched created_by_email in org context"
        );
        assert!(
            metadata["context_name"].is_string(),
            "Expected enriched context_name in org context"
        );

        let personal_file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("personal-test.jsonl");
        let personal_multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", personal_file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let personal_upload_resp = app
            .post("/ai/v1/files")
            .multipart(personal_multipart)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        personal_upload_resp.assert_status(StatusCode::CREATED);
        let personal_file: serde_json::Value = personal_upload_resp.json();
        let personal_file_id = personal_file["id"].as_str().unwrap();

        let personal_batch_req = CreateBatchRequest {
            input_file_id: personal_file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };
        let personal_create_resp = app
            .post("/ai/v1/batches")
            .json(&personal_batch_req)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        personal_create_resp.assert_status(StatusCode::CREATED);

        let list_resp_personal = app
            .get("/ai/v1/batches")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        list_resp_personal.assert_status_ok();
        let list_body_personal: serde_json::Value = list_resp_personal.json();
        let personal_batches = list_body_personal["data"].as_array().unwrap();
        assert!(!personal_batches.is_empty(), "Expected at least one personal batch");
        for batch in personal_batches {
            let meta = batch["metadata"].as_object().expect("metadata should exist");
            assert!(
                meta.contains_key("created_by_email"),
                "created_by_email should be present even in personal context"
            );
            assert!(
                meta.contains_key("context_name"),
                "context_name should be present even in personal context"
            );
            assert_eq!(
                meta.get("context_type").and_then(|v| v.as_str()),
                Some("personal"),
                "personal batch should have context_type=personal"
            );
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_results_with_reasoning_tokens(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "thinking-model-endpoint", "thinking-model").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let file_id = Uuid::new_v4();
        let batch_id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO fusillade.files (id, name, status, created_at, updated_at) VALUES ($1, 'thinking-test.jsonl', 'processed', NOW(), NOW())",
        )
        .bind(file_id)
        .execute(&pool)
        .await
        .expect("Failed to create file");

        sqlx::query(
            "INSERT INTO fusillade.batches (id, created_by, file_id, endpoint, completion_window, expires_at, created_at, total_requests) VALUES ($1, $2, $3, '/v1/chat/completions', '24h', NOW() + interval '24 hours', NOW(), 3)",
        )
        .bind(batch_id)
        .bind(user.id.to_string())
        .bind(file_id)
        .execute(&pool)
        .await
        .expect("Failed to create batch");
        attach_results_files(&pool, batch_id).await;

        let thinking_responses = vec![
            (
                "req-think-1",
                serde_json::json!({
                    "id": "chatcmpl-think-1",
                    "object": "chat.completion",
                    "model": "thinking-model",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "The capital of France is Paris."}, "finish_reason": "stop"}],
                    "usage": {
                        "prompt_tokens": 22,
                        "completion_tokens": 891,
                        "total_tokens": 913,
                        "completion_tokens_details": {
                            "reasoning_tokens": 733,
                            "audio_tokens": 0,
                            "image_tokens": 0
                        },
                        "prompt_tokens_details": {
                            "audio_tokens": 0,
                            "cached_tokens": 0,
                            "cache_write_tokens": 0,
                            "video_tokens": 0
                        }
                    }
                }),
            ),
            (
                "req-think-2",
                serde_json::json!({
                    "id": "chatcmpl-think-2",
                    "object": "chat.completion",
                    "model": "thinking-model",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "Quantum entanglement is..."}, "finish_reason": "stop"}],
                    "usage": {
                        "prompt_tokens": 20,
                        "completion_tokens": 2101,
                        "total_tokens": 2121,
                        "completion_tokens_details": {
                            "reasoning_tokens": 1412,
                            "audio_tokens": 0,
                            "image_tokens": 0
                        },
                        "prompt_tokens_details": {
                            "audio_tokens": 0,
                            "cached_tokens": 0,
                            "cache_write_tokens": 0,
                            "video_tokens": 0
                        }
                    }
                }),
            ),
            (
                "req-think-3",
                serde_json::json!({
                    "id": "chatcmpl-think-3",
                    "object": "chat.completion",
                    "model": "thinking-model",
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "The ocean is vast..."}, "finish_reason": "stop"}],
                    "usage": {
                        "prompt_tokens": 18,
                        "completion_tokens": 1813,
                        "total_tokens": 1831,
                        "completion_tokens_details": {
                            "reasoning_tokens": 1735,
                            "audio_tokens": 0,
                            "image_tokens": 0
                        },
                        "prompt_tokens_details": {
                            "audio_tokens": 0,
                            "cached_tokens": 0,
                            "cache_write_tokens": 0,
                            "video_tokens": 0
                        }
                    }
                }),
            ),
        ];

        for (custom_id, response_body) in &thinking_responses {
            let template_id = Uuid::new_v4();
            let request_id = Uuid::new_v4();
            let body = serde_json::json!({"model": "thinking-model", "messages": [{"role": "user", "content": "Test"}], "thinking": {"type": "enabled", "budget_tokens": 4096}});

            crate::test::utils::insert_fusillade_template(
                &pool,
                template_id,
                Some(file_id),
                "thinking-model",
                "http://test",
                "/v1/chat/completions",
                &serde_json::to_string(&body).unwrap(),
                Some(custom_id),
            )
            .await;

            sqlx::query(
                "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, response_status, response_body, created_at, completed_at) VALUES ($1, $2, $3, 'thinking-model', 'completed', 200, $4, NOW(), NOW())",
            )
            .bind(request_id)
            .bind(batch_id)
            .bind(template_id)
            .bind(serde_json::to_string(response_body).unwrap())
            .execute(&pool)
            .await
            .expect("Failed to create completed request");
        }

        let auth = add_auth_headers(&user);

        let response = app
            .get(&format!("/ai/v1/batches/{}/results", batch_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        response.assert_status(StatusCode::OK);
        response.assert_header("content-type", "application/x-ndjson");
        response.assert_header("X-Incomplete", "false");

        let body = response.text();
        let lines: Vec<&str> = body.trim().lines().collect();
        assert_eq!(lines.len(), 3, "Should return all 3 thinking results");

        let mut total_prompt = 0i64;
        let mut total_completion = 0i64;
        let mut total_reasoning = 0i64;
        let mut total_tokens = 0i64;

        for line in &lines {
            let item: serde_json::Value = serde_json::from_str(line).expect("Each line should be valid JSON");
            assert_eq!(item["status"].as_str().unwrap(), "completed");

            let response_body = &item["response_body"];
            let usage = &response_body["usage"];

            assert!(usage["prompt_tokens"].is_number(), "prompt_tokens should be present");
            assert!(usage["completion_tokens"].is_number(), "completion_tokens should be present");
            assert!(usage["total_tokens"].is_number(), "total_tokens should be present");

            let details = &usage["completion_tokens_details"];
            assert!(
                details["reasoning_tokens"].is_number(),
                "reasoning_tokens should be present in completion_tokens_details"
            );

            let prompt = usage["prompt_tokens"].as_i64().unwrap();
            let completion = usage["completion_tokens"].as_i64().unwrap();
            let reasoning = details["reasoning_tokens"].as_i64().unwrap();
            let total = usage["total_tokens"].as_i64().unwrap();

            assert!(
                reasoning <= completion,
                "reasoning_tokens ({}) should be <= completion_tokens ({})",
                reasoning,
                completion
            );
            assert_eq!(total, prompt + completion, "total_tokens should equal prompt + completion");

            total_prompt += prompt;
            total_completion += completion;
            total_reasoning += reasoning;
            total_tokens += total;
        }

        assert_eq!(total_prompt, 60, "Total prompt tokens should be 60");
        assert_eq!(total_completion, 4805, "Total completion tokens should be 4805");
        assert_eq!(total_reasoning, 3880, "Total reasoning tokens should be 3880");
        assert_eq!(total_tokens, 4865, "Total tokens should be 4865");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_analytics_with_reasoning_tokens(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let group = create_test_group(&pool).await;
        add_user_to_group(&pool, user.id, group.id).await;

        let deployment = create_test_deployment(&pool, user.id, "thinking-analytics-endpoint", "thinking-model").await;
        add_deployment_to_group(&pool, deployment.id, group.id, user.id).await;

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"thinking-model","messages":[{"role":"user","content":"Hello"}],"reasoning_effort":"high"}}"#;
        let file_part = axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("thinking-test.jsonl");
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part("file", file_part)
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap();

        let create_req = CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id: None,
        };

        let create_resp = app
            .post("/ai/v1/batches")
            .json(&create_req)
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;
        create_resp.assert_status(StatusCode::CREATED);
        let batch: serde_json::Value = create_resp.json();
        let batch_id = batch["id"].as_str().unwrap();
        let batch_uuid = Uuid::parse_str(batch_id).unwrap();

        sqlx::query(
            r#"
            INSERT INTO batch_aggregates (
                fusillade_batch_id, user_id, total_amount, transaction_count, max_seq,
                created_at, updated_at, service_tier, total_requests,
                total_prompt_tokens, total_completion_tokens, total_reasoning_tokens, total_tokens,
                sum_duration_ms, count_duration_ms, sum_ttfb_ms, count_ttfb_ms, total_list_cost
            ) VALUES ($1, $2, 0, 3, 0, NOW(), NOW(), 'batch', 3,
                60, 4805, 3880, 4865, 300, 3, 0, 0, 0)
            "#,
        )
        .bind(batch_uuid)
        .bind(user.id)
        .execute(&pool)
        .await
        .expect("Failed to seed batch_aggregates read model");

        let auth = add_auth_headers(&user);

        let analytics_resp = app
            .get(&format!("/ai/v1/batches/{}/analytics", batch_id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        analytics_resp.assert_status(StatusCode::OK);
        let analytics: serde_json::Value = analytics_resp.json();

        assert_eq!(
            analytics["total_prompt_tokens"].as_i64().unwrap(),
            60,
            "Aggregated prompt tokens should be 60"
        );
        assert_eq!(
            analytics["total_completion_tokens"].as_i64().unwrap(),
            4805,
            "Aggregated completion tokens should be 4805"
        );
        assert_eq!(
            analytics["total_reasoning_tokens"].as_i64().unwrap(),
            3880,
            "Aggregated reasoning tokens should be 3880"
        );
        assert_eq!(
            analytics["total_tokens"].as_i64().unwrap(),
            4865,
            "Aggregated total tokens should be 4865"
        );
        assert_eq!(analytics["total_requests"].as_i64().unwrap(), 3, "Should have 3 analytics records");

        let list_resp = app
            .get("/ai/v1/batches?include=analytics")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        list_resp.assert_status_ok();
        let list_body: serde_json::Value = list_resp.json();
        let batches = list_body["data"].as_array().unwrap();
        let our_batch = batches
            .iter()
            .find(|b| b["id"].as_str().unwrap() == batch_id)
            .expect("Our batch should be in the list");

        let list_analytics = &our_batch["analytics"];
        assert!(list_analytics.is_object(), "analytics should be present with include=analytics");
        assert_eq!(
            list_analytics["total_reasoning_tokens"].as_i64().unwrap(),
            3880,
            "Reasoning tokens should match in list analytics"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_analytics_missing_read_model_row_returns_zeros(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let (batch_id, _request_ids) = insert_batch_with_pending_requests(&pool, user.id, 3).await;

        let resp = app
            .get(&format!("/ai/v1/batches/{batch_id}/analytics"))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;

        resp.assert_status(StatusCode::OK);
        let body: serde_json::Value = resp.json();
        assert_eq!(
            body["total_requests"],
            serde_json::json!(0),
            "unfolded batch reports zero requests, not 404"
        );
    }

    async fn insert_batch_with_pending_requests(pool: &PgPool, user_id: Uuid, n: usize) -> (Uuid, Vec<Uuid>) {
        let file_id = Uuid::new_v4();
        let batch_id = Uuid::new_v4();

        sqlx::query(
            "INSERT INTO fusillade.files (id, name, status, created_at, updated_at, uploaded_by) VALUES ($1, 'test.jsonl', 'processed', NOW(), NOW(), $2)",
        )
        .bind(file_id)
        .bind(user_id.to_string())
        .execute(pool)
        .await
        .expect("insert file");

        sqlx::query(
            "INSERT INTO fusillade.batches (id, created_by, file_id, endpoint, completion_window, expires_at, created_at, total_requests) VALUES ($1, $2, $3, '/v1/chat/completions', '24h', NOW() + interval '24 hours', NOW(), $4)",
        )
        .bind(batch_id)
        .bind(user_id.to_string())
        .bind(file_id)
        .bind(n as i32)
        .execute(pool)
        .await
        .expect("insert batch");

        let mut request_ids = Vec::new();
        for i in 0..n {
            let template_id = Uuid::new_v4();
            let request_id = Uuid::new_v4();
            let body = serde_json::json!({"model": "test-model", "messages": [{"role": "user", "content": format!("Test {}", i)}]});

            crate::test::utils::insert_fusillade_template(
                pool,
                template_id,
                Some(file_id),
                "test-model",
                "http://test",
                "/v1/chat/completions",
                &serde_json::to_string(&body).unwrap(),
                Some(&format!("req-{i}")),
            )
            .await;

            sqlx::query(
                "INSERT INTO fusillade.requests (id, batch_id, template_id, model, state, created_at) VALUES ($1, $2, $3, 'test-model', 'pending', NOW())",
            )
            .bind(request_id)
            .bind(batch_id)
            .bind(template_id)
            .execute(pool)
            .await
            .expect("insert request");

            request_ids.push(request_id);
        }

        (batch_id, request_ids)
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_cancel_batch_cascades_state_to_child_requests(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let (batch_id, request_ids) = insert_batch_with_pending_requests(&pool, user.id, 5).await;

        let pending_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM fusillade.requests WHERE id = ANY($1) AND state = 'pending'")
                .bind(&request_ids)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(pending_count, 5, "all requests should start as pending");

        let resp = app
            .post(&format!("/ai/v1/batches/{batch_id}/cancel"))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        resp.assert_status_ok();

        let updated = _bg_services
            .request_manager
            .cascade_batch_state_to_requests(fusillade::BatchId(batch_id), fusillade::CascadeTargetState::Canceled)
            .await
            .expect("cascade should succeed");
        assert_eq!(updated, 5, "all 5 pending requests should be transitioned");

        let canceled_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM fusillade.requests WHERE id = ANY($1) AND state = 'canceled'")
                .bind(&request_ids)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(canceled_count, 5, "all requests should now be canceled");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_batch_removes_generated_files_and_cascades_state(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser, Role::PlatformManager]).await;
        let auth = add_auth_headers(&user);

        let (batch_id, request_ids) = insert_batch_with_pending_requests(&pool, user.id, 5).await;
        let (output_file_id, error_file_id) = attach_results_files(&pool, batch_id).await;
        let input_file_id: Uuid = sqlx::query_scalar("SELECT file_id FROM fusillade.batches WHERE id = $1")
            .bind(batch_id)
            .fetch_one(&pool)
            .await
            .expect("batch should retain its input file");

        let resp = app
            .delete(&format!("/ai/v1/batches/{batch_id}"))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        resp.assert_status(StatusCode::NO_CONTENT);

        for generated_file_id in [output_file_id, error_file_id] {
            let response = app
                .get(&format!("/ai/v1/files/{generated_file_id}"))
                .add_header(&auth[0].0, &auth[0].1)
                .add_header(&auth[1].0, &auth[1].1)
                .await;
            response.assert_status(StatusCode::NOT_FOUND);
        }

        let input_response = app
            .get(&format!("/ai/v1/files/{input_file_id}"))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        input_response.assert_status(StatusCode::OK);

        let updated = _bg_services
            .request_manager
            .cascade_batch_state_to_requests(fusillade::BatchId(batch_id), fusillade::CascadeTargetState::Canceled)
            .await
            .expect("cascade should succeed");
        assert_eq!(updated, 5);

        let canceled_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM fusillade.requests WHERE id = ANY($1) AND state = 'canceled'")
                .bind(&request_ids)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(canceled_count, 5, "all requests should now be canceled");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_retry_immediately_after_cancel_resumes_batch(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let (batch_id, request_ids) = insert_batch_with_pending_requests(&pool, user.id, 3).await;

        let resp = app
            .post(&format!("/ai/v1/batches/{batch_id}/cancel"))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        resp.assert_status_ok();

        let cancelling_at: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("SELECT cancelling_at FROM fusillade.batches WHERE id = $1")
                .bind(batch_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(cancelling_at.is_some(), "cancel should stamp cancelling_at");
        let pending_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM fusillade.requests WHERE id = ANY($1) AND state = 'pending'")
                .bind(&request_ids)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(pending_count, 3, "no requests should have settled to canceled yet");

        let resp = app
            .post(&format!("/ai/v1/batches/{batch_id}/retry"))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        resp.assert_status_ok();
        let batch: serde_json::Value = resp.json();
        assert_ne!(batch["status"], "cancelling", "resumed batch must not report cancelling");
        assert_ne!(batch["status"], "cancelled", "resumed batch must not report cancelled");

        let (cancelling_at, cancelled_at): (Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as("SELECT cancelling_at, cancelled_at FROM fusillade.batches WHERE id = $1")
                .bind(batch_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(cancelling_at.is_none(), "retry should clear cancelling_at");
        assert!(cancelled_at.is_none(), "retry should clear cancelled_at");

        let pending_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*)::bigint FROM fusillade.requests WHERE id = ANY($1) AND state = 'pending'")
                .bind(&request_ids)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(pending_count, 3, "requests should remain pending and claimable after resume");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_retry_with_nothing_to_retry_returns_400(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&user);

        let (batch_id, _request_ids) = insert_batch_with_pending_requests(&pool, user.id, 3).await;

        let resp = app
            .post(&format!("/ai/v1/batches/{batch_id}/retry"))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .await;
        resp.assert_status(StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_retry_requests_only_retries_requests_in_path_batch(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let other = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let auth = add_auth_headers(&owner);

        let (owner_batch, owner_requests) = insert_batch_with_pending_requests(&pool, owner.id, 1).await;
        let (other_batch, other_requests) = insert_batch_with_pending_requests(&pool, other.id, 1).await;
        sqlx::query("UPDATE fusillade.requests SET state = 'failed', failed_at = NOW(), error = 'test-error' WHERE batch_id = ANY($1)")
            .bind(vec![owner_batch, other_batch])
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE fusillade.batches SET cancelled_at = NOW() WHERE id = $1")
            .bind(other_batch)
            .execute(&pool)
            .await
            .unwrap();

        let state = |id: Uuid| {
            sqlx::query_scalar::<_, String>("SELECT state FROM fusillade.requests WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
        };
        assert_eq!(state(other_requests[0]).await.unwrap(), "failed");
        assert_eq!(state(owner_requests[0]).await.unwrap(), "failed");

        let resp = app
            .post(&format!("/ai/v1/batches/{owner_batch}/retry-requests"))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .json(&serde_json::json!({
                "request_ids": [other_requests[0].to_string(), owner_requests[0].to_string()]
            }))
            .await;
        resp.assert_status_ok();

        assert_eq!(
            state(other_requests[0]).await.unwrap(),
            "failed",
            "requests outside the path batch must not be retried"
        );
        let other_cancelled_at: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("SELECT cancelled_at FROM fusillade.batches WHERE id = $1")
                .bind(other_batch)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(other_cancelled_at.is_some(), "batches other than the path batch must be unchanged");
        assert_eq!(
            state(owner_requests[0]).await.unwrap(),
            "pending",
            "requests in the path batch must still retry"
        );
    }


    async fn setup_org_batch_env(
        pool: &PgPool,
    ) -> (
        axum_test::TestServer,
        crate::BackgroundServices,
        crate::api::models::users::UserResponse,
        crate::api::models::users::UserResponse,
        crate::api::models::users::UserResponse,
        String,
    ) {
        let (app, bg_services) = create_test_app(pool.clone(), false).await;

        let owner = create_test_user_with_roles(pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let member = create_test_user_with_roles(pool, vec![Role::StandardUser, Role::BatchAPIUser]).await;
        let org = create_test_org(pool, owner.id).await;
        add_org_member(pool, org.id, member.id, "member").await;

        let group = create_test_group(pool).await;
        add_user_to_group(pool, org.id, group.id).await;
        add_user_to_group(pool, owner.id, group.id).await;
        let deployment = create_test_deployment(pool, owner.id, "gpt-4-model", "gpt-4").await;
        add_deployment_to_group(pool, deployment.id, group.id, owner.id).await;

        let auth = add_auth_headers(&owner);
        let org_cookie = format!("dw_active_org={}", org.id);
        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let multipart = axum_test::multipart::MultipartForm::new()
            .add_part(
                "file",
                axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("test-batch.jsonl"),
            )
            .add_part("purpose", axum_test::multipart::Part::text("batch"));
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(multipart)
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let file: serde_json::Value = upload_resp.json();
        let file_id = file["id"].as_str().unwrap().to_string();

        (app, bg_services, owner, member, org, file_id)
    }

    fn batch_req(file_id: &str, api_key_id: Option<crate::types::ApiKeyId>) -> CreateBatchRequest {
        CreateBatchRequest {
            input_file_id: file_id.to_string(),
            endpoint: "/v1/chat/completions".to_string(),
            completion_window: "24h".to_string(),
            metadata: None,
            api_key_id,
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_negative_balance_checks_execution_account_in_org_context(pool: PgPool) {
        let (app, _bg_services, owner, _member, org, file_id) = setup_org_batch_env(&pool).await;
        let auth = add_auth_headers(&owner);
        let response = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", owner.id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .json(&serde_json::json!({"name": "Personal capped key", "purpose": "realtime", "spend_limit": "10"}))
            .await;
        response.assert_status(StatusCode::CREATED);
        let key: serde_json::Value = response.json();
        let secret = key["key"].as_str().unwrap();
        sqlx::query("UPDATE user_balance_checkpoints SET balance = -100 WHERE user_id = $1 OR user_id = $2")
            .bind(owner.id)
            .bind(org.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', true)")
            .bind(org.id)
            .execute(&pool)
            .await
            .unwrap();

        for enabled in [false, true, false] {
            sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', $2) ON CONFLICT (user_id, feature_flag) DO UPDATE SET enabled = EXCLUDED.enabled")
                .bind(owner.id).bind(enabled).execute(&pool).await.unwrap();
            app.post("/ai/v1/batches")
                .json(&batch_req(&file_id, None))
                .add_header("authorization", &format!("Bearer {secret}"))
                .add_header("cookie", &format!("dw_active_org={}", org.id))
                .await
                .assert_status(if enabled {
                    StatusCode::CREATED
                } else {
                    StatusCode::PAYMENT_REQUIRED
                });
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_batch_with_selected_api_key(pool: PgPool) {
        let (app, _bg_services, owner, member, org, file_id) = setup_org_batch_env(&pool).await;
        let owner_auth = add_auth_headers(&owner);
        let org_cookie = format!("dw_active_org={}", org.id);

        let resp = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&owner_auth[0].0, &owner_auth[0].1)
            .add_header(&owner_auth[1].0, &owner_auth[1].1)
            .json(&serde_json::json!({"name": "Capped Org Key", "purpose": "realtime", "spend_limit": "10"}))
            .await;
        resp.assert_status(StatusCode::CREATED);
        let key: serde_json::Value = resp.json();
        let key_id: crate::types::ApiKeyId = key["id"].as_str().unwrap().parse().unwrap();

        let jsonl_content = r#"{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}"#;
        let upload_resp = app
            .post("/ai/v1/files")
            .multipart(
                axum_test::multipart::MultipartForm::new()
                    .add_part(
                        "file",
                        axum_test::multipart::Part::bytes(jsonl_content.as_bytes()).file_name("personal.jsonl"),
                    )
                    .add_part("purpose", axum_test::multipart::Part::text("batch")),
            )
            .add_header(&owner_auth[0].0, &owner_auth[0].1)
            .add_header(&owner_auth[1].0, &owner_auth[1].1)
            .await;
        upload_resp.assert_status(StatusCode::CREATED);
        let personal_file: serde_json::Value = upload_resp.json();
        let personal_file_id = personal_file["id"].as_str().unwrap().to_string();
        app.post("/ai/v1/batches")
            .json(&batch_req(&personal_file_id, Some(key_id)))
            .add_header(&owner_auth[0].0, &owner_auth[0].1)
            .add_header(&owner_auth[1].0, &owner_auth[1].1)
            .await
            .assert_status(StatusCode::BAD_REQUEST);

        let member_auth = add_auth_headers(&member);
        app.post("/ai/v1/batches")
            .json(&batch_req(&file_id, Some(key_id)))
            .add_header(&member_auth[0].0, &member_auth[0].1)
            .add_header(&member_auth[1].0, &member_auth[1].1)
            .add_header("cookie", &org_cookie)
            .await
            .assert_status(StatusCode::NOT_FOUND);

        let resp = app
            .post("/ai/v1/batches")
            .json(&batch_req(&file_id, Some(key_id)))
            .add_header(&owner_auth[0].0, &owner_auth[0].1)
            .add_header(&owner_auth[1].0, &owner_auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        resp.assert_status(StatusCode::CREATED);

        let batch_key_id: Option<Uuid> = sqlx::query_scalar("SELECT api_key_id FROM fusillade.batches ORDER BY created_at DESC LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        let child_id: Uuid = sqlx::query_scalar("SELECT id FROM api_keys WHERE parent_api_key_id = $1")
            .bind(key_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(batch_key_id, Some(child_id), "capped selection must execute on the cap-scope child");

        let resp = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&owner_auth[0].0, &owner_auth[0].1)
            .add_header(&owner_auth[1].0, &owner_auth[1].1)
            .json(&serde_json::json!({"name": "Platform Org Key", "purpose": "platform"}))
            .await;
        resp.assert_status(StatusCode::CREATED);
        let platform_key: serde_json::Value = resp.json();
        let platform_key_id: crate::types::ApiKeyId = platform_key["id"].as_str().unwrap().parse().unwrap();
        app.post("/ai/v1/batches")
            .json(&batch_req(&file_id, Some(platform_key_id)))
            .add_header(&owner_auth[0].0, &owner_auth[0].1)
            .add_header(&owner_auth[1].0, &owner_auth[1].1)
            .add_header("cookie", &org_cookie)
            .await
            .assert_status(StatusCode::NOT_FOUND);

        let hidden_root_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM api_keys WHERE hidden = true AND parent_api_key_id IS NULL AND user_id = $1 AND created_by = $2 LIMIT 1",
        )
        .bind(org.id)
        .bind(owner.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        app.post("/ai/v1/batches")
            .json(&batch_req(&file_id, Some(hidden_root_id)))
            .add_header(&owner_auth[0].0, &owner_auth[0].1)
            .add_header(&owner_auth[1].0, &owner_auth[1].1)
            .add_header("cookie", &org_cookie)
            .await
            .assert_status(StatusCode::NOT_FOUND);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_creation_rejects_platform_key_auth(pool: PgPool) {
        let (app, _bg_services, owner, _member, org, file_id) = setup_org_batch_env(&pool).await;
        let owner_auth = add_auth_headers(&owner);

        let mut secrets = std::collections::HashMap::new();
        for (name, purpose) in [("infer-key", "realtime"), ("mgmt-key", "platform")] {
            let resp = app
                .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
                .add_header(&owner_auth[0].0, &owner_auth[0].1)
                .add_header(&owner_auth[1].0, &owner_auth[1].1)
                .json(&serde_json::json!({"name": name, "purpose": purpose}))
                .await;
            resp.assert_status(StatusCode::CREATED);
            let key: serde_json::Value = resp.json();
            secrets.insert(purpose, key["key"].as_str().unwrap().to_string());
        }

        let resp = app
            .post("/ai/v1/batches")
            .json(&batch_req(&file_id, None))
            .add_header("authorization", format!("Bearer {}", secrets["platform"]))
            .await;
        resp.assert_status_forbidden();

        app.post("/ai/v1/batches")
            .json(&batch_req(&file_id, None))
            .add_header("authorization", format!("Bearer {}", secrets["realtime"]))
            .await
            .assert_status(StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_member_without_manage_keys_must_select_batch_key(pool: PgPool) {
        let (app, _bg_services, owner, member, org, file_id) = setup_org_batch_env(&pool).await;
        sqlx::query(
            "DELETE FROM organization_member_roles omr USING user_organizations uo \
             WHERE uo.id = omr.user_organization_id AND uo.organization_id = $1 AND uo.user_id = $2 AND omr.role = 'manage_keys'",
        )
        .bind(org.id)
        .bind(member.id)
        .execute(&pool)
        .await
        .unwrap();

        let owner_auth = add_auth_headers(&owner);
        let member_auth = add_auth_headers(&member);
        let org_cookie = format!("dw_active_org={}", org.id);

        let resp = app
            .post("/ai/v1/batches")
            .json(&batch_req(&file_id, None))
            .add_header(&member_auth[0].0, &member_auth[0].1)
            .add_header(&member_auth[1].0, &member_auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        resp.assert_status(StatusCode::BAD_REQUEST);
        assert!(resp.text().contains("select"), "error should tell the member to select a key");

        let resp = app
            .post(&format!("/admin/api/v1/users/{}/api-keys", org.id))
            .add_header(&owner_auth[0].0, &owner_auth[0].1)
            .add_header(&owner_auth[1].0, &owner_auth[1].1)
            .json(&serde_json::json!({"name": "Issued", "purpose": "realtime", "member_id": member.id}))
            .await;
        resp.assert_status(StatusCode::CREATED);
        let issued: serde_json::Value = resp.json();
        let issued_id: crate::types::ApiKeyId = issued["id"].as_str().unwrap().parse().unwrap();

        let resp = app
            .post("/ai/v1/batches")
            .json(&batch_req(&file_id, Some(issued_id)))
            .add_header(&member_auth[0].0, &member_auth[0].1)
            .add_header(&member_auth[1].0, &member_auth[1].1)
            .add_header("cookie", &org_cookie)
            .await;
        resp.assert_status(StatusCode::CREATED);
        let batch_key_id: Uuid = sqlx::query_scalar("SELECT api_key_id FROM fusillade.batches ORDER BY created_at DESC LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        let attributed_to: Uuid = sqlx::query_scalar("SELECT created_by FROM api_keys WHERE id = $1")
            .bind(batch_key_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(attributed_to, member.id, "batch usage is attributed to the issued key's holder");

        app.post("/ai/v1/batches")
            .json(&batch_req(&file_id, None))
            .add_header(&owner_auth[0].0, &owner_auth[0].1)
            .add_header(&owner_auth[1].0, &owner_auth[1].1)
            .add_header("cookie", &org_cookie)
            .await
            .assert_status(StatusCode::CREATED);
    }
}
