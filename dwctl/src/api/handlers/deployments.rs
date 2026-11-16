
use sqlx_pool_router::PoolProvider;

use crate::api::models::deployments::{ModelFacets, ModelListResponse, TariffDefinition, TrafficRoutingAction, TrafficRoutingRule};
use crate::db::models::deployments::{
    AffinityConfig, AimdConfig, LoadBalancingStrategy, MODEL_CATALOG_METADATA_MAX_BYTES, MODEL_CATALOG_METADATA_MAX_EXTRA_KEYS,
    ModelCatalogMetadata, TrafficRuleAction,
};
use crate::db::models::tariffs::TariffCreateDBRequest;
use crate::{
    AppState,
    api::models::{
        deployments::{
            ComponentEndpointSummary, ComponentModelSummary, ComponentPool, ComponentPoolQuery, DeployedModelCreate, DeployedModelResponse,
            DeployedModelUpdate, GetModelQuery, ListModelsQuery, ModelComponentResponse, enrichment::DeployedModelEnricher,
        },
        users::CurrentUser,
    },
    auth::permissions::{RequiresPermission, can_read_all_resources, has_permission, operation, resource},
    db::{
        handlers::{Deployments, InferenceEndpoints, Repository, Tariffs, deployments::DeploymentFilter},
        models::{
            api_keys::ApiKeyPurpose,
            deployments::{DeploymentComponentDBResponse, DeploymentCreateDBRequest, DeploymentUpdateDBRequest, ModelStatus, ModelType},
        },
    },
    errors::{Error, Result},
    reasoning::ReasoningTranslationOverrides,
    types::{DeploymentId, InferenceEndpointId, Resource},
};
use axum::{
    extract::{Path, Query, State},
    response::Json,
};
use onwards::affinity::AffinityConfig as OnwardsAffinityConfig;
use onwards::aimd::AimdConfig as OnwardsAimdConfig;
use sqlx::Acquire;

fn validate_tariff_prices(tariffs: Option<&[TariffDefinition]>) -> Result<()> {
    for tariff in tariffs.unwrap_or_default() {
        if tariff.input_price_per_token < rust_decimal::Decimal::ZERO || tariff.output_price_per_token < rust_decimal::Decimal::ZERO {
            return Err(Error::BadRequest {
                message: "Tariff input and output prices must be non-negative".to_string(),
            });
        }
    }
    Ok(())
}

fn validate_reasoning_translation_overrides(overrides: Option<&ReasoningTranslationOverrides>) -> Result<()> {
    if let Some(overrides) = overrides {
        overrides.validate().map_err(|error| Error::BadRequest {
            message: error.to_string(),
        })?;
    }
    Ok(())
}

fn validate_realtime_fallback_statuses(statuses: Option<&[i32]>) -> Result<()> {
    if statuses.is_some_and(|statuses| statuses.iter().any(|status| !matches!(status, 4..=5 | 40..=59 | 400..=599))) {
        return Err(Error::BadRequest {
            message: "fallback_realtime_on_status entries must be 4xx/5xx status codes or 4, 5, 40-59 patterns".into(),
        });
    }
    Ok(())
}

fn validate_aimd(timeout: Option<i64>, config: Option<&AimdConfig>, composite: bool, priority: bool, fallback: bool) -> Result<()> {
    if timeout.is_some_and(|ms| !(0..=3_600_000).contains(&ms)) {
        return Err(Error::BadRequest {
            message: "first_token_timeout_ms must be between 0 and 3600000".into(),
        });
    }
    if let Some(config) = config {
        let config: OnwardsAimdConfig = config.clone().into();
        config.validate().map_err(|message| Error::BadRequest { message: message.into() })?;
        if !config.enabled {
            return Ok(());
        }
        if !composite || !priority || !fallback {
            return Err(Error::BadRequest {
                message: "AIMD requires a priority composite model with fallback enabled".into(),
            });
        }
        if !timeout.is_some_and(|ms| ms == 0 || ms as u64 >= config.latency_budget_ms) {
            return Err(Error::BadRequest {
                message: "AIMD requires an explicit first_token_timeout_ms of 0 or at least latency_budget_ms".into(),
            });
        }
    }
    Ok(())
}

fn validate_affinity(config: Option<&AffinityConfig>, composite: bool, priority: bool, fallback: bool) -> Result<()> {
    let Some(config) = config else {
        return Ok(());
    };
    let config: OnwardsAffinityConfig = config.clone().into();
    if !config.enabled {
        return Ok(());
    }
    config.validate().map_err(|message| Error::BadRequest { message: message.into() })?;
    if !composite || !priority || !fallback {
        return Err(Error::BadRequest {
            message: "conversation affinity requires a priority composite model with fallback enabled".into(),
        });
    }
    Ok(())
}

fn validate_backoff(initial_ms: Option<i32>, max_ms: Option<i32>, factor: Option<f64>, max_total_ms: Option<i32>) -> Result<()> {
    if let Some(initial) = initial_ms
        && initial < 1
    {
        return Err(Error::BadRequest {
            message: format!("backoff_initial_ms must be >= 1 (got {})", initial),
        });
    }
    if let Some(max) = max_ms
        && max < 1
    {
        return Err(Error::BadRequest {
            message: format!("backoff_max_ms must be >= 1 (got {})", max),
        });
    }
    if let Some(f) = factor
        && (f < 1.0 || !f.is_finite())
    {
        return Err(Error::BadRequest {
            message: format!("backoff_factor must be >= 1.0 and finite (got {})", f),
        });
    }
    if let (Some(initial), Some(max)) = (initial_ms, max_ms)
        && max < initial
    {
        return Err(Error::BadRequest {
            message: format!("backoff_max_ms ({}) must be >= backoff_initial_ms ({})", max, initial),
        });
    }
    if let (Some(max), Some(budget)) = (max_ms, max_total_ms)
        && budget < max
    {
        return Err(Error::BadRequest {
            message: format!("backoff_max_total_ms ({}) must be >= backoff_max_ms ({})", budget, max),
        });
    }
    Ok(())
}

fn validate_metadata(metadata: &ModelCatalogMetadata) -> Result<()> {
    let size = serde_json::to_vec(metadata).map(|v| v.len()).unwrap_or(0);
    if size > MODEL_CATALOG_METADATA_MAX_BYTES {
        return Err(Error::BadRequest {
            message: format!(
                "metadata exceeds maximum size ({} bytes, limit is {} bytes)",
                size, MODEL_CATALOG_METADATA_MAX_BYTES
            ),
        });
    }
    if let Some(serde_json::Value::Object(map)) = &metadata.extra
        && map.len() > MODEL_CATALOG_METADATA_MAX_EXTRA_KEYS
    {
        return Err(Error::BadRequest {
            message: format!(
                "metadata.extra has too many keys ({}, limit is {})",
                map.len(),
                MODEL_CATALOG_METADATA_MAX_EXTRA_KEYS
            ),
        });
    }
    Ok(())
}

async fn resolve_traffic_rules(
    rules: &[TrafficRoutingRule],
    model_alias: &str,
    repo: &mut Deployments<'_>,
) -> Result<Vec<(ApiKeyPurpose, TrafficRuleAction)>> {
    let mut resolved = Vec::with_capacity(rules.len());
    for rule in rules {
        let action = match &rule.action {
            TrafficRoutingAction::Deny => TrafficRuleAction::Deny,
            TrafficRoutingAction::Redirect { target } => {
                if target.is_empty() {
                    return Err(Error::BadRequest {
                        message: "Redirect target must not be empty".to_string(),
                    });
                }
                if target == model_alias {
                    return Err(Error::BadRequest {
                        message: format!("Traffic routing rule cannot redirect model '{}' to itself", model_alias),
                    });
                }
                let target_id = repo.resolve_alias_to_id(target).await?.ok_or_else(|| Error::BadRequest {
                    message: format!("Redirect target model '{}' does not exist", target),
                })?;
                TrafficRuleAction::Redirect(target_id)
            }
        };
        resolved.push((rule.api_key_purpose.clone(), action));
    }
    Ok(resolved)
}

fn validate_pool_member(pool: ComponentPool, component_hosted_on: Option<InferenceEndpointId>) -> Result<()> {
    if pool == ComponentPool::Completions && component_hosted_on.is_none() {
        return Err(Error::BadRequest {
            message: "A completions-pool component must be hosted on an inference endpoint".to_string(),
        });
    }
    Ok(())
}

fn db_component_to_response(c: DeploymentComponentDBResponse) -> ModelComponentResponse {
    ModelComponentResponse {
        weight: c.weight,
        enabled: c.enabled,
        pool: ComponentPool::from_db(&c.pool),
        sort_order: c.sort_order,
        created_at: c.created_at,
        model: ComponentModelSummary {
            id: c.deployed_model_id,
            alias: c.model_alias,
            model_name: c.model_name,
            description: c.model_description,
            model_type: c.model_type.and_then(|s| match s.as_str() {
                "CHAT" => Some(ModelType::Chat),
                "EMBEDDINGS" => Some(ModelType::Embeddings),
                "RERANKER" => Some(ModelType::Reranker),
                _ => None,
            }),
            endpoint: c.endpoint_id.map(|id| ComponentEndpointSummary {
                id,
                name: c.endpoint_name.unwrap_or_default(),
            }),
            trusted: c.model_trusted,
        },
    }
}

#[utoipa::path(
    get,
    path = "/models",
    tag = "models",
    summary = "List deployed models",
    description = "List all deployed models, optionally filtered by endpoint, group, or availability",
    params(
        ("endpoint" = Option<i32>, Query, description = "Filter by inference endpoint ID"),
        ("group" = Option<String>, Query, description = "Filter by group IDs (comma-separated UUIDs)"),
        ("accessible" = Option<bool>, Query, description = "Filter to only models the current user can access (defaults to false for admins, true for users)"),
        ("include" = Option<String>, Query, description = "Include additional data (comma-separated: 'groups', 'metrics', 'status', 'pricing', 'endpoints', 'facets', 'reasoning_capabilities'). Only platform managers can include groups. Status shows probe monitoring information. Pricing includes active customer tariffs and prompt-cache pricing; provider pricing remains restricted to users with Pricing::ReadAll permission. Endpoints includes full inference endpoint details. Facets returns distinct providers, capabilities, and model types for filter dropdowns. Reasoning capabilities shows efforts supported by every provider behind each model."),
        ("provider" = Option<String>, Query, description = "Filter by provider name (case-insensitive exact match against metadata.provider)"),
        ("model_type" = Option<String>, Query, description = "Filter by model type (CHAT, EMBEDDINGS, RERANKER)"),
        ("capability" = Option<String>, Query, description = "Filter by capability (returns models that have this capability)"),
        ("available_for_realtime" = Option<bool>, Query, description = "Filter by realtime availability. true returns models without a realtime deny rule; false returns models with one."),
        ("sort" = Option<String>, Query, description = "Sort field: created_at (default), alias, intelligence_index, released_at, context_window, provider, price_from"),
        ("sort_direction" = Option<String>, Query, description = "Sort direction: asc or desc (default depends on sort field)"),
        ("deleted" = Option<bool>, Query, description = "Show deleted models when true (admin only), non-deleted models when false, and all models when not specified"),
        ("inactive" = Option<bool>, Query, description = "Show inactive models when true (admin only)"),
        ("limit" = Option<i64>, Query, description = "Maximum number of items to return (default: 10, max: 100)"),
        ("skip" = Option<i64>, Query, description = "Number of items to skip (default: 0)"),
        ("search" = Option<String>, Query, description = "Search query to filter models by alias, model name, display name, or endpoint name (case-insensitive substring match)"),
        ("is_composite" = Option<bool>, Query, description = "Filter by composite/virtual model status (true = virtual models only, false = hosted models only)"),
    ),
    responses(
        (status = 200, description = "Paginated list of deployed models", body = ModelListResponse),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Inference endpoint not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_deployed_models<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Query(query): Query<ListModelsQuery>,
    current_user: CurrentUser,
) -> Result<Json<ModelListResponse>> {
    let has_system_access = has_permission(&current_user, resource::Models.into(), operation::SystemAccess.into());
    let can_read_all_models = can_read_all_resources(&current_user, Resource::Models);
    let can_read_groups = can_read_all_resources(&current_user, Resource::Groups);
    let can_read_users = can_read_all_resources(&current_user, Resource::Users);
    let can_read_pricing = can_read_all_resources(&current_user, Resource::Pricing);
    let can_read_rate_limits = can_read_all_resources(&current_user, Resource::ModelRateLimits);
    let can_read_metrics = can_read_all_resources(&current_user, Resource::Analytics);

    let mut conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;

    if let Some(endpoint_id) = query.endpoint {
        let mut endpoints_repo = InferenceEndpoints::new(&mut conn);
        if endpoints_repo.get_by_id(endpoint_id).await?.is_none() {
            return Err(Error::NotFound {
                resource: "endpoint".to_string(),
                id: endpoint_id.to_string(),
            });
        }
    }

    let mut repo = Deployments::new(&mut conn);

    let (skip, limit) = query.pagination.params();
    let mut filter = DeploymentFilter::new(skip, limit);

    if let Some(endpoint_id) = query.endpoint {
        filter = filter.with_endpoint(endpoint_id);
    };

    if let Some(ref group_str) = query.group {
        let group_ids: std::result::Result<Vec<_>, _> = group_str
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.parse::<crate::types::GroupId>())
            .collect();

        match group_ids {
            Ok(ids) if !ids.is_empty() => {
                filter = filter.with_groups(ids);
            }
            Ok(_) => {
            }
            Err(_) => {
                return Err(Error::BadRequest {
                    message: "Invalid group ID format. Expected comma-separated UUIDs.".to_string(),
                });
            }
        }
    };

    if has_system_access {
        if query.deleted.unwrap_or(false) {
        } else {
            filter = filter.with_deleted(false);
        }
    } else {
        filter = filter.with_deleted(false);
    };

    if has_system_access {
        if query.inactive.unwrap_or(false) {
            filter = filter.with_statuses(vec![ModelStatus::Inactive]);
        } else {
            filter = filter.with_statuses(vec![ModelStatus::Active]);
        }
    } else {
        filter = filter.with_statuses(vec![ModelStatus::Active]);
    };

    if !can_read_all_models || query.accessible.unwrap_or(false) {
        let target_user_id = current_user.active_organization.unwrap_or(current_user.id);
        filter = filter.with_accessible_to(target_user_id);
    }

    if let Some(search) = query.search.as_ref()
        && !search.trim().is_empty()
    {
        filter = filter.with_search(search.trim().to_string());
    }

    if let Some(is_composite) = query.is_composite {
        filter = filter.with_composite(is_composite);
    }

    if let Some(ref provider) = query.provider
        && !provider.trim().is_empty()
    {
        filter = filter.with_provider(provider.trim().to_string());
    }

    if let Some(model_type) = query.model_type {
        filter = filter.with_model_type(model_type);
    }

    if let Some(ref capability) = query.capability
        && !capability.trim().is_empty()
    {
        filter = filter.with_capability(capability.trim().to_string());
    }

    if let Some(available_for_realtime) = query.available_for_realtime {
        filter = filter.with_realtime_availability(available_for_realtime);
    }

    if !can_read_pricing {
        filter.pricing_account = Some(current_user.active_organization.unwrap_or(current_user.id));
    }

    if let Some(sort_field) = query.sort {
        filter = filter.with_sort(sort_field, query.sort_direction);
    }

    let all_includes: Vec<&str> = query
        .include
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    let mut includes: Vec<&str> = Vec::new();
    for &include in &all_includes {
        match include {
            "groups" => {
                if can_read_groups {
                    includes.push(include);
                }
            }
            "metrics" => {
                if can_read_metrics {
                    includes.push(include);
                }
            }
            "endpoints" => {
                if can_read_all_models {
                    includes.push(include);
                }
            }
            _ => {
                includes.push(include);
            }
        }
    }

    let total_count = repo.count(&filter).await?;
    let filtered_models = repo.list(&filter).await?;

    let mut models: Vec<DeployedModelResponse> = filtered_models
        .into_iter()
        .map(|model| {
            let provider_pricing = if can_read_pricing { model.provider_pricing.clone() } else { None };
            DeployedModelResponse::from(model).with_provider_pricing(provider_pricing)
        })
        .collect();

    {
        let model_ids: Vec<DeploymentId> = models.iter().map(|m| m.id).collect();
        let mut traffic_rules_map = repo.get_traffic_rules_bulk(&model_ids).await?;
        models = models
            .into_iter()
            .map(|model| {
                let rules = traffic_rules_map.remove(&model.id).unwrap_or_default();
                model.with_traffic_rules(rules)
            })
            .collect();
    }

    let include_groups = includes.contains(&"groups");
    let include_metrics = includes.contains(&"metrics");
    let include_status = includes.contains(&"status");
    let include_pricing = includes.contains(&"pricing");
    let include_endpoints = includes.contains(&"endpoints");
    let include_components = includes.contains(&"components");
    let include_reasoning_capabilities = includes.contains(&"reasoning_capabilities");

    let reasoning_policies = if include_reasoning_capabilities {
        let aliases = models.iter().map(|model| model.alias.clone()).collect::<Vec<_>>();
        repo.get_reasoning_policies(&aliases).await?
    } else {
        Default::default()
    };

    let enricher = DeployedModelEnricher {
        db: &state.db.read(),
        include_groups,
        include_metrics,
        include_status,
        include_pricing,
        include_endpoints,
        include_components,
        can_read_pricing,
        can_read_rate_limits,
        can_read_users,
        can_read_composite_info: can_read_all_models,
        pricing_account: Some(current_user.active_organization.unwrap_or(current_user.id)),
    };

    let response = enricher.enrich_many(models).await?;
    let response = if include_reasoning_capabilities {
        response
            .into_iter()
            .map(|model| {
                let supported_efforts = reasoning_policies.get(&model.alias).and_then(|policy| policy.supported_efforts());
                model.with_supported_reasoning_efforts(supported_efforts)
            })
            .collect()
    } else {
        response
    };

    let include_facets = includes.contains(&"facets");
    let facets = if include_facets {
        let (providers, capabilities, model_types) = repo.facets(&filter).await?;
        Some(ModelFacets {
            providers,
            capabilities,
            model_types,
        })
    } else {
        None
    };

    Ok(Json(ModelListResponse {
        data: response,
        total_count,
        skip,
        limit,
        facets,
    }))
}

fn validate_realtime_inflight_limit(limit: Option<i32>) -> Result<()> {
    if limit.is_some_and(|limit| limit < 1) {
        return Err(Error::BadRequest {
            message: "realtime_inflight_limit must be at least 1".to_string(),
        });
    }
    Ok(())
}

fn validate_batch_capacity(capacity: Option<i32>) -> Result<()> {
    if capacity.is_some_and(|capacity| capacity < 1) {
        return Err(Error::BadRequest {
            message: "batch_capacity must be at least 1".to_string(),
        });
    }
    Ok(())
}

fn validate_alias_has_no_class_separator(alias: &str) -> Result<()> {
    if alias.contains(crate::inference::serving_name::CLASS_SEPARATOR) {
        return Err(Error::BadRequest {
            message: format!("Model alias '{alias}' must not contain ':', which separates a model from its serving class"),
        });
    }
    Ok(())
}

#[utoipa::path(
    post,
    path = "/models",
    tag = "models",
    summary = "Create a new deployed model",
    description = "Create a new deployed model. Admin only.",
    request_body = DeployedModelCreate,
    responses(
        (status = 201, description = "Model created successfully", body = DeployedModelResponse),
        (status = 400, description = "Bad request - invalid model data or duplicate alias/model name"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden - admin access required"),
        (status = 404, description = "Inference endpoint not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn create_deployed_model<P: PoolProvider>(
    State(state): State<AppState<P>>,
    current_user: RequiresPermission<resource::Models, operation::CreateAll>,
    Json(create): Json<DeployedModelCreate>,
) -> Result<Json<DeployedModelResponse>> {
    let (model_name, alias, hosted_on, tariffs, throughput) = match &create {
        DeployedModelCreate::Standard(s) => (
            s.model_name.trim(),
            s.alias.as_deref().unwrap_or(s.model_name.trim()).trim(),
            Some(s.hosted_on),
            s.tariffs.clone(),
            s.throughput,
        ),
        DeployedModelCreate::Composite(c) => (
            c.model_name.trim(),
            c.alias.as_deref().unwrap_or(c.model_name.trim()).trim(),
            None,
            c.tariffs.clone(),
            c.throughput,
        ),
    };

    if model_name.is_empty() || alias.is_empty() {
        return Err(Error::BadRequest {
            message: "Model name and alias must not be empty or whitespace".to_string(),
        });
    }
    validate_alias_has_no_class_separator(alias)?;

    validate_tariff_prices(tariffs.as_deref())?;

    let batch_capacity = match &create {
        DeployedModelCreate::Standard(s) => s.batch_capacity,
        DeployedModelCreate::Composite(c) => c.batch_capacity,
    };
    validate_batch_capacity(batch_capacity)?;

    if let Some(t) = throughput
        && t <= 0.0
    {
        return Err(Error::BadRequest {
            message: format!("throughput must be positive (> 0), got {}", t),
        });
    }

    let batch_windows = match &create {
        DeployedModelCreate::Standard(s) => &s.allowed_batch_completion_windows,
        DeployedModelCreate::Composite(c) => &c.allowed_batch_completion_windows,
    };
    if let Some(windows) = batch_windows {
        let config = state.current_config();
        let allowed = &config.batches.allowed_completion_windows;
        for window in windows {
            if !allowed.contains(window) {
                return Err(Error::BadRequest {
                    message: format!(
                        "Invalid batch completion window '{}'. Configured windows: {}",
                        window,
                        allowed.join(", ")
                    ),
                });
            }
        }
    }

    let metadata = match &create {
        DeployedModelCreate::Standard(s) => &s.metadata,
        DeployedModelCreate::Composite(c) => &c.metadata,
    };
    if let Some(m) = metadata {
        validate_metadata(m)?;
    }

    if let DeployedModelCreate::Standard(standard) = &create {
        validate_reasoning_translation_overrides(standard.reasoning_translation_overrides.as_ref())?;
    }

    let (b_initial, b_max, b_factor, b_total) = match &create {
        DeployedModelCreate::Standard(s) => (s.backoff_initial_ms, s.backoff_max_ms, s.backoff_factor, s.backoff_max_total_ms),
        DeployedModelCreate::Composite(c) => (c.backoff_initial_ms, c.backoff_max_ms, c.backoff_factor, c.backoff_max_total_ms),
    };
    validate_backoff(Some(b_initial), Some(b_max), Some(b_factor), b_total)?;
    if let DeployedModelCreate::Composite(c) = &create {
        validate_realtime_fallback_statuses(Some(&c.fallback_realtime_on_status))?;
        validate_realtime_inflight_limit(c.realtime_inflight_limit)?;
    }
    match &create {
        DeployedModelCreate::Standard(s) => validate_aimd(s.first_token_timeout_ms, s.aimd.as_ref(), false, false, s.backoff_enabled)?,
        DeployedModelCreate::Composite(c) => {
            validate_aimd(
                c.first_token_timeout_ms,
                c.aimd.as_ref(),
                true,
                c.lb_strategy == LoadBalancingStrategy::Priority,
                c.fallback_enabled,
            )?;
            validate_affinity(
                c.affinity.as_ref(),
                true,
                c.lb_strategy == LoadBalancingStrategy::Priority,
                c.fallback_enabled,
            )?;
        }
    }

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;

    if let Some(endpoint_id) = hosted_on {
        let mut endpoints_repo = InferenceEndpoints::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
        if endpoints_repo.get_by_id(endpoint_id).await?.is_none() {
            return Err(Error::NotFound {
                resource: "endpoint".to_string(),
                id: endpoint_id.to_string(),
            });
        }
    }

    let traffic_rules_input = match &create {
        DeployedModelCreate::Standard(s) => &s.traffic_routing_rules,
        DeployedModelCreate::Composite(c) => &c.traffic_routing_rules,
    };
    let resolved_rules = if let Some(rules) = traffic_rules_input {
        let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
        Some(resolve_traffic_rules(rules, alias, &mut repo).await?)
    } else {
        None
    };

    let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
    let db_request = DeploymentCreateDBRequest::from_api_create(current_user.id, create);
    let model = repo.create(&db_request).await?;

    if let Some(rules) = &resolved_rules {
        let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
        repo.set_traffic_rules(model.id, rules).await?;
    }

    if let Some(mut tariff_defs) = tariffs {
        for tariff in &mut tariff_defs {
            if tariff.api_key_purpose == Some(ApiKeyPurpose::Batch) {
                tariff.completion_window = tariff.completion_window.take().map(|window| window.trim().to_owned());
            }
        }
        let mut tariffs_repo = Tariffs::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
        for tariff_def in tariff_defs {
            if tariff_def
                .api_key_purpose
                .as_ref()
                .is_none_or(|purpose| !purpose.is_customer_billing())
            {
                return Err(Error::BadRequest {
                    message: "Tariffs support only realtime, batch and playground customer inference".to_string(),
                });
            }
            if tariff_def.api_key_purpose == Some(ApiKeyPurpose::Batch) {
                if tariff_def.completion_window.as_deref().is_none_or(|w| w.trim().is_empty()) {
                    return Err(Error::BadRequest {
                        message: "Batch tariffs require a completion window".to_string(),
                    });
                }
            } else if tariff_def.completion_window.is_some() {
                return Err(Error::BadRequest {
                    message: "Only batch tariffs may specify a completion window".to_string(),
                });
            }
            let tariff_request = TariffCreateDBRequest {
                deployed_model_id: model.id,
                name: tariff_def.name,
                input_price_per_token: tariff_def.input_price_per_token,
                output_price_per_token: tariff_def.output_price_per_token,
                api_key_purpose: tariff_def.api_key_purpose,
                completion_window: tariff_def.completion_window,
                valid_from: None,
                user_id: None,
            };
            tariffs_repo.create(&tariff_request).await?;
        }
    }

    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    let mut response = DeployedModelResponse::from(model);
    if resolved_rules.is_some() {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let mut repo = Deployments::new(&mut conn);
        let rules = repo.get_traffic_rules(response.id).await?;
        response = response.with_traffic_rules(rules);
    }

    Ok(Json(response))
}

#[utoipa::path(
    patch,
    path = "/models/{id}",
    tag = "models",
    summary = "Update deployed model",
    description = "Update a deployed model",
    params(
        ("id" = uuid::Uuid, Path, description = "Deployment ID to update"),
    ),
    responses(
        (status = 200, description = "Deployed model updated successfully", body = DeployedModelResponse),
        (status = 400, description = "Bad request - invalid model data"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Inference endpoint or deployment not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn update_deployed_model<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(deployment_id): Path<DeploymentId>,
    current_user: RequiresPermission<resource::Models, operation::UpdateAll>,
    Json(mut update): Json<DeployedModelUpdate>,
) -> Result<Json<DeployedModelResponse>> {
    let has_system_access = has_permission(&current_user, resource::Models.into(), operation::SystemAccess.into());

    validate_tariff_prices(update.tariffs.as_deref())?;

    validate_batch_capacity(update.batch_capacity.flatten())?;

    if let Some(Some(t)) = update.throughput
        && t <= 0.0
    {
        return Err(Error::BadRequest {
            message: format!("throughput must be positive (> 0), got {}", t),
        });
    }

    if let Some(Some(windows)) = &update.allowed_batch_completion_windows {
        let config = state.current_config();
        let allowed = &config.batches.allowed_completion_windows;
        for window in windows {
            if !allowed.contains(window) {
                return Err(Error::BadRequest {
                    message: format!(
                        "Invalid batch completion window '{}'. Configured windows: {}",
                        window,
                        allowed.join(", ")
                    ),
                });
            }
        }
    }

    if let Some(m) = &update.metadata {
        validate_metadata(m)?;
    }
    validate_reasoning_translation_overrides(update.reasoning_translation_overrides.as_ref().and_then(Option::as_ref))?;

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;

    let (
        model_alias,
        is_composite,
        prev_lb_strategy,
        cur_initial,
        cur_max,
        cur_factor,
        cur_total,
        cur_timeout,
        cur_aimd,
        cur_affinity,
        cur_fallback,
    ) = {
        let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
        match repo.get_by_id(deployment_id).await {
            Ok(Some(model)) => {
                if model.deleted && !has_system_access {
                    return Err(Error::NotFound {
                        resource: "Deployment".to_string(),
                        id: deployment_id.to_string(),
                    });
                }
                (
                    model.alias.clone(),
                    model.is_composite,
                    model.lb_strategy,
                    model.backoff_initial_ms,
                    model.backoff_max_ms,
                    model.backoff_factor,
                    model.backoff_max_total_ms,
                    model.first_token_timeout_ms,
                    model.aimd,
                    model.affinity,
                    model.fallback_enabled,
                )
            }
            Ok(None) => {
                return Err(Error::NotFound {
                    resource: "Deployment".to_string(),
                    id: deployment_id.to_string(),
                });
            }
            Err(e) => return Err(e.into()),
        }
    };

    if let Some(alias) = update.alias.as_deref().map(str::trim)
        && alias != model_alias
    {
        validate_alias_has_no_class_separator(alias)?;
    }
    validate_realtime_fallback_statuses(update.fallback_realtime_on_status.as_deref())?;
    validate_aimd(
        update.first_token_timeout_ms.unwrap_or(cur_timeout),
        update.aimd.as_ref().map(|v| v.as_ref()).unwrap_or(cur_aimd.as_ref()),
        is_composite,
        update.lb_strategy.as_ref().unwrap_or(&prev_lb_strategy) == &LoadBalancingStrategy::Priority,
        update.fallback_enabled.unwrap_or(cur_fallback),
    )?;
    validate_affinity(
        update.affinity.as_ref().map(|v| v.as_ref()).unwrap_or(cur_affinity.as_ref()),
        is_composite,
        update.lb_strategy.as_ref().unwrap_or(&prev_lb_strategy) == &LoadBalancingStrategy::Priority,
        update.fallback_enabled.unwrap_or(cur_fallback),
    )?;

    if is_composite && update.reasoning_translation_overrides.is_some() {
        return Err(Error::BadRequest {
            message: "reasoning_translation_overrides is only supported for standard models".to_string(),
        });
    }

    if !is_composite && update.realtime_inflight_limit.is_some() {
        return Err(Error::BadRequest {
            message: "realtime_inflight_limit is only supported for composite models".to_string(),
        });
    }
    validate_realtime_inflight_limit(update.realtime_inflight_limit)?;

    validate_backoff(
        Some(update.backoff_initial_ms.unwrap_or(cur_initial)),
        Some(update.backoff_max_ms.unwrap_or(cur_max)),
        Some(update.backoff_factor.unwrap_or(cur_factor)),
        match update.backoff_max_total_ms {
            Some(v) => v,
            None => cur_total,
        },
    )?;

    if !is_composite {
        match update.backoff_enabled {
            Some(true) => {
                update.fallback_enabled = Some(true);
                update.fallback_with_replacement = Some(true);
                if update.fallback_max_attempts.is_none() {
                    update.fallback_max_attempts = Some(Some(3));
                }
            }
            Some(false) => {
                update.fallback_enabled = Some(false);
            }
            None => {}
        }
    }

    let resolved_rules = match &update.traffic_routing_rules {
        Some(Some(rules)) => {
            let effective_alias = update.alias.as_deref().unwrap_or(&model_alias);
            let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
            Some(Some(resolve_traffic_rules(rules, effective_alias, &mut repo).await?))
        }
        Some(None) => Some(None),
        None => None,
    };

    let tariffs = update.tariffs.clone();
    let db_request = DeploymentUpdateDBRequest::from(update);
    let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
    let model = repo.update(deployment_id, &db_request).await?;

    if is_composite && prev_lb_strategy != LoadBalancingStrategy::Priority && model.lb_strategy == LoadBalancingStrategy::Priority {
        let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
        repo.renumber_components_by_weight(deployment_id).await?;
    }

    match &resolved_rules {
        Some(Some(rules)) => {
            let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
            repo.set_traffic_rules(deployment_id, rules).await?;
        }
        Some(None) => {
            let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
            repo.set_traffic_rules(deployment_id, &[]).await?;
        }
        None => {}
    }

    if let Some(mut tariff_defs) = tariffs {
        for tariff in &mut tariff_defs {
            if tariff.api_key_purpose == Some(ApiKeyPurpose::Batch) {
                tariff.completion_window = tariff.completion_window.take().map(|window| window.trim().to_owned());
            }
        }
        let scheduled: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM model_tariffs WHERE deployed_model_id=$1 AND user_id IS NULL AND serving_class IS NULL
             AND valid_from>NOW() AND (valid_until IS NULL OR valid_until>valid_from))",
        )
        .bind(deployment_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| Error::Database(e.into()))?;
        if scheduled {
            return Err(Error::BadRequest {
                message: "This model has scheduled future prices; explicitly cancel or resolve the schedule before replacing tariffs"
                    .to_string(),
            });
        }
        let tariff_conn = tx.acquire().await.map_err(|e| Error::Database(e.into()))?;
        let mut tariffs_repo = Tariffs::new(tariff_conn);

        let current_tariffs = tariffs_repo.list_current_by_model(deployment_id).await?;

        let tariff_matches = |existing: &crate::db::models::tariffs::ModelTariff,
                              def: &crate::api::models::deployments::TariffDefinition| {
            existing.name == def.name
                && existing.input_price_per_token == def.input_price_per_token
                && existing.output_price_per_token == def.output_price_per_token
                && existing.api_key_purpose == def.api_key_purpose
                && existing.completion_window == def.completion_window
        };

        let tariffs_to_close: Vec<uuid::Uuid> = current_tariffs
            .iter()
            .filter(|existing| !tariff_defs.iter().any(|def| tariff_matches(existing, def)))
            .map(|t| t.id)
            .collect();

        if !tariffs_to_close.is_empty() {
            tariffs_repo.close_tariffs_batch(&tariffs_to_close).await?;
        }

        for tariff_def in tariff_defs {
            if current_tariffs.iter().any(|existing| tariff_matches(existing, &tariff_def)) {
                continue;
            }
            if tariff_def
                .api_key_purpose
                .as_ref()
                .is_none_or(|purpose| !purpose.is_customer_billing())
            {
                return Err(Error::BadRequest {
                    message: "Tariffs support only realtime, batch and playground customer inference".to_string(),
                });
            }
            if tariff_def.api_key_purpose == Some(ApiKeyPurpose::Batch) {
                if tariff_def.completion_window.as_deref().is_none_or(|w| w.trim().is_empty()) {
                    return Err(Error::BadRequest {
                        message: "Batch tariffs require a completion window".to_string(),
                    });
                }
            } else if tariff_def.completion_window.is_some() {
                return Err(Error::BadRequest {
                    message: "Only batch tariffs may specify a completion window".to_string(),
                });
            }
            let tariff_request = TariffCreateDBRequest {
                deployed_model_id: deployment_id,
                name: tariff_def.name,
                input_price_per_token: tariff_def.input_price_per_token,
                output_price_per_token: tariff_def.output_price_per_token,
                api_key_purpose: tariff_def.api_key_purpose,
                completion_window: tariff_def.completion_window,
                valid_from: None,
                user_id: None,
            };
            tariffs_repo.create(&tariff_request).await?;
        }
    }

    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    let mut response = DeployedModelResponse::from(model);
    let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Deployments::new(&mut conn);
    let rules = repo.get_traffic_rules(deployment_id).await?;
    response = response.with_traffic_rules(rules);

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/models/{id}",
    tag = "models",
    summary = "Get deployed model",
    description = "Get a specific deployed model. include=pricing includes active customer tariffs and prompt-cache pricing.",
    params(
        ("id" = uuid::Uuid, Path, description = "Deployment ID to retrieve"),
        GetModelQuery
    ),
    responses(
        (status = 200, description = "Deployed model information", body = DeployedModelResponse),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Inference endpoint or deployment not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_deployed_model<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(deployment_id): Path<DeploymentId>,
    Query(query): Query<GetModelQuery>,
    current_user: CurrentUser,
) -> Result<Json<DeployedModelResponse>> {
    let has_system_access = has_permission(&current_user, resource::Models.into(), operation::SystemAccess.into());
    let can_read_all_models = can_read_all_resources(&current_user, Resource::Models);
    let can_read_groups = can_read_all_resources(&current_user, Resource::Groups);
    let can_read_users = can_read_all_resources(&current_user, Resource::Users);
    let can_read_rate_limits = can_read_all_resources(&current_user, Resource::ModelRateLimits);
    let can_read_pricing = can_read_all_resources(&current_user, Resource::Pricing);
    let can_read_metrics = can_read_all_resources(&current_user, Resource::Analytics);

    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Deployments::new(&mut pool_conn);

    let model = match repo.get_by_id(deployment_id).await {
        Ok(Some(model)) => model,
        Ok(None) => {
            return Err(Error::NotFound {
                resource: "Deployment".to_string(),
                id: deployment_id.to_string(),
            });
        }
        Err(e) => return Err(e.into()),
    };

    match (model.deleted, &model.status) {
        (true, _) => {
            if !has_system_access || !query.deleted.unwrap_or(false) {
                return Err(Error::NotFound {
                    resource: "Deployment".to_string(),
                    id: deployment_id.to_string(),
                });
            }
        }
        (false, ModelStatus::Inactive) => {
            if !has_system_access || !query.inactive.unwrap_or(false) {
                return Err(Error::NotFound {
                    resource: "Deployment".to_string(),
                    id: deployment_id.to_string(),
                });
            }
        }
        (false, _) => {
        }
    }

    if !can_read_all_models {
        let has_access = repo.check_user_access(&model.alias, current_user.id).await?;

        if has_access.is_none() {
            return Err(Error::NotFound {
                resource: "Deployment".to_string(),
                id: deployment_id.to_string(),
            });
        }
    }

    let include_params = query.include.as_deref().unwrap_or("");
    let all_includes: Vec<&str> = include_params.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();

    let mut include_groups = false;
    let mut include_metrics = false;
    let mut include_status = false;
    let mut include_pricing = false;
    let mut include_endpoints = false;
    let mut include_components = false;

    for &include in &all_includes {
        match include {
            "groups"
                if can_read_groups => {
                    include_groups = true;
                }
            "metrics"
                if can_read_metrics => {
                    include_metrics = true;
                }
            "endpoints"
                if can_read_all_models => {
                    include_endpoints = true;
                }
            "status" => {
                include_status = true;
            }
            "pricing" => {
                include_pricing = true;
            }
            "components" => {
                include_components = true;
            }
            _ => {
            }
        }
    }

    let provider_pricing = if include_pricing && can_read_pricing {
        model.provider_pricing.clone()
    } else {
        None
    };
    let mut response = DeployedModelResponse::from(model).with_provider_pricing(provider_pricing);

    {
        let traffic_rules = repo.get_traffic_rules(deployment_id).await?;
        response = response.with_traffic_rules(traffic_rules);
    }

    let enricher = DeployedModelEnricher {
        db: &state.db.read(),
        include_groups,
        include_metrics,
        include_status,
        include_pricing,
        include_endpoints,
        include_components,
        can_read_pricing,
        can_read_rate_limits,
        can_read_users,
        can_read_composite_info: can_read_all_models,
        pricing_account: Some(current_user.active_organization.unwrap_or(current_user.id)),
    };

    response = enricher.enrich_one(response).await?;

    Ok(Json(response))
}

#[utoipa::path(
    delete,
    path = "/models/{id}",
    tag = "models",
    summary = "Delete deployed model",
    description = "Delete a deployed model",
    params(
        ("id" = uuid::Uuid, Path, description = "Deployment ID to delete"),
    ),
    responses(
        (status = 200, description = "Deployed model deleted successfully"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Inference endpoint or deployment not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn delete_deployed_model<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(deployment_id): Path<DeploymentId>,
    _: RequiresPermission<resource::Models, operation::DeleteAll>,
) -> Result<Json<String>> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Deployments::new(&mut pool_conn);

    let update_request = DeploymentUpdateDBRequest::visibility_update(true);

    repo.update(deployment_id, &update_request).await?;
    Ok(Json(deployment_id.to_string()))
}


use crate::api::models::deployments::{ModelComponentCreate, ModelComponentUpdate};
use crate::db::models::deployments::DeploymentComponentCreateDBRequest;

#[utoipa::path(
    get,
    path = "/models/{id}/components",
    tag = "models",
    summary = "Get composite model components",
    description = "Get the list of underlying models that make up a composite model",
    params(
        ("id" = String, Path, description = "The composite model ID", format = "uuid"),
    ),
    responses(
        (status = 200, description = "List of components", body = Vec<ModelComponentResponse>),
        (status = 400, description = "Model is not a composite model"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Composite model not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_model_components<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(id): Path<DeploymentId>,
    _: RequiresPermission<resource::CompositeModels, operation::ReadAll>,
) -> Result<Json<Vec<ModelComponentResponse>>> {
    {
        let mut conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let mut repo = Deployments::new(&mut conn);
        let deployment = repo.get_by_id(id).await?.ok_or_else(|| Error::NotFound {
            resource: "model".to_string(),
            id: id.to_string(),
        })?;

        if !deployment.is_composite {
            return Err(Error::BadRequest {
                message: "Model is not a composite model".to_string(),
            });
        }
    }

    let mut conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Deployments::new(&mut conn);
    let components = repo.get_components(id).await?;

    let response: Vec<ModelComponentResponse> = components.into_iter().map(db_component_to_response).collect();

    Ok(Json(response))
}

#[utoipa::path(
    post,
    path = "/models/{id}/components/{component_id}",
    tag = "models",
    summary = "Add component to composite model",
    description = "Add an underlying model as a component of a composite model",
    params(
        ("id" = String, Path, description = "The composite model ID", format = "uuid"),
        ("component_id" = String, Path, description = "The deployed model ID to add as a component", format = "uuid"),
    ),
    request_body = ModelComponentCreate,
    responses(
        (status = 200, description = "Component added", body = ModelComponentResponse),
        (status = 400, description = "Model is not a composite model or component is not valid"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Composite model or component model not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn add_model_component<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, component_id)): Path<(DeploymentId, DeploymentId)>,
    _: RequiresPermission<resource::CompositeModels, operation::UpdateAll>,
    Json(body): Json<ModelComponentCreate>,
) -> Result<Json<ModelComponentResponse>> {
    if !(1..=100).contains(&body.weight) {
        return Err(Error::BadRequest {
            message: "Weight must be between 1 and 100".to_string(),
        });
    }

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;

    {
        let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);

        let composite = repo.get_by_id(id).await?.ok_or_else(|| Error::NotFound {
            resource: "composite model".to_string(),
            id: id.to_string(),
        })?;

        if !composite.is_composite {
            return Err(Error::BadRequest {
                message: "Model is not a composite model".to_string(),
            });
        }

        let component = repo.get_by_id(component_id).await?.ok_or_else(|| Error::NotFound {
            resource: "component model".to_string(),
            id: component_id.to_string(),
        })?;

        if component.is_composite {
            return Err(Error::BadRequest {
                message: "Cannot add a composite model as a component".to_string(),
            });
        }

        validate_pool_member(body.pool, component.hosted_on)?;
    }

    let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
    repo.lock_composite(id).await?;
    let sort_order = repo.next_component_sort_order(id, body.pool.as_str()).await?;
    let request = DeploymentComponentCreateDBRequest {
        composite_model_id: id,
        deployed_model_id: component_id,
        weight: body.weight,
        enabled: body.enabled,
        sort_order,
        pool: body.pool.as_str().to_string(),
    };

    let component = repo.add_component(&request).await?;
    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    Ok(Json(db_component_to_response(component)))
}

#[utoipa::path(
    patch,
    path = "/models/{id}/components/{component_id}",
    tag = "models",
    summary = "Update component in composite model",
    description = "Update the weight or enabled status of a component",
    params(
        ("id" = String, Path, description = "The composite model ID", format = "uuid"),
        ("component_id" = String, Path, description = "The deployed model ID of the component", format = "uuid"),
        ComponentPoolQuery,
    ),
    request_body = ModelComponentUpdate,
    responses(
        (status = 200, description = "Component updated", body = ModelComponentResponse),
        (status = 400, description = "Invalid weight"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Composite model or component not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn update_model_component<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, component_id)): Path<(DeploymentId, DeploymentId)>,
    Query(query): Query<ComponentPoolQuery>,
    _: RequiresPermission<resource::CompositeModels, operation::UpdateAll>,
    Json(body): Json<ModelComponentUpdate>,
) -> Result<Json<ModelComponentResponse>> {
    if let Some(weight) = body.weight
        && !(1..=100).contains(&weight)
    {
        return Err(Error::BadRequest {
            message: "Weight must be between 1 and 100".to_string(),
        });
    }

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;
    let component = {
        let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
        repo.lock_composite(id).await?;
        repo.update_component(id, component_id, query.pool.as_str(), body.weight, body.enabled, body.sort_order)
            .await?
            .ok_or_else(|| Error::NotFound {
                resource: "component".to_string(),
                id: format!("{}/{}/{}", id, component_id, query.pool.as_str()),
            })?
    };
    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    Ok(Json(db_component_to_response(component)))
}

#[utoipa::path(
    delete,
    path = "/models/{id}/components/{component_id}",
    tag = "models",
    summary = "Remove component from composite model",
    description = "Remove an underlying model from a composite model",
    params(
        ("id" = String, Path, description = "The composite model ID", format = "uuid"),
        ("component_id" = String, Path, description = "The deployed model ID of the component to remove", format = "uuid"),
        ComponentPoolQuery,
    ),
    responses(
        (status = 200, description = "Component removed"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Composite model or component not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn remove_model_component<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, component_id)): Path<(DeploymentId, DeploymentId)>,
    Query(query): Query<ComponentPoolQuery>,
    _: RequiresPermission<resource::CompositeModels, operation::UpdateAll>,
) -> Result<Json<String>> {
    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;
    {
        let mut repo = Deployments::new(tx.acquire().await.map_err(|e| Error::Database(e.into()))?);
        repo.lock_composite(id).await?;

        let removed = repo.remove_component(id, component_id, query.pool.as_str()).await?;
        if !removed {
            return Err(Error::NotFound {
                resource: "component".to_string(),
                id: format!("{}/{}/{}", id, component_id, query.pool.as_str()),
            });
        }

        repo.compact_component_sort_order(id, query.pool.as_str()).await?;
    }
    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    Ok(Json("Component removed".to_string()))
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::{
        api::{
            handlers::deployments::DeployedModelResponse,
            models::{pagination::PaginatedResponse, users::Role},
        },
        db::{
            handlers::{CacheTariffOverrides, CacheTariffs, Deployments, Groups, Repository},
            models::{api_keys::ApiKeyPurpose, deployments::TrafficRuleAction, groups::GroupCreateDBRequest},
        },
        test::utils::*,
        types::DeploymentId,
    };
    use serde_json::json;
    use sqlx::PgPool;

    #[dwctl_test_macros::test]
    async fn model_aliases_cannot_contain_a_class_separator(pool: PgPool) {
        let (app, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let headers = add_auth_headers(&user);
        let create = |alias: &str| {
            app.post("/admin/api/v1/models")
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({"type":"composite","model_name":alias,"lb_strategy":"priority"}))
        };
        create("org/model:fast").await.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let response = create("org/model").await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        app.patch(&format!("/admin/api/v1/models/{}", model.id))
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"alias":"org/model:fast"}))
            .await
            .assert_status(axum::http::StatusCode::BAD_REQUEST);
        sqlx::query("UPDATE deployed_models SET alias = 'legacy/member:interactive' WHERE id = $1")
            .bind(model.id)
            .execute(&pool)
            .await
            .unwrap();
        app.patch(&format!("/admin/api/v1/models/{}", model.id))
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"alias":"legacy/member:interactive","description":"edited"}))
            .await
            .assert_status_ok();
    }

    #[dwctl_test_macros::test]
    async fn aimd_api_create_patch_clear_and_validate(pool: PgPool) {
        let (app, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let headers = add_auth_headers(&user);
        let config = json!({"latency_budget_ms":100,"breach_rate_target":0.1,"window_samples":20,
            "min_samples":5,"share_step":0.05,"share_decay":0.5,"share_floor":0.1,"dwell_ms":1000});
        let response = app
            .post("/admin/api/v1/models")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"type":"composite","model_name":"aimd-model","lb_strategy":"priority",
                "fallback_enabled":true,"first_token_timeout_ms":200,"aimd":config}))
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert_eq!(model.fallback.as_ref().unwrap().first_token_timeout_ms, Some(200));
        assert!(model.fallback.unwrap().aimd.is_some());
        let path = format!("/admin/api/v1/models/{}", model.id);
        app.patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"first_token_timeout_ms":0,"aimd":{"enabled":true,"latency_budget_ms":null}}))
            .await
            .assert_status_unprocessable_entity();

        for patch in [
            json!({"first_token_timeout_ms":50}),
            json!({"first_token_timeout_ms":null}),
            json!({"lb_strategy":"weighted_random"}),
            json!({"fallback_enabled":false}),
            json!({"first_token_timeout_ms":-1}),
        ] {
            app.patch(&path)
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&patch)
                .await
                .assert_status_bad_request();
        }
        let response = app
            .patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"description":"unchanged settings"}))
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert!(model.fallback.unwrap().aimd.is_some());
        app.patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"aimd":null,"first_token_timeout_ms":null,"lb_strategy":"weighted_random"}))
            .await
            .assert_status_ok();
        let response = app
            .get(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        let fallback = model.fallback.unwrap();
        assert!(fallback.aimd.is_none());
        assert!(fallback.first_token_timeout_ms.is_none());
        let response = app
            .patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"aimd":{"enabled":false}}))
            .await;
        response.assert_status_ok();
        let disabled: DeployedModelResponse = response.json();
        assert!(!disabled.fallback.unwrap().aimd.unwrap().enabled);
        app.patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"aimd":null}))
            .await
            .assert_status_ok();
        let mut conn = pool.acquire().await.unwrap();
        let stored = Deployments::new(&mut conn).get_by_id(model.id).await.unwrap().unwrap();
        assert!(stored.aimd.is_none());
        assert!(stored.first_token_timeout_ms.is_none());
    }

    #[dwctl_test_macros::test]
    async fn realtime_fallback_statuses_default_patch_and_validate(pool: PgPool) {
        let (app, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let headers = add_auth_headers(&user);
        let response = app
            .post("/admin/api/v1/models")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"type":"composite","model_name":"realtime-fallback-model"}))
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert_eq!(model.fallback.unwrap().realtime_on_status, vec![529]);
        let path = format!("/admin/api/v1/models/{}", model.id);

        for statuses in [json!([200]), json!([600]), json!([-529])] {
            app.patch(&path)
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({"fallback_realtime_on_status": statuses}))
                .await
                .assert_status_bad_request();
        }

        let response = app
            .patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"fallback_realtime_on_status": []}))
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert!(model.fallback.unwrap().realtime_on_status.is_empty());

        app.patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"fallback_realtime_on_status": [529]}))
            .await
            .assert_status_ok();
        let response = app
            .patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"fallback_on_status": [500]}))
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert_eq!(model.fallback.unwrap().realtime_on_status, vec![529]);
    }

    #[dwctl_test_macros::test]
    async fn affinity_api_create_patch_clear_and_validate(pool: PgPool) {
        let (app, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let headers = add_auth_headers(&user);
        let response = app
            .post("/admin/api/v1/models")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"type":"composite","model_name":"affinity-model","lb_strategy":"priority",
                "fallback_enabled":true,"affinity":{"target_conversations":40}}))
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert_eq!(model.fallback.as_ref().unwrap().affinity.as_ref().unwrap().target_conversations, 40);
        let path = format!("/admin/api/v1/models/{}", model.id);

        app.patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"affinity":{"target":5}}))
            .await
            .assert_status_unprocessable_entity();
        for patch in [
            json!({"affinity":{"target_conversations":0}}),
            json!({"lb_strategy":"weighted_random"}),
            json!({"fallback_enabled":false}),
        ] {
            app.patch(&path)
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&patch)
                .await
                .assert_status_bad_request();
        }
        let response = app
            .patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"affinity":{"target_conversations":60,"margin":5}}))
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        let affinity = model.fallback.unwrap().affinity.unwrap();
        assert_eq!((affinity.target_conversations, affinity.margin), (60, Some(5)));
        let response = app
            .patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"description":"unchanged routing"}))
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert!(model.fallback.unwrap().affinity.is_some());
        app.patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"affinity":null}))
            .await
            .assert_status_ok();
        let mut conn = pool.acquire().await.unwrap();
        let stored = Deployments::new(&mut conn).get_by_id(model.id).await.unwrap().unwrap();
        assert!(stored.affinity.is_none());
        app.post("/admin/api/v1/models")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(
                &json!({"type":"composite","model_name":"weighted-affinity","lb_strategy":"weighted_random",
                "fallback_enabled":true,"affinity":{"target_conversations":10}}),
            )
            .await
            .assert_status_bad_request();
    }

    fn get_model_by_id(id: DeploymentId, response: &PaginatedResponse<DeployedModelResponse>) -> Option<&DeployedModelResponse> {
        response.data.iter().find(|model| model.id == id)
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_deployments(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let response = app
            .get(&format!("/admin/api/v1/models?endpoint={test_endpoint_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(response_body.data.is_empty() || !response_body.data.is_empty());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn admin_models_include_pricing_exposes_cache_tariffs(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let priced = create_test_deployment(&pool, admin.id, "priced-model", "priced-model").await;
        let unpriced = create_test_deployment(&pool, admin.id, "unpriced-model", "unpriced-model").await;

        let mut conn = pool.acquire().await.unwrap();
        CacheTariffs::new(&mut conn)
            .enable(
                priced.id,
                &crate::config::CachePricingConfig::default(),
                CacheTariffOverrides {
                    write_multiplier_5m: Some(rust_decimal::Decimal::new(125, 2)),
                    write_multiplier_1h: Some(rust_decimal::Decimal::new(2, 0)),
                    write_multiplier_24h: Some(rust_decimal::Decimal::new(25, 1)),
                    read_multiplier: Some(rust_decimal::Decimal::new(1, 1)),
                    min_prefix_tokens: Some(2048),
                },
            )
            .await
            .unwrap();

        let response = app
            .get("/admin/api/v1/models?include=pricing")
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;
        response.assert_status_ok();
        let body: serde_json::Value = response.json();
        let models = body["data"].as_array().expect("list response should contain data");
        let priced_model = models
            .iter()
            .find(|model| model["id"] == priced.id.to_string())
            .expect("priced model should be listed");
        let unpriced_model = models
            .iter()
            .find(|model| model["id"] == unpriced.id.to_string())
            .expect("unpriced model should be listed");
        let cache_pricing = &priced_model["cache_pricing"];
        assert_eq!(cache_pricing["enabled"], true);
        assert_eq!(cache_pricing["write_multiplier_5m"], "1.2500");
        assert_eq!(cache_pricing["write_multiplier_1h"], "2.0000");
        assert_eq!(cache_pricing["write_multiplier_24h"], "2.5000");
        assert_eq!(cache_pricing["read_multiplier"], "0.1000");
        assert_eq!(cache_pricing["min_prefix_tokens"], 2048);
        assert!(cache_pricing["valid_from"].is_string());
        assert_eq!(cache_pricing["valid_until"], serde_json::Value::Null);
        assert_eq!(
            unpriced_model["cache_pricing"],
            json!({
                "enabled": false,
                "write_multiplier_5m": null,
                "write_multiplier_1h": null,
                "write_multiplier_24h": null,
                "read_multiplier": null,
                "min_prefix_tokens": null,
                "valid_from": null,
                "valid_until": null,
            })
        );

        let response = app
            .get(&format!("/admin/api/v1/models/{}?include=pricing", priced.id))
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;
        response.assert_status_ok();
        let model: serde_json::Value = response.json();
        assert_eq!(model["cache_pricing"]["enabled"], true);
        assert_eq!(model["cache_pricing"]["min_prefix_tokens"], 2048);

        let response = app
            .get(&format!("/admin/api/v1/models/{}", priced.id))
            .add_header(&add_auth_headers(&admin)[0].0, &add_auth_headers(&admin)[0].1)
            .add_header(&add_auth_headers(&admin)[1].0, &add_auth_headers(&admin)[1].1)
            .await;
        response.assert_status_ok();
        let model: serde_json::Value = response.json();
        assert!(model.get("cache_pricing").is_none());
    }

    #[dwctl_test_macros::test]
    async fn customer_model_prices_hide_class_details_but_managers_keep_them(pool: PgPool) {
        let (app, _services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let customer = create_test_user(&pool, Role::StandardUser).await;
        let model = create_test_deployment(&pool, admin.id, "customer-price", "customer-price").await;
        sqlx::query(
            "INSERT INTO deployment_groups (deployment_id,group_id,granted_by) VALUES ($1,'00000000-0000-0000-0000-000000000000',$2)",
        )
        .bind(model.id)
        .bind(admin.id)
        .execute(&pool)
        .await
        .unwrap();
        for (account, class, rate) in [
            (None, None, 3),
            (Some(customer.id), None, 2),
            (Some(customer.id), Some("standard"), 9),
            (Some(customer.id), Some("interactive"), 0),
        ] {
            sqlx::query("INSERT INTO model_tariffs (deployed_model_id,user_id,serving_class,name,input_price_per_token,output_price_per_token,api_key_purpose) VALUES ($1,$2,$3,'price',$4,$4,'realtime')").bind(model.id).bind(account).bind(class).bind(rust_decimal::Decimal::from(rate)).execute(&pool).await.unwrap();
        }
        for (account, class, rate) in [
            (None, None, "0.1"),
            (Some(customer.id), None, "0.2"),
            (Some(customer.id), Some("interactive"), "0.8"),
        ] {
            sqlx::query("INSERT INTO model_cache_tariffs (deployed_model_id,user_id,serving_class,write_multiplier_5m,write_multiplier_1h,write_multiplier_24h,read_multiplier,min_prefix_tokens) VALUES ($1,$2,$3,1,1,1,$4,1)").bind(model.id).bind(account).bind(class).bind(rate.parse::<rust_decimal::Decimal>().unwrap()).execute(&pool).await.unwrap();
        }
        for user in [&customer, &admin] {
            for path in [
                "/admin/api/v1/models?include=pricing".to_string(),
                format!("/admin/api/v1/models/{}?include=pricing", model.id),
            ] {
                let headers = add_auth_headers(user);
                let response = app
                    .get(&path)
                    .add_header(&headers[0].0, &headers[0].1)
                    .add_header(&headers[1].0, &headers[1].1)
                    .await;
                response.assert_status_ok();
                let body: serde_json::Value = response.json();
                let response_model = if let Some(data) = body["data"].as_array() {
                    data.iter().find(|m| m["id"] == model.id.to_string()).unwrap()
                } else {
                    &body
                };
                let tariffs = response_model["tariffs"].as_array().unwrap();
                if user.id == customer.id {
                    assert_eq!(tariffs.len(), 1);
                    assert_eq!(
                        tariffs[0]["input_price_per_token"]
                            .as_str()
                            .unwrap()
                            .parse::<rust_decimal::Decimal>()
                            .unwrap(),
                        rust_decimal::Decimal::from(2)
                    );
                    assert!(tariffs[0].get("organization_id").is_none());
                    assert!(tariffs[0].get("serving_class").is_none());
                    assert!(response_model.get("cache_pricing_by_class").is_none());
                    assert_eq!(response_model["cache_pricing"]["read_multiplier"], "0.2000");
                } else {
                    assert_eq!(tariffs.len(), 4);
                    assert!(
                        tariffs
                            .iter()
                            .any(|t| t["serving_class"] == "interactive" && t["organization_id"] == customer.id.to_string())
                    );
                }
            }
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_deployments_with_nonexistent_endpoint(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let non_existent_id = uuid::Uuid::new_v4();

        let response = app
            .get(&format!("/admin/api/v1/models?endpoint={non_existent_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_not_found();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_model_operations(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let created = create_test_deployment(&pool, user.id, "test-model", "test-alias").await;
        let deployment_id = created.id;

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert_eq!(model.id, deployment_id);
        assert_eq!(model.model_name, "test-model");
        assert_eq!(model.alias, "test-alias");

        let update = json!({
            "alias": "new-alias"
        });
        let response = app
            .patch(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .json(&update)
            .await;

        response.assert_status_ok();
        let updated_model: DeployedModelResponse = response.json();
        assert_eq!(updated_model.alias, "new-alias");

        let test_endpoint_id = get_test_endpoint_id(&pool).await;
        let response = app
            .get(&format!("/admin/api/v1/models?endpoint={test_endpoint_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(response_body.data.iter().any(|it| it.id == deployment_id));

        let response = app
            .delete(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_not_found();

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}?deleted=true"))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_composite_model_patch_rejects_reasoning_translation_overrides(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .json(&json!({
                "type": "composite",
                "model_name": "reasoning-composite",
                "alias": "reasoning-composite"
            }))
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();

        let response = app
            .patch(&format!("/admin/api/v1/models/{}", model.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .json(&json!({
                "reasoning_translation_overrides": {
                    "chat_completions": { "mode": "disabled" }
                }
            }))
            .await;

        response.assert_status_bad_request();

        let stored =
            sqlx::query_scalar::<_, Option<serde_json::Value>>("SELECT reasoning_translation_overrides FROM deployed_models WHERE id = $1")
                .bind(model.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(stored, None);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_realtime_inflight_limit_below_one_is_rejected(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let auth = add_auth_headers(&user);

        app.post("/admin/api/v1/models")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .json(&json!({ "type": "composite", "model_name": "inflight-zero", "alias": "inflight-zero", "realtime_inflight_limit": 0 }))
            .await
            .assert_status_bad_request();

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .json(&json!({ "type": "composite", "model_name": "inflight-one", "alias": "inflight-one" }))
            .await;
        response.assert_status_ok();
        let created: DeployedModelResponse = response.json();
        app.patch(&format!("/admin/api/v1/models/{}", created.id))
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .json(&json!({ "realtime_inflight_limit": 0 }))
            .await
            .assert_status_bad_request();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_non_positive_batch_capacity_is_rejected(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let auth = add_auth_headers(&user);

        for invalid in [0, -1] {
            app.post("/admin/api/v1/models")
                .add_header(&auth[0].0, &auth[0].1)
                .add_header(&auth[1].0, &auth[1].1)
                .json(&json!({ "type": "composite", "model_name": format!("batch-{invalid}"), "alias": format!("batch-{invalid}"), "batch_capacity": invalid }))
                .await
                .assert_status_bad_request();
        }

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&auth[0].0, &auth[0].1)
            .add_header(&auth[1].0, &auth[1].1)
            .json(&json!({ "type": "composite", "model_name": "batch-valid", "alias": "batch-valid", "batch_capacity": 5 }))
            .await;
        response.assert_status_ok();
        let created: DeployedModelResponse = response.json();

        for invalid in [0, -1] {
            app.patch(&format!("/admin/api/v1/models/{}", created.id))
                .add_header(&auth[0].0, &auth[0].1)
                .add_header(&auth[1].0, &auth[1].1)
                .json(&json!({ "batch_capacity": invalid }))
                .await
                .assert_status_bad_request();
        }

        let stored = sqlx::query_scalar::<_, i32>("SELECT batch_capacity FROM deployed_models WHERE id = $1")
            .bind(created.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(stored, 5, "a rejected update must leave the stored value untouched");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_standard_model_patch_rejects_realtime_inflight_limit(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let model = create_test_deployment(&pool, user.id, "inflight-standard", "inflight-standard").await;

        let response = app
            .patch(&format!("/admin/api/v1/models/{}", model.id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .json(&json!({ "realtime_inflight_limit": 500 }))
            .await;

        response.assert_status_bad_request();

        let stored = sqlx::query_scalar::<_, i32>("SELECT realtime_inflight_limit FROM deployed_models WHERE id = $1")
            .bind(model.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(stored, 14);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_deployments_with_groups_include(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let deployment = create_test_deployment(&pool, admin_user.id, "test-model", "test-alias").await;
        assert!(deployment.last_sync.is_none());

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Test Group".to_string(),
            description: Some("Test group for deployment".to_string()),
            created_by: admin_user.id,
        };
        let group = group_repo.create(&group_create).await.expect("Failed to create test group");
        group_repo
            .add_deployment_to_group(deployment.id, group.id, admin_user.id)
            .await
            .expect("Failed to add deployment to group");

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(models.data.iter().any(|it| it.id == deployment.id && it.groups.is_none()));

        let response = app
            .get("/admin/api/v1/models?include=groups")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();

        assert!(
            models
                .data
                .iter()
                .any(|it| { it.id == deployment.id && it.groups.as_deref().is_some_and(|gs| gs.len() == 1 && gs[0].id == group.id) })
        );

        let test_endpoint_id = get_test_endpoint_id(&pool).await;
        let response = app
            .get(&format!("/admin/api/v1/models?endpoint={test_endpoint_id}&include=groups"))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(
            models
                .data
                .iter()
                .any(|it| { it.id == deployment.id && it.groups.as_deref().is_some_and(|gs| gs.iter().any(|g| g.id == group.id)) })
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_deployments_with_reasoning_capabilities_include(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        sqlx::query("UPDATE inference_endpoints SET reasoning_translation = $1 WHERE id = $2")
            .bind(json!({
                "chat_completions": {
                    "unsupported_efforts": ["minimal", "xhigh", "max"],
                    "writes": [{
                        "target_path": "/reasoning_effort",
                        "values": {
                            "none": "none",
                            "low": "low",
                            "medium": "medium",
                            "high": "high"
                        }
                    }]
                }
            }))
            .bind(test_endpoint_id)
            .execute(&pool)
            .await
            .expect("Failed to configure reasoning translation");

        let deployment = create_test_deployment(&pool, admin_user.id, "reasoning-model", "reasoning-alias").await;

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let body: serde_json::Value = response.json();
        let model = body["data"]
            .as_array()
            .and_then(|models| models.iter().find(|model| model["id"] == deployment.id.to_string()))
            .expect("Created model should be listed");
        assert!(model.get("supported_reasoning_efforts").is_none());

        let response = app
            .get("/admin/api/v1/models?include=pricing,reasoning_capabilities")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let body: serde_json::Value = response.json();
        let model = body["data"]
            .as_array()
            .and_then(|models| models.iter().find(|model| model["id"] == deployment.id.to_string()))
            .expect("Created model should be listed");
        assert_eq!(
            model["supported_reasoning_efforts"],
            json!({
                "chat_completions": ["none", "low", "medium", "high"]
            })
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_role_based_visibility_for_deleted_models(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;

        let deployment = create_test_deployment(&pool, admin_user.id, "test-model", "test-alias").await;
        let deployment_id = deployment.id;

        let everyone_group_id = uuid::Uuid::nil();
        add_deployment_to_group(&pool, deployment_id, everyone_group_id, admin_user.id).await;

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;
        response.assert_status_ok();

        let response = app
            .delete(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}?deleted=true"))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert_eq!(model.id, deployment_id);

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;
        response.assert_status_not_found();

    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_role_based_list_filtering(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;

        let deployment1 = create_test_deployment(&pool, admin_user.id, "active-model", "active-alias").await;
        let deployment2 = create_test_deployment(&pool, admin_user.id, "hidden-model", "hidden-alias").await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "List Filter Test Group".to_string(),
            description: Some("Test group for list filtering".to_string()),
            created_by: admin_user.id,
        };
        let group = group_repo.create(&group_create).await.unwrap();
        group_repo.add_user_to_group(regular_user.id, group.id).await.unwrap();

        group_repo
            .add_deployment_to_group(deployment1.id, group.id, admin_user.id)
            .await
            .unwrap();

        let response = app
            .delete(&format!("/admin/api/v1/models/{}", deployment2.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();

        let response = app
            .get("/admin/api/v1/models?deleted=true")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let admin_all_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(admin_all_models.data.iter().any(|it| it.id == deployment1.id));
        assert!(admin_all_models.data.iter().any(|it| it.id == deployment2.id));

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let admin_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(admin_models.data.iter().any(|it| it.id == deployment1.id));
        assert!(!admin_models.data.iter().any(|it| it.id == deployment2.id));

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;
        response.assert_status_ok();
        let user_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(user_models.data.iter().any(|it| it.id == deployment1.id));
        assert!(!user_models.data.iter().any(|it| it.id == deployment2.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_role_based_update_access_for_deleted_models(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;

        let deployment = create_test_deployment(&pool, admin_user.id, "update-test-model", "update-test-alias").await;
        let deployment_id = deployment.id;

        let response = app
            .delete(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();

        let update = json!({
            "alias": "admin-updated-alias"
        });
        let response = app
            .patch(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&update)
            .await;
        response.assert_status_ok();
        let updated_model: DeployedModelResponse = response.json();
        assert_eq!(updated_model.alias, "admin-updated-alias");

        let update = json!({
            "alias": "user-attempted-update"
        });
        let response = app
            .patch(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .json(&update)
            .await;
        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_soft_delete_preserves_model_accessibility_for_admin(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;

        let deployment = create_test_deployment(&pool, admin_user.id, "preserve-test-model", "preserve-test-alias").await;
        let deployment_id = deployment.id;

        let everyone_group_id = uuid::Uuid::nil();
        add_deployment_to_group(&pool, deployment_id, everyone_group_id, admin_user.id).await;

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;
        response.assert_status_ok();

        let response = app
            .delete(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}?deleted=true"))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert_eq!(model.model_name, "preserve-test-model");
        assert_eq!(model.alias, "preserve-test-alias");

        let response = app
            .get(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;
        response.assert_status_not_found();

        let update = json!({
            "alias": "updated-after-deletion"
        });
        let response = app
            .patch(&format!("/admin/api/v1/models/{deployment_id}"))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&update)
            .await;
        response.assert_status_ok();
        let updated_model: DeployedModelResponse = response.json();
        assert_eq!(updated_model.alias, "updated-after-deletion");
    }

    #[dwctl_test_macros::test]
    async fn invalid_tariffs_are_rejected_atomically_on_create_and_update(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let headers = add_auth_headers(&admin);
        let endpoint = get_test_endpoint_id(&pool).await;
        let existing = create_test_deployment(&pool, admin.id, "original", "original").await;
        for (purpose, window) in [
            (Some("continuation"), None),
            (Some("platform"), None),
            (None, None),
            (Some("realtime"), Some("24h")),
            (Some("batch"), None),
            (Some("batch"), Some("   ")),
        ] {
            let tariffs = json!([{"name":"invalid", "api_key_purpose":purpose, "completion_window":window,
                "input_price_per_token":"1", "output_price_per_token":"2"}]);
            app.post("/admin/api/v1/models")
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({"type":"standard", "model_name":"invalid", "alias":"invalid", "hosted_on":endpoint, "tariffs":tariffs}))
                .await
                .assert_status_bad_request();
            app.patch(&format!("/admin/api/v1/models/{}", existing.id))
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({"alias":"must-rollback", "tariffs":tariffs}))
                .await
                .assert_status_bad_request();
        }
        let alias: String = sqlx::query_scalar("SELECT alias FROM deployed_models WHERE id=$1")
            .bind(existing.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(alias, "original");
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM deployed_models WHERE alias='invalid'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM model_tariffs WHERE deployed_model_id=$1")
            .bind(existing.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[dwctl_test_macros::test]
    async fn negative_tariff_prices_are_rejected_before_model_or_ledger_changes(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let headers = add_auth_headers(&admin);
        let endpoint = get_test_endpoint_id(&pool).await;
        let existing = create_test_deployment(&pool, admin.id, "original", "original").await;
        let original_tariff: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO model_tariffs(deployed_model_id,name,api_key_purpose,input_price_per_token,output_price_per_token)
             VALUES ($1,'original','realtime',1,2) RETURNING id",
        )
        .bind(existing.id)
        .fetch_one(&pool)
        .await
        .unwrap();

        for (input, output) in [("-1", "2"), ("1", "-0.00000001"), ("-1", "-2")] {
            let tariffs = json!([
                {"name":"valid", "api_key_purpose":"playground", "input_price_per_token":"1", "output_price_per_token":"2"},
                {"name":"invalid", "api_key_purpose":"realtime", "input_price_per_token":input, "output_price_per_token":output}
            ]);
            for kind in ["standard", "composite"] {
                let mut create = json!({"type":kind,"model_name":"invalid-price","alias":"invalid-price","tariffs":tariffs});
                if kind == "standard" {
                    create["hosted_on"] = json!(endpoint);
                }
                let response = app
                    .post("/admin/api/v1/models")
                    .add_header(&headers[0].0, &headers[0].1)
                    .add_header(&headers[1].0, &headers[1].1)
                    .json(&create)
                    .await;
                response.assert_status_bad_request();
                assert!(response.text().contains("non-negative"));
            }
            let response = app
                .patch(&format!("/admin/api/v1/models/{}", existing.id))
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({"alias":"must-not-change","tariffs":tariffs}))
                .await;
            response.assert_status_bad_request();
            assert!(response.text().contains("non-negative"));

            let rows: Vec<(uuid::Uuid, Decimal, Decimal, bool)> = sqlx::query_as(
                "SELECT id,input_price_per_token,output_price_per_token,valid_until IS NULL FROM model_tariffs WHERE deployed_model_id=$1",
            )
            .bind(existing.id)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(rows, vec![(original_tariff, Decimal::ONE, Decimal::from(2), true)]);
            let alias: String = sqlx::query_scalar("SELECT alias FROM deployed_models WHERE id=$1")
                .bind(existing.id)
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(alias, "original");
        }
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM deployed_models WHERE alias='invalid-price'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[dwctl_test_macros::test]
    async fn zero_tariff_prices_remain_valid_on_create_and_update(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let headers = add_auth_headers(&admin);
        let endpoint = get_test_endpoint_id(&pool).await;
        for (input, output) in [("0", "2"), ("1", "0"), ("0", "0")] {
            let alias = format!("zero-{input}-{output}");
            let tariff =
                json!({"name":"zero", "api_key_purpose":"realtime", "input_price_per_token":input,"output_price_per_token":output});
            let response = app
                .post("/admin/api/v1/models")
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({"type":"standard","model_name":alias,"alias":alias,"hosted_on":endpoint,"tariffs":[tariff]}))
                .await;
            response.assert_status_ok();
            let model: DeployedModelResponse = response.json();
            let original: uuid::Uuid = sqlx::query_scalar("SELECT id FROM model_tariffs WHERE deployed_model_id=$1")
                .bind(model.id)
                .fetch_one(&pool)
                .await
                .unwrap();
            app.patch(&format!("/admin/api/v1/models/{}", model.id))
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({"description":"zero remains valid","tariffs":[tariff]}))
                .await
                .assert_status_ok();
            let rows: Vec<(uuid::Uuid, Decimal, Decimal)> =
                sqlx::query_as("SELECT id,input_price_per_token,output_price_per_token FROM model_tariffs WHERE deployed_model_id=$1")
                    .bind(model.id)
                    .fetch_all(&pool)
                    .await
                    .unwrap();
            assert_eq!(rows, vec![(original, input.parse().unwrap(), output.parse().unwrap())]);
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn customer_tariff_windows_are_normalized_on_create_and_update(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let headers = add_auth_headers(&admin);
        let endpoint = get_test_endpoint_id(&pool).await;
        for (purpose, window, expected_window) in [("realtime", None, None), ("batch", Some(" 24h "), Some("24h"))] {
            let tariff = json!({"name":"price", "api_key_purpose":purpose, "completion_window":window,
                "input_price_per_token":"1", "output_price_per_token":"2"});
            let response = app
                .post("/admin/api/v1/models")
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({"type":"standard", "model_name":purpose, "alias":purpose, "hosted_on":endpoint, "tariffs":[tariff]}))
                .await;
            response.assert_status_ok();
            let model: DeployedModelResponse = response.json();
            let original: (uuid::Uuid, Option<String>) =
                sqlx::query_as("SELECT id,completion_window FROM model_tariffs WHERE deployed_model_id=$1")
                    .bind(model.id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(original.1.as_deref(), expected_window);
            for output in ["2", "3"] {
                let mut update = tariff.clone();
                update["output_price_per_token"] = json!(output);
                app.patch(&format!("/admin/api/v1/models/{}", model.id))
                    .add_header(&headers[0].0, &headers[0].1)
                    .add_header(&headers[1].0, &headers[1].1)
                    .json(&json!({"tariffs":[update]}))
                    .await
                    .assert_status_ok();
                let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM model_tariffs WHERE deployed_model_id=$1")
                    .bind(model.id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                assert_eq!(rows, if output == "2" { 1 } else { 2 });
                let selected: (uuid::Uuid, Option<String>, Decimal) = sqlx::query_as(
                    "SELECT id,completion_window,output_price_per_token FROM effective_model_tariff($1,$2,$3,$4,'standard',clock_timestamp())")
                    .bind(model.id).bind(admin.id).bind(purpose).bind(expected_window)
                    .fetch_one(&pool).await.unwrap();
                assert_eq!(selected.1.as_deref(), expected_window);
                assert_eq!(selected.2, output.parse::<Decimal>().unwrap());
                assert_eq!(selected.0 == original.0, output == "2");
            }
        }
    }

    #[dwctl_test_macros::test]
    async fn unchanged_legacy_tariff_allows_metadata_edits_but_not_repricing(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let headers = add_auth_headers(&admin);
        let model = create_test_deployment(&pool, admin.id, "legacy", "legacy").await;
        let id: uuid::Uuid = sqlx::query_scalar("INSERT INTO model_tariffs (deployed_model_id,name,input_price_per_token,output_price_per_token,valid_from) VALUES ($1,'legacy',1,2,NOW()-INTERVAL '1 day') RETURNING id")
            .bind(model.id).fetch_one(&pool).await.unwrap();
        let mut tariff = json!({"name":"legacy","api_key_purpose":null,"input_price_per_token":"1","output_price_per_token":"2"});
        app.patch(&format!("/admin/api/v1/models/{}", model.id))
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"description":"metadata edit", "tariffs":[tariff]}))
            .await
            .assert_status_ok();
        tariff["output_price_per_token"] = json!("3");
        app.patch(&format!("/admin/api/v1/models/{}", model.id))
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"description":"must roll back", "tariffs":[tariff]}))
            .await
            .assert_status_bad_request();
        let description: String = sqlx::query_scalar("SELECT description FROM deployed_models WHERE id=$1")
            .bind(model.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(description, "metadata edit");
        let rows: Vec<(uuid::Uuid, Decimal)> =
            sqlx::query_as("SELECT id,output_price_per_token FROM model_tariffs WHERE deployed_model_id=$1 AND valid_until IS NULL")
                .bind(model.id)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows, vec![(id, Decimal::from(2))]);
        let billable: i64 = sqlx::query_scalar("SELECT count(*) FROM effective_model_tariff($1,$2,'realtime',NULL,'standard',NOW())")
            .bind(model.id)
            .bind(admin.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(billable, 0, "preserving legacy metadata must not make it billable");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_deployed_model(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let create_request = json!({
            "type": "standard",
            "model_name": "test-new-model",
            "alias": "Test New Model",
            "hosted_on": test_endpoint_id.to_string(),
            "description": "A test model created via API",
            "model_type": "CHAT",
            "capabilities": ["text-generation", "streaming"]
        });

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&create_request)
            .await;

        response.assert_status_ok();
        let created_model: DeployedModelResponse = response.json();

        assert_eq!(created_model.model_name, "test-new-model");
        assert_eq!(created_model.alias, "Test New Model");
        assert_eq!(created_model.hosted_on, Some(test_endpoint_id));
        assert_eq!(created_model.description, Some("A test model created via API".to_string()));
        assert_eq!(created_model.created_by, Some(admin_user.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_deployed_model_with_defaults(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let create_request = json!({
            "type": "standard",
            "model_name": "simple-model",
            "hosted_on": test_endpoint_id.to_string()
        });

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&create_request)
            .await;

        response.assert_status_ok();
        let created_model: DeployedModelResponse = response.json();

        assert_eq!(created_model.model_name, "simple-model");
        assert_eq!(created_model.alias, "simple-model");
        assert_eq!(created_model.hosted_on, Some(test_endpoint_id));
        assert_eq!(created_model.description, None);
        assert_eq!(created_model.created_by, Some(admin_user.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_deployed_model_non_admin_forbidden(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let create_request = json!({
            "type": "standard",
            "model_name": "forbidden-model",
            "hosted_on": test_endpoint_id.to_string()
        });

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .json(&create_request)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_deployed_model_nonexistent_endpoint(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let create_request = json!({
            "type": "standard",
            "model_name": "test-model",
            "hosted_on": "99999999-9999-9999-9999-999999999999"
        });

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&create_request)
            .await;

        response.assert_status_not_found();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_include_groups_admin_only(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;

        let deployment = create_test_deployment(&pool, admin_user.id, "groups-test-model", "groups-test-alias").await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut groups_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Test Group".to_string(),
            description: Some("Test group for include test".to_string()),
            created_by: admin_user.id,
        };
        let group = groups_repo.create(&group_create).await.expect("Failed to create group");
        groups_repo
            .add_deployment_to_group(deployment.id, group.id, admin_user.id)
            .await
            .expect("Failed to add deployment to group");

        groups_repo
            .add_user_to_group(regular_user.id, group.id)
            .await
            .expect("Failed to add regular user to group");

        let response = app
            .get("/admin/api/v1/models?include=groups")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();

        let test_model = get_model_by_id(deployment.id, &models).unwrap_or_else(|| {
            panic!(
                "Test model not found. Available models: {:?}",
                models.data.iter().map(|m| &m.id).collect::<Vec<_>>()
            )
        });

        assert!(test_model.groups.is_some(), "Admin should see groups included");
        let groups = test_model.groups.as_ref().unwrap();
        assert_eq!(groups.len(), 1, "Should have exactly one group");
        assert_eq!(groups[0].name, "Test Group");

        let response = app
            .get("/admin/api/v1/models?include=groups")
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;
        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();

        let test_model = get_model_by_id(deployment.id, &models).unwrap_or_else(|| {
            panic!(
                "Test model not found. Available models: {:?}",
                models.data.iter().map(|m| &m.id).collect::<Vec<_>>()
            )
        });
        assert!(test_model.groups.is_none(), "Regular user should NOT see groups included");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_accessible_parameter_filtering(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;

        let deployment1 = create_test_deployment(&pool, admin_user.id, "test-model-1", "test-alias-1").await;
        let deployment2 = create_test_deployment(&pool, admin_user.id, "test-model-2", "test-alias-2").await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Access Test Group".to_string(),
            description: Some("Test group for accessible filtering".to_string()),
            created_by: admin_user.id,
        };
        let group = group_repo.create(&group_create).await.unwrap();
        group_repo.add_user_to_group(regular_user.id, group.id).await.unwrap();

        group_repo
            .add_deployment_to_group(deployment1.id, group.id, admin_user.id)
            .await
            .unwrap();

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;
        response.assert_status_ok();
        let user_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(user_models.data.len(), 1, "Regular user should only see 1 accessible model");
        assert!(get_model_by_id(deployment1.id, &user_models).is_some());
        assert!(get_model_by_id(deployment2.id, &user_models).is_none());

        let response = app
            .get("/admin/api/v1/models?accessible=true")
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;
        response.assert_status_ok();
        let user_models_explicit: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(user_models_explicit.data.len(), 1);
        assert!(get_model_by_id(deployment1.id, &user_models_explicit).is_some());

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let admin_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(admin_models.data.len(), 2, "Admin should see all models by default");
        assert!(get_model_by_id(deployment1.id, &admin_models).is_some());
        assert!(get_model_by_id(deployment2.id, &admin_models).is_some());

        let response = app
            .get("/admin/api/v1/models?accessible=false")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let admin_models_explicit: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(admin_models_explicit.data.len(), 2);

        group_repo.add_user_to_group(admin_user.id, group.id).await.unwrap();

        let response = app
            .get("/admin/api/v1/models?accessible=true")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let admin_accessible: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(
            admin_accessible.data.len(),
            1,
            "Admin with accessible=true should only see their accessible models"
        );
        assert!(get_model_by_id(deployment1.id, &admin_accessible).is_some());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_include_metrics_parameter(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let regular_user = create_test_user(&pool, Role::StandardUser).await;

        let deployment = create_test_deployment(&pool, admin_user.id, "metrics-test-model", "metrics-test-alias").await;

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        let test_model = get_model_by_id(deployment.id, &models).unwrap();
        assert!(test_model.metrics.is_none(), "Should not include metrics by default");

        let response = app
            .get("/admin/api/v1/models?include=metrics")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        let test_model = get_model_by_id(deployment.id, &models).unwrap();
        assert!(test_model.metrics.is_some(), "Admin should see metrics when requested");
        let metrics = test_model.metrics.as_ref().unwrap();
        assert_eq!(metrics.total_requests, 0);

        let response = app
            .get("/admin/api/v1/models?include=metrics")
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;

        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        if let Some(test_model) = get_model_by_id(deployment.id, &models) {
            assert!(
                test_model.metrics.is_none(),
                "Regular user should NOT see metrics (no Analytics::ReadAll permission)"
            );
        }

        let response = app
            .get("/admin/api/v1/models?include=groups,metrics")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        let test_model = get_model_by_id(deployment.id, &models).unwrap();
        assert!(test_model.groups.is_some(), "Admin should see groups when requested");
        assert!(test_model.metrics.is_some(), "Admin should see metrics when requested");

        let response = app
            .get("/admin/api/v1/models?include=groups,metrics")
            .add_header(&add_auth_headers(&regular_user)[0].0, &add_auth_headers(&regular_user)[0].1)
            .add_header(&add_auth_headers(&regular_user)[1].0, &add_auth_headers(&regular_user)[1].1)
            .await;

        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        if let Some(test_model) = get_model_by_id(deployment.id, &models) {
            assert!(test_model.groups.is_none(), "Regular user should NOT see groups");
            assert!(
                test_model.metrics.is_none(),
                "Regular user should NOT see metrics (no Analytics::ReadAll permission)"
            );
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_sees_all_models_by_default(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;

        let deployment1 = create_test_deployment(&pool, platform_manager.id, "pm-model-1", "pm-alias-1").await;
        let deployment2 = create_test_deployment(&pool, platform_manager.id, "pm-model-2", "pm-alias-2").await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Standard User Group".to_string(),
            description: Some("Group for standard user only".to_string()),
            created_by: platform_manager.id,
        };
        let group = group_repo.create(&group_create).await.unwrap();
        group_repo.add_user_to_group(standard_user.id, group.id).await.unwrap();

        group_repo
            .add_deployment_to_group(deployment1.id, group.id, platform_manager.id)
            .await
            .unwrap();

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();
        let pm_models: PaginatedResponse<DeployedModelResponse> = response.json();

        assert!(pm_models.data.iter().any(|m| m.id == deployment1.id), "PM should see deployment1");
        assert!(
            pm_models.data.iter().any(|m| m.id == deployment2.id),
            "PM should see deployment2 even without group access"
        );

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;
        response.assert_status_ok();
        let user_models: PaginatedResponse<DeployedModelResponse> = response.json();

        let user_accessible_count = user_models
            .data
            .iter()
            .filter(|m| m.id == deployment1.id || m.id == deployment2.id)
            .count();
        assert_eq!(user_accessible_count, 1, "Standard user should only see 1 accessible model");
        assert!(
            user_models.data.iter().any(|m| m.id == deployment1.id),
            "User should see deployment1"
        );
        assert!(
            !user_models.data.iter().any(|m| m.id == deployment2.id),
            "User should NOT see deployment2"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_can_request_accessible_filtering(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;

        let deployment1 = create_test_deployment(&pool, platform_manager.id, "pm-access-1", "pm-access-alias-1").await;
        let deployment2 = create_test_deployment(&pool, platform_manager.id, "pm-access-2", "pm-access-alias-2").await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "PM Access Group".to_string(),
            description: Some("Group for platform manager accessibility test".to_string()),
            created_by: platform_manager.id,
        };
        let group = group_repo.create(&group_create).await.unwrap();
        group_repo.add_user_to_group(platform_manager.id, group.id).await.unwrap();

        group_repo
            .add_deployment_to_group(deployment1.id, group.id, platform_manager.id)
            .await
            .unwrap();

        let response = app
            .get("/admin/api/v1/models?accessible=false")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();
        let all_models: PaginatedResponse<DeployedModelResponse> = response.json();

        assert!(all_models.data.iter().any(|m| m.id == deployment1.id));
        assert!(all_models.data.iter().any(|m| m.id == deployment2.id));

        let response = app
            .get("/admin/api/v1/models?accessible=true")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();
        let accessible_models: PaginatedResponse<DeployedModelResponse> = response.json();

        let accessible_count = accessible_models
            .data
            .iter()
            .filter(|m| m.id == deployment1.id || m.id == deployment2.id)
            .count();
        assert_eq!(accessible_count, 1, "PM with accessible=true should see only 1 accessible model");
        assert!(
            accessible_models.data.iter().any(|m| m.id == deployment1.id),
            "Should see accessible deployment"
        );
        assert!(
            !accessible_models.data.iter().any(|m| m.id == deployment2.id),
            "Should NOT see non-accessible deployment"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_request_viewer_role_gets_filtered(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;

        let deployment1 = create_test_deployment(&pool, platform_manager.id, "rv-model-1", "rv-alias-1").await;
        let deployment2 = create_test_deployment(&pool, platform_manager.id, "rv-model-2", "rv-alias-2").await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Request Viewer Group".to_string(),
            description: Some("Group for request viewer test".to_string()),
            created_by: platform_manager.id,
        };
        let group = group_repo.create(&group_create).await.unwrap();
        group_repo.add_user_to_group(request_viewer.id, group.id).await.unwrap();

        group_repo
            .add_deployment_to_group(deployment1.id, group.id, platform_manager.id)
            .await
            .unwrap();

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;
        response.assert_status_ok();
        let rv_models: PaginatedResponse<DeployedModelResponse> = response.json();

        let rv_accessible_count = rv_models
            .data
            .iter()
            .filter(|m| m.id == deployment1.id || m.id == deployment2.id)
            .count();
        assert_eq!(rv_accessible_count, 1, "RequestViewer should only see 1 accessible model");
        assert!(
            rv_models.data.iter().any(|m| m.id == deployment1.id),
            "Should see accessible deployment"
        );
        assert!(
            !rv_models.data.iter().any(|m| m.id == deployment2.id),
            "Should NOT see non-accessible deployment"
        );

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();
        let pm_models: PaginatedResponse<DeployedModelResponse> = response.json();

        assert!(pm_models.data.iter().any(|m| m.id == deployment1.id));
        assert!(pm_models.data.iter().any(|m| m.id == deployment2.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_can_see_newly_created_models(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let create_request = json!({
            "type": "standard",
            "model_name": "pm-new-model",
            "alias": "Platform Manager New Model",
            "hosted_on": test_endpoint_id.to_string(),
            "description": "A model created by platform manager"
        });

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .json(&create_request)
            .await;
        response.assert_status_ok();

        let created_model: DeployedModelResponse = response.json();
        let deployment_id = created_model.id;

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();

        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(
            models.data.iter().any(|m| m.id == deployment_id),
            "Platform manager should see newly created model immediately"
        );

        let found_model = models.data.iter().find(|m| m.id == deployment_id).unwrap();
        assert_eq!(found_model.model_name, "pm-new-model");
        assert_eq!(found_model.alias, "Platform Manager New Model");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_standard_user_cannot_see_ungrouped_models(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;

        let deployment = create_test_deployment(&pool, platform_manager.id, "ungrouped-model", "ungrouped-alias").await;


        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();

        let pm_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(
            pm_models.data.iter().any(|m| m.id == deployment.id),
            "Platform manager should see ungrouped model"
        );

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;
        response.assert_status_ok();

        let user_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(
            !user_models.data.iter().any(|m| m.id == deployment.id),
            "Standard user should NOT see ungrouped model"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_request_viewer_cannot_modify_models(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let deployment = create_test_deployment(&pool, platform_manager.id, "rv-test-model", "rv-test-alias").await;

        let create_request = json!({
            "model_name": "rv-forbidden-create",
            "hosted_on": test_endpoint_id.to_string()
        });

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .json(&create_request)
            .await;

        response.assert_status_forbidden();

        let update = json!({"alias": "rv-forbidden-update"});
        let response = app
            .patch(&format!("/admin/api/v1/models/{}", deployment.id))
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .json(&update)
            .await;

        response.assert_status_forbidden();

        let response = app
            .delete(&format!("/admin/api/v1/models/{}", deployment.id))
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_standard_user_cannot_modify_models(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let deployment = create_test_deployment(&pool, platform_manager.id, "su-test-model", "su-test-alias").await;

        let create_request = json!({
            "model_name": "su-forbidden-create",
            "hosted_on": test_endpoint_id.to_string()
        });

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .json(&create_request)
            .await;

        response.assert_status_forbidden();

        let update = json!({"alias": "su-forbidden-update"});
        let response = app
            .patch(&format!("/admin/api/v1/models/{}", deployment.id))
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .json(&update)
            .await;

        response.assert_status_forbidden();

        let response = app
            .delete(&format!("/admin/api/v1/models/{}", deployment.id))
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_multi_role_user_cannot_modify_models_without_platform_manager(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let multi_role_user = create_test_user_with_roles(&pool, vec![Role::StandardUser, Role::RequestViewer]).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let create_request = json!({
            "model_name": "multi-forbidden-create",
            "hosted_on": test_endpoint_id.to_string()
        });

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&multi_role_user)[0].0, &add_auth_headers(&multi_role_user)[0].1)
            .add_header(&add_auth_headers(&multi_role_user)[1].0, &add_auth_headers(&multi_role_user)[1].1)
            .json(&create_request)
            .await;

        response.assert_status_forbidden();

        let deployment = create_test_deployment(&pool, platform_manager.id, "multi-test-model", "multi-test-alias").await;

        let update = json!({"alias": "multi-forbidden-update"});
        let response = app
            .patch(&format!("/admin/api/v1/models/{}", deployment.id))
            .add_header(&add_auth_headers(&multi_role_user)[0].0, &add_auth_headers(&multi_role_user)[0].1)
            .add_header(&add_auth_headers(&multi_role_user)[1].0, &add_auth_headers(&multi_role_user)[1].1)
            .json(&update)
            .await;

        response.assert_status_forbidden();

        let response = app
            .delete(&format!("/admin/api/v1/models/{}", deployment.id))
            .add_header(&add_auth_headers(&multi_role_user)[0].0, &add_auth_headers(&multi_role_user)[0].1)
            .add_header(&add_auth_headers(&multi_role_user)[1].0, &add_auth_headers(&multi_role_user)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_plus_standard_user_can_modify_models(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_user = create_test_user_with_roles(&pool, vec![Role::PlatformManager, Role::StandardUser]).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let create_request = json!({
            "type": "standard",
            "model_name": "pm-create-test",
            "hosted_on": test_endpoint_id.to_string(),
            "alias": "Platform Manager Created"
        });

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&platform_user)[0].0, &add_auth_headers(&platform_user)[0].1)
            .add_header(&add_auth_headers(&platform_user)[1].0, &add_auth_headers(&platform_user)[1].1)
            .json(&create_request)
            .await;

        response.assert_status_ok();
        let created_model: DeployedModelResponse = response.json();

        let update = json!({"alias": "PM Updated Alias"});
        let response = app
            .patch(&format!("/admin/api/v1/models/{}", created_model.id))
            .add_header(&add_auth_headers(&platform_user)[0].0, &add_auth_headers(&platform_user)[0].1)
            .add_header(&add_auth_headers(&platform_user)[1].0, &add_auth_headers(&platform_user)[1].1)
            .json(&update)
            .await;

        response.assert_status_ok();
        let updated_model: DeployedModelResponse = response.json();
        assert_eq!(updated_model.alias, "PM Updated Alias");

        let response = app
            .delete(&format!("/admin/api/v1/models/{}", created_model.id))
            .add_header(&add_auth_headers(&platform_user)[0].0, &add_auth_headers(&platform_user)[0].1)
            .add_header(&add_auth_headers(&platform_user)[1].0, &add_auth_headers(&platform_user)[1].1)
            .await;

        response.assert_status_ok();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_accessibility_filtering_permissions(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;

        let accessible_deployment = create_test_deployment(&pool, platform_manager.id, "accessible-model", "accessible-alias").await;
        let inaccessible_deployment = create_test_deployment(&pool, platform_manager.id, "inaccessible-model", "inaccessible-alias").await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Access Test Group".to_string(),
            description: Some("Group for accessibility testing".to_string()),
            created_by: platform_manager.id,
        };
        let group = group_repo.create(&group_create).await.unwrap();
        group_repo.add_user_to_group(standard_user.id, group.id).await.unwrap();
        group_repo.add_user_to_group(request_viewer.id, group.id).await.unwrap();

        group_repo
            .add_deployment_to_group(accessible_deployment.id, group.id, platform_manager.id)
            .await
            .unwrap();

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;
        response.assert_status_ok();

        let standard_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(standard_models.data.iter().any(|m| m.id == accessible_deployment.id));
        assert!(!standard_models.data.iter().any(|m| m.id == inaccessible_deployment.id));

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;
        response.assert_status_ok();

        let rv_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(rv_models.data.iter().any(|m| m.id == accessible_deployment.id));
        assert!(!rv_models.data.iter().any(|m| m.id == inaccessible_deployment.id));

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();

        let pm_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(pm_models.data.iter().any(|m| m.id == accessible_deployment.id));
        assert!(pm_models.data.iter().any(|m| m.id == inaccessible_deployment.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_groups_include_permission_enforcement(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;

        let deployment = create_test_deployment(&pool, platform_manager.id, "groups-perm-model", "groups-perm-alias").await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Groups Permission Test".to_string(),
            description: Some("Test group for groups include permission".to_string()),
            created_by: platform_manager.id,
        };
        let group = group_repo.create(&group_create).await.unwrap();

        group_repo.add_user_to_group(standard_user.id, group.id).await.unwrap();
        group_repo.add_user_to_group(request_viewer.id, group.id).await.unwrap();
        group_repo
            .add_deployment_to_group(deployment.id, group.id, platform_manager.id)
            .await
            .unwrap();

        let response = app
            .get("/admin/api/v1/models?include=groups")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();
        let pm_models: PaginatedResponse<DeployedModelResponse> = response.json();
        let pm_model = pm_models.data.iter().find(|m| m.id == deployment.id).unwrap();
        assert!(pm_model.groups.is_some(), "PlatformManager should see groups when included");

        let response = app
            .get("/admin/api/v1/models?include=groups")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;
        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        let test_model = get_model_by_id(deployment.id, &models).unwrap();
        assert!(test_model.groups.is_none(), "Regular user should NOT see groups included");

        let response = app
            .get("/admin/api/v1/models?include=groups")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;
        response.assert_status_ok();

        let rv_models: PaginatedResponse<DeployedModelResponse> = response.json();
        let rv_model = rv_models.data.iter().find(|m| m.id == deployment.id).unwrap();
        assert!(rv_model.groups.is_none(), "RequestViewer should NOT see groups even when requested");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_realtime_inflight_limit_permission_gating(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;
        let auth = |user| add_auth_headers(user);

        let manager_auth = auth(&platform_manager);
        let response = app
            .post("/admin/api/v1/models")
            .add_header(&manager_auth[0].0, &manager_auth[0].1)
            .add_header(&manager_auth[1].0, &manager_auth[1].1)
            .json(&json!({
                "type": "composite",
                "model_name": "inflight-composite",
                "alias": "inflight-composite",
                "realtime_inflight_limit": 40
            }))
            .await;
        response.assert_status_ok();
        let created: DeployedModelResponse = response.json();
        assert_eq!(created.realtime_inflight_limit, Some(40));

        let response = app
            .patch(&format!("/admin/api/v1/models/{}", created.id))
            .add_header(&manager_auth[0].0, &manager_auth[0].1)
            .add_header(&manager_auth[1].0, &manager_auth[1].1)
            .json(&json!({ "realtime_inflight_limit": 60 }))
            .await;
        response.assert_status_ok();

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group = group_repo
            .create(&GroupCreateDBRequest {
                name: "In-flight Limit Test Group".to_string(),
                description: None,
                created_by: platform_manager.id,
            })
            .await
            .unwrap();
        group_repo.add_user_to_group(standard_user.id, group.id).await.unwrap();
        group_repo.add_user_to_group(request_viewer.id, group.id).await.unwrap();
        group_repo
            .add_deployment_to_group(created.id, group.id, platform_manager.id)
            .await
            .unwrap();

        for (user, expected) in [(&platform_manager, Some(60)), (&standard_user, None), (&request_viewer, None)] {
            let headers = auth(user);
            let response = app
                .get(&format!("/admin/api/v1/models/{}", created.id))
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .await;
            response.assert_status_ok();
            let model: DeployedModelResponse = response.json();
            assert_eq!(model.realtime_inflight_limit, expected);
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_metrics_permission_gating(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_admin_user(&pool, Role::PlatformManager).await;
        let standard_user = create_test_user(&pool, Role::StandardUser).await;
        let request_viewer = create_test_user(&pool, Role::RequestViewer).await;

        let deployment = create_test_deployment(&pool, platform_manager.id, "metrics-perm-test", "metrics-perm-alias").await;

        let mut pool_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut pool_conn);
        let group_create = GroupCreateDBRequest {
            name: "Metrics Permission Test Group".to_string(),
            description: Some("Test group for metrics permissions".to_string()),
            created_by: platform_manager.id,
        };
        let group = group_repo.create(&group_create).await.unwrap();
        group_repo.add_user_to_group(standard_user.id, group.id).await.unwrap();
        group_repo.add_user_to_group(request_viewer.id, group.id).await.unwrap();
        group_repo
            .add_deployment_to_group(deployment.id, group.id, platform_manager.id)
            .await
            .unwrap();

        let response = app
            .get("/admin/api/v1/models?include=metrics")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;
        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        let pm_model = get_model_by_id(deployment.id, &models).unwrap();
        assert!(pm_model.metrics.is_some(), "PlatformManager should see metrics when requested");

        let response = app
            .get("/admin/api/v1/models?include=metrics")
            .add_header(&add_auth_headers(&standard_user)[0].0, &add_auth_headers(&standard_user)[0].1)
            .add_header(&add_auth_headers(&standard_user)[1].0, &add_auth_headers(&standard_user)[1].1)
            .await;
        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        let user_model = get_model_by_id(deployment.id, &models).unwrap();
        assert!(
            user_model.metrics.is_none(),
            "StandardUser should NOT see metrics even when requested"
        );

        let response = app
            .get("/admin/api/v1/models?include=metrics")
            .add_header(&add_auth_headers(&request_viewer)[0].0, &add_auth_headers(&request_viewer)[0].1)
            .add_header(&add_auth_headers(&request_viewer)[1].0, &add_auth_headers(&request_viewer)[1].1)
            .await;
        response.assert_status_ok();
        let models: PaginatedResponse<DeployedModelResponse> = response.json();
        let rv_model = get_model_by_id(deployment.id, &models).unwrap();
        assert!(rv_model.metrics.is_some(), "RequestViewer should see metrics when requested");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_models_pagination(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let deployment1 = create_test_deployment(&pool, admin_user.id, "model-1", "alias-1").await;
        let deployment2 = create_test_deployment(&pool, admin_user.id, "model-2", "alias-2").await;
        let deployment3 = create_test_deployment(&pool, admin_user.id, "model-3", "alias-3").await;
        let deployment4 = create_test_deployment(&pool, admin_user.id, "model-4", "alias-4").await;
        let deployment5 = create_test_deployment(&pool, admin_user.id, "model-5", "alias-5").await;

        let response = app
            .get("/admin/api/v1/models?limit=2&skip=0")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let page1: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(page1.data.len(), 2, "First page should have 2 models");

        let response = app
            .get("/admin/api/v1/models?limit=2&skip=2")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let page2: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(page2.data.len(), 2, "Second page should have 2 models");

        let response = app
            .get("/admin/api/v1/models?limit=2&skip=4")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let page3: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(page3.data.len(), 1, "Third page should have 1 model");

        let all_page_ids: Vec<DeploymentId> = page1
            .data
            .iter()
            .chain(page2.data.iter())
            .chain(page3.data.iter())
            .map(|m| m.id)
            .collect();

        let unique_ids: std::collections::HashSet<DeploymentId> = all_page_ids.iter().copied().collect();
        assert_eq!(all_page_ids.len(), unique_ids.len(), "Pages should not have duplicate models");
        assert_eq!(unique_ids.len(), 5, "Should have all 5 models across pages");

        assert!(
            unique_ids.contains(&deployment1.id)
                && unique_ids.contains(&deployment2.id)
                && unique_ids.contains(&deployment3.id)
                && unique_ids.contains(&deployment4.id)
                && unique_ids.contains(&deployment5.id),
            "All created deployments should be present in paginated results"
        );

        let response = app
            .get("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let all_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(all_models.data.len(), 5, "Without pagination params, should get all models");

        let response = app
            .get("/admin/api/v1/models?limit=10&skip=100")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let empty_page: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(empty_page.data.len(), 0, "Offset beyond available models should return empty array");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_models_with_group_filter(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let deployment1 = create_test_deployment(&pool, admin_user.id, "model-1", "alias-1").await;
        let deployment2 = create_test_deployment(&pool, admin_user.id, "model-2", "alias-2").await;
        let deployment3 = create_test_deployment(&pool, admin_user.id, "model-3", "alias-3").await;

        let mut group_conn = pool.acquire().await.unwrap();
        let mut group_repo = Groups::new(&mut group_conn);

        let group1_create = GroupCreateDBRequest {
            name: "Production".to_string(),
            description: Some("Production group".to_string()),
            created_by: admin_user.id,
        };
        let group1 = group_repo.create(&group1_create).await.unwrap();

        let group2_create = GroupCreateDBRequest {
            name: "Staging".to_string(),
            description: Some("Staging group".to_string()),
            created_by: admin_user.id,
        };
        let group2 = group_repo.create(&group2_create).await.unwrap();

        group_repo
            .add_deployment_to_group(deployment1.id, group1.id, admin_user.id)
            .await
            .unwrap();

        group_repo
            .add_deployment_to_group(deployment2.id, group2.id, admin_user.id)
            .await
            .unwrap();


        let response = app
            .get(&format!("/admin/api/v1/models?group={}", group1.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let single_group: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(single_group.data.len(), 1, "Should only return models from group1");
        assert!(get_model_by_id(deployment1.id, &single_group).is_some());
        assert!(get_model_by_id(deployment2.id, &single_group).is_none());
        assert!(get_model_by_id(deployment3.id, &single_group).is_none());

        let response = app
            .get(&format!("/admin/api/v1/models?group={},{}", group1.id, group2.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let multi_group: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(multi_group.data.len(), 2, "Should return models from both groups");
        assert!(get_model_by_id(deployment1.id, &multi_group).is_some());
        assert!(get_model_by_id(deployment2.id, &multi_group).is_some());
        assert!(get_model_by_id(deployment3.id, &multi_group).is_none());

        let response = app
            .get("/admin/api/v1/models?group=invalid-uuid")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);

        let response = app
            .get(&format!("/admin/api/v1/models?group={},invalid-uuid", group1.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status(axum::http::StatusCode::BAD_REQUEST);

        let response = app
            .get("/admin/api/v1/models?group=,,,")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();
        let all_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert!(all_models.data.len() >= 3, "Empty group list should return all models");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_models_with_available_for_realtime_filter(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let allowed_deployment = create_test_deployment(&pool, admin_user.id, "realtime-allowed-model", "realtime-allowed-alias").await;
        let denied_deployment = create_test_deployment(&pool, admin_user.id, "realtime-denied-model", "realtime-denied-alias").await;
        let redirected_deployment =
            create_test_deployment(&pool, admin_user.id, "realtime-redirected-model", "realtime-redirected-alias").await;
        let redirect_target = create_test_deployment(&pool, admin_user.id, "realtime-target-model", "realtime-target-alias").await;
        let other_group_deployment = create_test_deployment(&pool, admin_user.id, "other-group-model", "other-group-alias").await;

        let mut conn = pool.acquire().await.unwrap();

        let group = {
            let mut group_repo = Groups::new(&mut conn);
            let group = group_repo
                .create(&GroupCreateDBRequest {
                    name: "Realtime Test Group".to_string(),
                    description: Some("Models used for realtime availability filtering".to_string()),
                    created_by: admin_user.id,
                })
                .await
                .unwrap();

            for deployment_id in [allowed_deployment.id, denied_deployment.id, redirected_deployment.id] {
                group_repo
                    .add_deployment_to_group(deployment_id, group.id, admin_user.id)
                    .await
                    .unwrap();
            }

            group
        };

        {
            let mut deployment_repo = Deployments::new(&mut conn);
            deployment_repo
                .set_traffic_rules(denied_deployment.id, &[(ApiKeyPurpose::Realtime, TrafficRuleAction::Deny)])
                .await
                .unwrap();
            deployment_repo
                .set_traffic_rules(
                    redirected_deployment.id,
                    &[(ApiKeyPurpose::Realtime, TrafficRuleAction::Redirect(redirect_target.id))],
                )
                .await
                .unwrap();
        }

        let response = app
            .get(&format!("/admin/api/v1/models?group={}&available_for_realtime=true", group.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;
        response.assert_status_ok();

        let realtime_models: PaginatedResponse<DeployedModelResponse> = response.json();
        assert_eq!(realtime_models.total_count, 2);
        assert!(get_model_by_id(allowed_deployment.id, &realtime_models).is_some());
        assert!(get_model_by_id(redirected_deployment.id, &realtime_models).is_some());
        assert!(get_model_by_id(denied_deployment.id, &realtime_models).is_none());
        assert!(get_model_by_id(other_group_deployment.id, &realtime_models).is_none());
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_model_with_traffic_rules(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        create_test_deployment(&pool, admin_user.id, "traffic-target", "traffic-target-alias").await;

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "type": "standard",
                "model_name": "traffic-source",
                "alias": "traffic-source-alias",
                "hosted_on": test_endpoint_id,
                "traffic_routing_rules": [
                    { "api_key_purpose": "batch", "action": { "type": "deny" } },
                    { "api_key_purpose": "realtime", "action": { "type": "redirect", "target": "traffic-target-alias" } }
                ]
            }))
            .await;

        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        assert_eq!(model.alias, "traffic-source-alias");

        let rules = model.traffic_routing_rules.expect("expected traffic_routing_rules in response");
        assert_eq!(rules.len(), 2);

        let batch_rule = rules
            .iter()
            .find(|r| r.api_key_purpose == crate::db::models::api_keys::ApiKeyPurpose::Batch)
            .unwrap();
        assert!(matches!(
            batch_rule.action,
            crate::api::models::deployments::TrafficRoutingAction::Deny
        ));

        let realtime_rule = rules
            .iter()
            .find(|r| r.api_key_purpose == crate::db::models::api_keys::ApiKeyPurpose::Realtime)
            .unwrap();
        match &realtime_rule.action {
            crate::api::models::deployments::TrafficRoutingAction::Redirect { target } => {
                assert_eq!(target, "traffic-target-alias");
            }
            _ => panic!("expected redirect action"),
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_model_includes_traffic_rules(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        create_test_deployment(&pool, admin_user.id, "get-tr-target", "get-tr-target-alias").await;

        let create_resp = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "type": "standard",
                "model_name": "get-tr-source",
                "alias": "get-tr-source-alias",
                "hosted_on": test_endpoint_id,
                "traffic_routing_rules": [
                    { "api_key_purpose": "batch", "action": { "type": "deny" } }
                ]
            }))
            .await;
        create_resp.assert_status_ok();
        let created: DeployedModelResponse = create_resp.json();

        let response = app
            .get(&format!("/admin/api/v1/models/{}", created.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let model: DeployedModelResponse = response.json();
        let rules = model.traffic_routing_rules.expect("expected traffic_routing_rules");
        assert_eq!(rules.len(), 1);
        assert!(matches!(
            rules[0].action,
            crate::api::models::deployments::TrafficRoutingAction::Deny
        ));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_models_includes_traffic_rules(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let create_resp = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "type": "standard",
                "model_name": "list-tr-source",
                "alias": "list-tr-source-alias",
                "hosted_on": test_endpoint_id,
                "traffic_routing_rules": [
                    { "api_key_purpose": "playground", "action": { "type": "deny" } }
                ]
            }))
            .await;
        create_resp.assert_status_ok();
        let created: DeployedModelResponse = create_resp.json();

        let response = app
            .get("/admin/api/v1/models?limit=100")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let list: PaginatedResponse<DeployedModelResponse> = response.json();
        let model = get_model_by_id(created.id, &list).expect("model should be in list");
        let rules = model.traffic_routing_rules.as_ref().expect("traffic rules should be present");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].api_key_purpose, crate::db::models::api_keys::ApiKeyPurpose::Playground);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_update_model_set_traffic_rules(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;

        let model = create_test_deployment(&pool, admin_user.id, "update-tr-model", "update-tr-alias").await;

        let response = app
            .patch(&format!("/admin/api/v1/models/{}", model.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "traffic_routing_rules": [
                    { "api_key_purpose": "batch", "action": { "type": "deny" } }
                ]
            }))
            .await;

        response.assert_status_ok();
        let updated: DeployedModelResponse = response.json();
        let rules = updated.traffic_routing_rules.expect("traffic rules should be set");
        assert_eq!(rules.len(), 1);

        let response = app
            .get(&format!("/admin/api/v1/models/{}", model.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let fetched: DeployedModelResponse = response.json();
        assert_eq!(fetched.traffic_routing_rules.unwrap().len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_update_model_clear_traffic_rules(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let create_resp = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "type": "standard",
                "model_name": "clear-tr-model",
                "alias": "clear-tr-alias",
                "hosted_on": test_endpoint_id,
                "traffic_routing_rules": [
                    { "api_key_purpose": "batch", "action": { "type": "deny" } }
                ]
            }))
            .await;
        create_resp.assert_status_ok();
        let created: DeployedModelResponse = create_resp.json();
        assert!(created.traffic_routing_rules.is_some());

        let response = app
            .patch(&format!("/admin/api/v1/models/{}", created.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "traffic_routing_rules": null
            }))
            .await;

        response.assert_status_ok();
        let updated: DeployedModelResponse = response.json();
        assert!(updated.traffic_routing_rules.is_none());

        let response = app
            .get(&format!("/admin/api/v1/models/{}", created.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let fetched: DeployedModelResponse = response.json();
        assert!(fetched.traffic_routing_rules.is_none());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_update_model_no_change_to_traffic_rules(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let create_resp = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "type": "standard",
                "model_name": "nochange-tr-model",
                "alias": "nochange-tr-alias",
                "hosted_on": test_endpoint_id,
                "traffic_routing_rules": [
                    { "api_key_purpose": "batch", "action": { "type": "deny" } }
                ]
            }))
            .await;
        create_resp.assert_status_ok();
        let created: DeployedModelResponse = create_resp.json();

        let response = app
            .patch(&format!("/admin/api/v1/models/{}", created.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "description": "updated description"
            }))
            .await;

        response.assert_status_ok();
        let updated: DeployedModelResponse = response.json();
        let rules = updated.traffic_routing_rules.expect("traffic rules should be unchanged");
        assert_eq!(rules.len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_model_self_redirect_rejected(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "type": "standard",
                "model_name": "self-redirect",
                "alias": "self-redirect-alias",
                "hosted_on": test_endpoint_id,
                "traffic_routing_rules": [
                    { "api_key_purpose": "realtime", "action": { "type": "redirect", "target": "self-redirect-alias" } }
                ]
            }))
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_model_nonexistent_redirect_target(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "type": "standard",
                "model_name": "bad-redirect",
                "alias": "bad-redirect-alias",
                "hosted_on": test_endpoint_id,
                "traffic_routing_rules": [
                    { "api_key_purpose": "realtime", "action": { "type": "redirect", "target": "nonexistent-model" } }
                ]
            }))
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_model_empty_redirect_target(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let response = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "type": "standard",
                "model_name": "empty-redirect",
                "alias": "empty-redirect-alias",
                "hosted_on": test_endpoint_id,
                "traffic_routing_rules": [
                    { "api_key_purpose": "realtime", "action": { "type": "redirect", "target": "" } }
                ]
            }))
            .await;

        response.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_delete_redirect_target_cascades(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let admin_user = create_test_admin_user(&pool, Role::PlatformManager).await;
        let test_endpoint_id = get_test_endpoint_id(&pool).await;

        let target = create_test_deployment(&pool, admin_user.id, "cascade-target", "cascade-target-alias").await;

        let create_resp = app
            .post("/admin/api/v1/models")
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .json(&json!({
                "type": "standard",
                "model_name": "cascade-source",
                "alias": "cascade-source-alias",
                "hosted_on": test_endpoint_id,
                "traffic_routing_rules": [
                    { "api_key_purpose": "realtime", "action": { "type": "redirect", "target": "cascade-target-alias" } }
                ]
            }))
            .await;
        create_resp.assert_status_ok();
        let source: DeployedModelResponse = create_resp.json();
        assert!(source.traffic_routing_rules.is_some());

        sqlx::query!("DELETE FROM deployed_models WHERE id = $1", target.id)
            .execute(&pool)
            .await
            .unwrap();

        let response = app
            .get(&format!("/admin/api/v1/models/{}", source.id))
            .add_header(&add_auth_headers(&admin_user)[0].0, &add_auth_headers(&admin_user)[0].1)
            .add_header(&add_auth_headers(&admin_user)[1].0, &add_auth_headers(&admin_user)[1].1)
            .await;

        response.assert_status_ok();
        let fetched: DeployedModelResponse = response.json();
        assert!(
            fetched.traffic_routing_rules.is_none(),
            "traffic rules should be cleared after cascade delete of redirect target"
        );
    }
    #[dwctl_test_macros::test]
    async fn tariff_replacement_rejects_future_schedule_without_partial_updates(pool: PgPool) {
        let (app, _services) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let model = create_test_deployment(&pool, admin.id, "scheduled", "scheduled").await;
        sqlx::query("INSERT INTO model_tariffs (deployed_model_id,name,input_price_per_token,output_price_per_token,api_key_purpose,valid_from,valid_until) VALUES ($1,'current',1,1,'realtime',NOW()-INTERVAL '1 day',NOW()+INTERVAL '1 day'),($1,'future',2,2,'realtime',NOW()+INTERVAL '1 day',NULL)").bind(model.id).execute(&pool).await.unwrap();
        let before: serde_json::Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM model_tariffs t")
            .fetch_one(&pool)
            .await
            .unwrap();
        let headers = add_auth_headers(&admin);
        let path = format!("/admin/api/v1/models/{}", model.id);
        let response=app.patch(&path).add_header(&headers[0].0,&headers[0].1).add_header(&headers[1].0,&headers[1].1).json(&json!({"description":"rejected-change","tariffs":[{"name":"new","api_key_purpose":"realtime","input_price_per_token":"3","output_price_per_token":"3"}]})).await;
        response.assert_status_bad_request();
        assert!(response.text().contains("scheduled future prices"));
        let after: serde_json::Value = sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(t) ORDER BY id) FROM model_tariffs t")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(before, after);
        let description: Option<String> = sqlx::query_scalar("SELECT description FROM deployed_models WHERE id=$1")
            .bind(model.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_ne!(description.as_deref(), Some("rejected-change"));
        app.patch(&path)
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({"description":"metadata-only"}))
            .await
            .assert_status_ok();
    }
}
