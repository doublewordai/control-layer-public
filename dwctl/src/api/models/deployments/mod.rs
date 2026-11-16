
pub mod enrichment;

use super::pagination::Pagination;
use crate::api::models::cache_pricing::CachePricingResponse;
use crate::api::models::groups::GroupResponse;
use crate::db::models::api_keys::ApiKeyPurpose;
use crate::db::models::deployments::{
    AffinityConfig, AimdConfig, BackoffConfig, DeploymentDBResponse, FallbackConfig, JitterStrategy, LoadBalancingStrategy,
    ModelCatalogMetadata, ModelType, ProviderPricing, ProviderPricingUpdate, TrafficRuleDBRow,
};
use crate::reasoning::{ReasoningTranslationOverrides, SupportedReasoningEfforts};
use crate::types::{DeploymentId, InferenceEndpointId, UserId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_with::rust::double_option;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ModelSortField {
    CreatedAt,
    Alias,
    IntelligenceIndex,
    ReleasedAt,
    ContextWindow,
    Provider,
    PriceFrom,
}

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ModelFacets {
    pub providers: Vec<String>,
    pub capabilities: Vec<String>,
    pub model_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ModelListResponse {
    pub data: Vec<DeployedModelResponse>,
    pub total_count: i64,
    pub skip: i64,
    pub limit: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<ModelFacets>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListModelsQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,
    #[param(value_type = Option<String>, format = "uuid")]
    #[schema(value_type = Option<String>, format = "uuid")]
    pub endpoint: Option<InferenceEndpointId>,
    pub group: Option<String>,
    pub include: Option<String>,
    pub deleted: Option<bool>,
    pub inactive: Option<bool>,
    pub accessible: Option<bool>,
    pub search: Option<String>,
    pub is_composite: Option<bool>,
    pub provider: Option<String>,
    pub model_type: Option<ModelType>,
    pub capability: Option<String>,
    pub available_for_realtime: Option<bool>,
    pub sort: Option<ModelSortField>,
    pub sort_direction: Option<SortDirection>,
}

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct GetModelQuery {
    pub deleted: Option<bool>,
    pub inactive: Option<bool>,
    pub include: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ModelTimeSeriesPoint {
    pub timestamp: DateTime<Utc>,
    pub requests: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ModelMetrics {
    pub avg_latency_ms: Option<f64>,
    pub total_requests: i64,
    pub total_input_tokens: i64,
    pub total_output_tokens: i64,
    pub last_active_at: Option<DateTime<Utc>>,
    pub time_series: Option<Vec<ModelTimeSeriesPoint>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TariffDefinition {
    pub name: String,
    #[schema(value_type = String)]
    pub input_price_per_token: rust_decimal::Decimal,
    #[schema(value_type = String)]
    pub output_price_per_token: rust_decimal::Decimal,
    pub api_key_purpose: Option<crate::db::models::api_keys::ApiKeyPurpose>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_window: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TrafficRoutingRule {
    pub api_key_purpose: crate::db::models::api_keys::ApiKeyPurpose,
    pub action: TrafficRoutingAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum TrafficRoutingAction {
    Deny,
    Redirect {
        target: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeployedModelCreate {
    Standard(StandardModelCreate),
    Composite(CompositeModelCreate),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StandardModelCreate {
    pub model_name: String,
    pub alias: Option<String>,
    pub display_name: Option<String>,
    #[schema(value_type = String, format = "uuid")]
    pub hosted_on: InferenceEndpointId,
    pub description: Option<String>,
    pub model_type: Option<ModelType>,
    pub capabilities: Option<Vec<String>>,
    pub batch_capacity: Option<i32>,
    pub throughput: Option<f32>,
    pub provider_pricing: Option<ProviderPricing>,
    pub tariffs: Option<Vec<TariffDefinition>>,
    #[serde(default)]
    pub sanitize_responses: Option<bool>,
    #[serde(default)]
    pub trusted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_translation_overrides: Option<ReasoningTranslationOverrides>,
    #[serde(default)]
    pub backoff_enabled: bool,
    #[serde(default = "default_backoff_initial_ms")]
    pub backoff_initial_ms: i32,
    #[serde(default = "default_backoff_max_ms")]
    pub backoff_max_ms: i32,
    #[serde(default = "default_backoff_factor")]
    pub backoff_factor: f64,
    #[serde(default)]
    pub backoff_jitter: JitterStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_max_total_ms: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_token_timeout_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aimd: Option<AimdConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_routing_rules: Option<Vec<TrafficRoutingRule>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_batch_completion_windows: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ModelCatalogMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CompositeModelCreate {
    pub model_name: String,
    pub alias: Option<String>,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub model_type: Option<ModelType>,
    pub capabilities: Option<Vec<String>>,
    pub realtime_inflight_limit: Option<i32>,
    pub batch_capacity: Option<i32>,
    pub throughput: Option<f32>,
    pub tariffs: Option<Vec<TariffDefinition>>,
    #[serde(default)]
    pub lb_strategy: LoadBalancingStrategy,
    #[serde(default = "default_true")]
    pub fallback_enabled: bool,
    #[serde(default = "default_true")]
    pub fallback_on_rate_limit: bool,
    #[serde(default = "default_fallback_statuses")]
    pub fallback_on_status: Vec<i32>,
    #[serde(default = "default_realtime_fallback_statuses")]
    pub fallback_realtime_on_status: Vec<i32>,
    #[serde(default)]
    pub fallback_with_replacement: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_max_attempts: Option<i32>,
    #[serde(default)]
    pub backoff_enabled: bool,
    #[serde(default = "default_backoff_initial_ms")]
    pub backoff_initial_ms: i32,
    #[serde(default = "default_backoff_max_ms")]
    pub backoff_max_ms: i32,
    #[serde(default = "default_backoff_factor")]
    pub backoff_factor: f64,
    #[serde(default)]
    pub backoff_jitter: JitterStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_max_total_ms: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_token_timeout_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aimd: Option<AimdConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<AffinityConfig>,
    #[serde(default)]
    pub sanitize_responses: bool,
    #[serde(default)]
    pub trusted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_routing_rules: Option<Vec<TrafficRoutingRule>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_batch_completion_windows: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ModelCatalogMetadata>,
}

fn default_true() -> bool {
    true
}

fn default_fallback_statuses() -> Vec<i32> {
    vec![499, 500, 502, 503, 504]
}

fn default_realtime_fallback_statuses() -> Vec<i32> {
    vec![529]
}

fn default_backoff_initial_ms() -> i32 {
    100
}

fn default_backoff_max_ms() -> i32 {
    5_000
}

fn default_backoff_factor() -> f64 {
    2.0
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeployedModelUpdate {
    pub alias: Option<String>,
    pub display_name: Option<String>,
    pub description: Option<Option<String>>,
    pub model_type: Option<Option<ModelType>>,
    pub capabilities: Option<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub realtime_inflight_limit: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub batch_capacity: Option<Option<i32>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub throughput: Option<Option<f32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_pricing: Option<ProviderPricingUpdate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tariffs: Option<Vec<TariffDefinition>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lb_strategy: Option<LoadBalancingStrategy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_on_rate_limit: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_on_status: Option<Vec<i32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_realtime_on_status: Option<Vec<i32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_with_replacement: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub fallback_max_attempts: Option<Option<i32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_initial_ms: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_max_ms: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_factor: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_jitter: Option<JitterStrategy>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub backoff_max_total_ms: Option<Option<i32>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub first_token_timeout_ms: Option<Option<i64>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub aimd: Option<Option<AimdConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub affinity: Option<Option<AffinityConfig>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sanitize_responses: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub reasoning_translation_overrides: Option<Option<ReasoningTranslationOverrides>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub traffic_routing_rules: Option<Option<Vec<TrafficRoutingRule>>>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "double_option")]
    pub allowed_batch_completion_windows: Option<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ModelCatalogMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeployedModelUpdateRequest {
    #[schema(value_type = String, format = "uuid")]
    pub id: DeploymentId,
    pub deployed_model: DeployedModelUpdate,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ModelProbeStatus {
    #[schema(value_type = Option<String>, format = "uuid")]
    pub probe_id: Option<Uuid>,
    pub active: bool,
    pub interval_seconds: Option<i32>,
    pub last_check: Option<DateTime<Utc>>,
    pub last_success: Option<bool>,
    pub uptime_percentage: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DeployedModelResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: DeploymentId,
    pub model_name: String,
    pub alias: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub model_type: Option<ModelType>,
    pub capabilities: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, format = "uuid")]
    pub created_by: Option<UserId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, format = "uuid")]
    pub hosted_on: Option<InferenceEndpointId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub realtime_inflight_limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_capacity: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(no_recursion)]
    pub groups: Option<Vec<GroupResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<ModelMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ModelProbeStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_pricing: Option<ProviderPricing>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_pricing: Option<CachePricingResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_pricing_by_class: Option<std::collections::BTreeMap<String, CachePricingResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<super::inference_endpoints::InferenceEndpointResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tariffs: Option<Vec<super::tariffs::TariffResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_composite: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lb_strategy: Option<LoadBalancingStrategy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback: Option<FallbackConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub components: Option<Vec<ModelComponentResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sanitize_responses: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trusted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_translation_overrides: Option<ReasoningTranslationOverrides>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported_reasoning_efforts: Option<SupportedReasoningEfforts>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traffic_routing_rules: Option<Vec<TrafficRoutingRule>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_batch_completion_windows: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ModelCatalogMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provisioning_source: Option<String>,
}

impl From<DeploymentDBResponse> for DeployedModelResponse {
    fn from(db: DeploymentDBResponse) -> Self {
        let backoff = db.backoff_enabled.then_some(BackoffConfig {
            initial_ms: db.backoff_initial_ms,
            max_ms: db.backoff_max_ms,
            factor: db.backoff_factor,
            jitter: match db.backoff_jitter.as_str() {
                "none" => JitterStrategy::None,
                _ => JitterStrategy::Full,
            },
        });
        let fallback = Some(FallbackConfig {
            enabled: db.fallback_enabled,
            on_rate_limit: db.fallback_on_rate_limit,
            on_status: db.fallback_on_status,
            realtime_on_status: db.fallback_realtime_on_status,
            with_replacement: db.fallback_with_replacement,
            max_attempts: db.fallback_max_attempts,
            backoff,
            max_total_backoff_ms: db.backoff_max_total_ms,
            first_token_timeout_ms: db.first_token_timeout_ms,
            aimd: db.aimd,
            affinity: db.affinity,
        });

        Self {
            id: db.id,
            model_name: db.model_name,
            alias: db.alias,
            display_name: db.display_name,
            description: db.description,
            model_type: db.model_type,
            capabilities: db.capabilities,
            created_by: Some(db.created_by),
            hosted_on: db.hosted_on,
            created_at: db.created_at,
            updated_at: db.updated_at,
            realtime_inflight_limit: db.is_composite.then_some(db.realtime_inflight_limit),
            batch_capacity: db.batch_capacity,
            throughput: db.throughput,
            groups: None,
            metrics: None,
            status: None,
            provider_pricing: None,
            cache_pricing_by_class: None,
            cache_pricing: None,
            endpoint: None,
            tariffs: None,
            is_composite: Some(db.is_composite),
            lb_strategy: if db.is_composite { Some(db.lb_strategy) } else { None },
            fallback,
            components: None,
            sanitize_responses: Some(db.sanitize_responses),
            trusted: Some(db.trusted),
            reasoning_translation_overrides: if db.is_composite {
                None
            } else {
                Some(db.reasoning_translation_overrides.unwrap_or_default())
            },
            supported_reasoning_efforts: None,
            traffic_routing_rules: None,
            allowed_batch_completion_windows: db.allowed_batch_completion_windows,
            metadata: serde_json::from_value::<ModelCatalogMetadata>(db.metadata)
                .inspect_err(|e| tracing::warn!(error = %e, "failed to deserialize model metadata"))
                .ok()
                .filter(|m| *m != ModelCatalogMetadata::default()),
            provisioning_source: db.provisioning_source,
        }
    }
}

impl DeployedModelResponse {
    pub fn with_groups(mut self, groups: Vec<GroupResponse>) -> Self {
        self.groups = Some(groups);
        self
    }

    pub fn with_metrics(mut self, metrics: ModelMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_status(mut self, status: ModelProbeStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn with_provider_pricing(mut self, provider_pricing: Option<ProviderPricing>) -> Self {
        self.provider_pricing = provider_pricing;
        self
    }

    pub fn with_cache_pricing(mut self, cache_pricing: CachePricingResponse) -> Self {
        self.cache_pricing = Some(cache_pricing);
        self
    }

    pub fn with_supported_reasoning_efforts(mut self, supported_reasoning_efforts: Option<SupportedReasoningEfforts>) -> Self {
        self.supported_reasoning_efforts = supported_reasoning_efforts;
        self
    }

    pub fn mask_rate_limiting(mut self) -> Self {
        self.realtime_inflight_limit = None;
        self
    }

    pub fn mask_capacity(mut self) -> Self {
        self.batch_capacity = None;
        self.throughput = None;
        self
    }

    pub fn mask_created_by(mut self) -> Self {
        self.created_by = None;
        self
    }

    pub fn mask_composite_fields(mut self) -> Self {
        self.is_composite = None;
        self.lb_strategy = None;
        self.fallback = None;
        self.components = None;
        self
    }

    pub fn mask_response_config(mut self) -> Self {
        self.sanitize_responses = None;
        self.trusted = None;
        self.reasoning_translation_overrides = None;
        self
    }

    pub fn filter_disabled_batch_tariffs(mut self) -> Self {
        if let Some(ref allowed) = self.allowed_batch_completion_windows
            && let Some(ref mut tariffs) = self.tariffs
        {
            tariffs.retain(|t| match (&t.api_key_purpose, &t.completion_window) {
                (Some(ApiKeyPurpose::Batch), Some(window)) => allowed.contains(window),
                (Some(ApiKeyPurpose::Batch), None) => !allowed.is_empty(),
                _ => true,
            });
        }
        self
    }

    pub fn filter_denied_purpose_tariffs(mut self) -> Self {
        if let (Some(rules), Some(tariffs)) = (self.traffic_routing_rules.as_ref(), self.tariffs.as_mut()) {
            let denied: Vec<&ApiKeyPurpose> = rules
                .iter()
                .filter(|r| matches!(r.action, TrafficRoutingAction::Deny))
                .map(|r| &r.api_key_purpose)
                .collect();
            if !denied.is_empty() {
                tariffs.retain(|t| match &t.api_key_purpose {
                    Some(purpose) => !denied.contains(&purpose),
                    None => true,
                });
            }
        }
        self
    }

    pub fn with_endpoint(mut self, endpoint: super::inference_endpoints::InferenceEndpointResponse) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn with_tariffs(mut self, tariffs: Vec<super::tariffs::TariffResponse>) -> Self {
        self.tariffs = Some(tariffs);
        self
    }

    pub fn with_components(mut self, components: Vec<ModelComponentResponse>) -> Self {
        self.components = Some(components);
        self
    }

    pub fn with_traffic_rules(mut self, rules: Vec<TrafficRuleDBRow>) -> Self {
        self.traffic_routing_rules = if rules.is_empty() {
            None
        } else {
            Some(
                rules
                    .into_iter()
                    .filter_map(|r| {
                        let purpose: crate::db::models::api_keys::ApiKeyPurpose =
                            serde_json::from_value(serde_json::Value::String(r.api_key_purpose)).ok()?;
                        let action = match r.action.as_str() {
                            "deny" => TrafficRoutingAction::Deny,
                            "redirect" => TrafficRoutingAction::Redirect {
                                target: r.redirect_target_alias.unwrap_or_default(),
                            },
                            _ => return None,
                        };
                        Some(TrafficRoutingRule {
                            api_key_purpose: purpose,
                            action,
                        })
                    })
                    .collect(),
            )
        };
        self
    }
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ComponentPool {
    #[default]
    Default,
    Completions,
}

impl ComponentPool {
    pub fn as_str(self) -> &'static str {
        match self {
            ComponentPool::Default => "default",
            ComponentPool::Completions => "completions",
        }
    }

    pub fn from_db(value: &str) -> Self {
        match value {
            "completions" => ComponentPool::Completions,
            _ => ComponentPool::Default,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, IntoParams)]
pub struct ComponentPoolQuery {
    #[serde(default)]
    pub pool: ComponentPool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ModelComponentCreate {
    #[serde(default = "default_weight")]
    pub weight: i32,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub sort_order: i32,
    #[serde(default)]
    pub pool: ComponentPool,
}

fn default_weight() -> i32 {
    1
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ModelComponentUpdate {
    pub weight: Option<i32>,
    pub enabled: Option<bool>,
    pub sort_order: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ComponentModelSummary {
    #[schema(value_type = String, format = "uuid")]
    pub id: DeploymentId,
    pub alias: String,
    pub model_name: String,
    pub description: Option<String>,
    pub model_type: Option<ModelType>,
    pub endpoint: Option<ComponentEndpointSummary>,
    pub trusted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ComponentEndpointSummary {
    #[schema(value_type = String, format = "uuid")]
    pub id: InferenceEndpointId,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ModelComponentResponse {
    pub weight: i32,
    pub enabled: bool,
    pub pool: ComponentPool,
    pub sort_order: i32,
    pub created_at: DateTime<Utc>,
    pub model: ComponentModelSummary,
}
