
pub mod ai_models;
pub mod api_keys;

pub(crate) fn validate_toleration_preset<P: sqlx_pool_router::PoolProvider + Clone>(
    state: &crate::AppState<P>,
    preset: &str,
) -> crate::errors::Result<()> {
    let config = state.current_config();
    if config.scheduling.preset(preset).is_some() {
        return Ok(());
    }
    Err(crate::errors::Error::BadRequest {
        message: format!(
            "Unknown scheduling toleration preset '{preset}'. Valid values: {}.",
            config
                .scheduling
                .toleration_presets
                .iter()
                .map(|preset| preset.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    })
}
pub mod auth;
pub mod batch_requests;
pub mod batches;
pub mod cache_pricing;
pub mod config;
pub mod connections;
pub mod daemons;
pub mod deployments;
pub mod files;
pub mod groups;
pub mod images;
pub mod inference_endpoints;
pub mod modalities;
pub mod openapi_docs;
pub mod openrouter_models;
pub mod organizations;
pub mod payments;
pub mod probes;
pub mod provider_display_configs;
pub mod queue;
pub mod realtime_inflight_limits;
pub mod recompute;
pub mod requests;
pub mod serving;
pub mod sla_capacity;
pub mod static_assets;
pub mod support;
pub mod transactions;
pub mod unverified_volume;
pub mod users;
pub mod webhooks;
