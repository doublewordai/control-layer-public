
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument};

use crate::config::UsageRefreshConfig;
use crate::db::handlers::analytics::refresh_user_model_usage_daily;
use crate::metrics::errors::component::USAGE_REFRESH;

#[instrument(skip_all)]
pub async fn run_usage_refresh_daemon(
    pools: impl sqlx_pool_router::PoolProvider,
    config: UsageRefreshConfig,
    notify: Arc<Notify>,
    shutdown: CancellationToken,
) {
    let pools = sqlx_pool_router::DynPools::new(pools);
    let min_interval = Duration::from_millis(config.min_interval_milliseconds);

    let mut fallback = (config.fallback_interval_milliseconds > 0).then(|| {
        let mut t = tokio::time::interval(Duration::from_millis(config.fallback_interval_milliseconds));
        t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        t
    });

    info!(
        fallback_interval_ms = config.fallback_interval_milliseconds,
        min_interval_ms = config.min_interval_milliseconds,
        "Usage-refresh daemon started"
    );

    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                info!("Usage-refresh daemon shutting down");
                break;
            }
            _ = notify.notified() => {}
            _ = async { match fallback.as_mut() { Some(t) => { t.tick().await; }, None => std::future::pending().await } } => {}
        }

        if let Err(e) = refresh_user_model_usage_daily(&pools.write()).await {
            crate::background_error!(USAGE_REFRESH, "refresh", Error, error = %e, "Failed to refresh user_model_usage_daily");
        }

        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                info!("Usage-refresh daemon shutting down");
                break;
            }
            _ = tokio::time::sleep(min_interval) => {}
        }
    }
}
