
pub mod embedded;
pub mod errors;
pub mod handlers;
pub mod models;
pub mod pools;

pub use pools::{LabeledPool, PoolMetricsConfig, run_pool_metrics_sampler};

pub fn pool_options(settings: &crate::config::PoolSettings) -> sqlx::postgres::PgPoolOptions {
    use std::time::Duration;
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(settings.max_connections)
        .min_connections(settings.min_connections)
        .acquire_timeout(Duration::from_secs(settings.acquire_timeout_secs))
        .idle_timeout((settings.idle_timeout_secs > 0).then(|| Duration::from_secs(settings.idle_timeout_secs)))
        .max_lifetime((settings.max_lifetime_secs > 0).then(|| Duration::from_secs(settings.max_lifetime_secs)))
}

#[derive(Clone, Debug)]
pub struct PoolPair {
    pub pooled: sqlx_pool_router::DbPools,
    pub direct: sqlx_pool_router::DbPools,
}

impl PoolPair {
    pub fn unsplit(pools: sqlx_pool_router::DbPools) -> Self {
        Self {
            pooled: pools.clone(),
            direct: pools,
        }
    }

    pub fn is_split(&self) -> bool {
        !std::ptr::eq(
            std::sync::Arc::as_ptr(&self.pooled.write().connect_options()),
            std::sync::Arc::as_ptr(&self.direct.write().connect_options()),
        )
    }
}

#[derive(Clone, Debug)]
pub struct DatabasePools {
    pub main: PoolPair,
    pub fusillade: PoolPair,
    pub outlet: Option<PoolPair>,
}
