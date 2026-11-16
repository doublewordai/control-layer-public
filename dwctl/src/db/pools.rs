
use std::time::Duration;

use metrics::gauge;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

#[derive(Debug, Clone)]
pub struct PoolMetricsConfig {
    pub sample_interval: Duration,
}

impl Default for PoolMetricsConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_secs(5),
        }
    }
}

pub struct LabeledPool {
    pub name: &'static str,
    pub pool: sqlx_pool_router::DynPools,
}

pub async fn run_pool_metrics_sampler(
    pools: Vec<LabeledPool>,
    config: PoolMetricsConfig,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    info!(
        "Starting pool metrics sampler for {} pools with {:?} interval",
        pools.len(),
        config.sample_interval
    );

    let mut interval = tokio::time::interval(config.sample_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Pool metrics sampler shutting down");
                break;
            }
            _ = interval.tick() => {
                for labeled in &pools {
                    let current = labeled.pool.write();
                    let max = current.options().get_max_connections();
                    let size = current.size();
                    let idle = current.num_idle();
                    let in_use = size as usize - idle;

                    gauge!("dwctl_db_pool_connections_max", "pool" => labeled.name.to_string())
                        .set(max as f64);

                    gauge!("dwctl_db_pool_connections_total", "pool" => labeled.name.to_string())
                        .set(size as f64);
                    gauge!("dwctl_db_pool_connections_idle", "pool" => labeled.name.to_string())
                        .set(idle as f64);
                    gauge!("dwctl_db_pool_connections_in_use", "pool" => labeled.name.to_string())
                        .set(in_use as f64);

                    debug!(
                        pool = labeled.name,
                        size,
                        idle,
                        in_use,
                        "Sampled pool metrics"
                    );
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[dwctl_test_macros::test]
    async fn test_pool_metrics_sampler_runs_and_shuts_down(pool: sqlx::PgPool) {
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        let pools = vec![LabeledPool {
            name: "test",
            pool: sqlx_pool_router::DynPools::new(pool.clone()),
        }];

        let config = PoolMetricsConfig {
            sample_interval: Duration::from_millis(10),
        };

        let handle = tokio::spawn(async move { run_pool_metrics_sampler(pools, config, shutdown_clone).await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(!handle.is_finished(), "Sampler should still be running");

        shutdown.cancel();

        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Sampler should exit cleanly");
    }

    #[test]
    fn test_pool_metrics_config_default() {
        let config = PoolMetricsConfig::default();
        assert_eq!(config.sample_interval, Duration::from_secs(5));
    }
}
