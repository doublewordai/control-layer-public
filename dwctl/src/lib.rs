#[ctor::ctor]
fn install_crypto_provider() {
    rustls::crypto::aws_lc_rs::default_provider().install_default().ok();
}

pub mod account_limits;
pub mod api;
pub mod api_key_usage;
pub mod auth;
pub mod clickhouse;
pub mod config;
mod config_watcher;
pub mod connections;
pub mod continuation;
mod crypto;
pub mod db;
mod email;
pub mod encryption;
mod error_enrichment;
pub mod errors;
pub mod image_normalizer;
pub mod inference;
pub mod keystore;
mod leader_election;
pub mod limits;
pub mod log_redaction;
mod metrics;
pub use metrics::allocator as allocator_metrics;
pub mod migrations;
pub mod modalities;
pub mod model_provisioning;
mod notifications;
mod openapi;
pub mod openrouter;
pub mod org_overlays;
mod payment_providers;
mod pending_secrets;
pub mod prefix_chain;
pub mod pricing;
mod probes;
pub mod profiling;
pub mod prompt_cache;
pub mod realtime_inflight;
pub mod reasoning;
mod recompute;
pub mod reference_cache;
mod request_logging;
pub mod sample_files;
pub mod scheduling;
mod static_assets;
mod sync;
pub mod task_retention;
pub mod tasks;
pub mod telemetry;
mod types;
mod upload_compression;
pub mod webhooks;

macro_rules! startup_stage {
    ($name:literal) => {
        #[cfg(test)]
        crate::test::utils::startup_stage($name);
    };
}

#[cfg(test)]
mod test;

use crate::metrics::errors::component::{ONWARDS_HEARTBEAT, SUPERVISOR};
use crate::{
    api::models::{
        deployments::{DeployedModelCreate, StandardModelCreate},
        users::Role,
    },
    auth::password,
    config::{CorsConfig, CorsOrigin},
    db::handlers::{Deployments, Groups, Repository, Users},
    db::models::{deployments::DeploymentCreateDBRequest, users::UserCreateDBRequest},
    metrics::GenAiMetrics,
    request_logging::serializers::{parse_ai_request, parse_ai_response},
};
use sqlx_pool_router::{DbPools, PoolProvider};

use crate::db::errors::initialize_database_error_metrics;
use anyhow::Context;
use auth::middleware::admin_ai_proxy_middleware;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::HeaderValue;
use axum::response::Response;
use axum::{
    Router, ServiceExt, http, middleware,
    routing::{delete, get, patch, post, put},
    serve::ListenerExt,
};
use axum_prometheus::PrometheusMetricLayerBuilder;
use bon::Builder;
pub use config::Config;
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use outlet::{MultiHandler, RequestLoggerConfig, RequestLoggerLayer};
use outlet_postgres::PostgresHandler;
use request_logging::{AiResponse, ParsedAIRequest};
use sqlx::{ConnectOptions, Executor, PgPool, postgres::PgConnectOptions};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use tokio::net::TcpListener;
use tower::Layer;
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
};
use tracing::{debug, info, instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

pub use types::{ApiKeyId, DeploymentId, GroupId, InferenceEndpointId, UserId};

#[derive(Clone)]
pub struct SharedConfig(Arc<arc_swap::ArcSwap<Config>>);

impl SharedConfig {
    pub fn new(config: Config) -> Self {
        Self(Arc::new(arc_swap::ArcSwap::from_pointee(config)))
    }

    pub fn snapshot(&self) -> Arc<Config> {
        self.0.load_full()
    }

    pub fn store(&self, config: Config) {
        self.0.store(Arc::new(config));
    }
}

impl From<Config> for SharedConfig {
    fn from(config: Config) -> Self {
        Self::new(config)
    }
}

#[derive(Clone, Builder)]
pub struct AppState<P = DbPools>
where
    P: PoolProvider + Clone,
{
    pub db: P,
    pub config: SharedConfig,
    pub outlet_db: Option<DbPools>,
    pub metrics_recorder: Option<GenAiMetrics>,
    #[builder(default = false)]
    pub is_leader: bool,
    #[builder(default)]
    pub batch_sla_metrics: Arc<arc_swap::ArcSwap<metrics::batch_sla::Snapshot>>,
    pub request_manager: Arc<fusillade_arsenal::PostgresRequestManager<P>>,
    pub task_runner: Arc<tasks::TaskRunner<P>>,
    pub limiters: limits::Limiters,
    pub connections_encryption_key: Option<Vec<u8>>,
    pub response_store: Arc<crate::inference::store::FusilladeResponseStore<P>>,
    pub image_normalizer: Arc<dyn crate::image_normalizer::ImageNormalizer>,
    pub keystore: Option<crate::keystore::Keystore>,
    pub key_policy_cache: crate::sync::key_policy::KeyPolicyCache,
    pub onwards_targets: Option<onwards::target::Targets>,
    #[builder(default)]
    pub internal_credentials: auth::internal_credential::InternalCredentials,
    #[builder(default)]
    pub api_key_usage: api_key_usage::ApiKeyUsage,
}

impl<P> AppState<P>
where
    P: PoolProvider + Clone,
{
    pub(crate) fn internal_auth_state(&self) -> auth::internal_credential::InternalAuthState {
        auth::internal_credential::InternalAuthState {
            credentials: self.internal_credentials.clone(),
            targets: self.onwards_targets.clone().unwrap_or_default(),
        }
    }

    pub fn current_config(&self) -> Arc<Config> {
        self.config.snapshot()
    }
}

pub fn migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}

static PROMETHEUS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static AXUM_PROMETHEUS_PREFIX_SET: OnceLock<()> = OnceLock::new();

fn get_or_install_prometheus_handle() -> PrometheusHandle {
    PROMETHEUS_HANDLE
        .get_or_init(|| {
            const ANALYTICS_LAG_BUCKETS: &[f64] = &[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0];

            const CACHE_SYNC_LAG_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

            const CACHE_LATENCY_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

            const RENDER_DRIFT_TOKEN_BUCKETS: &[f64] = &[-500.0, -100.0, -50.0, -20.0, -5.0, 0.0, 5.0, 20.0, 50.0, 100.0, 500.0];

            const RETRY_ATTEMPTS_BUCKETS: &[f64] = &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

            const SUBMISSION_LATENCY_BUCKETS: &[f64] = &[1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 900.0, 1800.0, 3600.0];

            const HEAP_PROFILE_DUMP_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0];

            let handle = PrometheusBuilder::new()
                .set_buckets_for_metric(
                    Matcher::Full("dwctl_heap_profile_dump_duration_seconds".to_string()),
                    HEAP_PROFILE_DUMP_BUCKETS,
                )
                .expect("Failed to set custom buckets for dwctl_heap_profile_dump_duration_seconds")
                .set_buckets_for_metric(Matcher::Full("dwctl_analytics_lag_seconds".to_string()), ANALYTICS_LAG_BUCKETS)
                .expect("Failed to set custom buckets for dwctl_analytics_lag_seconds")
                .set_buckets_for_metric(Matcher::Full("dwctl_cache_sync_lag_seconds".to_string()), CACHE_SYNC_LAG_BUCKETS)
                .expect("Failed to set custom buckets for dwctl_cache_sync_lag_seconds")
                .set_buckets_for_metric(
                    Matcher::Full("dwctl_cache_classify_duration_seconds".to_string()),
                    CACHE_LATENCY_BUCKETS,
                )
                .expect("Failed to set custom buckets for dwctl_cache_classify_duration_seconds")
                .set_buckets_for_metric(
                    Matcher::Full("dwctl_cache_tokenizer_duration_seconds".to_string()),
                    CACHE_LATENCY_BUCKETS,
                )
                .expect("Failed to set custom buckets for dwctl_cache_tokenizer_duration_seconds")
                .set_buckets_for_metric(
                    Matcher::Full("dwctl_cache_commit_duration_seconds".to_string()),
                    CACHE_LATENCY_BUCKETS,
                )
                .expect("Failed to set custom buckets for dwctl_cache_commit_duration_seconds")
                .set_buckets_for_metric(
                    Matcher::Full("dwctl_cache_lookup_duration_seconds".to_string()),
                    CACHE_LATENCY_BUCKETS,
                )
                .expect("Failed to set custom buckets for dwctl_cache_lookup_duration_seconds")
                .set_buckets_for_metric(
                    Matcher::Full("dwctl_cache_render_drift_tokens".to_string()),
                    RENDER_DRIFT_TOKEN_BUCKETS,
                )
                .expect("Failed to set custom buckets for dwctl_cache_render_drift_tokens")
                .set_buckets_for_metric(
                    Matcher::Full("fusillade_retry_attempts_on_success".to_string()),
                    RETRY_ATTEMPTS_BUCKETS,
                )
                .expect("Failed to set custom buckets for fusillade_retry_attempts_on_success")
                .set_buckets_for_metric(
                    Matcher::Full("fusillade_request_time_to_first_token_seconds".to_string()),
                    SUBMISSION_LATENCY_BUCKETS,
                )
                .expect("Failed to set custom buckets for fusillade_request_time_to_first_token_seconds")
                .set_buckets_for_metric(
                    Matcher::Full("fusillade_request_pickup_delay_seconds".to_string()),
                    SUBMISSION_LATENCY_BUCKETS,
                )
                .expect("Failed to set custom buckets for fusillade_request_pickup_delay_seconds")
                .set_buckets_for_metric(
                    Matcher::Full("onwards_first_token_seconds".to_string()),
                    onwards::FIRST_TOKEN_SECONDS_BUCKETS,
                )
                .expect("Failed to set custom buckets for onwards_first_token_seconds")
                .set_buckets_for_metric(
                    Matcher::Full(crate::inference::body_limit::DECODE_SLOT_WAIT_SECONDS.to_string()),
                    crate::inference::body_limit::DECODE_SLOT_WAIT_BUCKETS,
                )
                .expect("Failed to set custom buckets for dwctl_inference_decode_slot_wait_seconds")
                .install_recorder()
                .expect("Failed to install Prometheus recorder");
            initialize_database_error_metrics();
            crate::metrics::describe_cache_info_metrics();
            crate::inference::body_limit::describe_decode_slot_metrics();
            crate::api::handlers::files::initialize_upload_rejection_metrics();
            handle
        })
        .clone()
}

#[instrument(skip_all)]
pub async fn create_initial_admin_user(
    email: &str,
    password: Option<&str>,
    argon2_params: password::Argon2Params,
    db: &PgPool,
) -> Result<UserId, sqlx::Error> {
    let password_hash = if let Some(pwd) = password {
        Some(
            password::hash_string_with_params(pwd, Some(argon2_params))
                .map_err(|e| sqlx::Error::Encode(format!("Failed to hash admin password: {e}").into()))?,
        )
    } else {
        None
    };

    let mut tx = db.begin().await?;
    let mut user_repo = Users::new(&mut tx);

    if let Some(existing_user) = user_repo
        .get_user_by_email(email)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("Failed to check existing user: {e}")))?
    {
        if let Some(password_hash) = password_hash {
            sqlx::query!("UPDATE users SET password_hash = $1 WHERE id = $2", password_hash, existing_user.id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        return Ok(existing_user.id);
    }

    let user_create = UserCreateDBRequest {
        username: email.to_string(),
        email: email.to_string(),
        display_name: None,
        avatar_url: None,
        is_admin: true,
        roles: vec![Role::PlatformManager],
        auth_source: "system".to_string(),
        password_hash,
        external_user_id: None,
    };

    let created_user = user_repo
        .create(&user_create)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("Failed to create admin user: {e}")))?;

    tx.commit().await?;
    Ok(created_user.id)
}

#[instrument(skip_all)]
pub async fn seed_database(sources: &[config::ModelSource], db: &PgPool) -> Result<(), anyhow::Error> {
    let mut tx = db.begin().await?;

    let seeded = sqlx::query_scalar!("SELECT value FROM system_config WHERE key = 'endpoints_seeded'")
        .fetch_optional(&mut *tx)
        .await?;

    if let Some(true) = seeded {
        info!("Database already seeded, skipping seeding operations");
        tx.commit().await?;
        return Ok(());
    }

    info!("Seeding database with initial configuration");

    let system_user_id = Uuid::nil();
    for source in sources {
        if let Some(endpoint_id) = sqlx::query_scalar!(
            "INSERT INTO inference_endpoints (name, description, url, created_by)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (name) DO NOTHING
            RETURNING id",
            source.name,
            None::<String>,
            source.url.as_str(),
            system_user_id,
        )
        .fetch_optional(&mut *tx)
        .await?
        {
            for model in source.default_models.as_deref().unwrap_or(&[]) {
                let mut model_repo = Deployments::new(&mut tx);
                if let Ok(row) = model_repo
                    .create(&DeploymentCreateDBRequest::from_api_create(
                        Uuid::nil(),
                        DeployedModelCreate::Standard(StandardModelCreate {
                            model_name: model.name.clone(),
                            alias: Some(model.name.clone()),
                            display_name: None,
                            hosted_on: endpoint_id,
                            description: None,
                            model_type: None,
                            capabilities: None,
                            batch_capacity: None,
                            throughput: None,
                            tariffs: None,
                            provider_pricing: None,
                            sanitize_responses: None,
                            trusted: None,
                            reasoning_translation_overrides: None,
                            backoff_enabled: false,
                            backoff_initial_ms: 100,
                            backoff_max_ms: 5_000,
                            backoff_factor: 2.0,
                            backoff_jitter: Default::default(),
                            backoff_max_total_ms: None,
                            first_token_timeout_ms: None,
                            aimd: None,
                            traffic_routing_rules: None,
                            allowed_batch_completion_windows: None,
                            metadata: None,
                        }),
                    ))
                    .await
                    && model.add_to_everyone_group
                {
                    let mut groups_repo = Groups::new(&mut tx);
                    if let Err(e) = groups_repo.add_deployment_to_group(row.id, Uuid::nil(), Uuid::nil()).await {
                        debug!(
                            "Failed to add deployed model {} to 'everyone' group during seeding: {}",
                            model.name, e
                        );
                    }
                }
            }
        }
    }

    let system_api_key_id = Uuid::nil();
    let new_secret = crypto::generate_api_key();
    let new_secret_hash = crypto::hash_api_key(&new_secret);
    sqlx::query!(
        "UPDATE api_keys SET secret_hash = $1, secret_prefix = $2, purpose = 'platform' WHERE id = $3",
        &new_secret_hash[..],
        crypto::api_key_prefix(&new_secret),
        system_api_key_id
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query!(
        "UPDATE system_config SET value = true, updated_at = NOW()
         WHERE key = 'endpoints_seeded'"
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    debug!("Database seeded successfully");

    Ok(())
}

pub(crate) async fn create_schema_pool(
    schema: &str,
    opts: sqlx::postgres::PgConnectOptions,
    settings: &config::PoolSettings,
) -> Result<sqlx::PgPool, sqlx::Error> {
    info!("Setting search_path={} via connection options for schema pool", schema);
    let opts_with_schema = opts.options([("search_path".to_string(), schema.to_string())]);
    db::pool_options(settings).connect_with(opts_with_schema).await
}

pub(crate) fn connect_options(url: &str, slow_threshold: std::time::Duration) -> anyhow::Result<PgConnectOptions> {
    Ok(PgConnectOptions::from_str(url)?.log_slow_statements(log::LevelFilter::Warn, slow_threshold))
}

async fn verify_schema_on_default_search_path(component_name: &str, schema: &str, pooled: &DbPools) -> anyhow::Result<()> {
    for pool in [pooled.write(), pooled.read()] {
        let mut transaction = pool.begin().await?;
        let (current_schema, search_path): (Option<String>, String) =
            sqlx::query_as("SELECT current_schema(), current_setting('search_path')")
                .fetch_one(&mut *transaction)
                .await?;
        sqlx::query("SET LOCAL search_path TO DEFAULT").execute(&mut *transaction).await?;
        let default_schema: Option<String> = sqlx::query_scalar("SELECT current_schema()").fetch_one(&mut *transaction).await?;
        transaction.rollback().await?;
        anyhow::ensure!(
            current_schema.as_deref() == Some(schema) && default_schema.as_deref() == Some(schema),
            "{component_name}: pooled endpoint current_schema() is {current_schema:?}, default schema is {default_schema:?}, expected `{schema}` \
             (search_path: {search_path}). Configure database.{component_name}.pooled_url and, if needed, \
             replica_url using a component role with ALTER ROLE <component_role> IN DATABASE <db> \
             SET search_path TO \"{schema}\". Recycle existing pooler backends after changing role defaults."
        );
    }
    Ok(())
}

pub(crate) async fn verify_same_live_database(component_name: &str, main: &PgPool, component: &PgPool) -> anyhow::Result<()> {
    let challenge = Uuid::new_v4().as_u128() as i64;
    let mut main_transaction = main.begin().await?;
    let held: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(challenge)
        .fetch_one(&mut *main_transaction)
        .await?;
    anyhow::ensure!(held, "{component_name}: could not acquire live database identity challenge");
    let mut component_transaction = component.begin().await?;
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1)")
        .bind(challenge)
        .fetch_one(&mut *component_transaction)
        .await?;
    component_transaction.rollback().await?;
    main_transaction.rollback().await?;
    anyhow::ensure!(
        !acquired,
        "{component_name}: schema url must connect to the same live database as main"
    );
    Ok(())
}

async fn verify_schema_in_transaction(schema: &str, pooled: &DbPools) -> anyhow::Result<()> {
    for pool in [pooled.write(), pooled.read()] {
        let mut transaction = pool.begin().await?;
        sqlx::query(&format!("SET LOCAL search_path TO \"{}\"", schema.replace('"', "\"\"")))
            .execute(&mut *transaction)
            .await?;
        let current: Option<String> = sqlx::query_scalar("SELECT current_schema()").fetch_one(&mut *transaction).await?;
        anyhow::ensure!(
            current.as_deref() == Some(schema),
            "pooled endpoint cannot access schema `{schema}`"
        );
        transaction.rollback().await?;
    }
    Ok(())
}

#[cfg(test)]
mod pooled_schema_tests {
    use std::io::{Result as IoResult, Write};
    use std::sync::Mutex;

    use super::*;

    #[tokio::test]
    async fn startup_does_not_log_schema_credentials() {
        #[derive(Clone)]
        struct Capture(Arc<Mutex<Vec<u8>>>);
        impl Write for Capture {
            fn write(&mut self, bytes: &[u8]) -> IoResult<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> IoResult<()> {
                Ok(())
            }
        }
        let captured = Capture(Default::default());
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let config = Config {
            database: serde_json::from_value(serde_json::json!({
                "type": "external", "url": "invalid", "fusillade": {
                    "mode": "schema", "name": "fusillade",
                    "url": "postgres://fusillade:scoped_password_sentinel@localhost/db"
                }
            }))
            .unwrap(),
            ..Config::default()
        };
        assert!(Application::new_with_pool_and_config_path(config, None, None, None).await.is_err());
        let logs = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("Starting control layer"), "startup logging must be captured");
        assert!(
            !logs.contains("scoped_password_sentinel"),
            "startup must not log schema credentials"
        );
    }

    #[sqlx::test(migrations = false)]
    async fn check_mode_reports_a_missing_schema_instead_of_creating_it(pool: PgPool) {
        let component = serde_json::from_value(serde_json::json!({"mode": "schema", "name": "fusillade_missing"})).unwrap();
        let main = db::PoolPair::unsplit(DbPools::new(pool.clone()));
        let err = setup_component_pools("fusillade", &component, &main, std::time::Duration::from_secs(1), false)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not exist"), "{err}");
        let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'fusillade_missing')")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(!exists, "check mode executed DDL");
        setup_component_pools("fusillade", &component, &main, std::time::Duration::from_secs(1), true)
            .await
            .unwrap();
    }

    #[sqlx::test(migrations = false)]
    async fn pooled_fusillade_defaults_to_role_schema_validation(pool: PgPool) {
        let mut endpoint = pool.connect_options().to_url_lossy();
        endpoint.set_query(None);
        let component = serde_json::from_value(serde_json::json!({
            "mode": "schema", "name": "fusillade", "pooled_url": endpoint.as_str(),
            "pool": {"max_connections": 2, "min_connections": 0}
        }))
        .unwrap();
        let main = db::PoolPair::unsplit(DbPools::new(pool));
        let result = setup_component_pools("fusillade", &component, &main, std::time::Duration::from_secs(1), true).await;
        assert!(
            result.is_err(),
            "role_default must reject a shared login whose default schema is public"
        );
    }

    #[sqlx::test(migrations = false)]
    async fn scoped_schema_rejects_inherited_pooled_identity(pool: PgPool) {
        let component = serde_json::from_value(serde_json::json!({
            "mode": "schema", "name": "fusillade", "url": pool.connect_options().to_url_lossy().as_str(),
            "pooled_schema_mode": "transaction"
        }))
        .unwrap();
        let main = db::PoolPair {
            direct: DbPools::new(pool.clone()),
            pooled: DbPools::new(
                sqlx::postgres::PgPoolOptions::new()
                    .connect_with(pool.connect_options().as_ref().clone())
                    .await
                    .unwrap(),
            ),
        };
        let result = setup_component_pools("fusillade", &component, &main, std::time::Duration::from_secs(1), true).await;
        assert!(
            result.is_err(),
            "explicit direct credentials must not inherit main pooled credentials"
        );
    }

    #[sqlx::test(migrations = false)]
    async fn scoped_schema_rejects_outlet_transaction_mode(pool: PgPool) {
        let component = serde_json::from_value(serde_json::json!({
            "mode": "schema", "name": "outlet", "pooled_schema_mode": "transaction"
        }))
        .unwrap();
        let main = db::PoolPair::unsplit(DbPools::new(pool));
        assert!(
            setup_component_pools("outlet", &component, &main, std::time::Duration::from_secs(1), true)
                .await
                .is_err()
        );
    }

    #[sqlx::test(migrations = false)]
    async fn scoped_schema_rejects_other_database_before_creating_schema(pool: PgPool) {
        let other = sqlx::postgres::PgPoolOptions::new()
            .connect_with(pool.connect_options().as_ref().clone().database("postgres"))
            .await
            .unwrap();
        let schema = format!("scoped_proof_{}", Uuid::new_v4().simple());
        let component = serde_json::from_value(serde_json::json!({
            "mode": "schema", "name": schema, "url": other.connect_options().to_url_lossy().as_str()
        }))
        .unwrap();
        let main = db::PoolPair::unsplit(DbPools::new(pool.clone()));
        let result = setup_component_pools("fusillade", &component, &main, std::time::Duration::from_secs(1), true).await;
        let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
            .bind(&schema)
            .fetch_one(&other)
            .await
            .unwrap();
        assert!(!exists);
        assert!(result.is_err(), "schema mode must reject a different live database");
    }

    #[sqlx::test(migrations = false)]
    async fn scoped_schema_uses_explicit_direct_identity_without_main_replica(pool: PgPool) {
        let role = format!("scoped_{}", Uuid::new_v4().simple());
        pool.execute(format!("CREATE ROLE {role} LOGIN PASSWORD 'scoped_test_password'").as_str())
            .await
            .unwrap();
        pool.execute(format!("CREATE SCHEMA fusillade AUTHORIZATION {role}").as_str())
            .await
            .unwrap();
        let endpoint = pool
            .connect_options()
            .as_ref()
            .clone()
            .username(&role)
            .password("scoped_test_password")
            .to_url_lossy();
        let main = db::PoolPair::unsplit(DbPools::with_replica(pool.clone(), pool.clone()));
        let component = serde_json::from_value(serde_json::json!({
            "mode": "schema", "name": "fusillade", "url": endpoint.as_str()
        }))
        .unwrap();
        let result = setup_component_pools("fusillade", &component, &main, std::time::Duration::from_secs(1), true).await;
        let (identity, has_replica) = match result {
            Ok(pair) => {
                let identity: String = sqlx::query_scalar("SELECT current_user")
                    .fetch_one(pair.direct.write())
                    .await
                    .unwrap();
                let has_replica = pair.pooled.has_replica();
                pair.direct.write().close().await;
                (identity, has_replica)
            }
            Err(error) => panic!("explicit direct identity setup failed: {error}"),
        };
        pool.execute("DROP SCHEMA fusillade CASCADE").await.unwrap();
        pool.execute(format!("DROP ROLE {role}").as_str()).await.unwrap();
        assert_eq!(identity, role);
        assert!(!has_replica);
    }

    #[sqlx::test(migrations = false)]
    async fn pooled_role_default_rejects_contaminated_session(pool: PgPool) {
        pool.execute("CREATE SCHEMA fusillade").await.unwrap();
        let mut endpoint = pool.connect_options().to_url_lossy();
        endpoint.set_query(None);
        let single = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(endpoint.as_str())
            .await
            .unwrap();
        single.execute("SET search_path TO fusillade").await.unwrap();
        assert!(
            verify_schema_on_default_search_path("fusillade", "fusillade", &DbPools::new(single))
                .await
                .is_err(),
            "a session override must not be mistaken for a persistent role default"
        );
    }

    #[sqlx::test]
    async fn pooled_component_endpoint_preserves_retained_response_lifecycle(pool: PgPool) {
        use fusillade_arsenal::manager::{RetainedResponseArchiveCutoffs, RetentionPolicy};
        use fusillade_arsenal::request::{PersistCompletedRealtimeInput, RequestId};
        use fusillade_arsenal::{DaemonStorage, Storage};
        let mut endpoint = pool.connect_options().to_url_lossy();
        endpoint.set_query(None);
        let component: config::ComponentDb = serde_json::from_value(serde_json::json!({
            "mode": "schema", "name": "fusillade", "pooled_schema_mode": "transaction", "pooled_url": endpoint.as_str(),
            "pool": {"max_connections": 2, "min_connections": 0}
        }))
        .unwrap();
        let main = db::PoolPair::unsplit(DbPools::new(pool.clone()));
        let pair = setup_component_pools("fusillade", &component, &main, std::time::Duration::from_secs(1), true)
            .await
            .unwrap();
        assert!(
            pair.is_split(),
            "an explicit component endpoint must be used even without a main pooler"
        );
        fusillade_arsenal::migrator().run(&*pair.direct.write()).await.unwrap();
        migrator().run(&pool).await.unwrap();
        sqlx::query(
            r#"
            CREATE INDEX idx_requests_batchless_retention_due ON requests (
                service_tier,
                (CASE state WHEN 'completed' THEN completed_at
                            WHEN 'failed' THEN failed_at
                            WHEN 'canceled' THEN canceled_at END), id
            ) WHERE batch_id IS NULL AND state IN ('completed', 'failed', 'canceled')
        "#,
        )
        .execute(pair.direct.write())
        .await
        .unwrap();
        let manager =
            fusillade_arsenal::PostgresRequestManager::new(pair.pooled.clone(), Default::default()).with_query_schema("fusillade");
        assert!(manager.retained_response_archive_index_ready().await.unwrap());
        let now = chrono::Utc::now();
        let request_id = Uuid::new_v4();
        manager
            .persist_completed_realtime_batch(&[PersistCompletedRealtimeInput {
                request_id,
                response_body: r#"{"output":"retained"}"#.into(),
                status_code: 200,
                request_body: r#"{"model":"test"}"#.into(),
                model: "test".into(),
                endpoint: "https://example.invalid".into(),
                method: "POST".into(),
                path: "/v1/responses".into(),
                created_by: "test-owner".into(),
                started_at: now - chrono::Duration::hours(2),
                completed_at: now - chrono::Duration::hours(1),
            }])
            .await
            .unwrap();
        let policy = RetentionPolicy {
            batchless_seconds_by_service_tier: [("priority".into(), 604_800)].into(),
            max_late_writer_seconds: Some(3600),
            ..Default::default()
        };
        manager.ensure_retained_response_partitions(&policy, 1).await.unwrap();
        let cutoff = now - chrono::Duration::minutes(30);
        let outcome = manager
            .archive_terminal_batchless_responses(
                &policy,
                &RetainedResponseArchiveCutoffs::new(now, cutoff, cutoff).unwrap(),
                1,
                i64::MAX,
            )
            .await
            .unwrap();
        assert_eq!(outcome.groups_archived, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM fusillade.requests")
                .fetch_one(pair.pooled.write())
                .await
                .unwrap(),
            0
        );
        let retained = manager.get_request_detail(RequestId(request_id)).await.unwrap();
        assert_eq!(retained.id, request_id);
        assert_eq!(retained.created_by, "test-owner");
        assert_eq!(retained.response_body.as_deref(), Some(r#"{"output":"retained"}"#));
    }

    #[sqlx::test(migrations = false)]
    async fn pooled_outlet_rejects_wrong_replica_default(pool: PgPool) {
        pool.execute("CREATE SCHEMA outlet").await.unwrap();
        let primary = create_schema_pool("outlet", pool.connect_options().as_ref().clone(), &config::PoolSettings::default())
            .await
            .unwrap();
        let replica = create_schema_pool(
            "public,outlet",
            pool.connect_options().as_ref().clone(),
            &config::PoolSettings::default(),
        )
        .await
        .unwrap();
        let pools = DbPools::with_replica(primary, replica);
        assert!(verify_schema_on_default_search_path("outlet", "outlet", &pools).await.is_err());
    }

    #[sqlx::test(migrations = false)]
    async fn pooled_outlet_rejects_visible_but_noncurrent_schema(pool: PgPool) {
        pool.execute("CREATE SCHEMA outlet").await.unwrap();
        let pooled = sqlx::postgres::PgPoolOptions::new()
            .connect_with(pool.connect_options().as_ref().clone().options([("search_path", "public,outlet")]))
            .await
            .unwrap();
        assert!(
            verify_schema_on_default_search_path("outlet", "outlet", &DbPools::new(pooled))
                .await
                .is_err()
        );
    }
}

async fn setup_component_pools(
    component_name: &str,
    component: &config::ComponentDb,
    main: &db::PoolPair,
    slow_threshold: std::time::Duration,
    create_schema: bool,
) -> anyhow::Result<db::PoolPair> {
    component
        .validate_schema_pooling(component_name, main.is_split())
        .map_err(anyhow::Error::msg)?;
    let replica_settings = component.replica_pool_settings();
    match component {
        config::ComponentDb::Schema {
            name,
            url,
            pooled_schema_mode,
            pool,
            direct_pool,
            pooled_url,
            replica_url,
            ..
        } => {
            let split = main.is_split() || pooled_url.is_some();
            let direct = create_schema_pool(
                name,
                match url {
                    Some(url) => connect_options(url, slow_threshold)?,
                    None => main.direct.write().connect_options().as_ref().clone(),
                },
                if split { direct_pool } else { pool },
            )
            .await?;
            if url.is_some() {
                verify_same_live_database(component_name, &main.direct.write(), &direct).await?;
            }
            let schema_exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)")
                .bind(name)
                .fetch_one(&direct)
                .await?;
            if !schema_exists && !create_schema {
                anyhow::bail!("{component_name}: schema `{name}` does not exist and migrations.mode is `check`; run `dwctl migrate` first");
            }
            if !schema_exists {
                direct
                    .execute(&*format!("CREATE SCHEMA IF NOT EXISTS \"{}\"", name.replace('"', "\"\"")))
                    .await?;
            }
            let replica_options = match replica_url {
                Some(url) => Some(connect_options(url, slow_threshold)?),
                None if url.is_none() && main.pooled.has_replica() => Some(main.pooled.read().connect_options().as_ref().clone()),
                None => None,
            };
            if split {
                let options = match pooled_url {
                    Some(url) => connect_options(url, slow_threshold)?,
                    None => main.pooled.write().connect_options().as_ref().clone(),
                };
                let primary = db::pool_options(pool).connect_with(options).await?;
                let pooled = match replica_options {
                    Some(options) => DbPools::with_replica(primary, db::pool_options(replica_settings).connect_with(options).await?),
                    None => DbPools::new(primary),
                };
                if *pooled_schema_mode == config::PooledSchemaMode::Transaction {
                    verify_schema_in_transaction(name, &pooled).await?;
                } else {
                    verify_schema_on_default_search_path(component_name, name, &pooled).await?;
                }
                Ok(db::PoolPair {
                    pooled,
                    direct: DbPools::new(direct),
                })
            } else {
                let pools = match replica_options {
                    Some(options) => DbPools::with_replica(direct, create_schema_pool(name, options, replica_settings).await?),
                    None => DbPools::new(direct),
                };
                Ok(db::PoolPair::unsplit(pools))
            }
        }
        config::ComponentDb::Dedicated {
            url,
            replica_url,
            pooled_url,
            pool,
            direct_pool,
            ..
        } => {
            info!("Using dedicated database for {component_name}");
            let direct_settings = if pooled_url.is_some() { direct_pool } else { pool };
            let direct = db::pool_options(direct_settings)
                .connect_with(connect_options(url, slow_threshold)?)
                .await?;
            let replica = match replica_url {
                Some(replica_url) => {
                    info!("Setting up {component_name} read replica");
                    Some(
                        db::pool_options(replica_settings)
                            .connect_with(connect_options(replica_url, slow_threshold)?)
                            .await?,
                    )
                }
                None => None,
            };
            let with_replica = |primary: sqlx::PgPool, replica: Option<sqlx::PgPool>| match replica {
                Some(replica) => DbPools::with_replica(primary, replica),
                None => DbPools::new(primary),
            };
            Ok(match pooled_url {
                Some(pooled_url) => {
                    info!("Using pooled endpoint for {component_name} query traffic");
                    let pooled = db::pool_options(pool)
                        .connect_with(connect_options(pooled_url, slow_threshold)?)
                        .await?;
                    db::PoolPair {
                        pooled: with_replica(pooled, replica),
                        direct: DbPools::new(direct),
                    }
                }
                None => db::PoolPair::unsplit(with_replica(direct, replica)),
            })
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RetainedResponseMaintenanceState {
    unfinished_retirements: i64,
    identity_mismatch: bool,
    retired_routes_exist: bool,
}

fn partition_maintenance_required(daemon: &config::DaemonConfig, state: RetainedResponseMaintenanceState) -> anyhow::Result<bool> {
    if state.identity_mismatch {
        anyhow::bail!("retained-response retirement recovery identity is inconsistent");
    }
    if matches!(daemon.enabled, config::DaemonEnabled::Never) {
        if state.unfinished_retirements > 0 || state.retired_routes_exist {
            tracing::warn!(
                unfinished_retirements = state.unfinished_retirements,
                retired_routes_exist = state.retired_routes_exist,
                "retained-response maintenance work is pending but the batch daemon never runs on this instance; \
                 another instance must run it"
            );
        }
        return Ok(false);
    }
    if state.retired_routes_exist && daemon.retention.max_late_writer_seconds.is_none() {
        anyhow::bail!("retired response route cleanup requires an explicit late-writer fence period");
    }
    if state.retired_routes_exist && (daemon.purge_interval_ms == 0 || daemon.purge_batch_size < 1) {
        anyhow::bail!("retired response route cleanup requires an enabled positive bounded cleanup configuration");
    }
    Ok(daemon.retained_response_retirement_enabled
        || daemon.batch_archive_retirement_enabled
        || daemon.template_retirement_enabled
        || state.unfinished_retirements > 0)
}

#[cfg(test)]
mod retained_response_maintenance_preflight_tests {
    use super::*;

    fn state(unfinished_retirements: i64, identity_mismatch: bool, retired_routes_exist: bool) -> RetainedResponseMaintenanceState {
        RetainedResponseMaintenanceState {
            unfinished_retirements,
            identity_mismatch,
            retired_routes_exist,
        }
    }

    #[test]
    fn an_instance_that_never_runs_the_daemon_owns_no_maintenance() {
        let mut daemon = config::DaemonConfig::default();
        daemon.enabled = config::DaemonEnabled::Never;
        daemon.retained_response_retirement_enabled = true;
        daemon.batch_archive_retirement_enabled = true;
        assert!(!partition_maintenance_required(&daemon, state(1, false, true)).unwrap());
        assert!(partition_maintenance_required(&daemon, state(0, true, false)).is_err());
    }

    #[test]
    fn an_enabled_daemon_owns_maintenance_when_retirement_or_recovery_is_pending() {
        let mut daemon = config::DaemonConfig::default();
        daemon.enabled = config::DaemonEnabled::Always;
        assert!(!partition_maintenance_required(&daemon, state(0, false, false)).unwrap());
        assert!(partition_maintenance_required(&daemon, state(1, false, false)).unwrap());
        daemon.retained_response_retirement_enabled = true;
        assert!(partition_maintenance_required(&daemon, state(0, false, false)).unwrap());
    }

    #[test]
    fn orphan_lifecycle_identity_always_fails_startup() {
        let daemon = config::DaemonConfig::default();
        let error = partition_maintenance_required(&daemon, state(0, true, false)).expect_err("orphan retiring state is not recoverable");
        assert!(error.to_string().contains("identity is inconsistent"));
    }

    #[test]
    fn retired_route_backlog_requires_fence_and_positive_bounded_cleanup() {
        let mut daemon = config::DaemonConfig::default();
        assert!(
            partition_maintenance_required(&daemon, state(0, false, true))
                .unwrap_err()
                .to_string()
                .contains("fence period")
        );

        daemon.retention.max_late_writer_seconds = Some(600);
        daemon.purge_interval_ms = 0;
        assert!(
            partition_maintenance_required(&daemon, state(0, false, true))
                .unwrap_err()
                .to_string()
                .contains("positive bounded cleanup")
        );
        daemon.purge_interval_ms = 1;
        daemon.purge_batch_size = 0;
        assert!(
            partition_maintenance_required(&daemon, state(0, false, true))
                .unwrap_err()
                .to_string()
                .contains("positive bounded cleanup")
        );
        daemon.purge_batch_size = 1;
        assert!(!partition_maintenance_required(&daemon, state(0, false, true)).unwrap());
    }
}

async fn setup_database(
    config: &Config,
    pool: Option<PgPool>,
) -> anyhow::Result<(Option<db::embedded::EmbeddedDatabase>, db::DatabasePools, Option<PgPool>)> {
    let slow_threshold = std::time::Duration::from_millis(config.slow_statement_threshold_ms);

    let (embedded_db, main) = if let Some(existing_pool) = pool {
        info!("Using provided database pool with TestDbPools for read/write separation");
        let test_pools = sqlx_pool_router::TestDbPools::new(existing_pool.clone())
            .await
            .expect("Failed to create TestDbPools");
        let replica_pool = test_pools.read().into_inner();
        migrations::at_startup(config.migrations.mode, &migrations::Target::main(), &existing_pool).await?;
        startup_stage!("db.main");
        (None, db::PoolPair::unsplit(DbPools::with_replica(existing_pool, replica_pool)))
    } else {
        let (embedded_db, database_url) = match &config.database {
            config::DatabaseConfig::Embedded { .. } => {
                let persistent = config.database.embedded_persistent();
                info!("Starting with embedded database (persistent: {})", persistent);
                if !persistent {
                    info!("persistent=false: database will be ephemeral and data will be lost on shutdown");
                }
                #[cfg(feature = "embedded-db")]
                {
                    let data_dir = config.database.embedded_data_dir();
                    let embedded_db = db::embedded::EmbeddedDatabase::start(data_dir, persistent).await?;
                    let url = embedded_db.connection_string().to_string();
                    (Some(embedded_db), url)
                }
                #[cfg(not(feature = "embedded-db"))]
                {
                    anyhow::bail!(
                        "Embedded database is configured but the feature is not enabled. \
                         Rebuild with --features embedded-db to use embedded database."
                    );
                }
            }
            config::DatabaseConfig::External { url, .. } => {
                info!("Using external database");
                (None::<db::embedded::EmbeddedDatabase>, url.clone())
            }
        };

        let pooled_url = config.database.external_pooled_url();
        let direct_settings = if pooled_url.is_some() {
            config.database.direct_pool_settings()
        } else {
            config.database.main_pool_settings()
        };
        let direct = db::pool_options(direct_settings)
            .connect_with(connect_options(&database_url, slow_threshold)?)
            .await?;

        migrations::at_startup(config.migrations.mode, &migrations::Target::main(), &direct).await?;

        let replica = match config.database.external_replica_url() {
            Some(replica_url) => {
                info!("Setting up read replica pool");
                Some(
                    db::pool_options(config.database.main_replica_pool_settings())
                        .connect_with(connect_options(replica_url, slow_threshold)?)
                        .await?,
                )
            }
            None => None,
        };
        let with_replica = |primary: sqlx::PgPool, replica: Option<sqlx::PgPool>| match replica {
            Some(replica) => DbPools::with_replica(primary, replica),
            None => DbPools::new(primary),
        };
        let main = match pooled_url {
            Some(pooled_url) => {
                info!("Using pooled endpoint for main database query traffic");
                let pooled = db::pool_options(config.database.main_pool_settings())
                    .connect_with(connect_options(pooled_url, slow_threshold)?)
                    .await?;
                db::PoolPair {
                    pooled: with_replica(pooled, replica),
                    direct: DbPools::new(direct),
                }
            }
            None => db::PoolPair::unsplit(with_replica(direct, replica)),
        };
        (embedded_db, main)
    };

    if main.is_split() {
        const MIN_DIRECT_FOR_SESSION_WORK: u32 = 6;
        let direct_max = main.direct.write().options().get_max_connections();
        if direct_max < MIN_DIRECT_FOR_SESSION_WORK {
            warn!(
                direct_max,
                minimum = MIN_DIRECT_FOR_SESSION_WORK,
                "database.direct_pool.max_connections is below the number of session-scoped consumers \
                 (LISTEN loops + leader lock); listeners will fail to acquire a connection"
            );
        }
    }

    info!("Setting up fusillade batch processing pool");
    let fusillade = setup_component_pools(
        "fusillade",
        config.database.fusillade(),
        &main,
        slow_threshold,
        config.migrations.mode == config::MigrationsMode::Run,
    )
    .await?;
    migrations::at_startup(config.migrations.mode, &migrations::Target::fusillade(), &fusillade.direct.write()).await?;

    let daemon = &config.background_services.batch_daemon;
    let batch_capable = !matches!(daemon.mode, config::DaemonMode::RequestOnly);
    let partition_maintenance_pool = if batch_capable {
        let (unfinished_retirements, identity_mismatch, retired_routes_exist): (i64, bool, bool) =
            sqlx::query_as(include_str!("retention_preflight.sql"))
                .fetch_one(fusillade.direct.write())
                .await
                .map_err(|error| anyhow::anyhow!("failed to inspect retained-response retirement recovery state: {error}"))?;
        let required = partition_maintenance_required(
            daemon,
            RetainedResponseMaintenanceState {
                unfinished_retirements,
                identity_mismatch,
                retired_routes_exist,
            },
        )?;
        if required {
            let connect_options = match config.database.fusillade() {
                config::ComponentDb::Schema { name, .. } => {
                    if let Some(url) = daemon.retained_response_partition_maintenance_url.as_ref() {
                        PgConnectOptions::from_str(url.expose())?.options([("search_path", name.as_str())])
                    } else {
                        fusillade.direct.write().connect_options().as_ref().clone()
                    }
                }
                config::ComponentDb::Dedicated { .. } => {
                    let url = daemon.retained_response_partition_maintenance_url.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("retained-response recovery on a dedicated database requires an explicit direct session endpoint")
                    })?;
                    PgConnectOptions::from_str(url.expose())?
                }
            }
            .log_slow_statements(log::LevelFilter::Warn, slow_threshold);
            Some(
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .min_connections(0)
                    .acquire_timeout(std::time::Duration::from_secs(30))
                    .idle_timeout(Some(std::time::Duration::from_secs(600)))
                    .max_lifetime(Some(std::time::Duration::from_secs(1_800)))
                    .connect_with(connect_options)
                    .await
                    .map_err(|_| anyhow::anyhow!("failed to connect retained-response partition maintenance session"))?,
            )
        } else {
            None
        }
    } else {
        None
    };

    startup_stage!("db.fusillade");
    migrations::underway_at_startup(config.migrations.mode, &main.direct.write()).await?;
    startup_stage!("db.underway");

    let outlet = if config.enable_request_logging {
        info!("Setting up outlet request logging pool (logging enabled)");
        let outlet = setup_component_pools(
            "outlet",
            config.database.outlet(),
            &main,
            slow_threshold,
            config.migrations.mode == config::MigrationsMode::Run,
        )
        .await?;
        migrations::at_startup(config.migrations.mode, &migrations::Target::outlet(), &outlet.direct.write()).await?;
        Some(outlet)
    } else {
        info!("Skipping outlet pool setup (logging disabled)");
        None
    };

    let argon2_params = password::Argon2Params {
        memory_kib: config.auth.native.password.argon2_memory_kib,
        iterations: config.auth.native.password.argon2_iterations,
        parallelism: config.auth.native.password.argon2_parallelism,
    };
    create_initial_admin_user(
        &config.admin_email,
        config.admin_password.as_deref(),
        argon2_params,
        &main.pooled.write(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create initial admin user: {}", e))?;

    if config.continuation.enabled {
        continuation::provision_global_key(&main.pooled.write())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to provision continuation key: {}", e))?;
    }

    seed_database(&config.model_sources, &main.pooled.write()).await?;
    startup_stage!("db.seed");

    if config.model_provisioning.enabled {
        let catalog = model_provisioning::Catalog::load(&config.model_provisioning.directory)?;
        let overlays = org_overlays::OrgCatalog::load(&config.model_provisioning.org_overlays_directory)?;
        let account_limits = account_limits::AccountLimitsCatalog::load(&config.model_provisioning.account_limits_directory)?;
        account_limits.validate_against_overlays(&overlays)?;
        for preset in account_limits.toleration_presets() {
            if !config
                .scheduling
                .toleration_presets
                .iter()
                .any(|configured| configured.id == preset)
            {
                tracing::warn!(
                    preset,
                    "account files name a toleration preset this deployment does not configure; \
                     its requests get the empty (most restrictive) toleration list"
                );
            }
        }
        model_provisioning::apply(&main.pooled.write(), &catalog).await?;
        org_overlays::apply(&main.pooled.write(), &overlays).await?;
        account_limits::apply(&main.pooled.write(), &account_limits).await?;
    }

    Ok((
        embedded_db,
        db::DatabasePools { main, fusillade, outlet },
        partition_maintenance_pool,
    ))
}

pub(crate) fn loopback_base_url(host: &str, port: u16) -> String {
    use std::net::IpAddr;
    let host = match host.trim_start_matches('[').trim_end_matches(']').parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) if ip.is_unspecified() => "127.0.0.1".to_string(),
        Ok(IpAddr::V6(ip)) if ip.is_unspecified() => "[::1]".to_string(),
        Ok(IpAddr::V6(ip)) => format!("[{ip}]"),
        _ => host.to_string(),
    };
    format!("http://{host}:{port}/ai")
}

fn create_cors_layer(cors: &CorsConfig) -> anyhow::Result<CorsLayer> {
    let mut exposed: Vec<http::HeaderName> = cors.exposed_headers.iter().filter_map(|h| h.parse().ok()).collect();
    let request_id_header = http::HeaderName::from_static(REQUEST_ID_HEADER);
    if !exposed.contains(&request_id_header) {
        exposed.push(request_id_header);
    }

    let mut cors_layer = CorsLayer::new()
        .allow_methods([
            http::Method::GET,
            http::Method::POST,
            http::Method::PUT,
            http::Method::DELETE,
            http::Method::PATCH,
            http::Method::OPTIONS,
        ])
        .allow_headers([http::header::CONTENT_TYPE, http::header::AUTHORIZATION, http::header::ACCEPT])
        .expose_headers(exposed);

    if cors.allow_any_origin_without_credentials {
        info!(
            "Configuring CORS: reflecting any origin without credentials (credentialed allowlist: {:?})",
            cors.allowed_origins
        );
        cors_layer = cors_layer.allow_origin(AllowOrigin::mirror_request()).allow_credentials(false);
    } else {
        let mut origins = Vec::new();
        for origin in &cors.allowed_origins {
            let header_value = match origin {
                CorsOrigin::Wildcard => "*".parse::<HeaderValue>()?,
                CorsOrigin::Url(url) => {
                    let url_str = url.as_str().trim_end_matches('/');
                    url_str.parse::<HeaderValue>()?
                }
            };
            origins.push(header_value);
        }
        info!("Configuring CORS with allowed origins: {:?}", origins);
        cors_layer = cors_layer.allow_origin(origins).allow_credentials(cors.allow_credentials);
    }

    if let Some(max_age) = cors.max_age {
        cors_layer = cors_layer.max_age(std::time::Duration::from_secs(max_age));
    }

    Ok(cors_layer)
}

fn apply_cors(router: Router, cors: &CorsConfig) -> anyhow::Result<Router> {
    if cors.allow_any_origin_without_credentials {
        warn!(
            "CORS: allow_any_origin_without_credentials is enabled — any origin may call the API without credentials; credentialed CORS stays limited to allowed_origins"
        );
    }

    let router = router.layer(create_cors_layer(cors)?);

    if cors.allow_any_origin_without_credentials && cors.allow_credentials {
        let allowlist = Arc::new(origins_to_grant_credentials(cors)?);
        Ok(router.layer(middleware::from_fn_with_state(allowlist, stamp_credentials_for_allowlisted_origins)))
    } else {
        Ok(router)
    }
}

fn origins_to_grant_credentials(cors: &CorsConfig) -> anyhow::Result<Vec<HeaderValue>> {
    let mut out = Vec::new();
    for origin in &cors.allowed_origins {
        if let CorsOrigin::Url(url) = origin {
            let url_str = url.as_str().trim_end_matches('/');
            out.push(url_str.parse::<HeaderValue>()?);
        }
    }
    Ok(out)
}

async fn stamp_credentials_for_allowlisted_origins(
    State(allowlist): State<Arc<Vec<HeaderValue>>>,
    request: Request,
    next: middleware::Next,
) -> Response {
    let origin = request.headers().get(http::header::ORIGIN).cloned();
    let allow = origin.as_ref().is_some_and(|o| allowlist.iter().any(|allowed| allowed == o));
    let mut response = next.run(request).await;
    if allow {
        response
            .headers_mut()
            .insert(http::header::ACCESS_CONTROL_ALLOW_CREDENTIALS, HeaderValue::from_static("true"));
    }
    response
}

fn security_header_pairs(cfg: &crate::config::SecurityHeadersConfig) -> anyhow::Result<Vec<(http::HeaderName, http::HeaderValue)>> {
    let mut pairs: Vec<(http::HeaderName, http::HeaderValue)> = Vec::new();
    if !cfg.enabled {
        return Ok(pairs);
    }

    let mut push = |name: &'static str, value: &str| -> anyhow::Result<()> {
        if value.is_empty() {
            return Ok(());
        }
        let header_value = http::HeaderValue::from_str(value).with_context(|| format!("invalid value for security header `{name}`"))?;
        pairs.push((http::HeaderName::from_static(name), header_value));
        Ok(())
    };

    push("x-content-type-options", "nosniff")?;
    push("x-frame-options", &cfg.frame_options)?;
    push("referrer-policy", &cfg.referrer_policy)?;
    push("permissions-policy", &cfg.permissions_policy)?;
    push("content-security-policy", &cfg.content_security_policy)?;
    push("content-security-policy-report-only", &cfg.content_security_policy_report_only)?;
    push("strict-transport-security", &cfg.strict_transport_security)?;

    Ok(pairs)
}

#[instrument(skip_all)]
#[allow(clippy::too_many_arguments)]
pub async fn build_router(
    state: &mut AppState,
    onwards_router: Router,
    analytics_writer: Option<request_logging::batcher::AnalyticsOutboxWriter>,
    requests_writer_sender: Option<crate::inference::engine::writer::RequestsWriterSender>,
    metrics_recorder: Option<GenAiMetrics>,
    strict_mode: bool,
    inference_middleware_state: Option<crate::inference::middleware::InferenceMiddlewareState>,
    prefix_chain: Option<Arc<crate::prefix_chain::PrefixChainRecorder>>,
) -> anyhow::Result<Router> {
    let config = state.current_config();

    let request_logging_enabled = state.outlet_db.is_some() && config.enable_request_logging;
    let analytics_enabled = config.enable_analytics;

    let reference_cache = (config.reference_cache.enabled && analytics_writer.is_some()).then(|| {
        tracing::info!(principals = config.reference_cache.orgs.len(), models = ?config.reference_cache.models, "Reference-cache estimate enabled");
        Arc::new(crate::reference_cache::ReferenceCache::new(
            &config.reference_cache,
            sqlx_pool_router::DynPools::new(state.db.clone()),
            crate::prompt_cache::TelemetryPolicy::from_config(
                config.cache.telemetry_blocks.strip_from_prompt,
                &config.cache.telemetry_blocks.prefixes,
            ),
        ))
    });

    let outlet_layer = if request_logging_enabled || analytics_enabled || prefix_chain.is_some() {
        state.metrics_recorder = metrics_recorder;

        let mut multi_handler = MultiHandler::new();
        let instance_id = uuid::Uuid::new_v4();

        if request_logging_enabled {
            let outlet_pool = state.outlet_db.as_ref().expect("outlet_db checked above");
            let postgres_handler =
                PostgresHandler::<sqlx::PgPool, ParsedAIRequest, AiResponse>::from_pool_provider(outlet_pool.write().into_inner())
                    .await
                    .expect("Failed to create PostgresHandler for request logging")
                    .with_request_serializer(parse_ai_request)
                    .with_response_serializer(parse_ai_response);
            let postgres_handler = request_logging::credential_scrubber::CredentialHeaderScrubber::new(postgres_handler);
            let postgres_handler = crate::inference::engine::outlet_handler::ZdrBodyScrubber::new(postgres_handler);
            multi_handler = multi_handler.with(postgres_handler);
        }

        if let Some(writer) = analytics_writer {
            let mut analytics_handler = request_logging::AnalyticsHandler::new(writer, instance_id, config.as_ref().clone())
                .with_key_identity(state.internal_auth_state());
            if let Some(cache) = &reference_cache {
                analytics_handler = analytics_handler.with_reference_cache(cache.clone(), state.internal_auth_state());
            }
            multi_handler = multi_handler.with(analytics_handler);
        }

        if let Some(recorder) = prefix_chain {
            multi_handler = multi_handler.with(crate::prefix_chain::PrefixChainHandler::new(
                recorder,
                instance_id,
                config.as_ref().clone(),
                state.internal_auth_state(),
            ));
        }

        if let (Some(rms), Some(sender)) = (inference_middleware_state.as_ref(), requests_writer_sender.clone()) {
            let fusillade_handler = crate::inference::engine::outlet_handler::FusilladeOutletHandler::new(
                sender,
                rms.dwctl_pool.clone(),
                state.internal_auth_state(),
            );
            multi_handler = multi_handler.with(fusillade_handler);
        }

        if multi_handler.is_empty() {
            None
        } else {
            let outlet_config = RequestLoggerConfig {
                capture_request_body: true,
                capture_response_body: true,
                path_filter: None,
                ..Default::default()
            };
            Some(RequestLoggerLayer::new(outlet_config, multi_handler))
        }
    } else {
        None
    };
    let auth_routes = Router::new()
        .route(
            "/authentication/register",
            get(api::handlers::auth::get_registration_info).post(api::handlers::auth::register),
        )
        .route(
            "/authentication/login",
            get(api::handlers::auth::get_login_info).post(api::handlers::auth::login),
        )
        .route("/authentication/logout", post(api::handlers::auth::logout))
        .route("/authentication/password-resets", post(api::handlers::auth::request_password_reset))
        .route(
            "/authentication/password-resets/{token_id}/confirm",
            post(api::handlers::auth::confirm_password_reset),
        )
        .route("/authentication/password-change", post(api::handlers::auth::change_password))
        .with_state(state.clone());

    let api_routes = Router::new()
        .route("/config", get(api::handlers::config::get_config))
        .route("/auth/cli-callback", get(api::handlers::auth::cli_callback))
        .route("/users", get(api::handlers::users::list_users))
        .route("/users", post(api::handlers::users::create_user))
        .route("/users/{id}", get(api::handlers::users::get_user))
        .route("/users/{id}", patch(api::handlers::users::update_user))
        .route("/users/{id}", delete(api::handlers::users::delete_user))
        .route("/users/{user_id}/api-keys", get(api::handlers::api_keys::list_user_api_keys))
        .route("/users/{user_id}/api-keys", post(api::handlers::api_keys::create_user_api_key))
        .route("/users/{user_id}/api-keys/{id}", get(api::handlers::api_keys::get_user_api_key))
        .route(
            "/users/{user_id}/api-keys/{id}",
            patch(api::handlers::api_keys::update_user_api_key),
        )
        .route(
            "/users/{user_id}/api-keys/{id}/rotate",
            post(api::handlers::api_keys::rotate_user_api_key),
        )
        .route(
            "/users/{user_id}/api-keys/{id}/reveal",
            post(api::handlers::api_keys::reveal_user_api_key),
        )
        .route(
            "/users/{user_id}/api-keys/{id}",
            delete(api::handlers::api_keys::delete_user_api_key),
        )
        .route("/users/{user_id}/webhooks", get(api::handlers::webhooks::list_webhooks))
        .route("/users/{user_id}/webhooks", post(api::handlers::webhooks::create_webhook))
        .route("/users/{user_id}/webhooks/{webhook_id}", get(api::handlers::webhooks::get_webhook))
        .route(
            "/users/{user_id}/webhooks/{webhook_id}",
            patch(api::handlers::webhooks::update_webhook),
        )
        .route(
            "/users/{user_id}/webhooks/{webhook_id}",
            delete(api::handlers::webhooks::delete_webhook),
        )
        .route(
            "/users/{user_id}/webhooks/{webhook_id}/rotate-secret",
            post(api::handlers::webhooks::rotate_secret),
        )
        .route("/users/{user_id}/groups", get(api::handlers::groups::get_user_groups))
        .route("/users/{user_id}/groups/{group_id}", post(api::handlers::groups::add_group_to_user))
        .route(
            "/users/{user_id}/groups/{group_id}",
            delete(api::handlers::groups::remove_group_from_user),
        )
        .route("/transactions", post(api::handlers::transactions::create_transaction))
        .route("/transactions/{transaction_id}", get(api::handlers::transactions::get_transaction))
        .route("/transactions", get(api::handlers::transactions::list_transactions))
        .route("/payments", post(api::handlers::payments::create_payment))
        .route("/payments/setup", post(api::handlers::payments::create_payment_setup))
        .route("/payments/{id}", patch(api::handlers::payments::process_payment))
        .route("/billing-portal", post(api::handlers::payments::create_billing_portal_session))
        .route("/auto-topup/enable", post(api::handlers::payments::enable_auto_topup))
        .route("/auto-topup/disable", post(api::handlers::payments::disable_auto_topup))
        .route("/endpoints", get(api::handlers::inference_endpoints::list_inference_endpoints))
        .route("/endpoints", post(api::handlers::inference_endpoints::create_inference_endpoint))
        .route(
            "/endpoints/validate",
            post(api::handlers::inference_endpoints::validate_inference_endpoint),
        )
        .route("/endpoints/{id}", get(api::handlers::inference_endpoints::get_inference_endpoint))
        .route(
            "/endpoints/{id}",
            patch(api::handlers::inference_endpoints::update_inference_endpoint),
        )
        .route(
            "/endpoints/{id}",
            delete(api::handlers::inference_endpoints::delete_inference_endpoint),
        )
        .route(
            "/endpoints/{id}/synchronize",
            post(api::handlers::inference_endpoints::synchronize_endpoint),
        )
        .route("/models", get(api::handlers::deployments::list_deployed_models))
        .route("/models", post(api::handlers::deployments::create_deployed_model))
        .route("/models/{id}", get(api::handlers::deployments::get_deployed_model))
        .route("/models/{id}", patch(api::handlers::deployments::update_deployed_model))
        .route("/models/{id}", delete(api::handlers::deployments::delete_deployed_model))
        .route("/models/{id}/overlays", get(api::handlers::serving::list_model_overlays))
        .route(
            "/models/{id}/realtime-inflight-limits",
            get(api::handlers::realtime_inflight_limits::list_realtime_inflight_limits),
        )
        .route("/models/{id}/cache-pricing", get(api::handlers::cache_pricing::get_cache_pricing))
        .route(
            "/models/{id}/cache-pricing",
            put(api::handlers::cache_pricing::enable_cache_pricing),
        )
        .route(
            "/models/{id}/cache-pricing",
            delete(api::handlers::cache_pricing::disable_cache_pricing),
        )
        .route(
            "/provider-display-configs",
            get(api::handlers::provider_display_configs::list_provider_display_configs),
        )
        .route(
            "/provider-display-configs",
            post(api::handlers::provider_display_configs::create_provider_display_config),
        )
        .route(
            "/provider-display-configs/{provider_key}",
            get(api::handlers::provider_display_configs::get_provider_display_config),
        )
        .route(
            "/provider-display-configs/{provider_key}",
            patch(api::handlers::provider_display_configs::update_provider_display_config),
        )
        .route(
            "/provider-display-configs/{provider_key}",
            delete(api::handlers::provider_display_configs::delete_provider_display_config),
        )
        .route(
            "/provider-display-configs/{provider_key}/icon",
            get(api::handlers::provider_display_configs::get_provider_display_config_icon),
        )
        .route("/models/{id}/components", get(api::handlers::deployments::get_model_components))
        .route(
            "/models/{id}/components/{component_id}",
            post(api::handlers::deployments::add_model_component),
        )
        .route(
            "/models/{id}/components/{component_id}",
            patch(api::handlers::deployments::update_model_component),
        )
        .route(
            "/models/{id}/components/{component_id}",
            delete(api::handlers::deployments::remove_model_component),
        )
        .route("/images/{sha256}", get(api::handlers::images::get_image))
        .route("/groups", get(api::handlers::groups::list_groups))
        .route("/groups", post(api::handlers::groups::create_group))
        .route("/groups/{id}", get(api::handlers::groups::get_group))
        .route("/groups/{id}", patch(api::handlers::groups::update_group))
        .route("/groups/{id}", delete(api::handlers::groups::delete_group))
        .route("/groups/{group_id}/users", get(api::handlers::groups::get_group_users))
        .route("/groups/{group_id}/users/{user_id}", post(api::handlers::groups::add_user_to_group))
        .route(
            "/groups/{group_id}/users/{user_id}",
            delete(api::handlers::groups::remove_user_from_group),
        )
        .route("/groups/{group_id}/models", get(api::handlers::groups::get_group_deployments))
        .route(
            "/groups/{group_id}/models/{deployment_id}",
            post(api::handlers::groups::add_deployment_to_group),
        )
        .route(
            "/groups/{group_id}/models/{deployment_id}",
            delete(api::handlers::groups::remove_deployment_from_group),
        )
        .route("/models/{deployment_id}/groups", get(api::handlers::groups::get_deployment_groups))
        .route("/organizations", get(api::handlers::organizations::list_organizations))
        .route("/organizations", post(api::handlers::organizations::create_organization))
        .route("/organizations/{id}", get(api::handlers::organizations::get_organization))
        .route("/organizations/{id}", patch(api::handlers::organizations::update_organization))
        .route("/organizations/{id}", delete(api::handlers::organizations::delete_organization))
        .route("/organizations/{id}/serving", get(api::handlers::serving::get_organization_serving))
        .route("/organizations/{id}/members", get(api::handlers::organizations::list_members))
        .route("/organizations/{id}/members", post(api::handlers::organizations::add_member))
        .route(
            "/organizations/{id}/members/{user_id}",
            patch(api::handlers::organizations::update_member_role),
        )
        .route(
            "/organizations/{id}/members/{user_id}",
            delete(api::handlers::organizations::remove_member),
        )
        .route("/organizations/{id}/leave", post(api::handlers::organizations::leave_organization))
        .route("/organizations/{id}/invites", post(api::handlers::organizations::invite_member))
        .route(
            "/organizations/{id}/invites/{invite_id}",
            delete(api::handlers::organizations::cancel_invite),
        )
        .route(
            "/organizations/{id}/invites/{invite_id}/resend",
            post(api::handlers::organizations::resend_invite),
        )
        .route(
            "/organizations/{id}/join-requests",
            get(api::handlers::organizations::list_join_requests),
        )
        .route(
            "/organizations/{id}/join-requests/{request_id}/approve",
            post(api::handlers::organizations::approve_join_request),
        )
        .route(
            "/organizations/{id}/join-requests/{request_id}/decline",
            post(api::handlers::organizations::decline_join_request),
        )
        .route(
            "/organizations/{id}/invites/{invite_id}/accept",
            post(api::handlers::organizations::accept_invite_by_id),
        )
        .route(
            "/organizations/{id}/invites/{invite_id}/decline",
            post(api::handlers::organizations::decline_invite_by_id),
        )
        .route(
            "/organizations/invites/{token}",
            get(api::handlers::organizations::get_invite_details),
        )
        .route(
            "/organizations/invites/{token}/accept",
            post(api::handlers::organizations::accept_invite),
        )
        .route(
            "/organizations/invites/{token}/decline",
            post(api::handlers::organizations::decline_invite),
        )
        .route(
            "/organizations/email-change/{token}/confirm",
            get(api::handlers::organizations::confirm_email_change),
        )
        .route(
            "/users/{user_id}/organizations",
            get(api::handlers::organizations::list_user_organizations),
        )
        .route(
            "/users/{user_id}/join-requests",
            get(api::handlers::organizations::list_user_join_requests),
        )
        .route(
            "/users/{user_id}/join-requests",
            post(api::handlers::organizations::create_user_join_request),
        )
        .route(
            "/users/{user_id}/onboarding-context",
            get(api::handlers::organizations::get_onboarding_context),
        )
        .route("/session/organization", post(api::handlers::organizations::set_active_organization))
        .route("/support/requests", post(api::handlers::support::submit_support_request))
        .route("/batches/requests", get(api::handlers::batch_requests::list_batch_requests))
        .route(
            "/batches/requests/{request_id}",
            get(api::handlers::batch_requests::get_batch_request),
        )
        .route(
            "/batches/requests/{request_id}",
            delete(api::handlers::batch_requests::delete_batch_request),
        )
        .route("/requests", get(api::handlers::requests::list_requests))
        .route("/requests/aggregate", get(api::handlers::requests::aggregate_requests))
        .route("/requests/aggregate-by-user", get(api::handlers::requests::aggregate_by_user))
        .route("/usage", get(api::handlers::requests::get_usage))
        .route("/usage-recompute", get(api::handlers::recompute::recompute_usage))
        .route("/probes", get(api::handlers::probes::list_probes))
        .route("/probes", post(api::handlers::probes::create_probe))
        .route("/probes/test/{deployment_id}", post(api::handlers::probes::test_probe))
        .route("/probes/{id}", get(api::handlers::probes::get_probe))
        .route("/probes/{id}", patch(api::handlers::probes::update_probe))
        .route("/probes/{id}", delete(api::handlers::probes::delete_probe))
        .route("/probes/{id}/activate", patch(api::handlers::probes::activate_probe))
        .route("/probes/{id}/deactivate", patch(api::handlers::probes::deactivate_probe))
        .route("/probes/{id}/execute", post(api::handlers::probes::execute_probe))
        .route("/probes/{id}/results", get(api::handlers::probes::get_probe_results))
        .route("/probes/{id}/statistics", get(api::handlers::probes::get_statistics))
        .route("/monitoring/demand", get(api::handlers::queue::get_demand))
        .route("/connections", post(api::handlers::connections::create_connection))
        .route("/connections", get(api::handlers::connections::list_connections))
        .route("/connections/{connection_id}", get(api::handlers::connections::get_connection))
        .route(
            "/connections/{connection_id}",
            delete(api::handlers::connections::delete_connection),
        )
        .route(
            "/connections/{connection_id}/test",
            post(api::handlers::connections::test_connection),
        )
        .route(
            "/connections/{connection_id}/files",
            get(api::handlers::connections::list_connection_files),
        )
        .route(
            "/connections/{connection_id}/synced-keys",
            get(api::handlers::connections::list_synced_keys),
        )
        .route("/connections/{connection_id}/sync", post(api::handlers::connections::trigger_sync))
        .route("/connections/{connection_id}/syncs", get(api::handlers::connections::list_syncs))
        .route(
            "/connections/{connection_id}/syncs/{sync_id}",
            get(api::handlers::connections::get_sync),
        )
        .route(
            "/connections/{connection_id}/syncs/{sync_id}/entries",
            get(api::handlers::connections::list_sync_entries),
        );

    let api_routes_with_state = api_routes.with_state(state.clone());

    let batches_routes = if config.batches.enabled {
        let file_upload_limit = config.limits.files.max_file_size;
        let body_limit_layer = if file_upload_limit == 0 {
            DefaultBodyLimit::disable()
        } else {
            let body_limit_u64 = file_upload_limit.saturating_add(limits::MULTIPART_OVERHEAD);
            let body_limit = usize::try_from(body_limit_u64).unwrap_or(usize::MAX);
            DefaultBodyLimit::max(body_limit)
        };
        let file_router = Router::new().route("/files", post(api::handlers::files::upload_file).layer(body_limit_layer));

        Some(
            Router::new()
                .merge(file_router)
                .route("/files", get(api::handlers::files::list_files))
                .route("/files/{file_id}", get(api::handlers::files::get_file))
                .route("/files/{file_id}", delete(api::handlers::files::delete_file))
                .route("/files/{file_id}/content", get(api::handlers::files::get_file_content))
                .route("/files/{file_id}/cost-estimate", get(api::handlers::files::get_file_cost_estimate))
                .route("/responses/{response_id}", get(crate::inference::handler::get_response))
                .route("/responses/{response_id}", delete(crate::inference::handler::delete_response))
                .route("/batches", post(api::handlers::batches::create_batch))
                .route("/batches", get(api::handlers::batches::list_batches))
                .route("/batches/{batch_id}", get(api::handlers::batches::get_batch))
                .route("/batches/{batch_id}", delete(api::handlers::batches::delete_batch))
                .route("/batches/{batch_id}/analytics", get(api::handlers::batches::get_batch_analytics))
                .route("/batches/{batch_id}/results", get(api::handlers::batches::get_batch_results))
                .route("/batches/{batch_id}/cancel", post(api::handlers::batches::cancel_batch))
                .route(
                    "/batches/{batch_id}/retry",
                    post(api::handlers::batches::retry_failed_batch_requests),
                )
                .route(
                    "/batches/{batch_id}/retry-requests",
                    post(api::handlers::batches::retry_specific_requests),
                )
                .route("/daemons", get(api::handlers::daemons::list_daemons))
                .with_state(state.clone()),
        )
    } else {
        None
    };

    let fallback = get(api::handlers::static_assets::serve_embedded_asset).fallback(get(api::handlers::static_assets::spa_fallback));


    let onwards_router = Router::new()
        .route("/models", get(api::handlers::ai_models::list_ai_models))
        .route(
            "/integrations/openrouter/models",
            get(api::handlers::openrouter_models::list_openrouter_models),
        )
        .fallback_service(onwards_router);

    let onwards_router = {
        let daemon = &state.current_config().background_services.batch_daemon;
        let outbound_config = crate::inference::outbound_request::OutboundConfig {
            timeouts: crate::inference::outbound_request::StreamTimeouts::from_daemon_config(daemon),
            key_policy: state.key_policy_cache.clone(),
        };
        onwards_router.layer(middleware::from_fn_with_state(
            outbound_config,
            crate::inference::outbound_request::outbound_request_middleware,
        ))
    };

    let onwards_router = {
        let cfg = state.current_config();
        let normalizer = state.image_normalizer.clone();
        let realtime_ttl = cfg.image_normalizer.signing.realtime_ttl();
        let processing_timeout = std::time::Duration::from_millis(cfg.background_services.batch_daemon.processing_timeout_ms);
        let token_ttl = cfg.image_normalizer.signing.dispatch_ttl(processing_timeout);
        let image_normalizer_state = crate::inference::image_normalizer_middleware::ImageNormalizerMiddlewareState {
            enabled: cfg.image_normalizer.enabled,
            normalizer,
            realtime_ttl,
            token_ttl,
            pool: Some(sqlx_pool_router::DynPools::new(state.db.clone())),
            keystore: state.keystore.clone(),
            key_policy_cache: inference_middleware_state
                .as_ref()
                .map(|s| s.key_policy_cache.clone())
                .unwrap_or_else(crate::sync::key_policy::KeyPolicyCache::empty),
        };
        onwards_router.layer(middleware::from_fn_with_state(
            image_normalizer_state,
            crate::inference::image_normalizer_middleware::image_normalizer_middleware,
        ))
    };

    let onwards_router = onwards_router.layer(middleware::from_fn_with_state(
        sqlx_pool_router::DynPools::new(state.db.clone()),
        error_enrichment::error_enrichment_middleware,
    ));

    let onwards_router = {
        let cfg = state.current_config();
        if cfg.continuation.enabled {
            let resume_target = onwards_router.clone().with_state(state.clone());
            let body_limit = match cfg.limits.requests.max_body_size {
                0 => usize::MAX,
                n => usize::try_from(n).unwrap_or(usize::MAX),
            };
            match crate::continuation::ContinuationState::build(
                &cfg.continuation,
                &cfg.cache.tokenizer_url,
                sqlx_pool_router::DynPools::new(state.db.clone()),
                resume_target,
                body_limit,
                cfg.onwards.sse_buffer_limit,
            )
            .await
            {
                Ok(continuation_state) => {
                    tracing::info!("Mid-stream continuation enabled - wiring resume layer into onwards stack");
                    crate::continuation::metrics::record_layer_wired(true);
                    onwards_router.layer(middleware::from_fn_with_state(
                        continuation_state,
                        crate::continuation::continuation_middleware,
                    ))
                }
                Err(e) => {
                    crate::background_error!(
                        crate::metrics::errors::component::CONTINUATION,
                        "state_build",
                        Error,
                        error = %e,
                        "Failed to build continuation state - resume layer NOT wired"
                    );
                    crate::continuation::metrics::record_layer_wired(false);
                    onwards_router
                }
            }
        } else {
            onwards_router
        }
    };

    let onwards_router = {
        let cfg = state.current_config();
        if cfg.cache.enabled {
            let pool = sqlx_pool_router::DynPools::new(state.db.clone());
            let classifier = crate::prompt_cache::Classifier::new(
                crate::prompt_cache::PrincipalResolver::new(pool.clone()),
                crate::prompt_cache::ModelConfigResolver::new(pool.clone()),
                crate::prompt_cache::TokenizerClient::new(cfg.cache.tokenizer_url.clone()),
                Arc::new(crate::prompt_cache::PostgresIndex::new(pool, cfg.cache.index_conn_retries)),
                crate::prompt_cache::TierPolicy::from_config(&cfg.cache.enabled_ttls, &cfg.cache.default_ttl),
                crate::prompt_cache::TelemetryPolicy::from_config(
                    cfg.cache.telemetry_blocks.strip_from_prompt,
                    &cfg.cache.telemetry_blocks.prefixes,
                ),
                cfg.cache.render_counting,
            );
            let body_limit = match cfg.limits.requests.max_body_size {
                0 => usize::MAX,
                n => usize::try_from(n).unwrap_or(usize::MAX),
            };
            tracing::info!("Cached-input pricing enabled - wiring cache layer into onwards stack");
            onwards_router.layer(middleware::from_fn_with_state(
                crate::prompt_cache::CacheLayerState::new(
                    classifier,
                    body_limit,
                    std::time::Duration::from_secs(cfg.cache.classify_deadline_secs),
                )
                .with_sse_buffer_limit(cfg.onwards.sse_buffer_limit),
                crate::prompt_cache::cache_middleware,
            ))
        } else {
            onwards_router
        }
    };

    let onwards_router = {
        let translation_body_limit = match config.limits.requests.max_body_size {
            0 => usize::MAX,
            n => usize::try_from(n).unwrap_or(usize::MAX),
        };
        let translators: Vec<std::sync::Arc<dyn crate::inference::translation::ProtocolTranslator>> = vec![
            std::sync::Arc::new(crate::inference::translation::anthropic::AnthropicMessages::new(
                config.cache.enabled,
            )),
            std::sync::Arc::new(crate::inference::translation::anthropic::models::AnthropicModels),
            std::sync::Arc::new(crate::inference::translation::responses::OpenResponses::new()),
        ];
        let translation_registry =
            crate::inference::translation::TranslationRegistry::new(translators).with_max_body_size(translation_body_limit);
        onwards_router.layer(middleware::from_fn_with_state(
            translation_registry,
            crate::inference::translation::middleware::translation_middleware,
        ))
    };

    let onwards_router = if let Some(outlet_layer) = outlet_layer.clone() {
        let onwards_router = match &reference_cache {
            Some(cache) => onwards_router.layer(middleware::from_fn_with_state(
                crate::reference_cache::layer::ReferenceLayerState {
                    cache: cache.clone(),
                    body_limit: match config.limits.requests.max_body_size {
                        0 => usize::MAX,
                        n => usize::try_from(n).unwrap_or(usize::MAX),
                    },
                },
                crate::reference_cache::reference_cache_middleware,
            )),
            None => onwards_router,
        };
        onwards_router
            .layer(middleware::from_fn(auth::internal_credential::carry_presented_key))
            .layer(middleware::from_fn(crate::telemetry::carry_trace_parent))
            .layer(middleware::from_fn(request_logging::stream_completion::track_stream_completion))
            .layer(outlet_layer)
    } else {
        onwards_router
    };

    let onwards_router = if let Some(rms) = inference_middleware_state {
        onwards_router.layer(middleware::from_fn_with_state(
            rms,
            crate::inference::middleware::inference_middleware,
        ))
    } else {
        onwards_router
    };

    let onwards_router = onwards_router.layer(middleware::from_fn_with_state(
        state.api_key_usage.clone(),
        api_key_usage::record_proxy_key_use,
    ));

    let onwards_router = onwards_router.layer(middleware::from_fn_with_state(
        state.limiters.inference_body_size,
        crate::inference::body_limit::limit_inference_body,
    ));

    let onwards_router = onwards_router.layer(middleware::from_fn(crate::telemetry::carry_batch_completion_window));

    let onwards_router = onwards_router.layer(middleware::from_fn_with_state(
        state.internal_auth_state(),
        auth::internal_credential::internal_credential_middleware,
    ));

    let onwards_router = onwards_router.layer(axum::Extension(crate::inference::rejections::Recorder::new(
        state.onwards_targets.clone().unwrap_or_default(),
    )));

    let mut router = Router::new()
        .route("/healthz", get(|| async { "OK" }))
        .route("/webhooks/payments", post(api::handlers::payments::webhook_handler))
        .with_state(state.clone())
        .merge(auth_routes);

    if strict_mode {
        let ai_router = if let Some(batches) = batches_routes {
            batches.merge(onwards_router)
        } else {
            onwards_router
        };
        router = router.nest("/ai/v1", ai_router);
    } else {
        let ai_router = if let Some(batches) = batches_routes {
            batches.merge(onwards_router)
        } else {
            onwards_router
        };
        router = router.nest("/ai/v1", ai_router);
    }

    let not_found = || async { axum::http::StatusCode::NOT_FOUND };
    let openapi_router = Router::new()
        .route(
            "/admin/openapi.json",
            if config.openapi.admin_enabled {
                get(api::handlers::openapi_docs::admin_openapi_json)
            } else {
                get(not_found)
            },
        )
        .route(
            "/admin/docs",
            if config.openapi.admin_enabled {
                get(api::handlers::openapi_docs::admin_openapi_docs)
            } else {
                get(not_found)
            },
        )
        .route(
            "/ai/openapi.json",
            if config.openapi.ai_enabled {
                get(api::handlers::openapi_docs::ai_openapi_json)
            } else {
                get(not_found)
            },
        )
        .route(
            "/ai/docs",
            if config.openapi.ai_enabled {
                get(api::handlers::openapi_docs::ai_openapi_docs)
            } else {
                get(not_found)
            },
        );

    let router = router
        .nest("/admin/api/v1", api_routes_with_state)
        .merge(openapi_router.with_state(state.clone()))
        .fallback_service(fallback.with_state(state.clone()))
        .with_state(state.clone());

    let mut router = apply_cors(router, &config.auth.security.cors)?;

    for (name, value) in security_header_pairs(&config.auth.security.headers)? {
        router = router.layer(SetResponseHeaderLayer::if_not_present(name, value));
    }

    if config.enable_metrics {
        let metric_handle = get_or_install_prometheus_handle();

        let prometheus_layer = if AXUM_PROMETHEUS_PREFIX_SET.set(()).is_ok() {
            PrometheusMetricLayerBuilder::new()
                .with_prefix("dwctl")
                .with_metrics_from_fn(move || metric_handle.clone())
                .build_pair()
                .0
        } else {
            PrometheusMetricLayerBuilder::new()
                .with_metrics_from_fn(move || metric_handle.clone())
                .build_pair()
                .0
        };

        crate::metrics::error_series::register_documented_routes();

        let gen_ai_registry = if let Some(ref recorder) = state.metrics_recorder {
            recorder.registry().clone()
        } else {
            prometheus::Registry::new()
        };

        let endpoint_handle = get_or_install_prometheus_handle();
        let batch_sla_metrics = state.batch_sla_metrics.clone();

        router = router
            .route(
                "/internal/metrics",
                get(|| async move {
                    use prometheus::{Encoder, TextEncoder};

                    let mut axum_metrics = endpoint_handle.render();

                    let encoder = TextEncoder::new();
                    let gen_ai_families = gen_ai_registry.gather();
                    let mut gen_ai_buffer = vec![];
                    encoder.encode(&gen_ai_families, &mut gen_ai_buffer).unwrap();

                    axum_metrics.push_str(&String::from_utf8_lossy(&gen_ai_buffer));
                    axum_metrics.push_str(&batch_sla_metrics.load().render());
                    axum_metrics
                }),
            )
            .layer(prometheus_layer)
            .layer(middleware::from_fn(crate::metrics::error_series::register_error_series));
    }

    let router = router.layer(middleware::from_fn(record_provider_request_id));
    let router = router.layer(middleware::from_fn(inject_trace_id)).layer(
        TraceLayer::new_for_http()
            .make_span_with(|request: &http::Request<_>| {
                let path = request.uri().path();
                let route = request
                    .extensions()
                    .get::<axum::extract::MatchedPath>()
                    .map(|mp| mp.as_str().to_owned());
                let span_name = if let Some(ref route) = route {
                    format!("{} {}", request.method(), route)
                } else {
                    format!("{} {}", request.method(), path)
                };
                let api_type = if path.starts_with("/ai/") {
                    "ai_proxy"
                } else if path.starts_with("/admin/") {
                    "admin"
                } else {
                    "other"
                };
                let span = tracing::info_span!(
                    "request",
                    trace_id = tracing::field::Empty,
                    x_request_id = tracing::field::Empty,
                    edge_request_id = tracing::field::Empty,
                    provider_request_id = tracing::field::Empty,
                    otel.name = %span_name,
                    otel.kind = "server",
                );

                if let Some(ids) = request.extensions().get::<RequestIds>() {
                    span.record("x_request_id", tracing::field::display(&ids.x_request_id));
                    if let Some(edge) = &ids.edge_request_id {
                        span.record("edge_request_id", tracing::field::display(edge));
                    }
                }

                let parent = TraceContextPropagator::new().extract(&onwards::HeaderExtractor(request.headers()));
                if parent.span().span_context().is_valid() {
                    let _ = span.set_parent(parent);
                }

                span.set_attribute("api.type", api_type.to_string());
                span.set_attribute("http.request.method", request.method().to_string());
                span.set_attribute("http.route", route.unwrap_or_default());
                span.set_attribute("url.path", path.to_string());
                span.set_attribute("url.query", request.uri().query().unwrap_or("").to_string());
                span
            })
            .on_request(tower_http::trace::DefaultOnRequest::new().level(tracing::Level::TRACE))
            .on_response(|response: &http::Response<_>, latency: std::time::Duration, span: &tracing::Span| {
                let status = response.status().as_u16();
                span.set_attribute("http.response.status_code", i64::from(status));
                crate::telemetry::record_ai_response_attributes(response, span, status);
                if status >= 500 {
                    span.set_attribute("otel.status_code", "ERROR");
                    span.set_attribute("error.type", status.to_string());
                } else if status >= 400 {
                    match response.extensions().get::<crate::inference::rejections::RejectionCode>() {
                        Some(rejection) if rejection.recorded_on(span) => {}
                        Some(rejection) => span.set_attribute("error.type", rejection.code()),
                        None => span.set_attribute("error.type", status.to_string()),
                    }
                }
                tracing::info!(
                    http.response.status_code = status,
                    latency_ms = latency.as_millis() as u64,
                    "finished processing request"
                );
            })
            .on_failure(
                |error: tower_http::classify::ServerErrorsFailureClass, latency: std::time::Duration, span: &tracing::Span| {
                    span.set_attribute("otel.status_code", "ERROR");
                    span.set_attribute("error.type", error.to_string());
                    tracing::error!(
                        error = %error,
                        latency_ms = latency.as_millis() as u64,
                        "request failed"
                    );
                },
            ),
    );

    Ok(router)
}

const REQUEST_ID_HEADER: &str = "x-request-id";

const MAX_INBOUND_REQUEST_ID_LEN: usize = 128;

#[derive(Debug, Clone)]
struct RequestIds {
    x_request_id: String,
    edge_request_id: Option<String>,
}

fn sanitise_inbound_request_id(value: &http::HeaderValue) -> Option<String> {
    let s = value.to_str().ok()?;
    (!s.is_empty() && s.len() <= MAX_INBOUND_REQUEST_ID_LEN && s.bytes().all(|b| b.is_ascii_graphic())).then(|| s.to_owned())
}

async fn assign_request_id(mut request: axum::extract::Request, next: middleware::Next) -> axum::response::Response {
    let x_request_id = Uuid::now_v7().to_string();
    let edge_request_id = request.headers().get(REQUEST_ID_HEADER).and_then(sanitise_inbound_request_id);
    let value = http::HeaderValue::from_str(&x_request_id).expect("UUID is a valid header value");
    request.headers_mut().insert(REQUEST_ID_HEADER, value.clone());
    request.extensions_mut().insert(RequestIds {
        x_request_id,
        edge_request_id,
    });
    let mut response = next.run(request).await;
    response.headers_mut().insert(REQUEST_ID_HEADER, value);
    response
}

async fn inject_trace_id(mut request: axum::extract::Request, next: middleware::Next) -> axum::response::Response {
    let span = tracing::Span::current();
    request.extensions_mut().insert(crate::telemetry::RequestSpan(span.clone()));
    let sc = span.context().span().span_context().clone();
    if sc.is_valid() {
        span.record("trace_id", tracing::field::display(sc.trace_id()));
        request.headers_mut().remove("traceparent");
        request.headers_mut().remove("tracestate");
    }
    next.run(request).await
}

async fn record_provider_request_id(request: axum::extract::Request, next: middleware::Next) -> axum::response::Response {
    let span = tracing::Span::current();
    let response = next.run(request).await;
    if let Some(provider) = response.headers().get(REQUEST_ID_HEADER).and_then(sanitise_inbound_request_id) {
        span.record("provider_request_id", tracing::field::display(provider));
    }
    response
}

#[derive(Clone)]
struct BackgroundShutdownTokens {
    services: tokio_util::sync::CancellationToken,
    after_http_drain: tokio_util::sync::CancellationToken,
}

const RESPONSES_WRITER_TASK_NAME: &str = "responses-writer";
const API_KEY_USAGE_TASK_NAME: &str = "api-key-usage";
const AFTER_HTTP_DRAIN_TASKS: [&str; 2] = [RESPONSES_WRITER_TASK_NAME, API_KEY_USAGE_TASK_NAME];

impl BackgroundShutdownTokens {
    fn new(services: tokio_util::sync::CancellationToken) -> Self {
        Self {
            services,
            after_http_drain: tokio_util::sync::CancellationToken::new(),
        }
    }

    fn services(&self) -> tokio_util::sync::CancellationToken {
        self.services.clone()
    }

    fn after_http_drain(&self) -> tokio_util::sync::CancellationToken {
        self.after_http_drain.clone()
    }

    fn begin_http_drain(&self) {
        self.services.cancel();
    }

    fn finish_http_drain(&self) {
        self.services.cancel();
        self.after_http_drain.cancel();
    }

    fn task_is_shutting_down(&self, task_name: &str) -> bool {
        if AFTER_HTTP_DRAIN_TASKS.contains(&task_name) {
            self.after_http_drain.is_cancelled()
        } else {
            self.services.is_cancelled()
        }
    }
}

pub struct BackgroundServices {
    pub batch_sla_metrics: Arc<arc_swap::ArcSwap<metrics::batch_sla::Snapshot>>,
    request_manager: Arc<fusillade_arsenal::PostgresRequestManager<DbPools>>,
    onwards_daemon_id: Option<Uuid>,
    fusillade_write_pool: Option<sqlx_pool_router::DynPools>,
    task_runner: Arc<tasks::TaskRunner>,
    is_leader: bool,
    onwards_targets: onwards::target::Targets,
    key_policy_cache: crate::sync::key_policy::KeyPolicyCache,
    #[cfg_attr(not(test), allow(dead_code))]
    onwards_sender: Option<tokio::sync::watch::Sender<onwards::target::Targets>>,
    #[allow(dead_code)]
    strict_mode: bool,
    analytics_writer: Option<request_logging::batcher::AnalyticsOutboxWriter>,
    prefix_chain: Option<Arc<crate::prefix_chain::PrefixChainRecorder>>,
    requests_writer_sender: Option<crate::inference::engine::writer::RequestsWriterSender>,
    background_tasks: tokio::task::JoinSet<anyhow::Result<()>>,
    task_names: std::collections::HashMap<tokio::task::Id, &'static str>,
    shutdown_tokens: BackgroundShutdownTokens,
    pub drop_guard: Option<tokio_util::sync::DropGuard>,
    connections_encryption_key: Option<Vec<u8>>,
    keystore: Option<crate::keystore::Keystore>,
    api_key_usage: api_key_usage::ApiKeyUsage,
}

impl BackgroundServices {
    fn spawn<F>(&mut self, name: &'static str, future: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let abort_handle = self.background_tasks.spawn(future);
        self.task_names.insert(abort_handle.id(), name);
    }

    fn task_is_shutting_down(&self, task_id: tokio::task::Id) -> bool {
        let task_name = self.task_names.get(&task_id).copied().unwrap_or("unknown");
        self.shutdown_tokens.task_is_shutting_down(task_name)
    }

    pub async fn wait_for_failure(&mut self) -> anyhow::Result<std::convert::Infallible> {
        loop {
            match self.background_tasks.join_next_with_id().await {
                None => {
                    futures::future::pending::<()>().await;
                    unreachable!()
                }
                Some(Ok((task_id, Ok(())))) if self.task_is_shutting_down(task_id) => {
                    let task_name = self.task_names.get(&task_id).copied().unwrap_or("unknown");
                    tracing::debug!(task = task_name, "Background task completed during shutdown");
                }
                Some(Ok((task_id, Ok(())))) => {
                    let task_name = self.task_names.get(&task_id).copied().unwrap_or("unknown");
                    crate::background_error!(
                        SUPERVISOR,
                        "task_exit_unexpected",
                        Error,
                        task = task_name,
                        "Background task completed unexpectedly"
                    );
                    anyhow::bail!("Background task '{}' completed early", task_name)
                }
                Some(Ok((task_id, Err(e)))) if self.task_is_shutting_down(task_id) => {
                    let task_name = self.task_names.get(&task_id).copied().unwrap_or("unknown");
                    tracing::debug!(task = task_name, error = %e, "Background task exited with error during shutdown");
                }
                Some(Ok((task_id, Err(e)))) => {
                    let task_name = self.task_names.get(&task_id).copied().unwrap_or("unknown");
                    crate::background_error!(SUPERVISOR, "task_failed", Error, task = task_name, error = %e, "Background task failed");
                    anyhow::bail!("Background task '{}' failed: {}", task_name, e)
                }
                Some(Err(e)) if self.task_is_shutting_down(e.id()) => {
                    let task_id = e.id();
                    let task_name = self.task_names.get(&task_id).copied().unwrap_or("unknown");
                    tracing::debug!(task = task_name, error = %e, "Background task panicked during shutdown");
                }
                Some(Err(e)) => {
                    let task_id = e.id();
                    let task_name = self.task_names.get(&task_id).copied().unwrap_or("unknown");
                    crate::background_error!(SUPERVISOR, "task_panicked", Error, task = task_name, error = %e, "Background task panicked");
                    anyhow::bail!("Background task '{}' panicked: {}", task_name, e)
                }
            }
        }
    }

    pub fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.shutdown_tokens.services()
    }

    pub async fn shutdown(mut self) {
        self.shutdown_tokens.finish_http_drain();

        if let (Some(daemon_id), Some(pool)) = (self.onwards_daemon_id, self.fusillade_write_pool.as_ref()) {
            let mark_dead = sqlx::query(
                "UPDATE daemons SET status = 'dead', stopped_at = NOW() \
                 WHERE id = $1",
            )
            .bind(daemon_id)
            .execute(pool)
            .await;
            if let Err(e) = mark_dead {
                tracing::warn!(error = %e, daemon_id = %daemon_id,
                    "SIGTERM drain: failed to mark onwards daemon Dead — \
                     rows will be reclaimed via stale-daemon detection");
            } else {
                tracing::info!(daemon_id = %daemon_id, "SIGTERM drain: marked onwards daemon Dead");
            }

            let unclaim = sqlx::query(
                "UPDATE requests \
                 SET state = 'pending', daemon_id = NULL, claimed_at = NULL, started_at = NULL \
                 WHERE daemon_id = $1 AND state IN ('claimed', 'processing')",
            )
            .bind(daemon_id)
            .execute(pool)
            .await;
            match unclaim {
                Ok(result) => {
                    if result.rows_affected() > 0 {
                        tracing::info!(
                            daemon_id = %daemon_id,
                            rows_released = result.rows_affected(),
                            "SIGTERM drain: released claimed/processing rows back to pending"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, daemon_id = %daemon_id,
                        "SIGTERM drain: failed to unclaim rows — \
                         rows will be reclaimed via stale-daemon detection");
                }
            }
        }

        while let Some(result) = self.background_tasks.join_next_with_id().await {
            match result {
                Ok((task_id, Ok(()))) => {
                    let task_name = self.task_names.get(&task_id).copied().unwrap_or("unknown");
                    tracing::debug!(task = task_name, "Background task completed successfully");
                }
                Ok((task_id, Err(e))) => {
                    let task_name = self.task_names.get(&task_id).copied().unwrap_or("unknown");
                    crate::background_error!(SUPERVISOR, "task_failed", Error, task = task_name, error = %e, "Background task failed");
                }
                Err(e) => {
                    let task_id = e.id();
                    let task_name = self.task_names.get(&task_id).copied().unwrap_or("unknown");
                    crate::background_error!(SUPERVISOR, "task_panicked", Error, task = task_name, error = %e, "Background task panicked");
                }
            }
        }
    }

    #[cfg(test)]
    pub async fn sync_onwards_config(&self, pool: &sqlx::PgPool) -> anyhow::Result<()> {
        let sender = self
            .onwards_sender
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Onwards sync not enabled"))?;

        let new_targets = crate::sync::onwards_config::load_targets_from_db(pool, &[], self.strict_mode).await?;

        let expected: Vec<(String, Option<onwards::auth::KeySet>)> = new_targets
            .targets
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().default_pool().keys().cloned()))
            .collect();

        let desired = new_targets.clone();

        sender
            .send(new_targets)
            .map_err(|_| anyhow::anyhow!("Failed to send targets update"))?;

        const SETTLE: std::time::Duration = std::time::Duration::from_millis(250);
        const REASSERT_EVERY: std::time::Duration = std::time::Duration::from_millis(200);
        const POLL_EVERY: std::time::Duration = std::time::Duration::from_millis(10);
        const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

        let deadline = std::time::Instant::now() + TIMEOUT;
        let mut stable_since: Option<std::time::Instant> = None;
        let mut last_reassert = std::time::Instant::now();

        loop {
            let live = &self.onwards_targets.targets;
            let applied = live.len() == expected.len()
                && expected
                    .iter()
                    .all(|(alias, keys)| live.get(alias).is_some_and(|pool| pool.default_pool().keys() == keys.as_ref()));

            let now = std::time::Instant::now();
            if applied {
                if now.duration_since(*stable_since.get_or_insert(now)) >= SETTLE {
                    return Ok(());
                }
            } else {
                stable_since = None;
                if now.duration_since(last_reassert) >= REASSERT_EVERY {
                    sender
                        .send(desired.clone())
                        .map_err(|_| anyhow::anyhow!("Failed to re-send targets update"))?;
                    last_reassert = now;
                }
            }

            if now >= deadline {
                anyhow::bail!("onwards did not apply the config update within {:?}", TIMEOUT);
            }

            tokio::time::sleep(POLL_EVERY).await;
        }
    }

    #[cfg(test)]
    pub async fn sync_key_policy(&self, pool: &sqlx::PgPool) -> anyhow::Result<()> {
        crate::sync::key_policy::refresh(pool, &self.key_policy_cache).await?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn flush_api_key_usage(&self, pool: &sqlx::PgPool) -> anyhow::Result<u64> {
        Ok(self.api_key_usage.flush(pool).await?)
    }
}

struct BackgroundTaskBuilder {
    tasks: tokio::task::JoinSet<anyhow::Result<()>>,
    names: std::collections::HashMap<tokio::task::Id, &'static str>,
}

const FUSILLADE_LEADERSHIP_SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

fn supervise_elected_fusillade_daemon(
    daemon_handle: tokio::task::JoinHandle<fusillade::Result<()>>,
    failure_sender: tokio::sync::mpsc::UnboundedSender<String>,
) -> tokio::task::JoinHandle<fusillade::Result<()>> {
    let daemon_handle = tokio_util::task::AbortOnDropHandle::new(daemon_handle);
    tokio::spawn(async move {
        match daemon_handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                let message = format!("Fusillade daemon error: {error}");
                let _ = failure_sender.send(message);
                Err(error)
            }
            Err(error) => {
                let message = format!("Fusillade daemon panic: {error}");
                let _ = failure_sender.send(message.clone());
                Err(fusillade::FusilladeError::Other(anyhow::anyhow!(message)))
            }
        }
    })
}

async fn stop_leader_fusillade_daemon(
    leadership_shutdown: &tokio::sync::Mutex<Option<tokio_util::sync::CancellationToken>>,
    daemon_handle: &tokio::sync::Mutex<Option<tokio::task::JoinHandle<fusillade::Result<()>>>>,
    grace: std::time::Duration,
) {
    let token = leadership_shutdown.lock().await.take();
    if let Some(token) = token {
        token.cancel();
    }
    let Some(mut handle) = daemon_handle.lock().await.take() else {
        return;
    };

    match tokio::time::timeout(grace, &mut handle).await {
        Ok(Ok(Ok(()))) => {
            tracing::info!("Fusillade daemon stopped cooperatively after leadership loss");
        }
        Ok(Ok(Err(error))) => {
            crate::background_error!(
                crate::metrics::errors::component::LEADER_ELECTION,
                "daemon_stop_failed",
                Error,
                error = %error,
                "Fusillade daemon failed while stopping after leadership loss"
            );
        }
        Ok(Err(error)) => {
            crate::background_error!(
                crate::metrics::errors::component::LEADER_ELECTION,
                "daemon_stop_panicked",
                Error,
                error = %error,
                "Fusillade daemon task panicked while stopping after leadership loss"
            );
        }
        Err(_) => {
            crate::background_error!(
                crate::metrics::errors::component::LEADER_ELECTION,
                "daemon_stop_timeout",
                Critical,
                grace_seconds = grace.as_secs_f64(),
                "Fusillade daemon exceeded the cooperative leadership-loss shutdown grace; aborting"
            );
            handle.abort();
            if let Err(error) = handle.await
                && !error.is_cancelled()
            {
                crate::background_error!(
                    crate::metrics::errors::component::LEADER_ELECTION,
                    "daemon_stop_abort_failed",
                    Error,
                    error = %error,
                    "Fusillade daemon task failed after leadership-loss abort"
                );
            }
        }
    }
}

#[cfg(test)]
mod leadership_shutdown_tests {
    use super::{stop_leader_fusillade_daemon, supervise_elected_fusillade_daemon};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn leadership_loss_cancels_and_awaits_daemon_without_holding_mutex() {
        let token = tokio_util::sync::CancellationToken::new();
        let token_slot = tokio::sync::Mutex::new(Some(token.clone()));
        let handle_slot = Arc::new(tokio::sync::Mutex::new(None));
        let observed_unlocked = Arc::new(AtomicBool::new(false));

        let handle_slot_for_task = handle_slot.clone();
        let observed_unlocked_for_task = observed_unlocked.clone();
        let handle = tokio::spawn(async move {
            token.cancelled().await;
            observed_unlocked_for_task.store(handle_slot_for_task.try_lock().is_ok(), Ordering::SeqCst);
            Ok(())
        });
        *handle_slot.lock().await = Some(handle);

        stop_leader_fusillade_daemon(&token_slot, handle_slot.as_ref(), Duration::from_secs(1)).await;

        assert!(observed_unlocked.load(Ordering::SeqCst));
        assert!(token_slot.lock().await.is_none());
        assert!(handle_slot.lock().await.is_none());
    }

    #[tokio::test]
    async fn leadership_loss_abort_propagates_through_supervisor_wrapper() {
        struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let token_slot = tokio::sync::Mutex::new(Some(tokio_util::sync::CancellationToken::new()));
        let handle_slot = tokio::sync::Mutex::new(None);
        let (dropped_sender, dropped_receiver) = tokio::sync::oneshot::channel();
        let guard = DropSignal(Some(dropped_sender));
        let daemon_handle = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<fusillade::Result<()>>().await
        });
        let (failure_sender, _failure_receiver) = tokio::sync::mpsc::unbounded_channel();
        let handle = supervise_elected_fusillade_daemon(daemon_handle, failure_sender);
        *handle_slot.lock().await = Some(handle);
        tokio::task::yield_now().await;

        stop_leader_fusillade_daemon(&token_slot, &handle_slot, Duration::from_millis(1)).await;

        tokio::time::timeout(Duration::from_secs(1), dropped_receiver)
            .await
            .expect("inner daemon should be aborted promptly when its wrapper is aborted")
            .expect("inner daemon drop signal should be delivered");
        assert!(handle_slot.lock().await.is_none());
    }

    #[tokio::test]
    async fn elected_daemon_failure_is_forwarded_to_process_supervisor() {
        let daemon_handle = tokio::spawn(async { Err(fusillade::FusilladeError::Other(anyhow::anyhow!("heartbeat lease expired"))) });
        let (failure_sender, mut failure_receiver) = tokio::sync::mpsc::unbounded_channel();
        let supervised = supervise_elected_fusillade_daemon(daemon_handle, failure_sender);

        let error = supervised
            .await
            .expect("supervisor wrapper should not panic")
            .expect_err("daemon failure must be preserved for leadership-loss cleanup");
        assert!(error.to_string().contains("heartbeat lease expired"));
        assert_eq!(
            failure_receiver.recv().await.as_deref(),
            Some("Fusillade daemon error: heartbeat lease expired")
        );
    }

    #[tokio::test]
    async fn elected_daemon_clean_shutdown_is_not_forwarded_as_failure() {
        let daemon_handle = tokio::spawn(async { Ok(()) });
        let (failure_sender, mut failure_receiver) = tokio::sync::mpsc::unbounded_channel();
        let supervised = supervise_elected_fusillade_daemon(daemon_handle, failure_sender);

        supervised
            .await
            .expect("supervisor wrapper should not panic")
            .expect("clean daemon shutdown should remain clean");
        assert!(failure_receiver.recv().await.is_none());
    }
}

impl BackgroundTaskBuilder {
    fn new() -> Self {
        Self {
            tasks: tokio::task::JoinSet::new(),
            names: std::collections::HashMap::new(),
        }
    }

    fn spawn<F>(&mut self, name: &'static str, future: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let abort_handle = self.tasks.spawn(future);
        self.names.insert(abort_handle.id(), name);
    }

    fn into_parts(
        self,
    ) -> (
        tokio::task::JoinSet<anyhow::Result<()>>,
        std::collections::HashMap<tokio::task::Id, &'static str>,
    ) {
        (self.tasks, self.names)
    }
}

pub(crate) struct BackgroundServicesInput {
    pub request_manager: Arc<fusillade_arsenal::PostgresRequestManager<DbPools>>,
    pub postgres_daemon: Arc<fusillade::PostgresDaemon<DbPools, fusillade::ReqwestHttpClient>>,
    pub model_capacity_limits: Arc<dashmap::DashMap<String, usize>>,
    pub pools: db::DatabasePools,
    pub config: Config,
    pub shared_config: SharedConfig,
    pub shutdown_token: tokio_util::sync::CancellationToken,
    pub metrics_recorder: Option<GenAiMetrics>,
    pub keystore: Option<crate::keystore::Keystore>,
    pub internal_credentials: auth::internal_credential::InternalCredentials,
}

async fn setup_background_services(input: BackgroundServicesInput) -> anyhow::Result<BackgroundServices> {
    let BackgroundServicesInput {
        request_manager,
        postgres_daemon,
        model_capacity_limits,
        pools,
        config,
        shared_config,
        shutdown_token,
        metrics_recorder,
        keystore,
        internal_credentials,
    } = input;

    let shutdown_tokens = BackgroundShutdownTokens::new(shutdown_token);
    let shutdown_token = shutdown_tokens.services();

    let db_pools = pools.main.pooled.clone();
    let fusillade_pools = pools.fusillade.pooled.clone();
    let outlet_pools = pools.outlet.clone();
    let dyn_pools = sqlx_pool_router::DynPools::new(db_pools.clone());
    let direct_pools = sqlx_pool_router::DynPools::new(pools.main.direct.clone());

    if let Some(ks) = keystore.clone()
        && let Err(e) = postgres_daemon.set_response_transformer(std::sync::Arc::new(crate::inference::zdr::ZdrResponseEncryptor::new(ks)))
    {
        tracing::warn!(error = e, "ZDR response transformer was already set; skipping");
    }

    let drop_guard = shutdown_token.clone().drop_guard();
    let mut background_tasks = BackgroundTaskBuilder::new();


    #[cfg_attr(not(test), allow(unused_variables))]
    let (initial_targets, onwards_sender) = if config.background_services.onwards_sync.enabled {
        let escalation_models: Vec<String> = config
            .background_services
            .batch_daemon
            .model_escalations
            .values()
            .map(|e| e.escalation_model.clone())
            .collect();

        let (onwards_config_sync, initial_targets, onwards_stream) = sync::onwards_config::OnwardsConfigSync::new_with_daemon_limits(
            dyn_pools.clone(),
            direct_pools.clone(),
            Some(model_capacity_limits.clone()),
            config.background_services.batch_daemon.default_model_concurrency,
            config.limits.batch_inflight.default_capacity,
            escalation_models,
            config.onwards.strict_mode,
        )
        .await?;

        let sender = onwards_config_sync.sender();

        initial_targets
            .receive_updates(onwards_stream)
            .await
            .map_err(anyhow::Error::from)
            .context("Onwards target updates failed")?;

        let onwards_shutdown = shutdown_token.clone();
        let fallback_interval = config.background_services.onwards_sync.fallback_interval_milliseconds;
        background_tasks.spawn("onwards-config-sync", async move {
            info!(
                "Starting onwards configuration listener (fallback sync every {}ms)",
                fallback_interval
            );
            let sync_config = sync::onwards_config::SyncConfig {
                status_tx: None,
                fallback_interval_milliseconds: fallback_interval,
            };
            onwards_config_sync
                .start(sync_config, onwards_shutdown)
                .await
                .context("Onwards configuration listener failed")
        });

        (initial_targets, Some(sender))
    } else {
        info!("Onwards config sync disabled - AI proxy will not receive config updates");
        let empty_config = onwards::target::ConfigFile {
            targets: std::collections::HashMap::new(),
            auth: None,
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };
        (onwards::target::Targets::from_config(empty_config)?, None)
    };

    let key_policy_cache =
        crate::sync::key_policy::initial_cache(&db_pools.write(), crate::scheduling::TolerationPresets::new(&config.scheduling)).await?;
    if config.background_services.key_policy_sync.enabled {
        let policy_pool = dyn_pools.clone();
        let policy_listener_pool = direct_pools.clone();
        let policy_cache = key_policy_cache.clone();
        let policy_shutdown = shutdown_token.clone();
        let policy_fallback = config.background_services.key_policy_sync.fallback_interval_milliseconds;
        background_tasks.spawn("key-policy-sync", async move {
            crate::sync::key_policy::run(policy_pool, policy_listener_pool, policy_cache, policy_fallback, policy_shutdown)
                .await
                .context("key policy sync failed")
        });
    }

    {
        let pending_secrets_pool = dyn_pools.clone();
        let pending_secrets_keystore = keystore.clone();
        let pending_secrets_shutdown = shutdown_token.clone();
        background_tasks.spawn("pending-secrets-reconcile", async move {
            pending_secrets::run(pending_secrets_pool, pending_secrets_keystore, pending_secrets_shutdown).await
        });
    }

    const LEADER_LOCK_ID: i64 = 0x4457_4354_5052_4F42_i64;

    let probe_scheduler =
        probes::ProbeScheduler::new(dyn_pools.clone(), config.clone(), internal_credentials).with_listener_pool(direct_pools.clone());

    let fusillade_pools_for_metrics = sqlx_pool_router::DynPools::new(fusillade_pools.clone());
    drop(fusillade_pools);

    let is_leader: bool;

    if !config.background_services.leader_election.enabled {
        info!("Launching without leader election: running as leader");
        is_leader = true;

        if config.background_services.probe_scheduler.enabled {
            probe_scheduler.initialize(shutdown_token.clone()).await?;

            let daemon_scheduler = probe_scheduler.clone();
            let daemon_shutdown = shutdown_token.clone();
            background_tasks.spawn("probe-scheduler", async move {
                let use_listen_notify = !cfg!(test);
                daemon_scheduler.run_daemon(daemon_shutdown, use_listen_notify, 300).await;
                Ok(())
            });
        } else {
            info!("Probe scheduler disabled by configuration");
        }

        use crate::config::DaemonEnabled;
        match config.background_services.batch_daemon.enabled {
            DaemonEnabled::Always | DaemonEnabled::Leader => {
                let daemon_handle = postgres_daemon.clone().run(shutdown_token.clone())?;
                background_tasks.spawn("fusillade-daemon", async move {
                    match daemon_handle.await {
                        Ok(Ok(())) => {
                            tracing::info!("Fusillade daemon exited normally");
                        }
                        Ok(Err(e)) => {
                            tracing::error!(error = %e, "Fusillade daemon failed");
                            anyhow::bail!("Fusillade daemon error: {}", e);
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Fusillade daemon task panicked");
                            anyhow::bail!("Fusillade daemon panic: {}", e);
                        }
                    }
                    Ok(())
                });
                info!("Skipping leader election - running as leader with probe scheduler and fusillade daemon");
            }
            DaemonEnabled::Never => {
                info!("Skipping leader election - running as leader with probe scheduler (fusillade daemon disabled)");
            }
        }

        {
            let daemon_config = config.clone();
            let daemon_request_manager = request_manager.clone();
            let daemon_pool = dyn_pools.clone();
            let daemon_listener_pool = direct_pools.clone();
            let daemon_shutdown = shutdown_token.clone();
            background_tasks.spawn("batch-completion", async move {
                notifications::run_notification_poller(
                    daemon_config.background_services.notifications.clone(),
                    daemon_config,
                    daemon_request_manager,
                    daemon_pool,
                    daemon_listener_pool,
                    daemon_shutdown,
                )
                .await;
                Ok(())
            });
        }
    } else {
        is_leader = false;
        info!("Starting leader election - will attempt to acquire leadership");

        use crate::config::DaemonEnabled;
        if config.background_services.batch_daemon.enabled == DaemonEnabled::Always {
            let daemon_handle = postgres_daemon.clone().run(shutdown_token.clone())?;
            background_tasks.spawn("fusillade-daemon", async move {
                match daemon_handle.await {
                    Ok(Ok(())) => {
                        tracing::info!("Fusillade daemon exited normally");
                    }
                    Ok(Err(e)) => {
                        tracing::error!(error = %e, "Fusillade daemon failed");
                        anyhow::bail!("Fusillade daemon error: {}", e);
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Fusillade daemon task panicked");
                        anyhow::bail!("Fusillade daemon panic: {}", e);
                    }
                }
                Ok(())
            });
            info!("Fusillade batch daemon started (configured to always run)");
        }

        let is_leader_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let leader_election_pool = pools.main.direct.write().into_inner();
        let leader_election_pools_gain = dyn_pools.clone();
        let leader_election_listener_pools_gain = direct_pools.clone();
        let leader_election_scheduler_gain = probe_scheduler.clone();
        let leader_election_scheduler_lose = probe_scheduler.clone();
        let leader_election_request_manager_gain = request_manager.clone();
        let leader_election_postgres_daemon_gain = postgres_daemon.clone();
        let leader_election_config = config.clone();
        let leader_election_flag = is_leader_flag.clone();

        let daemon_handle: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<fusillade::Result<()>>>>> =
            Arc::new(tokio::sync::Mutex::new(None));
        let daemon_handle_gain = daemon_handle.clone();
        let daemon_handle_lose = daemon_handle.clone();

        let leadership_shutdown: Arc<tokio::sync::Mutex<Option<tokio_util::sync::CancellationToken>>> =
            Arc::new(tokio::sync::Mutex::new(None));
        let leadership_shutdown_gain = leadership_shutdown.clone();
        let leadership_shutdown_lose = leadership_shutdown.clone();

        let (leader_daemon_failure_sender, mut leader_daemon_failure_receiver) = tokio::sync::mpsc::unbounded_channel::<String>();
        background_tasks.spawn("leader-fusillade-daemon", async move {
            match leader_daemon_failure_receiver.recv().await {
                Some(message) => anyhow::bail!(message),
                None => Ok(()),
            }
        });

        let leader_election_shutdown = shutdown_token.clone();
        background_tasks.spawn("leader-election", async move {
            leader_election::leader_election_task(
                leader_election_pool,
                leader_election_config,
                leader_election_flag,
                LEADER_LOCK_ID,
                leader_election_shutdown,
                move |_pool, config| {
                    let gain_pools = leader_election_pools_gain.clone();
                    let gain_listener_pools = leader_election_listener_pools_gain.clone();
                    let scheduler = leader_election_scheduler_gain.clone();
                    let request_manager = leader_election_request_manager_gain.clone();
                    let postgres_daemon = leader_election_postgres_daemon_gain.clone();
                    let daemon_handle = daemon_handle_gain.clone();
                    let leadership_shutdown = leadership_shutdown_gain.clone();
                    let leader_daemon_failure_sender = leader_daemon_failure_sender.clone();
                    async move {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                        let session_token = tokio_util::sync::CancellationToken::new();
                        *leadership_shutdown.lock().await = Some(session_token.clone());

                        if config.background_services.probe_scheduler.enabled {
                            scheduler
                                .initialize(session_token.clone())
                                .await
                                .map_err(|e| anyhow::anyhow!("Failed to initialize probe scheduler: {}", e))?;

                            let daemon_scheduler = scheduler.clone();
                            let daemon_session_token = session_token.clone();
                            tokio::spawn(async move {
                                let use_listen_notify = !cfg!(test);
                                daemon_scheduler.run_daemon(daemon_session_token, use_listen_notify, 300).await;
                            });
                        } else {
                            tracing::info!("Probe scheduler disabled by configuration");
                        }

                        let notification_request_manager = request_manager.clone();

                        use crate::config::DaemonEnabled;
                        match config.background_services.batch_daemon.enabled {
                            DaemonEnabled::Leader => {
                                let handle = postgres_daemon
                                    .run(session_token.clone())
                                    .map_err(|e| anyhow::anyhow!("Failed to start fusillade daemon: {}", e))?;
                                let handle = supervise_elected_fusillade_daemon(handle, leader_daemon_failure_sender);

                                *daemon_handle.lock().await = Some(handle);

                                tracing::info!("Fusillade batch daemon started on elected leader");
                            }
                            DaemonEnabled::Always => {
                            }
                            DaemonEnabled::Never => {
                                tracing::info!("Fusillade batch daemon disabled by configuration");
                            }
                        }

                        {
                            let daemon_config = config.clone();
                            let daemon_session_token = session_token.clone();
                            tokio::spawn(async move {
                                notifications::run_notification_poller(
                                    daemon_config.background_services.notifications.clone(),
                                    daemon_config,
                                    notification_request_manager,
                                    gain_pools,
                                    gain_listener_pools,
                                    daemon_session_token,
                                )
                                .await;
                            });
                            tracing::info!("Batch completion poller started on elected leader");
                        }

                        Ok(())
                    }
                },
                move |_pool, config| {
                    let scheduler = leader_election_scheduler_lose.clone();
                    let daemon_handle = daemon_handle_lose.clone();
                    let leadership_shutdown = leadership_shutdown_lose.clone();
                    async move {
                        stop_leader_fusillade_daemon(
                            leadership_shutdown.as_ref(),
                            daemon_handle.as_ref(),
                            FUSILLADE_LEADERSHIP_SHUTDOWN_GRACE,
                        )
                        .await;

                        if config.background_services.probe_scheduler.enabled {
                            scheduler
                                .stop_all()
                                .await
                                .map_err(|e| anyhow::anyhow!("Failed to stop probe scheduler: {}", e))?;
                        }

                        Ok(())
                    }
                },
            )
            .await;
            Ok(())
        });
    }

    let uw = config.database.underway_pool_settings();
    let underway_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(uw.max_connections)
        .min_connections(uw.min_connections)
        .acquire_timeout(std::time::Duration::from_secs(uw.acquire_timeout_secs))
        .idle_timeout(if uw.idle_timeout_secs > 0 {
            Some(std::time::Duration::from_secs(uw.idle_timeout_secs))
        } else {
            None
        })
        .max_lifetime(if uw.max_lifetime_secs > 0 {
            Some(std::time::Duration::from_secs(uw.max_lifetime_secs))
        } else {
            None
        })
        .connect_with(pools.main.direct.write().connect_options().as_ref().clone())
        .await?;

    let batch_sla_metrics = Arc::new(arc_swap::ArcSwap::from_pointee(metrics::batch_sla::Snapshot::default()));
    if config.enable_metrics && config.background_services.batch_sla_metrics.enabled {
        let snapshot = batch_sla_metrics.clone();
        let main = db_pools.clone();
        let manager = request_manager.clone();
        let direct = pools.fusillade.direct.clone();
        let collector_config = config.background_services.batch_sla_metrics.clone();
        let shutdown = shutdown_token.clone();
        background_tasks.spawn("batch-sla-metrics", async move {
            metrics::batch_sla::run(main, manager, direct, collector_config, snapshot, shutdown).await
        });
    }

    if config.enable_metrics {
        let mut labeled = vec![
            db::LabeledPool {
                name: "main",
                pool: dyn_pools.clone(),
            },
            db::LabeledPool {
                name: "fusillade",
                pool: fusillade_pools_for_metrics,
            },
            db::LabeledPool {
                name: "main_underway",
                pool: sqlx_pool_router::DynPools::new(underway_pool.clone()),
            },
        ];
        if pools.main.is_split() {
            labeled.push(db::LabeledPool {
                name: "main_direct",
                pool: direct_pools.clone(),
            });
        }
        if pools.fusillade.is_split() {
            labeled.push(db::LabeledPool {
                name: "fusillade_direct",
                pool: sqlx_pool_router::DynPools::new(pools.fusillade.direct.clone()),
            });
        }
        if let Some(outlet) = outlet_pools.as_ref() {
            labeled.push(db::LabeledPool {
                name: "outlet",
                pool: sqlx_pool_router::DynPools::new(outlet.pooled.clone()),
            });
            if outlet.is_split() {
                labeled.push(db::LabeledPool {
                    name: "outlet_direct",
                    pool: sqlx_pool_router::DynPools::new(outlet.direct.clone()),
                });
            }
        }
        let pools = labeled;
        let metrics_shutdown = shutdown_token.clone();
        let metrics_config = db::PoolMetricsConfig {
            sample_interval: config.background_services.pool_metrics.sample_interval,
        };
        background_tasks.spawn("pool-metrics-sampler", async move {
            db::run_pool_metrics_sampler(pools, metrics_config, metrics_shutdown).await
        });
    }

    if config.enable_metrics && config.background_services.allocator_metrics.enabled {
        let allocator_config = config.background_services.allocator_metrics.clone();
        let allocator_shutdown = shutdown_token.clone();
        background_tasks.spawn("allocator-metrics-sampler", async move {
            metrics::allocator::run_allocator_metrics_sampler(allocator_config, allocator_shutdown).await
        });
    }

    profiling::log_heap_profiling_status();
    if config.heap_profiling.enabled {
        let profiling_config = config.heap_profiling.clone();
        let profiling_shutdown = shutdown_token.clone();
        background_tasks.spawn("heap-profiling-server", async move {
            profiling::run_heap_profiling_server(profiling_config, profiling_shutdown).await
        });
    }

    let usage_refresh_notify = std::sync::Arc::new(tokio::sync::Notify::new());
    if config.enable_analytics && config.background_services.usage_refresh.enabled {
        let daemon_pool = dyn_pools.clone();
        let daemon_config = config.background_services.usage_refresh.clone();
        let daemon_notify = usage_refresh_notify.clone();
        let daemon_shutdown = shutdown_token.clone();
        background_tasks.spawn("usage-refresh", async move {
            sync::usage_refresh::run_usage_refresh_daemon(daemon_pool, daemon_config, daemon_notify, daemon_shutdown).await;
            Ok(())
        });
    }

    let api_key_usage = api_key_usage::ApiKeyUsage::default();
    {
        let flusher_pool = dyn_pools.clone();
        let flusher_usage = api_key_usage.clone();
        let flusher_shutdown = shutdown_tokens.after_http_drain();
        background_tasks.spawn(API_KEY_USAGE_TASK_NAME, async move {
            api_key_usage::run_api_key_usage_flusher(flusher_pool, flusher_usage, flusher_shutdown).await;
            Ok(())
        });
    }

    if config.background_services.task_retention.enabled {
        let daemon_pool = direct_pools.clone();
        let daemon_config = config.background_services.task_retention.clone();
        let daemon_shutdown = shutdown_token.clone();
        background_tasks.spawn("task-retention", async move {
            task_retention::run_task_retention_daemon(daemon_pool, daemon_config, daemon_shutdown).await;
            Ok(())
        });
    }

    if config.background_services.prompt_cache_retention.enabled {
        let daemon_pool = direct_pools.clone();
        let daemon_config = config.background_services.prompt_cache_retention.clone();
        let daemon_shutdown = shutdown_token.clone();
        background_tasks.spawn("prompt-cache-retention", async move {
            prompt_cache::retention::run_prompt_cache_retention_daemon(daemon_pool, daemon_config, daemon_shutdown).await;
            Ok(())
        });
    }

    if config.reference_cache.enabled {
        let store = crate::reference_cache::ReferenceStore::new(dyn_pools.clone(), config.reference_cache.ttl(), 1);
        let interval = std::time::Duration::from_secs(config.reference_cache.sweep_interval_secs);
        let sweep_shutdown = shutdown_token.clone();
        background_tasks.spawn("reference-cache-sweep", async move {
            crate::reference_cache::run_sweeper(store, interval, sweep_shutdown).await;
            Ok(())
        });
    }

    let analytics_writer = if config.enable_analytics {
        let (batcher, writer) = request_logging::AnalyticsBatcher::new(dyn_pools.clone(), config.clone(), metrics_recorder);
        let batcher = batcher.with_usage_refresh_notify(usage_refresh_notify.clone());

        let batcher_shutdown = shutdown_token.clone();
        background_tasks.spawn("analytics-batcher", async move {
            batcher.run(batcher_shutdown).await;
            Ok(())
        });

        Some(writer)
    } else {
        None
    };

    let prefix_chain = if config.prefix_chain.enabled {
        let ch_config = config
            .clickhouse
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("prefix_chain.enabled requires a clickhouse section"))?;
        let client = crate::clickhouse::ClickHouseClient::from_config(ch_config)?;
        let (sink_handle, sink) =
            crate::clickhouse::ClickHouseSink::<crate::prefix_chain::PromptChainRow>::new(client, crate::prefix_chain::sink_options())?;
        let sink_shutdown = shutdown_token.clone();
        background_tasks.spawn("prefix-chain-sink", async move {
            sink.run(sink_shutdown).await;
            Ok(())
        });
        let key = config.prefix_chain.key_bytes().map_err(|e| anyhow::anyhow!("prefix_chain: {e}"))?;
        let recorder = crate::prefix_chain::PrefixChainRecorder::new(
            key,
            crate::prefix_chain::ModelFilter::from_config(&config.prefix_chain.models),
            crate::prompt_cache::TierPolicy::from_config(&config.cache.enabled_ttls, &config.cache.default_ttl),
            crate::prompt_cache::TelemetryPolicy::from_config(
                config.cache.telemetry_blocks.strip_from_prompt,
                &config.cache.telemetry_blocks.prefixes,
            ),
            crate::prompt_cache::TokenizerClient::new(config.cache.tokenizer_url.clone()),
            crate::prompt_cache::PrincipalResolver::new(dyn_pools.clone()),
            sink_handle,
        );
        info!(models = ?config.prefix_chain.models, "Prefix-chain capture enabled");
        Some(Arc::new(recorder))
    } else {
        None
    };

    let requests_writer_sender = {
        let (writer, sender) = crate::inference::engine::writer::RequestsWriter::new(
            request_manager.clone(),
            config.background_services.task_workers.response_writer_batch_size,
        );
        let writer_shutdown = shutdown_tokens.after_http_drain();
        background_tasks.spawn(RESPONSES_WRITER_TASK_NAME, async move {
            writer.run(writer_shutdown).await;
            Ok(())
        });
        Some(sender)
    };

    let encryption_key = match config.connections.encryption_key.as_deref().or(config.secret_key.as_deref()) {
        Some(secret) if !secret.trim().is_empty() => Some(encryption::derive_encryption_key(secret.trim())),
        Some(_) => {
            tracing::warn!("Encryption key is empty/whitespace — connection features will be unavailable");
            None
        }
        None => {
            tracing::info!("No encryption key configured for connections (set secret_key or connections.encryption_key)");
            None
        }
    };

    let task_state = tasks::TaskState {
        request_manager: request_manager.clone(),
        dwctl_pool: dyn_pools.clone(),
        config: shared_config.clone(),
        encryption_key: encryption_key.clone(),
        ingest_file_job: Arc::new(std::sync::OnceLock::new()),
        activate_batch_job: Arc::new(std::sync::OnceLock::new()),
        create_batch_job: Arc::new(std::sync::OnceLock::new()),
        cascade_batch_state_job: Arc::new(std::sync::OnceLock::new()),
    };
    let task_runner = Arc::new(tasks::TaskRunner::new(underway_pool, task_state, &config.background_services.task_workers).await?);
    for (name, handle) in task_runner.start(
        shutdown_token.clone(),
        &config.background_services.task_workers,
        &config.background_services.sync_workers,
    ) {
        background_tasks.spawn(name, async move { handle.await.map_err(|e| anyhow::anyhow!("{}", e)) });
    }

    let (background_tasks, task_names) = background_tasks.into_parts();

    Ok(BackgroundServices {
        batch_sla_metrics,
        request_manager,
        task_runner,
        is_leader,
        onwards_targets: initial_targets,
        key_policy_cache,
        onwards_sender,
        strict_mode: config.onwards.strict_mode,
        analytics_writer,
        prefix_chain,
        requests_writer_sender,
        background_tasks,
        task_names,
        shutdown_tokens,
        drop_guard: Some(drop_guard),
        connections_encryption_key: encryption_key.clone(),
        keystore,
        api_key_usage,
        onwards_daemon_id: None,
        fusillade_write_pool: None,
    })
}

pub struct Application {
    router: Router,
    app_state: AppState,
    config: Config,
    pools: db::DatabasePools,
    _embedded_db: Option<db::embedded::EmbeddedDatabase>,
    _tracer_provider: Option<telemetry::SdkTracerProvider>,
    bg_services: BackgroundServices,
}

impl Application {
    pub async fn new(config: Config, tracer_provider: Option<telemetry::SdkTracerProvider>) -> anyhow::Result<Self> {
        Self::new_with_pool_and_config_path(config, None, None, tracer_provider).await
    }

    pub async fn new_with_config_path(
        config: Config,
        config_path: Option<PathBuf>,
        tracer_provider: Option<telemetry::SdkTracerProvider>,
    ) -> anyhow::Result<Self> {
        Self::new_with_pool_and_config_path(config, config_path, None, tracer_provider).await
    }

    pub async fn new_with_pool(
        config: Config,
        pool: Option<PgPool>,
        tracer_provider: Option<telemetry::SdkTracerProvider>,
    ) -> anyhow::Result<Self> {
        Self::new_with_pool_and_config_path(config, None, pool, tracer_provider).await
    }

    pub async fn new_with_pool_and_config_path(
        config: Config,
        config_path: Option<PathBuf>,
        pool: Option<PgPool>,
        tracer_provider: Option<telemetry::SdkTracerProvider>,
    ) -> anyhow::Result<Self> {
        debug!("Starting control layer");

        let (_embedded_db, pools, partition_maintenance_pool) = setup_database(&config, pool).await?;
        let db_pools = pools.main.pooled.clone();
        let fusillade_pools = pools.fusillade.pooled.clone();
        let outlet_pools = pools.outlet.as_ref().map(|p| p.pooled.clone());

        if config.enable_metrics {
            get_or_install_prometheus_handle();
        }

        let shutdown_token = tokio_util::sync::CancellationToken::new();

        let metrics_recorder = if config.enable_metrics && config.enable_analytics {
            let gen_ai_registry = prometheus::Registry::new();
            Some(GenAiMetrics::new(&gen_ai_registry).map_err(|e| anyhow::anyhow!("Failed to create GenAI metrics: {}", e))?)
        } else {
            None
        };

        let shared_config = SharedConfig::new(config.clone());

        let model_capacity_limits: Arc<dashmap::DashMap<String, usize>> = Arc::new(dashmap::DashMap::new());

        let fusillade_daemon_config = config
            .background_services
            .batch_daemon
            .to_fusillade_config_with_limits(Some(model_capacity_limits.clone()));
        let retention_maintenance_config = config
            .background_services
            .batch_daemon
            .to_fusillade_retention_maintenance_config(config.batches.files.max_expiry_seconds);

        if let Some(value) = config.background_services.batch_daemon.template_generation_writes_enabled {
            tracing::warn!(
                template_generation_writes_enabled = value,
                "batch_daemon.template_generation_writes_enabled is deprecated and ignored: \
                 the weekly generation-2 template store is the only write path. Remove the key."
            );
        }
        for (key, value) in [
            (
                "template_retention_days",
                config.background_services.batch_daemon.template_retention_days,
            ),
            (
                "batch_archive_retention_days",
                config.background_services.batch_daemon.batch_archive_retention_days,
            ),
        ] {
            if let Some(days) = value {
                tracing::warn!(
                    key,
                    days,
                    max_expiry_seconds = config.batches.files.max_expiry_seconds,
                    "batch_daemon.{key} is deprecated and ignored: content retention follows \
                     batches.files.max_expiry_seconds. Remove the key."
                );
            }
        }
        let mut request_manager = fusillade_arsenal::PostgresRequestManager::new(
            fusillade_pools.clone(),
            fusillade_arsenal::PostgresStorageConfig::from(&fusillade_daemon_config),
        )
        .with_maintenance_query_timeout(std::time::Duration::from_millis(fusillade_daemon_config.claim_query_timeout_ms))
        .with_retained_response_fence_seconds(config.background_services.batch_daemon.retention.max_late_writer_seconds)
        .with_realtime_retention_seconds(
            config
                .background_services
                .batch_daemon
                .retention
                .batchless_seconds_by_service_tier
                .get("priority")
                .copied(),
        )
        .with_retained_response_retention_bounds_seconds(
            config
                .background_services
                .batch_daemon
                .retention
                .batchless_retention_bounds_seconds(),
        )
        .with_download_buffer_size(config.batches.files.download_buffer_size)
        .with_file_expiry(fusillade_arsenal::FileExpiry {
            default: chrono::Duration::seconds(config.batches.files.default_expiry_seconds),
            max: chrono::Duration::seconds(config.batches.files.max_expiry_seconds),
        })
        .with_batch_insert_strategy(fusillade_arsenal::BatchInsertStrategy::Batched {
            batch_size: config.batches.files.batch_insert_size,
        })
        .with_retry_age_limit(retention_maintenance_config.retry_age_limit());
        if pools.fusillade.is_split()
            && let config::ComponentDb::Schema {
                name,
                pooled_schema_mode: config::PooledSchemaMode::Transaction,
                ..
            } = config.database.fusillade()
        {
            request_manager = request_manager.with_query_schema(name.clone());
        }
        if let Some(partition_maintenance_pool) = partition_maintenance_pool {
            request_manager = request_manager
                .with_partition_maintenance_pool(partition_maintenance_pool)?
                .attest_partition_maintenance_pool()
                .await?;
        }
        let request_manager = Arc::new(request_manager);
        let postgres_daemon = Arc::new(
            fusillade::PostgresDaemon::from_store(request_manager.clone(), fusillade_daemon_config.clone())
                .with_retention_maintenance(retention_maintenance_config)
                .with_leak_config(config.background_services.batch_daemon.leak.clone()),
        );
        startup_stage!("app.fusillade-daemon");
        let keystore = match config.keystore.as_ref() {
            Some(c) => Some(crate::keystore::Keystore::from_config(c).map_err(|e| anyhow::anyhow!("failed to initialise keystore: {e}"))?),
            None => None,
        };
        let response_store =
            Arc::new(crate::inference::store::FusilladeResponseStore::new(request_manager.clone()).with_keystore(keystore.clone()));

        let image_normalizer =
            crate::image_normalizer::from_config(&config.image_normalizer).map_err(|e| anyhow::anyhow!("image normaliser config: {e}"))?;

        let internal_credentials = auth::internal_credential::InternalCredentials::generate();

        {
            let dispatch_auth = crate::inference::engine::dispatch_processor::DispatchAuth::new(
                internal_credentials.clone(),
                std::time::Duration::from_millis(config.background_services.batch_daemon.processing_timeout_ms),
                &config.host,
                config.port,
            );
            let dispatch_processor = crate::inference::engine::dispatch_processor::DispatchProcessor::new()
                .with_auth(dispatch_auth)
                .with_keystore(keystore.clone())
                .with_streamable_endpoints(config.background_services.batch_daemon.streamable_endpoints.clone());
            if let Err(e) = postgres_daemon.set_processor(Arc::new(dispatch_processor)) {
                tracing::warn!(error = e, "Dispatch processor was already set; skipping");
            }
        }

        let mut bg_services = setup_background_services(BackgroundServicesInput {
            request_manager: request_manager.clone(),
            postgres_daemon: postgres_daemon.clone(),
            model_capacity_limits,
            pools: pools.clone(),
            config: config.clone(),
            shared_config: shared_config.clone(),
            shutdown_token: shutdown_token.clone(),
            metrics_recorder: metrics_recorder.clone(),
            keystore: keystore.clone(),
            internal_credentials: internal_credentials.clone(),
        })
        .await?;

        startup_stage!("app.background-services");
        let onwards_daemon_id = uuid::Uuid::new_v4();
        let fusillade_write_pool = sqlx_pool_router::DynPools::new(pools.fusillade.direct.clone());
        let daemon_insert_result = sqlx::query(
            "INSERT INTO daemons (id, hostname, pid, version, config_snapshot, status, started_at, last_heartbeat)
             VALUES ($1, $2, $3, $4, $5, 'running', NOW(), NOW())",
        )
        .bind(onwards_daemon_id)
        .bind(std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()))
        .bind(std::process::id() as i32)
        .bind(env!("CARGO_PKG_VERSION"))
        .bind(serde_json::json!({"type": "onwards"}))
        .execute(&fusillade_write_pool)
        .await;

        let daemon_registered = match &daemon_insert_result {
            Ok(_) => {
                tracing::info!(daemon_id = %onwards_daemon_id, "Registered onwards as fusillade daemon");
                bg_services.onwards_daemon_id = Some(onwards_daemon_id);
                bg_services.fusillade_write_pool = Some(fusillade_write_pool.clone());
                true
            }
            Err(e) => {
                crate::background_error!(ONWARDS_HEARTBEAT, "registration", Warning, error = %e, "Failed to register onwards daemon (table may not exist yet)");
                false
            }
        };

        if daemon_registered {
            let heartbeat_pool = fusillade_write_pool.clone();
            let heartbeat_daemon_id = onwards_daemon_id;
            let heartbeat_shutdown = bg_services.shutdown_token();
            bg_services.spawn("onwards-daemon-heartbeat", async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let result = sqlx::query(
                                "UPDATE daemons SET last_heartbeat = NOW() WHERE id = $1",
                            )
                            .bind(heartbeat_daemon_id)
                            .execute(&heartbeat_pool)
                            .await;

                            if let Err(e) = result {
                                crate::background_error!(ONWARDS_HEARTBEAT, "heartbeat", Warning, error = %e, "Failed to send onwards daemon heartbeat");
                            }
                        }
                        _ = heartbeat_shutdown.cancelled() => {
                            let _ = sqlx::query(
                                "UPDATE daemons SET status = 'dead', stopped_at = NOW() WHERE id = $1",
                            )
                            .bind(heartbeat_daemon_id)
                            .execute(&heartbeat_pool)
                            .await;
                            tracing::info!(daemon_id = %heartbeat_daemon_id, "Onwards daemon marked as dead");
                            break;
                        }
                    }
                }
                Ok(())
            });
        }


        let inference_middleware_state = crate::inference::middleware::InferenceMiddlewareState {
            request_manager: bg_services.request_manager.clone(),
            daemon_id: crate::inference::store::OnwardsDaemonId(onwards_daemon_id),
            loopback_base_url: loopback_base_url(&config.host, config.port),
            dwctl_pool: sqlx_pool_router::DynPools::new(db_pools.clone()),
            response_store: response_store.clone(),
            image_normalizer: image_normalizer.clone(),
            image_normalizer_enabled: config.image_normalizer.enabled,
            unverified_requests_per_completion_hour: config.batches.unverified_requests_per_completion_hour,
            flex_completion_window: config.batches.async_requests.completion_window.clone(),
            keystore: bg_services.keystore.clone(),
            key_policy_cache: bg_services.key_policy_cache.clone(),
            sse_buffer_limit: config.onwards.sse_buffer_limit,
            api_key_usage: bg_services.api_key_usage.clone(),
        };

        let onwards_body_limit = match config.limits.requests.max_body_size {
            0 => usize::MAX,
            n => usize::try_from(n).unwrap_or(usize::MAX),
        };
        let limits_redis =
            crate::realtime_inflight::RealtimeInflightLimiter::shared_redis(config.limits.realtime_inflight.redis_url.as_deref())?;
        let mut onwards_app_state = onwards::AppState::new(bg_services.onwards_targets.clone())
            .with_response_transform(onwards::create_openai_sanitizer())
            .with_upstream_rate_limit_message(config.onwards.upstream_rate_limit_message.clone())
            .with_response_id_header("x-fusillade-request-id")
            .with_body_limit(onwards_body_limit)
            .with_sse_buffer_limit(config.onwards.sse_buffer_limit)
            .with_first_token_timeout_exempt_header(crate::inference::rejections::DISPATCHED_HEADER)
            .with_inflight_limiter(Arc::new(crate::realtime_inflight::RealtimeInflightLimiter::from_parts(
                "realtime",
                config.limits.realtime_inflight.enforce,
                limits_redis.clone(),
            )))
            .with_batch_inflight_limiter(Arc::new(crate::realtime_inflight::RealtimeInflightLimiter::from_parts(
                "batch",
                true,
                limits_redis,
            )))
            .with_batch_inflight_enforce(config.limits.batch_inflight.enforce)
            .with_rejected_params(&config.onwards.rejected_params)
            .map_err(|error| anyhow::anyhow!("onwards.rejected_params: {error}"))?;
        onwards_app_state = onwards_app_state
            .with_model_allowed_params(
                config
                    .onwards
                    .model_allowed_params
                    .iter()
                    .map(|(model, params)| (model.clone(), params.iter())),
            )
            .map_err(|error| anyhow::anyhow!("onwards.model_allowed_params: {error}"))?;
        if config.onwards.first_token_timeout_ms > 0 {
            onwards_app_state =
                onwards_app_state.with_first_token_timeout(std::time::Duration::from_millis(config.onwards.first_token_timeout_ms));
        }

        let onwards_router = if bg_services.onwards_targets.strict_mode {
            tracing::info!("Strict mode enabled - using typed request validation");
            onwards::strict::build_strict_router(onwards_app_state)
        } else {
            onwards::build_router(onwards_app_state)
        };

        let limiters = limits::Limiters::new(&config.limits);

        let mut app_state = AppState::builder()
            .db(db_pools.clone())
            .config(shared_config.clone())
            .is_leader(bg_services.is_leader)
            .batch_sla_metrics(bg_services.batch_sla_metrics.clone())
            .request_manager(bg_services.request_manager.clone())
            .task_runner(bg_services.task_runner.clone())
            .maybe_outlet_db(outlet_pools.clone())
            .limiters(limiters)
            .key_policy_cache(bg_services.key_policy_cache.clone())
            .maybe_connections_encryption_key(bg_services.connections_encryption_key.clone())
            .maybe_keystore(bg_services.keystore.clone())
            .response_store(response_store)
            .image_normalizer(image_normalizer)
            .onwards_targets(bg_services.onwards_targets.clone())
            .internal_credentials(internal_credentials)
            .api_key_usage(bg_services.api_key_usage.clone())
            .build();

        if let Some(config_path) = config_path {
            bg_services.spawn(
                "config-watcher",
                config_watcher::watch_config_file(
                    config_path,
                    shared_config,
                    bg_services.key_policy_cache.clone(),
                    bg_services.shutdown_token(),
                ),
            );
        }

        let router = build_router(
            &mut app_state,
            onwards_router,
            bg_services.analytics_writer.clone(),
            bg_services.requests_writer_sender.clone(),
            metrics_recorder,
            bg_services.onwards_targets.strict_mode,
            Some(inference_middleware_state),
            bg_services.prefix_chain.clone(),
        )
        .await?;

        startup_stage!("app.router");
        Ok(Self {
            router,
            app_state,
            config,
            pools,
            _embedded_db,
            _tracer_provider: tracer_provider,
            bg_services,
        })
    }

    #[cfg(test)]
    pub fn into_test_server(self) -> (axum_test::TestServer, BackgroundServices) {
        let middleware = middleware::from_fn_with_state(self.app_state, admin_ai_proxy_middleware);
        let service = middleware::from_fn(assign_request_id)
            .layer(middleware.layer(self.router))
            .into_make_service();
        let server = axum_test::TestServer::new(service).expect("Failed to create test server");
        (server, self.bg_services)
    }

    pub async fn serve<F>(self, shutdown: F) -> anyhow::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = TcpListener::bind(self.config.bind_address()).await?;
        self.serve_with_listener(listener, shutdown).await
    }

    pub async fn serve_with_listener<F>(mut self, listener: TcpListener, shutdown: F) -> anyhow::Result<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        info!(
            "Control layer listening on http://{}, available at http://localhost:{}",
            listener.local_addr()?,
            self.config.port
        );

        let middleware = middleware::from_fn_with_state(self.app_state, admin_ai_proxy_middleware);
        let service = middleware::from_fn(assign_request_id).layer(middleware.layer(self.router));

        let shutdown_tokens = self.bg_services.shutdown_tokens.clone();
        let shutdown = async move {
            shutdown.await;
            shutdown_tokens.begin_http_drain();
        };

        let listener = listener.tap_io(|tcp| {
            if let Err(err) = tcp.set_nodelay(true) {
                warn!(%err, "failed to set TCP_NODELAY on an accepted connection");
            }
        });

        let server_error: Option<anyhow::Error> = tokio::select! {
            result = axum::serve(listener, service.into_make_service()).with_graceful_shutdown(shutdown) => {
                result.err().map(Into::into)
            }
            result = self.bg_services.wait_for_failure() => {
                match result {
                    Ok(_infallible) => unreachable!("wait_for_failure never returns Ok"),
                    Err(e) => Some(e),
                }
            }
        };

        info!("Shutting down background services...");
        self.bg_services.shutdown().await;

        info!("Closing database connections...");
        for pair in [Some(&self.pools.main), Some(&self.pools.fusillade), self.pools.outlet.as_ref()]
            .into_iter()
            .flatten()
        {
            pair.pooled.close().await;
            if pair.is_split() {
                pair.direct.close().await;
            }
        }

        if let Some(ref provider) = self._tracer_provider {
            info!("Flushing telemetry...");
            if let Err(e) = provider.force_flush() {
                tracing::error!("Failed to flush tracer provider: {}", e);
            }
        }

        if let Some(embedded_db) = self._embedded_db {
            info!("Shutting down embedded database...");
            embedded_db.stop().await?;
        }

        if let Some(e) = server_error {
            return Err(e);
        }

        Ok(())
    }
}

#[cfg(test)]
mod loopback_tests {
    use super::loopback_base_url;

    #[test]
    fn loopback_base_url_targets_this_process() {
        let cases = [
            ("0.0.0.0", "http://127.0.0.1:3001/ai"),
            ("127.0.0.1", "http://127.0.0.1:3001/ai"),
            ("::", "http://[::1]:3001/ai"),
            ("[::]", "http://[::1]:3001/ai"),
            ("::1", "http://[::1]:3001/ai"),
            ("10.1.2.3", "http://10.1.2.3:3001/ai"),
            ("localhost", "http://localhost:3001/ai"),
        ];
        for (bind_host, expected) in cases {
            let url = loopback_base_url(bind_host, 3001);
            assert_eq!(url, expected, "bind host {bind_host}");
            let parsed = url::Url::parse(&url).unwrap();
            let host = parsed.host_str().unwrap().trim_start_matches('[').trim_end_matches(']');
            let local = host.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
                || host == "localhost"
                || host == bind_host.trim_start_matches('[').trim_end_matches(']');
            assert!(local, "{url} must target this process (bind host {bind_host})");
            assert_eq!(parsed.port(), Some(3001));
        }
    }
}

#[cfg(test)]
mod security_header_tests {
    use super::security_header_pairs;
    use crate::config::SecurityHeadersConfig;

    fn names(cfg: &SecurityHeadersConfig) -> Vec<String> {
        security_header_pairs(cfg)
            .unwrap()
            .into_iter()
            .map(|(name, _)| name.as_str().to_string())
            .collect()
    }

    #[test]
    fn defaults_emit_the_safe_hardening_headers() {
        let names = names(&SecurityHeadersConfig::default());
        assert!(names.contains(&"x-content-type-options".to_string()));
        assert!(names.contains(&"x-frame-options".to_string()));
        assert!(names.contains(&"referrer-policy".to_string()));
        assert!(names.contains(&"permissions-policy".to_string()));
        assert!(!names.contains(&"content-security-policy".to_string()));
        assert!(!names.contains(&"content-security-policy-report-only".to_string()));
        assert!(!names.contains(&"strict-transport-security".to_string()));
    }

    #[test]
    fn disabled_emits_nothing() {
        let cfg = SecurityHeadersConfig {
            enabled: false,
            ..Default::default()
        };
        assert!(security_header_pairs(&cfg).unwrap().is_empty());
    }

    #[test]
    fn opt_in_headers_emitted_when_set() {
        let cfg = SecurityHeadersConfig {
            content_security_policy: "default-src 'self'".to_string(),
            strict_transport_security: "max-age=31536000; includeSubDomains".to_string(),
            ..Default::default()
        };
        let names = names(&cfg);
        assert!(names.contains(&"content-security-policy".to_string()));
        assert!(names.contains(&"strict-transport-security".to_string()));
    }

    #[test]
    fn empty_value_disables_individual_header() {
        let cfg = SecurityHeadersConfig {
            frame_options: String::new(),
            ..Default::default()
        };
        assert!(!names(&cfg).contains(&"x-frame-options".to_string()));
    }

    #[test]
    fn invalid_header_value_is_rejected() {
        let cfg = SecurityHeadersConfig {
            referrer_policy: "bad\nvalue".to_string(),
            ..Default::default()
        };
        assert!(security_header_pairs(&cfg).is_err());
    }
}

#[cfg(test)]
mod cors_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{HeaderMap, Request};
    use tower::ServiceExt;
    use url::Url;

    fn cors_config(allow_any_origin_without_credentials: bool) -> CorsConfig {
        CorsConfig {
            allowed_origins: vec![CorsOrigin::Url(Url::parse("https://app.doubleword.ai").unwrap())],
            allow_credentials: true,
            max_age: Some(3600),
            exposed_headers: vec![],
            allow_any_origin_without_credentials,
        }
    }

    async fn preflight(cors: &CorsConfig, origin: &str) -> HeaderMap {
        let app = apply_cors(Router::new().route("/", get(|| async { "ok" })), cors).expect("apply_cors");
        let resp = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/")
                    .header("origin", origin)
                    .header("access-control-request-method", "GET")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.headers().clone()
    }

    fn acao(h: &HeaderMap) -> Option<String> {
        h.get("access-control-allow-origin").map(|v| v.to_str().unwrap().to_string())
    }

    fn acac(h: &HeaderMap) -> Option<String> {
        h.get("access-control-allow-credentials").map(|v| v.to_str().unwrap().to_string())
    }

    #[tokio::test]
    async fn third_party_origin_allowed_without_credentials() {
        let h = preflight(&cors_config(true), "https://evil.example.com").await;
        assert_eq!(
            acao(&h).as_deref(),
            Some("https://evil.example.com"),
            "non-allowlisted origin should be reflected when the flag is set",
        );
        assert_eq!(
            acac(&h),
            None,
            "non-allowlisted origin must NOT receive Access-Control-Allow-Credentials",
        );
    }

    #[tokio::test]
    async fn first_party_origin_keeps_credentials() {
        let h = preflight(&cors_config(true), "https://app.doubleword.ai").await;
        assert_eq!(acao(&h).as_deref(), Some("https://app.doubleword.ai"));
        assert_eq!(acac(&h).as_deref(), Some("true"), "allowlisted origin must keep credentialed CORS",);
    }

    #[tokio::test]
    async fn flag_off_blocks_non_allowlisted_origin() {
        let h = preflight(&cors_config(false), "https://evil.example.com").await;
        assert_eq!(
            acao(&h),
            None,
            "with the flag off, non-allowlisted origins get no CORS (unchanged behavior)",
        );
    }

    #[tokio::test]
    async fn near_miss_origin_does_not_get_credentials() {
        let h = preflight(&cors_config(true), "https://app.doubleword.ai.evil.com").await;
        assert_eq!(acao(&h).as_deref(), Some("https://app.doubleword.ai.evil.com"),);
        assert_eq!(acac(&h), None, "a look-alike of an allowlisted origin must not receive credentials",);
    }

    #[tokio::test]
    async fn request_without_origin_gets_no_cors_headers() {
        let app = apply_cors(Router::new().route("/", get(|| async { "ok" })), &cors_config(true)).expect("apply_cors");
        let resp = app
            .oneshot(Request::builder().method("GET").uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let h = resp.headers();
        assert!(h.get("access-control-allow-origin").is_none());
        assert!(h.get("access-control-allow-credentials").is_none());
    }

    async fn actual_get(cors: &CorsConfig, origin: &str) -> HeaderMap {
        let app = apply_cors(Router::new().route("/", get(|| async { "ok" })), cors).expect("apply_cors");
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/")
                    .header("origin", origin)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        resp.headers().clone()
    }

    #[tokio::test]
    async fn third_party_actual_get_is_reflected_without_credentials() {
        let h = actual_get(&cors_config(true), "https://evil.example.com").await;
        assert_eq!(acao(&h).as_deref(), Some("https://evil.example.com"));
        assert_eq!(acac(&h), None);
    }

    #[tokio::test]
    async fn first_party_actual_get_keeps_credentials() {
        let h = actual_get(&cors_config(true), "https://app.doubleword.ai").await;
        assert_eq!(acao(&h).as_deref(), Some("https://app.doubleword.ai"));
        assert_eq!(acac(&h).as_deref(), Some("true"));
    }
}
