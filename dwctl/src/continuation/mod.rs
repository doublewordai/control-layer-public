
pub mod accumulate;
pub mod detect;
pub mod dsv4;
pub mod layer;
pub mod metrics;
pub mod render;
pub mod resume;
pub mod rewrap;

#[cfg(test)]
mod resume_tests;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use dashmap::DashMap;
use moka::future::Cache;
use sqlx::PgPool;

use crate::UserId;
use crate::crypto::ApiKeyHash;
#[cfg(test)]
use crate::crypto::hash_api_key;
use crate::db::handlers::api_keys::ApiKeys;
use crate::db::models::api_keys::ApiKeyPurpose;
use crate::metrics::errors::component;

pub use layer::{ContinuationState, continuation_middleware};

const ROUTE_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

pub fn global_key_owner() -> UserId {
    uuid::Uuid::nil()
}

pub async fn provision_global_key(pool: &PgPool) -> anyhow::Result<ApiKeyHash> {
    let owner = global_key_owner();
    let mut tx = pool.begin().await?;
    let id = ApiKeys::new(&mut tx)
        .get_or_create_hidden_key(owner, ApiKeyPurpose::Continuation, owner)
        .await?;
    let hash = sqlx::query_scalar!("SELECT secret_hash FROM api_keys WHERE id = $1", id)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;
    ApiKeyHash::try_from(hash.as_slice()).map_err(|_| anyhow::anyhow!("continuation key {id} has a malformed secret hash"))
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct RouteInfo {
    pub render_kwargs: Option<serde_json::Value>,
    pub strip_leading_bos: bool,
}

impl RouteInfo {
    pub fn thinking(&self) -> bool {
        let Some(kwargs) = self.render_kwargs.as_ref() else {
            return true;
        };
        if let Some(mode) = kwargs.get("thinking_mode").and_then(|v| v.as_str()) {
            return !mode.eq_ignore_ascii_case("chat");
        }
        kwargs.get("thinking").and_then(|v| v.as_bool()).unwrap_or(true)
    }

    pub fn merged_render_kwargs(&self, request_kwargs: Option<&serde_json::Value>) -> Option<serde_json::Value> {
        match (self.render_kwargs.as_ref(), request_kwargs) {
            (None, other) => other.cloned(),
            (Some(route), None) => Some(route.clone()),
            (Some(serde_json::Value::Object(route)), Some(serde_json::Value::Object(request))) => {
                let mut merged = route.clone();
                for (k, v) in request {
                    merged.insert(k.clone(), v.clone());
                }
                Some(serde_json::Value::Object(merged))
            }
            (Some(route), Some(request)) => Some(if request.is_null() { route.clone() } else { request.clone() }),
        }
    }
}

pub struct ContinuationRoutes {
    routes: RwLock<Arc<HashMap<String, RouteInfo>>>,
}

impl Default for ContinuationRoutes {
    fn default() -> Self {
        Self::new()
    }
}

impl ContinuationRoutes {
    pub fn new() -> Self {
        Self {
            routes: RwLock::new(Arc::new(HashMap::new())),
        }
    }

    pub fn with_models<I: IntoIterator<Item = String>>(models: I) -> Self {
        Self::with_routes(models.into_iter().map(|m| (m, RouteInfo::default())))
    }

    pub fn with_routes<I: IntoIterator<Item = (String, RouteInfo)>>(routes: I) -> Self {
        Self {
            routes: RwLock::new(Arc::new(routes.into_iter().collect())),
        }
    }

    pub fn is_enabled(&self, model: &str) -> bool {
        self.get(model).is_some()
    }

    pub fn get(&self, model: &str) -> Option<RouteInfo> {
        self.routes.read().ok()?.get(model).cloned()
    }

    pub fn len(&self) -> usize {
        self.routes.read().map(|s| s.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn store(&self, routes: HashMap<String, RouteInfo>) {
        if let Ok(mut guard) = self.routes.write() {
            *guard = Arc::new(routes);
        }
    }

    pub async fn refresh(&self, pool: &PgPool) -> anyhow::Result<()> {
        let rows = sqlx::query!(
            r#"
            SELECT DISTINCT ON (cm.alias)
                cm.alias,
                first_value(dmc.render_kwargs) OVER (
                    PARTITION BY dmc.composite_model_id
                    ORDER BY (dmc.render_kwargs IS NULL), dmc.sort_order ASC
                ) AS render_kwargs,
                dmc.strip_leading_bos
            FROM deployed_model_components dmc
            JOIN deployed_models cm ON cm.id = dmc.composite_model_id
            JOIN deployed_models dm ON dm.id = dmc.deployed_model_id
            WHERE dmc.pool = 'completions'
              AND dmc.enabled = true
              AND cm.deleted = false
              -- A class-routed model's targets have no completions pool.
              AND NOT class_routes_active(cm.id)
              AND dm.deleted = false
            ORDER BY
                cm.alias,
                EXISTS (
                    SELECT 1 FROM deployed_model_components shared
                    WHERE shared.composite_model_id = dmc.composite_model_id
                      AND shared.deployed_model_id = dmc.deployed_model_id
                      AND shared.pool = 'default'
                ),
                dmc.sort_order ASC
            "#
        )
        .fetch_all(pool)
        .await?;
        self.store(
            rows.into_iter()
                .map(|row| {
                    (
                        row.alias,
                        RouteInfo {
                            render_kwargs: row.render_kwargs,
                            strip_leading_bos: row.strip_leading_bos,
                        },
                    )
                })
                .collect(),
        );
        Ok(())
    }

    pub fn spawn_poller(self: Arc<Self>, pools: sqlx_pool_router::DynPools) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(ROUTE_REFRESH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if let Err(e) = self.refresh(&pools.write()).await {
                    crate::background_error!(
                        component::CONTINUATION,
                        "route_refresh",
                        Warning,
                        error = %e,
                        "Failed to refresh continuation routes; keeping the previous set"
                    );
                }
            }
        });
    }
}

pub struct InflightLimiter {
    max: u32,
    counts: DashMap<String, AtomicU32>,
}

impl InflightLimiter {
    pub fn new(max: u32) -> Self {
        Self {
            max,
            counts: DashMap::new(),
        }
    }

    pub fn try_acquire(self: &Arc<Self>, model: &str) -> Option<InflightGuard> {
        let entry = self.counts.entry(model.to_string()).or_default();
        #[allow(deprecated)]
        let acquired = entry
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| (n < self.max).then_some(n + 1))
            .is_ok();
        drop(entry);
        acquired.then(|| InflightGuard {
            limiter: Arc::clone(self),
            model: model.to_string(),
        })
    }

    pub fn in_flight(&self, model: &str) -> u32 {
        self.counts.get(model).map(|c| c.load(Ordering::SeqCst)).unwrap_or(0)
    }
}

pub struct InflightGuard {
    limiter: Arc<InflightLimiter>,
    model: String,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        if let Some(entry) = self.limiter.counts.get(&self.model) {
            #[allow(deprecated)]
            let _ = entry.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| Some(n.saturating_sub(1)));
        }
    }
}

#[derive(Clone)]
pub struct PurposeResolver {
    pools: sqlx_pool_router::DynPools,
    l1: Cache<[u8; 32], Option<ApiKeyPurpose>>,
}

impl PurposeResolver {
    pub fn new(pools: impl sqlx_pool_router::PoolProvider) -> Self {
        Self {
            pools: sqlx_pool_router::DynPools::new(pools),
            l1: Cache::builder()
                .max_capacity(100_000)
                .time_to_live(Duration::from_secs(3600))
                .build(),
        }
    }

    pub async fn resolve(&self, token_hash: &ApiKeyHash) -> Option<ApiKeyPurpose> {
        if let Some(cached) = self.l1.get(token_hash).await {
            return cached;
        }
        let Ok(mut conn) = self.pools.write().acquire().await else {
            return None;
        };
        let looked_up = ApiKeys::new(&mut conn).get_user_info_by_hash(token_hash).await;
        drop(conn);
        match looked_up {
            Ok(found) => {
                let purpose = found.map(|(_, _, purpose)| purpose);
                self.l1.insert(*token_hash, purpose.clone()).await;
                purpose
            }
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::users::Role;
    use crate::test::utils::{create_test_endpoint, create_test_model, create_test_user};

    #[dwctl_test_macros::test]
    async fn provision_global_key_is_idempotent_hidden_and_system_owned(pool: PgPool) {
        let first = provision_global_key(&pool).await.unwrap();
        let second = provision_global_key(&pool).await.unwrap();
        assert_eq!(first, second);

        let row = sqlx::query!(
            r#"SELECT user_id, purpose, hidden FROM api_keys WHERE secret_hash = $1"#,
            &first[..]
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.user_id, global_key_owner());
        assert_eq!(row.purpose, "continuation");
        assert!(row.hidden);
    }

    async fn add_component(pool: &PgPool, composite: uuid::Uuid, component: uuid::Uuid, component_pool: &str) {
        add_component_at(pool, composite, component, component_pool, 0).await
    }

    async fn add_component_at(pool: &PgPool, composite: uuid::Uuid, component: uuid::Uuid, component_pool: &str, sort_order: i32) {
        sqlx::query!(
            "INSERT INTO deployed_model_components (composite_model_id, deployed_model_id, weight, enabled, sort_order, pool)
             VALUES ($1, $2, 1, true, $4, $3)",
            composite,
            component,
            component_pool,
            sort_order
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn create_composite(pool: &PgPool, alias: &str, created_by: UserId) -> uuid::Uuid {
        let id = uuid::Uuid::new_v4();
        sqlx::query!(
            "INSERT INTO deployed_models (id, model_name, alias, created_by, deleted, is_composite)
             VALUES ($1, $2, $3, $4, false, true)",
            id,
            alias,
            alias,
            created_by
        )
        .execute(pool)
        .await
        .unwrap();
        id
    }

    #[dwctl_test_macros::test]
    async fn routes_only_include_composites_with_a_completions_pool(pool: PgPool) {
        let user = create_test_user(&pool, Role::PlatformManager).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;

        let resumable = create_composite(&pool, "alias-resumable", user.id).await;
        let plain = create_composite(&pool, "alias-plain", user.id).await;
        let dynamo = create_test_model(&pool, "m-dynamo", "dynamo", endpoint, user.id).await;
        let openrouter = create_test_model(&pool, "m-or", "openrouter", endpoint, user.id).await;
        let fireworks = create_test_model(&pool, "m-fw", "fireworks", endpoint, user.id).await;

        add_component(&pool, resumable, dynamo, "default").await;
        add_component(&pool, plain, openrouter, "default").await;

        let routes = ContinuationRoutes::new();
        routes.refresh(&pool).await.unwrap();
        assert!(routes.is_empty(), "a composite with only a default pool is not resumable");
        assert!(!routes.is_enabled("alias-resumable"));

        add_component_at(&pool, resumable, dynamo, "completions", 0).await;
        add_component_at(&pool, resumable, fireworks, "completions", 1).await;
        routes.refresh(&pool).await.unwrap();

        assert!(routes.is_enabled("alias-resumable"));
        assert!(
            !routes.is_enabled("alias-plain"),
            "a composite with no completions pool is never resumable"
        );
        assert_eq!(routes.len(), 1);
        let route = routes.get("alias-resumable").unwrap();
        assert_eq!(route.render_kwargs, None);
        assert!(!route.strip_leading_bos);

        sqlx::query!(
            "UPDATE deployed_model_components SET enabled = false WHERE composite_model_id = $1 AND pool = 'completions'",
            resumable
        )
        .execute(&pool)
        .await
        .unwrap();
        routes.refresh(&pool).await.unwrap();
        assert!(routes.is_empty());
    }

    #[dwctl_test_macros::test]
    async fn class_routed_models_are_not_resumable(pool: PgPool) {
        let user = create_test_user(&pool, Role::PlatformManager).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let resumable = create_composite(&pool, "alias-resumable", user.id).await;
        let dynamo = create_test_model(&pool, "m-dynamo", "dynamo", endpoint, user.id).await;
        add_component(&pool, resumable, dynamo, "default").await;
        add_component_at(&pool, resumable, dynamo, "completions", 0).await;

        let routes = ContinuationRoutes::new();
        routes.refresh(&pool).await.unwrap();
        assert!(routes.is_enabled("alias-resumable"));

        sqlx::query!("UPDATE deployed_models SET routing_mode = 'class_routes' WHERE id = $1", resumable)
            .execute(&pool)
            .await
            .unwrap();
        routes.refresh(&pool).await.unwrap();
        assert!(
            routes.is_enabled("alias-resumable"),
            "an activation without a standard class serves the legacy route, continuation included"
        );

        sqlx::query!(
            "INSERT INTO model_serving_classes (deployed_model_id, class_key, display_name, inference_endpoint_id, upstream_model_name)
             VALUES ($1, 'standard', 'Standard', $2, 'upstream')",
            resumable,
            endpoint
        )
        .execute(&pool)
        .await
        .unwrap();
        routes.refresh(&pool).await.unwrap();
        assert!(!routes.is_enabled("alias-resumable"), "class routes have no completions pool");
    }

    #[dwctl_test_macros::test]
    async fn per_route_config_reaches_the_cache(pool: PgPool) {
        let user = create_test_user(&pool, Role::PlatformManager).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let composite = create_composite(&pool, "dsv4-flash", user.id).await;
        let fireworks = create_test_model(&pool, "m-fw", "fireworks", endpoint, user.id).await;
        add_component(&pool, composite, fireworks, "completions").await;
        sqlx::query!(
            r#"UPDATE deployed_model_components
               SET render_kwargs = '{"thinking_mode": "chat"}'::jsonb,
                   strip_leading_bos = true,
                   continuation_validated_at = NOW()
               WHERE composite_model_id = $1"#,
            composite
        )
        .execute(&pool)
        .await
        .unwrap();

        let routes = ContinuationRoutes::new();
        routes.refresh(&pool).await.unwrap();
        let route = routes.get("dsv4-flash").expect("the canary route");
        assert_eq!(route.render_kwargs, Some(serde_json::json!({"thinking_mode": "chat"})));
        assert!(route.strip_leading_bos);
        assert!(!route.thinking(), "a chat-mode route must not close a think tag");
    }

    #[dwctl_test_macros::test]
    async fn the_route_config_comes_from_the_completions_only_member(pool: PgPool) {
        let user = create_test_user(&pool, Role::PlatformManager).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let composite = create_composite(&pool, "dsv4-flash", user.id).await;
        let dynamo = create_test_model(&pool, "m-dynamo", "dynamo", endpoint, user.id).await;
        let fireworks = create_test_model(&pool, "m-fw", "fireworks", endpoint, user.id).await;

        add_component(&pool, composite, dynamo, "default").await;
        add_component_at(&pool, composite, dynamo, "completions", 0).await;
        add_component_at(&pool, composite, fireworks, "completions", 1).await;
        sqlx::query!(
            r#"UPDATE deployed_model_components
               SET render_kwargs = '{"thinking_mode": "chat"}'::jsonb, strip_leading_bos = true
               WHERE composite_model_id = $1 AND deployed_model_id = $2 AND pool = 'completions'"#,
            composite,
            fireworks
        )
        .execute(&pool)
        .await
        .unwrap();

        let routes = ContinuationRoutes::new();
        routes.refresh(&pool).await.unwrap();
        let route = routes.get("dsv4-flash").expect("the canary route");
        assert_eq!(
            route.render_kwargs,
            Some(serde_json::json!({"thinking_mode": "chat"})),
            "the validated target's config wins over the shared first hop's defaults"
        );
        assert!(route.strip_leading_bos);
    }

    #[dwctl_test_macros::test]
    async fn the_first_non_null_render_kwargs_in_the_pool_wins(pool: PgPool) {
        let user = create_test_user(&pool, Role::PlatformManager).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let composite = create_composite(&pool, "dsv4-flash", user.id).await;
        let blackhole = create_test_model(&pool, "m-dynamo", "dynamo", endpoint, user.id).await;
        let fireworks = create_test_model(&pool, "m-fw", "fireworks", endpoint, user.id).await;

        add_component_at(&pool, composite, blackhole, "completions", 0).await;
        add_component_at(&pool, composite, fireworks, "completions", 1).await;
        sqlx::query!(
            r#"UPDATE deployed_model_components
               SET render_kwargs = '{"thinking_mode": "chat"}'::jsonb
               WHERE composite_model_id = $1 AND deployed_model_id = $2 AND pool = 'completions'"#,
            composite,
            fireworks
        )
        .execute(&pool)
        .await
        .unwrap();

        let routes = ContinuationRoutes::new();
        routes.refresh(&pool).await.unwrap();
        let route = routes.get("dsv4-flash").expect("the canary route");
        assert_eq!(
            route.render_kwargs,
            Some(serde_json::json!({"thinking_mode": "chat"})),
            "a NULL at sort 0 must not override the mode a later member states"
        );
        assert!(!route.thinking(), "a chat-mode route must not close a think tag");
    }

    #[test]
    fn thinking_is_read_from_the_route_kwargs() {
        let route = |kwargs: Option<serde_json::Value>| RouteInfo {
            render_kwargs: kwargs,
            strip_leading_bos: false,
        };
        assert!(route(None).thinking());
        assert!(route(Some(serde_json::json!({}))).thinking());
        assert!(route(Some(serde_json::json!({"thinking_mode": "thinking"}))).thinking());
        assert!(!route(Some(serde_json::json!({"thinking_mode": "chat"}))).thinking());
        assert!(!route(Some(serde_json::json!({"thinking_mode": "CHAT"}))).thinking());
        assert!(!route(Some(serde_json::json!({"thinking": false}))).thinking());
        assert!(route(Some(serde_json::json!({"thinking": true}))).thinking());
    }

    #[test]
    fn request_kwargs_override_the_route_key_by_key() {
        let route = RouteInfo {
            render_kwargs: Some(serde_json::json!({"thinking_mode": "chat", "tool_style": "dsml"})),
            strip_leading_bos: false,
        };
        assert_eq!(route.merged_render_kwargs(None), route.render_kwargs);
        assert_eq!(
            route.merged_render_kwargs(Some(&serde_json::json!({"thinking_mode": "thinking"}))),
            Some(serde_json::json!({"thinking_mode": "thinking", "tool_style": "dsml"}))
        );
        let bare = RouteInfo::default();
        assert_eq!(
            bare.merged_render_kwargs(Some(&serde_json::json!({"thinking": true}))),
            Some(serde_json::json!({"thinking": true}))
        );
        assert_eq!(bare.merged_render_kwargs(None), None);
    }

    #[dwctl_test_macros::test]
    async fn purpose_resolver_reads_through_and_memoises(pool: PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = crate::test::utils::create_test_api_key_for_user(&pool, user.id).await;

        let resolver = PurposeResolver::new(pool);
        let hash = hash_api_key(&key.secret);
        assert_eq!(resolver.resolve(&hash).await, Some(ApiKeyPurpose::Realtime));
        assert_eq!(resolver.resolve(&hash).await, Some(ApiKeyPurpose::Realtime));
        assert_eq!(resolver.resolve(&hash_api_key("not-a-real-secret")).await, None);
    }

    #[test]
    fn inflight_limiter_caps_per_model_and_releases_on_drop() {
        let limiter = Arc::new(InflightLimiter::new(2));
        let a1 = limiter.try_acquire("m").expect("first slot");
        let _a2 = limiter.try_acquire("m").expect("second slot");
        assert_eq!(limiter.in_flight("m"), 2);
        assert!(limiter.try_acquire("m").is_none(), "the cap is per model");
        assert!(limiter.try_acquire("other").is_some());

        drop(a1);
        assert_eq!(limiter.in_flight("m"), 1);
        assert!(limiter.try_acquire("m").is_some(), "a released slot is reusable");
    }
}
