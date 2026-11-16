
use std::time::Duration;

use moka::future::Cache;

use crate::crypto::ApiKeyHash;
use crate::db::handlers::api_keys::ApiKeys;
use crate::types::UserId;

use super::index::CacheResult;
use super::metrics as cache_metrics;

#[derive(Clone)]
pub struct PrincipalResolver {
    pool: sqlx_pool_router::DynPools,
    l1: Cache<[u8; 32], Option<UserId>>,
}

impl PrincipalResolver {
    pub fn new(pool: impl sqlx_pool_router::PoolProvider) -> Self {
        Self::with_capacity(pool, 100_000)
    }

    pub fn with_capacity(pool: impl sqlx_pool_router::PoolProvider, max_entries: u64) -> Self {
        let l1 = Cache::builder()
            .max_capacity(max_entries)
            .time_to_live(Duration::from_secs(3600))
            .build();
        Self {
            pool: sqlx_pool_router::DynPools::new(pool),
            l1,
        }
    }

    pub async fn resolve(&self, token_hash: &ApiKeyHash) -> CacheResult<Option<UserId>> {
        if let Some(cached) = self.l1.get(token_hash).await {
            cache_metrics::record_principal_resolve(if cached.is_some() { "hit" } else { "unknown_key" });
            return Ok(cached);
        }
        let mut conn = self.pool.write().acquire().await?;
        let user_id = ApiKeys::new(&mut conn).get_user_id_by_hash(token_hash).await?;
        cache_metrics::record_principal_resolve(if user_id.is_some() { "miss" } else { "unknown_key" });
        self.l1.insert(*token_hash, user_id).await;
        Ok(user_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::users::Role;
    use crate::test::utils::{create_test_api_key_for_user, create_test_user};

    #[dwctl_test_macros::test]
    async fn resolves_validated_key_to_user(pool: sqlx::PgPool) {
        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await;

        let resolver = PrincipalResolver::new(pool);
        let hash = crate::crypto::hash_api_key(&key.secret);
        assert_eq!(resolver.resolve(&hash).await.unwrap(), Some(user.id));
        assert_eq!(resolver.resolve(&hash).await.unwrap(), Some(user.id));
    }

    #[dwctl_test_macros::test]
    async fn unknown_token_resolves_none(pool: sqlx::PgPool) {
        let resolver = PrincipalResolver::new(pool);
        assert_eq!(
            resolver.resolve(&crate::crypto::hash_api_key("not-a-real-secret")).await.unwrap(),
            None
        );
    }
}
