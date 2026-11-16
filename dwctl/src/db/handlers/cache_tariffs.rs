
use crate::config::CachePricingConfig;
use crate::db::errors::Result;
use crate::types::DeploymentId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{Connection, PgConnection};
use std::collections::HashMap;
use tracing::instrument;

#[derive(Debug, Default, Clone)]
pub struct CacheTariffOverrides {
    pub write_multiplier_5m: Option<Decimal>,
    pub write_multiplier_1h: Option<Decimal>,
    pub write_multiplier_24h: Option<Decimal>,
    pub read_multiplier: Option<Decimal>,
    pub min_prefix_tokens: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct ActiveTariff {
    pub write_multiplier_5m: Decimal,
    pub write_multiplier_1h: Decimal,
    pub write_multiplier_24h: Decimal,
    pub read_multiplier: Decimal,
    pub min_prefix_tokens: i32,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
struct ActiveTariffRow {
    deployed_model_id: DeploymentId,
    write_multiplier_5m: Decimal,
    write_multiplier_1h: Decimal,
    write_multiplier_24h: Decimal,
    read_multiplier: Decimal,
    min_prefix_tokens: i32,
    valid_from: DateTime<Utc>,
    valid_until: Option<DateTime<Utc>>,
}

pub struct CacheTariffs<'c> {
    db: &'c mut PgConnection,
}

impl<'c> CacheTariffs<'c> {
    pub fn new(db: &'c mut PgConnection) -> Self {
        Self { db }
    }

    pub async fn get_class_prices_bulk(
        &mut self,
        ids: &[DeploymentId],
        account: uuid::Uuid,
    ) -> Result<HashMap<DeploymentId, HashMap<String, ActiveTariff>>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            deployed_model_id: DeploymentId,
            serving_class: String,
            write_multiplier_5m: Decimal,
            write_multiplier_1h: Decimal,
            write_multiplier_24h: Decimal,
            read_multiplier: Decimal,
            min_prefix_tokens: i32,
            valid_from: DateTime<Utc>,
            valid_until: Option<DateTime<Utc>>,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT DISTINCT ON (own.deployed_model_id, own.serving_class)
                own.deployed_model_id, own.serving_class, own.write_multiplier_5m, own.write_multiplier_1h,
                own.write_multiplier_24h, own.read_multiplier, general.min_prefix_tokens, own.valid_from, own.valid_until
             FROM model_cache_tariffs own JOIN model_cache_tariffs general ON general.deployed_model_id = own.deployed_model_id
                AND general.user_id IS NULL AND general.serving_class IS NULL AND general.valid_from <= NOW() AND (general.valid_until IS NULL OR general.valid_until > NOW())
             WHERE own.deployed_model_id = ANY($1) AND own.user_id = $2 AND own.serving_class IS NOT NULL
                AND own.valid_from <= NOW() AND (own.valid_until IS NULL OR own.valid_until > NOW())
             ORDER BY own.deployed_model_id, own.serving_class, own.valid_from DESC, general.valid_from DESC",
        )
        .bind(ids)
        .bind(account)
        .fetch_all(&mut *self.db)
        .await?;
        let mut result: HashMap<DeploymentId, HashMap<String, ActiveTariff>> = HashMap::new();
        for r in rows {
            result.entry(r.deployed_model_id).or_default().insert(
                r.serving_class,
                ActiveTariff {
                    write_multiplier_5m: r.write_multiplier_5m,
                    write_multiplier_1h: r.write_multiplier_1h,
                    write_multiplier_24h: r.write_multiplier_24h,
                    read_multiplier: r.read_multiplier,
                    min_prefix_tokens: r.min_prefix_tokens,
                    valid_from: r.valid_from,
                    valid_until: r.valid_until,
                },
            );
        }
        Ok(result)
    }

    #[instrument(level = "debug", skip(self, defaults, overrides), fields(deployed_model_id = %model_id), err)]
    pub async fn enable(&mut self, model_id: DeploymentId, defaults: &CachePricingConfig, overrides: CacheTariffOverrides) -> Result<()> {
        let mut tx = self.db.begin().await?;

        sqlx::query!(
            r#"UPDATE model_cache_tariffs SET valid_until = now()
               WHERE deployed_model_id = $1
                 AND user_id IS NULL AND serving_class IS NULL
                 AND valid_from <= now()
                 AND (valid_until IS NULL OR valid_until > now())"#,
            model_id,
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query!(
            r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h,
                  read_multiplier, min_prefix_tokens)
               VALUES ($1, $2, $3, $4, $5, $6)"#,
            model_id,
            overrides.write_multiplier_5m.unwrap_or(defaults.default_write_multiplier_5m),
            overrides.write_multiplier_1h.unwrap_or(defaults.default_write_multiplier_1h),
            overrides.write_multiplier_24h.unwrap_or(defaults.default_write_multiplier_24h),
            overrides.read_multiplier.unwrap_or(defaults.default_read_multiplier),
            overrides.min_prefix_tokens.unwrap_or(defaults.default_min_prefix_tokens),
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self), fields(deployed_model_id = %model_id), err)]
    pub async fn disable(&mut self, model_id: DeploymentId) -> Result<bool> {
        let res = sqlx::query!(
            r#"UPDATE model_cache_tariffs SET valid_until = now()
               WHERE deployed_model_id = $1
                 AND user_id IS NULL AND serving_class IS NULL
                 AND valid_from <= now()
                 AND (valid_until IS NULL OR valid_until > now())"#,
            model_id,
        )
        .execute(&mut *self.db)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    #[instrument(level = "debug", skip(self), fields(deployed_model_id = %model_id), err)]
    pub async fn get_active(&mut self, model_id: DeploymentId) -> Result<Option<ActiveTariff>> {
        let row = sqlx::query_as!(
            ActiveTariff,
            r#"SELECT write_multiplier_5m, write_multiplier_1h, write_multiplier_24h,
                      read_multiplier, min_prefix_tokens, valid_from, valid_until
               FROM model_cache_tariffs
               WHERE deployed_model_id = $1
                 AND user_id IS NULL AND serving_class IS NULL
                 AND valid_from <= now()
                 AND (valid_until IS NULL OR valid_until > now())
               ORDER BY valid_from DESC
               LIMIT 1"#,
            model_id,
        )
        .fetch_optional(&mut *self.db)
        .await?;
        Ok(row)
    }

    #[instrument(level = "debug", skip(self, model_ids), fields(model_count = model_ids.len()), err)]
    pub async fn get_active_for_account_bulk(
        &mut self,
        model_ids: &[DeploymentId],
        account: uuid::Uuid,
    ) -> Result<HashMap<DeploymentId, ActiveTariff>> {
        if model_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query_as::<_, ActiveTariffRow>(
            r#"SELECT DISTINCT ON (deployed_model_id)
                      deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h,
                      read_multiplier, min_prefix_tokens, valid_from, valid_until
               FROM model_cache_tariffs
               WHERE deployed_model_id = ANY($1)
                 AND user_id = $2 AND serving_class IS NULL
                 AND valid_from <= now()
                 AND (valid_until IS NULL OR valid_until > now())
               ORDER BY deployed_model_id, valid_from DESC"#,
        )
        .bind(model_ids)
        .bind(account)
        .fetch_all(&mut *self.db)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.deployed_model_id,
                    ActiveTariff {
                        write_multiplier_5m: row.write_multiplier_5m,
                        write_multiplier_1h: row.write_multiplier_1h,
                        write_multiplier_24h: row.write_multiplier_24h,
                        read_multiplier: row.read_multiplier,
                        min_prefix_tokens: row.min_prefix_tokens,
                        valid_from: row.valid_from,
                        valid_until: row.valid_until,
                    },
                )
            })
            .collect())
    }

    #[instrument(level = "debug", skip(self, model_ids), fields(model_count = model_ids.len()), err)]
    pub async fn get_active_bulk(&mut self, model_ids: &[DeploymentId]) -> Result<HashMap<DeploymentId, ActiveTariff>> {
        if model_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query_as::<_, ActiveTariffRow>(
            r#"SELECT DISTINCT ON (deployed_model_id)
                      deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h,
                      read_multiplier, min_prefix_tokens, valid_from, valid_until
               FROM model_cache_tariffs
               WHERE deployed_model_id = ANY($1)
                 AND user_id IS NULL AND serving_class IS NULL
                 AND valid_from <= now()
                 AND (valid_until IS NULL OR valid_until > now())
               ORDER BY deployed_model_id, valid_from DESC"#,
        )
        .bind(model_ids)
        .fetch_all(&mut *self.db)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.deployed_model_id,
                    ActiveTariff {
                        write_multiplier_5m: row.write_multiplier_5m,
                        write_multiplier_1h: row.write_multiplier_1h,
                        write_multiplier_24h: row.write_multiplier_24h,
                        read_multiplier: row.read_multiplier,
                        min_prefix_tokens: row.min_prefix_tokens,
                        valid_from: row.valid_from,
                        valid_until: row.valid_until,
                    },
                )
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prompt_cache::ModelConfigResolver;
    use crate::test::utils::{create_test_endpoint, create_test_model, create_test_user};
    use sqlx::PgPool;

    #[dwctl_test_macros::test]
    async fn enable_then_disable_toggles_resolver(pool: PgPool) {
        let user = create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let id = create_test_model(&pool, "m", "enable-alias", endpoint, user.id).await;
        let defaults = CachePricingConfig::default();

        let resolver = ModelConfigResolver::new(pool.clone());
        assert!(!resolver.resolve("enable-alias").await.unwrap().enabled);

        {
            let mut conn = pool.acquire().await.unwrap();
            CacheTariffs::new(&mut conn)
                .enable(id, &defaults, CacheTariffOverrides::default())
                .await
                .unwrap();
        }
        let cfg = ModelConfigResolver::new(pool.clone()).resolve("enable-alias").await.unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.min_prefix_tokens, defaults.default_min_prefix_tokens.max(0) as u32);

        {
            let mut conn = pool.acquire().await.unwrap();
            assert!(CacheTariffs::new(&mut conn).disable(id).await.unwrap());
        }
        assert!(!ModelConfigResolver::new(pool).resolve("enable-alias").await.unwrap().enabled);
    }

    #[dwctl_test_macros::test]
    async fn enable_twice_supersedes_keeping_history(pool: PgPool) {
        let user = create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let id = create_test_model(&pool, "m", "reprice-alias", endpoint, user.id).await;
        let defaults = CachePricingConfig::default();

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = CacheTariffs::new(&mut conn);
        repo.enable(id, &defaults, CacheTariffOverrides::default()).await.unwrap();
        repo.enable(
            id,
            &defaults,
            CacheTariffOverrides {
                min_prefix_tokens: Some(2048),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let total = sqlx::query_scalar!("SELECT COUNT(*) FROM model_cache_tariffs WHERE deployed_model_id = $1", id)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        let active = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM model_cache_tariffs WHERE deployed_model_id = $1 AND valid_until IS NULL",
            id
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(total, Some(2), "old version retained for audit");
        assert_eq!(active, Some(1), "exactly one active version");
    }

    #[dwctl_test_macros::test]
    async fn partial_unique_index_rejects_two_active_versions(pool: PgPool) {
        let user = create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let id = create_test_model(&pool, "m", "dup-active-alias", endpoint, user.id).await;

        const INSERT_ACTIVE: &str = "INSERT INTO model_cache_tariffs
               (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens)
             VALUES ($1, 1.25, 2.0, 2.5, 1024)";

        sqlx::query(INSERT_ACTIVE)
            .bind(id)
            .execute(&pool)
            .await
            .expect("first active row inserts");

        let err = sqlx::query(INSERT_ACTIVE)
            .bind(id)
            .execute(&pool)
            .await
            .expect_err("second active row must be rejected");
        assert!(
            err.as_database_error().is_some_and(|e| e.is_unique_violation()),
            "expected a unique violation from idx_model_cache_tariffs_unique_active, got: {err:?}"
        );
    }

    #[dwctl_test_macros::test]
    async fn get_active_reflects_enable_disable(pool: PgPool) {
        let user = create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let endpoint = create_test_endpoint(&pool, "ep", user.id).await;
        let id = create_test_model(&pool, "m", "get-active-alias", endpoint, user.id).await;
        let defaults = CachePricingConfig::default();

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = CacheTariffs::new(&mut conn);

        assert!(repo.get_active(id).await.unwrap().is_none());

        repo.enable(
            id,
            &defaults,
            CacheTariffOverrides {
                min_prefix_tokens: Some(2048),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let active = repo.get_active(id).await.unwrap().expect("active after enable");
        assert_eq!(active.min_prefix_tokens, 2048);
        assert_eq!(active.write_multiplier_5m, defaults.default_write_multiplier_5m);
        assert!(active.valid_until.is_none(), "active version is open-ended");

        repo.disable(id).await.unwrap();
        assert!(repo.get_active(id).await.unwrap().is_none());
    }
}
