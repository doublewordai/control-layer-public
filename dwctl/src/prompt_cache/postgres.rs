
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rand::RngExt;
use tracing::instrument;

use super::index::{CacheEntry, CacheError, CacheIndex, CacheMatch, CacheResult, IndexScope, PrefixHash, TtlTier};
use super::metrics as cache_metrics;

#[derive(Clone)]
pub struct PostgresIndex {
    pool: sqlx_pool_router::DynPools,
    conn_retries: u32,
}

impl PostgresIndex {
    pub fn new(pool: impl sqlx_pool_router::PoolProvider, conn_retries: u32) -> Self {
        Self {
            pool: sqlx_pool_router::DynPools::new(pool),
            conn_retries,
        }
    }
}

fn is_transient_connection_error(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::Protocol(_) => true,
        sqlx::Error::Database(db) => db.message().contains("Authentication timed out"),
        _ => false,
    }
}

macro_rules! with_conn_retry {
    ($op_name:literal, $retries:expr, $op:expr) => {{
        let mut attempt: u32 = 0;
        loop {
            match $op.await {
                Err(e) if is_transient_connection_error(&e) && attempt < $retries => {
                    cache_metrics::record_index_conn_retry($op_name);
                    tracing::debug!(op = $op_name, attempt, error = %e, "cache index connection error — retrying with a fresh connection");
                    let base_ms = 100u64 << attempt.min(4);
                    let jitter_ms = rand::rng().random_range(0..(base_ms / 5 + 1));
                    tokio::time::sleep(std::time::Duration::from_millis(base_ms + jitter_ms)).await;
                    attempt += 1;
                }
                other => break other,
            }
        }
    }};
}

impl PostgresIndex {
    async fn lookup_once(&self, scope: &IndexScope, candidate_hashes: &[PrefixHash]) -> Result<Vec<LookupRow>, sqlx::Error> {
        sqlx::query_as!(
            LookupRow,
            r#"
            SELECT prefix_hash, cumulative_token_count, ttl_tier, expires_at
            FROM prompt_cache_entries
            WHERE principal_id = $1 AND virtual_model = $2 AND tokenizer_version = $3
              AND prefix_hash = ANY($4) AND expires_at > now()
            "#,
            scope.principal_id,
            scope.virtual_model,
            scope.tokenizer_version,
            candidate_hashes,
        )
        .fetch_all(&self.pool)
        .await
    }

    async fn write_once(&self, entry: &CacheEntry) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO prompt_cache_entries
              (principal_id, virtual_model, tokenizer_version, prefix_hash,
               cumulative_token_count, ttl_tier, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (principal_id, virtual_model, tokenizer_version, prefix_hash)
            DO UPDATE SET
              cumulative_token_count = EXCLUDED.cumulative_token_count,
              ttl_tier               = EXCLUDED.ttl_tier,
              expires_at             = EXCLUDED.expires_at
            "#,
            entry.scope.principal_id,
            entry.scope.virtual_model,
            entry.scope.tokenizer_version,
            entry.prefix_hash,
            i32::try_from(entry.cumulative_token_count).unwrap_or(i32::MAX),
            entry.ttl_tier.as_str(),
            entry.expires_at,
        )
        .execute(&self.pool)
        .await
        .map(|_| ())
    }

    async fn refresh_once(&self, scope: &IndexScope, prefix_hash: &PrefixHash, new_expires_at: DateTime<Utc>) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE prompt_cache_entries
            SET expires_at = $5::timestamptz
            WHERE principal_id = $1 AND virtual_model = $2 AND tokenizer_version = $3
              AND prefix_hash = $4
              AND expires_at < $5::timestamptz - ($5::timestamptz - now()) / 100
            "#,
            scope.principal_id,
            scope.virtual_model,
            scope.tokenizer_version,
            prefix_hash,
            new_expires_at,
        )
        .execute(&self.pool)
        .await
        .map(|_| ())
    }
}

struct LookupRow {
    prefix_hash: Vec<u8>,
    cumulative_token_count: i32,
    ttl_tier: String,
    expires_at: DateTime<Utc>,
}

#[async_trait]
impl CacheIndex for PostgresIndex {
    #[instrument(skip_all, fields(model = %scope.virtual_model, candidates = candidate_hashes.len()), err)]
    async fn lookup(&self, scope: &IndexScope, candidate_hashes: &[PrefixHash]) -> CacheResult<Vec<CacheMatch>> {
        if candidate_hashes.is_empty() {
            return Ok(Vec::new());
        }
        let rows = with_conn_retry!("lookup", self.conn_retries, self.lookup_once(scope, candidate_hashes))?;

        rows.into_iter()
            .map(|r| {
                let ttl_tier =
                    TtlTier::parse(&r.ttl_tier).ok_or_else(|| CacheError::Invalid(format!("unknown ttl_tier {:?}", r.ttl_tier)))?;
                Ok(CacheMatch {
                    prefix_hash: r.prefix_hash,
                    cumulative_token_count: r.cumulative_token_count.max(0) as u32,
                    ttl_tier,
                    expires_at: r.expires_at,
                })
            })
            .collect()
    }

    #[instrument(skip_all, fields(model = %entry.scope.virtual_model, ttl = entry.ttl_tier.as_str()), err)]
    async fn write(&self, entry: &CacheEntry) -> CacheResult<()> {
        with_conn_retry!("write", self.conn_retries, self.write_once(entry))?;
        Ok(())
    }

    #[instrument(skip_all, fields(model = %scope.virtual_model), err)]
    async fn refresh(&self, scope: &IndexScope, prefix_hash: &PrefixHash, new_expires_at: DateTime<Utc>) -> CacheResult<()> {
        with_conn_retry!("refresh", self.conn_retries, self.refresh_once(scope, prefix_hash, new_expires_at))?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> IndexScope {
        IndexScope {
            principal_id: uuid::Uuid::new_v4(),
            virtual_model: "test-model".to_string(),
            tokenizer_version: "sha256:abc".to_string(),
        }
    }

    fn entry(scope: &IndexScope, hash: &[u8], tokens: u32, tier: TtlTier) -> CacheEntry {
        CacheEntry {
            scope: scope.clone(),
            prefix_hash: hash.to_vec(),
            cumulative_token_count: tokens,
            ttl_tier: tier,
            expires_at: Utc::now() + tier.duration(),
        }
    }

    #[dwctl_test_macros::test]
    async fn write_then_lookup_returns_match(pool: sqlx::PgPool) {
        let idx = PostgresIndex::new(pool, 1);
        let s = scope();
        idx.write(&entry(&s, b"hash-a", 1024, TtlTier::OneHour)).await.unwrap();

        let hits = idx.lookup(&s, &[b"hash-a".to_vec(), b"hash-missing".to_vec()]).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].prefix_hash, b"hash-a");
        assert_eq!(hits[0].cumulative_token_count, 1024);
        assert_eq!(hits[0].ttl_tier, TtlTier::OneHour);
    }

    #[dwctl_test_macros::test]
    async fn lookup_excludes_expired_and_other_scopes(pool: sqlx::PgPool) {
        let idx = PostgresIndex::new(pool, 1);
        let s = scope();

        let mut expired = entry(&s, b"old", 10, TtlTier::FiveMinutes);
        expired.expires_at = Utc::now() - chrono::Duration::seconds(1);
        idx.write(&expired).await.unwrap();
        assert!(idx.lookup(&s, &[b"old".to_vec()]).await.unwrap().is_empty());

        idx.write(&entry(&s, b"shared", 5, TtlTier::OneHour)).await.unwrap();
        let other = IndexScope {
            principal_id: uuid::Uuid::new_v4(),
            ..s.clone()
        };
        assert!(idx.lookup(&other, &[b"shared".to_vec()]).await.unwrap().is_empty());
    }

    #[dwctl_test_macros::test]
    async fn refresh_slides_expiry(pool: sqlx::PgPool) {
        let idx = PostgresIndex::new(pool, 1);
        let s = scope();
        let mut e = entry(&s, b"refreshable", 7, TtlTier::FiveMinutes);
        e.expires_at = Utc::now() + chrono::Duration::seconds(2);
        idx.write(&e).await.unwrap();

        let new_expiry = Utc::now() + chrono::Duration::hours(1);
        idx.refresh(&s, &b"refreshable".to_vec(), new_expiry).await.unwrap();

        let hits = idx.lookup(&s, &[b"refreshable".to_vec()]).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert!(hits[0].expires_at > Utc::now() + chrono::Duration::minutes(30));
    }

    #[dwctl_test_macros::test]
    async fn refresh_skips_a_negligible_extension(pool: sqlx::PgPool) {
        let idx = PostgresIndex::new(pool, 1);
        let s = scope();
        let written = Utc::now() + chrono::Duration::hours(1);
        let mut e = entry(&s, b"hot-prefix", 7, TtlTier::OneHour);
        e.expires_at = written;
        idx.write(&e).await.unwrap();

        idx.refresh(&s, &b"hot-prefix".to_vec(), written + chrono::Duration::seconds(5))
            .await
            .unwrap();
        let hits = idx.lookup(&s, &[b"hot-prefix".to_vec()]).await.unwrap();
        assert_eq!(hits[0].expires_at.timestamp_micros(), written.timestamp_micros());

        let later = written + chrono::Duration::minutes(5);
        idx.refresh(&s, &b"hot-prefix".to_vec(), later).await.unwrap();
        let hits = idx.lookup(&s, &[b"hot-prefix".to_vec()]).await.unwrap();
        assert_eq!(hits[0].expires_at.timestamp_micros(), later.timestamp_micros());
    }

    #[dwctl_test_macros::test]
    async fn write_upserts_on_conflict(pool: sqlx::PgPool) {
        let idx = PostgresIndex::new(pool, 1);
        let s = scope();
        idx.write(&entry(&s, b"dup", 100, TtlTier::FiveMinutes)).await.unwrap();
        idx.write(&entry(&s, b"dup", 200, TtlTier::OneHour)).await.unwrap();

        let hits = idx.lookup(&s, &[b"dup".to_vec()]).await.unwrap();
        assert_eq!(hits.len(), 1, "upsert must not create a duplicate row");
        assert_eq!(hits[0].cumulative_token_count, 200);
        assert_eq!(hits[0].ttl_tier, TtlTier::OneHour);
    }

    #[test]
    fn transient_connection_errors_are_classified_for_retry() {
        use std::io;
        assert!(is_transient_connection_error(&sqlx::Error::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "expected to read 5 bytes, got 0 bytes at EOF"
        ))));
        assert!(is_transient_connection_error(&sqlx::Error::Protocol("unexpected EOF".into())));
        assert!(!is_transient_connection_error(&sqlx::Error::PoolTimedOut));
        assert!(!is_transient_connection_error(&sqlx::Error::RowNotFound));
    }
}
