
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::prompt_cache::{CacheEntry, CacheError, CacheIndex, CacheMatch, CacheResult, IndexScope, PrefixHash, TtlTier};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fidelity {
    Exact,
    Approximate,
}

pub struct HistoricalIndex {
    pool: PgPool,
    at: DateTime<Utc>,
}

impl HistoricalIndex {
    pub fn new(pool: PgPool, at: DateTime<Utc>) -> Self {
        Self { pool, at }
    }
}

struct LookupRow {
    prefix_hash: Vec<u8>,
    cumulative_token_count: i32,
    ttl_tier: String,
    expires_at: DateTime<Utc>,
}

#[async_trait]
impl CacheIndex for HistoricalIndex {
    async fn lookup(&self, scope: &IndexScope, candidate_hashes: &[PrefixHash]) -> CacheResult<Vec<CacheMatch>> {
        if candidate_hashes.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlx::query_as!(
            LookupRow,
            r#"
            SELECT prefix_hash AS "prefix_hash!", cumulative_token_count AS "cumulative_token_count!",
                   ttl_tier AS "ttl_tier!", expires_at AS "expires_at!"
            FROM prompt_cache_entries
            WHERE principal_id = $1 AND virtual_model = $2 AND tokenizer_version = $3
              AND prefix_hash = ANY($4)
              AND created_at <= $5 AND expires_at > $5
            "#,
            scope.principal_id,
            scope.virtual_model,
            scope.tokenizer_version,
            candidate_hashes,
            self.at,
        )
        .fetch_all(&self.pool)
        .await?;

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

    async fn write(&self, _entry: &CacheEntry) -> CacheResult<()> {
        Err(CacheError::Invalid(
            "a historical cache index is read-only: reconstruction must never mutate the index it reads".into(),
        ))
    }

    async fn refresh(&self, _scope: &IndexScope, _hash: &PrefixHash, _new_expires_at: DateTime<Utc>) -> CacheResult<()> {
        Err(CacheError::Invalid(
            "a historical cache index is read-only: refreshing an entry would slide a window that already closed".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    async fn seed_entry(pool: &PgPool, principal: Uuid, hash: &[u8], created: DateTime<Utc>, expires: DateTime<Utc>) {
        sqlx::query!(
            "INSERT INTO prompt_cache_entries
               (principal_id, virtual_model, tokenizer_version, prefix_hash,
                cumulative_token_count, ttl_tier, created_at, expires_at)
             VALUES ($1,'m','tok-v1',$2,4096,'5m',$3,$4)",
            principal,
            hash,
            created,
            expires,
        )
        .execute(pool)
        .await
        .unwrap();
    }

    fn scope(principal: Uuid) -> IndexScope {
        IndexScope {
            principal_id: principal,
            virtual_model: "m".to_string(),
            tokenizer_version: "tok-v1".to_string(),
        }
    }

    #[dwctl_test_macros::test]
    async fn an_expired_entry_is_found_at_an_instant_it_covered(pool: PgPool) {
        let principal = Uuid::new_v4();
        let hash = b"prefix-a".to_vec();
        let created = Utc::now() - chrono::Duration::days(3);
        let expires = created + chrono::Duration::minutes(5);
        seed_entry(&pool, principal, &hash, created, expires).await;

        let at = created + chrono::Duration::minutes(2);
        let hits = HistoricalIndex::new(pool.clone(), at)
            .lookup(&scope(principal), std::slice::from_ref(&hash))
            .await
            .unwrap();
        assert_eq!(hits.len(), 1, "the episode covered this instant");
        assert_eq!(hits[0].cumulative_token_count, 4096);

        let after = expires + chrono::Duration::minutes(1);
        let miss = HistoricalIndex::new(pool.clone(), after)
            .lookup(&scope(principal), std::slice::from_ref(&hash))
            .await
            .unwrap();
        assert!(miss.is_empty(), "the episode had closed by then");

        let miss = HistoricalIndex::new(pool.clone(), expires)
            .lookup(&scope(principal), std::slice::from_ref(&hash))
            .await
            .unwrap();
        assert!(miss.is_empty(), "an entry is not live at its own expiry instant");

        let before = created - chrono::Duration::minutes(1);
        let miss = HistoricalIndex::new(pool, before)
            .lookup(&scope(principal), std::slice::from_ref(&hash))
            .await
            .unwrap();
        assert!(miss.is_empty(), "the entry did not exist yet");
    }

    #[dwctl_test_macros::test]
    async fn lookup_is_scoped_to_the_principal(pool: PgPool) {
        let mine = Uuid::new_v4();
        let theirs = Uuid::new_v4();
        let hash = b"shared-content".to_vec();
        let created = Utc::now() - chrono::Duration::hours(1);
        seed_entry(&pool, theirs, &hash, created, created + chrono::Duration::hours(2)).await;

        let hits = HistoricalIndex::new(pool, Utc::now())
            .lookup(&scope(mine), std::slice::from_ref(&hash))
            .await
            .unwrap();
        assert!(hits.is_empty(), "another principal's cache is not mine to read");
    }

    #[dwctl_test_macros::test]
    async fn writes_are_refused_not_silently_dropped(pool: PgPool) {
        let idx = HistoricalIndex::new(pool, Utc::now());
        let entry = CacheEntry {
            scope: scope(Uuid::new_v4()),
            prefix_hash: b"x".to_vec(),
            cumulative_token_count: 1,
            ttl_tier: TtlTier::FiveMinutes,
            expires_at: Utc::now(),
        };
        assert!(idx.write(&entry).await.is_err());
        assert!(idx.refresh(&scope(Uuid::new_v4()), &b"x".to_vec(), Utc::now()).await.is_err());
    }
}

#[derive(Debug, Clone)]
pub struct ReconstructedSplit {
    pub read: u64,
    pub creation_5m: u64,
    pub creation_1h: u64,
    pub creation_24h: u64,
    pub render_total: Option<u64>,
    pub fidelity: Fidelity,
}

impl ReconstructedSplit {
    pub fn creations_total(&self) -> u64 {
        self.creation_5m + self.creation_1h + self.creation_24h
    }
}

pub async fn reconstruct_split(
    classifier: &crate::prompt_cache::Classifier,
    model: &str,
    class: Option<&str>,
    request_body: &[u8],
    principal: crate::types::UserId,
    at: DateTime<Utc>,
    route_has_blocks: bool,
) -> CacheResult<Option<ReconstructedSplit>> {
    let virtual_model = match class {
        Some(class) if class != crate::inference::serving_name::STANDARD_CLASS => format!("{model}:{class}"),
        _ => model.to_owned(),
    };
    let outcome = classifier
        .classify(crate::prompt_cache::ClassifyRequest {
            virtual_model: &virtual_model,
            model,
            body: request_body,
            api_key: None,
            principal: Some(principal),
            route_has_blocks,
        })
        .await?;

    if !outcome.active {
        return Ok(None);
    }
    if outcome.degraded {
        return Err(CacheError::Invalid(
            "classifier degraded (tokenizer unavailable): a zero split from a degraded classify is not evidence".into(),
        ));
    }

    let s = outcome.stats;
    Ok(Some(ReconstructedSplit {
        read: s.read,
        creation_5m: s.creation_5m,
        creation_1h: s.creation_1h,
        creation_24h: s.creation_24h,
        render_total: s.render_total,
        fidelity: if at < Utc::now() { Fidelity::Approximate } else { Fidelity::Exact },
    }))
}
