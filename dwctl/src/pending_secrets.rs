
use std::time::Duration;

use metrics::gauge;
use sqlx_pool_router::PoolProvider;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::crypto::hash_api_key;
use crate::db::handlers::api_keys::{ApiKeys, PendingReveal};
use crate::keystore::Keystore;
use crate::metrics::errors::component;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(15 * 60);

fn unwrap_current(keystore: &Keystore, wrapped: &[u8], secret_hash: &[u8]) -> Option<String> {
    let plaintext = keystore.unwrap(wrapped).ok()?;
    let secret = String::from_utf8(plaintext).ok()?;
    (hash_api_key(&secret)[..] == *secret_hash).then_some(secret)
}

pub fn resolve_reveal(keystore: Option<&Keystore>, reveal: &PendingReveal) -> Option<String> {
    let (Some(keystore), Some(wrapped)) = (keystore, reveal.pending_secret.as_deref()) else {
        return None;
    };
    unwrap_current(keystore, wrapped, &reveal.secret_hash)
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub rewrapped: u64,
    pub cleared: u64,
    pub unrecoverable: u64,
}

enum PendingState {
    Verified,
    Rewrap { secret: String },
    Unrecoverable,
}

fn pending_state(keystore: Option<&Keystore>, row: &PendingReveal) -> PendingState {
    let (Some(keystore), Some(wrapped)) = (keystore, row.pending_secret.as_deref()) else {
        return PendingState::Unrecoverable;
    };
    match unwrap_current(keystore, wrapped, &row.secret_hash) {
        Some(_) if keystore.is_current(wrapped) => PendingState::Verified,
        Some(secret) => PendingState::Rewrap { secret },
        None => PendingState::Unrecoverable,
    }
}

async fn rewrap_rows(
    repo: &mut ApiKeys<'_>,
    keystore: Option<&Keystore>,
    rows: Vec<PendingReveal>,
    report: &mut ReconcileReport,
) -> anyhow::Result<()> {
    let Some(keystore) = keystore else { return Ok(()) };
    for row in rows {
        let PendingState::Rewrap { secret } = pending_state(Some(keystore), &row) else {
            continue;
        };
        let wrapped = keystore.wrap(secret.as_bytes())?;
        if repo.set_pending_secret(row.id, &wrapped, &row.secret_hash).await? {
            report.rewrapped += 1;
        }
    }
    Ok(())
}

async fn count_unrecoverable(repo: &mut ApiKeys<'_>, keystore: Option<&Keystore>) -> anyhow::Result<u64> {
    Ok(repo
        .list_pending_reveals()
        .await?
        .iter()
        .filter(|row| matches!(pending_state(keystore, row), PendingState::Unrecoverable))
        .count() as u64)
}

pub async fn reconcile<P: PoolProvider>(db: &P, keystore: Option<&Keystore>) -> anyhow::Result<ReconcileReport> {
    let mut conn = db.write().acquire().await?;
    let mut repo = ApiKeys::new(&mut conn);
    let mut report = ReconcileReport {
        cleared: repo.clear_orphaned_pending_secrets().await?,
        ..Default::default()
    };
    let rows = repo.list_pending_reveals().await?;
    rewrap_rows(&mut repo, keystore, rows, &mut report).await?;
    report.unrecoverable = count_unrecoverable(&mut repo, keystore).await?;
    Ok(report)
}

pub async fn run<P: PoolProvider>(db: P, keystore: Option<Keystore>, shutdown: CancellationToken) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = interval.tick() => {}
        }
        match reconcile(&db, keystore.as_ref()).await {
            Ok(report) => {
                gauge!("dwctl_api_key_pending_reveals_unrecoverable").set(report.unrecoverable as f64);
                if report.rewrapped + report.cleared > 0 {
                    info!(
                        rewrapped = report.rewrapped,
                        cleared = report.cleared,
                        "reconciled wrapped pending API key secrets"
                    );
                }
                if report.unrecoverable > 0 {
                    crate::background_error!(
                        component::PENDING_SECRETS,
                        "unrecoverable",
                        Warning,
                        count = report.unrecoverable,
                        keystore_configured = keystore.is_some(),
                        "issued API keys awaiting reveal have no usable wrapped secret; their reveal fails until the key is rotated (by its holder, for example)"
                    );
                }
            }
            Err(e) => {
                crate::background_error!(
                    component::PENDING_SECRETS,
                    "reconcile_failed",
                    Error,
                    error = %e,
                    "failed to reconcile wrapped pending API key secrets"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::handlers::Repository;
    use crate::db::models::api_keys::ApiKeyCreateDBRequest;
    use crate::keystore::test_keystore;
    use crate::test::utils::create_test_user;
    use crate::{api::models::users::Role, types::ApiKeyId};
    use sqlx::PgPool;
    use sqlx_pool_router::TestDbPools;

    fn reveal(pending_secret: Option<Vec<u8>>, secret: &str) -> PendingReveal {
        PendingReveal {
            id: uuid::Uuid::new_v4(),
            pending_secret,
            secret_hash: hash_api_key(secret).to_vec(),
        }
    }

    #[test]
    fn resolve_returns_a_wrap_of_the_current_secret() {
        let ks = test_keystore("k1", &[("k1", "wrap-one")]);
        let wrapped = ks.wrap(b"sk-current").unwrap();
        assert_eq!(
            resolve_reveal(Some(&ks), &reveal(Some(wrapped), "sk-current")),
            Some("sk-current".to_string())
        );
    }

    #[test]
    fn resolve_fails_for_missing_stale_or_unreadable_wraps() {
        let ks = test_keystore("k1", &[("k1", "wrap-one")]);
        assert_eq!(resolve_reveal(Some(&ks), &reveal(None, "sk-current")), None);
        let stale = reveal(Some(ks.wrap(b"sk-old").unwrap()), "sk-current");
        assert_eq!(resolve_reveal(Some(&ks), &stale), None);
        let other = test_keystore("k9", &[("k9", "wrap-nine")]);
        let foreign = reveal(Some(other.wrap(b"sk-current").unwrap()), "sk-current");
        assert_eq!(resolve_reveal(Some(&ks), &foreign), None);
        let wrapped = reveal(Some(ks.wrap(b"sk-current").unwrap()), "sk-current");
        assert_eq!(resolve_reveal(None, &wrapped), None);
    }

    async fn pending_key(pool: &PgPool) -> (ApiKeyId, String) {
        let user = create_test_user(pool, Role::StandardUser).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = ApiKeys::new(&mut conn);
        let request = ApiKeyCreateDBRequest {
            user_id: user.id,
            created_by: user.id,
            name: "issued".to_string(),
            description: None,
            purpose: crate::db::models::api_keys::ApiKeyPurpose::Realtime,
            spend_limit: None,
            spend_limit_interval: None,
        };
        let key = repo.create(&request).await.unwrap();
        sqlx::query!("UPDATE api_keys SET secret_revealed_at = NULL WHERE id = $1", key.id)
            .execute(pool)
            .await
            .unwrap();
        (key.id, key.secret)
    }

    async fn stored_wrap(pool: &PgPool, id: ApiKeyId) -> Option<Vec<u8>> {
        sqlx::query_scalar!("SELECT pending_secret FROM api_keys WHERE id = $1", id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn set_wrap(pool: &PgPool, id: ApiKeyId, wrapped: Vec<u8>) {
        sqlx::query!("UPDATE api_keys SET pending_secret = $2 WHERE id = $1", id, wrapped)
            .execute(pool)
            .await
            .unwrap();
    }

    #[dwctl_test_macros::test]
    async fn reconcile_rewraps_clears_and_reports_unrecoverable(pool: PgPool) {
        let db = TestDbPools::new(pool.clone()).await.unwrap();
        let (unwrapped, _) = pending_key(&pool).await;
        let (old_wrap, old_wrap_secret) = pending_key(&pool).await;
        let (current_wrap, current_wrap_secret) = pending_key(&pool).await;
        let (revealed, _) = pending_key(&pool).await;

        let k1 = test_keystore("k1", &[("k1", "wrap-one")]);
        let ks = test_keystore("k2", &[("k1", "wrap-one"), ("k2", "wrap-two")]);
        set_wrap(&pool, old_wrap, k1.wrap(old_wrap_secret.as_bytes()).unwrap()).await;
        set_wrap(&pool, current_wrap, ks.wrap(current_wrap_secret.as_bytes()).unwrap()).await;
        sqlx::query!(
            "UPDATE api_keys SET secret_revealed_at = NOW(), pending_secret = '\\x00'::bytea WHERE id = $1",
            revealed
        )
        .execute(&pool)
        .await
        .unwrap();

        let report = reconcile(&db, None).await.unwrap();
        assert_eq!(
            report,
            ReconcileReport {
                rewrapped: 0,
                cleared: 1,
                unrecoverable: 3
            }
        );
        assert_eq!(stored_wrap(&pool, revealed).await, None);

        let report = reconcile(&db, Some(&ks)).await.unwrap();
        assert_eq!(
            report,
            ReconcileReport {
                rewrapped: 1,
                cleared: 0,
                unrecoverable: 1
            }
        );
        assert_eq!(stored_wrap(&pool, unwrapped).await, None);
        let rewrapped = stored_wrap(&pool, old_wrap).await.unwrap();
        assert!(ks.is_current(&rewrapped));
        assert_eq!(ks.unwrap(&rewrapped).unwrap(), old_wrap_secret.as_bytes());

        assert_eq!(
            reconcile(&db, Some(&ks)).await.unwrap(),
            ReconcileReport {
                unrecoverable: 1,
                ..Default::default()
            }
        );
    }

    #[dwctl_test_macros::test]
    async fn a_wrap_under_an_older_wrap_key_is_not_unrecoverable(pool: PgPool) {
        let k1 = test_keystore("k1", &[("k1", "wrap-one")]);
        let ks = test_keystore("k2", &[("k1", "wrap-one"), ("k2", "wrap-two")]);
        let (id, secret) = pending_key(&pool).await;
        set_wrap(&pool, id, k1.wrap(secret.as_bytes()).unwrap()).await;
        let mut conn = pool.acquire().await.unwrap();
        assert_eq!(count_unrecoverable(&mut ApiKeys::new(&mut conn), Some(&ks)).await.unwrap(), 0);
    }

    #[dwctl_test_macros::test]
    async fn rotation_between_read_and_rewrap_is_not_reported_settled(pool: PgPool) {
        let k1 = test_keystore("k1", &[("k1", "wrap-one")]);
        let ks = test_keystore("k2", &[("k1", "wrap-one"), ("k2", "wrap-two")]);
        let (id, secret) = pending_key(&pool).await;
        set_wrap(&pool, id, k1.wrap(secret.as_bytes()).unwrap()).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = ApiKeys::new(&mut conn);
        let rows = repo.list_pending_reveals().await.unwrap();
        assert_eq!(rows.len(), 1);

        sqlx::query!(
            "UPDATE api_keys SET secret_hash = $2 WHERE id = $1",
            id,
            &hash_api_key("sk-rotated-by-previous-release")[..]
        )
        .execute(&pool)
        .await
        .unwrap();

        let mut report = ReconcileReport::default();
        rewrap_rows(&mut repo, Some(&ks), rows, &mut report).await.unwrap();
        assert_eq!(report.rewrapped, 0);
        assert!(!ks.is_current(&stored_wrap(&pool, id).await.unwrap()));
        assert_eq!(count_unrecoverable(&mut repo, Some(&ks)).await.unwrap(), 1);
    }
}
