
use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use metrics::gauge;
use serde::Deserialize;
use sqlx::PgPool;
use sqlx_pool_router::PoolProvider;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::api::models::users::Role;
use crate::metrics::errors::component;

const SERVICE_ACCOUNTS_LOCK: i64 = 0x4457_5356_4341_4343;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceAccountsFile {
    pub service_accounts: Vec<ServiceAccountSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceAccountSpec {
    pub id: Uuid,
    pub name: String,
    pub role: Role,
    pub key_hashes: Vec<String>,
}

pub type LoadedAccount = (ServiceAccountSpec, Vec<Vec<u8>>);

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub accounts: usize,
    pub keys_created: u64,
    pub keys_deleted: u64,
    pub accounts_retired: u64,
}

#[derive(Debug)]
pub enum Skipped {
    Missing,
    Invalid(anyhow::Error),
}

pub fn load(path: &Path) -> Result<Vec<LoadedAccount>, Skipped> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(Skipped::Missing),
        Err(e) => return Err(Skipped::Invalid(anyhow::Error::new(e).context("reading the service accounts file"))),
    };
    parse(&text).map_err(Skipped::Invalid)
}

pub fn parse(text: &str) -> anyhow::Result<Vec<LoadedAccount>> {
    let file: ServiceAccountsFile = serde_json::from_str(text).context("parsing the service accounts file")?;
    ensure!(
        !file.service_accounts.is_empty(),
        "the file lists no service accounts; refusing to retire every one"
    );
    let mut ids = HashSet::new();
    let mut hashes = HashSet::new();
    let mut accounts = Vec::with_capacity(file.service_accounts.len());
    for spec in file.service_accounts {
        ensure!(ids.insert(spec.id), "service account {} is listed twice", spec.id);
        ensure!(!spec.id.is_nil(), "service account id must not be the nil UUID");
        let name = spec.name.trim();
        ensure!(
            !name.is_empty() && name.chars().count() <= 200,
            "service account {} needs a name of 1 to 200 characters",
            spec.id
        );
        ensure!(
            spec.role.is_service_role(),
            "service account {} has role {:?}, which is not a service role",
            spec.id,
            spec.role
        );
        ensure!(!spec.key_hashes.is_empty(), "service account {} lists no key hashes", spec.id);
        let mut decoded = Vec::with_capacity(spec.key_hashes.len());
        for hash in &spec.key_hashes {
            let bytes = decode_sha256_hex(hash).with_context(|| format!("service account {}", spec.id))?;
            ensure!(hashes.insert(bytes.clone()), "key hash {hash} is listed twice");
            decoded.push(bytes);
        }
        accounts.push((spec, decoded));
    }
    Ok(accounts)
}

fn decode_sha256_hex(hash: &str) -> anyhow::Result<Vec<u8>> {
    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        bail!("key hash {hash:?} is not a lowercase hex SHA-256 digest");
    }
    Ok((0..32)
        .map(|i| u8::from_str_radix(&hash[2 * i..2 * i + 2], 16).expect("checked hex"))
        .collect())
}

pub async fn apply(pool: &PgPool, accounts: &[LoadedAccount]) -> anyhow::Result<ReconcileReport> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SERVICE_ACCOUNTS_LOCK)
        .execute(&mut *tx)
        .await?;
    let mut report = ReconcileReport {
        accounts: accounts.len(),
        ..Default::default()
    };

    for (spec, hashes) in accounts {
        let name = spec.name.trim();
        let claimed = sqlx::query_scalar!(
            r#"
            INSERT INTO users (id, username, email, display_name, auth_source, user_type, is_service_account)
            VALUES ($1, $2, $3, $4, 'service', 'individual', TRUE)
            ON CONFLICT (id) DO UPDATE
                SET display_name = EXCLUDED.display_name, is_deleted = FALSE, updated_at = NOW()
                WHERE users.is_service_account
            RETURNING id
            "#,
            spec.id,
            format!("svc-{}", spec.id),
            format!("svc-{}@service-accounts.invalid", spec.id),
            name,
        )
        .fetch_optional(&mut *tx)
        .await?;
        ensure!(
            claimed.is_some(),
            "id {} belongs to an account that is not a service account",
            spec.id
        );

        let role = spec.role.clone();
        sqlx::query!(
            r#"DELETE FROM user_roles WHERE user_id = $1 AND role <> $2"#,
            spec.id,
            role.clone() as Role,
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            r#"INSERT INTO user_roles (user_id, role) VALUES ($1, $2) ON CONFLICT (user_id, role) DO NOTHING"#,
            spec.id,
            role as Role,
        )
        .execute(&mut *tx)
        .await?;

        report.keys_deleted += sqlx::query!(
            r#"
            UPDATE api_keys SET is_deleted = TRUE
            WHERE user_id = $1 AND NOT is_deleted AND secret_hash <> ALL($2)
            "#,
            spec.id,
            hashes as &[Vec<u8>],
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();

        for hash in hashes {
            let key_name = format!("{name} ({})", hash.iter().map(|b| format!("{b:02x}")).collect::<String>());
            let row = sqlx::query!(
                r#"
                INSERT INTO api_keys (name, description, purpose, user_id, created_by, hidden, secret_hash)
                VALUES ($1, 'Service account key, managed by the service-account loader', 'platform', $2, $2, FALSE, $3)
                ON CONFLICT (secret_hash) DO UPDATE SET is_deleted = FALSE
                    WHERE api_keys.user_id = EXCLUDED.user_id AND api_keys.created_by = EXCLUDED.created_by
                      AND api_keys.purpose = 'platform'
                RETURNING (xmax = 0) AS "inserted!"
                "#,
                key_name,
                spec.id,
                &hash[..],
            )
            .fetch_optional(&mut *tx)
            .await?;
            match row {
                Some(row) if row.inserted => report.keys_created += 1,
                Some(_) => {}
                None => bail!("a key hash of service account {} is already used by another key", spec.id),
            }
        }
    }

    let listed: Vec<Uuid> = accounts.iter().map(|(spec, _)| spec.id).collect();
    report.keys_deleted += sqlx::query!(
        r#"
        UPDATE api_keys SET is_deleted = TRUE
        WHERE NOT is_deleted
          AND user_id IN (SELECT id FROM users WHERE is_service_account AND id <> ALL($1))
        "#,
        &listed,
    )
    .execute(&mut *tx)
    .await?
    .rows_affected();
    report.accounts_retired = sqlx::query!(
        r#"
        DELETE FROM user_roles
        WHERE user_id IN (SELECT id FROM users WHERE is_service_account AND id <> ALL($1))
        "#,
        &listed,
    )
    .execute(&mut *tx)
    .await?
    .rows_affected();

    tx.commit().await?;
    Ok(report)
}

pub async fn run<P: PoolProvider>(db: P, file: std::path::PathBuf, interval: Duration, shutdown: CancellationToken) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = ticker.tick() => {}
        }
        match load(&file) {
            Err(Skipped::Missing) => {
                crate::background_error!(
                    component::SERVICE_ACCOUNTS,
                    "file_missing",
                    Error,
                    path = %file.display(),
                    "service accounts file not found; nothing changed"
                );
            }
            Err(Skipped::Invalid(e)) => {
                crate::background_error!(
                    component::SERVICE_ACCOUNTS,
                    "file_invalid",
                    Error,
                    path = %file.display(),
                    error = %format!("{e:#}"),
                    "service accounts file is invalid; nothing changed"
                );
            }
            Ok(accounts) => match apply(&db.write(), &accounts).await {
                Ok(report) => {
                    gauge!("dwctl_service_accounts").set(report.accounts as f64);
                }
                Err(e) => {
                    crate::background_error!(
                        component::SERVICE_ACCOUNTS,
                        "apply_failed",
                        Error,
                        error = %format!("{e:#}"),
                        "failed to apply the service accounts file; nothing changed"
                    );
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::hash_api_key;

    fn hex(secret: &str) -> String {
        hash_api_key(secret).iter().map(|b| format!("{b:02x}")).collect()
    }

    fn file(accounts: &[(Uuid, &str, &str, &[&str])]) -> String {
        let accounts: Vec<_> = accounts
            .iter()
            .map(|(id, name, role, secrets)| {
                serde_json::json!({
                    "id": id, "name": name, "role": role,
                    "key_hashes": secrets.iter().map(|s| hex(s)).collect::<Vec<_>>(),
                })
            })
            .collect();
        serde_json::json!({ "service_accounts": accounts }).to_string()
    }

    async fn apply_text(pool: &PgPool, text: &str) -> anyhow::Result<ReconcileReport> {
        apply(pool, &parse(text)?).await
    }

    async fn live_key_count(pool: &PgPool, id: Uuid) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM api_keys WHERE user_id = $1 AND NOT is_deleted")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn roles(pool: &PgPool, id: Uuid) -> Vec<String> {
        sqlx::query_scalar::<_, String>("SELECT role::text FROM user_roles WHERE user_id = $1 ORDER BY role::text")
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap()
    }

    #[test]
    fn files_that_could_retire_everything_or_grant_too_much_are_refused() {
        let id = Uuid::new_v4();
        assert!(parse(r#"{"service_accounts": []}"#).is_err(), "empty list");
        assert!(parse("not json").is_err());
        assert!(parse(&file(&[(id, "x", "PlatformManager", &["a"])])).is_err(), "not a service role");
        assert!(parse(&file(&[(id, "x", "StandardUser", &["a"])])).is_err(), "not a service role");
        assert!(parse(&file(&[(id, "x", "QueueDemandReader", &[])])).is_err(), "no keys");
        assert!(parse(&file(&[(id, "", "QueueDemandReader", &["a"])])).is_err(), "no name");
        assert!(
            parse(&file(&[
                (id, "x", "QueueDemandReader", &["a"]),
                (id, "y", "CatalogueReader", &["b"])
            ]))
            .is_err(),
            "same id twice"
        );
        assert!(
            parse(&file(&[
                (id, "x", "QueueDemandReader", &["a"]),
                (Uuid::new_v4(), "y", "CatalogueReader", &["a"])
            ]))
            .is_err(),
            "same key twice"
        );
        let bad_hash = format!(r#"{{"service_accounts":[{{"id":"{id}","name":"x","role":"QueueDemandReader","key_hashes":["ABC"]}}]}}"#);
        assert!(parse(&bad_hash).is_err());
        let unknown_field = format!(
            r#"{{"service_accounts":[{{"id":"{id}","name":"x","role":"QueueDemandReader","key_hashes":["{}"],"admin":true}}]}}"#,
            hex("a")
        );
        assert!(parse(&unknown_field).is_err());
        assert_eq!(
            parse(&file(&[(id, " Queue scaler ", "QueueDemandReader", &["a", "b"])])).unwrap()[0]
                .1
                .len(),
            2
        );
    }

    #[dwctl_test_macros::test]
    async fn the_database_follows_the_file(pool: PgPool) {
        let scaler = Uuid::new_v4();
        let docs = Uuid::new_v4();

        let report = apply_text(
            &pool,
            &file(&[
                (scaler, "Queue scaler", "QueueDemandReader", &["s1"]),
                (docs, "Docs", "CatalogueReader", &["d1"]),
            ]),
        )
        .await
        .unwrap();
        assert_eq!((report.accounts, report.keys_created, report.keys_deleted), (2, 2, 0));
        assert_eq!(roles(&pool, scaler).await, vec!["QUEUEDEMANDREADER"]);
        let row = sqlx::query!(
            "SELECT username, display_name, is_service_account, password_hash, auth_source FROM users WHERE id = $1",
            scaler
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.username, format!("svc-{scaler}"));
        assert_eq!(row.display_name.as_deref(), Some("Queue scaler"));
        assert!(row.is_service_account && row.password_hash.is_none());
        let key = sqlx::query!(
            "SELECT purpose, user_id, created_by FROM api_keys WHERE secret_hash = $1",
            &hash_api_key("s1")[..]
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((key.purpose.as_str(), key.user_id, key.created_by), ("platform", scaler, scaler));

        let again = apply_text(
            &pool,
            &file(&[
                (scaler, "Queue scaler", "QueueDemandReader", &["s1"]),
                (docs, "Docs", "CatalogueReader", &["d1"]),
            ]),
        )
        .await
        .unwrap();
        assert_eq!((again.keys_created, again.keys_deleted), (0, 0));

        apply_text(
            &pool,
            &file(&[
                (scaler, "Queue scaler", "QueueDemandReader", &["s1", "s2"]),
                (docs, "Docs", "CatalogueReader", &["d1"]),
            ]),
        )
        .await
        .unwrap();
        assert_eq!(live_key_count(&pool, scaler).await, 2);
        let done = apply_text(
            &pool,
            &file(&[
                (scaler, "Queue scaler", "CatalogueReaderAll", &["s2"]),
                (docs, "Docs", "CatalogueReader", &["d1"]),
            ]),
        )
        .await
        .unwrap();
        assert_eq!(done.keys_deleted, 1);
        assert_eq!(live_key_count(&pool, scaler).await, 1);
        assert_eq!(roles(&pool, scaler).await, vec!["CATALOGUEREADERALL"], "role replaced, not added");

        apply_text(&pool, &file(&[(scaler, "Queue scaler", "CatalogueReaderAll", &["s2"])]))
            .await
            .unwrap();
        assert_eq!(live_key_count(&pool, docs).await, 0);
        assert!(roles(&pool, docs).await.is_empty());

        apply_text(
            &pool,
            &file(&[
                (scaler, "Queue scaler", "CatalogueReaderAll", &["s2"]),
                (docs, "Docs", "CatalogueReader", &["d1"]),
            ]),
        )
        .await
        .unwrap();
        assert_eq!(live_key_count(&pool, docs).await, 1);
    }

    #[dwctl_test_macros::test]
    async fn a_file_cannot_take_over_an_ordinary_account_or_key(pool: PgPool) {
        let person = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let before = roles(&pool, person.id).await;
        let taken = apply_text(&pool, &file(&[(person.id, "Takeover", "QueueDemandReader", &["t1"])])).await;
        assert!(taken.is_err());
        assert_eq!(roles(&pool, person.id).await, before, "nothing applied");

        sqlx::query("INSERT INTO api_keys (name, purpose, user_id, created_by, secret_hash) VALUES ('mine', 'realtime', $1, $1, $2)")
            .bind(person.id)
            .bind(&hash_api_key("theirs")[..])
            .execute(&pool)
            .await
            .unwrap();
        let svc = Uuid::new_v4();
        assert!(
            apply_text(&pool, &file(&[(svc, "Svc", "QueueDemandReader", &["theirs"])]))
                .await
                .is_err()
        );
        let purpose: String = sqlx::query_scalar("SELECT purpose FROM api_keys WHERE secret_hash = $1")
            .bind(&hash_api_key("theirs")[..])
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(purpose, "realtime");

        apply_text(&pool, &file(&[(svc, "Svc", "QueueDemandReader", &["ok"])]))
            .await
            .unwrap();
        let set_password = sqlx::query("UPDATE users SET password_hash = 'x' WHERE id = $1")
            .bind(svc)
            .execute(&pool)
            .await;
        assert!(set_password.is_err());
    }
}
