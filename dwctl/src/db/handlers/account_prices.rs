
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

pub(crate) const ADMIN_API_TARIFF_SOURCE: &str = "admin-api";

pub(crate) const STANDARD_CLASS: &str = "standard";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompetingRow {
    pub table: &'static str,
    pub source: Option<String>,
    pub serving_class: Option<String>,
    pub valid_from: DateTime<Utc>,
}

impl CompetingRow {
    pub fn owner(&self) -> &str {
        match self.source.as_deref() {
            Some("account-catalog") => "an account file",
            Some("org-overlays") => "an organisation overlay",
            Some(_) => "another writer",
            None => "a price set by hand",
        }
    }
}

pub(crate) struct StoredChange {
    pub fingerprint: String,
    pub response: Value,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CacheBase {
    pub write_multiplier_5m: Decimal,
    pub write_multiplier_1h: Decimal,
    pub write_multiplier_24h: Decimal,
    pub min_prefix_tokens: i32,
}

pub(crate) struct ChangeRecord<'a> {
    pub request_id: Option<&'a str>,
    pub fingerprint: &'a str,
    pub action: &'static str,
    pub account: Uuid,
    pub model: Uuid,
    pub serving_class: Option<&'a str>,
    pub valid_from: DateTime<Utc>,
    pub old: Option<crate::pricing::BilledPrice>,
    pub new: Option<crate::pricing::BilledPrice>,
    pub reason: &'a str,
    pub changed_by: Uuid,
}

pub(crate) struct AccountPrices<'c> {
    db: &'c mut PgConnection,
}

impl<'c> AccountPrices<'c> {
    pub fn new(db: &'c mut PgConnection) -> Self {
        Self { db }
    }

    pub async fn lock(&mut self, account: Uuid, model: Uuid) -> sqlx::Result<()> {
        lock_account_model(self.db, account, model).await
    }

    pub async fn find_change(&mut self, request_id: &str) -> sqlx::Result<Option<StoredChange>> {
        Ok(
            sqlx::query("SELECT request_fingerprint, response FROM account_price_changes WHERE request_id = $1")
                .bind(request_id)
                .fetch_optional(&mut *self.db)
                .await?
                .map(|row| StoredChange {
                    fingerprint: row.get("request_fingerprint"),
                    response: row.get("response"),
                }),
        )
    }

    pub async fn competing_tariff(
        &mut self,
        account: Uuid,
        model: Uuid,
        class: Option<&str>,
        at: DateTime<Utc>,
    ) -> sqlx::Result<Option<CompetingRow>> {
        let row = sqlx::query(
            "SELECT provisioning_source, serving_class, valid_from FROM model_tariffs
             WHERE user_id = $1 AND deployed_model_id = $2 AND api_key_purpose = 'realtime'
               AND COALESCE(serving_class, 'standard') = COALESCE($3, 'standard')
               AND (valid_until IS NULL OR valid_until > GREATEST(valid_from, $4))
               AND provisioning_source IS DISTINCT FROM $5
             ORDER BY valid_from LIMIT 1",
        )
        .bind(account)
        .bind(model)
        .bind(class)
        .bind(at)
        .bind(ADMIN_API_TARIFF_SOURCE)
        .fetch_optional(&mut *self.db)
        .await?;
        Ok(row.map(|row| competing("model_tariffs", &row)))
    }

    pub async fn competing_cache(
        &mut self,
        account: Uuid,
        model: Uuid,
        class: Option<&str>,
        at: DateTime<Utc>,
    ) -> sqlx::Result<Option<CompetingRow>> {
        let row = sqlx::query(
            "SELECT provisioning_source, serving_class, valid_from FROM model_cache_tariffs
             WHERE user_id = $1 AND deployed_model_id = $2
               AND (serving_class IS NULL OR $3::TEXT IS NULL OR serving_class = $3)
               AND (valid_until IS NULL OR valid_until > GREATEST(valid_from, $4))
               AND provisioning_source IS DISTINCT FROM $5
             ORDER BY valid_from LIMIT 1",
        )
        .bind(account)
        .bind(model)
        .bind(class)
        .bind(at)
        .bind(ADMIN_API_TARIFF_SOURCE)
        .fetch_optional(&mut *self.db)
        .await?;
        Ok(row.map(|row| competing("model_cache_tariffs", &row)))
    }

    pub async fn end_tariffs(&mut self, account: Uuid, model: Uuid, class: Option<&str>, at: DateTime<Utc>) -> sqlx::Result<u64> {
        let scope = "user_id = $1 AND deployed_model_id = $2 AND api_key_purpose = 'realtime'
                     AND COALESCE(serving_class, 'standard') = COALESCE($3, 'standard') AND provisioning_source = $5";
        self.end_rows("model_tariffs", scope, account, model, class, at).await
    }

    pub async fn end_cache(&mut self, account: Uuid, model: Uuid, class: Option<&str>, at: DateTime<Utc>) -> sqlx::Result<u64> {
        let scope = "user_id = $1 AND deployed_model_id = $2 AND serving_class IS NOT DISTINCT FROM $3 AND provisioning_source = $5";
        self.end_rows("model_cache_tariffs", scope, account, model, class, at).await
    }

    async fn end_rows(
        &mut self,
        table: &str,
        scope: &str,
        account: Uuid,
        model: Uuid,
        class: Option<&str>,
        at: DateTime<Utc>,
    ) -> sqlx::Result<u64> {
        let removed = sqlx::query(&format!("DELETE FROM {table} WHERE {scope} AND valid_from >= $4"))
            .bind(account)
            .bind(model)
            .bind(class)
            .bind(at)
            .bind(ADMIN_API_TARIFF_SOURCE)
            .execute(&mut *self.db)
            .await?
            .rows_affected();
        let ended = sqlx::query(&format!(
            "UPDATE {table} SET valid_until = $4 WHERE {scope} AND valid_from < $4 AND (valid_until IS NULL OR valid_until > $4)"
        ))
        .bind(account)
        .bind(model)
        .bind(class)
        .bind(at)
        .bind(ADMIN_API_TARIFF_SOURCE)
        .execute(&mut *self.db)
        .await?
        .rows_affected();
        Ok(removed + ended)
    }

    pub async fn insert_tariff(
        &mut self,
        account: Uuid,
        model: Uuid,
        class: Option<&str>,
        input: Decimal,
        output: Decimal,
        valid_from: DateTime<Utc>,
    ) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO model_tariffs (deployed_model_id, name, input_price_per_token, output_price_per_token,
                                        valid_from, api_key_purpose, user_id, serving_class, provisioning_source)
             VALUES ($1, 'admin-api', $2, $3, $4, 'realtime', $5, $6, $7)",
        )
        .bind(model)
        .bind(input)
        .bind(output)
        .bind(valid_from)
        .bind(account)
        .bind(class)
        .bind(ADMIN_API_TARIFF_SOURCE)
        .execute(&mut *self.db)
        .await?;
        Ok(())
    }

    pub async fn cache_base(
        &mut self,
        account: Uuid,
        model: Uuid,
        class: Option<&str>,
        at: DateTime<Utc>,
    ) -> sqlx::Result<Option<CacheBase>> {
        let row = sqlx::query(
            "SELECT write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens
             FROM model_cache_tariffs
             WHERE deployed_model_id = $1 AND valid_from <= $4 AND (valid_until IS NULL OR valid_until > $4)
               AND ((user_id = $2 AND (serving_class = $3 OR serving_class IS NULL))
                    OR (user_id IS NULL AND (serving_class IS NULL OR (serving_class = $3 AND class_routes_active($1)))))
               AND EXISTS (SELECT 1 FROM model_cache_tariffs g WHERE g.deployed_model_id = $1 AND g.user_id IS NULL
                           AND g.serving_class IS NULL AND g.valid_from <= $4 AND (g.valid_until IS NULL OR g.valid_until > $4))
             ORDER BY CASE WHEN user_id = $2 AND serving_class = $3 THEN 0 WHEN user_id = $2 THEN 1
                           WHEN serving_class IS NOT NULL THEN 2 ELSE 3 END, valid_from DESC
             LIMIT 1",
        )
        .bind(model)
        .bind(account)
        .bind(class)
        .bind(at)
        .fetch_optional(&mut *self.db)
        .await?;
        Ok(row.map(|row| CacheBase {
            write_multiplier_5m: row.get("write_multiplier_5m"),
            write_multiplier_1h: row.get("write_multiplier_1h"),
            write_multiplier_24h: row.get("write_multiplier_24h"),
            min_prefix_tokens: row.get("min_prefix_tokens"),
        }))
    }

    pub async fn class_cache_differs(&mut self, account: Uuid, model: Uuid, base: CacheBase, at: DateTime<Utc>) -> sqlx::Result<bool> {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM model_cache_tariffs
             WHERE deployed_model_id = $1 AND serving_class IS NOT NULL
               AND (user_id = $2 OR (user_id IS NULL AND class_routes_active($1)))
               AND valid_from <= $3 AND (valid_until IS NULL OR valid_until > $3)
               AND (write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, min_prefix_tokens)
                   IS DISTINCT FROM ($4::DECIMAL, $5::DECIMAL, $6::DECIMAL, $7::INTEGER))",
        )
        .bind(model)
        .bind(account)
        .bind(at)
        .bind(base.write_multiplier_5m)
        .bind(base.write_multiplier_1h)
        .bind(base.write_multiplier_24h)
        .bind(base.min_prefix_tokens)
        .fetch_one(&mut *self.db)
        .await
    }

    pub async fn insert_cache(
        &mut self,
        account: Uuid,
        model: Uuid,
        class: Option<&str>,
        base: CacheBase,
        read: Decimal,
        valid_from: DateTime<Utc>,
    ) -> sqlx::Result<()> {
        sqlx::query(
            "INSERT INTO model_cache_tariffs (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h,
                                              read_multiplier, min_prefix_tokens, valid_from, user_id, serving_class, provisioning_source)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(model)
        .bind(base.write_multiplier_5m)
        .bind(base.write_multiplier_1h)
        .bind(base.write_multiplier_24h)
        .bind(read)
        .bind(base.min_prefix_tokens)
        .bind(valid_from)
        .bind(account)
        .bind(class)
        .bind(ADMIN_API_TARIFF_SOURCE)
        .execute(&mut *self.db)
        .await?;
        Ok(())
    }

    pub async fn record(&mut self, change: &ChangeRecord<'_>, response: &Value) -> sqlx::Result<Uuid> {
        sqlx::query_scalar(
            "INSERT INTO account_price_changes (
                 request_id, request_fingerprint, action, user_id, deployed_model_id, serving_class, valid_from,
                 old_input_price_per_token, old_output_price_per_token, old_cache_read_multiplier,
                 new_input_price_per_token, new_output_price_per_token, new_cache_read_multiplier,
                 reason, changed_by, response)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
             RETURNING id",
        )
        .bind(change.request_id)
        .bind(change.fingerprint)
        .bind(change.action)
        .bind(change.account)
        .bind(change.model)
        .bind(change.serving_class)
        .bind(change.valid_from)
        .bind(change.old.map(|p| p.input))
        .bind(change.old.map(|p| p.output))
        .bind(change.old.and_then(|p| p.cache).map(|c| c.read))
        .bind(change.new.map(|p| p.input))
        .bind(change.new.map(|p| p.output))
        .bind(change.new.and_then(|p| p.cache).map(|c| c.read))
        .bind(change.reason)
        .bind(change.changed_by)
        .bind(response)
        .fetch_one(&mut *self.db)
        .await
    }

    pub async fn set_response(&mut self, id: Uuid, response: &Value) -> sqlx::Result<()> {
        sqlx::query("UPDATE account_price_changes SET response = $2 WHERE id = $1")
            .bind(id)
            .bind(response)
            .execute(&mut *self.db)
            .await?;
        Ok(())
    }
}

async fn lock_account_model(db: &mut PgConnection, account: Uuid, model: Uuid) -> sqlx::Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('account-price:' || $1::text || ':' || $2::text, 0))")
        .bind(account)
        .bind(model)
        .execute(&mut *db)
        .await?;
    Ok(())
}

fn competing(table: &'static str, row: &sqlx::postgres::PgRow) -> CompetingRow {
    CompetingRow {
        table,
        source: row.get("provisioning_source"),
        serving_class: row.get("serving_class"),
        valid_from: row.get("valid_from"),
    }
}

async fn competing_api_prices(
    db: &mut PgConnection,
    account: Uuid,
    model: Uuid,
    tariff_scopes: &[(Option<&str>, &str, Option<&str>)],
    cache_classes: &[Option<&str>],
    at: DateTime<Utc>,
) -> sqlx::Result<Option<CompetingRow>> {
    let rows = sqlx::query(
        "SELECT 'model_tariffs' AS tbl, serving_class, api_key_purpose, completion_window, valid_from FROM model_tariffs
         WHERE user_id = $1 AND deployed_model_id = $2 AND provisioning_source = $4
           AND (valid_until IS NULL OR valid_until > GREATEST(valid_from, $3))
         UNION ALL
         SELECT 'model_cache_tariffs', serving_class, NULL, NULL, valid_from FROM model_cache_tariffs
         WHERE user_id = $1 AND deployed_model_id = $2 AND provisioning_source = $4
           AND (valid_until IS NULL OR valid_until > GREATEST(valid_from, $3))",
    )
    .bind(account)
    .bind(model)
    .bind(at)
    .bind(ADMIN_API_TARIFF_SOURCE)
    .fetch_all(&mut *db)
    .await?;
    let tariff_class = |class: Option<&str>| class.unwrap_or(STANDARD_CLASS).to_owned();
    for row in rows {
        let table: String = row.get("tbl");
        let class: Option<String> = row.get("serving_class");
        let competes = if table == "model_tariffs" {
            let purpose: Option<String> = row.get("api_key_purpose");
            let window: Option<String> = row.get("completion_window");
            tariff_scopes.iter().any(|(declared_class, declared_purpose, declared_window)| {
                tariff_class(*declared_class) == tariff_class(class.as_deref())
                    && purpose.as_deref() == Some(*declared_purpose)
                    && window.as_deref() == *declared_window
            })
        } else {
            cache_classes
                .iter()
                .any(|declared| declared.is_none() || class.is_none() || *declared == class.as_deref())
        };
        if competes {
            return Ok(Some(CompetingRow {
                table: if table == "model_tariffs" {
                    "model_tariffs"
                } else {
                    "model_cache_tariffs"
                },
                source: Some(ADMIN_API_TARIFF_SOURCE.to_owned()),
                serving_class: class,
                valid_from: row.get("valid_from"),
            }));
        }
    }
    Ok(None)
}

#[derive(Default)]
pub(crate) struct DeclaredPrices<'a> {
    pub tariffs: Vec<(Option<&'a str>, &'a crate::model_provisioning::Tariff)>,
    pub cache_classes: Vec<Option<&'a str>>,
}

pub(crate) async fn refuse_competing_api_prices(
    db: &mut PgConnection,
    account: Uuid,
    model: Uuid,
    declared: &DeclaredPrices<'_>,
    at: DateTime<Utc>,
) -> anyhow::Result<()> {
    let (tariffs, cache_classes) = (&declared.tariffs, &declared.cache_classes);
    if tariffs.is_empty() && cache_classes.is_empty() {
        return Ok(());
    }
    lock_account_model(db, account, model).await?;
    let scopes: Vec<_> = tariffs
        .iter()
        .map(|(class, tariff)| (*class, tariff.purpose.as_db_str(), tariff.completion_window.as_deref()))
        .collect();
    if let Some(row) = competing_api_prices(db, account, model, &scopes, cache_classes, at).await? {
        anyhow::bail!(
            "declares a price that the admin API also sets ({}, class {:?}, from {}); end the API price or remove it from the file",
            row.table,
            row.serving_class.as_deref().unwrap_or("none"),
            row.valid_from
        );
    }
    Ok(())
}

pub(crate) struct DeclaredEntry<'a> {
    pub source: &'a str,
    pub account: &'a str,
    pub alias: &'a str,
    pub prices: DeclaredPrices<'a>,
}

pub(crate) async fn api_price_conflicts(db: &mut PgConnection, entries: &[DeclaredEntry<'_>]) -> anyhow::Result<Vec<String>> {
    let entries: Vec<&DeclaredEntry<'_>> = entries
        .iter()
        .filter(|entry| !entry.prices.tariffs.is_empty() || !entry.prices.cache_classes.is_empty())
        .collect();
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let names: Vec<&str> = entries.iter().map(|entry| entry.account).collect();
    let aliases: Vec<&str> = entries.iter().map(|entry| entry.alias).collect();
    let accounts: std::collections::HashMap<String, Uuid> =
        sqlx::query_as("SELECT username, id FROM users WHERE username = ANY($1) AND is_deleted = FALSE")
            .bind(&names)
            .fetch_all(&mut *db)
            .await?
            .into_iter()
            .collect();
    let models: std::collections::HashMap<String, Uuid> =
        sqlx::query_as("SELECT alias, id FROM deployed_models WHERE alias = ANY($1) AND deleted = FALSE")
            .bind(&aliases)
            .fetch_all(&mut *db)
            .await?
            .into_iter()
            .collect();
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT NOW()").fetch_one(&mut *db).await?;
    let mut conflicts = Vec::new();
    for entry in entries {
        let (Some(account), Some(model)) = (accounts.get(entry.account), models.get(entry.alias)) else {
            continue;
        };
        let scopes: Vec<_> = entry
            .prices
            .tariffs
            .iter()
            .map(|(class, tariff)| (*class, tariff.purpose.as_db_str(), tariff.completion_window.as_deref()))
            .collect();
        if let Some(row) = competing_api_prices(db, *account, *model, &scopes, &entry.prices.cache_classes, now).await? {
            conflicts.push(format!(
                "{}: account {:?}, model {:?} declares a price that the admin API also sets ({}, class {:?}, from {}); end the API price or remove it from the file",
                entry.source,
                entry.account,
                entry.alias,
                row.table,
                row.serving_class.as_deref().unwrap_or("none"),
                row.valid_from
            ));
        }
    }
    Ok(conflicts)
}
