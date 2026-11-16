
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::config::CachePricingConfig;
use crate::db::models::api_keys::ApiKeyPurpose;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenCounts {
    pub prompt: i64,
    pub completion: i64,
    pub cache_read: i64,
    pub cache_creation_5m: i64,
    pub cache_creation_1h: i64,
    pub cache_creation_24h: i64,
}

#[derive(Clone)]
pub(crate) struct CacheTariffRow {
    pub write_multiplier_5m: Decimal,
    pub write_multiplier_1h: Decimal,
    pub write_multiplier_24h: Decimal,
    pub read_multiplier: Decimal,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub account: Option<uuid::Uuid>,
    pub serving_class: Option<String>,
}

pub(crate) fn clamp_implicit_read_multiplier(mults: Option<CacheMultipliers>, read_source: Option<&str>) -> Option<CacheMultipliers> {
    match (mults, read_source) {
        (Some(m), Some(s)) if s == crate::prompt_cache::CacheReadSource::Engine.as_str() && m.read > Decimal::ONE => {
            Some(CacheMultipliers { read: Decimal::ONE, ..m })
        }
        _ => mults,
    }
}

#[derive(Clone, Copy)]
pub(crate) struct CacheMultipliers {
    pub read: Decimal,
    pub write_5m: Decimal,
    pub write_1h: Decimal,
    pub write_24h: Decimal,
}

impl CacheMultipliers {
    pub fn from_config(c: &CachePricingConfig) -> Self {
        Self {
            read: c.default_read_multiplier,
            write_5m: c.default_write_multiplier_5m,
            write_1h: c.default_write_multiplier_1h,
            write_24h: c.default_write_multiplier_24h,
        }
    }
}

impl Default for CacheMultipliers {
    fn default() -> Self {
        Self::from_config(&CachePricingConfig::default())
    }
}

#[derive(Debug)]
pub(crate) struct ModelInfo {
    pub provider_name: String,
    pub tariffs: Vec<TariffInfo>,
}

#[derive(Debug, Clone)]
pub(crate) struct TariffInfo {
    pub id: uuid::Uuid,
    pub purpose: ApiKeyPurpose,
    pub effective_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub input_price_per_token: Decimal,
    pub output_price_per_token: Decimal,
    pub completion_window: Option<String>,
    pub account: Option<uuid::Uuid>,
    pub serving_class: Option<String>,
}

pub(crate) async fn lookup_cache_tariffs<'e, E>(
    executor: E,
    aliases: &[String],
    accounts: &[uuid::Uuid],
) -> Result<std::collections::HashMap<String, Vec<CacheTariffRow>>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    struct Row {
        alias: String,
        write_multiplier_5m: Decimal,
        write_multiplier_1h: Decimal,
        write_multiplier_24h: Decimal,
        read_multiplier: Decimal,
        valid_from: DateTime<Utc>,
        valid_until: Option<DateTime<Utc>>,
        account: Option<uuid::Uuid>,
        serving_class: Option<String>,
    }

    let rows: Vec<Row> = sqlx::query_as!(
        Row,
        r#"
        SELECT
            dm.alias,
            mct.write_multiplier_5m,
            mct.write_multiplier_1h,
            mct.write_multiplier_24h,
            mct.read_multiplier,
            mct.valid_from,
            mct.valid_until,
            mct.user_id as "account?",
            mct.serving_class
        FROM deployed_models dm
        JOIN model_cache_tariffs mct ON mct.deployed_model_id = dm.id
        WHERE dm.alias = ANY($1)
          AND (mct.user_id IS NULL OR mct.user_id = ANY($2))
          AND (mct.user_id IS NOT NULL OR mct.serving_class IS NULL OR class_routes_active(dm.id))
        ORDER BY dm.alias, mct.valid_from DESC
        "#,
        aliases,
        accounts
    )
    .fetch_all(executor)
    .await?;

    let mut map: std::collections::HashMap<String, Vec<CacheTariffRow>> = std::collections::HashMap::new();
    for row in rows {
        map.entry(row.alias).or_default().push(CacheTariffRow {
            write_multiplier_5m: row.write_multiplier_5m,
            write_multiplier_1h: row.write_multiplier_1h,
            write_multiplier_24h: row.write_multiplier_24h,
            read_multiplier: row.read_multiplier,
            valid_from: row.valid_from,
            valid_until: row.valid_until,
            account: row.account,
            serving_class: row.serving_class,
        });
    }
    Ok(map)
}

pub(crate) fn resolve_cache_multipliers(
    rows: &[CacheTariffRow],
    timestamp: DateTime<Utc>,
    account: Option<uuid::Uuid>,
    serving_class: Option<&str>,
) -> Option<CacheMultipliers> {
    let valid = |scope: Option<uuid::Uuid>, class: Option<&str>| {
        rows.iter()
            .filter(|r| {
                r.account == scope
                    && r.serving_class.as_deref() == class
                    && r.valid_from <= timestamp
                    && r.valid_until.is_none_or(|u| u > timestamp)
            })
            .max_by_key(|r| r.valid_from)
    };
    let general = valid(None, None)?;
    Some(
        account
            .and_then(|a| {
                serving_class
                    .and_then(|class| valid(Some(a), Some(class)))
                    .or_else(|| valid(Some(a), None))
            })
            .or_else(|| serving_class.and_then(|class| valid(None, Some(class))))
            .unwrap_or(general),
    )
    .map(|r| CacheMultipliers {
        read: r.read_multiplier,
        write_5m: r.write_multiplier_5m,
        write_1h: r.write_multiplier_1h,
        write_24h: r.write_multiplier_24h,
    })
}

pub(crate) fn find_best_tariff(
    tariffs: &[TariffInfo],
    api_key_purpose: Option<&ApiKeyPurpose>,
    completion_window: Option<&str>,
    timestamp: DateTime<Utc>,
    account: Option<uuid::Uuid>,
    serving_class: Option<&str>,
) -> (Option<Decimal>, Option<Decimal>) {
    let purpose = api_key_purpose.unwrap_or(&ApiKeyPurpose::Realtime);
    if !purpose.is_customer_billing() {
        return (None, None);
    }
    let class = if *purpose == ApiKeyPurpose::Batch {
        Some("standard")
    } else {
        serving_class
    };
    let window = if *purpose == ApiKeyPurpose::Batch {
        completion_window
    } else {
        None
    };
    tariffs
        .iter()
        .filter_map(|t| {
            if t.effective_from > timestamp || t.valid_until.is_some_and(|until| until <= timestamp) {
                return None;
            }
            let purpose_rank = if t.purpose == *purpose {
                0
            } else if matches!(purpose, ApiKeyPurpose::Playground | ApiKeyPurpose::Batch) && t.purpose == ApiKeyPurpose::Realtime {
                1
            } else {
                return None;
            };
            if t.completion_window.as_deref() != if t.purpose == ApiKeyPurpose::Batch { window } else { None } {
                return None;
            }
            let scope_rank = match (t.account, t.serving_class.as_deref()) {
                (Some(owner), Some(c)) if Some(owner) == account && Some(c) == class => 0,
                (Some(owner), None) if Some(owner) == account && class.is_none_or(|c| c == "standard") => 1,
                (None, Some(c)) if Some(c) == class => 2,
                (None, None) => 3,
                _ => return None,
            };
            let fallback_phase = if *purpose == ApiKeyPurpose::Batch { purpose_rank } else { 0 };
            Some((
                (fallback_phase, scope_rank, purpose_rank, std::cmp::Reverse(t.effective_from), t.id),
                t,
            ))
        })
        .min_by_key(|(rank, _)| *rank)
        .map(|(_, t)| (Some(t.input_price_per_token), Some(t.output_price_per_token)))
        .unwrap_or((None, None))
}

pub(crate) fn charged_cost(
    counts: &TokenCounts,
    model: Option<&str>,
    input_price: Option<Decimal>,
    output_price: Option<Decimal>,
    cache_mults: Option<CacheMultipliers>,
    component: &'static str,
) -> Option<Decimal> {
    match cache_mults {
        Some(m) => compute_total_cost(counts, model, input_price, output_price, &m, component),
        None => list_price(counts.prompt, counts.completion, input_price, output_price),
    }
}

pub(crate) fn compute_total_cost(
    counts: &TokenCounts,
    model: Option<&str>,
    input_price: Option<Decimal>,
    output_price: Option<Decimal>,
    m: &CacheMultipliers,
    component: &'static str,
) -> Option<Decimal> {
    if input_price.is_none() && output_price.is_none() {
        return None;
    }
    let inp = input_price.unwrap_or(Decimal::ZERO);
    let outp = output_price.unwrap_or(Decimal::ZERO);

    let mut read = Decimal::from(counts.cache_read.max(0));
    let c5 = Decimal::from(counts.cache_creation_5m.max(0));
    let c1 = Decimal::from(counts.cache_creation_1h.max(0));
    let c24 = Decimal::from(counts.cache_creation_24h.max(0));
    let prompt = Decimal::from(counts.prompt.max(0));
    let creations = c5 + c1 + c24;

    if creations > prompt {
        crate::background_error!(
            component,
            "cache_split_exceeds_prompt",
            Warning,
            model = model.unwrap_or("?"),
            prompt_tokens = counts.prompt,
            "cache write counts alone exceed prompt_tokens; ignoring the split and billing at base rate"
        );
        return list_price(counts.prompt, counts.completion, input_price, output_price);
    }
    if read + creations > prompt {
        metrics::counter!("dwctl_cache_split_capped_total").increment(1);
        tracing::debug!(
            model = model.unwrap_or("?"),
            prompt_tokens = counts.prompt,
            overrun = %(read + creations - prompt),
            "cached token split exceeds prompt_tokens; capping the read count to fit"
        );
        read = prompt - creations;
    }
    let cached_total = read + creations;

    let uncached = (prompt - cached_total).max(Decimal::ZERO);

    let input_cost = uncached * inp + read * inp * m.read + c5 * inp * m.write_5m + c1 * inp * m.write_1h + c24 * inp * m.write_24h;
    let output_cost = Decimal::from(counts.completion.max(0)) * outp;
    Some(input_cost + output_cost)
}

pub(crate) fn list_price(
    prompt_tokens: i64,
    completion_tokens: i64,
    input_price: Option<Decimal>,
    output_price: Option<Decimal>,
) -> Option<Decimal> {
    if input_price.is_none() && output_price.is_none() {
        return None;
    }
    let inp = input_price.unwrap_or(Decimal::ZERO);
    let outp = output_price.unwrap_or(Decimal::ZERO);
    Some(Decimal::from(prompt_tokens.max(0)) * inp + Decimal::from(completion_tokens.max(0)) * outp)
}

pub(crate) fn parse_api_key_purpose(s: &str) -> ApiKeyPurpose {
    match s {
        "platform" => ApiKeyPurpose::Platform,
        "batch" => ApiKeyPurpose::Batch,
        "playground" => ApiKeyPurpose::Playground,
        "continuation" => ApiKeyPurpose::Continuation,
        _ => ApiKeyPurpose::Realtime,
    }
}

pub(crate) async fn lookup_model_tariffs<'e, E>(
    executor: E,
    aliases: &[String],
    accounts: &[uuid::Uuid],
) -> Result<std::collections::HashMap<String, ModelInfo>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    struct ModelRow {
        alias: String,
        provider_name: Option<String>,
        tariff_id: Option<uuid::Uuid>,
        tariff_purpose: Option<String>,
        tariff_valid_from: Option<DateTime<Utc>>,
        tariff_valid_until: Option<DateTime<Utc>>,
        tariff_input_price: Option<Decimal>,
        tariff_output_price: Option<Decimal>,
        tariff_completion_window: Option<String>,
        tariff_account: Option<uuid::Uuid>,
        tariff_class: Option<String>,
    }

    let rows: Vec<ModelRow> = sqlx::query_as!(
        ModelRow,
        r#"
            SELECT
                dm.alias,
                ie.name as "provider_name?",
                mt.id as "tariff_id?",
                mt.api_key_purpose as "tariff_purpose?",
                mt.valid_from as "tariff_valid_from?",
                mt.valid_until as "tariff_valid_until?",
                mt.input_price_per_token as "tariff_input_price?",
                mt.output_price_per_token as "tariff_output_price?",
                mt.completion_window as "tariff_completion_window?",
                mt.user_id as "tariff_account?",
                mt.serving_class as "tariff_class?"
            FROM deployed_models dm
            LEFT JOIN inference_endpoints ie ON dm.hosted_on = ie.id
            LEFT JOIN model_tariffs mt ON mt.deployed_model_id = dm.id
                AND (mt.user_id IS NULL OR mt.user_id = ANY($2))
                AND (mt.user_id IS NOT NULL OR mt.serving_class IS NULL OR class_routes_active(dm.id))
            WHERE dm.alias = ANY($1)
            ORDER BY dm.alias, mt.valid_from DESC
            "#,
        aliases,
        accounts
    )
    .fetch_all(executor)
    .await?;

    let mut map: std::collections::HashMap<String, ModelInfo> = std::collections::HashMap::new();
    for row in rows {
        let entry = map.entry(row.alias.clone()).or_insert_with(|| ModelInfo {
            provider_name: row.provider_name.unwrap_or_default(),
            tariffs: Vec::new(),
        });

        if let (Some(id), Some(purpose), Some(valid_from), Some(input_price), Some(output_price)) = (
            row.tariff_id,
            row.tariff_purpose,
            row.tariff_valid_from,
            row.tariff_input_price,
            row.tariff_output_price,
        ) {
            entry.tariffs.push(TariffInfo {
                id,
                serving_class: row.tariff_class,
                purpose: parse_api_key_purpose(&purpose),
                effective_from: valid_from,
                valid_until: row.tariff_valid_until,
                input_price_per_token: input_price,
                output_price_per_token: output_price,
                completion_window: row.tariff_completion_window,
                account: row.tariff_account,
            });
        }
    }
    Ok(map)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnitPrices {
    pub prompt: Decimal,
    pub cached: Decimal,
    pub output: Decimal,
}

impl UnitPrices {
    pub const FREE: Self = Self {
        prompt: Decimal::ZERO,
        cached: Decimal::ZERO,
        output: Decimal::ZERO,
    };

    pub fn new(input_price: Option<Decimal>, output_price: Option<Decimal>, cache: Option<CacheMultipliers>) -> Self {
        let input = input_price.unwrap_or(Decimal::ZERO);
        Self {
            prompt: input,
            cached: cache.map_or(input, |m| input * m.read),
            output: output_price.unwrap_or(Decimal::ZERO),
        }
    }

    pub fn to_billing(self) -> onwards::serving::BillingPrices {
        use rust_decimal::prelude::ToPrimitive;
        onwards::serving::BillingPrices {
            prompt: self.prompt.to_f64().unwrap_or(0.0),
            cached: self.cached.to_f64().unwrap_or(0.0),
            output: self.output.to_f64().unwrap_or(0.0),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PriceQuery<'a> {
    pub model: &'a str,
    pub completion_window: Option<&'a str>,
    pub batch_created_at: Option<DateTime<Utc>>,
    pub now: DateTime<Utc>,
    pub serving_class: &'a str,
    pub cache_reads: Option<crate::prompt_cache::CacheReadSource>,
}

#[derive(Default)]
pub(crate) struct PriceBook {
    models: std::collections::HashMap<String, ModelInfo>,
    cache_tariffs: std::collections::HashMap<String, Vec<CacheTariffRow>>,
    accounts: std::collections::HashSet<uuid::Uuid>,
}

impl PriceBook {
    pub async fn load(pool: &sqlx::PgPool, accounts: &[uuid::Uuid]) -> Result<Self, sqlx::Error> {
        let aliases = sqlx::query_scalar!("SELECT DISTINCT alias FROM deployed_models")
            .fetch_all(pool)
            .await?;
        Ok(Self {
            models: lookup_model_tariffs(pool, &aliases, accounts).await?,
            cache_tariffs: lookup_cache_tariffs(pool, &aliases, accounts).await?,
            accounts: accounts.iter().copied().collect(),
        })
    }

    pub fn unit_prices(&self, account: uuid::Uuid, purpose: &ApiKeyPurpose, query: &PriceQuery<'_>) -> Option<UnitPrices> {
        if account.is_nil() {
            return Some(UnitPrices::FREE);
        }
        if !self.accounts.contains(&account) {
            return None;
        }
        let model = self.models.get(query.model)?;
        let timestamp = query.batch_created_at.unwrap_or(query.now);
        let (input, output) = find_best_tariff(
            &model.tariffs,
            Some(purpose),
            query.completion_window,
            timestamp,
            Some(account),
            Some(query.serving_class),
        );
        let rows = self.cache_tariffs.get(query.model).map(Vec::as_slice).unwrap_or_default();
        let cache = query
            .cache_reads
            .filter(|_| resolve_cache_multipliers(rows, query.now, None, None).is_some())
            .and_then(|source| {
                let mults = resolve_cache_multipliers(rows, timestamp, Some(account), Some(query.serving_class));
                clamp_implicit_read_multiplier(mults, Some(source.as_str()))
            });
        Some(UnitPrices::new(input, output, cache))
    }

    #[cfg(test)]
    pub fn from_rows(
        accounts: impl IntoIterator<Item = uuid::Uuid>,
        models: impl IntoIterator<Item = (String, Vec<TariffInfo>)>,
        cache_tariffs: impl IntoIterator<Item = (String, Vec<CacheTariffRow>)>,
    ) -> Self {
        Self {
            models: models
                .into_iter()
                .map(|(alias, tariffs)| {
                    (
                        alias,
                        ModelInfo {
                            provider_name: String::new(),
                            tariffs,
                        },
                    )
                })
                .collect(),
            cache_tariffs: cache_tariffs.into_iter().collect(),
            accounts: accounts.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::errors::component::ANALYTICS_BATCHER;
    use rust_decimal::prelude::FromStr;

    fn counts(prompt: i64, completion: i64, read: i64, c5: i64, c1: i64, c24: i64) -> TokenCounts {
        TokenCounts {
            prompt,
            completion,
            cache_read: read,
            cache_creation_5m: c5,
            cache_creation_1h: c1,
            cache_creation_24h: c24,
        }
    }

    fn cost(c: &TokenCounts, m: &CacheMultipliers) -> Option<Decimal> {
        compute_total_cost(c, Some("m"), Some(inp()), Some(outp()), m, ANALYTICS_BATCHER)
    }

    fn list(c: &TokenCounts) -> Option<Decimal> {
        list_price(c.prompt, c.completion, Some(inp()), Some(outp()))
    }

    fn inp() -> Decimal {
        Decimal::new(1, 3)
    }
    fn outp() -> Decimal {
        Decimal::new(2, 3)
    }

    #[test]
    fn cost_without_cache_is_plain_arithmetic() {
        let c = counts(1000, 100, 0, 0, 0, 0);
        assert_eq!(cost(&c, &CacheMultipliers::default()).unwrap(), Decimal::new(12, 1));
    }

    #[test]
    fn cost_with_cache_applies_per_tier_multipliers() {
        let c = counts(2000, 100, 1000, 0, 500, 0);
        let m = CacheMultipliers {
            read: Decimal::new(1, 1),
            write_5m: Decimal::ONE,
            write_1h: Decimal::from(2),
            write_24h: Decimal::ONE,
        };
        assert_eq!(cost(&c, &m).unwrap(), Decimal::new(18, 1));
    }

    #[test]
    fn cost_none_when_no_pricing() {
        let c = counts(1000, 100, 0, 0, 0, 0);
        assert!(compute_total_cost(&c, Some("m"), None, None, &CacheMultipliers::default(), ANALYTICS_BATCHER).is_none());
    }

    #[test]
    fn corrupt_writes_exceeding_prompt_bill_at_base_rate() {
        let c = counts(100, 5, 0, 150, 0, 0);
        let got = cost(&c, &CacheMultipliers::default()).unwrap();
        assert_eq!(got, Decimal::new(11, 2));
        assert_eq!(got, list(&c).unwrap());
    }

    #[test]
    fn drifted_split_is_capped_by_reducing_read_not_discarded() {
        let m = CacheMultipliers::default();
        let c = counts(10_100, 5, 10_000, 200, 0, 0);
        let got = cost(&c, &m).unwrap();
        assert_eq!(got, Decimal::new(125, 2));
        assert!(got < list(&c).unwrap());
    }

    #[test]
    fn split_exactly_at_prompt_is_untouched() {
        let c = counts(10_000, 0, 9_800, 200, 0, 0);
        assert_eq!(cost(&c, &CacheMultipliers::default()).unwrap(), Decimal::new(123, 2));
    }

    #[test]
    fn cap_can_consume_the_entire_read() {
        let c = counts(1_000, 0, 50, 1_000, 0, 0);
        assert_eq!(cost(&c, &CacheMultipliers::default()).unwrap(), Decimal::new(125, 2));
    }

    #[test]
    fn charged_cost_gates_discount_on_enablement() {
        let c = counts(1000, 100, 600, 0, 0, 0);

        let not_enabled = charged_cost(&c, Some("m"), Some(inp()), Some(outp()), None, ANALYTICS_BATCHER).unwrap();
        assert_eq!(not_enabled, list(&c).unwrap());
        assert_eq!(not_enabled, Decimal::new(12, 1));

        let m = CacheMultipliers {
            read: Decimal::new(1, 1),
            write_5m: Decimal::ONE,
            write_1h: Decimal::ONE,
            write_24h: Decimal::ONE,
        };
        let enabled = charged_cost(&c, Some("m"), Some(inp()), Some(outp()), Some(m), ANALYTICS_BATCHER).unwrap();
        assert_eq!(enabled, Decimal::new(66, 2));
        assert!(enabled < not_enabled, "the discount must make the enabled case cheaper");
    }

    #[test]
    fn list_price_ignores_cache_split_and_is_none_without_pricing() {
        let c = counts(1000, 100, 500, 0, 200, 0);
        assert_eq!(list(&c).unwrap(), Decimal::new(12, 1));
        assert!(
            list_price(c.prompt, c.completion, None, None).is_none(),
            "no pricing → NULL list price"
        );
    }

    fn tariff_row(write_1h: Decimal, from_hrs: i64, valid_until: Option<DateTime<Utc>>) -> CacheTariffRow {
        CacheTariffRow {
            serving_class: None,
            write_multiplier_5m: Decimal::new(125, 2),
            write_multiplier_1h: write_1h,
            write_multiplier_24h: Decimal::new(25, 1),
            read_multiplier: Decimal::new(1, 1),
            valid_from: chrono::Utc::now() - chrono::Duration::hours(from_hrs),
            valid_until,
            account: None,
        }
    }

    #[test]
    fn resolve_multipliers_prefers_the_billed_organisations_version() {
        let now = chrono::Utc::now();
        let org = uuid::Uuid::new_v4();
        let other = uuid::Uuid::new_v4();
        let mut org_row = tariff_row(Decimal::from(7), 1, None);
        org_row.account = Some(org);
        let mut other_row = tariff_row(Decimal::from(9), 1, None);
        other_row.account = Some(other);
        let rows = vec![tariff_row(Decimal::from(2), 3, None), org_row, other_row];
        assert_eq!(
            resolve_cache_multipliers(&rows, now, Some(org), None).unwrap().write_1h,
            Decimal::from(7)
        );
        assert_eq!(
            resolve_cache_multipliers(&rows, now, None, None).unwrap().write_1h,
            Decimal::from(2)
        );
        assert_eq!(
            resolve_cache_multipliers(&rows, now, Some(uuid::Uuid::new_v4()), None)
                .unwrap()
                .write_1h,
            Decimal::from(2)
        );
        let mut expired = tariff_row(Decimal::from(7), 3, Some(now - chrono::Duration::hours(1)));
        expired.account = Some(org);
        let rows = vec![tariff_row(Decimal::from(2), 3, None), expired];
        assert_eq!(
            resolve_cache_multipliers(&rows, now, Some(org), None).unwrap().write_1h,
            Decimal::from(2)
        );
    }

    #[test]
    fn resolve_multipliers_picks_latest_valid_version() {
        let now = chrono::Utc::now();
        let rows = vec![tariff_row(Decimal::from(2), 1, None), tariff_row(Decimal::from(3), 5, None)];
        let m = resolve_cache_multipliers(&rows, now, None, None).expect("a valid version exists");
        assert_eq!(m.write_1h, Decimal::from(2), "latest valid version wins");
        assert_eq!(m.write_5m, Decimal::new(125, 2), "all tiers come from that one row");
        assert_eq!(m.write_24h, Decimal::new(25, 1));
        assert_eq!(m.read, Decimal::new(1, 1));
    }

    #[test]
    fn resolve_multipliers_none_when_empty_or_expired() {
        let now = chrono::Utc::now();
        assert!(resolve_cache_multipliers(&[], now, None, None).is_none(), "no rows → None");
        let expired = vec![tariff_row(Decimal::from(5), 2, Some(now - chrono::Duration::hours(1)))];
        assert!(
            resolve_cache_multipliers(&expired, now, None, None).is_none(),
            "expired version ignored → None"
        );
    }

    #[test]
    fn default_multipliers_mirror_config_pricing_defaults() {
        let m = CacheMultipliers::default();
        let c = CachePricingConfig::default();
        assert_eq!(m.read, c.default_read_multiplier, "read = config default (0.1)");
        assert_eq!(m.write_5m, c.default_write_multiplier_5m, "5m = config default (1.25)");
        assert_eq!(m.write_1h, c.default_write_multiplier_1h, "1h = config default (2.0)");
        assert_eq!(m.write_24h, c.default_write_multiplier_24h, "24h = config default (2.5)");
    }

    fn make_tariff(
        purpose: ApiKeyPurpose,
        effective_from: DateTime<Utc>,
        valid_until: Option<DateTime<Utc>>,
        input_price: &str,
        output_price: &str,
        completion_window: Option<&str>,
    ) -> TariffInfo {
        TariffInfo {
            id: uuid::Uuid::new_v4(),
            serving_class: None,
            purpose,
            effective_from,
            valid_until,
            input_price_per_token: Decimal::from_str(input_price).unwrap(),
            output_price_per_token: Decimal::from_str(output_price).unwrap(),
            completion_window: completion_window.map(|s| s.to_string()),
            account: None,
        }
    }

    #[test]
    fn find_best_tariff_overrides_only_the_requested_purpose() {
        let now = chrono::Utc::now();
        let org = uuid::Uuid::new_v4();
        let from = now - chrono::Duration::days(1);
        let mut org_realtime = make_tariff(ApiKeyPurpose::Realtime, from, None, "0.00001", "0.00002", None);
        org_realtime.account = Some(org);
        let tariffs = vec![
            make_tariff(ApiKeyPurpose::Realtime, from, None, "0.00010", "0.00020", None),
            make_tariff(ApiKeyPurpose::Batch, from, None, "0.00005", "0.00010", Some("24h")),
            org_realtime,
        ];
        let (input, _) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Realtime), None, now, Some(org), None);
        assert_eq!(input, Some(Decimal::from_str("0.00001").unwrap()));
        let (input, _) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Batch), Some("24h"), now, Some(org), None);
        assert_eq!(input, Some(Decimal::from_str("0.00005").unwrap()));
        let (input, _) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Realtime), None, now, None, None);
        assert_eq!(input, Some(Decimal::from_str("0.00010").unwrap()));
        let (input, _) = find_best_tariff(
            &tariffs,
            Some(&ApiKeyPurpose::Batch),
            Some("24h"),
            now,
            Some(uuid::Uuid::new_v4()),
            None,
        );
        assert_eq!(input, Some(Decimal::from_str("0.00005").unwrap()));
    }

    #[test]
    fn test_find_best_tariff_exact_match() {
        let now = chrono::Utc::now();
        let tariffs = vec![make_tariff(
            ApiKeyPurpose::Realtime,
            now - chrono::Duration::days(1),
            None,
            "0.00010",
            "0.00020",
            None,
        )];

        let (input, output) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Realtime), None, now, None, None);
        assert_eq!(input, Some(Decimal::from_str("0.00010").unwrap()));
        assert_eq!(output, Some(Decimal::from_str("0.00020").unwrap()));
    }

    #[test]
    fn test_find_best_tariff_batch_vs_realtime() {
        let now = chrono::Utc::now();
        let tariffs = vec![
            make_tariff(
                ApiKeyPurpose::Realtime,
                now - chrono::Duration::days(1),
                None,
                "0.00010",
                "0.00020",
                None,
            ),
            make_tariff(
                ApiKeyPurpose::Batch,
                now - chrono::Duration::days(1),
                None,
                "0.00005",
                "0.00010",
                None,
            ),
        ];

        let (input, output) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Batch), None, now, None, None);
        assert_eq!(input, Some(Decimal::from_str("0.00005").unwrap()));
        assert_eq!(output, Some(Decimal::from_str("0.00010").unwrap()));

        let (input, output) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Realtime), None, now, None, None);
        assert_eq!(input, Some(Decimal::from_str("0.00010").unwrap()));
        assert_eq!(output, Some(Decimal::from_str("0.00020").unwrap()));
    }

    #[test]
    fn missing_batch_and_playground_prices_fall_back_to_realtime() {
        let now = chrono::Utc::now();
        let tariffs = vec![make_tariff(
            ApiKeyPurpose::Realtime,
            now - chrono::Duration::days(1),
            None,
            "0.00015",
            "0.00030",
            None,
        )];

        assert_eq!(
            find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Batch), Some("24h"), now, None, None),
            (
                Some(Decimal::from_str("0.00015").unwrap()),
                Some(Decimal::from_str("0.00030").unwrap())
            )
        );
        let (input, output) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Playground), None, now, None, None);
        assert_eq!(input, Some(Decimal::from_str("0.00015").unwrap()));
        assert_eq!(output, Some(Decimal::from_str("0.00030").unwrap()));
    }

    #[test]
    fn test_find_best_tariff_historical_pricing() {
        let now = chrono::Utc::now();
        let old_tariff_start = now - chrono::Duration::days(30);
        let old_tariff_end = now - chrono::Duration::days(10);
        let new_tariff_start = now - chrono::Duration::days(10);

        let tariffs = vec![
            make_tariff(
                ApiKeyPurpose::Realtime,
                old_tariff_start,
                Some(old_tariff_end),
                "0.00020",
                "0.00040",
                None,
            ),
            make_tariff(
                ApiKeyPurpose::Realtime,
                new_tariff_start,
                None,
                "0.00010",
                "0.00020",
                None,
            ),
        ];

        let (input, output) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Realtime), None, now, None, None);
        assert_eq!(
            input,
            Some(Decimal::from_str("0.00010").unwrap()),
            "Current request should use new pricing"
        );
        assert_eq!(output, Some(Decimal::from_str("0.00020").unwrap()));

        let historical_time = now - chrono::Duration::days(20);
        let (input, output) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Realtime), None, historical_time, None, None);
        assert_eq!(
            input,
            Some(Decimal::from_str("0.00020").unwrap()),
            "Historical request should use old pricing"
        );
        assert_eq!(output, Some(Decimal::from_str("0.00040").unwrap()));
    }

    #[test]
    fn test_find_best_tariff_completion_window_exact_match() {
        let now = chrono::Utc::now();
        let tariffs = vec![
            make_tariff(
                ApiKeyPurpose::Batch,
                now - chrono::Duration::days(1),
                None,
                "0.00010",
                "0.00020",
                None,
            ),
            make_tariff(
                ApiKeyPurpose::Batch,
                now - chrono::Duration::days(1),
                None,
                "0.00005",
                "0.00010",
                Some("24h"),
            ),
        ];

        let (input, output) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Batch), Some("24h"), now, None, None);
        assert_eq!(
            input,
            Some(Decimal::from_str("0.00005").unwrap()),
            "24h priority should get specific pricing"
        );
        assert_eq!(output, Some(Decimal::from_str("0.00010").unwrap()));

        let (input, output) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Batch), None, now, None, None);
        assert_eq!(
            input,
            Some(Decimal::from_str("0.00010").unwrap()),
            "No priority should get generic pricing"
        );
        assert_eq!(output, Some(Decimal::from_str("0.00020").unwrap()));
    }

    #[test]
    fn missing_batch_window_does_not_borrow_another_tariff() {
        let now = chrono::Utc::now();
        let tariffs = vec![
            make_tariff(
                ApiKeyPurpose::Batch,
                now - chrono::Duration::days(1),
                None,
                "0.00010",
                "0.00020",
                None,
            ),
            make_tariff(
                ApiKeyPurpose::Batch,
                now - chrono::Duration::days(1),
                None,
                "0.00005",
                "0.00010",
                Some("24h"),
            ),
            make_tariff(
                ApiKeyPurpose::Batch,
                now - chrono::Duration::days(1),
                None,
                "0.00003",
                "0.00006",
                Some("7d"),
            ),
        ];

        let actual = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Batch), Some("1h"), now, None, None);
        assert_eq!(actual, (None, None), "An absent SLA is not another tier or a generic batch price");
    }

    #[test]
    fn test_find_best_tariff_no_matching_tariff() {
        let now = chrono::Utc::now();
        let tariffs = vec![];

        let (input, output) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Realtime), None, now, None, None);
        assert_eq!(input, None);
        assert_eq!(output, None);
    }

    #[test]
    fn test_find_best_tariff_future_tariff_not_used() {
        let now = chrono::Utc::now();
        let tariffs = vec![make_tariff(
            ApiKeyPurpose::Realtime,
            now + chrono::Duration::days(1),
            None,
            "0.00010",
            "0.00020",
            None,
        )];

        let (input, output) = find_best_tariff(&tariffs, Some(&ApiKeyPurpose::Realtime), None, now, None, None);
        assert_eq!(input, None, "Future tariff should not be selected");
        assert_eq!(output, None);
    }

    fn priced(unit: UnitPrices, prompt: i64, completion: i64, cached: i64) -> Decimal {
        unit.prompt * Decimal::from(prompt - cached) + unit.cached * Decimal::from(cached) + unit.output * Decimal::from(completion)
    }

    #[test]
    fn unit_prices_times_usage_is_the_charged_cost() {
        let tariffs = [
            (Some(inp()), Some(outp())),
            (Some(inp()), None),
            (None, Some(outp())),
            (Some(Decimal::ZERO), Some(Decimal::ZERO)),
            (None, None),
        ];
        let caches = [
            None,
            Some(CacheMultipliers::default()),
            Some(CacheMultipliers {
                read: Decimal::new(5, 1),
                ..CacheMultipliers::default()
            }),
            clamp_implicit_read_multiplier(
                Some(CacheMultipliers {
                    read: Decimal::new(15, 1),
                    ..CacheMultipliers::default()
                }),
                Some("engine"),
            ),
        ];
        let usages = [(1000, 100, 0), (1000, 100, 400), (1000, 0, 1000), (0, 50, 0), (7, 3, 7)];
        for (i, (input, output)) in tariffs.into_iter().enumerate() {
            for (j, cache) in caches.into_iter().enumerate() {
                for (prompt, completion, cached) in usages {
                    let charged = charged_cost(
                        &counts(prompt, completion, cached, 0, 0, 0),
                        Some("m"),
                        input,
                        output,
                        cache,
                        ANALYTICS_BATCHER,
                    )
                    .unwrap_or(Decimal::ZERO);
                    assert_eq!(
                        priced(UnitPrices::new(input, output, cache), prompt, completion, cached),
                        charged,
                        "tariff {i}, cache {j}, usage {prompt}/{completion}/{cached}"
                    );
                }
            }
        }
    }

    fn query(model: &str) -> PriceQuery<'_> {
        PriceQuery {
            model,
            completion_window: None,
            batch_created_at: None,
            now: Utc::now(),
            serving_class: "standard",
            cache_reads: None,
        }
    }

    fn price(input: &str, cached: &str, output: &str) -> UnitPrices {
        UnitPrices {
            prompt: Decimal::from_str(input).unwrap(),
            cached: Decimal::from_str(cached).unwrap(),
            output: Decimal::from_str(output).unwrap(),
        }
    }

    #[test]
    fn unit_prices_follow_the_tariff_billing_selects() {
        let now = Utc::now();
        let hours = chrono::Duration::hours;
        let deal_account = uuid::Uuid::new_v4();
        let account = uuid::Uuid::new_v4();
        let mut deal = make_tariff(ApiKeyPurpose::Realtime, now - hours(2), None, "0.000002", "0.000006", None);
        deal.account = Some(deal_account);
        let book = PriceBook::from_rows(
            [account, deal_account],
            [
                (
                    "m".to_string(),
                    vec![
                        make_tariff(
                            ApiKeyPurpose::Realtime,
                            now - hours(48),
                            Some(now - hours(2)),
                            "0.000005",
                            "0.000015",
                            None,
                        ),
                        make_tariff(ApiKeyPurpose::Realtime, now - hours(2), None, "0.000003", "0.000009", None),
                        make_tariff(ApiKeyPurpose::Batch, now - hours(2), None, "0.000001", "0.000003", Some("24h")),
                        deal,
                    ],
                ),
                ("unpriced".to_string(), vec![]),
                (
                    "zero".to_string(),
                    vec![make_tariff(ApiKeyPurpose::Realtime, now - hours(2), None, "0", "0", None)],
                ),
            ],
            [],
        );

        let realtime = Some(price("0.000003", "0.000003", "0.000009"));
        assert_eq!(book.unit_prices(account, &ApiKeyPurpose::Realtime, &query("m")), realtime);
        assert_eq!(
            book.unit_prices(account, &ApiKeyPurpose::Playground, &query("m")),
            realtime,
            "playground falls back to realtime"
        );
        assert_eq!(
            book.unit_prices(deal_account, &ApiKeyPurpose::Realtime, &query("m")),
            Some(price("0.000002", "0.000002", "0.000006")),
            "the account's own price wins"
        );

        let mut batch = query("m");
        batch.completion_window = Some("24h");
        assert_eq!(
            book.unit_prices(account, &ApiKeyPurpose::Batch, &batch),
            Some(price("0.000001", "0.000001", "0.000003"))
        );
        batch.completion_window = Some("1h");
        assert_eq!(
            book.unit_prices(account, &ApiKeyPurpose::Batch, &batch),
            realtime,
            "an SLA without a batch price falls back to realtime"
        );
        batch.batch_created_at = Some(now - hours(3));
        assert_eq!(
            book.unit_prices(account, &ApiKeyPurpose::Batch, &batch),
            Some(price("0.000005", "0.000005", "0.000015")),
            "a batch request is priced as of the batch's creation"
        );

        for (model, account, purpose) in [
            ("unpriced", account, ApiKeyPurpose::Realtime),
            ("zero", account, ApiKeyPurpose::Realtime),
            ("m", account, ApiKeyPurpose::Continuation),
            ("m", uuid::Uuid::nil(), ApiKeyPurpose::Realtime),
        ] {
            assert_eq!(
                book.unit_prices(account, &purpose, &query(model)),
                Some(UnitPrices::FREE),
                "{model} {account} {purpose:?}"
            );
        }

        assert_eq!(book.unit_prices(uuid::Uuid::new_v4(), &ApiKeyPurpose::Realtime, &query("m")), None);
        assert_eq!(book.unit_prices(account, &ApiKeyPurpose::Realtime, &query("not-deployed")), None);
    }

    #[test]
    fn cached_price_follows_how_the_cache_layer_reports_reads() {
        let now = Utc::now();
        let hours = chrono::Duration::hours;
        let tariff = || {
            vec![make_tariff(
                ApiKeyPurpose::Realtime,
                now - hours(2),
                None,
                "0.00001",
                "0.00003",
                None,
            )]
        };
        let mut surcharge = tariff_row(Decimal::TWO, 2, None);
        surcharge.read_multiplier = Decimal::new(15, 1);
        let account = uuid::Uuid::new_v4();
        let book = PriceBook::from_rows(
            [account],
            [
                ("cached".to_string(), tariff()),
                ("surcharge".to_string(), tariff()),
                ("expired".to_string(), tariff()),
                ("uncached".to_string(), tariff()),
            ],
            [
                ("cached".to_string(), vec![tariff_row(Decimal::TWO, 2, None)]),
                ("surcharge".to_string(), vec![surcharge]),
                ("expired".to_string(), vec![tariff_row(Decimal::TWO, 4, Some(now - hours(1)))]),
            ],
        );
        let cached = |model: &str, reads, created_at| {
            let query = PriceQuery {
                cache_reads: reads,
                batch_created_at: created_at,
                ..query(model)
            };
            book.unit_prices(account, &ApiKeyPurpose::Realtime, &query).unwrap().cached
        };
        let engine = Some(crate::prompt_cache::CacheReadSource::Engine);
        let module = Some(crate::prompt_cache::CacheReadSource::Module);
        let input = Decimal::from_str("0.00001").unwrap();

        assert_eq!(cached("cached", engine, None), Decimal::from_str("0.000001").unwrap());
        assert_eq!(cached("cached", module, None), Decimal::from_str("0.000001").unwrap());
        assert_eq!(
            cached("cached", None, None),
            input,
            "reads the cache layer does not report are billed as input"
        );
        assert_eq!(
            cached("surcharge", engine, None),
            input,
            "an engine read never costs more than input"
        );
        assert_eq!(cached("surcharge", module, None), Decimal::from_str("0.000015").unwrap());
        assert_eq!(cached("uncached", engine, None), input);
        assert_eq!(cached("expired", engine, Some(now - hours(2))), input);
    }
}

#[cfg(test)]
mod class_tests;
