
use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::{ArcSwap, ArcSwapOption};
use sqlx::{PgPool, postgres::PgListener};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::config::ONWARDS_CONFIG_CHANGED_CHANNEL;
#[cfg(test)]
use crate::crypto::hash_api_key;
use crate::db::models::api_keys::ApiKeyPurpose;
use crate::metrics::errors::component;
use crate::modalities::ModalitySet;
use crate::pricing::{PriceBook, PriceQuery, UnitPrices};
use crate::scheduling::TolerationPresets;

type KeyHash = crate::crypto::ApiKeyHash;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyPolicy {
    pub zdr: bool,
    pub disabled_modalities: ModalitySet,
    pub toleration_preset: Option<Arc<str>>,
    pub model_toleration_presets: Option<Arc<HashMap<String, Arc<str>>>>,
    pub billing: Option<KeyBilling>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyBilling {
    pub account: crate::types::UserId,
    pub purpose: ApiKeyPurpose,
}

impl KeyPolicy {
    pub fn toleration_preset_for(&self, model: &str) -> Option<&str> {
        let presets = self.model_toleration_presets.as_ref();
        presets
            .and_then(|presets| presets.get(model))
            .or_else(|| {
                let (alias, _class) = model.rsplit_once(':')?;
                presets.and_then(|presets| presets.get(&model_wide_key(alias)))
            })
            .or(self.toleration_preset.as_ref())
            .map(|preset| &**preset)
    }
}

#[derive(Clone, Default)]
pub struct AccountTolerations {
    policy: KeyPolicy,
    presets: TolerationPresets,
}

impl AccountTolerations {
    pub fn for_model(&self, model: &str) -> Option<Arc<serde_json::Value>> {
        self.presets.resolve(self.policy.toleration_preset_for(model))
    }
}

#[derive(Clone)]
pub struct KeyPolicyCache {
    inner: Arc<ArcSwap<HashMap<KeyHash, KeyPolicy>>>,
    presets: TolerationPresets,
    prices: Arc<ArcSwapOption<PriceBook>>,
}

impl KeyPolicyCache {
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            presets: TolerationPresets::default(),
            prices: Arc::new(ArcSwapOption::empty()),
        }
    }

    pub fn with_presets(presets: TolerationPresets) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            presets,
            prices: Arc::new(ArcSwapOption::empty()),
        }
    }

    pub fn policy(&self, key: &KeyHash) -> KeyPolicy {
        self.inner.load().get(key).cloned().unwrap_or_default()
    }

    pub fn policy_if_known(&self, key: &KeyHash) -> Option<KeyPolicy> {
        self.inner.load().get(key).cloned()
    }

    pub fn is_zdr(&self, key: &KeyHash) -> bool {
        self.policy(key).zdr
    }

    pub fn disabled_modalities(&self, key: &KeyHash) -> ModalitySet {
        self.policy(key).disabled_modalities
    }

    pub fn tolerations(&self, key: &KeyHash, model: &str) -> Option<Arc<serde_json::Value>> {
        let map = self.inner.load();
        self.presets
            .resolve(map.get(key).and_then(|policy| policy.toleration_preset_for(model)))
    }

    pub fn account_tolerations_if_known(&self, key: &KeyHash) -> Option<AccountTolerations> {
        self.policy_if_known(key).map(|policy| self.account_tolerations(policy))
    }

    pub fn account_tolerations(&self, policy: KeyPolicy) -> AccountTolerations {
        AccountTolerations {
            policy,
            presets: self.presets.clone(),
        }
    }

    pub fn presets(&self) -> &TolerationPresets {
        &self.presets
    }

    pub fn replace_presets(&self, presets: TolerationPresets) {
        self.presets.store(presets);
    }

    pub(crate) fn unit_prices(&self, key: &KeyHash, query: &PriceQuery<'_>) -> Option<UnitPrices> {
        let map = self.inner.load();
        let billing = map.get(key)?.billing.as_ref()?;
        let prices = self.prices.load();
        prices.as_ref()?.unit_prices(billing.account, &billing.purpose, query)
    }

    fn replace(&self, map: HashMap<KeyHash, KeyPolicy>) {
        self.inner.store(Arc::new(map));
    }

    #[cfg(test)]
    pub(crate) fn replace_prices(&self, prices: PriceBook) {
        self.prices.store(Some(Arc::new(prices)));
    }

    #[cfg(test)]
    pub fn from_pairs<I: IntoIterator<Item = (String, bool)>>(pairs: I) -> Self {
        let cache = Self::empty();
        cache.replace(
            pairs
                .into_iter()
                .map(|(secret, zdr)| (hash_api_key(&secret), KeyPolicy { zdr, ..Default::default() }))
                .collect(),
        );
        cache
    }

    #[cfg(test)]
    pub fn from_policies<I: IntoIterator<Item = (String, KeyPolicy)>>(pairs: I) -> Self {
        let cache = Self::empty();
        cache.replace(pairs.into_iter().map(|(secret, policy)| (hash_api_key(&secret), policy)).collect());
        cache
    }

    #[cfg(test)]
    pub fn from_policies_with_presets<I: IntoIterator<Item = (String, KeyPolicy)>>(presets: TolerationPresets, pairs: I) -> Self {
        let cache = Self::with_presets(presets);
        cache.replace(pairs.into_iter().map(|(secret, policy)| (hash_api_key(&secret), policy)).collect());
        cache
    }
}

async fn load(pool: &PgPool) -> Result<HashMap<KeyHash, KeyPolicy>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT ak.secret_hash, ak.user_id, ak.purpose, u.zero_data_retention, u.disabled_modalities, u.toleration_preset
        FROM api_keys ak
        JOIN users u ON u.id = ak.user_id
        WHERE NOT ak.is_deleted
        "#
    )
    .fetch_all(pool)
    .await?;
    let model_presets = load_model_presets(pool, None).await?;

    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let hash = KeyHash::try_from(r.secret_hash.as_slice()).ok()?;
            Some((
                hash,
                KeyPolicy {
                    zdr: r.zero_data_retention,
                    disabled_modalities: ModalitySet::from_db(&r.disabled_modalities),
                    toleration_preset: r.toleration_preset.map(Arc::from),
                    model_toleration_presets: model_presets.get(&r.user_id).cloned(),
                    billing: Some(KeyBilling {
                        account: r.user_id,
                        purpose: crate::pricing::parse_api_key_purpose(&r.purpose),
                    }),
                },
            ))
        })
        .collect())
}

fn model_wide_key(alias: &str) -> String {
    format!("{alias}\0")
}

async fn load_model_presets<'c>(
    conn: impl sqlx::PgExecutor<'c>,
    account: Option<crate::types::UserId>,
) -> Result<HashMap<crate::types::UserId, Arc<HashMap<String, Arc<str>>>>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT p.user_id, dm.alias, p.serving_class, p.toleration_preset AS "toleration_preset!"
        FROM account_model_policies p
        JOIN deployed_models dm ON dm.id = p.deployed_model_id
        WHERE p.toleration_preset IS NOT NULL
          AND dm.deleted = FALSE
          AND ($1::uuid IS NULL OR p.user_id = $1)
        ORDER BY p.serving_class NULLS FIRST
        "#,
        account
    )
    .fetch_all(conn)
    .await?;

    let mut presets: HashMap<crate::types::UserId, HashMap<String, Arc<str>>> = HashMap::new();
    for row in rows {
        let preset: Arc<str> = Arc::from(row.toleration_preset);
        let account = presets.entry(row.user_id).or_default();
        match row.serving_class.as_deref() {
            None => {
                account.insert(model_wide_key(&row.alias), preset.clone());
                account.insert(row.alias, preset);
            }
            Some("standard") => {
                account.insert(row.alias, preset);
            }
            Some(class) => {
                account.insert(format!("{}:{class}", row.alias), preset);
            }
        }
    }
    Ok(presets.into_iter().map(|(account, presets)| (account, Arc::new(presets))).collect())
}

pub async fn refresh(pool: &PgPool, cache: &KeyPolicyCache) -> Result<usize, sqlx::Error> {
    let map = load(pool).await?;
    let accounts: Vec<crate::types::UserId> = map
        .values()
        .filter_map(|policy| policy.billing.as_ref().map(|billing| billing.account))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    let prices = PriceBook::load(pool, &accounts).await;
    let n = map.len();
    cache.replace(map);
    match prices {
        Ok(prices) => cache.prices.store(Some(Arc::new(prices))),
        Err(e) => {
            crate::background_error!(component::ZDR_KEY_SYNC, "load_prices", Error, error = %e, "key policy sync: failed to load prices; keeping the previous prices");
        }
    }
    Ok(n)
}

pub async fn load_policy(conn: &mut sqlx::PgConnection, user_id: crate::types::UserId) -> Result<KeyPolicy, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT zero_data_retention, disabled_modalities, toleration_preset
        FROM users
        WHERE id = $1
        "#,
        user_id
    )
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        return Ok(KeyPolicy::default());
    };

    let model_toleration_presets = load_model_presets(&mut *conn, Some(user_id)).await?.remove(&user_id);
    Ok(KeyPolicy {
        zdr: row.zero_data_retention,
        disabled_modalities: ModalitySet::from_db(&row.disabled_modalities),
        toleration_preset: row.toleration_preset.map(Arc::from),
        model_toleration_presets,
        billing: None,
    })
}

pub async fn initial_cache(pool: &PgPool, presets: TolerationPresets) -> Result<KeyPolicyCache, sqlx::Error> {
    let cache = KeyPolicyCache::with_presets(presets);
    refresh(pool, &cache).await?;
    Ok(cache)
}

pub async fn run(
    pools: impl sqlx_pool_router::PoolProvider,
    listener_pools: impl sqlx_pool_router::PoolProvider,
    cache: KeyPolicyCache,
    fallback_interval_ms: u64,
    shutdown: CancellationToken,
) -> Result<(), anyhow::Error> {
    let pools = sqlx_pool_router::DynPools::new(pools);
    let listener_pools = sqlx_pool_router::DynPools::new(listener_pools);
    const MIN_RELOAD_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
    let fallback = (fallback_interval_ms > 0).then(|| std::time::Duration::from_millis(fallback_interval_ms));

    'outer: loop {
        let mut listener = PgListener::connect_with(&listener_pools.write()).await?;
        listener.listen(ONWARDS_CONFIG_CHANGED_CHANNEL).await?;
        info!("Started key policy sync listener");

        let mut last_reload = std::time::Instant::now();
        let mut fallback_timer = fallback.map(|iv| {
            let mut t = tokio::time::interval(iv);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            t
        });

        loop {
            let tick = async {
                match fallback_timer.as_mut() {
                    Some(t) => t.tick().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = shutdown.cancelled() => break 'outer,
                notif = listener.try_recv() => match notif {
                    Ok(Some(_)) => {
                        if last_reload.elapsed() < MIN_RELOAD_INTERVAL { continue; }
                        last_reload = std::time::Instant::now();
                        reload(&pools.write(), &cache).await;
                    }
                    Ok(None) => {
                        debug!("key policy sync: connection lost, reconnecting");
                        break;
                    }
                    Err(e) => {
                        crate::background_error!(component::ZDR_KEY_SYNC, "listen", Error, error = %e, "key policy sync: listener error");
                        break;
                    }
                },
                _ = tick => {
                    if last_reload.elapsed() < MIN_RELOAD_INTERVAL { continue; }
                    last_reload = std::time::Instant::now();
                    reload(&pools.write(), &cache).await;
                }
            }
        }
    }
    Ok(())
}

async fn reload(pool: &PgPool, cache: &KeyPolicyCache) {
    match refresh(pool, cache).await {
        Ok(keys) => debug!(keys, "key policy sync: reloaded map"),
        Err(e) => {
            crate::background_error!(component::ZDR_KEY_SYNC, "load", Error, error = %e, "key policy sync: failed to reload map");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_cache_reads_every_key_as_non_zdr() {
        let cache = KeyPolicyCache::empty();
        assert!(!cache.is_zdr(&hash_api_key("sk-anything")));
    }

    #[test]
    fn is_zdr_reflects_the_maps_flag() {
        let cache = KeyPolicyCache::from_pairs([("sk-on".to_string(), true), ("sk-off".to_string(), false)]);
        assert!(cache.is_zdr(&hash_api_key("sk-on")));
        assert!(!cache.is_zdr(&hash_api_key("sk-off")));
        assert!(!cache.is_zdr(&hash_api_key("sk-missing")));
    }

    #[test]
    fn clones_share_one_map_so_a_replace_is_visible_to_all_handles() {
        let cache = KeyPolicyCache::empty();
        let handle = cache.clone();
        assert!(!handle.is_zdr(&hash_api_key("sk-on")));
        cache.replace(
            [(
                hash_api_key("sk-on"),
                KeyPolicy {
                    zdr: true,
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
        );
        assert!(handle.is_zdr(&hash_api_key("sk-on")));
    }

    #[test]
    fn disabled_modalities_default_to_empty_and_follow_the_policy() {
        use crate::modalities::Modality;
        let cache = KeyPolicyCache::from_policies([(
            "sk-no-batch".to_string(),
            KeyPolicy {
                zdr: false,
                disabled_modalities: [Modality::Batch].into_iter().collect(),
                ..Default::default()
            },
        )]);
        assert!(cache.disabled_modalities(&hash_api_key("sk-no-batch")).contains(Modality::Batch));
        assert!(!cache.disabled_modalities(&hash_api_key("sk-no-batch")).contains(Modality::Realtime));
        assert!(cache.disabled_modalities(&hash_api_key("sk-missing")).is_empty());
    }

    #[test]
    fn tolerations_resolve_the_preset_id_and_default_to_none() {
        use crate::config::{SchedulingConfig, TolerationPreset};
        use crate::scheduling::{SchedulingOperator, SchedulingToleration};

        let presets = TolerationPresets::new(&SchedulingConfig {
            toleration_presets: vec![
                SchedulingConfig::default_preset(),
                TolerationPreset {
                    id: "gpu".to_string(),
                    label: "GPU".to_string(),
                    description: "GPU capacity only.".to_string(),
                    tolerations: vec![SchedulingToleration {
                        key: "pool".to_string(),
                        operator: Some(SchedulingOperator::Equal),
                        value: Some("dedicated".to_string()),
                        effect: None,
                    }],
                },
            ],
        });
        let cache = KeyPolicyCache::from_policies_with_presets(
            presets,
            [
                (
                    "sk-preset".to_string(),
                    KeyPolicy {
                        toleration_preset: Some("gpu".into()),
                        ..Default::default()
                    },
                ),
                (
                    "sk-empty".to_string(),
                    KeyPolicy {
                        toleration_preset: Some("no-overflow".into()),
                        ..Default::default()
                    },
                ),
                (
                    "sk-default".to_string(),
                    KeyPolicy {
                        toleration_preset: None,
                        ..Default::default()
                    },
                ),
            ],
        );
        assert_eq!(
            cache.tolerations(&hash_api_key("sk-preset"), "any/model").as_deref(),
            Some(&serde_json::json!([{"key": "pool", "operator": "Equal", "value": "dedicated"}]))
        );
        assert_eq!(
            cache.tolerations(&hash_api_key("sk-empty"), "any/model").as_deref(),
            Some(&serde_json::json!([]))
        );
        assert!(cache.tolerations(&hash_api_key("sk-default"), "any/model").is_none());
        assert!(cache.tolerations(&hash_api_key("sk-missing"), "any/model").is_none());
        assert!(
            cache
                .account_tolerations_if_known(&hash_api_key("sk-default"))
                .unwrap()
                .for_model("any/model")
                .is_none()
        );
        assert!(cache.account_tolerations_if_known(&hash_api_key("sk-missing")).is_none());
        assert_eq!(
            cache
                .account_tolerations_if_known(&hash_api_key("sk-preset"))
                .unwrap()
                .for_model("any/model")
                .as_deref(),
            Some(&serde_json::json!([{"key": "pool", "operator": "Equal", "value": "dedicated"}]))
        );
    }

    #[test]
    fn a_model_preset_overrides_the_account_preset_for_that_model_only() {
        use crate::config::{SchedulingConfig, TolerationPreset};
        use crate::scheduling::{SchedulingOperator, SchedulingToleration};

        let presets = TolerationPresets::new(&SchedulingConfig {
            toleration_presets: vec![
                SchedulingConfig::default_preset(),
                TolerationPreset {
                    id: "gpu".to_string(),
                    label: "GPU".to_string(),
                    description: "GPU capacity only.".to_string(),
                    tolerations: vec![SchedulingToleration {
                        key: "pool".to_string(),
                        operator: Some(SchedulingOperator::Equal),
                        value: Some("dedicated".to_string()),
                        effect: None,
                    }],
                },
            ],
        });
        let models = Arc::new(HashMap::from([("org/pinned".to_string(), Arc::<str>::from("no-overflow"))]));
        let cache = KeyPolicyCache::from_policies_with_presets(
            presets,
            [
                (
                    "sk-both".to_string(),
                    KeyPolicy {
                        toleration_preset: Some("gpu".into()),
                        model_toleration_presets: Some(models.clone()),
                        ..Default::default()
                    },
                ),
                (
                    "sk-model-only".to_string(),
                    KeyPolicy {
                        model_toleration_presets: Some(models),
                        ..Default::default()
                    },
                ),
            ],
        );
        let gpu = serde_json::json!([{"key": "pool", "operator": "Equal", "value": "dedicated"}]);
        assert_eq!(
            cache.tolerations(&hash_api_key("sk-both"), "org/pinned").as_deref(),
            Some(&serde_json::json!([]))
        );
        assert_eq!(cache.tolerations(&hash_api_key("sk-both"), "org/other").as_deref(), Some(&gpu));
        assert_eq!(
            cache.tolerations(&hash_api_key("sk-model-only"), "org/pinned").as_deref(),
            Some(&serde_json::json!([]))
        );
        assert!(cache.tolerations(&hash_api_key("sk-model-only"), "org/other").is_none());
        let account = cache.account_tolerations_if_known(&hash_api_key("sk-both")).unwrap();
        assert_eq!(account.for_model("org/pinned").as_deref(), Some(&serde_json::json!([])));
        assert_eq!(account.for_model("org/other").as_deref(), Some(&gpu));
    }

    #[test]
    fn an_unconfigured_preset_fails_closed_to_the_empty_list() {
        let cache = KeyPolicyCache::from_policies_with_presets(
            TolerationPresets::default(),
            [(
                "sk-retired".to_string(),
                KeyPolicy {
                    toleration_preset: Some("retired".into()),
                    ..Default::default()
                },
            )],
        );
        assert_eq!(
            cache.tolerations(&hash_api_key("sk-retired"), "any/model").as_deref(),
            Some(&serde_json::json!([]))
        );
    }

    #[test]
    fn class_presets_fall_back_to_the_model_wide_preset_not_the_standard_one() {
        let policy = |entries: &[(&str, &str)]| KeyPolicy {
            toleration_preset: Some("account".into()),
            model_toleration_presets: Some(Arc::new(
                entries.iter().map(|(k, v)| (k.to_string(), Arc::<str>::from(*v))).collect(),
            )),
            ..Default::default()
        };
        let wide = model_wide_key("m");
        let p = policy(&[(wide.as_str(), "wide"), ("m", "wide"), ("m:fast", "fast-only")]);
        assert_eq!(p.toleration_preset_for("m"), Some("wide"));
        assert_eq!(p.toleration_preset_for("m:fast"), Some("fast-only"));
        assert_eq!(
            p.toleration_preset_for("m:other"),
            Some("wide"),
            "a model-wide preset covers every class"
        );
        assert_eq!(p.toleration_preset_for("n:fast"), Some("account"));
        let p = policy(&[("m", "standard-only")]);
        assert_eq!(p.toleration_preset_for("m"), Some("standard-only"));
        assert_eq!(p.toleration_preset_for("m:fast"), Some("account"));
    }

    async fn deploy(pool: &sqlx::PgPool, by: crate::types::UserId, alias: &str) -> uuid::Uuid {
        sqlx::query_scalar(
            "WITH e AS (INSERT INTO inference_endpoints (name,url,created_by) VALUES ($1,'http://localhost:1',$2) RETURNING id)
             INSERT INTO deployed_models (model_name,alias,hosted_on,created_by,type) SELECT $1,$1,e.id,$2,'CHAT' FROM e RETURNING id",
        )
        .bind(alias)
        .bind(by)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn add_tariff(pool: &sqlx::PgPool, model: uuid::Uuid, account: Option<crate::types::UserId>, input: i64, output: i64) {
        sqlx::query(
            "INSERT INTO model_tariffs (deployed_model_id,user_id,name,input_price_per_token,output_price_per_token,api_key_purpose,valid_from) VALUES ($1,$2,'test',$3,$4,'realtime',$5)",
        )
        .bind(model)
        .bind(account)
        .bind(rust_decimal::Decimal::new(input, 6))
        .bind(rust_decimal::Decimal::new(output, 6))
        .bind(chrono::Utc::now() - chrono::Duration::minutes(1))
        .execute(pool)
        .await
        .unwrap();
    }

    #[dwctl_test_macros::test]
    async fn a_failed_price_load_still_refreshes_the_map_and_keeps_the_old_prices(pool: sqlx::PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::{create_test_api_key_for_user, create_test_user};
        use rust_decimal::Decimal;

        let user = create_test_user(&pool, Role::StandardUser).await;
        let key = create_test_api_key_for_user(&pool, user.id).await.secret;
        let model = deploy(&pool, user.id, "priced-model").await;
        add_tariff(&pool, model, None, 1, 3).await;
        let query = |model: &'static str| PriceQuery {
            model,
            completion_window: None,
            batch_created_at: None,
            now: chrono::Utc::now(),
            serving_class: "standard",
            cache_reads: None,
        };
        let priced = Some(UnitPrices {
            prompt: Decimal::new(1, 6),
            cached: Decimal::new(1, 6),
            output: Decimal::new(3, 6),
        });

        let cache = initial_cache(&pool, TolerationPresets::default()).await.unwrap();
        assert_eq!(cache.unit_prices(&hash_api_key(&key), &query("priced-model")), priced);
        assert!(!cache.is_zdr(&hash_api_key(&key)));

        sqlx::query("UPDATE users SET zero_data_retention = true WHERE id = $1")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();
        let deal = create_test_user(&pool, Role::StandardUser).await;
        let deal_key = create_test_api_key_for_user(&pool, deal.id).await.secret;
        add_tariff(&pool, model, Some(deal.id), 2, 6).await;
        let later = deploy(&pool, user.id, "later-model").await;
        add_tariff(&pool, later, None, 5, 15).await;
        sqlx::query("ALTER TABLE model_tariffs RENAME TO model_tariffs_unavailable")
            .execute(&pool)
            .await
            .unwrap();

        refresh(&pool, &cache).await.expect("a price load failure is not a refresh failure");
        assert!(cache.is_zdr(&hash_api_key(&key)), "the policy map was replaced");
        assert_eq!(
            cache.unit_prices(&hash_api_key(&key), &query("priced-model")),
            priced,
            "the previous prices are kept"
        );
        assert!(
            cache.policy_if_known(&hash_api_key(&deal_key)).is_some(),
            "the new account's key is in the map"
        );
        assert_eq!(
            cache.unit_prices(&hash_api_key(&deal_key), &query("priced-model")),
            None,
            "the kept prices hold nothing of the new account's own, so its key is not priced"
        );
        assert_eq!(
            cache.unit_prices(&hash_api_key(&key), &query("later-model")),
            None,
            "the kept prices predate the model, so it is not priced"
        );

        let booted = initial_cache(&pool, TolerationPresets::default())
            .await
            .expect("a price load failure does not fail startup");
        assert!(booted.is_zdr(&hash_api_key(&key)));
        assert_eq!(
            booted.unit_prices(&hash_api_key(&key), &query("priced-model")),
            None,
            "no prices have loaded, so none are sent"
        );
    }
}
