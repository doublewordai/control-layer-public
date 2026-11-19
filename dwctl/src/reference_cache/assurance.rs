
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arc_swap::ArcSwap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::metrics::errors::component::REFERENCE_CACHE;
use crate::types::UserId;

const REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const REFRESH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "AssuranceTerms", into = "AssuranceTerms")]
pub enum CacheAssurance {
    GapShare(f64),
    PromptFloor(f64),
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AssuranceTerms {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gap_share: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_floor: Option<f64>,
}

impl TryFrom<AssuranceTerms> for CacheAssurance {
    type Error = String;

    fn try_from(t: AssuranceTerms) -> Result<Self, String> {
        let terms = match (t.gap_share, t.prompt_floor) {
            (Some(alpha), None) => Self::GapShare(alpha),
            (None, Some(share)) => Self::PromptFloor(share),
            _ => return Err("cache_assurance takes exactly one of gap_share or prompt_floor".to_string()),
        };
        terms.validate("cache_assurance").map_err(|e| e.to_string())?;
        Ok(terms)
    }
}

impl From<CacheAssurance> for AssuranceTerms {
    fn from(c: CacheAssurance) -> Self {
        match c {
            CacheAssurance::GapShare(alpha) => Self {
                gap_share: Some(alpha),
                ..Self::default()
            },
            CacheAssurance::PromptFloor(share) => Self {
                prompt_floor: Some(share),
                ..Self::default()
            },
        }
    }
}

impl CacheAssurance {
    pub fn validate(&self, context: &str) -> anyhow::Result<()> {
        let (name, share) = match self {
            Self::GapShare(s) => ("gap_share", *s),
            Self::PromptFloor(s) => ("prompt_floor", *s),
        };
        anyhow::ensure!(
            share.is_finite() && (0.0..=1.0).contains(&share),
            "{context}: cache_assurance.{name} must be between 0 and 1, got {share}"
        );
        Ok(())
    }

    pub fn billed_read(&self, engine: u64, reference: u64, prompt_tokens: u64) -> u64 {
        let engine = engine.min(prompt_tokens);
        let reference = reference.min(prompt_tokens);
        let assured = match *self {
            Self::GapShare(alpha) => engine.saturating_add(share_of(alpha, reference.saturating_sub(engine))),
            Self::PromptFloor(share) => reference.min(share_of(share, prompt_tokens)),
        };
        [engine, assured].into_iter().max().unwrap_or(0).min(prompt_tokens)
    }
}

fn share_of(share: f64, tokens: u64) -> u64 {
    if !share.is_finite() || share <= 0.0 {
        return 0;
    }
    let written = format!("{share:e}");
    let Some((mantissa, exponent)) = written.split_once('e') else {
        return 0;
    };
    let (Ok(exponent), Ok(digits)) = (exponent.parse::<i32>(), mantissa.replace('.', "").parse::<u128>()) else {
        return 0;
    };
    let decimals = mantissa.split_once('.').map_or(0, |(_, frac)| frac.len() as i32);
    let shift = exponent - decimals;
    let Some(product) = digits.checked_mul(u128::from(tokens)) else {
        return u64::MAX;
    };
    let floored = if shift >= 0 {
        10u128
            .checked_pow(shift as u32)
            .and_then(|scale| product.checked_mul(scale))
            .unwrap_or(u128::MAX)
    } else {
        10u128.checked_pow(shift.unsigned_abs()).map_or(0, |scale| product / scale)
    };
    u64::try_from(floored).unwrap_or(u64::MAX)
}

#[derive(Debug, Default, PartialEq)]
struct Terms {
    by_pair: HashMap<(UserId, String), CacheAssurance>,
    models: HashSet<String>,
}

impl Terms {
    fn new(by_pair: HashMap<(UserId, String), CacheAssurance>) -> Self {
        let models = by_pair.keys().map(|(_, model)| model.clone()).collect();
        Self { by_pair, models }
    }
}

pub struct AssuranceBook {
    terms: ArcSwap<Terms>,
    pool: Option<sqlx_pool_router::DynPools>,
}

impl std::fmt::Debug for AssuranceBook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssuranceBook").field("terms", &self.len()).finish()
    }
}

impl AssuranceBook {
    pub fn new(pool: sqlx_pool_router::DynPools) -> Self {
        Self {
            terms: ArcSwap::from_pointee(Terms::default()),
            pool: Some(pool),
        }
    }

    pub fn fixed(terms: HashMap<(UserId, String), CacheAssurance>) -> Self {
        Self {
            terms: ArcSwap::from_pointee(Terms::new(terms)),
            pool: None,
        }
    }

    pub fn terms(&self, principal: UserId, model: &str) -> Option<CacheAssurance> {
        self.terms.load().by_pair.get(&(principal, model.to_string())).copied()
    }

    pub fn names_model(&self, model: &str) -> bool {
        self.terms.load().models.contains(model)
    }

    pub fn len(&self) -> usize {
        self.terms.load().by_pair.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub async fn refresh(&self) -> anyhow::Result<()> {
        let Some(pool) = &self.pool else {
            return Ok(());
        };
        let handle = pool.write();
        let query = sqlx::query!(
            r#"
            SELECT mo.user_id, dm.alias, mo.cache_assurance AS "cache_assurance!"
            FROM model_overlays mo
            JOIN deployed_models dm ON dm.id = mo.deployed_model_id
            WHERE mo.cache_assurance IS NOT NULL AND dm.deleted = false
            "#
        )
        .fetch_all(&handle);
        let rows = tokio::time::timeout(REFRESH_TIMEOUT, query)
            .await
            .context("loading cache-assurance terms timed out")?
            .context("loading cache-assurance terms")?;
        let mut terms = HashMap::with_capacity(rows.len());
        for row in rows {
            match serde_json::from_value::<CacheAssurance>(row.cache_assurance) {
                Ok(t) => {
                    terms.insert((row.user_id, row.alias), t);
                }
                Err(e) => {
                    crate::background_error!(REFERENCE_CACHE, "assurance_terms", Error, error = %e, alias = %row.alias, "unreadable cache_assurance terms; the pair bills from the engine");
                }
            }
        }
        let terms = Terms::new(terms);
        let changed = *self.terms.load_full() != terms;
        self.terms.store(Arc::new(terms));
        if changed {
            info!(pairs = self.len(), "Cache-assurance terms loaded");
        }
        Ok(())
    }

    pub fn spawn_reloads(self: &Arc<Self>) {
        if self.pool.is_none() {
            return;
        }
        let book = Arc::downgrade(self);
        tokio::spawn(async move {
            let start = tokio::time::Instant::now() + REFRESH_INTERVAL;
            let mut ticks = tokio::time::interval_at(start, REFRESH_INTERVAL);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticks.tick().await;
                let Some(book) = book.upgrade() else {
                    return;
                };
                if let Err(e) = book.refresh().await {
                    crate::background_error!(REFERENCE_CACHE, "assurance_refresh", Warning, error = %e, "reloading cache-assurance terms failed; keeping the previous ones");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gap_share_bills_the_engine_plus_a_share_of_the_gap() {
        let alpha = CacheAssurance::GapShare(0.4);
        assert_eq!(alpha.billed_read(7_000, 8_000, 10_000), 7_400);
        assert_eq!(alpha.billed_read(0, 3, 10), 1);
        assert_eq!(CacheAssurance::GapShare(0.29).billed_read(0, 100, 1_000), 29);
        assert_eq!(CacheAssurance::PromptFloor(0.29).billed_read(0, 1_000, 100), 29);
        assert_eq!(share_of(0.9999999999999999, 1_000_000_000_000_000_001), 999_999_999_999_999_900);
        assert_eq!(share_of(1.0, u64::MAX), u64::MAX);
        assert_eq!(share_of(0.0, u64::MAX), 0);
        assert_eq!(share_of(1e-20, 10u64.pow(19)), 0, "exponent notation parses too");
        assert_eq!(share_of(0.00000000000000006783115020737882, 14_742_489_209_496_226), 0);
        assert_eq!(share_of(0.5, 3), 1);
        assert_eq!(share_of(f64::MIN_POSITIVE, u64::MAX), 0);
        assert_eq!(alpha.billed_read(7_000, 5_000, 10_000), 7_000);
        assert_eq!(CacheAssurance::GapShare(0.0).billed_read(7_000, 8_000, 10_000), 7_000);
        assert_eq!(CacheAssurance::GapShare(1.0).billed_read(7_000, 8_000, 10_000), 8_000);
    }

    #[test]
    fn prompt_floor_bills_the_reference_up_to_a_share_of_the_prompt() {
        let floor = CacheAssurance::PromptFloor(0.9);
        assert_eq!(floor.billed_read(2_000, 8_000, 10_000), 8_000, "the reference, under the ceiling");
        assert_eq!(floor.billed_read(2_000, 9_800, 10_000), 9_000, "capped at 90% of the prompt");
        assert_eq!(floor.billed_read(9_500, 9_800, 10_000), 9_500, "never below the engine");
        assert_eq!(floor.billed_read(2_000, 0, 10_000), 2_000, "no reference hit, engine only");
    }

    #[test]
    fn the_billed_read_never_exceeds_the_prompt() {
        for terms in [CacheAssurance::GapShare(1.0), CacheAssurance::PromptFloor(1.0)] {
            assert_eq!(terms.billed_read(20_000, 30_000, 10_000), 10_000);
            assert_eq!(terms.billed_read(0, 30_000, 10_000), 10_000);
        }
    }

    #[test]
    fn terms_parse_strictly_and_validate_their_share() {
        let gap: CacheAssurance = serde_json::from_value(serde_json::json!({"gap_share": 0.4})).unwrap();
        assert_eq!(gap, CacheAssurance::GapShare(0.4));
        let floor: CacheAssurance = serde_yaml::from_str("prompt_floor: 0.9").unwrap();
        assert_eq!(floor, CacheAssurance::PromptFloor(0.9));
        assert!(serde_json::from_value::<CacheAssurance>(serde_json::json!({"gap_share": 0.4, "prompt_floor": 0.9})).is_err());
        assert!(serde_json::from_value::<CacheAssurance>(serde_json::json!({"alpha": 0.4})).is_err());
        assert!(serde_json::from_value::<CacheAssurance>(serde_json::json!({})).is_err());
        assert!(
            serde_json::from_value::<CacheAssurance>(serde_json::json!({"gap_share": 1.5})).is_err(),
            "the range is checked however the terms are read, stored rows included"
        );
        let schema = serde_json::to_value(schemars::schema_for!(CacheAssurance)).unwrap();
        assert!(schema["properties"]["gap_share"].is_object(), "{schema}");
        assert!(gap.validate("ctx").is_ok());
        for bad in [-0.1, 1.5, f64::NAN] {
            let err = CacheAssurance::GapShare(bad).validate("ctx").unwrap_err().to_string();
            assert!(err.contains("between 0 and 1"), "{err}");
        }
        assert_eq!(
            serde_json::to_value(CacheAssurance::PromptFloor(0.9)).unwrap(),
            serde_json::json!({"prompt_floor": 0.9}),
            "stored in model_overlays in this shape"
        );
    }

    #[test]
    fn a_fixed_book_answers_per_principal_and_model() {
        let org = uuid::Uuid::new_v4();
        let book = AssuranceBook::fixed(HashMap::from([((org, "chat".to_string()), CacheAssurance::GapShare(0.5))]));
        assert_eq!(book.terms(org, "chat"), Some(CacheAssurance::GapShare(0.5)));
        assert_eq!(book.terms(org, "other"), None);
        assert_eq!(book.terms(uuid::Uuid::new_v4(), "chat"), None);
        assert!(book.names_model("chat"));
        assert!(!book.names_model("other"));
    }
}
