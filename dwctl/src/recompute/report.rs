
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use super::RecomputedUsage;
use super::source::CorpusRow;
use crate::pricing::TokenCounts;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub enum Evidence {
    BodyFrame,
    ColumnsOnly,
    NotReplayable,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UsageFigures {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    pub cache_read: i64,
    pub cache_creation_5m: i64,
    pub cache_creation_1h: i64,
    pub cache_creation_24h: i64,
    #[schema(value_type = Option<String>)]
    pub cost: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ReportRow {
    pub analytics_id: i64,
    pub fusillade_request_id: Option<Uuid>,
    pub fusillade_batch_id: Option<Uuid>,
    pub timestamp: DateTime<Utc>,
    pub model: Option<String>,
    pub stored: UsageFigures,
    pub recomputed: Option<UsageFigures>,
    pub evidence: Evidence,
    pub token_source: Option<String>,
    pub cache_tier_inferred: bool,
    pub changed: bool,
    pub note: Option<String>,

    pub reconstructed_cache: Option<ReconstructedCache>,

    pub prompt_render_total: Option<i64>,
    pub prompt_render_agrees: Option<bool>,
    pub completion_token_source: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ReconstructedCache {
    pub cache_read: i64,
    pub cache_creation_5m: i64,
    pub cache_creation_1h: i64,
    pub cache_creation_24h: i64,
    pub render_total: Option<i64>,
    pub fidelity: String,
    pub agrees_with_stored: bool,
    pub render_matches_prompt: Option<bool>,
}

#[derive(Debug, Clone, Serialize, ToSchema, Default)]
pub struct ReportSummary {
    pub rows_total: i64,
    pub rows_changed: i64,
    pub rows_unchanged: i64,
    pub rows_not_replayable: i64,
    pub rows_columns_only: i64,
    pub rows_tier_inferred: i64,
    pub rows_cache_reconstructed: i64,
    pub rows_cache_disagreed: i64,
    pub rows_render_checked: i64,
    pub rows_render_disagreed: i64,
    pub rows_tokenizer_rescued: i64,
    pub stored_prompt_tokens: i64,
    pub recomputed_prompt_tokens: i64,
    pub stored_completion_tokens: i64,
    pub recomputed_completion_tokens: i64,
    #[schema(value_type = String)]
    pub stored_cost: Decimal,
    #[schema(value_type = String)]
    pub recomputed_cost: Decimal,
    #[schema(value_type = String)]
    pub net_correction: Decimal,
    #[schema(value_type = String)]
    pub net_correction_estimated: Decimal,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RecomputeReport {
    pub generated_at: DateTime<Utc>,
    pub corpus: serde_json::Value,
    pub summary: ReportSummary,
    pub warnings: Vec<String>,
    pub rows: Vec<ReportRow>,
}

pub const CACHE_GRACE_DAYS: i64 = 7;

pub fn corpus_warnings(oldest: Option<DateTime<Utc>>, cache_retention_grace: chrono::Duration) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(oldest) = oldest {
        let age = Utc::now() - oldest;
        if age > cache_retention_grace {
            out.push(format!(
                "corpus reaches {} days back, beyond the {}-day cache retention grace: \
                 prompt_cache_entries for the older rows may already be pruned, so the cache split \
                 may not be re-derivable for them and the tier may not resolve from ttl_tier",
                age.num_days(),
                cache_retention_grace.num_days()
            ));
        }
    }
    out
}

impl UsageFigures {
    fn from_stored(row: &CorpusRow) -> Self {
        Self {
            prompt_tokens: row.stored_prompt_tokens,
            completion_tokens: row.stored_completion_tokens,
            reasoning_tokens: row.stored_reasoning_tokens,
            total_tokens: row.stored_total_tokens,
            cache_read: row.stored_cache_read,
            cache_creation_5m: row.stored_cache_creation_5m,
            cache_creation_1h: row.stored_cache_creation_1h,
            cache_creation_24h: row.stored_cache_creation_24h,
            cost: row.stored_total_cost,
        }
    }

    fn from_recomputed(usage: &RecomputedUsage, cost: Option<Decimal>) -> Self {
        let TokenCounts {
            prompt,
            completion,
            cache_read,
            cache_creation_5m,
            cache_creation_1h,
            cache_creation_24h,
        } = usage.counts;
        Self {
            prompt_tokens: prompt,
            completion_tokens: completion,
            reasoning_tokens: usage.reasoning,
            total_tokens: usage.total,
            cache_read,
            cache_creation_5m,
            cache_creation_1h,
            cache_creation_24h,
            cost,
        }
    }

    fn differs_from(&self, other: &Self) -> bool {
        self.prompt_tokens != other.prompt_tokens
            || self.completion_tokens != other.completion_tokens
            || self.reasoning_tokens != other.reasoning_tokens
            || self.total_tokens != other.total_tokens
            || self.cache_read != other.cache_read
            || self.cache_creation_5m != other.cache_creation_5m
            || self.cache_creation_1h != other.cache_creation_1h
            || self.cache_creation_24h != other.cache_creation_24h
            || match (self.cost, other.cost) {
                (Some(a), Some(b)) => a != b,
                (None, None) => false,
                _ => true,
            }
    }
}

impl ReconstructedCache {
    pub fn compare(split: &super::cache_replay::ReconstructedSplit, row: &CorpusRow) -> Self {
        let (read, c5, c1, c24) = (
            split.read as i64,
            split.creation_5m as i64,
            split.creation_1h as i64,
            split.creation_24h as i64,
        );
        Self {
            cache_read: read,
            cache_creation_5m: c5,
            cache_creation_1h: c1,
            cache_creation_24h: c24,
            render_total: split.render_total.map(|t| t as i64),
            fidelity: match split.fidelity {
                super::cache_replay::Fidelity::Exact => "exact",
                super::cache_replay::Fidelity::Approximate => "approximate",
            }
            .to_string(),
            agrees_with_stored: read == row.stored_cache_read
                && c5 == row.stored_cache_creation_5m
                && c1 == row.stored_cache_creation_1h
                && c24 == row.stored_cache_creation_24h,
            render_matches_prompt: split.render_total.map(|t| t as i64 == row.stored_prompt_tokens),
        }
    }
}

impl ReportRow {
    pub(crate) fn replayed(row: &CorpusRow, usage: &RecomputedUsage, cost: Option<Decimal>) -> Self {
        let stored = UsageFigures::from_stored(row);
        let recomputed = UsageFigures::from_recomputed(usage, cost);
        let changed = stored.differs_from(&recomputed);
        Self {
            analytics_id: row.analytics_id,
            fusillade_request_id: row.fusillade_request_id,
            fusillade_batch_id: row.fusillade_batch_id,
            timestamp: row.timestamp,
            model: row.model.clone(),
            stored,
            recomputed: Some(recomputed),
            evidence: Evidence::BodyFrame,
            token_source: Some(usage.prompt_source.as_str().to_string()),
            cache_tier_inferred: usage.cache_tier_inferred,
            changed,
            note: None,
            reconstructed_cache: None,
            prompt_render_total: None,
            prompt_render_agrees: None,
            completion_token_source: Some(usage.completion_source.as_str().to_string()),
        }
    }

    pub fn unreplayable(row: &CorpusRow, evidence: Evidence, note: String) -> Self {
        Self {
            analytics_id: row.analytics_id,
            fusillade_request_id: row.fusillade_request_id,
            fusillade_batch_id: row.fusillade_batch_id,
            timestamp: row.timestamp,
            model: row.model.clone(),
            stored: UsageFigures::from_stored(row),
            recomputed: None,
            evidence,
            token_source: None,
            cache_tier_inferred: false,
            changed: false,
            note: Some(note),
            reconstructed_cache: None,
            prompt_render_total: None,
            prompt_render_agrees: None,
            completion_token_source: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grace() -> chrono::Duration {
        chrono::Duration::days(CACHE_GRACE_DAYS)
    }

    #[test]
    fn corpus_within_the_grace_warns_about_nothing() {
        let recent = Utc::now() - grace() + chrono::Duration::minutes(1);
        assert!(corpus_warnings(Some(recent), grace()).is_empty());
        assert!(
            corpus_warnings(None, grace()).is_empty(),
            "an empty corpus has nothing to warn about"
        );
    }

    #[test]
    fn corpus_just_past_the_grace_warns() {
        let just_past = Utc::now() - grace() - chrono::Duration::hours(1);
        assert_eq!(corpus_warnings(Some(just_past), grace()).len(), 1);
    }

    #[test]
    fn corpus_warning_follows_the_configured_grace() {
        let old = Utc::now() - chrono::Duration::days(CACHE_GRACE_DAYS + 3);
        assert!(corpus_warnings(Some(old), chrono::Duration::days(30)).is_empty());
    }

    #[test]
    fn corpus_beyond_the_grace_warns() {
        let old = Utc::now() - chrono::Duration::days(CACHE_GRACE_DAYS + 3);
        let warnings = corpus_warnings(Some(old), grace());
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("cache retention grace"), "got: {}", warnings[0]);
    }
}

impl ReportSummary {
    pub fn of(rows: &[ReportRow]) -> Self {
        let mut s = Self {
            rows_total: rows.len() as i64,
            ..Default::default()
        };
        for r in rows {
            match r.evidence {
                Evidence::NotReplayable => s.rows_not_replayable += 1,
                Evidence::ColumnsOnly => s.rows_columns_only += 1,
                Evidence::BodyFrame => {}
            }
            if r.cache_tier_inferred {
                s.rows_tier_inferred += 1;
            }
            if let Some(rc) = &r.reconstructed_cache {
                s.rows_cache_reconstructed += 1;
                if !rc.agrees_with_stored {
                    s.rows_cache_disagreed += 1;
                }
            }
            if let Some(agrees) = r.prompt_render_agrees {
                s.rows_render_checked += 1;
                if !agrees {
                    s.rows_render_disagreed += 1;
                }
            }
            if r.completion_token_source.as_deref() == Some("estimated") {
                s.rows_tokenizer_rescued += 1;
            }
            s.stored_prompt_tokens += r.stored.prompt_tokens;
            s.stored_completion_tokens += r.stored.completion_tokens;
            s.stored_cost += r.stored.cost.unwrap_or_default();

            match &r.recomputed {
                Some(rec) => {
                    if r.changed {
                        s.rows_changed += 1;
                    } else {
                        s.rows_unchanged += 1;
                    }
                    s.recomputed_prompt_tokens += rec.prompt_tokens;
                    s.recomputed_completion_tokens += rec.completion_tokens;
                    s.recomputed_cost += rec.cost.unwrap_or_default();
                    if r.completion_token_source.as_deref() == Some("estimated") {
                        s.net_correction_estimated += rec.cost.unwrap_or_default() - r.stored.cost.unwrap_or_default();
                    }
                }
                None => {
                    s.recomputed_prompt_tokens += r.stored.prompt_tokens;
                    s.recomputed_completion_tokens += r.stored.completion_tokens;
                    s.recomputed_cost += r.stored.cost.unwrap_or_default();
                }
            }
        }
        s.net_correction = s.recomputed_cost - s.stored_cost;
        s
    }
}
