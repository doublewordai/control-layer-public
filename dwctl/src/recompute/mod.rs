
use crate::pricing::TokenCounts;
use crate::request_logging::serializers::{
    TokenMetrics, extract_engine_cached_tokens, extract_from_last_usage, parse_ai_response, raw_usage_tokens,
};
use outlet::{RequestData, ResponseData};

pub mod cache_fields;
pub mod cache_replay;
pub mod replay;
pub mod report;
pub mod source;
#[cfg(test)]
mod verify_harness;

use cache_fields::{CacheReading, CreationTier, cache_tokens_both_shapes};
use replay::RecomputeError;

const RENDER_TOLERANCE_BPS: i64 = 100;

#[tracing::instrument(skip(pool, filter, classifier, tokenizer), fields(limit = filter.limit))]
pub async fn recompute_corpus(
    pool: &sqlx::PgPool,
    filter: &source::CorpusFilter,
    flat_tier: CreationTier,
    cache_retention_grace: chrono::Duration,
    classifier: Option<&crate::prompt_cache::Classifier>,
    tokenizer: Option<&crate::prompt_cache::TokenizerClient>,
) -> Result<report::RecomputeReport, sqlx::Error> {
    let corpus = source::load_corpus(pool, filter).await?;

    let mut aliases: Vec<String> = corpus.iter().filter_map(|r| r.model.clone()).collect();
    aliases.sort();
    aliases.dedup();
    let mut accounts: Vec<_> = corpus.iter().filter_map(|r| r.user_id).collect();
    accounts.sort();
    accounts.dedup();
    let cache_tariffs = crate::pricing::lookup_cache_tariffs(pool, &aliases, &accounts).await?;
    let mut rows_tariff_unresolvable = 0i64;

    let mut rows_tariff_unresolvable_count = |row: &source::CorpusRow, mults: &Option<crate::pricing::CacheMultipliers>| {
        if mults.is_none() && row.stored_cache_total() > 0 {
            rows_tariff_unresolvable += 1;
        }
    };
    let price = |row: &source::CorpusRow, usage: &RecomputedUsage, mults: Option<crate::pricing::CacheMultipliers>| {
        let mults = crate::pricing::clamp_implicit_read_multiplier(mults, row.cache_read_source.as_deref());
        crate::pricing::charged_cost(
            &usage.counts,
            row.model.as_deref(),
            row.input_price_per_token,
            row.output_price_per_token,
            mults,
            crate::metrics::errors::component::ANALYTICS,
        )
        .map(|c| c.round_dp_with_strategy(8, rust_decimal::RoundingStrategy::MidpointAwayFromZero))
    };

    let mut rows: Vec<report::ReportRow> = Vec::with_capacity(corpus.len());
    for row in corpus.iter() {
        let Some(exchange) = &row.exchange else {
            rows.push(report::ReportRow::unreplayable(
                row,
                report::Evidence::ColumnsOnly,
                "no stored request/response body for this request".to_string(),
            ));
            continue;
        };

        let replayed = exchange.to_outlet_pair().and_then(|(req, resp)| {
            recompute_from_stored_response(&req, &resp, flat_tier).map(|mut usage| {
                if row.cache_read_source.as_deref() == Some(crate::prompt_cache::CacheReadSource::Engine.as_str())
                    && let Some(engine) = extract_engine_cached_tokens(&resp)
                {
                    usage.counts.cache_read = engine.clamp(0, usage.counts.prompt);
                    usage.counts.cache_creation_5m = 0;
                    usage.counts.cache_creation_1h = 0;
                    usage.counts.cache_creation_24h = 0;
                    usage.cache_tier_inferred = false;
                }
                usage
            })
        });

        let report_row = match replayed {
            Ok(usage) => {
                let cache_mults = row
                    .model
                    .as_deref()
                    .and_then(|alias| cache_tariffs.get(alias))
                    .and_then(|versions| {
                        crate::pricing::resolve_cache_multipliers(
                            versions,
                            row.pricing_timestamp(),
                            row.user_id,
                            row.resolved_serving_class.as_deref(),
                        )
                    });
                rows_tariff_unresolvable_count(row, &cache_mults);
                let cost = price(row, &usage, cache_mults);
                let mut report_row = report::ReportRow::replayed(row, &usage, cost);

                if let (Some(tok), Some(model)) = (tokenizer, row.model.as_deref())
                    && usage.counts.prompt > 0
                    && let Some(total) = render_request_total(tok, model, exchange).await
                {
                    let divergence = Disagreement {
                        reported_prompt: usage.counts.prompt,
                        rendered_prompt: total,
                    };
                    report_row.prompt_render_total = Some(total);
                    report_row.prompt_render_agrees = Some(divergence.divergence_bps() <= RENDER_TOLERANCE_BPS);
                }
                report_row
            }
            Err(replay::RecomputeError::NoUsage) => {
                let rescued = match (tokenizer, row.model.as_deref()) {
                    (Some(tok), Some(model)) => rescue_usage_less_row(tok, model, exchange, flat_tier).await,
                    _ => None,
                };
                match rescued {
                    Some(usage) => {
                        let cache_mults = row
                            .model
                            .as_deref()
                            .and_then(|alias| cache_tariffs.get(alias))
                            .and_then(|versions| {
                                crate::pricing::resolve_cache_multipliers(
                                    versions,
                                    row.pricing_timestamp(),
                                    row.user_id,
                                    row.resolved_serving_class.as_deref(),
                                )
                            });
                        rows_tariff_unresolvable_count(row, &cache_mults);
                        let cost = price(row, &usage, cache_mults);
                        let mut report_row = report::ReportRow::replayed(row, &usage, cost);
                        report_row.prompt_render_total = Some(usage.counts.prompt);
                        report_row
                    }
                    None => {
                        report::ReportRow::unreplayable(row, report::Evidence::NotReplayable, replay::RecomputeError::NoUsage.to_string())
                    }
                }
            }
            Err(replay::RecomputeError::ZeroDataRetention) => report::ReportRow::unreplayable(
                row,
                report::Evidence::ColumnsOnly,
                "zero-data-retention: payload is encrypted, tokens cannot be verified".to_string(),
            ),
            Err(e) => report::ReportRow::unreplayable(row, report::Evidence::NotReplayable, e.to_string()),
        };
        rows.push(report_row);
    }

    if let Some(base) = classifier {
        for (report_row, row) in rows.iter_mut().zip(corpus.iter()) {
            let (Some(principal), Some(model), Some(exchange)) = (row.user_id, row.model.as_deref(), row.exchange.as_ref()) else {
                continue;
            };
            let Some(body) = exchange.request_body.as_deref().filter(|b| !b.is_empty()) else {
                continue;
            };
            if replay::is_zdr_envelope(Some(body)) {
                continue;
            }
            if row.cache_read_source.as_deref() == Some(crate::prompt_cache::CacheReadSource::Engine.as_str()) {
                continue;
            }

            let route_has_blocks = !crate::prompt_cache::path_is_plain_completions(&exchange.endpoint);
            let historical = base.with_index(std::sync::Arc::new(cache_replay::HistoricalIndex::new(pool.clone(), row.timestamp)));
            match cache_replay::reconstruct_split(
                &historical,
                model,
                row.resolved_serving_class.as_deref(),
                body,
                principal,
                row.timestamp,
                route_has_blocks,
            )
            .await
            {
                Ok(Some(split)) => {
                    report_row.reconstructed_cache = Some(report::ReconstructedCache::compare(&split, row));
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::debug!(analytics_id = row.analytics_id, error = %e, "cache split reconstruction failed");
                }
            }
        }
    }

    let oldest = corpus.iter().map(|r| r.timestamp).min();

    let mut warnings = report::corpus_warnings(oldest, cache_retention_grace);
    if rows_tariff_unresolvable > 0 {
        warnings.push(format!(
            "{rows_tariff_unresolvable} row(s) recorded cache tokens but no cache tariff resolves at their \
             pricing time (tariff history deleted with its model?): they re-price at list rate, which is not \
             what was charged — do not apply their cost deltas without resolving the tariff first"
        ));
    }

    Ok(report::RecomputeReport {
        generated_at: chrono::Utc::now(),
        warnings,
        corpus: serde_json::json!({
            "start": filter.start,
            "end": filter.end,
            "user_id": filter.user_id,
            "uri_pattern": filter.uri_pattern,
            "model": filter.model,
            "limit": filter.limit,
        }),
        summary: report::ReportSummary::of(&rows),
        rows,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenSource {
    Reported,
    Rendered,
    Estimated,
}

impl TokenSource {
    #[allow(dead_code)]
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Reported | Self::Rendered)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reported => "reported",
            Self::Rendered => "rendered",
            Self::Estimated => "estimated",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Disagreement {
    pub reported_prompt: i64,
    pub rendered_prompt: i64,
}

impl Disagreement {
    pub fn divergence_bps(&self) -> i64 {
        let hi = self.reported_prompt.max(self.rendered_prompt).max(1);
        let gap = (self.reported_prompt - self.rendered_prompt).abs();
        gap.saturating_mul(10_000) / hi
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RecomputedUsage {
    pub counts: TokenCounts,
    pub reasoning: i64,
    pub total: i64,
    pub prompt_source: TokenSource,
    pub completion_source: TokenSource,
    pub cache_tier_inferred: bool,
    #[allow(dead_code)]
    pub response_type: String,
    #[allow(dead_code)]
    pub response_model: Option<String>,
}

impl RecomputedUsage {
    #[allow(dead_code)]
    pub fn is_exact(&self) -> bool {
        self.prompt_source.is_exact() && self.completion_source.is_exact()
    }
}

pub fn recompute_from_stored_response(
    request_data: &RequestData,
    response_data: &ResponseData,
    flat_tier: CreationTier,
) -> Result<RecomputedUsage, RecomputeError> {
    let parsed = parse_ai_response(request_data, response_data).map_err(RecomputeError::Parse)?;
    let mut metrics = TokenMetrics::from(&parsed);
    if metrics.prompt_tokens == 0 && metrics.completion_tokens == 0 && metrics.total_tokens == 0 {
        match extract_from_last_usage(response_data, raw_usage_tokens) {
            Some(raw) => {
                metrics.prompt_tokens = raw.prompt;
                metrics.completion_tokens = raw.completion;
                metrics.reasoning_tokens = raw.reasoning;
                metrics.total_tokens = raw.total;
            }
            None => return Err(RecomputeError::NoUsage),
        }
    }
    let CacheReading {
        tokens: cache,
        tier_inferred,
    } = extract_from_last_usage(response_data, |usage| cache_tokens_both_shapes(usage, flat_tier));

    Ok(RecomputedUsage {
        counts: TokenCounts {
            prompt: metrics.prompt_tokens,
            completion: metrics.completion_tokens,
            cache_read: cache.read,
            cache_creation_5m: cache.creation_5m,
            cache_creation_1h: cache.creation_1h,
            cache_creation_24h: cache.creation_24h,
        },
        reasoning: metrics.reasoning_tokens,
        total: metrics.total_tokens,
        prompt_source: TokenSource::Reported,
        completion_source: TokenSource::Reported,
        cache_tier_inferred: tier_inferred,
        response_type: metrics.response_type,
        response_model: metrics.response_model,
    })
}

async fn render_request_total(
    tokenizer: &crate::prompt_cache::TokenizerClient,
    model: &str,
    exchange: &replay::StoredExchange,
) -> Option<i64> {
    let body = exchange.request_body.as_deref()?;
    if replay::is_zdr_envelope(Some(body)) {
        return None;
    }
    let req: serde_json::Value = serde_json::from_slice(body).ok()?;
    let messages = req.get("messages")?.clone();
    let tools = req.get("tools");
    match tokenizer.render(model, &messages, tools, true, &[]).await {
        Ok(resp) => Some(i64::from(resp.total)),
        Err(e) => {
            tracing::debug!(model, error = %e, "recompute render skipped");
            None
        }
    }
}

async fn rescue_usage_less_row(
    tokenizer: &crate::prompt_cache::TokenizerClient,
    model: &str,
    exchange: &replay::StoredExchange,
    _flat_tier: CreationTier,
) -> Option<RecomputedUsage> {
    let prompt = render_request_total(tokenizer, model, exchange).await?;
    let texts = raw_completion_texts(exchange.response_body.as_deref()?)?;
    let completion = if texts.is_empty() {
        0
    } else {
        match tokenizer.tokenize(model, &texts).await {
            Ok(resp) => i64::from(resp.total),
            Err(e) => {
                tracing::debug!(model, error = %e, "recompute rescue tokenize failed");
                return None;
            }
        }
    };

    Some(RecomputedUsage {
        counts: TokenCounts {
            prompt,
            completion,
            cache_read: 0,
            cache_creation_5m: 0,
            cache_creation_1h: 0,
            cache_creation_24h: 0,
        },
        reasoning: 0,
        total: prompt + completion,
        prompt_source: TokenSource::Rendered,
        completion_source: TokenSource::Estimated,
        cache_tier_inferred: false,
        response_type: "tokenizer_rescue".to_string(),
        response_model: None,
    })
}

fn raw_completion_texts(body: &[u8]) -> Option<Vec<String>> {
    use serde_json::Value;

    fn texts_from_message(m: &Value, out: &mut Vec<String>) {
        for key in ["content", "reasoning_content", "reasoning", "text"] {
            if let Some(s) = m.get(key).and_then(Value::as_str)
                && !s.is_empty()
            {
                out.push(s.to_string());
            }
        }
        if let Some(calls) = m.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                if let Some(f) = call.get("function") {
                    for key in ["name", "arguments"] {
                        if let Some(s) = f.get(key).and_then(Value::as_str) {
                            out.push(s.to_string());
                        }
                    }
                }
            }
        }
    }

    let text = String::from_utf8_lossy(body);
    let mut out = Vec::new();

    if let Ok(v) = serde_json::from_str::<Value>(text.trim()) {
        for choice in v.get("choices")?.as_array()? {
            if let Some(m) = choice.get("message") {
                texts_from_message(m, &mut out);
            }
            texts_from_message(choice, &mut out);
        }
        return Some(out);
    }

    let mut saw_frame = false;
    for line in text.lines() {
        let Some(data) = line.strip_prefix("data:") else { continue };
        let trimmed = data.trim();
        if trimmed == "[DONE]" {
            continue;
        }
        let Ok(chunk) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        saw_frame = true;
        for choice in chunk.get("choices").and_then(Value::as_array).unwrap_or(&Vec::new()) {
            if let Some(d) = choice.get("delta") {
                texts_from_message(d, &mut out);
            }
        }
    }
    saw_frame.then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(prompt: i64, read: i64, c1h: i64) -> RecomputedUsage {
        RecomputedUsage {
            counts: TokenCounts {
                prompt,
                completion: 50,
                cache_read: read,
                cache_creation_5m: 0,
                cache_creation_1h: c1h,
                cache_creation_24h: 0,
            },
            reasoning: 0,
            total: prompt + 50,
            prompt_source: TokenSource::Reported,
            completion_source: TokenSource::Reported,
            cache_tier_inferred: false,
            response_type: "chat_completion".to_string(),
            response_model: None,
        }
    }

    #[test]
    fn completion_texts_from_blocking_chat_body() {
        let body = br#"{"choices":[{"message":{"content":"answer","reasoning_content":"thinking",
            "tool_calls":[{"function":{"name":"f","arguments":"{\"x\":1}"}}]}}]}"#;
        let texts = raw_completion_texts(body).expect("blocking JSON parses");
        assert_eq!(texts, vec!["answer", "thinking", "f", "{\"x\":1}"]);
    }

    #[test]
    fn completion_texts_from_sse_deltas() {
        let body = b"data: {\"choices\":[{\"delta\":{\"content\":\"hel\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\ndata: [DONE]\n\n";
        let texts = raw_completion_texts(body).expect("SSE parses");
        assert_eq!(texts, vec!["hel", "lo"]);
    }

    #[test]
    fn completion_texts_refuse_an_unparseable_body() {
        assert!(raw_completion_texts(b"not json, not sse").is_none());
        assert_eq!(raw_completion_texts(br#"{"choices":[]}"#), Some(vec![]));
    }

    #[test]
    fn divergence_is_scale_free() {
        let small = Disagreement {
            reported_prompt: 200,
            rendered_prompt: 198,
        };
        let large = Disagreement {
            reported_prompt: 20_000,
            rendered_prompt: 19_800,
        };
        assert_eq!(small.divergence_bps(), 100);
        assert_eq!(large.divergence_bps(), 100);
    }

    #[test]
    fn divergence_handles_zero_without_dividing_by_it() {
        let d = Disagreement {
            reported_prompt: 0,
            rendered_prompt: 0,
        };
        assert_eq!(d.divergence_bps(), 0);
    }

    #[test]
    fn only_exact_sources_are_applicable_without_opt_in() {
        assert!(TokenSource::Reported.is_exact());
        assert!(TokenSource::Rendered.is_exact());
        assert!(!TokenSource::Estimated.is_exact(), "estimates must not auto-apply");

        let mut u = usage(100, 0, 0);
        assert!(u.is_exact());
        u.completion_source = TokenSource::Estimated;
        assert!(!u.is_exact(), "one estimated figure taints the whole row");
    }
}
