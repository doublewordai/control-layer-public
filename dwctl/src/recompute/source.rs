
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use super::replay::StoredExchange;

#[derive(Debug, Clone)]
pub struct CorpusFilter {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub user_id: Option<Uuid>,
    pub uri_pattern: Option<String>,
    pub model: Option<String>,
    pub limit: i64,
}

#[derive(Debug, Clone)]
pub struct CorpusRow {
    pub analytics_id: i64,
    pub user_id: Option<Uuid>,
    pub model: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub fusillade_request_id: Option<Uuid>,
    pub fusillade_batch_id: Option<Uuid>,
    pub batch_created_at: Option<DateTime<Utc>>,

    pub stored_prompt_tokens: i64,
    pub stored_completion_tokens: i64,
    pub stored_reasoning_tokens: i64,
    pub stored_total_tokens: i64,
    pub stored_cache_read: i64,
    pub stored_cache_creation_5m: i64,
    pub stored_cache_creation_1h: i64,
    pub stored_cache_creation_24h: i64,
    pub stored_total_cost: Option<Decimal>,

    pub input_price_per_token: Option<Decimal>,
    pub output_price_per_token: Option<Decimal>,

    pub cache_read_source: Option<String>,
    pub resolved_serving_class: Option<String>,

    pub exchange: Option<StoredExchange>,
}

impl CorpusRow {
    pub fn stored_cache_total(&self) -> i64 {
        self.stored_cache_read + self.stored_cache_creation_5m + self.stored_cache_creation_1h + self.stored_cache_creation_24h
    }

    pub fn pricing_timestamp(&self) -> DateTime<Utc> {
        self.batch_created_at.unwrap_or(self.timestamp)
    }
}

#[tracing::instrument(skip(pool), fields(limit = filter.limit))]
pub async fn load_corpus(pool: &PgPool, filter: &CorpusFilter) -> Result<Vec<CorpusRow>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT
            ha.id                              AS "analytics_id!",
            ha.user_id,
            ha.model,
            ha.timestamp                       AS "timestamp!",
            ha.uri                             AS "uri!",
            ha.status_code,
            ha.fusillade_request_id,
            ha.fusillade_batch_id,
            ha.prompt_tokens                   AS "prompt_tokens!",
            ha.completion_tokens               AS "completion_tokens!",
            ha.reasoning_tokens                AS "reasoning_tokens!",
            ha.total_tokens                    AS "total_tokens!",
            ha.cache_read_input_tokens         AS "cache_read!",
            ha.cache_creation_5m_input_tokens  AS "cache_creation_5m!",
            ha.cache_creation_1h_input_tokens  AS "cache_creation_1h!",
            ha.cache_creation_24h_input_tokens AS "cache_creation_24h!",
            ha.total_cost,
            ha.input_price_per_token,
            ha.output_price_per_token,
            ha.cache_read_source,
            ha.resolved_serving_class,
            -- `?` overrides sqlx's nullability inference: rt.body is NOT NULL in its own
            -- table, but this is a LEFT JOIN, so it is absent for any row with no fusillade
            -- link. Without the override sqlx types it as String and the None case vanishes.
            rt.body                            AS "request_body?",
            r.response_body                    AS "response_body?",
            r.response_status                  AS "response_status?",
            b.created_at                       AS "batch_created_at?"
        FROM http_analytics ha
        LEFT JOIN fusillade.requests r          ON r.id  = ha.fusillade_request_id
        LEFT JOIN fusillade.request_templates_all rt ON rt.id = r.template_id
        LEFT JOIN fusillade.batches b            ON b.id  = ha.fusillade_batch_id
        -- Ordered so the planner drives off a timestamp index. There is NO index on
        -- http_analytics.user_id, and ordering by `id` instead makes Postgres walk the
        -- primary key filtering as it goes — on a 186M-row table that scans most of it
        -- before the LIMIT is satisfied. The time window is the only selective thing here,
        -- so it has to lead: idx_analytics_model_timestamp when a model is given, otherwise
        -- idx_analytics_timestamp.
        WHERE ha.timestamp >= $1
          AND ha.timestamp <= $2
          AND ($3::uuid IS NULL OR ha.user_id = $3)
          AND ($4::text IS NULL OR ha.uri LIKE $4)
          AND ($5::text IS NULL OR ha.model = $5)
          -- Only rows that were billed: attributed, successful, and priced. A NULL
          -- total_cost is a free model, where no charge was ever expected.
          AND ha.user_id IS NOT NULL
          AND ha.status_code BETWEEN 200 AND 299
        ORDER BY ha.timestamp DESC
        LIMIT $6
        "#,
        filter.start,
        filter.end,
        filter.user_id,
        filter.uri_pattern,
        filter.model,
        filter.limit,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| {
            let exchange = r.response_body.map(|body| {
                let request_body = r.request_body.map(|b| b.into_bytes());
                StoredExchange {
                    endpoint: r.uri.clone(),
                    streamed: StoredExchange::streamed_from_body(request_body.as_deref()),
                    request_body,
                    response_body: Some(body.into_bytes()),
                    status_code: r.response_status.unwrap_or(r.status_code.unwrap_or(0) as i16) as u16,
                }
            });

            CorpusRow {
                analytics_id: r.analytics_id,
                user_id: r.user_id,
                model: r.model,
                timestamp: r.timestamp,
                fusillade_request_id: r.fusillade_request_id,
                fusillade_batch_id: r.fusillade_batch_id,
                batch_created_at: r.batch_created_at,
                stored_prompt_tokens: r.prompt_tokens,
                stored_completion_tokens: r.completion_tokens,
                stored_reasoning_tokens: r.reasoning_tokens,
                stored_total_tokens: r.total_tokens,
                stored_cache_read: r.cache_read,
                stored_cache_creation_5m: r.cache_creation_5m,
                stored_cache_creation_1h: r.cache_creation_1h,
                stored_cache_creation_24h: r.cache_creation_24h,
                stored_total_cost: r.total_cost,
                input_price_per_token: r.input_price_per_token,
                output_price_per_token: r.output_price_per_token,
                cache_read_source: r.cache_read_source,
                resolved_serving_class: r.resolved_serving_class,
                exchange,
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prompt_cache::TokenizerClient;
    use crate::recompute::cache_fields::CreationTier;
    use crate::recompute::recompute_corpus;
    use sqlx::PgPool;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn mock_tokenizer(render_total: u32, tokenize_total: u32) -> (MockServer, TokenizerClient) {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/render"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "virtual_model": "m", "tokenizer_version": "tok-v1", "template_version": "tpl-v1",
                "total": render_total, "prefix_counts": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/tokenize"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "virtual_model": "m", "tokenizer_version": "tok-v1",
                "segment_counts": [tokenize_total], "cumulative": [tokenize_total], "total": tokenize_total
            })))
            .mount(&server)
            .await;
        let client = TokenizerClient::new(server.uri());
        (server, client)
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed(
        pool: &PgPool,
        uri: &str,
        response_body: &str,
        prompt: i64,
        completion: i64,
        cache_read: i64,
        cache_creation_5m: i64,
        cost: Decimal,
    ) -> (Uuid, i64) {
        let user_id = Uuid::new_v4();
        sqlx::query!(
            "INSERT INTO users (id, username, email, is_admin, auth_source) VALUES ($1,$2,$3,false,'test')",
            user_id,
            format!("u_{}", user_id.simple()),
            format!("{}@example.com", user_id.simple()),
        )
        .execute(pool)
        .await
        .unwrap();

        let template_id = Uuid::new_v4();
        crate::test::utils::insert_fusillade_template(
            pool,
            template_id,
            None,
            "m",
            "http://x",
            uri,
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#,
            None,
        )
        .await;

        let request_id = Uuid::new_v4();
        sqlx::query!(
            "INSERT INTO fusillade.requests (id, template_id, state, model, response_status, response_body,
                                             claimed_at, started_at, completed_at, created_by)
             VALUES ($1,$2,'completed','m',200,$3,NOW(),NOW(),NOW(),$4)",
            request_id,
            template_id,
            response_body,
            user_id.to_string(),
        )
        .execute(pool)
        .await
        .unwrap();

        let analytics_id = sqlx::query_scalar!(
            "INSERT INTO http_analytics
               (instance_id, correlation_id, timestamp, method, uri, model, status_code, user_id,
                prompt_tokens, completion_tokens, total_tokens, cache_read_input_tokens,
                cache_creation_5m_input_tokens, cache_creation_input_tokens,
                total_cost, input_price_per_token, output_price_per_token, fusillade_request_id)
             VALUES ($1,1,NOW(),'POST',$2,'m',200,$3,$4,$5,$6,$7,$8,$8,$9,0.000001,0.000002,$10)
             RETURNING id",
            Uuid::new_v4(),
            uri,
            user_id,
            prompt,
            completion,
            prompt + completion,
            cache_read,
            cache_creation_5m,
            cost,
            request_id,
        )
        .fetch_one(pool)
        .await
        .unwrap();

        (user_id, analytics_id)
    }

    const GRACE: chrono::Duration = chrono::Duration::days(crate::recompute::report::CACHE_GRACE_DAYS);

    fn filter_for(user_id: Uuid) -> CorpusFilter {
        CorpusFilter {
            start: Utc::now() - chrono::Duration::hours(1),
            end: Utc::now() + chrono::Duration::hours(1),
            user_id: Some(user_id),
            uri_pattern: None,
            model: None,
            limit: 100,
        }
    }

    #[dwctl_test_macros::test]
    async fn healthy_traffic_recomputes_to_zero_delta(pool: PgPool) {
        let (user_id, _) = seed(
            &pool,
            "/chat/completions",
            r#"{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1000,"completion_tokens":100,"total_tokens":1100}}"#,
            1000,
            100,
            0,
            0,
            Decimal::from_str_exact("0.0012").unwrap(),
        )
        .await;

        let report = recompute_corpus(&pool, &filter_for(user_id), CreationTier::FiveMinute, GRACE, None, None)
            .await
            .unwrap();

        assert_eq!(report.summary.rows_total, 1);
        assert_eq!(report.summary.rows_changed, 0, "healthy traffic must propose no change");
        assert_eq!(report.summary.rows_unchanged, 1);
        assert_eq!(report.summary.net_correction, Decimal::ZERO);
    }

    #[dwctl_test_macros::test]
    async fn anthropic_incident_row_is_detected_and_corrected(pool: PgPool) {
        let (user_id, analytics_id) = seed(
            &pool,
            "/messages?beta=true",
            r#"{"id":"msg_1","type":"message","role":"assistant","model":"m","content":[],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":565,"output_tokens":658,"cache_read_input_tokens":17631,"cache_creation_input_tokens":538}}"#,
            565,
            658,
            17631,
            0,
            Decimal::from_str_exact("0.000447942276").unwrap(),
        )
        .await;

        let report = recompute_corpus(&pool, &filter_for(user_id), CreationTier::FiveMinute, GRACE, None, None)
            .await
            .unwrap();

        assert_eq!(report.summary.rows_changed, 1);
        let row = report.rows.iter().find(|r| r.analytics_id == analytics_id).unwrap();
        let rec = row.recomputed.as_ref().expect("row is replayable");

        assert_eq!(rec.prompt_tokens, 18_734, "565 + 17631 + 538");
        assert_eq!(rec.completion_tokens, 658, "completion was already right");
        assert_eq!(rec.cache_read, 17_631, "the split is carried through, not invented");
        assert_eq!(rec.cache_creation_5m, 538, "recovered from the FLAT field");
        assert!(row.cache_tier_inferred, "the body never stated a tier");
        assert!(report.summary.net_correction > Decimal::ZERO, "an undercharge");
    }

    #[dwctl_test_macros::test]
    async fn cached_row_reprices_with_the_tariff_valid_at_its_time(pool: PgPool) {
        let creator = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let endpoint = crate::test::utils::create_test_endpoint(&pool, "ep-tariff", creator.id).await;
        let model_id = crate::test::utils::create_test_model(&pool, "m", "m", endpoint, creator.id).await;
        sqlx::query!(
            r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, read_multiplier, min_prefix_tokens, valid_from, valid_until)
               VALUES ($1, 2.0, 2.0, 2.5, 0.5, 1024, now() - interval '2 hours', now() - interval '1 hour'),
                      ($1, 1.0, 1.0, 1.0, 0.8, 1024, now() - interval '1 hour', NULL)"#,
            model_id,
        )
        .execute(&pool)
        .await
        .unwrap();

        let (user_id, _) = seed(
            &pool,
            "/chat/completions",
            r#"{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":31840,"completion_tokens":709,"total_tokens":32549,"cache_read_input_tokens":30723,"cache_creation_input_tokens":251,"cache_creation":{"ephemeral_5m_input_tokens":251,"ephemeral_1h_input_tokens":0,"ephemeral_24h_input_tokens":0},"prompt_tokens_details":{"cached_tokens":30723}}}"#,
            31840,
            709,
            30723,
            251,
            Decimal::from_str_exact("0.0271134").unwrap(),
        )
        .await;

        let report = recompute_corpus(&pool, &filter_for(user_id), CreationTier::FiveMinute, GRACE, None, None)
            .await
            .unwrap();

        assert_eq!(report.summary.rows_total, 1);
        assert_eq!(
            report.summary.rows_changed, 0,
            "a healthy cached row must re-price to exactly what the live tariff charged"
        );
        assert_eq!(report.summary.net_correction, Decimal::ZERO);
        assert!(report.warnings.is_empty(), "the tariff resolved; nothing to warn about");
    }

    #[dwctl_test_macros::test]
    async fn cost_is_compared_at_the_stored_column_scale(pool: PgPool) {
        let creator = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let endpoint = crate::test::utils::create_test_endpoint(&pool, "ep-scale", creator.id).await;
        let model_id = crate::test::utils::create_test_model(&pool, "m", "m", endpoint, creator.id).await;
        sqlx::query!(
            r#"INSERT INTO model_cache_tariffs
                 (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, read_multiplier, min_prefix_tokens)
               VALUES ($1, 1.0, 1.0, 1.0, 0.5714, 1024)"#,
            model_id,
        )
        .execute(&pool)
        .await
        .unwrap();

        let (user_id, _) = seed(
            &pool,
            "/chat/completions",
            r#"{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":200,"completion_tokens":10,"total_tokens":210,"cache_read_input_tokens":101,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0,"ephemeral_24h_input_tokens":0},"prompt_tokens_details":{"cached_tokens":101}}}"#,
            200,
            10,
            101,
            0,
            Decimal::from_str_exact("0.00017671").unwrap(),
        )
        .await;

        let report = recompute_corpus(&pool, &filter_for(user_id), CreationTier::FiveMinute, GRACE, None, None)
            .await
            .unwrap();

        assert_eq!(
            report.summary.rows_changed, 0,
            "a sub-8dp arithmetic tail must not read as a correction"
        );
        assert_eq!(report.summary.net_correction, Decimal::ZERO);
    }

    #[dwctl_test_macros::test]
    async fn july_null_usage_row_surfaces_as_not_replayable(pool: PgPool) {
        let (user_id, _) = seed(
            &pool,
            "/chat/completions",
            r#"{"choices":[{"finish_reason":"tool_calls","index":0,"message":{"role":"assistant","content":null}}],"created":1,"id":"c","model":"m","object":"chat.completion","usage":null}"#,
            0,
            0,
            0,
            0,
            Decimal::ZERO,
        )
        .await;

        let report = recompute_corpus(&pool, &filter_for(user_id), CreationTier::FiveMinute, GRACE, None, None)
            .await
            .unwrap();

        assert_eq!(report.summary.rows_total, 1);
        assert_eq!(report.summary.rows_not_replayable, 1, "no usage to re-read → needs a human");
        assert_eq!(report.summary.rows_unchanged, 0, "must not be certified healthy");
        assert_eq!(report.summary.rows_changed, 0);
        let row = &report.rows[0];
        assert!(
            row.note.as_deref().is_some_and(|n| n.contains("no usage object")),
            "the note must say why: {:?}",
            row.note
        );
    }

    #[dwctl_test_macros::test]
    async fn render_disagreement_is_annotated_but_never_adopted(pool: PgPool) {
        let (user_id, _) = seed(
            &pool,
            "/chat/completions",
            r#"{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1000,"completion_tokens":100,"total_tokens":1100}}"#,
            1000,
            100,
            0,
            0,
            Decimal::from_str_exact("0.0012").unwrap(),
        )
        .await;

        let (_server, tokenizer) = mock_tokenizer(2000, 0).await;
        let report = recompute_corpus(&pool, &filter_for(user_id), CreationTier::FiveMinute, GRACE, None, Some(&tokenizer))
            .await
            .unwrap();

        assert_eq!(report.summary.rows_changed, 0, "a contested count is a finding, not a correction");
        assert_eq!(report.summary.rows_render_checked, 1);
        assert_eq!(report.summary.rows_render_disagreed, 1);
        let row = &report.rows[0];
        assert_eq!(row.recomputed.as_ref().unwrap().prompt_tokens, 1000, "provider count kept");
        assert_eq!(row.prompt_render_total, Some(2000));
        assert_eq!(row.prompt_render_agrees, Some(false));
    }

    #[dwctl_test_macros::test]
    async fn render_agreement_is_annotated(pool: PgPool) {
        let (user_id, _) = seed(
            &pool,
            "/chat/completions",
            r#"{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1000,"completion_tokens":100,"total_tokens":1100}}"#,
            1000,
            100,
            0,
            0,
            Decimal::from_str_exact("0.0012").unwrap(),
        )
        .await;

        let (_server, tokenizer) = mock_tokenizer(1005, 0).await;
        let report = recompute_corpus(&pool, &filter_for(user_id), CreationTier::FiveMinute, GRACE, None, Some(&tokenizer))
            .await
            .unwrap();

        assert_eq!(report.summary.rows_changed, 0);
        assert_eq!(report.summary.rows_render_checked, 1);
        assert_eq!(report.summary.rows_render_disagreed, 0);
        let row = &report.rows[0];
        assert_eq!(row.recomputed.as_ref().unwrap().prompt_tokens, 1000, "never adopted, even agreeing");
        assert_eq!(row.prompt_render_agrees, Some(true));
    }

    #[dwctl_test_macros::test]
    async fn usage_less_row_is_rescued_by_the_tokenizer(pool: PgPool) {
        let (user_id, analytics_id) = seed(
            &pool,
            "/chat/completions",
            r#"{"choices":[{"finish_reason":"tool_calls","index":0,"message":{"role":"assistant","content":"partial answer"}}],"created":1,"id":"c","model":"m","object":"chat.completion","usage":null}"#,
            0,
            0,
            0,
            0,
            Decimal::ZERO,
        )
        .await;

        let (_server, tokenizer) = mock_tokenizer(4096, 512).await;
        let report = recompute_corpus(&pool, &filter_for(user_id), CreationTier::FiveMinute, GRACE, None, Some(&tokenizer))
            .await
            .unwrap();

        assert_eq!(report.summary.rows_not_replayable, 0, "rescued, not refused");
        assert_eq!(report.summary.rows_changed, 1);
        assert_eq!(report.summary.rows_tokenizer_rescued, 1);
        let row = report.rows.iter().find(|r| r.analytics_id == analytics_id).unwrap();
        let rec = row.recomputed.as_ref().expect("rescued row is replayed");
        assert_eq!(rec.prompt_tokens, 4096, "prompt is the exact render");
        assert_eq!(rec.completion_tokens, 512, "completion is the text estimate");
        assert_eq!(rec.cache_read, 0, "no usage object → no split to read, none invented");
        assert_eq!(row.token_source.as_deref(), Some("rendered"));
        assert_eq!(row.completion_token_source.as_deref(), Some("estimated"));
        assert_eq!(rec.cost, Some(Decimal::from_str_exact("0.00512").unwrap()));
        assert!(report.summary.net_correction > Decimal::ZERO, "recovered usage, undercharge");
        assert_eq!(
            report.summary.net_correction_estimated, report.summary.net_correction,
            "the whole correction rests on an estimate here, and the summary must say so"
        );
    }

    #[dwctl_test_macros::test]
    async fn a_row_without_a_body_is_reported_as_columns_only(pool: PgPool) {
        let user_id = Uuid::new_v4();
        sqlx::query!(
            "INSERT INTO users (id, username, email, is_admin, auth_source) VALUES ($1,$2,$3,false,'test')",
            user_id,
            format!("u_{}", user_id.simple()),
            format!("{}@example.com", user_id.simple()),
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query!(
            "INSERT INTO http_analytics (instance_id, correlation_id, timestamp, method, uri, model,
                status_code, user_id, prompt_tokens, completion_tokens, total_cost)
             VALUES ($1,1,NOW(),'POST','/chat/completions','m',200,$2,10,5,0.0001)",
            Uuid::new_v4(),
            user_id,
        )
        .execute(&pool)
        .await
        .unwrap();

        let report = recompute_corpus(&pool, &filter_for(user_id), CreationTier::FiveMinute, GRACE, None, None)
            .await
            .unwrap();

        assert_eq!(report.summary.rows_total, 1);
        assert_eq!(report.summary.rows_columns_only, 1);
        assert_eq!(report.summary.rows_changed, 0, "unverifiable is not the same as changed");
        assert_eq!(
            report.summary.net_correction,
            Decimal::ZERO,
            "an unchecked row must not move the net correction"
        );
    }
}
