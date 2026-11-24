
use std::str::FromStr;
use std::time::Duration;

use axum::http::StatusCode;
use axum_test::TestServer;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::{
    api::models::users::{Role, UserResponse},
    db::models::api_keys::ApiKeyPurpose,
    pricing::{PriceBook, PriceQuery, UnitPrices},
    prompt_cache::CacheReadSource,
    test::utils::{
        add_auth_headers, add_deployment_to_group, create_test_admin_user, create_test_api_key_for_user, create_test_app_with_config,
        create_test_config, create_test_deployment, create_test_user,
    },
    types::DeploymentId,
};

const FEED: &str = "/ai/v1/integrations/openrouter/models";
const MODEL: &str = "example-org/Model";
const REFRESH: &str = "/api/v1/providers/example-provider/listings/refresh";

struct Fixture {
    server: TestServer,
    _services: crate::BackgroundServices,
    pool: PgPool,
    admin: UserResponse,
    account: UserResponse,
    key: String,
    model: DeploymentId,
    openrouter: MockServer,
}

impl Fixture {
    async fn new(pool: &PgPool, refresh_status: u16, configure: impl FnOnce(&mut crate::config::Config)) -> Self {
        let openrouter = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(REFRESH))
            .respond_with(ResponseTemplate::new(refresh_status).set_body_json(json!({"data": {"id": "job-1"}})))
            .mount(&openrouter)
            .await;
        let account = create_test_user(pool, Role::StandardUser).await;
        let mut config = create_test_config();
        config.integrations.openrouter = serde_json::from_value(json!({
            "account": account.username,
            "provider_slug": "example-provider",
            "api_base_url": openrouter.uri(),
            "refresh_coalesce": "50ms",
        }))
        .unwrap();
        config.integrations.openrouter.provider_api_key = Some("test-provider-key".to_string());
        configure(&mut config);
        let (server, services) = create_test_app_with_config(pool.clone(), config, false).await;
        let admin = create_test_admin_user(pool, Role::PlatformManager).await;
        let key = create_test_api_key_for_user(pool, account.id).await.secret;
        let model = create_test_deployment(pool, admin.id, MODEL, MODEL).await.id;
        add_deployment_to_group(pool, model, Uuid::nil(), admin.id).await;
        sqlx::query("UPDATE deployed_models SET openrouter = '{\"is_ready\": true}'::jsonb WHERE id = $1")
            .bind(model)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO model_tariffs (deployed_model_id, name, input_price_per_token, output_price_per_token, api_key_purpose)
             VALUES ($1, 'list', 0.000001, 0.000003, 'realtime')",
        )
        .bind(model)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO model_cache_tariffs (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, read_multiplier, min_prefix_tokens)
             VALUES ($1, 1.25, 2, 2.5, 0.2, 1024)",
        )
        .bind(model)
        .execute(pool)
        .await
        .unwrap();
        Self {
            server,
            _services: services,
            pool: pool.clone(),
            admin,
            account,
            key,
            model,
            openrouter,
        }
    }

    fn prices_path(&self) -> String {
        format!(
            "/admin/api/v1/accounts/{}/models/{}/prices",
            self.account.username,
            MODEL.replace('/', "%2F")
        )
    }

    async fn put(&self, body: Value, key: Option<&str>) -> axum_test::TestResponse {
        let mut request = self.server.put(&self.prices_path()).json(&body);
        for (name, value) in add_auth_headers(&self.admin) {
            request = request.add_header(name, value);
        }
        if let Some(key) = key {
            request = request.add_header("idempotency-key", key);
        }
        request.await
    }

    async fn delete(&self, query: &str) -> axum_test::TestResponse {
        let mut request = self.server.delete(&format!("{}?{query}", self.prices_path()));
        for (name, value) in add_auth_headers(&self.admin) {
            request = request.add_header(name, value);
        }
        request.await
    }

    async fn feed_prices(&self) -> Option<UnitPrices> {
        let response = self
            .server
            .get(FEED)
            .add_header("authorization", format!("Bearer {}", self.key))
            .await;
        response.assert_status_ok();
        let feed: Value = response.json();
        crate::openrouter::schema_check::assert_valid(&feed);
        let document = feed["data"].as_array().unwrap().iter().find(|d| d["id"] == MODEL).unwrap().clone();
        let cost = |modality: &Value, kind: &str| {
            modality["pricing"]
                .as_array()?
                .iter()
                .find(|price| price["type"] == kind)
                .map(|price| Decimal::from_str(price["cost_usd"].as_str().unwrap()).unwrap())
        };
        let prompt = cost(&document["input_modalities"][0], "prompt")?;
        Some(UnitPrices {
            prompt,
            cached: cost(&document["input_modalities"][0], "cached_prompt").unwrap_or(prompt),
            output: cost(&document["output_modalities"][0], "completion").unwrap(),
        })
    }

    async fn billed(&self) -> Option<UnitPrices> {
        let book = PriceBook::load(&self.pool, &[self.account.id]).await.unwrap();
        book.unit_prices(
            self.account.id,
            &ApiKeyPurpose::Realtime,
            &PriceQuery {
                model: MODEL,
                completion_window: None,
                batch_created_at: None,
                now: Utc::now(),
                serving_class: "standard",
                cache_reads: Some(CacheReadSource::Module),
            },
        )
    }

    async fn assert_feed_matches_billing(&self) -> UnitPrices {
        let feed = self.feed_prices().await.expect("the model is priced");
        assert_eq!(Some(feed), self.billed().await, "the feed shows what billing charges");
        feed
    }

    async fn refreshes(&self) -> usize {
        self.openrouter.received_requests().await.unwrap().len()
    }

    async fn wait_for_refreshes(&self, calls: usize) {
        for _ in 0..800 {
            if self.refreshes().await >= calls {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("expected {calls} refreshes, saw {}", self.refreshes().await);
    }
}

fn dec(value: &str) -> Decimal {
    Decimal::from_str(value).unwrap()
}

fn num(value: &Value) -> Decimal {
    dec(value.as_str().unwrap_or_else(|| panic!("not a decimal string: {value}")))
}

#[dwctl_test_macros::test]
async fn an_admin_sets_a_price_that_the_feed_shows_and_billing_charges(pool: PgPool) {
    let f = Fixture::new(&pool, 200, |_| {}).await;
    let before = f.assert_feed_matches_billing().await;
    assert_eq!(before.prompt, dec("0.000001"));

    let body = json!({
        "input_per_million_tokens": "0.40",
        "output_per_million_tokens": "1.60",
        "cache_read_multiplier": "0.1",
        "reason": "test price",
    });
    let response = f.put(body.clone(), Some("change-1")).await;
    response.assert_status_ok();
    let first: Value = response.json();
    assert_eq!(num(&first["previous"]["input_price_per_token"]), dec("0.000001"));
    assert_eq!(num(&first["price"]["input_price_per_token"]), dec("0.00000040"));
    assert_eq!(num(&first["price"]["cache_read_multiplier"]), dec("0.1"));
    assert!(first["change_id"].is_string());

    let after = f.assert_feed_matches_billing().await;
    assert_eq!(
        (after.prompt, after.cached, after.output),
        (dec("0.0000004"), dec("0.00000004"), dec("0.0000016"))
    );
    f.wait_for_refreshes(1).await;

    let retry = f.put(body.clone(), Some("change-1")).await;
    retry.assert_status_ok();
    assert_eq!(retry.json::<Value>(), first);
    let (api_rows, changes): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM model_tariffs WHERE provisioning_source = 'admin-api'),
                (SELECT COUNT(*) FROM account_price_changes)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((api_rows, changes), (1, 1));
    let mut other = body.clone();
    other["input_per_million_tokens"] = json!("0.50");
    f.put(other, Some("change-1")).await.assert_status(StatusCode::CONFLICT);

    let mut second = body.clone();
    second["input_per_million_tokens"] = json!("0.50");
    second.as_object_mut().unwrap().remove("cache_read_multiplier");
    f.put(second, None).await.assert_status_ok();
    let after = f.assert_feed_matches_billing().await;
    assert_eq!(after.prompt, dec("0.0000005"));
    assert_eq!(after.cached, dec("0.0000001"), "the model's own read multiplier applies again");
    let audit: (String, Decimal, Decimal, String, Uuid) = sqlx::query_as(
        "SELECT action, old_input_price_per_token, new_input_price_per_token, reason, changed_by
         FROM account_price_changes ORDER BY changed_at DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        audit,
        ("set".into(), dec("0.0000004"), dec("0.0000005"), "test price".into(), f.admin.id)
    );

    let ended = f.delete("reason=done").await;
    ended.assert_status_ok();
    assert!(ended.json::<Value>()["change_id"].is_string());
    assert_eq!(f.assert_feed_matches_billing().await, before);
    let nothing = f.delete("reason=again").await;
    nothing.assert_status_ok();
    assert!(nothing.json::<Value>()["change_id"].is_null(), "nothing left to end");
    let history: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM model_tariffs WHERE provisioning_source = 'admin-api'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(history, 2, "ended prices stay as history");

    let get = f
        .server
        .get(&f.prices_path())
        .add_header(add_auth_headers(&f.admin)[0].0.clone(), add_auth_headers(&f.admin)[0].1.clone())
        .add_header(add_auth_headers(&f.admin)[1].0.clone(), add_auth_headers(&f.admin)[1].1.clone())
        .await;
    get.assert_status_ok();
    assert_eq!(num(&get.json::<Value>()["price"]["input_price_per_token"]), dec("0.000001"));
}

#[dwctl_test_macros::test]
async fn only_admins_may_set_prices(pool: PgPool) {
    let f = Fixture::new(&pool, 200, |_| {}).await;
    let mut request = f.server.put(&f.prices_path()).json(&json!({
        "input_per_million_tokens": "0.40", "output_per_million_tokens": "1.60", "reason": "x",
    }));
    for (name, value) in add_auth_headers(&f.account) {
        request = request.add_header(name, value);
    }
    request.await.assert_status(StatusCode::FORBIDDEN);
}

#[dwctl_test_macros::test]
async fn a_failed_refresh_never_fails_the_write(pool: PgPool) {
    let f = Fixture::new(&pool, 503, |_| {}).await;
    f.put(
        json!({"input_per_million_tokens": "0.40", "output_per_million_tokens": "1.60", "reason": "x"}),
        None,
    )
    .await
    .assert_status_ok();
    f.wait_for_refreshes(1).await;
    assert_eq!(f.feed_prices().await.unwrap().prompt, dec("0.0000004"));
}

#[dwctl_test_macros::test]
async fn guards_refuse_unsafe_prices(pool: PgPool) {
    let f = Fixture::new(&pool, 200, |config| {
        config.account_prices = serde_json::from_value(json!({
            "floors": {MODEL: {"input_per_million_tokens": "0.10", "output_per_million_tokens": "0.40"}}
        }))
        .unwrap();
    })
    .await;
    let price = |input: &str, output: &str| json!({"input_per_million_tokens": input, "output_per_million_tokens": output, "reason": "x"});
    for (body, status) in [
        (price("0.05", "1"), StatusCode::UNPROCESSABLE_ENTITY),
        (price("0", "1"), StatusCode::UNPROCESSABLE_ENTITY),
        (price("-1", "1"), StatusCode::BAD_REQUEST),
        (price("0.000000001", "1"), StatusCode::BAD_REQUEST),
        (
            json!({"input_per_million_tokens": "1", "output_per_million_tokens": "1", "reason": "x", "valid_from": "2020-01-01T00:00:00Z"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"input_per_million_tokens": "1", "output_per_million_tokens": "1", "reason": " "}),
            StatusCode::BAD_REQUEST,
        ),
        (
            json!({"input_per_million_tokens": "1", "output_per_million_tokens": "1", "reason": "x", "cache_read_multiplier": "100"}),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let response = f.put(body.clone(), None).await;
        assert_eq!(response.status_code(), status, "{body}: {}", response.text());
    }
    assert_eq!(f.refreshes().await, 0, "no refresh for a refused write");
}

#[dwctl_test_macros::test]
async fn a_price_from_yaml_is_not_overridden(pool: PgPool) {
    let f = Fixture::new(&pool, 200, |_| {}).await;
    sqlx::query(
        "INSERT INTO model_tariffs (deployed_model_id, user_id, serving_class, name, input_price_per_token, output_price_per_token, api_key_purpose, provisioning_source)
         VALUES ($1, $2, 'standard', 'deal', 0.000002, 0.000002, 'realtime', 'account-catalog')",
    )
    .bind(f.model)
    .bind(f.account.id)
    .execute(&pool)
    .await
    .unwrap();
    let price = |class: Option<&str>| json!({"input_per_million_tokens": "0.40", "output_per_million_tokens": "1.60", "reason": "x", "serving_class": class});
    for class in [None, Some("standard")] {
        let response = f.put(price(class), None).await;
        response.assert_status(StatusCode::CONFLICT);
        assert!(response.text().contains("an account file"), "{}", response.text());
    }
    f.put(price(Some("fast")), None).await.assert_status_ok();
    assert_eq!(
        f.assert_feed_matches_billing().await.prompt,
        dec("0.000002"),
        "the standard class price stays"
    );
}

#[dwctl_test_macros::test]
async fn the_feed_shows_the_class_price_billing_charges(pool: PgPool) {
    let f = Fixture::new(&pool, 200, |_| {}).await;
    sqlx::query(
        "INSERT INTO model_serving_classes (deployed_model_id, class_key, display_name, inference_endpoint_id, upstream_model_name)
         SELECT $1, 'standard', 'standard', hosted_on, model_name FROM deployed_models WHERE id = $1",
    )
    .bind(f.model)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE deployed_models SET routing_mode = 'class_routes' WHERE id = $1")
        .bind(f.model)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO model_tariffs (deployed_model_id, serving_class, name, input_price_per_token, output_price_per_token, api_key_purpose)
         VALUES ($1, 'standard', 'standard', 0.0000015, 0.0000045, 'realtime')",
    )
    .bind(f.model)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO model_cache_tariffs (deployed_model_id, user_id, serving_class, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, read_multiplier, min_prefix_tokens)
         VALUES ($1, $2, 'standard', 1.25, 2, 2.5, 0.05, 1024)",
    )
    .bind(f.model)
    .bind(f.account.id)
    .execute(&pool)
    .await
    .unwrap();
    let prices = f.assert_feed_matches_billing().await;
    assert_eq!(
        (prices.prompt, prices.cached, prices.output),
        (dec("0.0000015"), dec("0.000000075"), dec("0.0000045")),
        "the class price and the account's class multiplier"
    );

    f.put(
        json!({"input_per_million_tokens": "0.80", "output_per_million_tokens": "2.40", "serving_class": "standard", "reason": "x"}),
        None,
    )
    .await
    .assert_status_ok();
    assert_eq!(f.assert_feed_matches_billing().await.prompt, dec("0.0000008"));

    let sql: (Decimal, Decimal) = sqlx::query_as(
        "SELECT input_price_per_token, output_price_per_token FROM effective_model_tariff($1, $2, 'realtime', NULL, 'standard', NOW())",
    )
    .bind(f.model)
    .bind(f.account.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(sql, (dec("0.0000008"), dec("0.0000024")));
}

#[dwctl_test_macros::test]
async fn a_future_price_starts_at_its_time(pool: PgPool) {
    let f = Fixture::new(&pool, 200, |_| {}).await;
    let starts = Utc::now() + chrono::Duration::hours(1);
    let response = f
        .put(
            json!({"input_per_million_tokens": "0.40", "output_per_million_tokens": "1.60", "reason": "x", "valid_from": starts}),
            None,
        )
        .await;
    response.assert_status_ok();
    assert_eq!(num(&response.json::<Value>()["price"]["input_price_per_token"]), dec("0.00000040"));
    assert_eq!(f.assert_feed_matches_billing().await.prompt, dec("0.000001"), "not yet");

    f.put(
        json!({"input_per_million_tokens": "0.50", "output_per_million_tokens": "1.60", "reason": "x", "valid_from": starts}),
        None,
    )
    .await
    .assert_status_ok();
    let scheduled: Vec<Decimal> = sqlx::query_scalar(
        "SELECT input_price_per_token FROM model_tariffs WHERE provisioning_source = 'admin-api' AND valid_from > NOW()",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(scheduled, vec![dec("0.0000005")]);
}

#[dwctl_test_macros::test]
async fn a_retried_delete_never_ends_a_later_price(pool: PgPool) {
    let f = Fixture::new(&pool, 200, |_| {}).await;
    let delete = |key: &'static str| {
        let mut request = f.server.delete(&format!("{}?reason=stop", f.prices_path()));
        for (name, value) in add_auth_headers(&f.admin) {
            request = request.add_header(name, value);
        }
        request.add_header("idempotency-key", key)
    };
    let first = delete("stop-1").await;
    first.assert_status_ok();
    assert!(first.json::<Value>()["change_id"].is_null());
    f.put(
        json!({"input_per_million_tokens": "0.40", "output_per_million_tokens": "1.60", "reason": "x"}),
        None,
    )
    .await
    .assert_status_ok();
    let retry = delete("stop-1").await;
    retry.assert_status_ok();
    assert_eq!(retry.json::<Value>(), first.json::<Value>());
    assert_eq!(f.assert_feed_matches_billing().await.prompt, dec("0.0000004"));
}

#[dwctl_test_macros::test]
async fn a_price_set_for_later_refreshes_again_when_it_starts(pool: PgPool) {
    let f = Fixture::new(&pool, 200, |_| {}).await;
    let starts = Utc::now() + chrono::Duration::milliseconds(1500);
    f.put(
        json!({"input_per_million_tokens": "0.40", "output_per_million_tokens": "1.60", "reason": "x", "valid_from": starts}),
        None,
    )
    .await
    .assert_status_ok();
    f.wait_for_refreshes(1).await;
    assert_eq!(f.feed_prices().await.unwrap().prompt, dec("0.000001"), "not yet in effect");
    f.wait_for_refreshes(2).await;
    assert!(Utc::now() > starts);
    assert_eq!(f.assert_feed_matches_billing().await.prompt, dec("0.0000004"));
}

#[dwctl_test_macros::test]
async fn a_class_less_cache_price_never_changes_a_class_s_own_write_prices(pool: PgPool) {
    let f = Fixture::new(&pool, 200, |_| {}).await;
    sqlx::query(
        "INSERT INTO model_serving_classes (deployed_model_id, class_key, display_name, inference_endpoint_id, upstream_model_name)
         SELECT $1, c, c, hosted_on, model_name FROM deployed_models, (VALUES ('standard'), ('fast')) v(c) WHERE id = $1",
    )
    .bind(f.model)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("UPDATE deployed_models SET routing_mode = 'class_routes' WHERE id = $1")
        .bind(f.model)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO model_cache_tariffs (deployed_model_id, serving_class, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, read_multiplier, min_prefix_tokens)
         VALUES ($1, 'fast', 2, 3, 4, 0.2, 1024)",
    )
    .bind(f.model)
    .execute(&pool)
    .await
    .unwrap();
    let price = |class: Option<&str>| {
        json!({"input_per_million_tokens": "0.40", "output_per_million_tokens": "1.60", "cache_read_multiplier": "0.1",
               "serving_class": class, "reason": "x"})
    };
    let refused = f.put(price(None), None).await;
    refused.assert_status(StatusCode::UNPROCESSABLE_ENTITY);
    assert!(refused.text().contains("per serving_class"), "{}", refused.text());
    f.put(price(Some("standard")), None).await.assert_status_ok();
    assert_eq!(f.assert_feed_matches_billing().await.cached, dec("0.00000004"));
}

#[dwctl_test_macros::test]
async fn the_openrouter_price_manager_prices_only_openrouter_s_account(pool: PgPool) {
    let f = Fixture::new(&pool, 200, |_| {}).await;
    let other = create_test_user(&pool, Role::StandardUser).await;
    let secrets: Vec<String> = (0..2).map(|_| crate::crypto::generate_api_key()).collect();
    let hex = |s: &str| -> String { crate::crypto::hash_api_key(s).iter().map(|b| format!("{b:02x}")).collect() };
    let file = json!({"service_accounts": [
        {"id": Uuid::new_v4(), "name": "Price controller", "role": "OpenRouterPriceManager", "key_hashes": [hex(&secrets[0])]},
        {"id": Uuid::new_v4(), "name": "Queue scaler", "role": "QueueDemandReader", "key_hashes": [hex(&secrets[1])]},
    ]});
    crate::service_accounts::apply(&pool, &crate::service_accounts::parse(&file.to_string()).unwrap())
        .await
        .unwrap();
    let (prices, demand) = (&secrets[0], &secrets[1]);
    let body = json!({"input_per_million_tokens": "0.40", "output_per_million_tokens": "1.60", "reason": "feed"});
    let path_for = |account: &str| format!("/admin/api/v1/accounts/{account}/models/{}/prices", MODEL.replace('/', "%2F"));

    let send = |method: &str, path: String, secret: &str| {
        let request = match method {
            "GET" => f.server.get(&path),
            "PUT" => f.server.put(&path).json(&body),
            _ => f.server.delete(&format!("{path}?reason=feed")),
        };
        request.add_header("authorization", format!("Bearer {secret}"))
    };

    send("PUT", path_for(&f.account.username), prices).await.assert_status_ok();
    send("GET", path_for(&f.account.username), prices).await.assert_status_ok();
    send("PUT", path_for(&f.account.id.to_string()), prices).await.assert_status_ok();
    for method in ["GET", "PUT", "DELETE"] {
        let status = send(method, path_for(&other.username), prices).await.status_code();
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} on another account");
        let status = send(method, path_for("no-such-account"), prices).await.status_code();
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} on a missing account");
        let status = send(method, path_for(&f.account.username), demand).await.status_code();
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} without the role");
    }
    send("DELETE", path_for(&f.account.username), prices).await.assert_status_ok();
}
