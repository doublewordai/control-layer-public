
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    api::models::users::Role,
    openrouter::schema_check::assert_valid,
    test::utils::{
        add_deployment_to_group, create_test_admin_user, create_test_api_key_for_user, create_test_app, create_test_app_with_config,
        create_test_config, create_test_deployment, create_test_group, create_test_user,
    },
    types::DeploymentId,
};

const FEED: &str = "/ai/v1/integrations/openrouter/models";

async fn list(pool: &PgPool, model: DeploymentId, listing: Value) {
    sqlx::query("UPDATE deployed_models SET openrouter = $1 WHERE id = $2")
        .bind(listing)
        .bind(model)
        .execute(pool)
        .await
        .unwrap();
}

#[dwctl_test_macros::test]
async fn openrouter_feed_lists_listed_models_with_the_accounts_prices(pool: PgPool) {
    let mut config = create_test_config();
    config.onwards.rejected_params = vec!["n".to_string(), "logprobs".to_string(), "top_logprobs".to_string()];
    config
        .onwards
        .model_allowed_params
        .insert("example-org/Allowed".to_string(), vec!["logprobs".to_string()]);
    config.integrations = serde_json::from_value(json!({"openrouter": {"datacenters": [{"country_code": "US"}]}})).unwrap();
    let (server, _services) = create_test_app_with_config(pool.clone(), config, false).await;
    let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
    let user = create_test_user(&pool, Role::StandardUser).await;
    let key = create_test_api_key_for_user(&pool, user.id).await;
    let private_group = create_test_group(&pool).await;

    let live = create_test_deployment(&pool, admin.id, "example-org/Live", "example-org/Live").await;
    let staged = create_test_deployment(&pool, admin.id, "example-org/Staged", "example-org/Staged").await;
    let allowed = create_test_deployment(&pool, admin.id, "example-org/Allowed", "example-org/Allowed").await;
    let unlisted = create_test_deployment(&pool, admin.id, "example-org/Unlisted", "example-org/Unlisted").await;
    let private = create_test_deployment(&pool, admin.id, "example-org/Private", "example-org/Private").await;
    for model in [live.id, staged.id, allowed.id, unlisted.id] {
        add_deployment_to_group(&pool, model, Uuid::nil(), admin.id).await;
    }
    add_deployment_to_group(&pool, private.id, private_group.id, admin.id).await;

    sqlx::query(
        "UPDATE deployed_models SET display_name = 'Live 1', capabilities = ARRAY['reasoning'],
         metadata = '{\"context_window\": 262144, \"quantization\": \"fp8\"}'::jsonb WHERE id = $1",
    )
    .bind(live.id)
    .execute(&pool)
    .await
    .unwrap();
    list(&pool, live.id, json!({"hugging_face_id": "example-org/Live", "is_ready": true})).await;
    list(&pool, staged.id, json!({})).await;
    list(&pool, allowed.id, json!({"is_ready": true})).await;
    list(&pool, private.id, json!({"is_ready": true})).await;

    for (account, purpose, window, input, output) in [
        (None, "realtime", None, 3, 9),
        (Some(user.id), "realtime", None, 12, 38),
        (None, "batch", Some("24h"), 1, 1),
    ] {
        sqlx::query(
            "INSERT INTO model_tariffs (deployed_model_id, user_id, name, input_price_per_token, output_price_per_token, api_key_purpose, completion_window)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(live.id)
        .bind(account)
        .bind(format!("{purpose}-{}", account.is_some()))
        .bind(Decimal::new(input, 7))
        .bind(Decimal::new(output, 7))
        .bind(purpose)
        .bind(window)
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "INSERT INTO model_cache_tariffs (deployed_model_id, write_multiplier_5m, write_multiplier_1h, write_multiplier_24h, read_multiplier, min_prefix_tokens)
         VALUES ($1, 1.25, 2, 2.5, 0.2, 1024)",
    )
    .bind(live.id)
    .execute(&pool)
    .await
    .unwrap();

    let response = server.get(FEED).add_header("authorization", format!("Bearer {}", key.secret)).await;
    response.assert_status_ok();
    let feed: Value = response.json();
    assert_valid(&feed);
    assert_eq!(feed.as_object().unwrap().len(), 1, "the feed body is only `data`");
    let documents = feed["data"].as_array().unwrap();
    let ids: Vec<&str> = documents.iter().map(|document| document["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids,
        ["example-org/Allowed", "example-org/Live", "example-org/Staged"],
        "unlisted models and models the account cannot use stay out"
    );

    let live = &documents[1];
    assert_eq!(live["name"], "Live 1");
    assert_eq!(live["hugging_face_id"], "example-org/Live", "the stored listing is read");
    assert_eq!(live["quantization"], "fp8");
    assert_eq!(live["datacenters"], json!([{"country_code": "US"}]));
    let text = &live["input_modalities"][0];
    assert_eq!(text["supported_inputs"]["max_context_length"]["value"], 262144);
    let input_prices: Vec<(&str, &str)> = text["pricing"]
        .as_array()
        .unwrap()
        .iter()
        .map(|price| (price["type"].as_str().unwrap(), price["cost_usd"].as_str().unwrap()))
        .collect();
    assert_eq!(input_prices[0], ("prompt", "0.0000012"), "the account's own realtime price");
    assert_eq!(
        input_prices.iter().map(|(kind, _)| *kind).collect::<Vec<_>>(),
        ["prompt", "cached_prompt", "cache_write", "cache_write", "cache_write"],
        "the model's cache tariff is read"
    );
    let output = &live["output_modalities"][0];
    assert_eq!(output["pricing"][0]["cost_usd"], "0.0000038");
    let parameters = output["supported_parameters"].as_object().unwrap();
    assert!(parameters.contains_key("reasoning"), "the model has the reasoning capability");
    for refused in ["n", "logprobs", "top_logprobs"] {
        assert!(!parameters.contains_key(refused), "{refused} is rejected by the gateway");
    }

    let allowed = &documents[0];
    let parameters = &allowed["output_modalities"][0]["supported_parameters"];
    assert_eq!(
        parameters["logprobs"],
        json!({"type": "boolean"}),
        "a model's allowed exception is declared"
    );
    assert!(parameters.get("top_logprobs").is_none());

    let staged = &documents[2];
    assert_eq!(staged["is_ready"], false, "a listing is not ready until it says so");
    assert!(
        staged["input_modalities"][0].get("pricing").is_none(),
        "no realtime price, no pricing"
    );
}

#[dwctl_test_macros::test]
async fn openrouter_feed_needs_an_inference_key(pool: PgPool) {
    let (server, _services) = create_test_app(pool.clone(), false).await;
    let user = create_test_user(&pool, Role::StandardUser).await;
    let key = create_test_api_key_for_user(&pool, user.id).await;

    for authorization in [None, Some("Bearer unknown-key".to_string())] {
        let mut request = server.get(FEED);
        if let Some(header) = authorization {
            request = request.add_header("authorization", header);
        }
        request.await.assert_status_unauthorized();
    }

    let response = server.get(FEED).add_header("authorization", format!("Bearer {}", key.secret)).await;
    response.assert_status_ok();
    assert_eq!(response.json::<Value>(), json!({"data": []}));

    sqlx::query("UPDATE api_keys SET purpose = 'platform' WHERE id = $1")
        .bind(key.id)
        .execute(&pool)
        .await
        .unwrap();
    server
        .get(FEED)
        .add_header("authorization", format!("Bearer {}", key.secret))
        .await
        .assert_status_unauthorized();
}

#[dwctl_test_macros::test]
async fn openrouter_feed_fails_rather_than_drop_a_model_it_cannot_read(pool: PgPool) {
    let (server, _services) = create_test_app(pool.clone(), false).await;
    let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
    let user = create_test_user(&pool, Role::StandardUser).await;
    let key = create_test_api_key_for_user(&pool, user.id).await;
    let good = create_test_deployment(&pool, admin.id, "example-org/Good", "example-org/Good").await;
    let bad = create_test_deployment(&pool, admin.id, "example-org/Bad", "example-org/Bad").await;
    for model in [good.id, bad.id] {
        add_deployment_to_group(&pool, model, Uuid::nil(), admin.id).await;
    }
    list(&pool, good.id, json!({"is_ready": true})).await;
    list(&pool, bad.id, json!({"is_ready": "yes"})).await;

    server
        .get(FEED)
        .add_header("authorization", format!("Bearer {}", key.secret))
        .await
        .assert_status_internal_server_error();

    list(&pool, bad.id, json!({"is_ready": false})).await;
    let response = server.get(FEED).add_header("authorization", format!("Bearer {}", key.secret)).await;
    response.assert_status_ok();
    assert_eq!(response.json::<Value>()["data"].as_array().unwrap().len(), 2);
}
