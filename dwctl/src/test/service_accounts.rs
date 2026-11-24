
use axum::http::StatusCode;
use axum_test::TestServer;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    api::models::users::{AuthMethod, CurrentUser, Role},
    crypto::{generate_api_key, hash_api_key},
    service_accounts::{apply, parse},
    test::utils::{add_deployment_to_group, create_test_admin_user, create_test_app, create_test_config, create_test_deployment},
};

async fn service_accounts(pool: &PgPool, roles: &[&str]) -> Vec<(Uuid, String)> {
    let accounts: Vec<(Uuid, String)> = roles.iter().map(|_| (Uuid::new_v4(), generate_api_key())).collect();
    let entries: Vec<Value> = roles
        .iter()
        .zip(&accounts)
        .map(|(role, (id, secret))| {
            let hash: String = hash_api_key(secret).iter().map(|b| format!("{b:02x}")).collect();
            json!({"id": id, "name": format!("{role} service"), "role": role, "key_hashes": [hash]})
        })
        .collect();
    let text = json!({ "service_accounts": entries }).to_string();
    apply(pool, &parse(&text).unwrap()).await.unwrap();
    accounts
}

async fn get(server: &TestServer, path: &str, secret: &str) -> axum_test::TestResponse {
    server.get(path).add_header("authorization", format!("Bearer {secret}")).await
}

#[dwctl_test_macros::test]
async fn a_demand_key_reaches_only_the_demand_endpoint(pool: PgPool) {
    let (server, _bg) = create_test_app(pool.clone(), false).await;
    let accounts = service_accounts(&pool, &["QueueDemandReader"]).await;
    let [(_, demand)] = &accounts[..] else { unreachable!() };

    get(&server, "/admin/api/v1/monitoring/demand?window=1h", demand)
        .await
        .assert_status_ok();
    for path in [
        "/admin/api/v1/users",
        "/admin/api/v1/models",
        "/admin/api/v1/endpoints",
        "/admin/openapi.json",
    ] {
        let status = get(&server, path, demand).await.status_code();
        assert_eq!(status, StatusCode::FORBIDDEN, "{path}");
    }
}

#[dwctl_test_macros::test]
async fn catalogue_keys_see_public_or_every_model(pool: PgPool) {
    let (server, _bg) = create_test_app(pool.clone(), false).await;
    let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
    let public = create_test_deployment(&pool, admin.id, "example/public", "example/public").await;
    add_deployment_to_group(&pool, public.id, Uuid::nil(), admin.id).await;
    create_test_deployment(&pool, admin.id, "example/private", "example/private").await;
    let accounts = service_accounts(&pool, &["CatalogueReader", "CatalogueReaderAll"]).await;
    let [(_, reader), (_, reader_all)] = &accounts[..] else {
        unreachable!()
    };

    let aliases = |response: axum_test::TestResponse| -> Vec<String> {
        response.assert_status_ok();
        let mut aliases: Vec<String> = response.json::<Value>()["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["alias"].as_str().unwrap().to_string())
            .collect();
        aliases.sort();
        aliases
    };
    assert_eq!(aliases(get(&server, "/admin/api/v1/models", reader).await), vec!["example/public"]);
    assert_eq!(
        aliases(get(&server, "/admin/api/v1/models", reader_all).await),
        vec!["example/private", "example/public"]
    );
    for secret in [reader, reader_all] {
        get(&server, "/admin/openapi.json", secret).await.assert_status_ok();
        let status = get(&server, "/admin/api/v1/monitoring/demand?window=1h", secret)
            .await
            .status_code();
        assert_eq!(status, StatusCode::FORBIDDEN);
        let status = get(&server, "/admin/api/v1/endpoints", secret).await.status_code();
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
}

#[dwctl_test_macros::test]
async fn a_service_account_never_signs_in(pool: PgPool) {
    let (server, _bg) = create_test_app(pool.clone(), false).await;
    let [(id, _)] = service_accounts(&pool, &["QueueDemandReader"]).await[..] else {
        unreachable!()
    };
    let config = create_test_config();

    let as_user = CurrentUser {
        auth_method: AuthMethod::NativeSession,
        id,
        username: format!("svc-{id}"),
        email: format!("svc-{id}@service-accounts.invalid"),
        is_admin: false,
        roles: vec![Role::QueueDemandReader],
        display_name: None,
        avatar_url: None,
        payment_provider_id: None,
        organizations: vec![],
        active_organization: None,
        api_key_id: None,
    };
    let token = crate::auth::session::create_session_token(&as_user, &config).unwrap();
    let response = server
        .get("/admin/api/v1/monitoring/demand?window=1h")
        .add_header("cookie", format!("{}={token}", config.auth.native.session.cookie_name))
        .await;
    response.assert_status(StatusCode::UNAUTHORIZED);

    let response = server
        .get("/admin/api/v1/users/current")
        .add_header(config.auth.proxy_header.header_name.clone(), format!("svc-{id}"))
        .add_header(
            config.auth.proxy_header.email_header_name.clone(),
            format!("svc-{id}@service-accounts.invalid"),
        )
        .await;
    if response.status_code() == StatusCode::OK {
        assert_ne!(response.json::<Value>()["id"].as_str(), Some(id.to_string().as_str()));
    }

    let set_password = sqlx::query("UPDATE users SET password_hash = 'x' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await;
    assert!(set_password.is_err());
}

#[dwctl_test_macros::test]
async fn only_a_service_account_s_own_platform_key_authenticates_it(pool: PgPool) {
    let (server, _bg) = create_test_app(pool.clone(), false).await;
    let [(id, _)] = service_accounts(&pool, &["QueueDemandReader"]).await[..] else {
        unreachable!()
    };
    let secret = generate_api_key();
    sqlx::query("INSERT INTO api_keys (name, purpose, user_id, created_by, secret_hash) VALUES ('x', 'realtime', $1, $1, $2)")
        .bind(id)
        .bind(&hash_api_key(&secret)[..])
        .execute(&pool)
        .await
        .unwrap();
    for path in ["/admin/api/v1/monitoring/demand?window=1h", "/admin/openapi.json"] {
        get(&server, path, &secret).await.assert_status(StatusCode::UNAUTHORIZED);
    }
}

#[dwctl_test_macros::test]
async fn service_roles_cannot_be_assigned_through_the_users_api(pool: PgPool) {
    let (server, _bg) = create_test_app(pool.clone(), false).await;
    let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
    let mut request = server.post("/admin/api/v1/users").json(&json!({
        "username": "someone", "email": "someone@example.com", "roles": ["StandardUser", "QueueDemandReader"],
    }));
    for (name, value) in crate::test::utils::add_auth_headers(&admin) {
        request = request.add_header(name, value);
    }
    let status = request.await.status_code();
    assert!(status == StatusCode::BAD_REQUEST || status == StatusCode::FORBIDDEN, "{status}");
}
