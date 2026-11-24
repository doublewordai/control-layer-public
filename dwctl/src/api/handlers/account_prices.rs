
use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sqlx::{PgConnection, Row};
use sqlx_pool_router::PoolProvider;
use uuid::Uuid;

use crate::{
    AppState,
    api::models::account_prices::{AccountPriceResponse, EndAccountPriceQuery, GetAccountPriceQuery, SetAccountPriceRequest},
    api::models::users::CurrentUser,
    auth::permissions::has_permission,
    db::handlers::{
        AccountPrices,
        account_prices::{ChangeRecord, CompetingRow, STANDARD_CLASS},
    },
    errors::{Error, Result},
    model_provisioning::{CacheTariff, Tariff, TariffPurpose, parse_decimal, parse_per_million, validate_cache_tariff, validate_tariffs},
    pricing::{BilledPrice, billed_realtime_prices},
    types::{Operation, Permission, Resource},
};

const IDEMPOTENCY_KEY: &str = "idempotency-key";

struct Target {
    account: Uuid,
    username: String,
    model: Uuid,
    alias: String,
}

#[utoipa::path(
    get,
    path = "/accounts/{account}/models/{model}/prices",
    tag = "pricing",
    summary = "Get an account's realtime price for a model",
    description = "The price billing charges the account now for a realtime request to the model in the class.",
    params(
        ("account" = String, Path, description = "Account id or username"),
        ("model" = String, Path, description = "Model id or alias (URL-encoded)"),
        GetAccountPriceQuery,
    ),
    responses(
        (status = 200, body = AccountPriceResponse),
        (status = 403, description = "Requires pricing management access"),
        (status = 404, description = "Account or model not found"),
    ),
    security(("BearerAuth" = []), ("CookieAuth" = []), ("X-Doubleword-User" = []))
)]
#[tracing::instrument(skip_all)]
pub async fn get_account_price<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((account, model)): Path<(String, String)>,
    Query(query): Query<GetAccountPriceQuery>,
    user: CurrentUser,
) -> Result<Json<AccountPriceResponse>> {
    may_price_some_account(&user, Operation::ReadAll)?;
    let class = checked_class(query.serving_class.as_deref())?;
    let mut conn = state.db.write().acquire().await.map_err(db_error)?;
    let target = resolve(&mut conn, &user, Operation::ReadAll, &state, &account, &model).await?;
    let now = Utc::now();
    let price = billed(&mut conn, &target, class, now).await?;
    Ok(Json(response(&target, class, now, None, price, None)))
}

#[utoipa::path(
    put,
    path = "/accounts/{account}/models/{model}/prices",
    tag = "pricing",
    summary = "Set an account's realtime price for a model",
    description = "Ends the price this API set before for the same account, model and class, and starts the new one at \
                   `valid_from` (now by default). Refused where the price comes from an account file, an organisation \
                   overlay or a hand-written row. Send an `Idempotency-Key` header to make retries safe.",
    params(
        ("account" = String, Path, description = "Account id or username"),
        ("model" = String, Path, description = "Model id or alias (URL-encoded)"),
        ("Idempotency-Key" = Option<String>, Header, description = "Retries with the same key return the first answer"),
    ),
    request_body = SetAccountPriceRequest,
    responses(
        (status = 200, description = "The price billing charges from `at`, and the one before", body = AccountPriceResponse),
        (status = 400, description = "Invalid price"),
        (status = 403, description = "Requires pricing management access"),
        (status = 404, description = "Account or model not found"),
        (status = 409, description = "The price is owned elsewhere, or the key was used for another request"),
        (status = 422, description = "Below the model's floor, zero without allow_zero, or no cache pricing to adjust"),
    ),
    security(("BearerAuth" = []), ("CookieAuth" = []), ("X-Doubleword-User" = []))
)]
#[tracing::instrument(skip_all)]
pub async fn set_account_price<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((account, model)): Path<(String, String)>,
    headers: HeaderMap,
    user: CurrentUser,
    Json(request): Json<SetAccountPriceRequest>,
) -> Result<Json<Value>> {
    may_price_some_account(&user, Operation::UpdateAll)?;
    let request_id = idempotency_key(&headers)?;
    let class = checked_class(request.serving_class.as_deref())?;
    let reason = checked_reason(&request.reason)?;
    let (input, output) = checked_prices(&request)?;
    let cache_read = request.cache_read_multiplier.as_deref().map(checked_read_multiplier).transpose()?;
    let now = Utc::now();
    let valid_from = request.valid_from.unwrap_or(now);

    let mut tx = state.db.write().begin().await.map_err(db_error)?;
    let target = resolve(&mut tx, &user, Operation::UpdateAll, &state, &account, &model).await?;
    let fingerprint = json!(["PUT", target.account, target.model, request]).to_string();
    let mut prices = AccountPrices::new(&mut tx);
    prices.lock(target.account, target.model).await.map_err(db_error)?;
    if let Some(replay) = replay(&mut prices, request_id.as_deref(), &fingerprint).await? {
        return Ok(Json(replay));
    }
    if valid_from < now {
        return Err(bad_request("valid_from cannot be in the past"));
    }
    check_floor(
        &state.current_config().account_prices,
        &target.alias,
        input,
        output,
        request.allow_zero,
    )?;
    if let Some(row) = prices
        .competing_tariff(target.account, target.model, class, valid_from)
        .await
        .map_err(db_error)?
    {
        return Err(owned_elsewhere(&target, &row));
    }
    if cache_read.is_some()
        && let Some(row) = prices
            .competing_cache(target.account, target.model, class, valid_from)
            .await
            .map_err(db_error)?
    {
        return Err(owned_elsewhere(&target, &row));
    }

    let previous = billed(&mut tx, &target, class, valid_from).await?;
    let mut prices = AccountPrices::new(&mut tx);
    prices
        .end_tariffs(target.account, target.model, class, valid_from)
        .await
        .map_err(db_error)?;
    prices
        .insert_tariff(target.account, target.model, class, input, output, valid_from)
        .await
        .map_err(db_error)?;
    prices
        .end_cache(target.account, target.model, class, valid_from)
        .await
        .map_err(db_error)?;
    if let Some(read) = cache_read {
        let base = prices
            .cache_base(target.account, target.model, class, valid_from)
            .await
            .map_err(db_error)?
            .ok_or_else(|| Error::UnprocessableEntity {
                message: format!(
                    "model {:?} has no cache pricing, so there is no cache read price to set",
                    target.alias
                ),
            })?;
        if class.is_none()
            && prices
                .class_cache_differs(target.account, target.model, base, valid_from)
                .await
                .map_err(db_error)?
        {
            return Err(Error::UnprocessableEntity {
                message: format!(
                    "model {:?} has classes with their own cache write prices; set the cache read multiplier per serving_class",
                    target.alias
                ),
            });
        }
        prices
            .insert_cache(target.account, target.model, class, base, read, valid_from)
            .await
            .map_err(db_error)?;
    }
    let price = billed(&mut tx, &target, class, valid_from).await?;

    let change = ChangeRecord {
        request_id: request_id.as_deref(),
        fingerprint: &fingerprint,
        action: "set",
        account: target.account,
        model: target.model,
        serving_class: class,
        valid_from,
        old: previous,
        new: price,
        reason,
        changed_by: user.id,
    };
    let body = record(&mut tx, &change, |id| {
        response(&target, class, valid_from, previous, price, Some(id))
    })
    .await?;
    tx.commit().await.map_err(db_error)?;
    tracing::info!(
        account = %target.username, model = %target.alias, serving_class = class, %valid_from,
        input = %input, output = %output, cache_read = ?cache_read, reason, changed_by = %user.id,
        "account price set through the admin API"
    );
    after_commit(&state, &target, valid_from);
    Ok(Json(body))
}

#[utoipa::path(
    delete,
    path = "/accounts/{account}/models/{model}/prices",
    tag = "pricing",
    summary = "End the price the API set for an account and model",
    description = "Ends the price this API set for the account, model and class now, so the account falls back to its \
                   normal price. A price from an account file or an overlay is never touched. Answers with the price \
                   billing charges afterwards; `change_id` is null when there was nothing to end.",
    params(
        ("account" = String, Path, description = "Account id or username"),
        ("model" = String, Path, description = "Model id or alias (URL-encoded)"),
        ("Idempotency-Key" = Option<String>, Header, description = "Retries with the same key return the first answer"),
        EndAccountPriceQuery,
    ),
    responses(
        (status = 200, body = AccountPriceResponse),
        (status = 403, description = "Requires pricing management access"),
        (status = 404, description = "Account or model not found"),
        (status = 409, description = "The key was used for another request"),
    ),
    security(("BearerAuth" = []), ("CookieAuth" = []), ("X-Doubleword-User" = []))
)]
#[tracing::instrument(skip_all)]
pub async fn end_account_price<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((account, model)): Path<(String, String)>,
    Query(query): Query<EndAccountPriceQuery>,
    headers: HeaderMap,
    user: CurrentUser,
) -> Result<Json<Value>> {
    may_price_some_account(&user, Operation::UpdateAll)?;
    let request_id = idempotency_key(&headers)?;
    let class = checked_class(query.serving_class.as_deref())?;
    let reason = checked_reason(&query.reason)?;

    let mut tx = state.db.write().begin().await.map_err(db_error)?;
    let target = resolve(&mut tx, &user, Operation::UpdateAll, &state, &account, &model).await?;
    let fingerprint = json!(["DELETE", target.account, target.model, query]).to_string();
    let mut prices = AccountPrices::new(&mut tx);
    prices.lock(target.account, target.model).await.map_err(db_error)?;
    if let Some(replay) = replay(&mut prices, request_id.as_deref(), &fingerprint).await? {
        return Ok(Json(replay));
    }
    let now = Utc::now();
    let previous = billed(&mut tx, &target, class, now).await?;
    let mut prices = AccountPrices::new(&mut tx);
    let ended = prices
        .end_tariffs(target.account, target.model, class, now)
        .await
        .map_err(db_error)?
        + prices.end_cache(target.account, target.model, class, now).await.map_err(db_error)?;
    let price = billed(&mut tx, &target, class, now).await?;
    if ended == 0 && request_id.is_none() {
        tx.rollback().await.map_err(db_error)?;
        let body = response(&target, class, now, previous, price, None);
        return Ok(Json(serde_json::to_value(body).map_err(internal)?));
    }
    let change = ChangeRecord {
        request_id: request_id.as_deref(),
        fingerprint: &fingerprint,
        action: if ended == 0 { "noop" } else { "end" },
        account: target.account,
        model: target.model,
        serving_class: class,
        valid_from: now,
        old: previous,
        new: price,
        reason,
        changed_by: user.id,
    };
    let body = record(&mut tx, &change, |id| {
        response(&target, class, now, previous, price, (ended > 0).then_some(id))
    })
    .await?;
    tx.commit().await.map_err(db_error)?;
    if ended > 0 {
        tracing::info!(
            account = %target.username, model = %target.alias, serving_class = class, reason, changed_by = %user.id,
            "account price ended through the admin API"
        );
        after_commit(&state, &target, now);
    }
    Ok(Json(body))
}

fn after_commit<P: PoolProvider>(state: &AppState<P>, target: &Target, starts: DateTime<Utc>) {
    let config = state.current_config();
    if config.integrations.openrouter.account.as_deref() == Some(target.username.as_str()) {
        state.openrouter_refresh.request();
        state.openrouter_refresh.request_at(starts);
    }
}

async fn record(conn: &mut PgConnection, change: &ChangeRecord<'_>, response: impl Fn(Uuid) -> AccountPriceResponse) -> Result<Value> {
    let mut prices = AccountPrices::new(conn);
    let id = prices.record(change, &Value::Null).await.map_err(|e| match e {
        sqlx::Error::Database(db) if db.is_unique_violation() => Error::Conflict {
            message: "Idempotency-Key was already used for another request".to_string(),
            conflicts: None,
        },
        other => db_error(other),
    })?;
    let body = serde_json::to_value(response(id)).map_err(internal)?;
    prices.set_response(id, &body).await.map_err(db_error)?;
    Ok(body)
}

async fn replay(prices: &mut AccountPrices<'_>, request_id: Option<&str>, fingerprint: &str) -> Result<Option<Value>> {
    let Some(request_id) = request_id else { return Ok(None) };
    match prices.find_change(request_id).await.map_err(db_error)? {
        Some(stored) if stored.fingerprint == fingerprint => Ok(Some(stored.response)),
        Some(_) => Err(Error::Conflict {
            message: "Idempotency-Key was already used for another request".to_string(),
            conflicts: None,
        }),
        None => Ok(None),
    }
}

async fn billed(conn: &mut PgConnection, target: &Target, class: Option<&str>, at: DateTime<Utc>) -> Result<Option<BilledPrice>> {
    let aliases = [target.alias.clone()];
    let mut prices = billed_realtime_prices(conn, &aliases, target.account, class.unwrap_or(STANDARD_CLASS), at)
        .await
        .map_err(db_error)?;
    Ok(prices.remove(&target.alias))
}

fn may_price_some_account(user: &CurrentUser, operation: Operation) -> Result<()> {
    if has_permission(user, Resource::Pricing, operation) || has_permission(user, Resource::OpenRouterPricing, operation) {
        return Ok(());
    }
    Err(pricing_denied(operation))
}

fn pricing_denied(operation: Operation) -> Error {
    Error::InsufficientPermissions {
        required: Permission::Allow(Resource::Pricing, operation),
        action: operation,
        resource: "account prices".to_string(),
    }
}

fn response(
    target: &Target,
    class: Option<&str>,
    at: DateTime<Utc>,
    previous: Option<BilledPrice>,
    price: Option<BilledPrice>,
    change_id: Option<Uuid>,
) -> AccountPriceResponse {
    AccountPriceResponse {
        account_id: target.account,
        model_id: target.model,
        model: target.alias.clone(),
        serving_class: class.map(str::to_owned),
        at,
        previous: previous.map(Into::into),
        price: price.map(Into::into),
        change_id,
    }
}

async fn resolve<P: PoolProvider>(
    conn: &mut PgConnection,
    user: &CurrentUser,
    operation: Operation,
    state: &AppState<P>,
    account: &str,
    model: &str,
) -> Result<Target> {
    let account_row = sqlx::query("SELECT id, username FROM users WHERE (id::text = $1 OR username = $1) AND is_deleted = FALSE LIMIT 1")
        .bind(account)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_error)?;
    if !has_permission(user, Resource::Pricing, operation) {
        let openrouter = state.current_config().integrations.openrouter.account.clone();
        let in_scope = has_permission(user, Resource::OpenRouterPricing, operation)
            && account_row
                .as_ref()
                .is_some_and(|row| openrouter.as_deref() == Some(row.get::<&str, _>("username")));
        if !in_scope {
            return Err(pricing_denied(operation));
        }
    }
    let account_row = account_row.ok_or_else(|| Error::NotFound {
        resource: "Account".to_string(),
        id: account.to_string(),
    })?;
    let model_row = sqlx::query("SELECT id, alias FROM deployed_models WHERE (id::text = $1 OR alias = $1) AND deleted = FALSE LIMIT 1")
        .bind(model)
        .fetch_optional(&mut *conn)
        .await
        .map_err(db_error)?
        .ok_or_else(|| Error::NotFound {
            resource: "Model".to_string(),
            id: model.to_string(),
        })?;
    Ok(Target {
        account: account_row.get("id"),
        username: account_row.get("username"),
        model: model_row.get("id"),
        alias: model_row.get("alias"),
    })
}

fn idempotency_key(headers: &HeaderMap) -> Result<Option<String>> {
    let Some(value) = headers.get(IDEMPOTENCY_KEY) else {
        return Ok(None);
    };
    let key = value
        .to_str()
        .ok()
        .map(str::trim)
        .filter(|key| !key.is_empty() && key.len() <= 255)
        .ok_or_else(|| bad_request("Idempotency-Key must be 1 to 255 visible characters"))?;
    Ok(Some(key.to_string()))
}

fn checked_class(class: Option<&str>) -> Result<Option<&str>> {
    match class {
        Some(class)
            if !class.bytes().next().is_some_and(|c| c.is_ascii_lowercase())
                || !class
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'-') =>
        {
            Err(bad_request(format!(
                "serving_class {class:?} must be lowercase letters, digits, '_' or '-', starting with a letter"
            )))
        }
        class => Ok(class),
    }
}

fn checked_reason(reason: &str) -> Result<&str> {
    let reason = reason.trim();
    if reason.is_empty() {
        return Err(bad_request("reason cannot be empty"));
    }
    Ok(reason)
}

fn checked_prices(request: &SetAccountPriceRequest) -> Result<(Decimal, Decimal)> {
    let tariff = Tariff {
        name: "admin-api".to_string(),
        purpose: TariffPurpose::Realtime,
        completion_window: None,
        input_per_million_tokens: request.input_per_million_tokens.clone(),
        output_per_million_tokens: request.output_per_million_tokens.clone(),
    };
    validate_tariffs(std::slice::from_ref(&tariff), "price").map_err(|e| bad_request(format!("{e:#}")))?;
    let input = parse_per_million(&tariff.input_per_million_tokens).map_err(|e| bad_request(format!("{e:#}")))?;
    let output = parse_per_million(&tariff.output_per_million_tokens).map_err(|e| bad_request(format!("{e:#}")))?;
    Ok((input, output))
}

fn checked_read_multiplier(value: &str) -> Result<Decimal> {
    let cache = CacheTariff {
        write_multiplier_5m: "1".to_string(),
        write_multiplier_1h: "1".to_string(),
        write_multiplier_24h: "1".to_string(),
        read_multiplier: value.to_string(),
        min_prefix_tokens: 1,
    };
    validate_cache_tariff(&cache, "price").map_err(|e| bad_request(format!("{e:#}")))?;
    parse_decimal(value).map_err(|e| bad_request(format!("{e:#}")))
}

fn check_floor(config: &crate::config::AccountPricesConfig, alias: &str, input: Decimal, output: Decimal, allow_zero: bool) -> Result<()> {
    if !allow_zero && (input.is_zero() || output.is_zero()) {
        return Err(Error::UnprocessableEntity {
            message: "a zero price is refused unless allow_zero is set".to_string(),
        });
    }
    if let Some(floor) = config.floors.get(alias) {
        let min_input = parse_per_million(&floor.input_per_million_tokens).map_err(internal)?;
        let min_output = parse_per_million(&floor.output_per_million_tokens).map_err(internal)?;
        if input < min_input || output < min_output {
            return Err(Error::UnprocessableEntity {
                message: format!(
                    "price is below the floor for {alias:?}: input {} and output {} per million tokens",
                    floor.input_per_million_tokens, floor.output_per_million_tokens
                ),
            });
        }
    }
    Ok(())
}

fn owned_elsewhere(target: &Target, row: &CompetingRow) -> Error {
    Error::Conflict {
        message: format!(
            "the price of {:?} for model {:?}, class {:?}, comes from {} ({}, from {}); remove it there before setting it here",
            target.username,
            target.alias,
            row.serving_class.as_deref().unwrap_or("none"),
            row.owner(),
            row.table,
            row.valid_from
        ),
        conflicts: None,
    }
}

fn bad_request(message: impl Into<String>) -> Error {
    Error::BadRequest { message: message.into() }
}

fn db_error(error: sqlx::Error) -> Error {
    Error::Database(error.into())
}

fn internal(error: impl std::fmt::Display) -> Error {
    Error::Internal {
        operation: format!("account price: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AccountPricesConfig, PriceFloor};

    fn request(input: &str, output: &str) -> SetAccountPriceRequest {
        SetAccountPriceRequest {
            input_per_million_tokens: input.to_string(),
            output_per_million_tokens: output.to_string(),
            cache_read_multiplier: None,
            serving_class: None,
            valid_from: None,
            allow_zero: false,
            reason: "test".to_string(),
        }
    }

    #[test]
    fn prices_follow_the_yaml_rules() {
        assert_eq!(
            checked_prices(&request("0.40", "1.60")).unwrap(),
            (Decimal::new(4, 7), Decimal::new(16, 7))
        );
        for (input, output) in [("-1", "1"), ("abc", "1"), ("0.000000001", "1")] {
            assert!(checked_prices(&request(input, output)).is_err(), "{input}/{output}");
        }
        assert!(checked_read_multiplier("0.25").is_ok());
        for value in ["-0.1", "100", "0.12345"] {
            assert!(checked_read_multiplier(value).is_err(), "{value}");
        }
    }

    #[test]
    fn a_price_below_the_floor_or_zero_is_refused() {
        let mut config = AccountPricesConfig::default();
        config.floors.insert(
            "example-org/Model".to_string(),
            PriceFloor {
                input_per_million_tokens: "0.10".to_string(),
                output_per_million_tokens: "0.40".to_string(),
            },
        );
        let (low, ok) = (Decimal::new(5, 8), Decimal::new(5, 7));
        assert!(check_floor(&config, "example-org/Model", ok, ok, false).is_ok());
        assert!(check_floor(&config, "example-org/Model", low, ok, false).is_err());
        assert!(
            check_floor(&config, "example-org/Other", low, low, false).is_ok(),
            "no floor configured"
        );
        assert!(check_floor(&config, "example-org/Other", Decimal::ZERO, ok, false).is_err());
        assert!(check_floor(&config, "example-org/Other", Decimal::ZERO, ok, true).is_ok());
        assert!(
            check_floor(&config, "example-org/Model", Decimal::ZERO, ok, true).is_err(),
            "allowing zero does not lift a floor"
        );
    }

    #[test]
    fn classes_are_class_keys() {
        assert_eq!(checked_class(Some("fast")).unwrap(), Some("fast"));
        assert!(checked_class(Some("Fast")).is_err());
        assert!(checked_class(Some("")).is_err());
        assert_eq!(checked_class(None).unwrap(), None);
    }
}
