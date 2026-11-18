
use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx_pool_router::PoolProvider;
use uuid::Uuid;

use crate::{
    AppState,
    api::models::users::CurrentUser,
    auth::permissions,
    db::{
        handlers::checkout_attempts::{CheckoutAttempts, CheckoutKind, Reservation},
        handlers::repository::Repository,
        handlers::users::Users,
        models::users::UserUpdateDBRequest,
    },
    metrics::errors::component::PAYMENTS,
    payment_providers,
};

struct BillingTarget {
    id: crate::types::UserId,
    payment_provider_id: Option<String>,
    email: String,
    invoicing_enabled: bool,
    display_name: Option<String>,
}

enum CheckoutGate {
    Proceed(Uuid),
    Refused(Response),
}

async fn begin_checkout<P: PoolProvider>(
    state: &AppState<P>,
    actor: &CurrentUser,
    provider: &dyn payment_providers::PaymentProvider,
    payer: &mut payment_providers::CheckoutPayer,
    display_name: Option<&str>,
    kind: CheckoutKind,
) -> Result<CheckoutGate, StatusCode> {
    let config = state.current_config();
    let limits = &config.limits.checkout;
    let window_hours = i32::try_from(limits.window_hours.max(1)).unwrap_or(i32::MAX);
    let max_unpaid = (limits.max_unpaid_per_user > 0).then(|| i64::from(limits.max_unpaid_per_user));

    let mut conn = state.db.write().acquire().await.map_err(|e| {
        tracing::error!("Failed to acquire database connection: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let reservation = CheckoutAttempts::new(&mut conn)
        .reserve(actor.id, payer.id, kind, max_unpaid, window_hours)
        .await
        .map_err(|e| {
            tracing::error!("Failed to reserve checkout attempt: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let attempt_id = match reservation {
        Reservation::Reserved(id) => id,
        Reservation::OverCap { unpaid } => {
            metrics::counter!("dwctl_checkout_limit_reached_total").increment(1);
            tracing::warn!(actor_id = %actor.id, payer_id = %payer.id, unpaid, "Refused checkout: too many unpaid checkouts");
            return Ok(CheckoutGate::Refused(
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(json!({
                        "message": "Too many incomplete payment attempts. Please try again later or contact support."
                    })),
                )
                    .into_response(),
            ));
        }
    };

    if payer.payment_provider_id.as_deref().is_none_or(str::is_empty) {
        let customer_id = ensure_provider_customer(&mut conn, provider, payer, display_name).await;
        drop(conn);
        match customer_id {
            Ok(customer_id) => payer.payment_provider_id = Some(customer_id),
            Err(status) => {
                abandon_checkout(state, attempt_id).await;
                return Err(status);
            }
        }
    }

    Ok(CheckoutGate::Proceed(attempt_id))
}

async fn ensure_provider_customer(
    conn: &mut sqlx::PgConnection,
    provider: &dyn payment_providers::PaymentProvider,
    payer: &payment_providers::CheckoutPayer,
    display_name: Option<&str>,
) -> Result<String, StatusCode> {
    let new_id = provider.create_customer(&payer.email, display_name).await.map_err(|e| {
        tracing::error!("Failed to create payment provider customer: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut users = Users::new(conn);
    let saved = users.set_payment_provider_id_if_empty(payer.id, &new_id).await.map_err(|e| {
        tracing::error!("Failed to save customer ID: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if saved {
        return Ok(new_id);
    }
    Ok(users
        .get_by_id(payer.id)
        .await
        .map_err(|e| {
            tracing::error!("Failed to reload customer ID: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .and_then(|u| u.payment_provider_id)
        .filter(|id| !id.is_empty())
        .unwrap_or(new_id))
}

async fn record_checkout<P: PoolProvider>(state: &AppState<P>, attempt_id: Uuid, session_id: &str) {
    let result = async {
        let mut conn = state.db.write().acquire().await?;
        CheckoutAttempts::new(&mut conn).bind_session(attempt_id, session_id).await?;
        Ok::<_, crate::db::errors::DbError>(())
    }
    .await;
    if let Err(e) = result {
        tracing::error!(%attempt_id, "Failed to record checkout session; paying it will not clear the attempt: {:?}", e);
    }
}

async fn abandon_checkout<P: PoolProvider>(state: &AppState<P>, attempt_id: Uuid) {
    let result = async {
        let mut conn = state.db.write().acquire().await?;
        CheckoutAttempts::new(&mut conn).release(attempt_id).await?;
        Ok::<_, crate::db::errors::DbError>(())
    }
    .await;
    if let Err(e) = result {
        tracing::error!(%attempt_id, "Failed to release checkout attempt: {:?}", e);
    }
}

async fn resolve_billing_target(user: &CurrentUser, conn: &mut sqlx::PgConnection) -> Result<BillingTarget, StatusCode> {
    if let Some(org_id) = user.active_organization {
        let can_manage = permissions::can_manage_org_resource(user, org_id, conn).await.map_err(|e| {
            tracing::error!("Failed to check org permissions: {:?}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        if !can_manage {
            return Err(StatusCode::FORBIDDEN);
        }
        let org = Users::new(conn)
            .get_by_id(org_id)
            .await
            .map_err(|e| {
                tracing::error!("Failed to load org user: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .ok_or(StatusCode::NOT_FOUND)?;
        Ok(BillingTarget {
            id: org.id,
            payment_provider_id: org.payment_provider_id,
            email: org.email,
            invoicing_enabled: org.invoicing_enabled,
            display_name: org.display_name,
        })
    } else {
        let invoicing_enabled = sqlx::query_scalar!("SELECT invoicing_enabled FROM users WHERE id = $1", user.id)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| {
                tracing::error!("Failed to read invoicing flag: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .unwrap_or(false);

        Ok(BillingTarget {
            id: user.id,
            payment_provider_id: user.payment_provider_id.clone(),
            email: user.email.clone(),
            invoicing_enabled,
            display_name: None,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PaymentQuery {
    pub creditee_id: Option<String>,
}

#[utoipa::path(
    post,
    path = "/payments",
    tag = "payments",
    summary = "Create payment",
    description = "Creates a payment checkout session with the payment provider. Returns a JSON object with the checkout URL for the client to handle navigation (better for SPAs). Optionally accepts a creditee_id query parameter to credit another user (admin feature).",
    params(
        ("creditee_id" = Option<String>, Query, description = "Optional user ID to credit (for admin granting credits to another user)")
    ),
    responses(
        (status = 200, description = "Payment session created successfully. Returns JSON with checkout URL.", body = inline(Object)),
        (status = 429, description = "Too many unpaid checkouts for this user"),
        (status = 501, description = "No payment provider configured"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn create_payment<P: PoolProvider>(
    State(state): State<AppState<P>>,
    user: CurrentUser,
    axum::extract::Query(query): axum::extract::Query<PaymentQuery>,
) -> Result<Response, StatusCode> {
    let config = state.current_config();
    let payment_config = match config.payment.clone() {
        Some(config) => config,
        None => {
            tracing::warn!("Checkout requested but no payment provider is configured");
            let error_response = Json(json!({
                "message": "Payment processing is currently unavailable. Please contact support."
            }));
            return Ok((StatusCode::SERVICE_UNAVAILABLE, error_response).into_response());
        }
    };

    let origin = config.dashboard_url.clone();

    let base_path = if let Some(creditee_id) = &query.creditee_id {
        format!("/cost-management?user={}", creditee_id)
    } else {
        "/cost-management".to_string()
    };

    let success_url = format!(
        "{}{}payment=success&session_id={{CHECKOUT_SESSION_ID}}",
        origin,
        if query.creditee_id.is_some() {
            format!("{}&", base_path)
        } else {
            format!("{}?", base_path)
        }
    );
    let cancel_url = format!(
        "{}{}payment=cancelled&session_id={{CHECKOUT_SESSION_ID}}",
        origin,
        if query.creditee_id.is_some() {
            format!("{}&", base_path)
        } else {
            format!("{}?", base_path)
        }
    );

    let mut conn = state.db.write().acquire().await.map_err(|e| {
        tracing::error!("Failed to acquire database connection: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let target = resolve_billing_target(&user, &mut conn).await?;
    drop(conn);

    if target.invoicing_enabled {
        tracing::info!(target_id = %target.id, "Refused card checkout for an invoice-billed account");
        return Ok((
            StatusCode::CONFLICT,
            Json(json!({
                "message": "This account is billed by invoice. Credits are added automatically and charged to your monthly invoice."
            })),
        )
            .into_response());
    }

    let mut payer = payment_providers::CheckoutPayer {
        id: target.id,
        email: target.email,
        payment_provider_id: target.payment_provider_id,
    };

    let provider = payment_providers::create_provider(payment_config);

    let attempt_id = match begin_checkout(
        &state,
        &user,
        provider.as_ref(),
        &mut payer,
        target.display_name.as_deref(),
        CheckoutKind::Payment,
    )
    .await?
    {
        CheckoutGate::Proceed(attempt_id) => attempt_id,
        CheckoutGate::Refused(response) => return Ok(response),
    };

    let checkout = match provider
        .create_checkout_session(&payer, query.creditee_id.as_deref(), &cancel_url, &success_url)
        .await
    {
        Ok(checkout) => checkout,
        Err(e) => {
            tracing::error!("Failed to create checkout session: {:?}", e);
            abandon_checkout(&state, attempt_id).await;
            return Err(StatusCode::from(e));
        }
    };

    record_checkout(&state, attempt_id, &checkout.session_id).await;

    Ok(Json(json!({
        "url": checkout.url
    }))
    .into_response())
}

#[utoipa::path(
    post,
    path = "/payments/setup",
    tag = "payments",
    summary = "Create card verification session",
    description = "Creates a setup-mode checkout session that verifies and saves a payment method without charging it. Returns a JSON object with the checkout URL for the client to navigate to. Confirm the result with PATCH /payments/{id}, as for a normal payment.",
    responses(
        (status = 200, description = "Setup session created successfully. Returns JSON with checkout URL.", body = inline(Object)),
        (status = 403, description = "Caller cannot manage billing for the active organization"),
        (status = 429, description = "Too many unpaid checkouts for this user"),
        (status = 503, description = "No payment provider configured"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn create_payment_setup<P: PoolProvider>(State(state): State<AppState<P>>, user: CurrentUser) -> Result<Response, StatusCode> {
    let config = state.current_config();
    let payment_config = match config.payment.clone() {
        Some(config) => config,
        None => {
            tracing::warn!("Card verification requested but no payment provider is configured");
            let error_response = Json(json!({
                "message": "Payment processing is currently unavailable. Please contact support."
            }));
            return Ok((StatusCode::SERVICE_UNAVAILABLE, error_response).into_response());
        }
    };

    let origin = config.dashboard_url.clone();
    let success_url = format!("{}/cost-management?payment=success&session_id={{CHECKOUT_SESSION_ID}}", origin);
    let cancel_url = format!("{}/cost-management?payment=cancelled&session_id={{CHECKOUT_SESSION_ID}}", origin);

    let mut conn = state.db.write().acquire().await.map_err(|e| {
        tracing::error!("Failed to acquire database connection: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let target = resolve_billing_target(&user, &mut conn).await?;
    drop(conn);

    let mut payer = payment_providers::CheckoutPayer {
        id: target.id,
        email: target.email,
        payment_provider_id: target.payment_provider_id,
    };

    let provider = payment_providers::create_provider(payment_config);

    let attempt_id = match begin_checkout(
        &state,
        &user,
        provider.as_ref(),
        &mut payer,
        target.display_name.as_deref(),
        CheckoutKind::Setup,
    )
    .await?
    {
        CheckoutGate::Proceed(attempt_id) => attempt_id,
        CheckoutGate::Refused(response) => return Ok(response),
    };

    let checkout = match provider.create_setup_checkout_session(&payer, &cancel_url, &success_url).await {
        Ok(checkout) => checkout,
        Err(e) => {
            tracing::error!("Failed to create setup checkout session: {:?}", e);
            abandon_checkout(&state, attempt_id).await;
            return Err(StatusCode::from(e));
        }
    };

    record_checkout(&state, attempt_id, &checkout.session_id).await;

    Ok(Json(json!({
        "url": checkout.url
    }))
    .into_response())
}

#[utoipa::path(
    patch,
    path = "/payments/{id}",
    tag = "payments",
    summary = "Process payment",
    description = "Processes a completed payment session and credits the user account. This is idempotent.",
    responses(
        (status = 200, description = "Payment processed successfully"),
        (status = 402, description = "Payment not completed yet"),
        (status = 409, description = "The payment method saved by this verification session has already verified another account; the account is not verified. Verify with a different card or add credit."),
        (status = 400, description = "Invalid payment ID or missing data"),
        (status = 501, description = "Payment provider not configured"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn process_payment<P: PoolProvider>(
    State(state): State<AppState<P>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    _user: CurrentUser,
) -> Result<Response, StatusCode> {
    let config = state.current_config();
    let provider = match config.payment.clone() {
        Some(payment_config) => payment_providers::create_provider(payment_config),
        None => {
            tracing::warn!("Payment processing requested but no payment provider is configured");
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "message": "Payment processing is currently unavailable. Please contact support."
                })),
            )
                .into_response());
        }
    };

    match provider.process_payment_session(&state.db.write(), &id, &config.credits).await {
        Ok(()) => Ok(Json(json!({
            "message": "Payment processed successfully"
        }))
        .into_response()),
        Err(e) => match e {
            payment_providers::PaymentError::PaymentNotCompleted => Ok((
                StatusCode::PAYMENT_REQUIRED,
                Json(json!({
                    "message": "Payment is still processing. Please check back in a moment."
                })),
            )
                .into_response()),
            payment_providers::PaymentError::AlreadyProcessed => {
                tracing::trace!("Transaction already processed (idempotent)");
                Ok(Json(json!({
                    "message": "Payment processed successfully"
                }))
                .into_response())
            }
            payment_providers::PaymentError::InstrumentAlreadyUsed => Ok((
                StatusCode::CONFLICT,
                Json(json!({
                    "message": "Sorry, this payment method has already been used for verification. Please top up your account or verify with a new payment method."
                })),
            )
                .into_response()),
            _ => {
                let detail = format!("{e:?}");
                let status = StatusCode::from(e);
                if status.is_server_error() {
                    crate::background_error!(PAYMENTS, "process_failed", Error,
                        error = %detail, %status, "Failed to process payment session");
                } else {
                    tracing::warn!(error = %detail, %status, "Payment session rejected");
                }
                Ok((
                    status,
                    Json(json!({
                        "message": "Unable to process payment. Please contact support."
                    })),
                )
                    .into_response())
            }
        },
    }
}

#[utoipa::path(
    post,
    path = "/webhooks/payments",
    tag = "payments",
    summary = "Payment webhook",
    description = "Receives webhook events from payment providers (Stripe, PayPal, etc.) and processes them.",
    responses(
        (status = 200, description = "Webhook processed successfully"),
        (status = 400, description = "Invalid webhook signature or malformed data"),
        (status = 501, description = "Payment provider not configured or doesn't support webhooks"),
    ),
)]
#[tracing::instrument(skip_all)]
pub async fn webhook_handler<P: PoolProvider>(
    State(state): State<AppState<P>>,
    headers: axum::http::HeaderMap,
    body: String,
) -> StatusCode {
    let config = state.current_config();
    let provider = match config.payment.clone() {
        Some(payment_config) => payment_providers::create_provider(payment_config),
        None => {
            tracing::warn!("Webhook received but no payment provider configured");
            return StatusCode::NOT_IMPLEMENTED;
        }
    };

    let event = match provider.validate_webhook(&headers, &body).await {
        Ok(Some(event)) => event,
        Ok(None) => {
            tracing::info!("Provider doesn't support webhooks");
            return StatusCode::NOT_IMPLEMENTED;
        }
        Err(e) => {
            tracing::error!("Webhook validation failed: {:?}", e);
            return StatusCode::BAD_REQUEST;
        }
    };

    tracing::trace!("Received webhook event: {}", event.event_type);

    match provider.process_webhook_event(&state.db.write(), &event, &config.credits).await {
        Ok(()) => {
            tracing::trace!("Successfully processed webhook event: {}", event.event_type);
            StatusCode::OK
        }
        Err(payment_providers::PaymentError::AlreadyProcessed) => {
            tracing::trace!("Webhook event already processed (idempotent): {}", event.event_type);
            StatusCode::OK
        }
        Err(payment_providers::PaymentError::UnknownReference(reference)) => {
            tracing::warn!(
                event_type = %event.event_type,
                client_reference_id = %reference,
                "Ignoring payment webhook for a user unknown to this plane (expected for another region's events)"
            );
            StatusCode::OK
        }
        Err(payment_providers::PaymentError::InstrumentAlreadyUsed) => {
            tracing::warn!(
                event_type = %event.event_type,
                "Setup session's payment method has already verified another account; acked"
            );
            StatusCode::OK
        }
        Err(payment_providers::PaymentError::Database(_)) => {
            tracing::error!("Transient database error processing webhook event: {}", event.event_type);
            StatusCode::INTERNAL_SERVER_ERROR
        }
        Err(e) => {
            tracing::error!("Failed to process webhook event (non-retryable): {:?}", e);
            StatusCode::OK
        }
    }
}

#[utoipa::path(
    post,
    path = "/billing-portal",
    tag = "payments",
    summary = "Create billing portal session",
    description = "Creates a billing portal session for the authenticated user. Requires the user to have a payment_provider_id (customer ID) set. The return URL is automatically constructed from the configured dashboard_url.",
    responses(
        (status = 200, description = "Billing portal session created successfully. Returns JSON with portal URL.", body = inline(Object)),
        (status = 400, description = "User does not have a payment provider customer ID"),
        (status = 503, description = "No payment provider configured"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn create_billing_portal_session<P: PoolProvider>(
    State(state): State<AppState<P>>,
    user: CurrentUser,
) -> Result<Response, StatusCode> {
    let config = state.current_config();
    let payment_config = match config.payment.clone() {
        Some(config) => config,
        None => {
            tracing::warn!("Billing portal requested but no payment provider is configured");
            let error_response = Json(json!({
                "message": "Billing management is currently unavailable. Please contact support."
            }));
            return Ok((StatusCode::SERVICE_UNAVAILABLE, error_response).into_response());
        }
    };

    let mut conn = state.db.write().acquire().await.map_err(|e| {
        tracing::error!("Failed to acquire database connection: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let target = resolve_billing_target(&user, &mut conn).await?;

    let customer_id = target.payment_provider_id.filter(|s| !s.is_empty()).ok_or_else(|| {
        tracing::warn!("Target {} has no payment provider customer ID", target.id);
        StatusCode::BAD_REQUEST
    })?;

    let return_url = format!("{}/cost-management", config.dashboard_url);

    let provider = payment_providers::create_provider(payment_config);

    let portal_url = provider
        .create_billing_portal_session(&customer_id, &return_url)
        .await
        .map_err(|e| {
            tracing::error!("Failed to create billing portal session: {:?}", e);
            StatusCode::from(e)
        })?;

    Ok(Json(json!({
        "url": portal_url
    }))
    .into_response())
}

#[utoipa::path(
    post,
    path = "/auto-topup/checkout",
    tag = "payments",
    summary = "Create auto top-up checkout session",
    description = "Creates a checkout session for auto top-up setup. The user must have a payment provider customer ID.",
    responses(
        (status = 200, description = "Checkout session created. Returns JSON with checkout URL.", body = inline(Object)),
        (status = 400, description = "User does not have a payment provider customer ID"),
        (status = 429, description = "Too many unpaid checkouts for this user"),
        (status = 503, description = "No payment provider configured"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn create_auto_topup_checkout<P: PoolProvider>(
    State(state): State<AppState<P>>,
    user: CurrentUser,
) -> Result<Response, StatusCode> {
    let config = state.current_config();
    let payment_config = match config.payment.clone() {
        Some(config) => config,
        None => {
            tracing::warn!("Auto top-up checkout requested but no payment provider is configured");
            let error_response = Json(json!({
                "message": "Payment processing is currently unavailable. Please contact support."
            }));
            return Ok((StatusCode::SERVICE_UNAVAILABLE, error_response).into_response());
        }
    };

    let origin = config.dashboard_url.clone();
    let success_url = format!("{}/cost-management?autoTopupId={{CHECKOUT_SESSION_ID}}&autoTopup=true", origin);
    let cancel_url = format!("{}/cost-management?autoTopup=true&autoTopupId=fail", origin);

    let mut payer = payment_providers::CheckoutPayer {
        id: user.id,
        email: user.email.clone(),
        payment_provider_id: user.payment_provider_id.clone(),
    };

    let provider = payment_providers::create_provider(payment_config);

    let attempt_id = match begin_checkout(&state, &user, provider.as_ref(), &mut payer, None, CheckoutKind::AutoTopup).await? {
        CheckoutGate::Proceed(attempt_id) => attempt_id,
        CheckoutGate::Refused(response) => return Ok(response),
    };

    let checkout = match provider.create_auto_topup_checkout_session(&payer, &cancel_url, &success_url).await {
        Ok(checkout) => checkout,
        Err(e) => {
            tracing::error!("Failed to create auto top-up checkout session: {:?}", e);
            abandon_checkout(&state, attempt_id).await;
            return Err(StatusCode::from(e));
        }
    };

    record_checkout(&state, attempt_id, &checkout.session_id).await;

    Ok(Json(json!({
        "url": checkout.url
    }))
    .into_response())
}

#[derive(Debug, Deserialize, Serialize, utoipa::ToSchema)]
pub struct ProcessAutoTopupRequest {
    pub threshold: f32,
    pub amount: f32,
    pub monthly_limit: Option<f32>,
}

#[utoipa::path(
    put,
    path = "/auto-topup/{id}",
    tag = "payments",
    summary = "Enable auto top-up",
    description = "Validates a checkout session with the payment provider and enables auto top-up for the current user at the specified threshold and amount.",
    params(
        ("id" = String, Path, description = "Checkout session ID from the payment provider")
    ),
    request_body(content = inline(Object), description = "Auto top-up configuration"),
    responses(
        (status = 200, description = "Auto top-up enabled successfully"),
        (status = 400, description = "Invalid session or threshold"),
        (status = 402, description = "Session not completed yet"),
        (status = 503, description = "No payment provider configured"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn process_auto_topup<P: PoolProvider>(
    State(state): State<AppState<P>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    user: CurrentUser,
    Json(body): Json<ProcessAutoTopupRequest>,
) -> Result<Response, StatusCode> {
    if body.threshold < 0.0 || body.amount <= 0.0 {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "message": "Threshold must be non-negative and amount must be positive"
            })),
        )
            .into_response());
    }

    if let Some(limit) = body.monthly_limit
        && limit <= 0.0
    {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "message": "Monthly limit must be positive"
            })),
        )
            .into_response());
    }

    let config = state.current_config();
    let provider = match config.payment.clone() {
        Some(payment_config) => payment_providers::create_provider(payment_config),
        None => {
            tracing::warn!("Auto top-up requested but no payment provider is configured");
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "message": "Payment processing is currently unavailable. Please contact support."
                })),
            )
                .into_response());
        }
    };

    let setup_result = match provider.process_auto_topup_session(&state.db.write(), &id).await {
        Ok(result) => result,
        Err(e) => match e {
            payment_providers::PaymentError::PaymentNotCompleted => {
                return Ok((
                    StatusCode::PAYMENT_REQUIRED,
                    Json(json!({
                        "message": "Session is still processing. Please check back in a moment."
                    })),
                )
                    .into_response());
            }
            _ => {
                tracing::error!("Failed to validate auto top-up session: {:?}", e);
                return Ok((
                    StatusCode::from(e),
                    Json(json!({
                        "message": "Failed to validate session with payment provider."
                    })),
                )
                    .into_response());
            }
        },
    };

    if let Some(ref session_user_id) = setup_result.user_id
        && session_user_id != &user.id.to_string()
    {
        tracing::warn!(
            authenticated_user = %user.id,
            session_user = %session_user_id,
            "Auto top-up session ownership mismatch"
        );
        return Ok((
            StatusCode::FORBIDDEN,
            Json(json!({
                "message": "This session does not belong to your account."
            })),
        )
            .into_response());
    }

    let update = UserUpdateDBRequest {
        display_name: None,
        avatar_url: None,
        roles: None,
        password_hash: None,
        batch_notifications_enabled: None,
        low_balance_threshold: None,
        auto_topup_amount: Some(Some(body.amount)),
        auto_topup_threshold: Some(Some(body.threshold)),
        auto_topup_monthly_limit: Some(body.monthly_limit),
        zero_data_retention: None,
        self_hosted_only: None,
        toleration_preset: Default::default(),
    };

    let mut conn = state.db.write().acquire().await.map_err(|e| {
        tracing::error!("Failed to acquire database connection: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut users = Users::new(&mut conn);
    users.update(user.id, &update).await.map_err(|e| {
        tracing::error!("Failed to enable auto top-up: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if let Some(customer_id) = &setup_result.customer_id
        && user.payment_provider_id.is_none()
    {
        let mut users = Users::new(&mut conn);
        if let Err(e) = users.set_payment_provider_id_if_empty(user.id, customer_id).await {
            tracing::warn!(user_id = %user.id, error = %e, "Failed to save customer ID from auto top-up setup");
        }
    }

    Ok(Json(json!({
        "message": "Auto top-up enabled successfully",
        "threshold": body.threshold,
        "amount": body.amount,
        "monthly_limit": body.monthly_limit
    }))
    .into_response())
}

#[utoipa::path(
    post,
    path = "/auto-topup/enable",
    tag = "payments",
    summary = "Enable auto top-up",
    description = "Validates threshold/amount, checks for a default payment method with the payment provider, and enables auto top-up if possible. Returns instructions for the frontend on what to do next.",
    request_body(content = ProcessAutoTopupRequest, description = "Auto top-up configuration"),
    responses(
        (status = 200, description = "Result of the enable attempt", body = inline(Object)),
        (status = 400, description = "Invalid threshold or amount"),
        (status = 404, description = "Target organization not found"),
        (status = 503, description = "No payment provider configured"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn enable_auto_topup<P: PoolProvider>(
    State(state): State<AppState<P>>,
    user: CurrentUser,
    Json(body): Json<ProcessAutoTopupRequest>,
) -> Result<Response, StatusCode> {
    if body.threshold < 0.0 || body.amount <= 0.0 {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "message": "Threshold must be non-negative and amount must be positive"
            })),
        )
            .into_response());
    }

    if let Some(limit) = body.monthly_limit
        && limit <= 0.0
    {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "message": "Monthly limit must be positive"
            })),
        )
            .into_response());
    }

    let config = state.current_config();
    let provider = match config.payment.clone() {
        Some(payment_config) => payment_providers::create_provider(payment_config),
        None => {
            tracing::warn!("Auto top-up enable requested but no payment provider is configured");
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "message": "Payment processing is currently unavailable. Please contact support."
                })),
            )
                .into_response());
        }
    };

    let mut conn = state.db.write().acquire().await.map_err(|e| {
        tracing::error!("Failed to acquire database connection: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let target = resolve_billing_target(&user, &mut conn).await?;

    let customer_id = match &target.payment_provider_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => {
            let new_id = provider
                .create_customer(&target.email, target.display_name.as_deref())
                .await
                .map_err(|e| {
                    tracing::error!("Failed to create payment provider customer: {:?}", e);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;

            Users::new(&mut conn)
                .set_payment_provider_id_if_empty(target.id, &new_id)
                .await
                .map_err(|e| {
                    tracing::error!("Failed to save customer ID: {:?}", e);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;

            new_id
        }
    };

    match provider.customer_has_address(&customer_id).await {
        Ok(true) => {}
        Ok(false) => {
            return Ok(Json(json!({
                "needs_billing_portal": true,
                "reason": "Customer must have an address on file for tax calculation. Please update your billing details."
            }))
            .into_response());
        }
        Err(e) => {
            tracing::error!("Failed to check customer address: {:?}", e);
            return Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "message": "Failed to verify billing address with payment provider."
                })),
            )
                .into_response());
        }
    }

    match provider.get_default_payment_method(&customer_id).await {
        Ok(Some(_pm_id)) => {
            let update = UserUpdateDBRequest {
                display_name: None,
                avatar_url: None,
                roles: None,
                password_hash: None,
                batch_notifications_enabled: None,
                low_balance_threshold: None,
                auto_topup_amount: Some(Some(body.amount)),
                auto_topup_threshold: Some(Some(body.threshold)),
                auto_topup_monthly_limit: Some(body.monthly_limit),
                zero_data_retention: None,
                self_hosted_only: None,
                toleration_preset: Default::default(),
            };

            Users::new(&mut conn).update(target.id, &update).await.map_err(|e| {
                tracing::error!("Failed to enable auto top-up: {:?}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            Ok(Json(json!({
                "has_payment_method": true,
                "threshold": body.threshold,
                "amount": body.amount,
                "monthly_limit": body.monthly_limit
            }))
            .into_response())
        }
        Ok(None) => {
            Ok(Json(json!({
                "needs_billing_portal": true
            }))
            .into_response())
        }
        Err(e) => {
            tracing::error!("Failed to check payment method: {:?}", e);
            Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "message": "Failed to check payment method with payment provider."
                })),
            )
                .into_response())
        }
    }
}

#[utoipa::path(
    post,
    path = "/auto-topup/disable",
    tag = "payments",
    summary = "Disable auto top-up",
    description = "Clears auto top-up configuration for the current user or active organization.",
    responses(
        (status = 200, description = "Auto top-up disabled"),
        (status = 404, description = "Target user not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn disable_auto_topup<P: PoolProvider>(State(state): State<AppState<P>>, user: CurrentUser) -> Result<Response, StatusCode> {
    let mut conn = state.db.write().acquire().await.map_err(|e| {
        tracing::error!("Failed to acquire database connection: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let target = resolve_billing_target(&user, &mut conn).await?;

    let update = UserUpdateDBRequest {
        display_name: None,
        avatar_url: None,
        roles: None,
        password_hash: None,
        batch_notifications_enabled: None,
        low_balance_threshold: None,
        auto_topup_amount: Some(None),
        auto_topup_threshold: Some(None),
        auto_topup_monthly_limit: Some(None),
        zero_data_retention: None,
        self_hosted_only: None,
        toleration_preset: Default::default(),
    };

    Users::new(&mut conn).update(target.id, &update).await.map_err(|e| match e {
        crate::db::errors::DbError::NotFound => StatusCode::NOT_FOUND,
        other => {
            tracing::error!("Failed to disable auto top-up: {:?}", other);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    })?;

    Ok(Json(json!({ "message": "Auto top-up disabled" })).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DummyConfig;
    use crate::{config::PaymentConfig, test::utils::create_test_config};
    use axum::Router;
    use axum::routing::{patch, post, put};
    use axum_test::TestServer;
    use rust_decimal::Decimal;
    use sqlx::PgPool;

    #[dwctl_test_macros::test]
    async fn test_dummy_payment_flow(pool: PgPool) {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/payments", post(create_payment))
            .route("/payments/{id}", patch(process_payment))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/payments");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);
        let checkout_response: serde_json::Value = response.json();
        let checkout_url = checkout_response["url"].as_str().unwrap();

        assert!(checkout_url.contains("session_id="));
        assert!(checkout_url.contains("payment=success"));

        let url = url::Url::parse(checkout_url).unwrap();
        let query_pairs: std::collections::HashMap<_, _> = url.query_pairs().collect();
        let session_id = query_pairs.get("session_id").unwrap();

        let count_before = sqlx::query!(
            r#"
            SELECT COUNT(*) as count
            FROM credits_transactions
            WHERE source_id = $1
            "#,
            session_id.to_string()
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count_before.count.unwrap(), 0, "Transaction should not exist before processing");

        let mut request = server.patch(&format!("/payments/{}", session_id));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);
        let process_response: serde_json::Value = response.json();
        assert_eq!(process_response["message"], "Payment processed successfully");

        let transaction = sqlx::query!(
            r#"
            SELECT amount, user_id, source_id
            FROM credits_transactions
            WHERE source_id = $1
            "#,
            session_id.to_string()
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(transaction.amount, Decimal::new(100, 0));
        assert_eq!(transaction.user_id, user.id);

        let mut request = server.patch(&format!("/payments/{}", session_id));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);

        let count = sqlx::query!(
            r#"
            SELECT COUNT(*) as count
            FROM credits_transactions
            WHERE source_id = $1
            "#,
            session_id.to_string()
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(count.count.unwrap(), 1, "Should only have one transaction (idempotent)");
    }

    #[dwctl_test_macros::test]
    async fn test_payment_no_provider_configured(pool: PgPool) {
        let config = create_test_config();

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new().route("/payments", post(create_payment)).with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/payments");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::SERVICE_UNAVAILABLE);
        let error_response: serde_json::Value = response.json();
        assert!(error_response["message"].as_str().unwrap().contains("unavailable"));
    }

    #[dwctl_test_macros::test]
    async fn test_payment_with_creditee_id(pool: PgPool) {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let payer = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let recipient = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&payer);

        let app = Router::new()
            .route("/payments", post(create_payment))
            .route("/payments/{id}", patch(process_payment))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post(&format!("/payments?creditee_id={}", recipient.id));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);
        let checkout_response: serde_json::Value = response.json();
        let checkout_url = checkout_response["url"].as_str().unwrap();

        assert!(checkout_url.contains("session_id="));
        assert!(checkout_url.contains(&format!("dummy_session_{}", recipient.id)));

        assert!(
            checkout_url.contains(&format!("user={}", recipient.id)),
            "Redirect URL should preserve user filter: {}",
            checkout_url
        );
        assert!(
            checkout_url.contains("payment=success"),
            "Redirect URL should contain payment status: {}",
            checkout_url
        );
    }

    #[dwctl_test_macros::test]
    async fn test_billing_portal_success(pool: PgPool) {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;

        let customer_id = format!("cus_test_{}", user.id);
        sqlx::query("UPDATE users SET payment_provider_id = $1 WHERE id = $2")
            .bind(&customer_id)
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();

        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/billing-portal", post(create_billing_portal_session))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/billing-portal");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);
        let body: serde_json::Value = response.json();

        let url = body["url"].as_str().expect("Response should contain url field");
        assert!(url.starts_with("http://localhost:3001/cost-management"));
        assert!(url.contains("dummy_billing_portal=true"));
        assert!(url.contains(&format!("customer_id=cus_test_{}", user.id)));
    }

    #[dwctl_test_macros::test]
    async fn test_billing_portal_no_customer_id(pool: PgPool) {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;

        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/billing-portal", post(create_billing_portal_session))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/billing-portal");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_billing_portal_no_provider_configured(pool: PgPool) {
        let config = create_test_config();

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;

        let customer_id = format!("cus_test_{}", user.id);
        sqlx::query("UPDATE users SET payment_provider_id = $1 WHERE id = $2")
            .bind(&customer_id)
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();

        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/billing-portal", post(create_billing_portal_session))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/billing-portal");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::SERVICE_UNAVAILABLE);
    }

    #[dwctl_test_macros::test]
    async fn test_auto_topup_checkout_success(pool: PgPool) {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;

        sqlx::query!("UPDATE users SET payment_provider_id = $1 WHERE id = $2", "cus_test_123", user.id)
            .execute(&pool)
            .await
            .unwrap();

        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/auto-topup/checkout", post(create_auto_topup_checkout))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/auto-topup/checkout");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);
        let body: serde_json::Value = response.json();
        let url = body["url"].as_str().expect("Should contain checkout URL");
        assert!(url.contains("autoTopupId="), "URL should contain autoTopupId param");
        assert!(url.contains("autoTopup=true"), "URL should contain autoTopup param");
    }

    #[dwctl_test_macros::test]
    async fn test_auto_topup_checkout_no_provider(pool: PgPool) {
        let config = create_test_config();
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/auto-topup/checkout", post(create_auto_topup_checkout))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/auto-topup/checkout");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::SERVICE_UNAVAILABLE);
    }

    #[dwctl_test_macros::test]
    async fn test_process_auto_topup_success(pool: PgPool) {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/auto-topup/checkout", post(create_auto_topup_checkout))
            .route("/auto-topup/{id}", put(process_auto_topup))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/auto-topup/checkout");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;
        response.assert_status(StatusCode::OK);

        let body: serde_json::Value = response.json();
        let checkout_url = body["url"].as_str().unwrap();

        let url = url::Url::parse(checkout_url).unwrap();
        let query_pairs: std::collections::HashMap<_, _> = url.query_pairs().collect();
        let session_id = query_pairs.get("autoTopupId").unwrap();

        let mut request = server.put(&format!("/auto-topup/{}", session_id)).json(&serde_json::json!({
            "threshold": 5.0,
            "amount": 25.0
        }));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);
        let body: serde_json::Value = response.json();
        assert_eq!(body["threshold"], 5.0);
        assert_eq!(body["amount"], 25.0);

        let row = sqlx::query!(
            "SELECT auto_topup_amount, auto_topup_threshold, payment_provider_id FROM users WHERE id = $1",
            user.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.auto_topup_amount, Some(25.0));
        assert_eq!(row.auto_topup_threshold, Some(5.0));
        assert!(
            row.payment_provider_id.is_some(),
            "Customer ID should be saved for first-time users"
        );
        assert!(
            row.payment_provider_id.unwrap().starts_with("dummy_cus_"),
            "Should be a dummy customer ID"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_process_auto_topup_invalid_params(pool: PgPool) {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new().route("/auto-topup/{id}", put(process_auto_topup)).with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.put("/auto-topup/dummy_session_fake").json(&serde_json::json!({
            "threshold": -1.0,
            "amount": 25.0
        }));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;
        response.assert_status(StatusCode::BAD_REQUEST);

        let mut request = server.put("/auto-topup/dummy_session_fake").json(&serde_json::json!({
            "threshold": 5.0,
            "amount": 0.0
        }));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;
        response.assert_status(StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    async fn test_enable_auto_topup_with_payment_method(pool: PgPool) {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;

        sqlx::query!(
            r#"
            UPDATE users
            SET payment_provider_id = $1,
                auto_topup_soft_failure_count = 1,
                auto_topup_retry_after = NOW() + INTERVAL '24 hours'
            WHERE id = $2
            "#,
            "cus_test_123",
            user.id
        )
        .execute(&pool)
        .await
        .unwrap();

        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new().route("/auto-topup/enable", post(enable_auto_topup)).with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/auto-topup/enable").json(&serde_json::json!({
            "threshold": 5.0,
            "amount": 25.0
        }));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);
        let body: serde_json::Value = response.json();
        assert_eq!(body["has_payment_method"], true);
        assert_eq!(body["threshold"], 5.0);
        assert_eq!(body["amount"], 25.0);

        let row = sqlx::query!(
            r#"
            SELECT auto_topup_amount, auto_topup_threshold,
                   auto_topup_soft_failure_count, auto_topup_retry_after
            FROM users WHERE id = $1
            "#,
            user.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(row.auto_topup_amount, Some(25.0));
        assert_eq!(row.auto_topup_threshold, Some(5.0));
        assert_eq!(row.auto_topup_soft_failure_count, 0);
        assert_eq!(row.auto_topup_retry_after, None);
    }

    #[dwctl_test_macros::test]
    async fn test_enable_auto_topup_no_customer(pool: PgPool) {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new().route("/auto-topup/enable", post(enable_auto_topup)).with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/auto-topup/enable").json(&serde_json::json!({
            "threshold": 5.0,
            "amount": 25.0
        }));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);
        let body: serde_json::Value = response.json();
        assert_eq!(body["has_payment_method"], true);

        let row = sqlx::query!("SELECT payment_provider_id FROM users WHERE id = $1", user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(row.payment_provider_id.is_some(), "Customer ID should be saved");
    }

    #[dwctl_test_macros::test]
    async fn test_enable_auto_topup_in_org_context(pool: PgPool) {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));

        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let org = crate::test::utils::create_test_org(&pool, user.id).await;

        sqlx::query!("UPDATE users SET payment_provider_id = $1 WHERE id = $2", "cus_org_123", org.id)
            .execute(&pool)
            .await
            .unwrap();

        let mut auth_headers = crate::test::utils::add_auth_headers(&user);
        auth_headers.push(("x-organization-id".to_string(), org.id.to_string()));

        let app = Router::new().route("/auto-topup/enable", post(enable_auto_topup)).with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/auto-topup/enable").json(&serde_json::json!({
            "threshold": 10.0,
            "amount": 50.0,
            "monthly_limit": 200.0
        }));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);
        let body: serde_json::Value = response.json();
        assert_eq!(body["has_payment_method"], true);

        let org_row = sqlx::query!(
            "SELECT auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit FROM users WHERE id = $1",
            org.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(org_row.auto_topup_amount, Some(50.0));
        assert_eq!(org_row.auto_topup_threshold, Some(10.0));
        assert_eq!(org_row.auto_topup_monthly_limit, Some(200.0));

        let user_row = sqlx::query!("SELECT auto_topup_amount, auto_topup_threshold FROM users WHERE id = $1", user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(user_row.auto_topup_amount, None);
        assert_eq!(user_row.auto_topup_threshold, None);
    }

    #[dwctl_test_macros::test]
    async fn test_disable_auto_topup(pool: PgPool) {
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), create_test_config()).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;

        sqlx::query!(
            "UPDATE users SET auto_topup_amount = 25.0, auto_topup_threshold = 5.0, auto_topup_monthly_limit = 100.0 WHERE id = $1",
            user.id
        )
        .execute(&pool)
        .await
        .unwrap();

        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/auto-topup/disable", post(disable_auto_topup))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/auto-topup/disable");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);

        let row = sqlx::query!(
            "SELECT auto_topup_amount, auto_topup_threshold, auto_topup_monthly_limit FROM users WHERE id = $1",
            user.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.auto_topup_amount, None);
        assert_eq!(row.auto_topup_threshold, None);
        assert_eq!(row.auto_topup_monthly_limit, None);
    }

    #[dwctl_test_macros::test]
    async fn test_disable_auto_topup_in_org_context(pool: PgPool) {
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), create_test_config()).await;

        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let org = crate::test::utils::create_test_org(&pool, user.id).await;

        sqlx::query!(
            "UPDATE users SET auto_topup_amount = 50.0, auto_topup_threshold = 10.0 WHERE id = $1",
            org.id
        )
        .execute(&pool)
        .await
        .unwrap();

        let mut auth_headers = crate::test::utils::add_auth_headers(&user);
        auth_headers.push(("x-organization-id".to_string(), org.id.to_string()));

        let app = Router::new()
            .route("/auto-topup/disable", post(disable_auto_topup))
            .with_state(state);

        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/auto-topup/disable");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);

        let org_row = sqlx::query!("SELECT auto_topup_amount, auto_topup_threshold FROM users WHERE id = $1", org.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(org_row.auto_topup_amount, None);
        assert_eq!(org_row.auto_topup_threshold, None);

        let user_row = sqlx::query!("SELECT auto_topup_amount, auto_topup_threshold FROM users WHERE id = $1", user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(user_row.auto_topup_amount, None);
        assert_eq!(user_row.auto_topup_threshold, None);
    }

    fn setup_flow_config(verification_credits: Decimal) -> crate::config::Config {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));
        config.credits.verification_credits = verification_credits;
        config
    }

    fn session_id_from(checkout_url: &str) -> String {
        let url = url::Url::parse(checkout_url).unwrap();
        let query_pairs: std::collections::HashMap<_, _> = url.query_pairs().collect();
        query_pairs.get("session_id").expect("session_id in return URL").to_string()
    }

    #[dwctl_test_macros::test]
    async fn test_create_payment_refuses_invoice_billed_accounts(pool: PgPool) {
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), setup_flow_config(Decimal::ZERO)).await;
        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new().route("/payments", post(create_payment)).with_state(state);
        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/payments");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        request.await.assert_status(StatusCode::OK);

        sqlx::query!("UPDATE users SET invoicing_enabled = true WHERE id = $1", user.id)
            .execute(&pool)
            .await
            .unwrap();

        let mut request = server.post("/payments");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;
        response.assert_status(StatusCode::CONFLICT);
        assert!(
            response.text().contains("billed by invoice"),
            "the refusal should explain why, not just fail"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_payment_setup_returns_checkout_url(pool: PgPool) {
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), setup_flow_config(Decimal::ZERO)).await;
        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new().route("/payments/setup", post(create_payment_setup)).with_state(state);
        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/payments/setup");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;

        response.assert_status(StatusCode::OK);
        let body: serde_json::Value = response.json();
        let url = body["url"].as_str().expect("Should contain checkout URL");

        assert!(url.contains("payment=success"), "URL should reuse the payment return marker");
        assert!(url.contains("session_id="), "URL should carry the session id back");
    }

    #[dwctl_test_macros::test]
    async fn test_payment_setup_no_provider(pool: PgPool) {
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), create_test_config()).await;
        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new().route("/payments/setup", post(create_payment_setup)).with_state(state);
        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/payments/setup");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        request.await.assert_status(StatusCode::SERVICE_UNAVAILABLE);
    }

    #[dwctl_test_macros::test]
    async fn test_payment_setup_verifies_user_and_grants_signup_credits(pool: PgPool) {
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), setup_flow_config(Decimal::new(25, 0))).await;
        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/payments/setup", post(create_payment_setup))
            .route("/payments/{id}", patch(process_payment))
            .with_state(state);
        let server = TestServer::new(app).unwrap();

        let before = sqlx::query!("SELECT verified, payment_provider_id FROM users WHERE id = $1", user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(!before.verified, "user starts unverified");
        assert!(before.payment_provider_id.is_none());

        let mut request = server.post("/payments/setup");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;
        response.assert_status(StatusCode::OK);
        let body: serde_json::Value = response.json();
        let session_id = session_id_from(body["url"].as_str().unwrap());

        let mut request = server.patch(&format!("/payments/{}", session_id));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        request.await.assert_status(StatusCode::OK);

        let after = sqlx::query!("SELECT verified, payment_provider_id FROM users WHERE id = $1", user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(after.verified, "verifying a card should clear the unverified tier");
        assert!(after.payment_provider_id.is_some(), "customer ID should be persisted");

        let grant: Option<Decimal> = sqlx::query_scalar!(
            "SELECT amount FROM credits_transactions WHERE user_id = $1 AND source_id LIKE 'verification-credits:%'",
            user.id
        )
        .fetch_optional(&pool)
        .await
        .unwrap();
        assert_eq!(grant, Some(Decimal::new(25, 0)), "signup credits should be granted");

        let purchases: Option<i64> = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM credits_transactions WHERE user_id = $1 AND transaction_type = 'purchase'",
            user.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(purchases, Some(0), "setup mode must not record a purchase");
    }

    #[dwctl_test_macros::test]
    async fn test_payment_setup_processing_is_idempotent(pool: PgPool) {
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), setup_flow_config(Decimal::new(25, 0))).await;
        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/payments/setup", post(create_payment_setup))
            .route("/payments/{id}", patch(process_payment))
            .with_state(state);
        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/payments/setup");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let response = request.await;
        let body: serde_json::Value = response.json();
        let session_id = session_id_from(body["url"].as_str().unwrap());

        for _ in 0..3 {
            let mut request = server.patch(&format!("/payments/{}", session_id));
            for (key, value) in &auth_headers {
                request = request.add_header(key.as_str(), value.as_str());
            }
            request.await.assert_status(StatusCode::OK);
        }

        let count: Option<i64> = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM credits_transactions WHERE user_id = $1 AND source_id LIKE 'verification-credits:%'",
            user.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, Some(1), "replaying the return must not double-grant");
    }

    #[dwctl_test_macros::test]
    async fn test_payment_setup_without_verification_credits_still_verifies(pool: PgPool) {
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), setup_flow_config(Decimal::ZERO)).await;
        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let auth_headers = crate::test::utils::add_auth_headers(&user);

        let app = Router::new()
            .route("/payments/setup", post(create_payment_setup))
            .route("/payments/{id}", patch(process_payment))
            .with_state(state);
        let server = TestServer::new(app).unwrap();

        let mut request = server.post("/payments/setup");
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        let body: serde_json::Value = request.await.json();
        let session_id = session_id_from(body["url"].as_str().unwrap());

        let mut request = server.patch(&format!("/payments/{}", session_id));
        for (key, value) in &auth_headers {
            request = request.add_header(key.as_str(), value.as_str());
        }
        request.await.assert_status(StatusCode::OK);

        let verified = sqlx::query_scalar!("SELECT verified FROM users WHERE id = $1", user.id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(verified, "verification is independent of the credits promotion");

        let count: Option<i64> = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM credits_transactions WHERE user_id = $1 AND source_id LIKE 'verification-credits:%'",
            user.id
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, Some(0), "zero credits configured means no grant row");
    }


    use crate::api::models::users::{Role, UserResponse};

    async fn checkout_cap_server(pool: &PgPool, tweak: impl FnOnce(&mut crate::config::CheckoutLimitsConfig)) -> TestServer {
        let mut config = create_test_config();
        config.payment = Some(PaymentConfig::Dummy(DummyConfig {
            amount: Decimal::new(100, 0),
        }));
        tweak(&mut config.limits.checkout);
        let state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;
        let app = Router::new()
            .route("/payments", post(create_payment))
            .route("/payments/setup", post(create_payment_setup))
            .route("/payments/{id}", patch(process_payment))
            .route("/auto-topup/checkout", post(create_auto_topup_checkout))
            .with_state(state);
        TestServer::new(app).unwrap()
    }

    async fn open_checkout(server: &TestServer, user: &UserResponse, path: &str) -> axum_test::TestResponse {
        let mut request = server.post(path);
        for (key, value) in crate::test::utils::add_auth_headers(user) {
            request = request.add_header(key.as_str(), value.as_str());
        }
        request.await
    }

    fn session_id_of(response: &axum_test::TestResponse) -> String {
        let body: serde_json::Value = response.json();
        let url = url::Url::parse(body["url"].as_str().unwrap()).unwrap();
        let pairs: std::collections::HashMap<_, _> = url.query_pairs().collect();
        pairs.get("session_id").unwrap().to_string()
    }

    async fn confirm_payment(server: &TestServer, user: &UserResponse, session_id: &str) {
        let mut request = server.patch(&format!("/payments/{}", session_id));
        for (key, value) in crate::test::utils::add_auth_headers(user) {
            request = request.add_header(key.as_str(), value.as_str());
        }
        request.await.assert_status_success();
    }

    async fn stored_customer_id(pool: &PgPool, user: &UserResponse) -> Option<String> {
        sqlx::query_scalar::<_, Option<String>>("SELECT payment_provider_id FROM users WHERE id = $1")
            .bind(user.id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_cap_refuses_after_default_unpaid_limit(pool: PgPool) {
        let server = checkout_cap_server(&pool, |_| {}).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        for attempt in 1..=5 {
            let response = open_checkout(&server, &user, "/payments").await;
            assert_eq!(response.status_code(), StatusCode::OK, "unpaid checkout {attempt} of 5 is allowed");
        }
        let refused = open_checkout(&server, &user, "/payments").await;
        refused.assert_status(StatusCode::TOO_MANY_REQUESTS);
        let body: serde_json::Value = refused.json();
        assert!(body["message"].as_str().unwrap().contains("incomplete payment attempts"));

        let other = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        open_checkout(&server, &other, "/payments").await.assert_status(StatusCode::OK);
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_cap_never_counts_paid_checkouts(pool: PgPool) {
        let server = checkout_cap_server(&pool, |limits| limits.max_unpaid_per_user = 2).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        for top_up in 1..=8 {
            let response = open_checkout(&server, &user, "/payments").await;
            assert_eq!(response.status_code(), StatusCode::OK, "paid top-up {top_up} must not be limited");
            confirm_payment(&server, &user, &session_id_of(&response)).await;
        }
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_cap_replayed_confirmation_clears_one_attempt(pool: PgPool) {
        let server = checkout_cap_server(&pool, |limits| limits.max_unpaid_per_user = 2).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        let first = open_checkout(&server, &user, "/payments/setup").await;
        first.assert_status(StatusCode::OK);
        open_checkout(&server, &user, "/payments/setup").await.assert_status(StatusCode::OK);
        open_checkout(&server, &user, "/payments/setup")
            .await
            .assert_status(StatusCode::TOO_MANY_REQUESTS);

        let session_id = session_id_of(&first);
        confirm_payment(&server, &user, &session_id).await;
        confirm_payment(&server, &user, &session_id).await;

        open_checkout(&server, &user, "/payments/setup").await.assert_status(StatusCode::OK);
        open_checkout(&server, &user, "/payments/setup")
            .await
            .assert_status(StatusCode::TOO_MANY_REQUESTS);
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_cap_is_shared_by_every_checkout_endpoint(pool: PgPool) {
        let server = checkout_cap_server(&pool, |limits| limits.max_unpaid_per_user = 3).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        open_checkout(&server, &user, "/payments").await.assert_status(StatusCode::OK);
        open_checkout(&server, &user, "/payments/setup").await.assert_status(StatusCode::OK);
        open_checkout(&server, &user, "/auto-topup/checkout")
            .await
            .assert_status(StatusCode::OK);

        for path in ["/payments", "/payments/setup", "/auto-topup/checkout"] {
            open_checkout(&server, &user, path)
                .await
                .assert_status(StatusCode::TOO_MANY_REQUESTS);
        }
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_cap_only_counts_the_rolling_window(pool: PgPool) {
        let server = checkout_cap_server(&pool, |limits| limits.max_unpaid_per_user = 2).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        open_checkout(&server, &user, "/payments").await.assert_status(StatusCode::OK);
        open_checkout(&server, &user, "/payments").await.assert_status(StatusCode::OK);
        open_checkout(&server, &user, "/payments")
            .await
            .assert_status(StatusCode::TOO_MANY_REQUESTS);

        sqlx::query("UPDATE checkout_attempts SET created_at = NOW() - INTERVAL '25 hours' WHERE payer_id = $1")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();

        open_checkout(&server, &user, "/payments").await.assert_status(StatusCode::OK);
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_cap_zero_means_unlimited(pool: PgPool) {
        let server = checkout_cap_server(&pool, |limits| limits.max_unpaid_per_user = 0).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        for _ in 0..12 {
            open_checkout(&server, &user, "/payments").await.assert_status(StatusCode::OK);
        }
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_cap_follows_the_user_across_organizations(pool: PgPool) {
        let server = checkout_cap_server(&pool, |limits| limits.max_unpaid_per_user = 3).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let org_a = crate::test::utils::create_test_org(&pool, user.id).await;
        let org_b = crate::test::utils::create_test_org(&pool, user.id).await;

        let open_as = |org: Option<crate::types::UserId>| {
            let mut request = server.post("/payments");
            for (key, value) in crate::test::utils::add_auth_headers(&user) {
                request = request.add_header(key.as_str(), value.as_str());
            }
            if let Some(org_id) = org {
                request = request.add_header("x-organization-id", org_id.to_string());
            }
            request
        };

        open_as(None).await.assert_status(StatusCode::OK);
        open_as(Some(org_a.id)).await.assert_status(StatusCode::OK);
        open_as(Some(org_b.id)).await.assert_status(StatusCode::OK);

        for org in [None, Some(org_a.id), Some(org_b.id)] {
            open_as(org).await.assert_status(StatusCode::TOO_MANY_REQUESTS);
        }

        let colleague = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        crate::test::utils::add_org_member(&pool, org_a.id, colleague.id, "admin").await;
        let mut request = server.post("/payments");
        for (key, value) in crate::test::utils::add_auth_headers(&colleague) {
            request = request.add_header(key.as_str(), value.as_str());
        }
        request
            .add_header("x-organization-id", org_a.id.to_string())
            .await
            .assert_status(StatusCode::OK);
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_cap_payment_clears_only_its_own_attempt(pool: PgPool) {
        let server = checkout_cap_server(&pool, |limits| limits.max_unpaid_per_user = 2).await;
        let owner = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let org = crate::test::utils::create_test_org(&pool, owner.id).await;
        let colleague = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        crate::test::utils::add_org_member(&pool, org.id, colleague.id, "admin").await;

        let open_for_org = |user: &UserResponse| {
            let mut request = server.post("/payments");
            for (key, value) in crate::test::utils::add_auth_headers(user) {
                request = request.add_header(key.as_str(), value.as_str());
            }
            request.add_header("x-organization-id", org.id.to_string())
        };
        let confirm_for_org = |user: &UserResponse, session_id: String| {
            let mut request = server.patch(&format!("/payments/{}", session_id));
            for (key, value) in crate::test::utils::add_auth_headers(user) {
                request = request.add_header(key.as_str(), value.as_str());
            }
            request.add_header("x-organization-id", org.id.to_string())
        };

        open_for_org(&colleague).await.assert_status(StatusCode::OK);
        open_for_org(&colleague).await.assert_status(StatusCode::OK);
        open_for_org(&colleague).await.assert_status(StatusCode::TOO_MANY_REQUESTS);

        for _ in 0..2 {
            let response = open_for_org(&owner).await;
            response.assert_status(StatusCode::OK);
            confirm_for_org(&owner, session_id_of(&response)).await.assert_status_success();
        }

        open_for_org(&colleague).await.assert_status(StatusCode::TOO_MANY_REQUESTS);
        open_for_org(&owner).await.assert_status(StatusCode::OK);
        open_for_org(&owner).await.assert_status(StatusCode::OK);
        open_for_org(&owner).await.assert_status(StatusCode::TOO_MANY_REQUESTS);
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_creates_one_customer_and_reuses_it(pool: PgPool) {
        let server = checkout_cap_server(&pool, |_| {}).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        assert_eq!(
            stored_customer_id(&pool, &user).await,
            None,
            "no customer before the first checkout"
        );

        open_checkout(&server, &user, "/payments").await.assert_status(StatusCode::OK);
        let customer = stored_customer_id(&pool, &user).await.expect("first checkout saves a customer id");

        for path in ["/payments", "/payments/setup", "/auto-topup/checkout"] {
            open_checkout(&server, &user, path).await.assert_status(StatusCode::OK);
            assert_eq!(
                stored_customer_id(&pool, &user).await.as_deref(),
                Some(customer.as_str()),
                "{path} must reuse the existing customer"
            );
        }
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_replaces_an_empty_customer_id_once(pool: PgPool) {
        let server = checkout_cap_server(&pool, |_| {}).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        sqlx::query("UPDATE users SET payment_provider_id = '' WHERE id = $1")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();

        open_checkout(&server, &user, "/payments").await.assert_status(StatusCode::OK);
        let customer = stored_customer_id(&pool, &user)
            .await
            .filter(|id| !id.is_empty())
            .expect("a customer id is saved");

        open_checkout(&server, &user, "/payments").await.assert_status(StatusCode::OK);
        assert_eq!(stored_customer_id(&pool, &user).await.as_deref(), Some(customer.as_str()));
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_cap_zero_window_still_enforces(pool: PgPool) {
        let server = checkout_cap_server(&pool, |limits| {
            limits.max_unpaid_per_user = 1;
            limits.window_hours = 0;
        })
        .await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        open_checkout(&server, &user, "/payments").await.assert_status(StatusCode::OK);
        open_checkout(&server, &user, "/payments")
            .await
            .assert_status(StatusCode::TOO_MANY_REQUESTS);
    }

    #[dwctl_test_macros::test]
    async fn test_checkout_cap_holds_for_parallel_requests(pool: PgPool) {
        let server = checkout_cap_server(&pool, |limits| limits.max_unpaid_per_user = 5).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        let responses = futures::future::join_all((0..10).map(|_| open_checkout(&server, &user, "/payments"))).await;

        let opened = responses.iter().filter(|r| r.status_code() == StatusCode::OK).count();
        let refused = responses
            .iter()
            .filter(|r| r.status_code() == StatusCode::TOO_MANY_REQUESTS)
            .count();
        assert_eq!((opened, refused), (5, 5), "exactly the cap gets through a burst of 10");
    }

    #[dwctl_test_macros::test]
    async fn test_released_reservation_does_not_count(pool: PgPool) {
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut attempts = CheckoutAttempts::new(&mut conn);

        let Reservation::Reserved(first) = attempts
            .reserve(user.id, user.id, CheckoutKind::Payment, Some(1), 24)
            .await
            .unwrap()
        else {
            panic!("the first checkout is under the cap");
        };
        assert_eq!(
            attempts
                .reserve(user.id, user.id, CheckoutKind::Payment, Some(1), 24)
                .await
                .unwrap(),
            Reservation::OverCap { unpaid: 1 }
        );

        attempts.release(first).await.unwrap();
        assert!(matches!(
            attempts
                .reserve(user.id, user.id, CheckoutKind::Payment, Some(1), 24)
                .await
                .unwrap(),
            Reservation::Reserved(_)
        ));
    }

    #[dwctl_test_macros::test]
    async fn test_refused_checkout_creates_no_customer(pool: PgPool) {
        let server = checkout_cap_server(&pool, |limits| limits.max_unpaid_per_user = 1).await;
        let user = crate::test::utils::create_test_user(&pool, Role::StandardUser).await;

        sqlx::query("INSERT INTO checkout_attempts (actor_id, payer_id, kind, session_id) VALUES ($1, $1, 'payment', 'cs_test_unpaid')")
            .bind(user.id)
            .execute(&pool)
            .await
            .unwrap();

        open_checkout(&server, &user, "/payments")
            .await
            .assert_status(StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(stored_customer_id(&pool, &user).await, None);
    }
}
