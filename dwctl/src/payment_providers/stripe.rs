
use async_trait::async_trait;
use dwctl_stripe::{
    CheckoutMode, CheckoutSession, DeclineKind, Error as StripeError, Payer, PaymentCheckout, SetupCheckout, SetupPurpose, StripeApi,
    StripeClient, verify_webhook,
};
use rust_decimal::Decimal;
use sqlx::{Connection, PgPool};

use crate::{
    db::{
        handlers::{credits::Credits, repository::Repository},
        models::credits::{CreditTransactionCreateDBRequest, CreditTransactionType},
    },
    payment_providers::{
        AutoTopupDeclineKind, AutoTopupSetupResult, CheckoutPayer, OpenedCheckout, PaymentError, PaymentProvider, PaymentSession, Result,
        WebhookEvent,
    },
    types::UserId,
};

impl From<StripeError> for PaymentError {
    fn from(error: StripeError) -> Self {
        match error {
            StripeError::Api(message) => PaymentError::ProviderApi(message),
            StripeError::InvalidData(message) => PaymentError::InvalidData(message),
            StripeError::AlreadyProcessed => PaymentError::AlreadyProcessed,
            StripeError::Declined(DeclineKind::Soft) => PaymentError::AutoTopupDeclined(AutoTopupDeclineKind::Soft),
            StripeError::Declined(DeclineKind::Hard) => PaymentError::AutoTopupDeclined(AutoTopupDeclineKind::Hard),
        }
    }
}

pub struct StripeProvider {
    config: crate::config::StripeConfig,
    client: StripeClient,
}

impl From<crate::config::StripeConfig> for StripeProvider {
    fn from(config: crate::config::StripeConfig) -> Self {
        let api_base = config.api_base().expect("Stripe api_base_url is validated at config load");
        let client = StripeClient::new(&config.api_key, api_base).expect("invalid Stripe API key or base URL");
        Self { config, client }
    }
}

fn stripe_payer<'a>(payer: &'a CheckoutPayer, reference_id: &'a str) -> Payer<'a> {
    Payer {
        reference_id,
        email: &payer.email,
        customer_id: payer.payment_provider_id.as_deref(),
    }
}

impl StripeProvider {
    async fn fulfil_setup_session(
        &self,
        conn: &mut sqlx::PgConnection,
        session: &CheckoutSession,
        session_id: &str,
        credits_config: &crate::config::CreditsConfig,
    ) -> Result<()> {
        if !session.complete {
            tracing::trace!("Setup session {} is not complete yet, skipping", session_id);
            return Err(PaymentError::PaymentNotCompleted);
        }

        let target_id: UserId = session
            .client_reference_id
            .as_deref()
            .ok_or_else(|| {
                tracing::error!("Setup session {} missing client_reference_id", session_id);
                PaymentError::InvalidData("Missing client_reference_id".to_string())
            })?
            .parse()
            .map_err(|e| {
                tracing::error!("Failed to parse setup session target ID: {:?}", e);
                PaymentError::InvalidData(format!("Invalid target user ID: {}", e))
            })?;

        let customer_id = session.customer_id.as_deref();

        let Some(setup_intent) = &session.setup_intent else {
            tracing::error!("Setup session {} has no expanded setup_intent", session_id);
            return Err(PaymentError::InvalidData("Setup intent not found or not expanded".to_string()));
        };

        if setup_intent.status != "succeeded" {
            tracing::warn!(
                session_id,
                status = setup_intent.status.as_str(),
                "Setup session completed but the SetupIntent did not succeed"
            );
            return Err(PaymentError::InvalidData("Payment method setup failed".to_string()));
        }

        if let (Some(cust_id), Some(pm_id)) = (customer_id, setup_intent.payment_method_id.as_deref())
            && !setup_intent.cvc_check_failed
        {
            self.client.set_default_payment_method(cust_id, pm_id).await;
        }

        {
            let mut users = crate::db::handlers::users::Users::new(&mut *conn);

            if users.get_by_id(target_id).await?.is_none() {
                tracing::error!(
                    "Target user {} not found for setup session {}. This indicates a data integrity issue.",
                    target_id,
                    session_id
                );
                return Err(PaymentError::InvalidData("Setup session target user not found".to_string()));
            }

            if let Some(provider_id) = customer_id
                && users.set_payment_provider_id_if_empty(target_id, provider_id).await?
            {
                tracing::debug!("Saved newly created stripe ID {} for user ID {}", provider_id, target_id);
            }
        }

        super::claim_checkout_attempt(conn, session_id).await;

        if setup_intent.cvc_check_failed {
            tracing::warn!(
                session_id,
                target_id = %target_id,
                purpose = setup_intent.purpose.as_str(),
                "Setup session card failed its CVC check; not verifying"
            );
            return match setup_intent.purpose {
                SetupPurpose::Verification => Err(PaymentError::InvalidData(
                    "Card failed its CVC check; verification requires a card that passes it".to_string(),
                )),
                SetupPurpose::AutoTopup => Ok(()),
            };
        }

        let purpose = setup_intent.purpose;
        let Some(fingerprint) = setup_intent.fingerprint.as_deref() else {
            tracing::warn!(
                session_id,
                target_id = %target_id,
                purpose = purpose.as_str(),
                "Setup session payment method has no fingerprint; not verifying"
            );
            return match purpose {
                SetupPurpose::Verification => Err(PaymentError::InvalidData(
                    "Payment method has no fingerprint; verification requires a card".to_string(),
                )),
                SetupPurpose::AutoTopup => Ok(()),
            };
        };
        let mut tx = conn.begin().await?;
        let owner = Credits::new(&mut tx).claim_verification_instrument(fingerprint, target_id).await?;
        let claimed_by_other = (owner != target_id).then_some(owner);
        if let Some(owner) = claimed_by_other {
            tx.rollback().await?;
            tracing::warn!(
                session_id,
                target_id = %target_id,
                owner = %owner,
                purpose = purpose.as_str(),
                "Payment instrument has already verified another account; not verifying this one"
            );
            return match purpose {
                SetupPurpose::Verification => Err(PaymentError::InstrumentAlreadyUsed),
                SetupPurpose::AutoTopup => Ok(()),
            };
        }

        crate::db::handlers::users::Users::new(&mut tx).set_verified(target_id).await?;
        tx.commit().await?;

        if let Err(e) = Credits::new(&mut *conn)
            .grant_verification_credits(credits_config.verification_credits, target_id, session_id)
            .await
        {
            tracing::error!(
                session_id,
                target_id = %target_id,
                error = %e,
                "Verification credits grant failed; card verification unaffected, grant manually if needed"
            );
        }

        tracing::debug!("Successfully fulfilled setup session {} for user {}", session_id, target_id);
        Ok(())
    }
}

fn parse_payment_session(checkout_session: &CheckoutSession) -> Result<PaymentSession> {
    let creditor_id: UserId = checkout_session
        .client_reference_id
        .as_deref()
        .ok_or_else(|| {
            tracing::error!("Checkout session missing client_reference_id");
            PaymentError::InvalidData("Missing client_reference_id".to_string())
        })?
        .parse()
        .map_err(|e| {
            tracing::error!("Failed to parse creditor ID: {:?}", e);
            PaymentError::InvalidData(format!("Invalid creditor user ID: {}", e))
        })?;

    let creditee_id: UserId = checkout_session
        .creditee_id
        .as_ref()
        .map(|s| s.parse())
        .transpose()
        .map_err(|e| {
            tracing::error!("Failed to parse creditee ID: {:?}", e);
            PaymentError::InvalidData(format!("Invalid creditee user ID: {}", e))
        })?
        .unwrap_or(creditor_id);

    let cents = checkout_session.pretax_cents.ok_or_else(|| {
        tracing::error!("Checkout session missing both line_items and amount_subtotal");
        PaymentError::InvalidData("Missing payment amount".to_string())
    })?;

    let amount = Decimal::new(cents, 2);

    Ok(PaymentSession {
        creditee_id,
        amount,
        is_paid: checkout_session.paid,
        creditor_id,
        payment_provider_id: checkout_session.customer_id.clone(),
    })
}

#[async_trait]
impl PaymentProvider for StripeProvider {
    async fn create_checkout_session(
        &self,
        payer: &CheckoutPayer,
        creditee_id: Option<&str>,
        cancel_url: &str,
        success_url: &str,
    ) -> Result<OpenedCheckout> {
        let payer_id = payer.id.to_string();
        let created = self
            .client
            .create_payment_checkout_session(PaymentCheckout {
                payer: stripe_payer(payer, &payer_id),
                creditee_id,
                cancel_url,
                success_url,
                price_id: &self.config.price_id,
                invoice_creation: self.config.enable_invoice_creation,
            })
            .await?;
        Ok(OpenedCheckout {
            url: created.url,
            session_id: created.id,
        })
    }

    async fn get_payment_session(&self, session_id: &str) -> Result<PaymentSession> {
        let checkout_session = self.client.get_payment_session(session_id).await?;

        parse_payment_session(&checkout_session)
    }

    async fn process_payment_session(
        &self,
        db_pool: &PgPool,
        session_id: &str,
        credits_config: &crate::config::CreditsConfig,
    ) -> Result<()> {
        let mut conn = db_pool.acquire().await?;

        {
            let mut credits = Credits::new(&mut conn);
            if credits.transaction_exists_by_source_id(session_id).await? {
                tracing::trace!("Transaction for session_id {} already exists, skipping (fast path)", session_id);
                return Ok(());
            }
        }

        let session = self.client.get_session_for_processing(session_id).await?;

        if session.mode == CheckoutMode::Setup {
            return self.fulfil_setup_session(&mut conn, &session, session_id, credits_config).await;
        }

        let payment_session = parse_payment_session(&session)?;

        if !payment_session.is_paid {
            tracing::trace!("Transaction for session_id {} has not been paid, skipping.", session_id);
            return Err(PaymentError::PaymentNotCompleted);
        }

        let description = {
            let mut users = crate::db::handlers::users::Users::new(&mut conn);

            if users.get_by_id(payment_session.creditee_id).await?.is_none() {
                return Err(PaymentError::UnknownReference(payment_session.creditee_id.to_string()));
            }
            let creditor_user = users.get_by_id(payment_session.creditor_id).await?;
            if creditor_user.is_none() {
                return Err(PaymentError::UnknownReference(payment_session.creditor_id.to_string()));
            }

            let description = if payment_session.creditor_id == payment_session.creditee_id {
                "Stripe payment".to_string()
            } else if let Some(creditor) = creditor_user.as_ref() {
                let creditor_name = creditor.display_name.as_ref().unwrap_or(&creditor.email);
                format!("Stripe payment from {}", creditor_name)
            } else {
                "Stripe payment".to_string()
            };

            if let Some(ref provider_id) = payment_session.payment_provider_id
                && users
                    .set_payment_provider_id_if_empty(payment_session.creditor_id, provider_id)
                    .await?
            {
                tracing::debug!(
                    "Saved newly created stripe ID {} for user ID {}",
                    provider_id,
                    payment_session.creditor_id
                );
            }

            description
        };

        let request = CreditTransactionCreateDBRequest {
            user_id: payment_session.creditee_id,
            transaction_type: CreditTransactionType::Purchase,
            amount: payment_session.amount,
            source_id: session_id.to_string(),
            description: Some(description),
            fusillade_batch_id: None,
            api_key_id: None,
        };

        super::claim_checkout_attempt(&mut conn, session_id).await;

        let mut credits = Credits::new(&mut conn);
        credits.create_transaction(&request).await?;

        if let Err(e) = credits
            .grant_first_payment_match(
                credits_config.first_payment_match_up_to,
                payment_session.creditee_id,
                payment_session.amount,
                session_id,
            )
            .await
        {
            tracing::error!(session_id, creditee_id = %payment_session.creditee_id, error = %e, "First-payment match failed; purchase unaffected, grant manually if needed");
        }

        crate::db::handlers::users::Users::new(&mut conn)
            .set_verified(payment_session.creditor_id)
            .await?;

        tracing::debug!(
            "Successfully fulfilled checkout session {} for user {}",
            session_id,
            payment_session.creditee_id
        );
        Ok(())
    }

    async fn validate_webhook(&self, headers: &axum::http::HeaderMap, body: &str) -> Result<Option<WebhookEvent>> {
        let signature = headers
            .get("stripe-signature")
            .ok_or_else(|| {
                tracing::error!("Missing stripe-signature header");
                PaymentError::InvalidData("Missing stripe-signature header".to_string())
            })?
            .to_str()
            .map_err(|e| {
                tracing::error!("Invalid stripe-signature header: {:?}", e);
                PaymentError::InvalidData("Invalid stripe-signature header".to_string())
            })?;

        let event = verify_webhook(body, signature, &self.config.webhook_secret)?;

        Ok(Some(WebhookEvent {
            event_type: event.event_type,
            session_id: event.session_id,
        }))
    }

    async fn process_webhook_event(
        &self,
        db_pool: &PgPool,
        event: &WebhookEvent,
        credits_config: &crate::config::CreditsConfig,
    ) -> Result<()> {
        if event.event_type != "checkout.session.completed" && event.event_type != "checkout.session.async_payment_succeeded" {
            tracing::trace!("Ignoring webhook event type: {}", event.event_type);
            return Ok(());
        }

        let session_id = event.session_id.as_ref().ok_or_else(|| {
            tracing::error!("Webhook event missing session_id");
            PaymentError::InvalidData("Missing session_id in webhook event".to_string())
        })?;

        tracing::trace!("Processing webhook event {} for session: {}", event.event_type, session_id);

        self.process_payment_session(db_pool, session_id, credits_config).await
    }

    async fn create_setup_checkout_session(&self, payer: &CheckoutPayer, cancel_url: &str, success_url: &str) -> Result<OpenedCheckout> {
        let payer_id = payer.id.to_string();
        let created = self
            .client
            .create_setup_checkout_session(SetupCheckout {
                payer: stripe_payer(payer, &payer_id),
                cancel_url,
                success_url,
                purpose: SetupPurpose::Verification,
                terms_of_service_text: self
                    .config
                    .setup_terms_of_service_text
                    .as_deref()
                    .or(self.config.auto_topup_terms_of_service_text.as_deref()),
            })
            .await?;
        Ok(OpenedCheckout {
            url: created.url,
            session_id: created.id,
        })
    }

    async fn create_auto_topup_checkout_session(
        &self,
        payer: &CheckoutPayer,
        cancel_url: &str,
        success_url: &str,
    ) -> Result<OpenedCheckout> {
        let payer_id = payer.id.to_string();
        let created = self
            .client
            .create_setup_checkout_session(SetupCheckout {
                payer: stripe_payer(payer, &payer_id),
                cancel_url,
                success_url,
                purpose: SetupPurpose::AutoTopup,
                terms_of_service_text: self.config.auto_topup_terms_of_service_text.as_deref(),
            })
            .await?;
        Ok(OpenedCheckout {
            url: created.url,
            session_id: created.id,
        })
    }

    async fn process_auto_topup_session(&self, db_pool: &PgPool, session_id: &str) -> Result<AutoTopupSetupResult> {
        let session = self.client.get_setup_session(session_id).await?;

        if !session.complete {
            return Err(PaymentError::PaymentNotCompleted);
        }

        let Some(setup_intent) = &session.setup_intent else {
            return Err(PaymentError::InvalidData("Setup intent not found or not expanded".to_string()));
        };

        if setup_intent.status != "succeeded" {
            return Err(PaymentError::InvalidData("Payment method setup failed".to_string()));
        }

        if setup_intent.cvc_check_failed {
            tracing::warn!(session_id, "Auto top-up setup card failed its CVC check; not enrolling");
            return Err(PaymentError::CardNotVerified);
        }

        if let (Some(cust_id), Some(pm_id)) = (session.customer_id.as_deref(), setup_intent.payment_method_id.as_deref()) {
            self.client.set_default_payment_method(cust_id, pm_id).await;
        }

        match db_pool.acquire().await {
            Ok(mut conn) => super::claim_checkout_attempt(&mut conn, session_id).await,
            Err(e) => tracing::error!(session_id, error = %e, "Failed to mark checkout attempt completed"),
        }

        Ok(AutoTopupSetupResult {
            customer_id: session.customer_id,
            user_id: session.client_reference_id,
        })
    }

    async fn accrue_invoice_item(&self, amount_cents: i64, customer_id: &str, description: &str, idempotency_key: &str) -> Result<String> {
        Ok(self
            .client
            .accrue_invoice_item(amount_cents, customer_id, description, idempotency_key)
            .await?)
    }

    async fn charge_auto_topup(
        &self,
        amount_cents: i64,
        customer_id: &str,
        payment_method_id: &str,
        idempotency_key: &str,
    ) -> Result<String> {
        Ok(self
            .client
            .charge_auto_topup(
                amount_cents,
                customer_id,
                payment_method_id,
                idempotency_key,
                self.config.tax_code.as_deref(),
            )
            .await?)
    }

    async fn get_default_payment_method(&self, customer_id: &str) -> Result<Option<String>> {
        Ok(self.client.get_default_payment_method(customer_id).await?)
    }

    async fn customer_has_address(&self, customer_id: &str) -> Result<bool> {
        Ok(self.client.customer_has_address(customer_id).await?)
    }

    async fn create_customer(&self, email: &str, name: Option<&str>) -> Result<String> {
        Ok(self.client.create_customer(email, name).await?)
    }

    async fn create_billing_portal_session(&self, customer_id: &str, return_url: &str) -> Result<String> {
        Ok(self.client.create_billing_portal_session(customer_id, return_url).await?)
    }
}

#[cfg(test)]
mod mock_api_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use sqlx::PgPool;
    use uuid::Uuid;

    async fn create_test_user(pool: &PgPool) -> Uuid {
        let user = crate::test::utils::create_test_user(pool, crate::api::models::users::Role::StandardUser).await;
        user.id
    }

    #[test]
    fn test_instrument_already_used_is_a_conflict_for_direct_callers() {
        let status: axum::http::StatusCode = PaymentError::InstrumentAlreadyUsed.into();
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn test_unknown_reference_is_not_found_for_direct_callers() {
        let status: axum::http::StatusCode = PaymentError::UnknownReference("some-user-id".to_string()).into();
        assert_eq!(
            status,
            axum::http::StatusCode::NOT_FOUND,
            "direct callers must be told the session is not ours"
        );
    }

    #[test]
    fn test_database_errors_are_still_retried() {
        let status: axum::http::StatusCode = PaymentError::Database(sqlx::Error::PoolClosed).into();
        assert_eq!(
            status,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "transient database failures must keep asking Stripe to retry"
        );
    }

    #[test]
    fn test_stripe_errors_map_onto_payment_errors() {
        let err = PaymentError::from(StripeError::AlreadyProcessed);
        assert!(matches!(err, PaymentError::AlreadyProcessed), "got {err:?}");

        let err = PaymentError::from(StripeError::Declined(DeclineKind::Hard));
        assert!(
            matches!(err, PaymentError::AutoTopupDeclined(AutoTopupDeclineKind::Hard)),
            "got {err:?}"
        );
        let err = PaymentError::from(StripeError::Declined(DeclineKind::Soft));
        assert!(
            matches!(err, PaymentError::AutoTopupDeclined(AutoTopupDeclineKind::Soft)),
            "got {err:?}"
        );

        let err = PaymentError::from(StripeError::Api("boom".to_string()));
        assert!(
            matches!(&err, PaymentError::ProviderApi(message) if message == "boom"),
            "got {err:?}"
        );
        let err = PaymentError::from(StripeError::InvalidData("bad id".to_string()));
        assert!(
            matches!(&err, PaymentError::InvalidData(message) if message == "bad id"),
            "got {err:?}"
        );
    }

    #[test]
    fn test_stripe_provider_from_config() {
        let config = crate::config::StripeConfig {
            api_key: "sk_test_fake".to_string(),
            price_id: "price_fake".to_string(),
            webhook_secret: "whsec_fake".to_string(),
            enable_invoice_creation: false,
            auto_topup_terms_of_service_text: None,
            setup_terms_of_service_text: None,
            tax_code: None,
            api_base_url: None,
        };
        let provider = StripeProvider::from(config);

        assert_eq!(provider.config.api_key, "sk_test_fake");
        assert_eq!(provider.config.price_id, "price_fake");
        assert_eq!(provider.config.webhook_secret, "whsec_fake");
        assert!(!provider.config.enable_invoice_creation);
    }

    #[test]
    fn test_stripe_provider_with_invoice_creation() {
        let config = crate::config::StripeConfig {
            api_key: "sk_test_fake".to_string(),
            price_id: "price_fake".to_string(),
            webhook_secret: "whsec_fake".to_string(),
            enable_invoice_creation: true,
            auto_topup_terms_of_service_text: None,
            setup_terms_of_service_text: None,
            tax_code: None,
            api_base_url: None,
        };
        let provider = StripeProvider::from(config);

        assert!(provider.config.enable_invoice_creation);
    }

    #[dwctl_test_macros::test]
    async fn test_stripe_idempotency_fast_path(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let session_id = "cs_test_fake_session_123";

        let mut conn = pool.acquire().await.unwrap();
        let mut credits = crate::db::handlers::Credits::new(&mut conn);

        let request = crate::db::models::credits::CreditTransactionCreateDBRequest {
            user_id,
            transaction_type: crate::db::models::credits::CreditTransactionType::Purchase,
            amount: Decimal::new(5000, 2),
            source_id: session_id.to_string(),
            description: Some("Test Stripe payment".to_string()),
            fusillade_batch_id: None,
            api_key_id: None,
        };

        credits.create_transaction(&request).await.unwrap();

        let config = crate::config::StripeConfig {
            api_key: "sk_test_fake".to_string(),
            price_id: "price_fake".to_string(),
            webhook_secret: "whsec_fake".to_string(),
            enable_invoice_creation: false,
            auto_topup_terms_of_service_text: None,
            setup_terms_of_service_text: None,
            tax_code: None,
            api_base_url: None,
        };
        let provider = StripeProvider::from(config);

        let result = provider
            .process_payment_session(&pool, session_id, &crate::config::CreditsConfig::default())
            .await;
        assert!(result.is_ok(), "Should succeed via fast path (transaction already exists)");

        let count = sqlx::query!(
            r#"
            SELECT COUNT(*) as count
            FROM credits_transactions
            WHERE source_id = $1
            "#,
            session_id
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(count.count.unwrap(), 1, "Should still have exactly one transaction");
    }

    fn paid_session(pretax_cents: Option<i64>) -> CheckoutSession {
        CheckoutSession {
            mode: CheckoutMode::Payment,
            complete: true,
            paid: true,
            client_reference_id: Some("550e8400-e29b-41d4-a716-446655440001".to_string()),
            customer_id: Some("cus_test123".to_string()),
            creditee_id: None,
            pretax_cents,
            setup_intent: None,
        }
    }

    #[test]
    fn test_parse_payment_session_credits_the_pretax_subtotal() {
        assert_eq!(
            parse_payment_session(&paid_session(Some(2500))).expect("session parses").amount,
            Decimal::new(2500, 2)
        );
    }

    #[test]
    fn test_parse_payment_session_keeps_partial_dollars() {
        let parsed = parse_payment_session(&paid_session(Some(1050))).expect("session parses");
        assert_eq!(parsed.amount, Decimal::new(1050, 2), "$10.50 must credit 10.50, not 10");
        assert_ne!(parsed.amount, Decimal::from(10));
    }

    #[test]
    fn test_payment_session_parsing() {
        let creditee_id = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        let creditor_id = "550e8400-e29b-41d4-a716-446655440001".parse().unwrap();

        let session = PaymentSession {
            creditee_id,
            creditor_id,
            amount: Decimal::new(5000, 2),
            is_paid: true,
            payment_provider_id: Some("cus_test123".to_string()),
        };

        assert_eq!(session.creditee_id, creditee_id);
        assert_eq!(session.creditor_id, creditor_id);
        assert_eq!(session.amount, Decimal::new(5000, 2));
        assert!(session.is_paid);
        assert_eq!(session.payment_provider_id, Some("cus_test123".to_string()));
    }

    #[test]
    fn test_webhook_event_parsing() {
        let event = WebhookEvent {
            event_type: "CheckoutSessionCompleted".to_string(),
            session_id: Some("cs_test_123".to_string()),
        };

        assert_eq!(event.event_type, "CheckoutSessionCompleted");
        assert_eq!(event.session_id, Some("cs_test_123".to_string()));
    }

    #[dwctl_test_macros::test]
    async fn test_payment_description_self(pool: PgPool) {
        let user = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;

        let customer_id = "cus_test_self_payment";
        sqlx::query!("UPDATE users SET payment_provider_id = $1 WHERE id = $2", customer_id, user.id)
            .execute(&pool)
            .await
            .unwrap();

        let payment_session = PaymentSession {
            creditee_id: user.id,
            creditor_id: user.id,
            amount: Decimal::new(5000, 2),
            is_paid: true,
            payment_provider_id: Some(customer_id.to_string()),
        };

        let description = if payment_session.creditor_id == payment_session.creditee_id {
            "Stripe payment".to_string()
        } else {
            let mut conn = pool.acquire().await.unwrap();
            let mut users = crate::db::handlers::users::Users::new(&mut conn);

            if let Some(creditor) = users.get_by_id(payment_session.creditor_id).await.unwrap() {
                let creditor_name = creditor.display_name.unwrap_or(creditor.email);
                format!("Stripe payment from {}", creditor_name)
            } else {
                "Stripe payment".to_string()
            }
        };

        assert_eq!(description, "Stripe payment", "Self-payment should not include 'from' attribution");
    }

    #[dwctl_test_macros::test]
    async fn test_payment_description_other(pool: PgPool) {
        let payer = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let recipient = crate::test::utils::create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;

        let customer_id = "cus_test_other_payment";
        sqlx::query!(
            "UPDATE users SET payment_provider_id = $1, display_name = $2 WHERE id = $3",
            customer_id,
            "John Admin",
            payer.id
        )
        .execute(&pool)
        .await
        .unwrap();

        let payment_session = PaymentSession {
            creditee_id: recipient.id,
            creditor_id: payer.id,
            amount: Decimal::new(5000, 2),
            is_paid: true,
            payment_provider_id: Some(customer_id.to_string()),
        };

        let description = if payment_session.creditor_id == payment_session.creditee_id {
            "Stripe payment".to_string()
        } else {
            let mut conn = pool.acquire().await.unwrap();
            let mut users = crate::db::handlers::users::Users::new(&mut conn);

            if let Some(creditor) = users.get_by_id(payment_session.creditor_id).await.unwrap() {
                let creditor_name = creditor.display_name.unwrap_or(creditor.email);
                format!("Stripe payment from {}", creditor_name)
            } else {
                "Stripe payment".to_string()
            }
        };

        assert_eq!(
            description, "Stripe payment from John Admin",
            "Payment for others should include 'from' attribution"
        );
    }
}
