
use async_trait::async_trait;
use rust_decimal::Decimal;
use sqlx::{Connection, PgPool};
use std::collections::HashMap;
use stripe::{ApiErrorsCode, Client, ClientBuilder};
use stripe_billing::billing_portal_session::CreateBillingPortalSession;
use stripe_checkout::checkout_session::{
    CreateCheckoutSessionConsentCollection, CreateCheckoutSessionConsentCollectionPaymentMethodReuseAgreement,
    CreateCheckoutSessionConsentCollectionPaymentMethodReuseAgreementPosition, CreateCheckoutSessionConsentCollectionTermsOfService,
    CreateCheckoutSessionCustomText, CreateCheckoutSessionCustomerUpdate, CreateCheckoutSessionCustomerUpdateAddress,
    CreateCheckoutSessionCustomerUpdateName, CreateCheckoutSessionInvoiceCreation, CreateCheckoutSessionNameCollection,
    CreateCheckoutSessionNameCollectionBusiness, CreateCheckoutSessionPaymentMethodTypes, CreateCheckoutSessionSavedPaymentMethodOptions,
    CreateCheckoutSessionSavedPaymentMethodOptionsPaymentMethodRemove, CreateCheckoutSessionSavedPaymentMethodOptionsPaymentMethodSave,
    CreateCheckoutSessionSetupIntentData, CustomTextPositionParam,
};
use stripe_checkout::{
    CheckoutSessionId, CheckoutSessionMode, CheckoutSessionPaymentStatus, CheckoutSessionStatus, CheckoutSessionUiMode,
    checkout_session::{
        CreateCheckoutSession, CreateCheckoutSessionAutomaticTax, CreateCheckoutSessionCustomerCreation, CreateCheckoutSessionLineItems,
        CreateCheckoutSessionTaxIdCollection, RetrieveCheckoutSession,
    },
};
use stripe_types::Currency;
use stripe_webhook::{EventObject, Webhook};

use crate::{
    db::{
        handlers::{credits::Credits, repository::Repository},
        models::credits::{CreditTransactionCreateDBRequest, CreditTransactionType},
    },
    payment_providers::{
        AutoTopupDeclineKind, AutoTopupSetupResult, CheckoutPayer, PaymentError, PaymentProvider, PaymentSession, Result, WebhookEvent,
    },
    types::UserId,
};

fn classify_card_decline(advice_code: Option<&str>, decline_code: Option<&str>) -> AutoTopupDeclineKind {
    const HARD_DECLINE_CODES: &[&str] = &[
        "do_not_honor",
        "fraudulent",
        "lost_card",
        "merchant_blacklist",
        "pickup_card",
        "restricted_card",
        "revocation_of_all_authorizations",
        "revocation_of_authorization",
        "security_violation",
        "stolen_card",
        "stop_payment_order",
        "transaction_not_allowed",
    ];

    if advice_code == Some("do_not_try_again") || decline_code.is_some_and(|code| HARD_DECLINE_CODES.contains(&code)) {
        AutoTopupDeclineKind::Hard
    } else {
        AutoTopupDeclineKind::Soft
    }
}

fn map_auto_topup_charge_error(error: stripe::StripeError) -> PaymentError {
    match error {
        stripe::StripeError::Stripe(api_error, _) if api_error.code == Some(ApiErrorsCode::IdempotencyKeyInUse) => {
            PaymentError::AlreadyProcessed
        }
        stripe::StripeError::Stripe(api_error, _) if matches!(api_error.type_, stripe::ApiErrorsType::CardError) => {
            PaymentError::AutoTopupDeclined(classify_card_decline(
                api_error.advice_code.as_deref(),
                api_error.decline_code.as_deref(),
            ))
        }
        stripe::StripeError::Stripe(api_error, status) => {
            tracing::error!(
                status,
                error_type = %api_error.type_,
                "Stripe rejected the auto top-up payment request"
            );
            PaymentError::ProviderApi(format!("Stripe {} error (HTTP {status})", api_error.type_))
        }
        other => {
            tracing::error!(error = %other, "Failed to create auto top-up payment intent");
            PaymentError::ProviderApi(other.to_string())
        }
    }
}

pub struct StripeProvider {
    config: crate::config::StripeConfig,
    client: Client,
}

impl From<crate::config::StripeConfig> for StripeProvider {
    fn from(config: crate::config::StripeConfig) -> Self {
        let mut builder = ClientBuilder::new(&config.api_key);
        if let Some(api_base) = config.api_base().expect("Stripe api_base_url is validated at config load") {
            builder = builder.url(api_base);
        }
        let client = builder.build().expect("invalid Stripe API key or base URL");
        Self { config, client }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SetupPurpose {
    Verification,
    AutoTopup,
}

impl SetupPurpose {
    const METADATA_KEY: &'static str = "purpose";

    fn as_str(self) -> &'static str {
        match self {
            SetupPurpose::Verification => "verification",
            SetupPurpose::AutoTopup => "auto_topup",
        }
    }

    fn from_metadata(metadata: Option<&HashMap<String, String>>) -> Self {
        match metadata.and_then(|m| m.get(Self::METADATA_KEY)).map(String::as_str) {
            Some("auto_topup") => SetupPurpose::AutoTopup,
            _ => SetupPurpose::Verification,
        }
    }
}

fn instrument_fingerprint(pm: &stripe_shared::PaymentMethod) -> Option<String> {
    pm.card
        .as_ref()
        .and_then(|card| card.fingerprint.clone())
        .or_else(|| pm.sepa_debit.as_ref().and_then(|sepa| sepa.fingerprint.clone()))
}

struct SetupSessionCopy<'a> {
    terms_of_service_text: Option<&'a str>,
    submit_label: &'a str,
    setup_intent_description: &'a str,
    purpose: SetupPurpose,
    payment_method_types: Vec<CreateCheckoutSessionPaymentMethodTypes>,
}

impl StripeProvider {
    async fn create_setup_mode_session(
        &self,
        payer: &CheckoutPayer,
        cancel_url: &str,
        success_url: &str,
        copy: SetupSessionCopy<'_>,
    ) -> Result<String> {
        let mut checkout_params = CreateCheckoutSession::new()
            .cancel_url(cancel_url)
            .success_url(success_url)
            .client_reference_id(payer.id.to_string())
            .mode(CheckoutSessionMode::Setup)
            .ui_mode(CheckoutSessionUiMode::HostedPage)
            .tax_id_collection(CreateCheckoutSessionTaxIdCollection::new(true))
            .name_collection(CreateCheckoutSessionNameCollection {
                business: Some(CreateCheckoutSessionNameCollectionBusiness::new(true)),
                individual: None,
            })
            .consent_collection(CreateCheckoutSessionConsentCollection {
                terms_of_service: Some(CreateCheckoutSessionConsentCollectionTermsOfService::Required),
                payment_method_reuse_agreement: Some(CreateCheckoutSessionConsentCollectionPaymentMethodReuseAgreement::new(
                    CreateCheckoutSessionConsentCollectionPaymentMethodReuseAgreementPosition::Auto,
                )),
                promotions: None,
            })
            .custom_text(CreateCheckoutSessionCustomText {
                terms_of_service_acceptance: copy.terms_of_service_text.map(CustomTextPositionParam::new),
                submit: Some(CustomTextPositionParam::new(copy.submit_label)),
                after_submit: None,
                shipping_address: None,
            })
            .payment_method_types(copy.payment_method_types.clone())
            .setup_intent_data(CreateCheckoutSessionSetupIntentData {
                description: Some(copy.setup_intent_description.to_string()),
                metadata: Some(HashMap::from([(
                    SetupPurpose::METADATA_KEY.to_string(),
                    copy.purpose.as_str().to_string(),
                )])),
                on_behalf_of: None,
            });

        if let Some(existing_id) = &payer.payment_provider_id {
            tracing::debug!("Using existing Stripe customer ID {} for payer {}", existing_id, payer.id);
            checkout_params = checkout_params
                .customer(existing_id)
                .customer_update(CreateCheckoutSessionCustomerUpdate {
                    address: Some(CreateCheckoutSessionCustomerUpdateAddress::Auto),
                    name: Some(CreateCheckoutSessionCustomerUpdateName::Auto),
                    shipping: None,
                })
        } else {
            tracing::debug!("No customer ID found for payer {}, Stripe will create one", payer.id);
            checkout_params = checkout_params
                .customer_email(&payer.email)
                .customer_creation(CreateCheckoutSessionCustomerCreation::Always);
        }

        let checkout_session = checkout_params.send(&self.client).await.map_err(|e| {
            tracing::error!(
                purpose = copy.setup_intent_description,
                "Failed to create Stripe setup checkout session: {:?}",
                e
            );
            PaymentError::ProviderApi(e.to_string())
        })?;

        tracing::debug!(
            purpose = copy.setup_intent_description,
            "Created setup checkout session {} for payer {}",
            checkout_session.id,
            payer.id
        );

        checkout_session.url.ok_or_else(|| {
            tracing::error!("Setup checkout session missing URL");
            PaymentError::ProviderApi("Checkout session missing URL".to_string())
        })
    }

    async fn charge_auto_topup_internal(
        &self,
        amount_cents: i64,
        customer_id: &str,
        payment_method_id: &str,
        idempotency_key: &str,
    ) -> Result<stripe_core::PaymentIntent> {
        use stripe::{IdempotencyKey, RequestStrategy, StripeRequest};
        use stripe_core::payment_intent::{
            AsyncWorkflowsInputsParam, AsyncWorkflowsInputsTaxParam, AsyncWorkflowsParam, CreatePaymentIntent,
            CreatePaymentIntentOffSession,
        };
        use stripe_misc::tax_calculation::{CreateTaxCalculation, CreateTaxCalculationLineItems};

        let mut line_item = CreateTaxCalculationLineItems::new(amount_cents);
        line_item.reference = Some("auto_topup".to_string());
        line_item.tax_code = self.config.tax_code.clone();

        let tax_idem_key = IdempotencyKey::new(format!("{}_tax", idempotency_key))
            .map_err(|e| PaymentError::InvalidData(format!("Invalid tax idempotency key: {e}")))?;

        let tax_calc = CreateTaxCalculation::new(Currency::USD, vec![line_item])
            .customer(customer_id)
            .customize()
            .request_strategy(RequestStrategy::Idempotent(tax_idem_key))
            .send(&self.client)
            .await
            .map_err(map_auto_topup_charge_error)?;

        let tax_calc_id = tax_calc
            .id
            .ok_or_else(|| PaymentError::ProviderApi("Tax calculation missing ID".to_string()))?;

        let idem_key =
            IdempotencyKey::new(idempotency_key).map_err(|e| PaymentError::InvalidData(format!("Invalid idempotency key: {e}")))?;

        CreatePaymentIntent::new(tax_calc.amount_total, Currency::USD)
            .customer(customer_id)
            .payment_method(payment_method_id)
            .off_session(CreatePaymentIntentOffSession::Bool(true))
            .confirm(true)
            .description("Automatic credit top-up")
            .statement_descriptor_suffix("AUTO-TOPUP")
            .hooks(AsyncWorkflowsParam {
                inputs: Some(AsyncWorkflowsInputsParam {
                    tax: Some(AsyncWorkflowsInputsTaxParam::new(tax_calc_id.to_string())),
                }),
            })
            .customize()
            .request_strategy(RequestStrategy::Idempotent(idem_key))
            .send(&self.client)
            .await
            .map_err(map_auto_topup_charge_error)
    }

    async fn get_setup_session(&self, session_id: &str) -> Result<stripe_checkout::CheckoutSession> {
        let session_id: CheckoutSessionId = session_id
            .parse()
            .map_err(|_| PaymentError::InvalidData("Invalid Stripe session ID".to_string()))?;

        RetrieveCheckoutSession::new(session_id)
            .expand(vec!["setup_intent".to_string(), "setup_intent.payment_method".to_string()])
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to retrieve Stripe setup session: {:?}", e);
                PaymentError::ProviderApi(e.to_string())
            })
    }

    async fn get_session_for_processing(&self, session_id: &str) -> Result<stripe_checkout::CheckoutSession> {
        let session_id: CheckoutSessionId = session_id
            .parse()
            .map_err(|_| PaymentError::InvalidData("Invalid Stripe session ID".to_string()))?;

        RetrieveCheckoutSession::new(session_id)
            .expand(vec![
                "line_items".to_string(),
                "setup_intent".to_string(),
                "setup_intent.payment_method".to_string(),
            ])
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to retrieve Stripe checkout session: {:?}", e);
                PaymentError::ProviderApi(e.to_string())
            })
    }

    async fn fulfil_setup_session(
        &self,
        conn: &mut sqlx::PgConnection,
        session: &stripe_checkout::CheckoutSession,
        session_id: &str,
        credits_config: &crate::config::CreditsConfig,
    ) -> Result<()> {
        if session.status != Some(CheckoutSessionStatus::Complete) {
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

        let customer_id = match &session.customer {
            Some(stripe_types::Expandable::Id(id)) => Some(id.to_string()),
            Some(stripe_types::Expandable::Object(c)) => Some(c.id.to_string()),
            None => None,
        };

        let setup_intent = match &session.setup_intent {
            Some(stripe_types::Expandable::Object(si)) => si.as_ref(),
            _ => {
                tracing::error!("Setup session {} has no expanded setup_intent", session_id);
                return Err(PaymentError::InvalidData("Setup intent not found or not expanded".to_string()));
            }
        };

        if setup_intent.status.as_str() != "succeeded" {
            tracing::warn!(
                session_id,
                status = setup_intent.status.as_str(),
                "Setup session completed but the SetupIntent did not succeed"
            );
            return Err(PaymentError::InvalidData("Payment method setup failed".to_string()));
        }

        if let (Some(cust_id), Some(pm)) = (&customer_id, &setup_intent.payment_method) {
            let pm_id = pm.id().to_string();
            let mut invoice_settings = stripe_core::customer::UpdateCustomerInvoiceSettings::new();
            invoice_settings.default_payment_method = Some(pm_id.clone());

            if let Err(e) = stripe_core::customer::UpdateCustomer::new(cust_id.as_str())
                .invoice_settings(invoice_settings)
                .send(&self.client)
                .await
            {
                tracing::warn!("Failed to set default payment method {} on customer {}: {:?}", pm_id, cust_id, e);
            }
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

            if let Some(ref provider_id) = customer_id
                && users.set_payment_provider_id_if_empty(target_id, provider_id).await?
            {
                tracing::debug!("Saved newly created stripe ID {} for user ID {}", provider_id, target_id);
            }
        }

        let purpose = SetupPurpose::from_metadata(setup_intent.metadata.as_ref());
        let instrument_fingerprint = setup_intent
            .payment_method
            .as_ref()
            .and_then(|pm| pm.as_object())
            .and_then(instrument_fingerprint);
        let Some(fingerprint) = instrument_fingerprint.as_deref() else {
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

fn parse_payment_session(checkout_session: &stripe_checkout::CheckoutSession) -> Result<PaymentSession> {
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
        .metadata
        .as_ref()
        .and_then(|m| m.get("creditee_id"))
        .map(|s| s.parse())
        .transpose()
        .map_err(|e| {
            tracing::error!("Failed to parse creditee ID: {:?}", e);
            PaymentError::InvalidData(format!("Invalid creditee user ID: {}", e))
        })?
        .unwrap_or(creditor_id);

    let cents = pretax_credit_cents(checkout_session).ok_or_else(|| {
        tracing::error!("Checkout session missing both line_items and amount_subtotal");
        PaymentError::InvalidData("Missing payment amount".to_string())
    })?;

    let amount = Decimal::new(cents, 2);

    Ok(PaymentSession {
        creditee_id,
        amount,
        is_paid: checkout_session.payment_status == CheckoutSessionPaymentStatus::Paid,
        creditor_id,
        payment_provider_id: checkout_session.customer.as_ref().map(|c| c.id().to_string()),
    })
}

fn pretax_credit_cents(session: &stripe_checkout::CheckoutSession) -> Option<i64> {
    session
        .line_items
        .as_ref()
        .and_then(|items| items.data.first().map(|item| item.amount_subtotal))
        .or(session.amount_subtotal)
}

#[async_trait]
impl PaymentProvider for StripeProvider {
    async fn create_checkout_session(
        &self,
        payer: &CheckoutPayer,
        creditee_id: Option<&str>,
        cancel_url: &str,
        success_url: &str,
    ) -> Result<String> {
        let mut checkout_params = CreateCheckoutSession::new()
            .cancel_url(cancel_url)
            .success_url(success_url)
            .client_reference_id(payer.id.to_string())
            .currency(Currency::USD)
            .line_items(vec![CreateCheckoutSessionLineItems {
                price: Some(self.config.price_id.clone()),
                quantity: Some(1),
                ..Default::default()
            }])
            .automatic_tax(CreateCheckoutSessionAutomaticTax::new(true))
            .mode(CheckoutSessionMode::Payment)
            .ui_mode(CheckoutSessionUiMode::HostedPage)
            .expand(vec!["line_items".to_string()])
            .tax_id_collection(CreateCheckoutSessionTaxIdCollection::new(true))
            .name_collection(CreateCheckoutSessionNameCollection {
                business: Some(CreateCheckoutSessionNameCollectionBusiness::new(true)),
                individual: None,
            })
            .saved_payment_method_options(CreateCheckoutSessionSavedPaymentMethodOptions {
                allow_redisplay_filters: None,
                payment_method_save: Some(CreateCheckoutSessionSavedPaymentMethodOptionsPaymentMethodSave::Enabled),
                payment_method_remove: Some(CreateCheckoutSessionSavedPaymentMethodOptionsPaymentMethodRemove::Enabled),
            });

        if let Some(user_receiving_credits) = creditee_id {
            let mut metadata = HashMap::new();
            metadata.insert("creditee_id".to_string(), user_receiving_credits.to_string());
            checkout_params = checkout_params.metadata(metadata);
        }

        if self.config.enable_invoice_creation {
            checkout_params = checkout_params.invoice_creation(CreateCheckoutSessionInvoiceCreation::new(true));
        }

        if let Some(existing_id) = &payer.payment_provider_id {
            tracing::debug!("Using existing Stripe customer ID {} for payer {}", existing_id, payer.id);
            checkout_params = checkout_params
                .customer(existing_id)
                .customer_update(CreateCheckoutSessionCustomerUpdate {
                    address: Some(CreateCheckoutSessionCustomerUpdateAddress::Auto),
                    name: Some(CreateCheckoutSessionCustomerUpdateName::Auto),
                    shipping: None,
                })
        } else {
            tracing::debug!("No customer ID found for payer {}, Stripe will create one", payer.id);
            checkout_params = checkout_params
                .customer_email(&payer.email)
                .customer_creation(CreateCheckoutSessionCustomerCreation::Always);
        }

        let checkout_session = checkout_params.send(&self.client).await.map_err(|e| {
            tracing::error!("Failed to create Stripe checkout session: {:?}", e);
            PaymentError::ProviderApi(e.to_string())
        })?;

        tracing::debug!(
            "Created checkout session {} for creditee {} (payer: {})",
            checkout_session.id,
            creditee_id.unwrap_or(&payer.id.to_string()),
            payer.id
        );

        checkout_session.url.ok_or_else(|| {
            tracing::error!("Checkout session missing URL");
            PaymentError::ProviderApi("Checkout session missing URL".to_string())
        })
    }

    async fn get_payment_session(&self, session_id: &str) -> Result<PaymentSession> {
        let session_id: CheckoutSessionId = session_id
            .parse()
            .map_err(|_| PaymentError::InvalidData("Invalid Stripe session ID".to_string()))?;

        let checkout_session = RetrieveCheckoutSession::new(session_id)
            .expand(vec!["line_items".to_string()])
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to retrieve Stripe checkout session: {:?}", e);
                PaymentError::ProviderApi(e.to_string())
            })?;

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

        let session = self.get_session_for_processing(session_id).await?;

        if session.mode == CheckoutSessionMode::Setup {
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

        let event = Webhook::construct_event(body, signature, &self.config.webhook_secret).map_err(|e| {
            tracing::error!("Failed to construct webhook event: {:?}", e);
            PaymentError::InvalidData(format!("Webhook validation failed: {}", e))
        })?;

        tracing::trace!("Validated Stripe webhook event: {:?}", event.type_);

        let session_id = match &event.data.object {
            EventObject::CheckoutSessionCompleted(session) | EventObject::CheckoutSessionAsyncPaymentSucceeded(session) => {
                Some(session.id.to_string())
            }
            _ => None,
        };

        let webhook_event = WebhookEvent {
            event_type: event.type_.to_string(),
            session_id,
        };

        Ok(Some(webhook_event))
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

    async fn create_setup_checkout_session(&self, payer: &CheckoutPayer, cancel_url: &str, success_url: &str) -> Result<String> {
        self.create_setup_mode_session(
            payer,
            cancel_url,
            success_url,
            SetupSessionCopy {
                terms_of_service_text: self
                    .config
                    .setup_terms_of_service_text
                    .as_deref()
                    .or(self.config.auto_topup_terms_of_service_text.as_deref()),
                submit_label: "Verify payment method",
                setup_intent_description: "Payment method verification",
                purpose: SetupPurpose::Verification,
                payment_method_types: vec![CreateCheckoutSessionPaymentMethodTypes::Card],
            },
        )
        .await
    }

    async fn create_auto_topup_checkout_session(&self, payer: &CheckoutPayer, cancel_url: &str, success_url: &str) -> Result<String> {
        self.create_setup_mode_session(
            payer,
            cancel_url,
            success_url,
            SetupSessionCopy {
                terms_of_service_text: self.config.auto_topup_terms_of_service_text.as_deref(),
                submit_label: "Set up auto top-up",
                setup_intent_description: "Auto top-up setup",
                purpose: SetupPurpose::AutoTopup,
                payment_method_types: vec![
                    CreateCheckoutSessionPaymentMethodTypes::Card,
                    CreateCheckoutSessionPaymentMethodTypes::Link,
                    CreateCheckoutSessionPaymentMethodTypes::SepaDebit,
                ],
            },
        )
        .await
    }

    async fn process_auto_topup_session(&self, _db_pool: &PgPool, session_id: &str) -> Result<AutoTopupSetupResult> {
        let session = self.get_setup_session(session_id).await?;

        if session.status != Some(CheckoutSessionStatus::Complete) {
            return Err(PaymentError::PaymentNotCompleted);
        }

        let customer_id = match &session.customer {
            Some(stripe_types::Expandable::Id(id)) => Some(id.to_string()),
            Some(stripe_types::Expandable::Object(c)) => Some(c.id.to_string()),
            None => None,
        };

        let setup_intent = match session.setup_intent {
            Some(stripe_types::Expandable::Object(si)) => *si,
            _ => return Err(PaymentError::InvalidData("Setup intent not found or not expanded".to_string())),
        };

        if setup_intent.status.as_str() != "succeeded" {
            return Err(PaymentError::InvalidData("Payment method setup failed".to_string()));
        }

        if let (Some(cust_id), Some(pm)) = (&customer_id, &setup_intent.payment_method) {
            let pm_id = pm.id().to_string();
            let mut invoice_settings = stripe_core::customer::UpdateCustomerInvoiceSettings::new();
            invoice_settings.default_payment_method = Some(pm_id.clone());

            if let Err(e) = stripe_core::customer::UpdateCustomer::new(cust_id.as_str())
                .invoice_settings(invoice_settings)
                .send(&self.client)
                .await
            {
                tracing::warn!("Failed to set default payment method {} on customer {}: {:?}", pm_id, cust_id, e);
            }
        }

        Ok(AutoTopupSetupResult {
            customer_id,
            user_id: session.client_reference_id,
        })
    }

    async fn accrue_invoice_item(&self, amount_cents: i64, customer_id: &str, description: &str, idempotency_key: &str) -> Result<String> {
        use stripe::{IdempotencyKey, RequestStrategy, StripeRequest};
        use stripe_billing::invoice_item::CreateInvoiceItem;

        let idem = IdempotencyKey::new(idempotency_key).map_err(|e| PaymentError::InvalidData(format!("Invalid idempotency key: {e}")))?;

        let item = CreateInvoiceItem::new()
            .customer(customer_id)
            .amount(amount_cents)
            .currency(Currency::USD)
            .description(description)
            .customize()
            .request_strategy(RequestStrategy::Idempotent(idem))
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to create Stripe invoice item: {:?}", e);
                PaymentError::ProviderApi(e.to_string())
            })?;

        tracing::info!(
            invoice_item_id = %item.id,
            customer_id,
            amount_cents,
            "Accrued charge onto the customer's next invoice"
        );

        Ok(item.id.to_string())
    }

    async fn charge_auto_topup(
        &self,
        amount_cents: i64,
        customer_id: &str,
        payment_method_id: &str,
        idempotency_key: &str,
    ) -> Result<String> {
        let pi = self
            .charge_auto_topup_internal(amount_cents, customer_id, payment_method_id, idempotency_key)
            .await?;
        Ok(pi.id.to_string())
    }

    async fn get_default_payment_method(&self, customer_id: &str) -> Result<Option<String>> {
        use stripe_core::customer::{ListPaymentMethodsCustomer, RetrieveCustomer, RetrieveCustomerReturned};

        let result = RetrieveCustomer::new(customer_id.to_string())
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to retrieve Stripe customer: {:?}", e);
                PaymentError::ProviderApi(e.to_string())
            })?;

        let customer = match result {
            RetrieveCustomerReturned::Customer(c) => c,
            RetrieveCustomerReturned::DeletedCustomer(_) => {
                tracing::warn!("Stripe customer {} has been deleted", customer_id);
                return Ok(None);
            }
        };

        let pm = customer
            .invoice_settings
            .and_then(|s| s.default_payment_method)
            .map(|expandable: stripe_types::Expandable<_>| expandable.id().to_string());

        if pm.is_some() {
            return Ok(pm);
        }

        let methods = ListPaymentMethodsCustomer::new(customer_id.to_string())
            .limit(1)
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to list payment methods for customer {}: {:?}", customer_id, e);
                PaymentError::ProviderApi(e.to_string())
            })?;

        Ok(methods.data.first().map(|pm| pm.id.to_string()))
    }

    async fn customer_has_address(&self, customer_id: &str) -> Result<bool> {
        use stripe_core::customer::{RetrieveCustomer, RetrieveCustomerReturned};

        let result = RetrieveCustomer::new(customer_id.to_string())
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to retrieve Stripe customer for address check: {:?}", e);
                PaymentError::ProviderApi(e.to_string())
            })?;

        let customer = match result {
            RetrieveCustomerReturned::Customer(c) => c,
            RetrieveCustomerReturned::DeletedCustomer(_) => return Ok(false),
        };

        Ok(customer.address.is_some())
    }

    async fn create_customer(&self, email: &str, name: Option<&str>) -> Result<String> {
        use stripe_core::customer::CreateCustomer;

        let mut params = CreateCustomer::new().email(email);
        if let Some(n) = name {
            params = params.name(n);
        }

        let customer = params.send(&self.client).await.map_err(|e| {
            tracing::error!("Failed to create Stripe customer: {:?}", e);
            PaymentError::ProviderApi(e.to_string())
        })?;

        Ok(customer.id.to_string())
    }

    async fn create_billing_portal_session(&self, customer_id: &str, return_url: &str) -> Result<String> {
        let session = CreateBillingPortalSession::new()
            .customer(customer_id)
            .return_url(return_url)
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to create Stripe billing portal session: {:?}", e);
                PaymentError::ProviderApi(e.to_string())
            })?;

        tracing::debug!("Created billing portal session {} for customer {}", session.id, customer_id);

        Ok(session.url)
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

    fn stripe_api_error(code: Option<ApiErrorsCode>, type_: stripe::ApiErrorsType) -> stripe::StripeError {
        stripe::StripeError::Stripe(
            Box::new(stripe::ApiErrors {
                advice_code: None,
                charge: None,
                code,
                decline_code: None,
                doc_url: None,
                message: None,
                network_advice_code: None,
                network_decline_code: None,
                param: None,
                payment_intent: None,
                payment_method: None,
                payment_method_type: None,
                request_log_url: None,
                setup_intent: None,
                source: None,
                type_,
            }),
            409,
        )
    }

    fn payment_method_json(type_: &str, details: &str) -> stripe_shared::PaymentMethod {
        let json = format!(
            r#"{{"id":"pm_test","object":"payment_method","billing_details":{{}},"created":1700000000,"livemode":false,"type":"{type_}",{details}}}"#
        );
        miniserde::json::from_str(&json).expect("payment method json")
    }

    #[test]
    fn test_instrument_fingerprint_reads_card_and_sepa_and_nothing_else() {
        let card = payment_method_json(
            "card",
            r#""card":{"brand":"visa","exp_month":1,"exp_year":2030,"funding":"credit","last4":"4242","fingerprint":"fp_card"}"#,
        );
        assert_eq!(instrument_fingerprint(&card).as_deref(), Some("fp_card"));

        let sepa = payment_method_json(
            "sepa_debit",
            r#""sepa_debit":{"bank_code":"ABNA","branch_code":"","country":"NL","last4":"0000","fingerprint":"fp_iban"}"#,
        );
        assert_eq!(instrument_fingerprint(&sepa).as_deref(), Some("fp_iban"));

        let link = payment_method_json("link", r#""link":{"email":"someone@example.com"}"#);
        assert_eq!(instrument_fingerprint(&link), None);

        let bare = payment_method_json(
            "card",
            r#""card":{"brand":"visa","exp_month":1,"exp_year":2030,"funding":"credit","last4":"4242"}"#,
        );
        assert_eq!(instrument_fingerprint(&bare), None);
    }

    #[test]
    fn test_instrument_already_used_is_a_conflict_for_direct_callers() {
        let status: axum::http::StatusCode = PaymentError::InstrumentAlreadyUsed.into();
        assert_eq!(status, axum::http::StatusCode::CONFLICT);
    }

    #[test]
    fn test_setup_purpose_round_trips_and_defaults_to_verification() {
        let meta = HashMap::from([(SetupPurpose::METADATA_KEY.to_string(), "auto_topup".to_string())]);
        assert_eq!(SetupPurpose::from_metadata(Some(&meta)), SetupPurpose::AutoTopup);
        let meta = HashMap::from([(SetupPurpose::METADATA_KEY.to_string(), "verification".to_string())]);
        assert_eq!(SetupPurpose::from_metadata(Some(&meta)), SetupPurpose::Verification);
        assert_eq!(SetupPurpose::from_metadata(None), SetupPurpose::Verification);
    }

    #[test]
    fn test_idempotency_key_in_use_maps_to_already_processed() {
        let err = map_auto_topup_charge_error(stripe_api_error(
            Some(ApiErrorsCode::IdempotencyKeyInUse),
            stripe::ApiErrorsType::InvalidRequestError,
        ));
        assert!(matches!(err, PaymentError::AlreadyProcessed), "got {err:?}");
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
    fn test_card_error_still_classifies_as_a_decline() {
        let err = map_auto_topup_charge_error(stripe_api_error(
            Some(ApiErrorsCode::CardDeclined),
            stripe::ApiErrorsType::CardError,
        ));
        assert!(matches!(err, PaymentError::AutoTopupDeclined(_)), "got {err:?}");
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

    #[test]
    fn classifies_auto_topup_do_not_retry_advice_as_hard_decline() {
        assert_eq!(
            classify_card_decline(Some("do_not_try_again"), Some("insufficient_funds")),
            AutoTopupDeclineKind::Hard
        );
    }

    #[test]
    fn classifies_auto_topup_terminal_codes_as_hard_declines() {
        for decline_code in ["do_not_honor", "fraudulent", "lost_card", "pickup_card", "stolen_card"] {
            assert_eq!(
                classify_card_decline(None, Some(decline_code)),
                AutoTopupDeclineKind::Hard,
                "{decline_code} should disable auto top-up immediately"
            );
        }
    }

    #[test]
    fn classifies_auto_topup_retryable_card_errors_as_soft_declines() {
        for decline_code in [Some("insufficient_funds"), Some("processing_error"), None] {
            assert_eq!(
                classify_card_decline(None, decline_code),
                AutoTopupDeclineKind::Soft,
                "{decline_code:?} should receive one retry after 24 hours"
            );
        }
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

    #[test]
    fn test_pretax_credit_cents_excludes_tax() {
        let session: stripe_checkout::CheckoutSession = serde_json::from_str(include_str!("test_fixtures/checkout_session_with_tax.json"))
            .expect("fixture should deserialize into a Stripe CheckoutSession");

        assert_eq!(session.amount_subtotal, Some(2500));
        assert_eq!(session.amount_total, Some(3000));

        assert_eq!(pretax_credit_cents(&session), Some(2500));

        assert_eq!(
            parse_payment_session(&session).expect("fixture parses").amount,
            Decimal::new(2500, 2)
        );
    }

    #[test]
    fn test_parse_payment_session_keeps_partial_dollars() {
        let mut session: stripe_checkout::CheckoutSession =
            serde_json::from_str(include_str!("test_fixtures/checkout_session_with_tax.json"))
                .expect("fixture should deserialize into a Stripe CheckoutSession");
        session.line_items = None;
        session.amount_subtotal = Some(1050);

        let parsed = parse_payment_session(&session).expect("session parses");
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
