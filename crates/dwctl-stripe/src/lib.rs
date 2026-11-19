
use std::collections::HashMap;

use async_trait::async_trait;
use stripe::{ApiErrorsCode, ApiErrorsType, Client, ClientBuilder, IdempotencyKey, RequestStrategy, StripeError, StripeRequest};
use stripe_billing::{billing_portal_session::CreateBillingPortalSession, invoice_item::CreateInvoiceItem};
use stripe_checkout::{
    CheckoutSessionId, CheckoutSessionMode, CheckoutSessionPaymentStatus, CheckoutSessionStatus, CheckoutSessionUiMode,
    checkout_session::{
        CreateCheckoutSession, CreateCheckoutSessionAutomaticTax, CreateCheckoutSessionConsentCollection,
        CreateCheckoutSessionConsentCollectionPaymentMethodReuseAgreement,
        CreateCheckoutSessionConsentCollectionPaymentMethodReuseAgreementPosition, CreateCheckoutSessionConsentCollectionTermsOfService,
        CreateCheckoutSessionCustomText, CreateCheckoutSessionCustomerCreation, CreateCheckoutSessionCustomerUpdate,
        CreateCheckoutSessionCustomerUpdateAddress, CreateCheckoutSessionCustomerUpdateName, CreateCheckoutSessionInvoiceCreation,
        CreateCheckoutSessionLineItems, CreateCheckoutSessionNameCollection, CreateCheckoutSessionNameCollectionBusiness,
        CreateCheckoutSessionPaymentMethodTypes, CreateCheckoutSessionSavedPaymentMethodOptions,
        CreateCheckoutSessionSavedPaymentMethodOptionsPaymentMethodRemove, CreateCheckoutSessionSavedPaymentMethodOptionsPaymentMethodSave,
        CreateCheckoutSessionSetupIntentData, CreateCheckoutSessionTaxIdCollection, CustomTextPositionParam, RetrieveCheckoutSession,
    },
};
use stripe_core::{
    PaymentIntent,
    customer::{
        CreateCustomer, ListPaymentMethodsCustomer, RetrieveCustomer, RetrieveCustomerReturned, UpdateCustomer,
        UpdateCustomerInvoiceSettings,
    },
    payment_intent::{
        AsyncWorkflowsInputsParam, AsyncWorkflowsInputsTaxParam, AsyncWorkflowsParam, CreatePaymentIntent, CreatePaymentIntentOffSession,
    },
};
use stripe_misc::tax_calculation::{CreateTaxCalculation, CreateTaxCalculationLineItems};
use stripe_shared::PaymentMethod;
use stripe_types::{Currency, Expandable};
use stripe_webhook::{EventObject, Webhook};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclineKind {
    Soft,
    Hard,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Stripe API error: {0}")]
    Api(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Request already in progress")]
    AlreadyProcessed,

    #[error("Card declined: {0:?}")]
    Declined(DeclineKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetupPurpose {
    Verification,
    AutoTopup,
}

impl SetupPurpose {
    const METADATA_KEY: &'static str = "purpose";

    pub fn as_str(self) -> &'static str {
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

    fn copy(self) -> SetupSessionCopy {
        match self {
            SetupPurpose::Verification => SetupSessionCopy {
                submit_label: "Verify payment method",
                setup_intent_description: "Payment method verification",
                payment_method_types: vec![CreateCheckoutSessionPaymentMethodTypes::Card],
            },
            SetupPurpose::AutoTopup => SetupSessionCopy {
                submit_label: "Set up auto top-up",
                setup_intent_description: "Auto top-up setup",
                payment_method_types: vec![
                    CreateCheckoutSessionPaymentMethodTypes::Card,
                    CreateCheckoutSessionPaymentMethodTypes::Link,
                    CreateCheckoutSessionPaymentMethodTypes::SepaDebit,
                ],
            },
        }
    }
}

struct SetupSessionCopy {
    submit_label: &'static str,
    setup_intent_description: &'static str,
    payment_method_types: Vec<CreateCheckoutSessionPaymentMethodTypes>,
}

#[derive(Debug, Clone, Copy)]
pub struct Payer<'a> {
    pub reference_id: &'a str,
    pub email: &'a str,
    pub customer_id: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub struct PaymentCheckout<'a> {
    pub payer: Payer<'a>,
    pub creditee_id: Option<&'a str>,
    pub cancel_url: &'a str,
    pub success_url: &'a str,
    pub price_id: &'a str,
    pub invoice_creation: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SetupCheckout<'a> {
    pub payer: Payer<'a>,
    pub cancel_url: &'a str,
    pub success_url: &'a str,
    pub purpose: SetupPurpose,
    pub terms_of_service_text: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckoutMode {
    Payment,
    Setup,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutSession {
    pub mode: CheckoutMode,
    pub complete: bool,
    pub paid: bool,
    pub client_reference_id: Option<String>,
    pub customer_id: Option<String>,
    pub creditee_id: Option<String>,
    pub pretax_cents: Option<i64>,
    pub setup_intent: Option<SetupIntent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetupIntent {
    pub status: String,
    pub payment_method_id: Option<String>,
    pub purpose: SetupPurpose,
    pub fingerprint: Option<String>,
    pub cvc_check_failed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookEvent {
    pub event_type: String,
    pub session_id: Option<String>,
}

fn classify_card_decline(advice_code: Option<&str>, decline_code: Option<&str>) -> DeclineKind {
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
        DeclineKind::Hard
    } else {
        DeclineKind::Soft
    }
}

fn map_auto_topup_charge_error(error: StripeError) -> Error {
    match error {
        StripeError::Stripe(api_error, _) if api_error.code == Some(ApiErrorsCode::IdempotencyKeyInUse) => Error::AlreadyProcessed,
        StripeError::Stripe(api_error, _) if matches!(api_error.type_, ApiErrorsType::CardError) => Error::Declined(classify_card_decline(
            api_error.advice_code.as_deref(),
            api_error.decline_code.as_deref(),
        )),
        StripeError::Stripe(api_error, status) => {
            tracing::error!(
                status,
                error_type = %api_error.type_,
                "Stripe rejected the auto top-up payment request"
            );
            Error::Api(format!("Stripe {} error (HTTP {status})", api_error.type_))
        }
        other => {
            tracing::error!(error = %other, "Failed to create auto top-up payment intent");
            Error::Api(other.to_string())
        }
    }
}

fn instrument_fingerprint(pm: &PaymentMethod) -> Option<String> {
    pm.card
        .as_ref()
        .and_then(|card| card.fingerprint.clone())
        .or_else(|| pm.sepa_debit.as_ref().and_then(|sepa| sepa.fingerprint.clone()))
}

fn cvc_check_failed(pm: &PaymentMethod) -> bool {
    pm.card
        .as_ref()
        .and_then(|card| card.checks.as_ref())
        .and_then(|checks| checks.cvc_check.as_deref())
        == Some("fail")
}

fn pretax_credit_cents(session: &stripe_checkout::CheckoutSession) -> Option<i64> {
    session
        .line_items
        .as_ref()
        .and_then(|items| items.data.first().map(|item| item.amount_subtotal))
        .or(session.amount_subtotal)
}

fn summarize_session(session: &stripe_checkout::CheckoutSession) -> CheckoutSession {
    let setup_intent = match &session.setup_intent {
        Some(Expandable::Object(setup_intent)) => Some(SetupIntent {
            status: setup_intent.status.as_str().to_string(),
            payment_method_id: setup_intent.payment_method.as_ref().map(|pm| pm.id().to_string()),
            purpose: SetupPurpose::from_metadata(setup_intent.metadata.as_ref()),
            fingerprint: setup_intent
                .payment_method
                .as_ref()
                .and_then(|pm| pm.as_object())
                .and_then(instrument_fingerprint),
            cvc_check_failed: setup_intent
                .payment_method
                .as_ref()
                .and_then(|pm| pm.as_object())
                .is_some_and(cvc_check_failed),
        }),
        _ => None,
    };

    CheckoutSession {
        mode: match session.mode {
            CheckoutSessionMode::Payment => CheckoutMode::Payment,
            CheckoutSessionMode::Setup => CheckoutMode::Setup,
            _ => CheckoutMode::Other,
        },
        complete: session.status == Some(CheckoutSessionStatus::Complete),
        paid: session.payment_status == CheckoutSessionPaymentStatus::Paid,
        client_reference_id: session.client_reference_id.clone(),
        customer_id: session.customer.as_ref().map(|customer| customer.id().to_string()),
        creditee_id: session.metadata.as_ref().and_then(|m| m.get("creditee_id")).cloned(),
        pretax_cents: pretax_credit_cents(session),
        setup_intent,
    }
}

fn parse_session_id(session_id: &str) -> Result<CheckoutSessionId> {
    session_id
        .parse()
        .map_err(|_| Error::InvalidData("Invalid Stripe session ID".to_string()))
}

pub fn verify_webhook(body: &str, signature: &str, webhook_secret: &str) -> Result<WebhookEvent> {
    let event = Webhook::construct_event(body, signature, webhook_secret).map_err(|e| {
        tracing::error!("Failed to construct webhook event: {:?}", e);
        Error::InvalidData(format!("Webhook validation failed: {}", e))
    })?;

    tracing::trace!("Validated Stripe webhook event: {:?}", event.type_);

    let session_id = match &event.data.object {
        EventObject::CheckoutSessionCompleted(session) | EventObject::CheckoutSessionAsyncPaymentSucceeded(session) => {
            Some(session.id.to_string())
        }
        _ => None,
    };

    Ok(WebhookEvent {
        event_type: event.type_.to_string(),
        session_id,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedCheckout {
    pub id: String,
    pub url: String,
}

#[async_trait]
pub trait StripeApi: Send + Sync {
    async fn create_payment_checkout_session(&self, checkout: PaymentCheckout<'_>) -> Result<CreatedCheckout>;

    async fn create_setup_checkout_session(&self, checkout: SetupCheckout<'_>) -> Result<CreatedCheckout>;

    async fn get_payment_session(&self, session_id: &str) -> Result<CheckoutSession>;

    async fn get_setup_session(&self, session_id: &str) -> Result<CheckoutSession>;

    async fn get_session_for_processing(&self, session_id: &str) -> Result<CheckoutSession>;

    async fn set_default_payment_method(&self, customer_id: &str, payment_method_id: &str);

    async fn charge_auto_topup(
        &self,
        amount_cents: i64,
        customer_id: &str,
        payment_method_id: &str,
        idempotency_key: &str,
        tax_code: Option<&str>,
    ) -> Result<String>;

    async fn accrue_invoice_item(&self, amount_cents: i64, customer_id: &str, description: &str, idempotency_key: &str) -> Result<String>;

    async fn get_default_payment_method(&self, customer_id: &str) -> Result<Option<String>>;

    async fn customer_has_address(&self, customer_id: &str) -> Result<bool>;

    async fn create_customer(&self, email: &str, name: Option<&str>) -> Result<String>;

    async fn create_billing_portal_session(&self, customer_id: &str, return_url: &str) -> Result<String>;
}

pub struct StripeClient {
    client: Client,
}

impl StripeClient {
    pub fn new(api_key: &str, api_base: Option<String>) -> Result<Self> {
        let mut builder = ClientBuilder::new(api_key);
        if let Some(api_base) = api_base {
            builder = builder.url(api_base);
        }
        let client = builder.build().map_err(|e| Error::Api(e.to_string()))?;
        Ok(Self { client })
    }

    async fn charge_auto_topup_internal(
        &self,
        amount_cents: i64,
        customer_id: &str,
        payment_method_id: &str,
        idempotency_key: &str,
        tax_code: Option<&str>,
    ) -> Result<PaymentIntent> {
        let mut line_item = CreateTaxCalculationLineItems::new(amount_cents);
        line_item.reference = Some("auto_topup".to_string());
        line_item.tax_code = tax_code.map(str::to_string);

        let tax_idem_key = IdempotencyKey::new(format!("{}_tax", idempotency_key))
            .map_err(|e| Error::InvalidData(format!("Invalid tax idempotency key: {e}")))?;

        let tax_calc = CreateTaxCalculation::new(Currency::USD, vec![line_item])
            .customer(customer_id)
            .customize()
            .request_strategy(RequestStrategy::Idempotent(tax_idem_key))
            .send(&self.client)
            .await
            .map_err(map_auto_topup_charge_error)?;

        let tax_calc_id = tax_calc.id.ok_or_else(|| Error::Api("Tax calculation missing ID".to_string()))?;

        let idem_key = IdempotencyKey::new(idempotency_key).map_err(|e| Error::InvalidData(format!("Invalid idempotency key: {e}")))?;

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
}

const CHECKOUT_LIFETIME: std::time::Duration = std::time::Duration::from_secs(31 * 60);

fn checkout_expiry() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from((now + CHECKOUT_LIFETIME).as_secs()).unwrap_or(i64::MAX)
}

#[async_trait]
impl StripeApi for StripeClient {
    async fn create_payment_checkout_session(&self, checkout: PaymentCheckout<'_>) -> Result<CreatedCheckout> {
        let payer = checkout.payer;
        let mut checkout_params = CreateCheckoutSession::new()
            .cancel_url(checkout.cancel_url)
            .success_url(checkout.success_url)
            .client_reference_id(payer.reference_id)
            .currency(Currency::USD)
            .line_items(vec![CreateCheckoutSessionLineItems {
                price: Some(checkout.price_id.to_string()),
                quantity: Some(1),
                ..Default::default()
            }])
            .automatic_tax(CreateCheckoutSessionAutomaticTax::new(true))
            .mode(CheckoutSessionMode::Payment)
            .ui_mode(CheckoutSessionUiMode::HostedPage)
            .expires_at(checkout_expiry())
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

        if let Some(user_receiving_credits) = checkout.creditee_id {
            let mut metadata = HashMap::new();
            metadata.insert("creditee_id".to_string(), user_receiving_credits.to_string());
            checkout_params = checkout_params.metadata(metadata);
        }

        if checkout.invoice_creation {
            checkout_params = checkout_params.invoice_creation(CreateCheckoutSessionInvoiceCreation::new(true));
        }

        if let Some(existing_id) = payer.customer_id {
            tracing::debug!("Using existing Stripe customer ID {} for payer {}", existing_id, payer.reference_id);
            checkout_params = checkout_params
                .customer(existing_id)
                .customer_update(CreateCheckoutSessionCustomerUpdate {
                    address: Some(CreateCheckoutSessionCustomerUpdateAddress::Auto),
                    name: Some(CreateCheckoutSessionCustomerUpdateName::Auto),
                    shipping: None,
                })
        } else {
            tracing::debug!("No customer ID found for payer {}, Stripe will create one", payer.reference_id);
            checkout_params = checkout_params
                .customer_email(payer.email)
                .customer_creation(CreateCheckoutSessionCustomerCreation::Always);
        }

        let checkout_session = checkout_params.send(&self.client).await.map_err(|e| {
            tracing::error!("Failed to create Stripe checkout session: {:?}", e);
            Error::Api(e.to_string())
        })?;

        tracing::debug!(
            "Created checkout session {} for creditee {} (payer: {})",
            checkout_session.id,
            checkout.creditee_id.unwrap_or(payer.reference_id),
            payer.reference_id
        );

        let url = checkout_session.url.ok_or_else(|| {
            tracing::error!("Checkout session missing URL");
            Error::Api("Checkout session missing URL".to_string())
        })?;
        Ok(CreatedCheckout {
            id: checkout_session.id.to_string(),
            url,
        })
    }

    async fn create_setup_checkout_session(&self, checkout: SetupCheckout<'_>) -> Result<CreatedCheckout> {
        let payer = checkout.payer;
        let copy = checkout.purpose.copy();
        let mut checkout_params = CreateCheckoutSession::new()
            .cancel_url(checkout.cancel_url)
            .success_url(checkout.success_url)
            .client_reference_id(payer.reference_id)
            .mode(CheckoutSessionMode::Setup)
            .ui_mode(CheckoutSessionUiMode::HostedPage)
            .expires_at(checkout_expiry())
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
                terms_of_service_acceptance: checkout.terms_of_service_text.map(CustomTextPositionParam::new),
                submit: Some(CustomTextPositionParam::new(copy.submit_label)),
                after_submit: None,
                shipping_address: None,
            })
            .payment_method_types(copy.payment_method_types)
            .setup_intent_data(CreateCheckoutSessionSetupIntentData {
                description: Some(copy.setup_intent_description.to_string()),
                metadata: Some(HashMap::from([(
                    SetupPurpose::METADATA_KEY.to_string(),
                    checkout.purpose.as_str().to_string(),
                )])),
                on_behalf_of: None,
            });

        if let Some(existing_id) = payer.customer_id {
            tracing::debug!("Using existing Stripe customer ID {} for payer {}", existing_id, payer.reference_id);
            checkout_params = checkout_params
                .customer(existing_id)
                .customer_update(CreateCheckoutSessionCustomerUpdate {
                    address: Some(CreateCheckoutSessionCustomerUpdateAddress::Auto),
                    name: Some(CreateCheckoutSessionCustomerUpdateName::Auto),
                    shipping: None,
                })
        } else {
            tracing::debug!("No customer ID found for payer {}, Stripe will create one", payer.reference_id);
            checkout_params = checkout_params
                .customer_email(payer.email)
                .customer_creation(CreateCheckoutSessionCustomerCreation::Always);
        }

        let checkout_session = checkout_params.send(&self.client).await.map_err(|e| {
            tracing::error!(
                purpose = copy.setup_intent_description,
                "Failed to create Stripe setup checkout session: {:?}",
                e
            );
            Error::Api(e.to_string())
        })?;

        tracing::debug!(
            purpose = copy.setup_intent_description,
            "Created setup checkout session {} for payer {}",
            checkout_session.id,
            payer.reference_id
        );

        let url = checkout_session.url.ok_or_else(|| {
            tracing::error!("Setup checkout session missing URL");
            Error::Api("Checkout session missing URL".to_string())
        })?;
        Ok(CreatedCheckout {
            id: checkout_session.id.to_string(),
            url,
        })
    }

    async fn get_payment_session(&self, session_id: &str) -> Result<CheckoutSession> {
        let checkout_session = RetrieveCheckoutSession::new(parse_session_id(session_id)?)
            .expand(vec!["line_items".to_string()])
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to retrieve Stripe checkout session: {:?}", e);
                Error::Api(e.to_string())
            })?;

        Ok(summarize_session(&checkout_session))
    }

    async fn get_setup_session(&self, session_id: &str) -> Result<CheckoutSession> {
        let checkout_session = RetrieveCheckoutSession::new(parse_session_id(session_id)?)
            .expand(vec!["setup_intent".to_string(), "setup_intent.payment_method".to_string()])
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to retrieve Stripe setup session: {:?}", e);
                Error::Api(e.to_string())
            })?;

        Ok(summarize_session(&checkout_session))
    }

    async fn get_session_for_processing(&self, session_id: &str) -> Result<CheckoutSession> {
        let checkout_session = RetrieveCheckoutSession::new(parse_session_id(session_id)?)
            .expand(vec![
                "line_items".to_string(),
                "setup_intent".to_string(),
                "setup_intent.payment_method".to_string(),
            ])
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to retrieve Stripe checkout session: {:?}", e);
                Error::Api(e.to_string())
            })?;

        Ok(summarize_session(&checkout_session))
    }

    async fn set_default_payment_method(&self, customer_id: &str, payment_method_id: &str) {
        let mut invoice_settings = UpdateCustomerInvoiceSettings::new();
        invoice_settings.default_payment_method = Some(payment_method_id.to_string());

        if let Err(e) = UpdateCustomer::new(customer_id)
            .invoice_settings(invoice_settings)
            .send(&self.client)
            .await
        {
            tracing::warn!(
                "Failed to set default payment method {} on customer {}: {:?}",
                payment_method_id,
                customer_id,
                e
            );
        }
    }

    async fn charge_auto_topup(
        &self,
        amount_cents: i64,
        customer_id: &str,
        payment_method_id: &str,
        idempotency_key: &str,
        tax_code: Option<&str>,
    ) -> Result<String> {
        let pi = self
            .charge_auto_topup_internal(amount_cents, customer_id, payment_method_id, idempotency_key, tax_code)
            .await?;
        Ok(pi.id.to_string())
    }

    async fn accrue_invoice_item(&self, amount_cents: i64, customer_id: &str, description: &str, idempotency_key: &str) -> Result<String> {
        let idem = IdempotencyKey::new(idempotency_key).map_err(|e| Error::InvalidData(format!("Invalid idempotency key: {e}")))?;

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
                Error::Api(e.to_string())
            })?;

        tracing::info!(
            invoice_item_id = %item.id,
            customer_id,
            amount_cents,
            "Accrued charge onto the customer's next invoice"
        );

        Ok(item.id.to_string())
    }

    async fn get_default_payment_method(&self, customer_id: &str) -> Result<Option<String>> {
        let result = RetrieveCustomer::new(customer_id.to_string())
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to retrieve Stripe customer: {:?}", e);
                Error::Api(e.to_string())
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
            .map(|expandable: Expandable<_>| expandable.id().to_string());

        if pm.is_some() {
            return Ok(pm);
        }

        let methods = ListPaymentMethodsCustomer::new(customer_id.to_string())
            .limit(1)
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to list payment methods for customer {}: {:?}", customer_id, e);
                Error::Api(e.to_string())
            })?;

        Ok(methods.data.first().map(|pm| pm.id.to_string()))
    }

    async fn customer_has_address(&self, customer_id: &str) -> Result<bool> {
        let result = RetrieveCustomer::new(customer_id.to_string())
            .send(&self.client)
            .await
            .map_err(|e| {
                tracing::error!("Failed to retrieve Stripe customer for address check: {:?}", e);
                Error::Api(e.to_string())
            })?;

        let customer = match result {
            RetrieveCustomerReturned::Customer(c) => c,
            RetrieveCustomerReturned::DeletedCustomer(_) => return Ok(false),
        };

        Ok(customer.address.is_some())
    }

    async fn create_customer(&self, email: &str, name: Option<&str>) -> Result<String> {
        let mut params = CreateCustomer::new().email(email);
        if let Some(n) = name {
            params = params.name(n);
        }

        let customer = params.send(&self.client).await.map_err(|e| {
            tracing::error!("Failed to create Stripe customer: {:?}", e);
            Error::Api(e.to_string())
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
                Error::Api(e.to_string())
            })?;

        tracing::debug!("Created billing portal session {} for customer {}", session.id, customer_id);

        Ok(session.url)
    }
}

#[cfg(test)]
mod tests {
    use stripe::ApiErrors;

    use super::*;

    fn stripe_api_error(code: Option<ApiErrorsCode>, type_: ApiErrorsType) -> StripeError {
        StripeError::Stripe(
            Box::new(ApiErrors {
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

    fn payment_method_json(type_: &str, details: &str) -> PaymentMethod {
        let json = format!(
            r#"{{"id":"pm_test","object":"payment_method","billing_details":{{}},"created":1700000000,"livemode":false,"type":"{type_}",{details}}}"#
        );
        miniserde::json::from_str(&json).expect("payment method json")
    }

    fn session_with_tax() -> stripe_checkout::CheckoutSession {
        serde_json::from_str(include_str!("../test_fixtures/checkout_session_with_tax.json"))
            .expect("fixture should deserialize into a Stripe CheckoutSession")
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
    fn test_cvc_check_failed_only_on_an_explicit_fail() {
        let card_with = |checks: &str| {
            payment_method_json(
                "card",
                &format!(
                    r#""card":{{"brand":"visa","exp_month":1,"exp_year":2030,"funding":"credit","last4":"4242","fingerprint":"fp_card"{checks}}}"#
                ),
            )
        };
        assert!(cvc_check_failed(&card_with(r#","checks":{"cvc_check":"fail"}"#)));
        assert!(!cvc_check_failed(&card_with(r#","checks":{"cvc_check":"pass"}"#)));
        assert!(!cvc_check_failed(&card_with(r#","checks":{"cvc_check":"unavailable"}"#)));
        assert!(!cvc_check_failed(&card_with(r#","checks":{"cvc_check":null}"#)));
        assert!(!cvc_check_failed(&card_with("")));

        let link = payment_method_json("link", r#""link":{"email":"someone@example.com"}"#);
        assert!(!cvc_check_failed(&link));
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
            ApiErrorsType::InvalidRequestError,
        ));
        assert!(matches!(err, Error::AlreadyProcessed), "got {err:?}");
    }

    #[test]
    fn test_card_error_still_classifies_as_a_decline() {
        let err = map_auto_topup_charge_error(stripe_api_error(Some(ApiErrorsCode::CardDeclined), ApiErrorsType::CardError));
        assert!(matches!(err, Error::Declined(_)), "got {err:?}");
    }

    #[test]
    fn classifies_auto_topup_do_not_retry_advice_as_hard_decline() {
        assert_eq!(
            classify_card_decline(Some("do_not_try_again"), Some("insufficient_funds")),
            DeclineKind::Hard
        );
    }

    #[test]
    fn classifies_auto_topup_terminal_codes_as_hard_declines() {
        for decline_code in ["do_not_honor", "fraudulent", "lost_card", "pickup_card", "stolen_card"] {
            assert_eq!(
                classify_card_decline(None, Some(decline_code)),
                DeclineKind::Hard,
                "{decline_code} should disable auto top-up immediately"
            );
        }
    }

    #[test]
    fn classifies_auto_topup_retryable_card_errors_as_soft_declines() {
        for decline_code in [Some("insufficient_funds"), Some("processing_error"), None] {
            assert_eq!(
                classify_card_decline(None, decline_code),
                DeclineKind::Soft,
                "{decline_code:?} should receive one retry after 24 hours"
            );
        }
    }

    #[test]
    fn test_pretax_credit_cents_excludes_tax() {
        let session = session_with_tax();

        assert_eq!(session.amount_subtotal, Some(2500));
        assert_eq!(session.amount_total, Some(3000));

        assert_eq!(pretax_credit_cents(&session), Some(2500));

        assert_eq!(summarize_session(&session).pretax_cents, Some(2500));
    }

    #[test]
    fn test_pretax_credit_cents_falls_back_to_the_session_subtotal() {
        let mut session = session_with_tax();
        session.line_items = None;
        session.amount_subtotal = Some(1050);

        assert_eq!(summarize_session(&session).pretax_cents, Some(1050));
    }
}
