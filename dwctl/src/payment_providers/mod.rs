
use async_trait::async_trait;
use axum::http::StatusCode;
use rust_decimal::Decimal;
use sqlx::PgPool;

use crate::{
    UserId,
    config::{CreditsConfig, PaymentConfig},
};

pub mod dummy;
pub mod stripe;

pub fn create_provider(config: PaymentConfig) -> Box<dyn PaymentProvider> {
    match config {
        PaymentConfig::Stripe(stripe_config) => Box::new(stripe::StripeProvider::from(stripe_config)),
        PaymentConfig::Dummy(dummy_config) => Box::new(dummy::DummyProvider::from(dummy_config)),
    }
}

pub type Result<T> = std::result::Result<T, PaymentError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoTopupDeclineKind {
    Soft,
    Hard,
}

#[derive(Debug, thiserror::Error)]
pub enum PaymentError {
    #[error("Payment provider API error: {0}")]
    ProviderApi(String),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Payment not completed yet")]
    PaymentNotCompleted,

    #[error("Invalid payment data: {0}")]
    InvalidData(String),

    #[error("Payment already processed")]
    AlreadyProcessed,

    #[error("User does not have a payment provider customer ID")]
    NoCustomerId,

    #[error("Automatic top-up payment was declined: {0:?}")]
    AutoTopupDeclined(AutoTopupDeclineKind),

    #[error("Payment session references a user unknown to this plane: {0}")]
    UnknownReference(String),

    #[error("Payment method has already verified another account")]
    InstrumentAlreadyUsed,
}

impl From<PaymentError> for StatusCode {
    fn from(err: PaymentError) -> Self {
        match err {
            PaymentError::PaymentNotCompleted => StatusCode::PAYMENT_REQUIRED,
            PaymentError::InvalidData(_) | PaymentError::NoCustomerId => StatusCode::BAD_REQUEST,
            PaymentError::AlreadyProcessed => StatusCode::OK,
            PaymentError::UnknownReference(_) => StatusCode::NOT_FOUND,
            PaymentError::AutoTopupDeclined(_) => StatusCode::PAYMENT_REQUIRED,
            PaymentError::InstrumentAlreadyUsed => StatusCode::CONFLICT,
            PaymentError::ProviderApi(_) | PaymentError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<crate::db::errors::DbError> for PaymentError {
    fn from(err: crate::db::errors::DbError) -> Self {
        match err {
            crate::db::errors::DbError::UniqueViolation { constraint, .. }
                if constraint.as_deref() == Some("credits_transactions_source_id_unique") =>
            {
                PaymentError::AlreadyProcessed
            }
            _ => {
                PaymentError::InvalidData(format!("Database error: {}", err))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AutoTopupSetupResult {
    pub customer_id: Option<String>,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PaymentSession {
    pub creditee_id: UserId,
    pub amount: Decimal,
    pub is_paid: bool,
    pub creditor_id: UserId,
    pub payment_provider_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookEvent {
    pub event_type: String,
    pub session_id: Option<String>,
}

pub struct CheckoutPayer {
    pub id: UserId,
    pub email: String,
    pub payment_provider_id: Option<String>,
}

#[async_trait]
pub trait PaymentProvider: Send + Sync {
    async fn create_checkout_session(
        &self,
        payer: &CheckoutPayer,
        creditee_id: Option<&str>,
        cancel_url: &str,
        success_url: &str,
    ) -> Result<String>;

    async fn get_payment_session(&self, session_id: &str) -> Result<PaymentSession>;

    async fn create_setup_checkout_session(&self, payer: &CheckoutPayer, cancel_url: &str, success_url: &str) -> Result<String>;

    async fn process_payment_session(&self, db_pool: &PgPool, session_id: &str, credits_config: &CreditsConfig) -> Result<()>;

    async fn validate_webhook(&self, headers: &axum::http::HeaderMap, body: &str) -> Result<Option<WebhookEvent>>;

    async fn process_webhook_event(&self, db_pool: &PgPool, event: &WebhookEvent, credits_config: &CreditsConfig) -> Result<()>;

    async fn create_billing_portal_session(&self, customer_id: &str, return_url: &str) -> Result<String>;

    async fn create_auto_topup_checkout_session(&self, payer: &CheckoutPayer, cancel_url: &str, success_url: &str) -> Result<String>;

    async fn process_auto_topup_session(&self, db_pool: &PgPool, session_id: &str) -> Result<AutoTopupSetupResult>;

    async fn accrue_invoice_item(&self, amount_cents: i64, customer_id: &str, description: &str, idempotency_key: &str) -> Result<String>;

    async fn charge_auto_topup(
        &self,
        amount_cents: i64,
        customer_id: &str,
        payment_method_id: &str,
        idempotency_key: &str,
    ) -> Result<String>;

    async fn get_default_payment_method(&self, customer_id: &str) -> Result<Option<String>>;

    async fn customer_has_address(&self, customer_id: &str) -> Result<bool>;

    async fn create_customer(&self, email: &str, name: Option<&str>) -> Result<String>;
}
