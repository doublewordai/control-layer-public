
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::pricing::BilledPrice;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SetAccountPriceRequest {
    pub input_per_million_tokens: String,
    pub output_per_million_tokens: String,
    #[serde(default)]
    pub cache_read_multiplier: Option<String>,
    #[serde(default)]
    pub serving_class: Option<String>,
    #[serde(default)]
    pub valid_from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub allow_zero: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct EndAccountPriceQuery {
    #[serde(default)]
    pub serving_class: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct GetAccountPriceQuery {
    #[serde(default)]
    pub serving_class: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BilledPriceResponse {
    #[schema(value_type = String)]
    pub input_price_per_token: Decimal,
    #[schema(value_type = String)]
    pub output_price_per_token: Decimal,
    #[schema(value_type = Option<String>)]
    pub cache_read_multiplier: Option<Decimal>,
}

impl From<BilledPrice> for BilledPriceResponse {
    fn from(price: BilledPrice) -> Self {
        Self {
            input_price_per_token: price.input,
            output_price_per_token: price.output,
            cache_read_multiplier: price.cache.map(|cache| cache.read),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AccountPriceResponse {
    pub account_id: Uuid,
    pub model_id: Uuid,
    pub model: String,
    pub serving_class: Option<String>,
    pub at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<BilledPriceResponse>,
    pub price: Option<BilledPriceResponse>,
    pub change_id: Option<Uuid>,
}
