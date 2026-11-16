
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::handlers::ActiveTariff;

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct CachePricingUpdateRequest {
    #[schema(value_type = Option<String>)]
    pub write_multiplier_5m: Option<Decimal>,
    #[schema(value_type = Option<String>)]
    pub write_multiplier_1h: Option<Decimal>,
    #[schema(value_type = Option<String>)]
    pub write_multiplier_24h: Option<Decimal>,
    #[schema(value_type = Option<String>)]
    pub read_multiplier: Option<Decimal>,
    pub min_prefix_tokens: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CachePricingResponse {
    pub enabled: bool,
    #[schema(value_type = Option<String>)]
    pub write_multiplier_5m: Option<Decimal>,
    #[schema(value_type = Option<String>)]
    pub write_multiplier_1h: Option<Decimal>,
    #[schema(value_type = Option<String>)]
    pub write_multiplier_24h: Option<Decimal>,
    #[schema(value_type = Option<String>)]
    pub read_multiplier: Option<Decimal>,
    pub min_prefix_tokens: Option<i32>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
}

impl CachePricingResponse {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            write_multiplier_5m: None,
            write_multiplier_1h: None,
            write_multiplier_24h: None,
            read_multiplier: None,
            min_prefix_tokens: None,
            valid_from: None,
            valid_until: None,
        }
    }
}

impl From<ActiveTariff> for CachePricingResponse {
    fn from(t: ActiveTariff) -> Self {
        Self {
            enabled: true,
            write_multiplier_5m: Some(t.write_multiplier_5m),
            write_multiplier_1h: Some(t.write_multiplier_1h),
            write_multiplier_24h: Some(t.write_multiplier_24h),
            read_multiplier: Some(t.read_multiplier),
            min_prefix_tokens: Some(t.min_prefix_tokens),
            valid_from: Some(t.valid_from),
            valid_until: t.valid_until,
        }
    }
}
