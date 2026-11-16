
use super::pagination::Pagination;
use crate::{
    db::models::credits::{CreditTransactionDBResponse, CreditTransactionType},
    types::UserId,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransactionType {
    AdminGrant,
    AdminRemoval,
}

impl From<&TransactionType> for CreditTransactionType {
    fn from(tx_type: &TransactionType) -> Self {
        match tx_type {
            TransactionType::AdminGrant => CreditTransactionType::AdminGrant,
            TransactionType::AdminRemoval => CreditTransactionType::AdminRemoval,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreditTransactionCreate {
    #[schema(value_type = String, format = "uuid")]
    pub user_id: UserId,
    pub transaction_type: TransactionType,
    #[schema(value_type = String)]
    pub amount: Decimal,
    pub source_id: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreditTransactionResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: Uuid,
    #[schema(value_type = String, format = "uuid")]
    pub user_id: UserId,
    pub transaction_type: CreditTransactionType,
    #[schema(value_type = Option<String>, format = "uuid")]
    pub batch_id: Option<Uuid>,
    #[schema(value_type = String)]
    pub amount: Decimal,
    pub source_id: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_request_count: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransactionListResponse {
    pub data: Vec<CreditTransactionResponse>,
    pub has_more: bool,
    #[schema(value_type = String)]
    pub page_start_balance: Decimal,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ListTransactionsQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[param(value_type = Option<String>, format = "uuid")]
    pub user_id: Option<UserId>,

    pub all: Option<bool>,

    pub group_batches: Option<bool>,

    pub search: Option<String>,

    pub transaction_types: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[param(value_type = Option<String>, format = "date-time")]
    pub start_date: Option<DateTime<Utc>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[param(value_type = Option<String>, format = "date-time")]
    pub end_date: Option<DateTime<Utc>>,

    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,
}

#[derive(Debug, Default, Clone)]
pub struct TransactionFilters {
    pub search: Option<String>,
    pub transaction_types: Option<Vec<CreditTransactionType>>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
}

impl ListTransactionsQuery {
    pub fn to_filters(&self) -> TransactionFilters {
        let transaction_types = self.transaction_types.as_ref().map(|types_str| {
            types_str
                .split(',')
                .filter_map(|t| match t.trim() {
                    "admin_grant" => Some(CreditTransactionType::AdminGrant),
                    "admin_removal" => Some(CreditTransactionType::AdminRemoval),
                    "usage" => Some(CreditTransactionType::Usage),
                    "purchase" => Some(CreditTransactionType::Purchase),
                    _ => None,
                })
                .collect()
        });

        TransactionFilters {
            search: self.search.clone(),
            transaction_types,
            start_date: self.start_date,
            end_date: self.end_date,
        }
    }
}

impl CreditTransactionResponse {
    pub fn from_db_with_batch_id(db: CreditTransactionDBResponse, batch_id: Option<Uuid>) -> Self {
        Self {
            id: db.id,
            user_id: db.user_id,
            transaction_type: db.transaction_type,
            batch_id,
            amount: db.amount,
            source_id: db.source_id,
            description: db.description,
            created_at: db.created_at,
            service_tier: db.service_tier,
            batch_request_count: None,
        }
    }

    pub fn from_db_with_metadata(
        db: CreditTransactionDBResponse,
        batch_id: Option<Uuid>,
        service_tier: Option<String>,
        batch_count: i32,
    ) -> Self {
        let batch_request_count = if batch_count > 1 { Some(batch_count) } else { None };

        Self {
            id: db.id,
            user_id: db.user_id,
            transaction_type: db.transaction_type,
            batch_id,
            amount: db.amount,
            source_id: db.source_id,
            description: db.description,
            created_at: db.created_at,
            service_tier,
            batch_request_count,
        }
    }
}

impl From<CreditTransactionDBResponse> for CreditTransactionResponse {
    fn from(db: CreditTransactionDBResponse) -> Self {
        Self::from_db_with_batch_id(db, None)
    }
}
