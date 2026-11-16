
use crate::{
    api::models::transactions::TransactionFilters,
    db::{
        errors::Result,
        models::credits::{CreditTransactionCreateDBRequest, CreditTransactionDBResponse, CreditTransactionType},
    },
    types::{UserId, abbrev_uuid},
};
use chrono::{DateTime, Utc};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgConnection};
use std::collections::{HashMap, HashSet};
use tracing::{instrument, trace};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CreditTransaction {
    pub id: Uuid,
    pub user_id: UserId,
    #[sqlx(rename = "transaction_type")]
    pub transaction_type: CreditTransactionType,
    pub amount: Decimal,
    pub description: Option<String>,
    pub source_id: String,
    pub created_at: DateTime<Utc>,
    pub seq: i64,
    pub api_key_id: Option<Uuid>,
}

impl From<CreditTransaction> for CreditTransactionDBResponse {
    fn from(tx: CreditTransaction) -> Self {
        Self {
            id: tx.id,
            user_id: tx.user_id,
            transaction_type: tx.transaction_type,
            amount: tx.amount,
            description: tx.description,
            source_id: tx.source_id,
            created_at: tx.created_at,
            api_key_id: tx.api_key_id,
            service_tier: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BalanceCheckpoint {
    pub user_id: UserId,
    pub checkpoint_seq: i64,
    pub balance: Decimal,
}

#[derive(Debug)]
pub struct AggregatedBatches {
    pub batched_transactions: Vec<(CreditTransactionDBResponse, Uuid)>,
    pub batched_source_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TransactionWithCategory {
    pub transaction: CreditTransactionDBResponse,
    pub batch_id: Option<Uuid>,
    pub service_tier: Option<String>,
    pub batch_count: i32,
}

fn transaction_type_to_string(t: &CreditTransactionType) -> String {
    match t {
        CreditTransactionType::Purchase => "purchase".to_string(),
        CreditTransactionType::AdminGrant => "admin_grant".to_string(),
        CreditTransactionType::AdminRemoval => "admin_removal".to_string(),
        CreditTransactionType::Usage => "usage".to_string(),
    }
}

pub struct Credits<'c> {
    db: &'c mut PgConnection,
}

impl<'c> Credits<'c> {
    pub fn new(db: &'c mut PgConnection) -> Self {
        Self { db }
    }

    #[instrument(level = "debug", skip(self, request), fields(user_id = %abbrev_uuid(&request.user_id), transaction_type = ?request.transaction_type, amount = %request.amount), err)]
    pub async fn create_transaction(&mut self, request: &CreditTransactionCreateDBRequest) -> Result<CreditTransactionDBResponse> {
        let signed_amount = match request.transaction_type {
            CreditTransactionType::AdminGrant | CreditTransactionType::Purchase => request.amount,
            CreditTransactionType::Usage | CreditTransactionType::AdminRemoval => -request.amount,
        };

        let row = sqlx::query!(
            r#"
            WITH inserted AS (
                INSERT INTO credits_transactions (user_id, transaction_type, amount, source_id, description, fusillade_batch_id, api_key_id, is_aggregated)
                VALUES ($1, $2, $3, $4, $5, $6::uuid, $7, $6::uuid IS NOT NULL)
                RETURNING id, user_id, transaction_type, amount, source_id, description, created_at, seq, api_key_id
            ),
            bumped AS (
                INSERT INTO user_balance_checkpoints (user_id, checkpoint_seq, balance)
                SELECT user_id, seq, $8 FROM inserted
                ON CONFLICT (user_id) DO UPDATE SET
                    balance = user_balance_checkpoints.balance + EXCLUDED.balance,
                    checkpoint_seq = GREATEST(user_balance_checkpoints.checkpoint_seq, EXCLUDED.checkpoint_seq),
                    updated_at = NOW()
                RETURNING balance
            ),
            aggregated AS (
                INSERT INTO batch_aggregates (fusillade_batch_id, user_id, total_amount, transaction_count, max_seq, created_at, updated_at)
                SELECT $6::uuid, user_id, amount, 1, seq, created_at, NOW()
                FROM inserted
                WHERE $6::uuid IS NOT NULL
                ON CONFLICT (fusillade_batch_id) DO UPDATE SET
                    total_amount = batch_aggregates.total_amount + EXCLUDED.total_amount,
                    transaction_count = batch_aggregates.transaction_count + EXCLUDED.transaction_count,
                    max_seq = GREATEST(batch_aggregates.max_seq, EXCLUDED.max_seq),
                    updated_at = NOW()
            )
            SELECT
                i.id AS "id!",
                i.user_id AS "user_id!",
                i.transaction_type AS "transaction_type!: CreditTransactionType",
                i.amount AS "amount!",
                i.source_id AS "source_id!",
                i.description,
                i.created_at AS "created_at!",
                i.seq AS "seq!",
                i.api_key_id,
                b.balance AS "new_balance!"
            FROM inserted i, bumped b
            "#,
            request.user_id,
            &request.transaction_type as &CreditTransactionType,
            request.amount,
            request.source_id,
            request.description,
            request.fusillade_batch_id,
            request.api_key_id,
            signed_amount,
        )
        .fetch_one(&mut *self.db)
        .await?;

        trace!("Created transaction {} for user_id {}", row.id, request.user_id);

        let new_balance = row.new_balance;
        let old_balance = new_balance - signed_amount;
        if old_balance <= Decimal::ZERO && new_balance > Decimal::ZERO {
            trace!("Balance crossed zero upward for user_id {}, notifying onwards", request.user_id);
            self.notify_balance_crossing().await?;
        } else if old_balance > Decimal::ZERO && new_balance <= Decimal::ZERO {
            trace!("Balance crossed zero downward for user_id {}, notifying onwards", request.user_id);
            self.notify_balance_crossing().await?;
        }

        Ok(CreditTransactionDBResponse {
            id: row.id,
            user_id: row.user_id,
            transaction_type: row.transaction_type,
            amount: row.amount,
            description: row.description,
            source_id: row.source_id,
            created_at: row.created_at,
            api_key_id: row.api_key_id,
            service_tier: None,
        })
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn has_purchased(&mut self, user_id: UserId) -> Result<bool> {
        let purchased = sqlx::query_scalar!(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM credits_transactions
                WHERE user_id = $1 AND transaction_type = 'purchase'
            ) AS "purchased!"
            "#,
            user_id
        )
        .fetch_one(&mut *self.db)
        .await?;

        Ok(purchased)
    }

    #[instrument(level = "debug", skip(self, user_ids), fields(count = user_ids.len()), err)]
    pub async fn has_purchased_bulk(&mut self, user_ids: &[UserId]) -> Result<HashSet<UserId>> {
        if user_ids.is_empty() {
            return Ok(HashSet::new());
        }

        let rows = sqlx::query_scalar!(
            r#"
            SELECT DISTINCT user_id AS "user_id!"
            FROM credits_transactions
            WHERE user_id = ANY($1::uuid[]) AND transaction_type = 'purchase'
            "#,
            user_ids
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(rows.into_iter().collect())
    }

    #[instrument(level = "debug", skip(self), fields(payee = %abbrev_uuid(&payee), match_up_to = %match_up_to), err)]
    pub async fn grant_first_payment_match(
        &mut self,
        match_up_to: Decimal,
        payee: UserId,
        payment_amount: Decimal,
        purchase_source_id: &str,
    ) -> Result<()> {
        if match_up_to <= Decimal::ZERO {
            return Ok(());
        }
        let match_amount = payment_amount.min(match_up_to);
        if match_amount <= Decimal::ZERO {
            return Ok(());
        }

        let Some(purchase_seq) = sqlx::query_scalar!(
            "SELECT seq FROM credits_transactions WHERE source_id = $1 AND transaction_type = 'purchase'",
            purchase_source_id
        )
        .fetch_optional(&mut *self.db)
        .await?
        else {
            return Ok(());
        };

        let is_first = sqlx::query_scalar!(
            r#"
            SELECT NOT EXISTS (
                SELECT 1 FROM credits_transactions
                WHERE user_id = $1
                  AND transaction_type = 'purchase'
                  AND seq < $2
            ) AS "is_first!"
            "#,
            payee,
            purchase_seq
        )
        .fetch_one(&mut *self.db)
        .await?;

        if !is_first {
            return Ok(());
        }

        let request = CreditTransactionCreateDBRequest {
            user_id: payee,
            transaction_type: CreditTransactionType::AdminGrant,
            amount: match_amount,
            source_id: format!("{purchase_source_id}:first-payment-match"),
            description: Some(format!("First payment match bonus (matched {})", match_amount)),
            fusillade_batch_id: None,
            api_key_id: None,
        };

        if self.grant_once(&request).await? {
            trace!("Granted first-payment match of {} to payee {}", match_amount, payee);
        }
        Ok(())
    }

    async fn grant_once(&mut self, request: &CreditTransactionCreateDBRequest) -> Result<bool> {
        match self.create_transaction(request).await {
            Ok(_) => Ok(true),
            Err(crate::db::errors::DbError::UniqueViolation { constraint, .. })
                if constraint.as_deref() == Some("credits_transactions_source_id_unique") =>
            {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(level = "debug", skip(self), fields(payee = %abbrev_uuid(&payee), amount = %amount), err)]
    pub async fn grant_verification_credits(&mut self, amount: Decimal, payee: UserId, setup_source_id: &str) -> Result<()> {
        if amount <= Decimal::ZERO {
            return Ok(());
        }

        let request = CreditTransactionCreateDBRequest {
            user_id: payee,
            transaction_type: CreditTransactionType::AdminGrant,
            amount,
            source_id: format!("verification-credits:{payee}"),
            description: Some(format!("Signup credits for verifying a payment method ({setup_source_id})")),
            fusillade_batch_id: None,
            api_key_id: None,
        };

        if self.grant_once(&request).await? {
            trace!("Granted {} verification credits to payee {}", amount, payee);
        } else {
            trace!("Payee {} already received verification credits, skipping", payee);
        }
        Ok(())
    }

    pub async fn claim_verification_instrument(&mut self, fingerprint: &str, payee: UserId) -> Result<UserId> {
        sqlx::query("INSERT INTO verification_instruments (fingerprint, user_id) VALUES ($1, $2) ON CONFLICT (fingerprint) DO NOTHING")
            .bind(fingerprint)
            .bind(payee)
            .execute(&mut *self.db)
            .await?;

        let owner: Uuid = sqlx::query_scalar("SELECT user_id FROM verification_instruments WHERE fingerprint = $1")
            .bind(fingerprint)
            .fetch_one(&mut *self.db)
            .await?;
        Ok(owner)
    }

    async fn notify_balance_crossing(&mut self) -> Result<()> {
        let epoch_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();

        let payload = format!("credits_transactions:{}", epoch_micros);

        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(crate::config::ONWARDS_CONFIG_CHANGED_CHANNEL)
            .bind(&payload)
            .execute(&mut *self.db)
            .await?;

        Ok(())
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn get_user_balance(&mut self, user_id: UserId) -> Result<Decimal> {
        let balance = sqlx::query_scalar!(r#"SELECT balance FROM user_balance_checkpoints WHERE user_id = $1"#, user_id)
            .fetch_optional(&mut *self.db)
            .await?;

        Ok(balance.unwrap_or(Decimal::ZERO))
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn get_balance_for_admission(&mut self, user_id: UserId) -> Result<Option<Decimal>> {
        if super::feature_flags::FeatureFlags::new(&mut *self.db)
            .has_feature(user_id, super::feature_flags::FeatureFlag::AllowNegativeBalance)
            .await?
        {
            return Ok(None);
        }
        self.get_user_balance(user_id).await.map(Some)
    }

    #[instrument(level = "debug", skip(self, user_ids), fields(count = user_ids.len()), err)]
    pub async fn get_users_balances_bulk(&mut self, user_ids: &[UserId]) -> Result<HashMap<UserId, Decimal>> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query!(
            r#"
            SELECT
                u.user_id as "user_id!",
                COALESCE(c.balance, 0) as "balance!"
            FROM unnest($1::uuid[]) AS u(user_id)
            LEFT JOIN user_balance_checkpoints c ON c.user_id = u.user_id
            "#,
            user_ids
        )
        .fetch_all(&mut *self.db)
        .await?;

        let mut balances_map = HashMap::with_capacity(rows.len());
        for row in rows {
            balances_map.insert(row.user_id, row.balance);
        }

        Ok(balances_map)
    }

    #[instrument(level = "debug", skip(self, filters), fields(user_id = %abbrev_uuid(&user_id), skip = skip, limit = limit), err)]
    pub async fn list_user_transactions(
        &mut self,
        user_id: UserId,
        skip: i64,
        limit: i64,
        filters: &TransactionFilters,
    ) -> Result<Vec<CreditTransactionDBResponse>> {
        let transaction_types: Option<Vec<String>> = filters
            .transaction_types
            .as_ref()
            .map(|types| types.iter().map(transaction_type_to_string).collect());

        let transactions = sqlx::query_as!(
            CreditTransactionDBResponse,
            r#"
            SELECT id, user_id, transaction_type as "transaction_type: CreditTransactionType", amount, source_id, description, created_at, api_key_id, service_tier
            FROM credits_transactions
            WHERE user_id = $1
              AND ($4::text IS NULL OR description ILIKE '%' || $4 || '%')
              AND ($5::text[] IS NULL OR transaction_type::text = ANY($5))
              AND ($6::timestamptz IS NULL OR created_at >= $6)
              AND ($7::timestamptz IS NULL OR created_at <= $7)
            ORDER BY seq DESC
            OFFSET $2
            LIMIT $3
            "#,
            user_id,
            skip,
            limit,
            filters.search.as_deref(),
            transaction_types.as_deref(),
            filters.start_date,
            filters.end_date,
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(transactions)
    }

    #[instrument(level = "debug", skip(self, filters), fields(skip = skip, limit = limit), err)]
    pub async fn list_all_transactions(
        &mut self,
        skip: i64,
        limit: i64,
        filters: &TransactionFilters,
    ) -> Result<Vec<CreditTransactionDBResponse>> {
        let transaction_types: Option<Vec<String>> = filters
            .transaction_types
            .as_ref()
            .map(|types| types.iter().map(transaction_type_to_string).collect());

        let transactions = sqlx::query_as!(
            CreditTransactionDBResponse,
            r#"
            SELECT id, user_id, transaction_type as "transaction_type: CreditTransactionType", amount, source_id, description, created_at, api_key_id, service_tier
            FROM credits_transactions
            WHERE ($3::text IS NULL OR description ILIKE '%' || $3 || '%')
              AND ($4::text[] IS NULL OR transaction_type::text = ANY($4))
              AND ($5::timestamptz IS NULL OR created_at >= $5)
              AND ($6::timestamptz IS NULL OR created_at <= $6)
            ORDER BY seq DESC
            OFFSET $1
            LIMIT $2
            "#,
            skip,
            limit,
            filters.search.as_deref(),
            transaction_types.as_deref(),
            filters.start_date,
            filters.end_date,
        )
        .fetch_all(&mut *self.db)
        .await?;

        Ok(transactions)
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn get_transaction_by_id(&mut self, transaction_id: Uuid) -> Result<Option<CreditTransactionDBResponse>> {
        let transaction = sqlx::query_as!(
            CreditTransaction,
            r#"
            SELECT id, user_id, transaction_type as "transaction_type: CreditTransactionType",
                amount, source_id, description, created_at, seq, api_key_id
            FROM credits_transactions
            WHERE id = $1
            "#,
            transaction_id
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(transaction.map(CreditTransactionDBResponse::from))
    }

    pub async fn transaction_exists_by_source_id(&mut self, source_id: &str) -> Result<bool> {
        let result = sqlx::query!(
            r#"
            SELECT id FROM credits_transactions
            WHERE source_id = $1
            LIMIT 1
            "#,
            source_id
        )
        .fetch_optional(&mut *self.db)
        .await?;

        Ok(result.is_some())
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn get_monthly_auto_topup_spend(&mut self, user_id: UserId) -> Result<rust_decimal::Decimal> {
        let row = sqlx::query!(
            r#"
            SELECT COALESCE(SUM(amount), 0)::decimal(20, 9) as "total!"
            FROM credits_transactions
            WHERE user_id = $1
              AND source_id LIKE 'auto_topup_%'
              AND created_at >= date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
            "#,
            user_id
        )
        .fetch_one(&mut *self.db)
        .await?;

        Ok(row.total)
    }

    #[instrument(level = "debug", skip(self, user_ids), fields(count = user_ids.len()), err)]
    pub async fn get_monthly_auto_topup_spend_bulk(&mut self, user_ids: &[UserId]) -> Result<HashMap<UserId, rust_decimal::Decimal>> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query!(
            r#"
            SELECT user_id, COALESCE(SUM(amount), 0)::decimal(20, 9) as "total!"
            FROM credits_transactions
            WHERE user_id = ANY($1)
              AND source_id LIKE 'auto_topup_%'
              AND created_at >= date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
            GROUP BY user_id
            "#,
            user_ids
        )
        .fetch_all(&mut *self.db)
        .await?;

        let mut map = HashMap::with_capacity(rows.len());
        for row in rows {
            map.insert(row.user_id, row.total);
        }
        Ok(map)
    }

    #[instrument(level = "debug", skip(self, filters), fields(user_id = %abbrev_uuid(&user_id), count = count), err)]
    pub async fn sum_recent_transactions(&mut self, user_id: UserId, count: i64, filters: &TransactionFilters) -> Result<Decimal> {
        let result = sqlx::query!(
            r#"
            SELECT COALESCE(SUM(
                CASE WHEN transaction_type IN ('admin_grant', 'purchase') THEN amount ELSE -amount END
            ), 0) as "sum!"
            FROM (
                SELECT transaction_type, amount
                FROM credits_transactions
                WHERE user_id = $1
                  AND ($3::timestamptz IS NULL OR created_at >= $3)
                  AND ($4::timestamptz IS NULL OR created_at <= $4)
                ORDER BY seq DESC
                LIMIT $2
            ) recent
            "#,
            user_id,
            count,
            filters.start_date,
            filters.end_date,
        )
        .fetch_one(&mut *self.db)
        .await?;

        Ok(result.sum)
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn sum_transactions_after_date(&mut self, user_id: UserId, after_date: DateTime<Utc>) -> Result<Decimal> {
        let result = sqlx::query!(
            r#"
            SELECT COALESCE(SUM(
                CASE WHEN transaction_type IN ('admin_grant', 'purchase') THEN amount ELSE -amount END
            ), 0) as "sum!"
            FROM credits_transactions
            WHERE user_id = $1
              AND created_at > $2
            "#,
            user_id,
            after_date,
        )
        .fetch_one(&mut *self.db)
        .await?;

        Ok(result.sum)
    }

    #[instrument(level = "debug", skip(self), fields(user_id = %abbrev_uuid(&user_id)), err)]
    pub async fn sum_transactions_after_date_grouped(&mut self, user_id: UserId, after_date: DateTime<Utc>) -> Result<Decimal> {
        let result = sqlx::query!(
            r#"
            SELECT COALESCE(SUM(signed_amount), 0) as "sum!"
            FROM (
                -- Batch aggregates after the date
                SELECT -ba.total_amount as signed_amount
                FROM batch_aggregates ba
                WHERE ba.user_id = $1
                  AND ba.created_at > $2

                UNION ALL

                -- Non-batched transactions after the date
                SELECT
                    CASE WHEN ct.transaction_type IN ('admin_grant', 'purchase')
                        THEN ct.amount
                        ELSE -ct.amount
                    END as signed_amount
                FROM credits_transactions ct
                WHERE ct.user_id = $1
                  AND ct.fusillade_batch_id IS NULL
                  AND ct.created_at > $2
            ) after_date
            "#,
            user_id,
            after_date,
        )
        .fetch_one(&mut *self.db)
        .await?;

        Ok(result.sum)
    }

    #[instrument(level = "debug", skip(self, filters), fields(user_id = %abbrev_uuid(&user_id), count = count), err)]
    pub async fn sum_recent_transactions_grouped(&mut self, user_id: UserId, count: i64, filters: &TransactionFilters) -> Result<Decimal> {
        let result = sqlx::query!(
            r#"
            SELECT COALESCE(SUM(signed_amount), 0) as "sum!"
            FROM (
                SELECT * FROM (
                    (SELECT
                        ba.max_seq,
                        -ba.total_amount as signed_amount
                    FROM batch_aggregates ba
                    WHERE ba.user_id = $1
                      AND ($3::timestamptz IS NULL OR ba.created_at >= $3)
                      AND ($4::timestamptz IS NULL OR ba.created_at <= $4)
                    ORDER BY ba.max_seq DESC
                    LIMIT $2)

                    UNION ALL

                    -- Non-batched transactions
                    (SELECT
                        ct.seq as max_seq,
                        CASE WHEN ct.transaction_type IN ('admin_grant', 'purchase')
                            THEN ct.amount
                            ELSE -ct.amount
                        END as signed_amount
                    FROM credits_transactions ct
                    WHERE ct.user_id = $1
                      AND ct.fusillade_batch_id IS NULL
                      AND ($3::timestamptz IS NULL OR ct.created_at >= $3)
                      AND ($4::timestamptz IS NULL OR ct.created_at <= $4)
                    ORDER BY ct.seq DESC
                    LIMIT $2)
                ) combined
                ORDER BY max_seq DESC
                LIMIT $2
            ) recent
            "#,
            user_id,
            count,
            filters.start_date,
            filters.end_date,
        )
        .fetch_one(&mut *self.db)
        .await?;

        Ok(result.sum)
    }

    #[instrument(level = "debug", skip(self, filters), fields(user_id = %abbrev_uuid(&user_id), skip = skip, limit = limit), err)]
    pub async fn list_transactions_with_batches(
        &mut self,
        user_id: UserId,
        skip: i64,
        limit: i64,
        filters: &TransactionFilters,
    ) -> Result<Vec<TransactionWithCategory>> {
        let transaction_types: Option<Vec<String>> = filters
            .transaction_types
            .as_ref()
            .map(|types| types.iter().map(transaction_type_to_string).collect());

        let include_batches = filters
            .transaction_types
            .as_ref()
            .map(|types| types.iter().any(|t| matches!(t, CreditTransactionType::Usage)))
            .unwrap_or(true);

        let search_matches_batch = filters
            .search
            .as_ref()
            .map(|s| "batch".contains(&s.to_lowercase()) || s.to_lowercase().contains("batch"))
            .unwrap_or(true);

        let fetch_limit = skip + limit;
        let rows = sqlx::query!(
            r#"
            SELECT * FROM (
                -- Top N from batch_aggregates (index scan on idx_batch_agg_user_seq)
                -- Only included if transaction_types filter includes 'usage' or is not set
                -- and search term matches "Batch" description
                -- service_tier read from batch_aggregates (async / batch).
                (SELECT
                    ba.fusillade_batch_id as id,
                    ba.user_id,
                    'usage' as "transaction_type!: CreditTransactionType",
                    ba.total_amount as amount,
                    ba.fusillade_batch_id::text as source_id,
                    'Batch'::text as description,
                    ba.created_at,
                    ba.max_seq,
                    ba.fusillade_batch_id as batch_id,
                    ba.transaction_count as batch_count,
                    ba.service_tier as service_tier
                FROM batch_aggregates ba
                WHERE ba.user_id = $1
                  AND $7::bool = true
                  AND $10::bool = true
                  AND ($5::text IS NULL OR 'Batch' ILIKE '%' || $5 || '%')
                  AND ($8::timestamptz IS NULL OR ba.created_at >= $8)
                  AND ($9::timestamptz IS NULL OR ba.created_at <= $9)
                ORDER BY ba.max_seq DESC
                LIMIT $2)

                UNION ALL

                -- Top N from non-batched transactions (index scan on idx_credits_tx_non_batched)
                -- service_tier read from the ledger (realtime / flex).
                (SELECT
                    ct.id,
                    ct.user_id,
                    ct.transaction_type as "transaction_type!: CreditTransactionType",
                    ct.amount,
                    ct.source_id,
                    ct.description,
                    ct.created_at,
                    ct.seq as max_seq,
                    NULL::uuid as batch_id,
                    1::int as batch_count,
                    ct.service_tier as service_tier
                FROM credits_transactions ct
                WHERE ct.user_id = $1
                  AND ct.fusillade_batch_id IS NULL
                  AND ($5::text IS NULL OR ct.description ILIKE '%' || $5 || '%')
                  AND ($6::text[] IS NULL OR ct.transaction_type::text = ANY($6))
                  AND ($8::timestamptz IS NULL OR ct.created_at >= $8)
                  AND ($9::timestamptz IS NULL OR ct.created_at <= $9)
                ORDER BY ct.seq DESC
                LIMIT $2)
            ) combined
            ORDER BY max_seq DESC
            LIMIT $3 OFFSET $4
            "#,
            user_id,
            fetch_limit,
            limit,
            skip,
            filters.search.as_deref(),
            transaction_types.as_deref(),
            include_batches,
            filters.start_date,
            filters.end_date,
            search_matches_batch,
        )
        .fetch_all(&mut *self.db)
        .await?;

        let mut results = Vec::new();
        for row in rows {
            let id = row.id.ok_or_else(|| sqlx::Error::Protocol("Query returned NULL id".to_string()))?;
            let row_user_id = row
                .user_id
                .ok_or_else(|| sqlx::Error::Protocol("Query returned NULL user_id".to_string()))?;
            let amount = row
                .amount
                .ok_or_else(|| sqlx::Error::Protocol("Query returned NULL amount".to_string()))?;
            let source_id = row
                .source_id
                .ok_or_else(|| sqlx::Error::Protocol("Query returned NULL source_id".to_string()))?;
            let created_at = row
                .created_at
                .ok_or_else(|| sqlx::Error::Protocol("Query returned NULL created_at".to_string()))?;

            let transaction = CreditTransactionDBResponse {
                id,
                user_id: row_user_id,
                transaction_type: row.transaction_type,
                amount,
                description: row.description,
                source_id,
                created_at,
                api_key_id: None,
                service_tier: row.service_tier.clone(),
            };
            results.push(TransactionWithCategory {
                transaction,
                batch_id: row.batch_id,
                service_tier: row.service_tier,
                batch_count: row.batch_count.unwrap_or(1),
            });
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::users::Role;
    use rust_decimal::Decimal;
    use sqlx::PgPool;
    use std::str::FromStr;
    use uuid::Uuid;

    async fn create_test_user(pool: &PgPool) -> UserId {
        let user_id = Uuid::new_v4();
        sqlx::query!(
            "INSERT INTO users (id, username, email, is_admin, auth_source) VALUES ($1, $2, $3, false, 'test')",
            user_id,
            format!("testuser_{}", user_id.simple()),
            format!("test_{}@example.com", user_id.simple())
        )
        .execute(pool)
        .await
        .expect("Failed to create test user");

        let role = Role::StandardUser;
        sqlx::query!("INSERT INTO user_roles (user_id, role) VALUES ($1, $2)", user_id, role as Role)
            .execute(pool)
            .await
            .expect("Failed to add user role");

        user_id
    }

    #[dwctl_test_macros::test]
    async fn test_allow_negative_balance_preserves_accounting(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        assert_eq!(
            Credits::new(&mut conn).get_balance_for_admission(user_id).await.unwrap(),
            Some(Decimal::ZERO)
        );
        sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', true) ON CONFLICT (user_id, feature_flag) DO UPDATE SET enabled = EXCLUDED.enabled")
            .bind(user_id)
            .execute(&pool)
            .await
            .unwrap();
        let mut credits = Credits::new(&mut conn);
        credits
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::from(25),
                source_id: Uuid::new_v4().to_string(),
                description: None,
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();
        assert_eq!(credits.get_user_balance(user_id).await.unwrap(), Decimal::from(-25));
        assert_eq!(credits.get_balance_for_admission(user_id).await.unwrap(), None);
        sqlx::query("INSERT INTO user_feature_flags (user_id, feature_flag, enabled) VALUES ($1, 'ALLOW_NEGATIVE_BALANCE', false) ON CONFLICT (user_id, feature_flag) DO UPDATE SET enabled = EXCLUDED.enabled")
            .bind(user_id)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(credits.get_balance_for_admission(user_id).await.unwrap(), Some(Decimal::from(-25)));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_user_balance_zero_for_new_user(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::ZERO);

        let row_balance = sqlx::query_scalar!("SELECT balance FROM user_balance_checkpoints WHERE user_id = $1", user_id)
            .fetch_one(&pool)
            .await
            .expect("checkpoint row must exist from the user-creation trigger");
        assert_eq!(row_balance, Decimal::ZERO);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_admin_grant(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let request = CreditTransactionCreateDBRequest::admin_grant(
            user_id,
            user_id,
            Decimal::from_str("100.50").unwrap(),
            Some("Test grant".to_string()),
        );

        let transaction = credits.create_transaction(&request).await.expect("Failed to create transaction");

        assert_eq!(transaction.user_id, user_id);
        assert_eq!(transaction.transaction_type, CreditTransactionType::AdminGrant);
        assert_eq!(transaction.amount, Decimal::from_str("100.50").unwrap());
        assert_eq!(transaction.description, Some("Test grant".to_string()));

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("100.50").unwrap());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn batched_transaction_is_invisible_without_an_aggregate_row(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let batch_id = Uuid::new_v4();
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");

        sqlx::query!(
            r#"INSERT INTO credits_transactions
               (user_id, transaction_type, amount, source_id, description, fusillade_batch_id, service_tier)
               VALUES ($1, 'usage', $2, $3, 'batched usage', $4, 'batch')"#,
            user_id,
            Decimal::from_str("1.25").unwrap(),
            format!("test-batched-{}", Uuid::new_v4()),
            batch_id,
        )
        .execute(&mut *conn)
        .await
        .expect("insert batched usage");

        let mut credits = Credits::new(&mut conn);
        let listed = credits
            .list_transactions_with_batches(user_id, 0, 50, &TransactionFilters::default())
            .await
            .expect("list transactions");

        assert!(
            listed.is_empty(),
            "a batched transaction with no aggregate row must not appear; got {listed:?}"
        );

        let ledger: Decimal = sqlx::query_scalar!(
            r#"SELECT COALESCE(SUM(CASE WHEN transaction_type IN ('admin_grant','purchase')
                                        THEN amount ELSE -amount END), 0) AS "total!"
               FROM credits_transactions WHERE user_id = $1"#,
            user_id
        )
        .fetch_one(&mut *conn)
        .await
        .expect("sum ledger");
        assert_eq!(ledger, Decimal::from_str("-1.25").unwrap(), "the charge is in the ledger");

        let mut credits = Credits::new(&mut conn);
        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(
            balance,
            Decimal::ZERO,
            "a hand-written ledger row does not fold the checkpoint - heal is required"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn unbatched_correction_surfaces_as_its_own_line(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");

        sqlx::query!(
            r#"INSERT INTO credits_transactions
               (user_id, transaction_type, amount, source_id, description, fusillade_batch_id, service_tier)
               VALUES ($1, 'usage', $2, $3, 'usage correction', NULL, 'batch')"#,
            user_id,
            Decimal::from_str("1.25").unwrap(),
            format!("test-correction-{}", Uuid::new_v4()),
        )
        .execute(&mut *conn)
        .await
        .expect("insert correction");

        let mut credits = Credits::new(&mut conn);
        let listed = credits
            .list_transactions_with_batches(user_id, 0, 50, &TransactionFilters::default())
            .await
            .expect("list transactions");

        assert_eq!(listed.len(), 1, "the correction must be visible to the customer");
        assert_eq!(listed[0].transaction.description.as_deref(), Some("usage correction"));
        assert_eq!(listed[0].batch_id, None);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_user_balance_after_transactions(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let request1 = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, Decimal::from_str("100.0").unwrap(), None);
        credits.create_transaction(&request1).await.expect("Failed to create transaction");

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("100.0").unwrap());

        let request2 = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, Decimal::from_str("50.0").unwrap(), None);
        credits.create_transaction(&request2).await.expect("Failed to create transaction");

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("150.0").unwrap());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_user_balance_after_transactions_negative_balance(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let request1 = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, Decimal::from_str("100.0").unwrap(), None);
        credits.create_transaction(&request1).await.expect("Failed to create transaction");

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("100.0").unwrap());

        let request2 = CreditTransactionCreateDBRequest {
            user_id,
            transaction_type: CreditTransactionType::AdminRemoval,
            amount: Decimal::from_str("500.0").unwrap(),
            source_id: Uuid::new_v4().to_string(),
            description: None,
            fusillade_batch_id: None,
            api_key_id: None,
        };
        credits.create_transaction(&request2).await.expect("Failed to create transaction");

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("-400.0").unwrap());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_balance_after_multiple_transactions(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let request1 = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, Decimal::from_str("100.50").unwrap(), None);
        let transaction1 = credits
            .create_transaction(&request1)
            .await
            .expect("Failed to create first transaction");

        assert_eq!(transaction1.user_id, user_id);
        assert_eq!(transaction1.transaction_type, CreditTransactionType::AdminGrant);
        assert_eq!(transaction1.amount, Decimal::from_str("100.50").unwrap());
        assert_eq!(transaction1.description, None);

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("100.50").unwrap());

        let request2 = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, Decimal::from_str("50.0").unwrap(), None);

        let transaction2 = credits
            .create_transaction(&request2)
            .await
            .expect("Failed to create second transaction");

        assert_eq!(transaction2.user_id, user_id);
        assert_eq!(transaction2.transaction_type, CreditTransactionType::AdminGrant);
        assert_eq!(transaction2.amount, Decimal::from_str("50.0").unwrap());
        assert_eq!(transaction2.description, None);

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("150.50").unwrap());

        let request3 = CreditTransactionCreateDBRequest {
            user_id,
            transaction_type: CreditTransactionType::AdminRemoval,
            amount: Decimal::from_str("30.0").unwrap(),
            source_id: Uuid::new_v4().to_string(),
            description: Some("Usage deduction".to_string()),
            fusillade_batch_id: None,
            api_key_id: None,
        };

        let transaction3 = credits
            .create_transaction(&request3)
            .await
            .expect("Failed to create third transaction");

        assert_eq!(transaction3.user_id, user_id);
        assert_eq!(transaction3.transaction_type, CreditTransactionType::AdminRemoval);
        assert_eq!(transaction3.amount, Decimal::from_str("30.0").unwrap());
        assert_eq!(transaction3.description, Some("Usage deduction".to_string()));

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("120.50").unwrap());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_user_transactions_ordering(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);
        let n_of_transactions = 10;

        for i in 1..n_of_transactions + 1 {
            let request = CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from(i * 10),
                Some(format!("Transaction {}", i + 1)),
            );
            credits.create_transaction(&request).await.expect("Failed to create transaction");
        }

        let transactions = credits
            .list_user_transactions(user_id, 0, n_of_transactions, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");

        assert_eq!(transactions.len(), n_of_transactions as usize);
        for i in 0..(transactions.len() - 1) {
            let t1 = &transactions[i];
            let t2 = &transactions[i + 1];
            assert!(t1.created_at >= t2.created_at, "Transactions are not ordered correctly");
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_user_transaction(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);
        let n_of_transactions = 10;
        let mut transaction_ids = Vec::new();

        for i in 1..n_of_transactions + 1 {
            let request = CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from(i * 10),
                Some(format!("Transaction {}", i + 1)),
            );
            transaction_ids.push(credits.create_transaction(&request).await.expect("Failed to create transaction").id);
        }

        for i in 1..n_of_transactions + 1 {
            match credits
                .get_transaction_by_id(transaction_ids[i - 1])
                .await
                .expect("Failed to get transaction by ID {transaction_id}")
            {
                Some(tx) => {
                    assert_eq!(tx.id, transaction_ids[i - 1]);
                    assert_eq!(tx.user_id, user_id);
                    assert_eq!(tx.transaction_type, CreditTransactionType::AdminGrant);
                    assert_eq!(tx.amount, Decimal::from(i * 10));
                    assert_eq!(tx.description, Some(format!("Transaction {}", i + 1)));
                }
                None => panic!("Transaction ID {} not found", transaction_ids[i - 1]),
            };
        }

        let total_balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(total_balance, Decimal::from(550));

        assert!(
            credits
                .get_transaction_by_id(Uuid::new_v4())
                .await
                .expect("Failed to get transaction by ID 99999999999")
                .is_none()
        )
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_user_transactions_pagination(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let mut cumulative_balance = Decimal::ZERO;
        for i in 1..=5 {
            let amount = Decimal::from(i * 10);
            cumulative_balance += amount;
            let request = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, amount, None);
            credits.create_transaction(&request).await.expect("Failed to create transaction");
        }

        let transactions = credits
            .list_user_transactions(user_id, 0, 2, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");
        assert_eq!(transactions.len(), 2);

        let transactions = credits
            .list_user_transactions(user_id, 2, 2, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");
        assert_eq!(transactions.len(), 2);

        let transactions = credits
            .list_user_transactions(user_id, 10, 2, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");
        assert_eq!(transactions.len(), 0);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_user_transactions_filters_by_user(pool: PgPool) {
        let user1_id = create_test_user(&pool).await;
        let user2_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let request1 = CreditTransactionCreateDBRequest::admin_grant(user1_id, user1_id, Decimal::from_str("100.0").unwrap(), None);
        credits.create_transaction(&request1).await.expect("Failed to create transaction");

        let request2 = CreditTransactionCreateDBRequest::admin_grant(user2_id, user2_id, Decimal::from_str("200.0").unwrap(), None);
        credits.create_transaction(&request2).await.expect("Failed to create transaction");

        let transactions = credits
            .list_user_transactions(user1_id, 0, 10, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].user_id, user1_id);
        let balance = credits.get_user_balance(user1_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("100.0").unwrap());

        let transactions = credits
            .list_user_transactions(user2_id, 0, 10, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");
        assert_eq!(transactions.len(), 1);
        assert_eq!(transactions[0].user_id, user2_id);
        let balance = credits.get_user_balance(user2_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("200.0").unwrap());

        let non_existent_user_id = Uuid::new_v4();
        let transactions = credits
            .list_user_transactions(non_existent_user_id, 0, 10, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");
        assert_eq!(transactions.len(), 0);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_all_transactions(pool: PgPool) {
        let user1_id = create_test_user(&pool).await;
        let user2_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let request1 = CreditTransactionCreateDBRequest::admin_grant(
            user1_id,
            user1_id,
            Decimal::from_str("100.0").unwrap(),
            Some("User 1 grant".to_string()),
        );
        credits.create_transaction(&request1).await.expect("Failed to create transaction");

        let request2 = CreditTransactionCreateDBRequest::admin_grant(
            user2_id,
            user2_id,
            Decimal::from_str("200.0").unwrap(),
            Some("User 2 grant".to_string()),
        );
        credits.create_transaction(&request2).await.expect("Failed to create transaction");

        let transactions = credits
            .list_all_transactions(0, 10, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");

        assert!(transactions.len() >= 2);

        assert!(transactions.iter().any(|t| t.user_id == user1_id));
        assert!(transactions.iter().any(|t| t.user_id == user2_id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_all_transactions_pagination(pool: PgPool) {
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let mut cumulative_balance = Decimal::ZERO;
        for i in 1..10 {
            let amount = Decimal::from(i * 10);
            cumulative_balance += amount;
            let user_id = create_test_user(&pool).await;
            let request = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, amount, None);
            credits.create_transaction(&request).await.expect("Failed to create transaction");
        }

        let transactions = credits
            .list_all_transactions(0, 2, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");
        assert_eq!(transactions.len(), 2);

        let skip_page = credits
            .list_all_transactions(2, 2, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");
        assert!(skip_page.len() >= 2);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_with_all_transaction_types(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let request =
            CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, Decimal::from_str("100.0").unwrap(), Some("Grant".to_string()));
        let tx = credits.create_transaction(&request).await.expect("Failed to create AdminGrant");
        assert_eq!(tx.transaction_type, CreditTransactionType::AdminGrant);

        let request = CreditTransactionCreateDBRequest {
            user_id,
            transaction_type: CreditTransactionType::Purchase,
            amount: Decimal::from_str("50.0").unwrap(),
            source_id: Uuid::new_v4().to_string(),
            description: Some("Purchase".to_string()),
            fusillade_batch_id: None,
            api_key_id: None,
        };
        let tx = credits.create_transaction(&request).await.expect("Failed to create Purchase");
        assert_eq!(tx.transaction_type, CreditTransactionType::Purchase);

        let request = CreditTransactionCreateDBRequest {
            user_id,
            transaction_type: CreditTransactionType::Usage,
            amount: Decimal::from_str("25.0").unwrap(),
            source_id: Uuid::new_v4().to_string(),
            description: Some("Usage".to_string()),
            fusillade_batch_id: None,
            api_key_id: None,
        };
        let tx = credits.create_transaction(&request).await.expect("Failed to create Usage");
        assert_eq!(tx.transaction_type, CreditTransactionType::Usage);

        let request = CreditTransactionCreateDBRequest {
            user_id,
            transaction_type: CreditTransactionType::AdminRemoval,
            amount: Decimal::from_str("25.0").unwrap(),
            source_id: Uuid::new_v4().to_string(),
            description: Some("Removal".to_string()),
            fusillade_batch_id: None,
            api_key_id: None,
        };
        let tx = credits.create_transaction(&request).await.expect("Failed to create AdminRemoval");
        assert_eq!(tx.transaction_type, CreditTransactionType::AdminRemoval);

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("100.0").unwrap());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_transaction_rollback_on_error(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let request1 = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, Decimal::from_str("100.0").unwrap(), None);
        credits.create_transaction(&request1).await.expect("Failed to create transaction");

        let request2 = CreditTransactionCreateDBRequest::admin_grant(
            user_id,
            user_id,
            Decimal::from_str("-200.0").unwrap(),
            None,
        );
        let result = credits.create_transaction(&request2).await;
        assert!(result.is_err());

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("100.0").unwrap());

        let transactions = credits
            .list_user_transactions(user_id, 0, 10, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");
        assert_eq!(transactions.len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_concurrent_transactions_balance_correctness(pool: PgPool) {
        use std::sync::Arc;
        use tokio::task;

        let user_id = create_test_user(&pool).await;

        let mut conn: sqlx::pool::PoolConnection<sqlx::Postgres> = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);
        let initial_request = CreditTransactionCreateDBRequest::admin_grant(
            user_id,
            user_id,
            Decimal::from_str("1000.0").unwrap(),
            Some("Initial balance".to_string()),
        );
        credits
            .create_transaction(&initial_request)
            .await
            .expect("Failed to create initial transaction");
        drop(conn);

        let pool = Arc::new(pool);
        let mut handles = vec![];

        for i in 0..100 {
            let pool_clone = Arc::clone(&pool);
            let handle = task::spawn(async move {
                let mut conn = pool_clone.acquire().await.expect("Failed to acquire connection");
                let mut credits = Credits::new(&mut conn);

                let request = CreditTransactionCreateDBRequest {
                    user_id,
                    transaction_type: if i % 2 == 0 {
                        CreditTransactionType::AdminGrant
                    } else {
                        CreditTransactionType::AdminRemoval
                    },
                    amount: if i % 2 == 0 {
                        Decimal::from_str("10.0").unwrap()
                    } else {
                        Decimal::from_str("5.0").unwrap()
                    },
                    source_id: Uuid::new_v4().to_string(),
                    description: Some(format!("Concurrent transaction {}", i)),
                    fusillade_batch_id: None,
                    api_key_id: None,
                };

                credits.create_transaction(&request).await.expect("Failed to create transaction")
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("Task panicked");
        }

        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);
        let transactions = credits
            .list_user_transactions(user_id, 0, 1000, &TransactionFilters::default())
            .await
            .expect("Failed to list transactions");

        assert_eq!(transactions.len(), 101, "Should have 101 transactions");

        let final_balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(
            final_balance,
            Decimal::from_str("1250.0").unwrap(),
            "Expected 1250.0 but got {}",
            final_balance
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_balance_restored_notification_on_admin_grant(pool: PgPool) {
        use sqlx::postgres::PgListener;
        use std::time::Duration;
        use tokio::time::timeout;

        let user_id = create_test_user(&pool).await;

        let mut listener = PgListener::connect_with(&pool).await.expect("Failed to create listener");
        listener.listen("auth_config_changed").await.expect("Failed to listen");

        {
            let mut conn = pool.acquire().await.expect("Failed to acquire connection");
            let mut credits = Credits::new(&mut conn);

            let grant = CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("10.0").unwrap(),
                Some("Initial grant".to_string()),
            );
            credits.create_transaction(&grant).await.expect("Failed to grant");
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        while timeout(Duration::from_millis(10), listener.try_recv()).await.is_ok() {}

        {
            let mut conn = pool.acquire().await.expect("Failed to acquire connection");
            let mut credits = Credits::new(&mut conn);

            let usage = CreditTransactionCreateDBRequest {
                user_id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::from_str("15.0").unwrap(),
                source_id: Uuid::new_v4().to_string(),
                description: Some("Usage to go negative".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            };
            credits.create_transaction(&usage).await.expect("Failed to use");
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
        while timeout(Duration::from_millis(10), listener.try_recv()).await.is_ok() {}

        {
            let mut conn = pool.acquire().await.expect("Failed to acquire connection");
            let mut credits = Credits::new(&mut conn);

            let grant = CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("20.0").unwrap(),
                Some("Grant to restore balance".to_string()),
            );
            credits.create_transaction(&grant).await.expect("Failed to grant");
        }

        let notification = timeout(Duration::from_secs(2), listener.recv())
            .await
            .expect("Timeout waiting for notification")
            .expect("Failed to receive notification");

        assert_eq!(notification.channel(), "auth_config_changed");

        let payload = notification.payload();
        assert!(
            payload.starts_with("credits_transactions:"),
            "Expected payload to start with 'credits_transactions:', got: {}",
            payload
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_large_amounts(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let large_amount = Decimal::from_str("100000000.00").unwrap();
        let request = CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, large_amount, Some("Large credit grant".to_string()));

        let transaction = credits
            .create_transaction(&request)
            .await
            .expect("Failed to create large transaction");

        assert_eq!(transaction.user_id, user_id);
        assert_eq!(transaction.amount, large_amount);

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, large_amount);

        let request2 =
            CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, large_amount, Some("Second large grant".to_string()));

        credits
            .create_transaction(&request2)
            .await
            .expect("Failed to create second large transaction");

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("200000000.00").unwrap());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_preserves_high_precision(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let request = CreditTransactionCreateDBRequest::admin_grant(
            user_id,
            user_id,
            Decimal::from_str("100.12345678").unwrap(),
            Some("High precision grant".to_string()),
        );

        let transaction = credits.create_transaction(&request).await.expect("Failed to create transaction");

        assert_eq!(transaction.amount, Decimal::from_str("100.12345678").unwrap());

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("100.12345678").unwrap());

        let micro_request = CreditTransactionCreateDBRequest {
            user_id,
            transaction_type: CreditTransactionType::Usage,
            amount: Decimal::from_str("0.000000405").unwrap(),
            source_id: "micro-txn".to_string(),
            description: Some("Micro-transaction".to_string()),
            fusillade_batch_id: None,
            api_key_id: None,
        };

        let micro_transaction = credits
            .create_transaction(&micro_request)
            .await
            .expect("Failed to create micro-transaction");

        assert_eq!(micro_transaction.amount, Decimal::from_str("0.000000405").unwrap());

        let balance = credits.get_user_balance(user_id).await.expect("Failed to get balance");
        assert_eq!(balance, Decimal::from_str("100.123456375").unwrap());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_transactions_with_date_range_filter(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("100.0").unwrap(),
                Some("Transaction 1".to_string()),
            ))
            .await
            .expect("Failed to create transaction 1");

        let tx2 = credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("200.0").unwrap(),
                Some("Transaction 2".to_string()),
            ))
            .await
            .expect("Failed to create transaction 2");

        credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("300.0").unwrap(),
                Some("Transaction 3".to_string()),
            ))
            .await
            .expect("Failed to create transaction 3");

        let filters = TransactionFilters {
            start_date: Some(tx2.created_at),
            end_date: Some(Utc::now() + chrono::Duration::hours(1)),
            ..Default::default()
        };

        let filtered_txs = credits
            .list_user_transactions(user_id, 0, 10, &filters)
            .await
            .expect("Failed to list filtered transactions");

        assert_eq!(filtered_txs.len(), 2, "Should return 2 transactions within date range");

        let all_txs = credits
            .list_user_transactions(user_id, 0, 10, &TransactionFilters::default())
            .await
            .expect("Failed to list all transactions");

        assert_eq!(all_txs.len(), 3, "Should return all 3 transactions with no date filter");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_transactions_with_only_start_date(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("100.0").unwrap(),
                Some("Transaction 1".to_string()),
            ))
            .await
            .expect("Failed to create transaction 1");

        let tx2 = credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("200.0").unwrap(),
                Some("Transaction 2".to_string()),
            ))
            .await
            .expect("Failed to create transaction 2");

        credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("300.0").unwrap(),
                Some("Transaction 3".to_string()),
            ))
            .await
            .expect("Failed to create transaction 3");

        let filters = TransactionFilters {
            start_date: Some(tx2.created_at),
            ..Default::default()
        };

        let filtered_txs = credits
            .list_user_transactions(user_id, 0, 10, &filters)
            .await
            .expect("Failed to list transactions with start_date");

        assert_eq!(filtered_txs.len(), 2, "Should return 2 transactions after cutoff");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_transactions_with_only_end_date(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("100.0").unwrap(),
                Some("Transaction 1".to_string()),
            ))
            .await
            .expect("Failed to create transaction 1");

        let tx2 = credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("200.0").unwrap(),
                Some("Transaction 2".to_string()),
            ))
            .await
            .expect("Failed to create transaction 2");

        credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("300.0").unwrap(),
                Some("Transaction 3".to_string()),
            ))
            .await
            .expect("Failed to create transaction 3");

        let filters = TransactionFilters {
            end_date: Some(tx2.created_at),
            ..Default::default()
        };

        let filtered_txs = credits
            .list_user_transactions(user_id, 0, 10, &filters)
            .await
            .expect("Failed to list transactions with end_date");

        assert_eq!(filtered_txs.len(), 2, "Should return 2 transactions before cutoff");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_all_transactions_with_date_filter(pool: PgPool) {
        let user1_id = create_test_user(&pool).await;
        let user2_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user1_id,
                user1_id,
                Decimal::from_str("100.0").unwrap(),
                Some("User 1 transaction".to_string()),
            ))
            .await
            .expect("Failed to create transaction");

        let tx2 = credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user2_id,
                user2_id,
                Decimal::from_str("200.0").unwrap(),
                Some("User 2 transaction".to_string()),
            ))
            .await
            .expect("Failed to create transaction");

        let filters = TransactionFilters {
            start_date: Some(tx2.created_at),
            ..Default::default()
        };

        let filtered_txs = credits
            .list_all_transactions(0, 10, &filters)
            .await
            .expect("Failed to list all transactions with filter");

        assert_eq!(filtered_txs.len(), 1, "Should have 1 transaction after cutoff");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_transactions_with_batches_date_filter(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let batch_id = Uuid::new_v4();

        let mut batch_txs = Vec::new();
        for i in 0..3 {
            let tx = credits
                .create_transaction(&CreditTransactionCreateDBRequest {
                    user_id,
                    transaction_type: CreditTransactionType::Usage,
                    amount: Decimal::from_str(&format!("{}.0", i + 1)).unwrap(),
                    source_id: format!("batch-{}", i),
                    description: Some(format!("Batch transaction {}", i)),
                    fusillade_batch_id: Some(batch_id),
                    api_key_id: None,
                })
                .await
                .expect("Failed to create batch transaction");
            batch_txs.push(tx);
        }

        let non_batch_tx = credits
            .create_transaction(&CreditTransactionCreateDBRequest::admin_grant(
                user_id,
                user_id,
                Decimal::from_str("100.0").unwrap(),
                Some("Non-batch transaction".to_string()),
            ))
            .await
            .expect("Failed to create non-batch transaction");

        let all_txs = credits
            .list_transactions_with_batches(user_id, 0, 10, &TransactionFilters::default())
            .await
            .expect("Failed to list all batched transactions");

        assert_eq!(all_txs.len(), 2, "Should have batch + non-batch");

        let filters = TransactionFilters {
            start_date: Some(non_batch_tx.created_at),
            ..Default::default()
        };

        let filtered_txs = credits
            .list_transactions_with_batches(user_id, 0, 10, &filters)
            .await
            .expect("Failed to list batched transactions with filter");

        assert_eq!(filtered_txs.len(), 1, "Should have only non-batch transaction");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_date_filter_handles_empty_results(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let request = CreditTransactionCreateDBRequest::admin_grant(
            user_id,
            user_id,
            Decimal::from_str("100.0").unwrap(),
            Some("Test transaction".to_string()),
        );
        credits.create_transaction(&request).await.expect("Failed to create transaction");

        let filters = TransactionFilters {
            start_date: Some(Utc::now() - chrono::Duration::days(7)),
            end_date: Some(Utc::now() - chrono::Duration::days(2)),
            ..Default::default()
        };

        let filtered_txs = credits
            .list_user_transactions(user_id, 0, 10, &filters)
            .await
            .expect("Failed to list transactions");

        assert_eq!(filtered_txs.len(), 0, "Should return no transactions outside date range");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_monthly_auto_topup_spend_zero_for_new_user(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        let spend = credits.get_monthly_auto_topup_spend(user_id).await.expect("Failed to get spend");
        assert_eq!(spend, Decimal::ZERO, "New user should have zero monthly auto-topup spend");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_monthly_auto_topup_spend_sums_only_auto_topup(pool: PgPool) {
        let user_id = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        credits
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id,
                transaction_type: CreditTransactionType::Purchase,
                amount: Decimal::from_str("25.0").unwrap(),
                source_id: format!("auto_topup_{}_2026-03-01T10:00", user_id),
                description: Some("Auto top-up".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();

        credits
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id,
                transaction_type: CreditTransactionType::Purchase,
                amount: Decimal::from_str("100.0").unwrap(),
                source_id: format!("manual_topup_{}", Uuid::new_v4()),
                description: Some("Manual purchase".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();

        credits
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id,
                transaction_type: CreditTransactionType::Purchase,
                amount: Decimal::from_str("25.0").unwrap(),
                source_id: format!("auto_topup_{}_2026-03-02T10:00", user_id),
                description: Some("Auto top-up 2".to_string()),
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();

        let spend = credits.get_monthly_auto_topup_spend(user_id).await.expect("Failed to get spend");
        assert_eq!(
            spend,
            Decimal::from_str("50.0").unwrap(),
            "Should sum only auto_topup_ transactions"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_monthly_auto_topup_spend_excludes_other_users(pool: PgPool) {
        let user_a = create_test_user(&pool).await;
        let user_b = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits = Credits::new(&mut conn);

        credits
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user_a,
                transaction_type: CreditTransactionType::Purchase,
                amount: Decimal::from_str("30.0").unwrap(),
                source_id: format!("auto_topup_{}_2026-03-01T10:00", user_a),
                description: None,
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();

        credits
            .create_transaction(&CreditTransactionCreateDBRequest {
                user_id: user_b,
                transaction_type: CreditTransactionType::Purchase,
                amount: Decimal::from_str("50.0").unwrap(),
                source_id: format!("auto_topup_{}_2026-03-01T10:00", user_b),
                description: None,
                fusillade_batch_id: None,
                api_key_id: None,
            })
            .await
            .unwrap();

        let spend_a = credits.get_monthly_auto_topup_spend(user_a).await.unwrap();
        assert_eq!(
            spend_a,
            Decimal::from_str("30.0").unwrap(),
            "User A should only see their own spend"
        );

        let spend_b = credits.get_monthly_auto_topup_spend(user_b).await.unwrap();
        assert_eq!(
            spend_b,
            Decimal::from_str("50.0").unwrap(),
            "User B should only see their own spend"
        );
    }

    async fn insert_purchase(credits: &mut Credits<'_>, user: UserId, amount: &str, source_id: &str) {
        let request = CreditTransactionCreateDBRequest {
            user_id: user,
            transaction_type: CreditTransactionType::Purchase,
            amount: Decimal::from_str(amount).unwrap(),
            source_id: source_id.to_string(),
            description: None,
            fusillade_batch_id: None,
            api_key_id: None,
        };
        credits.create_transaction(&request).await.expect("insert purchase");
    }

    async fn match_bonus_amount(pool: &PgPool, source_id: &str) -> Option<Decimal> {
        sqlx::query_scalar!(
            "SELECT amount FROM credits_transactions WHERE source_id = $1",
            format!("{source_id}:first-payment-match")
        )
        .fetch_optional(pool)
        .await
        .expect("query bonus")
    }

    #[dwctl_test_macros::test]
    async fn test_has_purchased_is_false_before_any_purchase(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        assert!(!credits.has_purchased(user).await.unwrap());

        insert_purchase(&mut credits, user, "30.0", "sess-1").await;

        assert!(credits.has_purchased(user).await.unwrap());
    }

    #[dwctl_test_macros::test]
    async fn test_has_purchased_ignores_grants_and_usage(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        for (transaction_type, source_id) in [
            (CreditTransactionType::AdminGrant, "signup-credits"),
            (CreditTransactionType::Usage, "some-request"),
            (CreditTransactionType::AdminRemoval, "clawback"),
        ] {
            credits
                .create_transaction(&CreditTransactionCreateDBRequest {
                    user_id: user,
                    transaction_type,
                    amount: Decimal::from_str("1.0").unwrap(),
                    source_id: source_id.to_string(),
                    description: None,
                    fusillade_batch_id: None,
                    api_key_id: None,
                })
                .await
                .unwrap();
        }

        assert!(!credits.has_purchased(user).await.unwrap());
    }

    #[dwctl_test_macros::test]
    async fn test_has_purchased_is_scoped_to_the_billing_target(pool: PgPool) {
        let payer = create_test_user(&pool).await;
        let other = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        insert_purchase(&mut credits, payer, "30.0", "sess-1").await;

        assert!(credits.has_purchased(payer).await.unwrap());
        assert!(!credits.has_purchased(other).await.unwrap());
    }

    #[dwctl_test_macros::test]
    async fn test_first_payment_match_grants_on_first_purchase(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        insert_purchase(&mut credits, user, "30.0", "sess-1").await;
        credits
            .grant_first_payment_match(
                Decimal::from_str("50.0").unwrap(),
                user,
                Decimal::from_str("30.0").unwrap(),
                "sess-1",
            )
            .await
            .unwrap();

        assert_eq!(match_bonus_amount(&pool, "sess-1").await, Some(Decimal::from_str("30.0").unwrap()));
    }

    #[dwctl_test_macros::test]
    async fn test_first_payment_match_caps_at_match_up_to(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        insert_purchase(&mut credits, user, "100.0", "sess-1").await;
        credits
            .grant_first_payment_match(
                Decimal::from_str("50.0").unwrap(),
                user,
                Decimal::from_str("100.0").unwrap(),
                "sess-1",
            )
            .await
            .unwrap();

        assert_eq!(match_bonus_amount(&pool, "sess-1").await, Some(Decimal::from_str("50.0").unwrap()));
    }

    #[dwctl_test_macros::test]
    async fn test_first_payment_match_skips_when_not_first(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        insert_purchase(&mut credits, user, "20.0", "sess-0").await;
        insert_purchase(&mut credits, user, "30.0", "sess-1").await;
        credits
            .grant_first_payment_match(
                Decimal::from_str("50.0").unwrap(),
                user,
                Decimal::from_str("30.0").unwrap(),
                "sess-1",
            )
            .await
            .unwrap();

        assert_eq!(match_bonus_amount(&pool, "sess-1").await, None);
    }

    #[dwctl_test_macros::test]
    async fn test_first_payment_match_disabled_when_zero(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        insert_purchase(&mut credits, user, "30.0", "sess-1").await;
        credits
            .grant_first_payment_match(Decimal::ZERO, user, Decimal::from_str("30.0").unwrap(), "sess-1")
            .await
            .unwrap();

        assert_eq!(match_bonus_amount(&pool, "sess-1").await, None);
    }

    #[dwctl_test_macros::test]
    async fn test_first_payment_match_idempotent(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        insert_purchase(&mut credits, user, "30.0", "sess-1").await;
        for _ in 0..2 {
            credits
                .grant_first_payment_match(
                    Decimal::from_str("50.0").unwrap(),
                    user,
                    Decimal::from_str("30.0").unwrap(),
                    "sess-1",
                )
                .await
                .unwrap();
        }

        let count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM credits_transactions WHERE source_id = $1",
            "sess-1:first-payment-match"
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, Some(1));
    }

    async fn verification_grant_amount(pool: &PgPool, payee: UserId) -> Option<Decimal> {
        sqlx::query_scalar!(
            "SELECT amount FROM credits_transactions WHERE source_id = $1",
            format!("verification-credits:{payee}")
        )
        .fetch_optional(pool)
        .await
        .expect("query verification grant")
    }

    async fn verification_grant_count(pool: &PgPool, user: UserId) -> i64 {
        sqlx::query_scalar!(
            "SELECT COUNT(*) FROM credits_transactions WHERE user_id = $1 AND source_id LIKE 'verification-credits:%'",
            user
        )
        .fetch_one(pool)
        .await
        .unwrap()
        .unwrap_or(0)
    }

    #[dwctl_test_macros::test]
    async fn test_verification_credits_granted_on_first_verification(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        assert_eq!(verification_grant_amount(&pool, user).await, None);

        credits
            .grant_verification_credits(Decimal::from_str("25.0").unwrap(), user, "cs_setup_1")
            .await
            .unwrap();

        assert_eq!(
            verification_grant_amount(&pool, user).await,
            Some(Decimal::from_str("25.0").unwrap())
        );
    }

    #[dwctl_test_macros::test]
    async fn test_verification_credits_disabled_at_zero(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        credits.grant_verification_credits(Decimal::ZERO, user, "cs_setup_1").await.unwrap();

        assert_eq!(verification_grant_amount(&pool, user).await, None);
    }

    #[dwctl_test_macros::test]
    async fn test_verification_credits_idempotent_for_one_session(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        for _ in 0..3 {
            credits
                .grant_verification_credits(Decimal::from_str("25.0").unwrap(), user, "cs_setup_1")
                .await
                .unwrap();
        }

        assert_eq!(verification_grant_count(&pool, user).await, 1);
    }

    #[dwctl_test_macros::test]
    async fn test_verification_credits_granted_only_once_per_user(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        credits
            .grant_verification_credits(Decimal::from_str("25.0").unwrap(), user, "cs_setup_1")
            .await
            .unwrap();
        credits
            .grant_verification_credits(Decimal::from_str("25.0").unwrap(), user, "cs_setup_2")
            .await
            .unwrap();

        assert_eq!(verification_grant_count(&pool, user).await, 1);
        let description: Option<String> = sqlx::query_scalar!(
            "SELECT description FROM credits_transactions WHERE source_id = $1",
            format!("verification-credits:{user}")
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            description.unwrap_or_default().contains("cs_setup_1"),
            "the triggering session should be recorded for reconciliation"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_verification_credits_survive_concurrent_sessions(pool: PgPool) {
        let user = create_test_user(&pool).await;

        let results = futures::future::join_all((0..5).map(|i| {
            let pool = pool.clone();
            async move {
                let mut conn = pool.acquire().await.unwrap();
                Credits::new(&mut conn)
                    .grant_verification_credits(Decimal::from_str("25.0").unwrap(), user, &format!("cs_setup_{i}"))
                    .await
            }
        }))
        .await;

        for result in results {
            result.expect("a losing racer must no-op, not error");
        }
        assert_eq!(verification_grant_count(&pool, user).await, 1);
    }

    #[dwctl_test_macros::test]
    async fn test_verification_credits_are_per_user(pool: PgPool) {
        let first = create_test_user(&pool).await;
        let second = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        credits
            .grant_verification_credits(Decimal::from_str("25.0").unwrap(), first, "cs_setup_1")
            .await
            .unwrap();
        credits
            .grant_verification_credits(Decimal::from_str("25.0").unwrap(), second, "cs_setup_2")
            .await
            .unwrap();

        assert_eq!(verification_grant_count(&pool, first).await, 1);
        assert_eq!(verification_grant_count(&pool, second).await, 1);
    }

    #[dwctl_test_macros::test]
    async fn test_instrument_claim_goes_to_first_account(pool: PgPool) {
        let first = create_test_user(&pool).await;
        let second = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        assert_eq!(credits.claim_verification_instrument("fp_shared", first).await.unwrap(), first);
        assert_eq!(
            credits.claim_verification_instrument("fp_shared", second).await.unwrap(),
            first,
            "a second account presenting the same instrument must see the first claimant"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_instrument_claim_is_idempotent_for_the_owner(pool: PgPool) {
        let user = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        for _ in 0..3 {
            assert_eq!(credits.claim_verification_instrument("fp_mine", user).await.unwrap(), user);
        }
    }

    #[dwctl_test_macros::test]
    async fn test_instrument_claim_race_has_one_winner(pool: PgPool) {
        let contenders: Vec<UserId> = vec![
            create_test_user(&pool).await,
            create_test_user(&pool).await,
            create_test_user(&pool).await,
            create_test_user(&pool).await,
        ];

        let owners = futures::future::join_all(contenders.iter().map(|&user| {
            let pool = pool.clone();
            async move {
                let mut conn = pool.acquire().await.unwrap();
                Credits::new(&mut conn).claim_verification_instrument("fp_raced", user).await
            }
        }))
        .await;

        let owners: Vec<UserId> = owners.into_iter().map(|r| r.expect("a losing racer must not error")).collect();
        let winner = owners[0];
        assert!(contenders.contains(&winner));
        assert!(
            owners.iter().all(|&o| o == winner),
            "every racer must observe the same owner: {owners:?}"
        );

        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM verification_instruments WHERE fingerprint = $1")
            .bind("fp_raced")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[dwctl_test_macros::test]
    async fn test_instrument_claim_rolled_back_frees_the_instrument(pool: PgPool) {
        let first = create_test_user(&pool).await;
        let second = create_test_user(&pool).await;

        let mut tx = pool.begin().await.unwrap();
        assert_eq!(
            Credits::new(&mut tx)
                .claim_verification_instrument("fp_abandoned", first)
                .await
                .unwrap(),
            first
        );
        tx.rollback().await.unwrap();

        let mut conn = pool.acquire().await.unwrap();
        assert_eq!(
            Credits::new(&mut conn)
                .claim_verification_instrument("fp_abandoned", second)
                .await
                .unwrap(),
            second,
            "a rolled-back claim must not hold the instrument"
        );
    }

    #[dwctl_test_macros::test]
    async fn test_instrument_claims_are_independent(pool: PgPool) {
        let first = create_test_user(&pool).await;
        let second = create_test_user(&pool).await;
        let mut conn = pool.acquire().await.unwrap();
        let mut credits = Credits::new(&mut conn);

        assert_eq!(credits.claim_verification_instrument("fp_a", first).await.unwrap(), first);
        assert_eq!(credits.claim_verification_instrument("fp_b", second).await.unwrap(), second);
    }
}
