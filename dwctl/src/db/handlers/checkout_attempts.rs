
use crate::db::errors::Result;
use crate::types::UserId;
use sqlx::{Connection, PgConnection};
use tracing::instrument;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckoutKind {
    Payment,
    Setup,
    AutoTopup,
}

impl CheckoutKind {
    fn as_str(self) -> &'static str {
        match self {
            CheckoutKind::Payment => "payment",
            CheckoutKind::Setup => "setup",
            CheckoutKind::AutoTopup => "auto_topup",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reservation {
    Reserved(Uuid),
    OverCap { unpaid: i64 },
}

pub struct CheckoutAttempts<'c> {
    db: &'c mut PgConnection,
}

impl<'c> CheckoutAttempts<'c> {
    pub fn new(db: &'c mut PgConnection) -> Self {
        Self { db }
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn reserve(
        &mut self,
        actor_id: UserId,
        payer_id: UserId,
        kind: CheckoutKind,
        max_unpaid: Option<i64>,
        window_hours: i32,
    ) -> Result<Reservation> {
        let mut tx = self.db.begin().await?;

        if let Some(max_unpaid) = max_unpaid {
            sqlx::query!(
                "SELECT pg_advisory_xact_lock(hashtextextended('checkout_attempts:' || $1, 0))",
                actor_id.to_string()
            )
            .execute(&mut *tx)
            .await?;

            let unpaid = sqlx::query_scalar!(
                r#"SELECT COUNT(*) AS "count!" FROM checkout_attempts
                   WHERE actor_id = $1 AND completed_at IS NULL
                     AND created_at > NOW() - make_interval(hours => $2)"#,
                actor_id,
                window_hours
            )
            .fetch_one(&mut *tx)
            .await?;
            if unpaid >= max_unpaid {
                return Ok(Reservation::OverCap { unpaid });
            }
        }

        let id = sqlx::query_scalar!(
            "INSERT INTO checkout_attempts (actor_id, payer_id, kind) VALUES ($1, $2, $3) RETURNING id",
            actor_id,
            payer_id,
            kind.as_str()
        )
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Reservation::Reserved(id))
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn bind_session(&mut self, attempt_id: Uuid, session_id: &str) -> Result<()> {
        sqlx::query!("UPDATE checkout_attempts SET session_id = $2 WHERE id = $1", attempt_id, session_id)
            .execute(&mut *self.db)
            .await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn release(&mut self, attempt_id: Uuid) -> Result<()> {
        sqlx::query!("DELETE FROM checkout_attempts WHERE id = $1 AND session_id IS NULL", attempt_id)
            .execute(&mut *self.db)
            .await?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self), err)]
    pub async fn mark_completed(&mut self, session_id: &str) -> Result<bool> {
        let rows = sqlx::query!(
            "UPDATE checkout_attempts SET completed_at = NOW() WHERE session_id = $1 AND completed_at IS NULL",
            session_id
        )
        .execute(&mut *self.db)
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}
