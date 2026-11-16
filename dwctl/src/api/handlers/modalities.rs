
use sqlx::PgConnection;

use crate::db::handlers::Organizations;
use crate::errors::{Error, Result};
use crate::modalities::Modality;
use crate::types::UserId;

pub async fn ensure_batch_enabled(conn: &mut PgConnection, account_id: UserId) -> Result<()> {
    let disabled = Organizations::new(conn).disabled_modalities(account_id).await?;
    if disabled.contains(Modality::Batch) {
        return Err(Error::ModalityDisabled { modality: Modality::Batch });
    }
    Ok(())
}
