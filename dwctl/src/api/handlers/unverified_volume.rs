
use fusillade::Storage;

use crate::errors::{Error, Result};
use crate::types::UserId;

use super::sla_capacity::parse_window_to_seconds;

#[derive(Clone, Copy)]
pub enum SubmissionKind {
    Batch,
    Flex,
}

pub async fn enforce_unverified_volume_limit<S: Storage>(
    request_manager: &S,
    per_hour: usize,
    owner: UserId,
    owner_verified: bool,
    completion_window: &str,
    requested: i64,
    kind: SubmissionKind,
) -> Result<()> {
    if per_hour == 0 {
        return Ok(());
    }

    if owner_verified {
        return Ok(());
    }

    let window_seconds = parse_window_to_seconds(completion_window);
    let cap = i64::try_from(per_hour).unwrap_or(i64::MAX).saturating_mul(window_seconds) / 3600;
    if cap <= 0 {
        return Ok(());
    }

    let cutoff = chrono::Utc::now() - chrono::Duration::seconds(window_seconds);
    let owner = owner.to_string();
    let current = match kind {
        SubmissionKind::Batch => {
            request_manager
                .sum_owner_batch_requests_in_window(&owner, completion_window, cutoff, true)
                .await
        }
        SubmissionKind::Flex => {
            request_manager
                .count_owner_flex_requests_since_capped(&owner, cutoff, true, cap.saturating_add(1))
                .await
        }
    }
    .map_err(|e| Error::Internal {
        operation: format!("count unverified upload volume: {e}"),
    })?;

    if current + requested > cap {
        let current = if current > cap {
            format!("more than {cap}")
        } else {
            current.to_string()
        };
        return Err(Error::TooManyRequests {
            message: format!(
                "Unverified accounts can submit at most {cap} request(s) per {completion_window} \
                 completion window. You have {current} in the last {completion_window}, so this \
                 submission of {requested} would exceed the limit. Verify your account by adding a \
                 payment method or making a payment to remove this limit."
            ),
        });
    }

    Ok(())
}
