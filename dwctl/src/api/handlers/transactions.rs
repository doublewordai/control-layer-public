
use sqlx_pool_router::PoolProvider;

use crate::{
    AppState,
    api::models::{
        transactions::{
            CreditTransactionCreate, CreditTransactionResponse, ListTransactionsQuery, TransactionFilters, TransactionListResponse,
        },
        users::CurrentUser,
    },
    auth::permissions::{self, RequiresPermission, operation, resource},
    db::{
        handlers::Credits,
        models::credits::{CreditTransactionCreateDBRequest, CreditTransactionType},
    },
    errors::{Error, Result},
    types::{Operation, Permission, Resource, UserId},
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
};

use rust_decimal::Decimal;
use uuid::Uuid;

#[utoipa::path(
    post,
    path = "/transactions",
    tag = "transactions",
    summary = "Create a credit transaction",
    description = "Create a new credit transaction to grant or remove credits (BillingManager role required)",
    request_body = CreditTransactionCreate,
    responses(
        (status = 201, description = "Transaction created successfully", body = CreditTransactionResponse),
        (status = 400, description = "Bad request - invalid transaction type or amount"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden - requires BillingManager role"),
        (status = 404, description = "User not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn create_transaction<P: PoolProvider>(
    State(state): State<AppState<P>>,
    actor: RequiresPermission<resource::Credits, operation::CreateAll>,
    Json(data): Json<CreditTransactionCreate>,
) -> Result<(StatusCode, Json<CreditTransactionResponse>)> {
    if data.amount <= Decimal::ZERO {
        return Err(Error::BadRequest {
            message: "Amount must be greater than zero".to_string(),
        });
    }

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Credits::new(&mut pool_conn);

    let db_request = CreditTransactionCreateDBRequest {
        user_id: data.user_id,
        transaction_type: CreditTransactionType::from(&data.transaction_type),
        amount: data.amount,
        source_id: data.source_id,
        description: data.description,
        fusillade_batch_id: None,
        api_key_id: None,
    };

    let transaction = repo.create_transaction(&db_request).await?;
    tracing::info!(actor_id = %actor.id, target_user_id = %data.user_id,
        transaction_id = %transaction.id, "credit transaction created");

    Ok((StatusCode::CREATED, Json(CreditTransactionResponse::from(transaction))))
}

#[utoipa::path(
    get,
    path = "/transactions/{transaction_id}",
    tag = "transactions",
    summary = "Get a specific transaction",
    description = "Get details of a specific credit transaction. Non-BillingManager users can only access their own transactions.",
    params(
        ("transaction_id" = i64, Path, description = "Transaction ID"),
    ),
    responses(
        (status = 200, description = "Transaction details", body = CreditTransactionResponse),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Transaction not found"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_transaction<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(transaction_id): Path<Uuid>,
    current_user: CurrentUser,
) -> Result<Json<CreditTransactionResponse>> {
    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Credits::new(&mut pool_conn);

    let can_read_all = permissions::has_permission(&current_user, Resource::Credits, Operation::ReadAll);

    let transaction = repo.get_transaction_by_id(transaction_id).await?;

    let transaction = match transaction {
        Some(tx) => {
            if !can_read_all && tx.user_id != current_user.id {
                let is_member = permissions::is_org_member(&current_user, tx.user_id, &mut pool_conn)
                    .await
                    .map_err(Error::Database)?;
                if !is_member {
                    return Err(Error::NotFound {
                        resource: "Transaction".to_string(),
                        id: transaction_id.to_string(),
                    });
                }
            }
            tx
        }
        None => {
            return Err(Error::NotFound {
                resource: "Transaction".to_string(),
                id: transaction_id.to_string(),
            });
        }
    };

    Ok(Json(CreditTransactionResponse::from(transaction)))
}

#[utoipa::path(
    get,
    path = "/transactions",
    tag = "transactions",
    summary = "List credit transactions",
    description = "Get a paginated list of credit transactions with balance context. By default, returns only the current user's transactions. Use 'all=true' to get all transactions (BillingManager/PlatformManager only). Use 'user_id' parameter to filter by a specific user (BillingManager/PlatformManager only for other users).",
    params(
        ListTransactionsQuery
    ),
    responses(
        (status = 200, description = "Paginated list of transactions with balance context", body = TransactionListResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden - cannot access other users' transactions or all transactions without proper permissions"),
        (status = 500, description = "Internal server error"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_transactions<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Query(query): Query<ListTransactionsQuery>,
    current_user: CurrentUser,
) -> Result<Json<TransactionListResponse>> {
    let skip = query.pagination.skip();
    let limit = query.pagination.limit();

    let can_read_all = permissions::has_permission(&current_user, Resource::Credits, Operation::ReadAll);

    if query.all == Some(true) && !can_read_all {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Credits, Operation::ReadAll),
            action: Operation::ReadAll,
            resource: "all transactions".to_string(),
        });
    }

    let filter_user_id = match (query.all, query.user_id) {
        (Some(true), _) => None,
        (_, Some(requested_user_id)) => {
            if !can_read_all && requested_user_id != current_user.id {
                let mut conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
                let is_member = permissions::is_org_member(&current_user, requested_user_id, &mut conn)
                    .await
                    .map_err(Error::Database)?;
                if !is_member {
                    return Err(Error::InsufficientPermissions {
                        required: Permission::Allow(Resource::Credits, Operation::ReadAll),
                        action: Operation::ReadAll,
                        resource: "transactions".to_string(),
                    });
                }
            }
            Some(requested_user_id)
        }
        (_, None) => Some(current_user.id),
    };

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Credits::new(&mut pool_conn);

    let filters = query.to_filters();

    let grouping_enabled = query.group_batches.unwrap_or(false) && filter_user_id.is_some();

    let mut transactions: Vec<CreditTransactionResponse> = if let (true, Some(user_id)) = (grouping_enabled, filter_user_id) {
        repo.list_transactions_with_batches(user_id, skip, limit + 1, &filters)
            .await?
            .into_iter()
            .map(|twc| CreditTransactionResponse::from_db_with_metadata(twc.transaction, twc.batch_id, twc.service_tier, twc.batch_count))
            .collect()
    } else if let Some(user_id) = filter_user_id {
        let txs = repo.list_user_transactions(user_id, skip, limit + 1, &filters).await?;
        txs.into_iter().map(CreditTransactionResponse::from).collect()
    } else {
        let txs = repo.list_all_transactions(skip, limit + 1, &filters).await?;
        txs.into_iter().map(CreditTransactionResponse::from).collect()
    };
    let has_more = transactions.len() as i64 > limit;
    transactions.truncate(limit as usize);

    let page_start_balance = if let Some(user_id) = filter_user_id {
        calculate_page_start_balance(&mut repo, user_id, skip, grouping_enabled, &filters).await?
    } else {
        Decimal::ZERO
    };

    Ok(Json(TransactionListResponse {
        data: transactions,
        has_more,
        page_start_balance,
    }))
}

async fn calculate_page_start_balance(
    repo: &mut Credits<'_>,
    user_id: UserId,
    skip: i64,
    use_grouped: bool,
    filters: &TransactionFilters,
) -> Result<Decimal> {
    let current_balance = repo.get_user_balance(user_id).await?;

    let balance_at_filter_end = if let Some(end_date) = filters.end_date {
        let after_sum = if use_grouped {
            repo.sum_transactions_after_date_grouped(user_id, end_date).await?
        } else {
            repo.sum_transactions_after_date(user_id, end_date).await?
        };
        current_balance - after_sum
    } else {
        current_balance
    };

    if skip == 0 {
        return Ok(balance_at_filter_end);
    }

    let skipped_sum = if use_grouped {
        repo.sum_recent_transactions_grouped(user_id, skip, filters).await?
    } else {
        repo.sum_recent_transactions(user_id, skip, filters).await?
    };

    Ok(balance_at_filter_end - skipped_sum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::models::users::Role,
        db::{handlers::Credits as CreditsHandler, models::credits::CreditTransactionCreateDBRequest},
        test::utils::*,
        types::UserId,
    };
    use rust_decimal::Decimal;
    use serde_json::json;
    use sqlx::PgPool;
    use std::str::FromStr;

    async fn create_initial_credit_transaction(pool: &PgPool, user_id: UserId, amount: &str) -> Uuid {
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits_repo = CreditsHandler::new(&mut conn);

        let amount_decimal = Decimal::from_str(amount).expect("Invalid decimal amount");
        let request =
            CreditTransactionCreateDBRequest::admin_grant(user_id, user_id, amount_decimal, Some("Initial credit grant".to_string()));

        credits_repo
            .create_transaction(&request)
            .await
            .expect("Failed to create transaction")
            .id
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_permission_matrix(pool: PgPool) {
        enum Source {
            Actor,
            Recipient,
        }

        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        for (role, allowed, source, description) in [
            (Role::BillingManager, true, Source::Recipient, "Test credit grant"),
            (Role::StandardUser, false, Source::Actor, "Unauthorized attempt"),
            (Role::PlatformManager, true, Source::Actor, "Test credit grant from PlatformManager"),
            (Role::RequestViewer, false, Source::Actor, "Unauthorized attempt"),
        ] {
            let actor = create_test_user(&pool, role.clone()).await;
            let user = create_test_user(&pool, Role::StandardUser).await;
            let source_id = match source {
                Source::Actor => actor.id,
                Source::Recipient => user.id,
            };
            let headers = add_auth_headers(&actor);
            let response = app
                .post("/admin/api/v1/transactions")
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({
                    "user_id": user.id.to_string(),
                    "transaction_type": "admin_grant",
                    "amount": "100.0",
                    "source_id": source_id.to_string(),
                    "description": description
                }))
                .await;
            let expected = if allowed {
                axum::http::StatusCode::CREATED
            } else {
                axum::http::StatusCode::FORBIDDEN
            };
            assert_eq!(response.status_code(), expected, "role {role:?}");
            if allowed {
                let transaction: CreditTransactionResponse = response.json();
                assert_eq!(transaction.user_id, user.id, "role {role:?}");
                assert_eq!(transaction.amount, Decimal::from_str("100.0").unwrap(), "role {role:?}");
                assert_eq!(transaction.transaction_type, CreditTransactionType::AdminGrant, "role {role:?}");
                assert_eq!(transaction.source_id, source_id.to_string(), "role {role:?}");
                assert_eq!(transaction.description.as_deref(), Some(description), "role {role:?}");
            }
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_own_transaction_as_standard_user(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let transaction_id = create_initial_credit_transaction(&pool, user.id, "50.0").await;

        let response = app
            .get(&format!("/admin/api/v1/transactions/{}", transaction_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let transaction: CreditTransactionResponse = response.json();
        assert_eq!(transaction.user_id, user.id);
        assert_eq!(transaction.id, transaction_id);
        assert_eq!(transaction.amount, Decimal::from_str("50.0").unwrap());
        assert_eq!(transaction.transaction_type, CreditTransactionType::AdminGrant);
        assert_eq!(transaction.description, Some("Initial credit grant".to_string()));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_other_user_transaction_returns_404(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        let transaction_id = create_initial_credit_transaction(&pool, user2.id, "50.0").await;

        let response = app
            .get(&format!("/admin/api/v1/transactions/{}", transaction_id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_not_found();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_billing_manager_can_view_any_transaction(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let transaction_id = create_initial_credit_transaction(&pool, user.id, "75.0").await;

        let response = app
            .get(&format!("/admin/api/v1/transactions/{}", transaction_id))
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .await;

        response.assert_status_ok();
        let transaction: CreditTransactionResponse = response.json();
        assert_eq!(transaction.user_id, user.id);
        assert_eq!(transaction.amount, Decimal::from_str("75.0").unwrap());
        assert_eq!(transaction.transaction_type, CreditTransactionType::AdminGrant);
        assert_eq!(transaction.description, Some("Initial credit grant".to_string()));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_transactions_returns_own_for_standard_user(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, user1.id, "100.0").await;
        create_initial_credit_transaction(&pool, user2.id, "200.0").await;

        let response = app
            .get("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(transactions.iter().all(|t| t.user_id == user1.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_transactions_with_other_user_id_forbidden(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        let response = app
            .get(&format!("/admin/api/v1/transactions?user_id={}", user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_billing_manager_can_list_all_transactions(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, billing_manager.id, "50.0").await;
        create_initial_credit_transaction(&pool, user1.id, "100.0").await;
        create_initial_credit_transaction(&pool, user2.id, "200.0").await;

        let response = app
            .get("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(transactions.iter().all(|t| t.user_id == billing_manager.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_billing_manager_can_filter_by_user_id(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, user1.id, "100.0").await;
        create_initial_credit_transaction(&pool, user2.id, "200.0").await;

        let response = app
            .get(&format!("/admin/api/v1/transactions?user_id={}", user1.id))
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(transactions.iter().all(|t| t.user_id == user1.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_validates_amount_zero(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let transaction_data = json!({
            "user_id": user.id.to_string(),
            "transaction_type": "admin_grant",
            "amount": "0",
            "source_id": billing_manager.id.to_string(),
            "description": "Invalid zero amount"
        });

        let response = app
            .post("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .json(&transaction_data)
            .await;

        response.assert_status_bad_request();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_validates_amount_negative(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let transaction_data = json!({
            "user_id": user.id.to_string(),
            "transaction_type": "admin_grant",
            "amount": "-50.0",
            "source_id": billing_manager.id.to_string(),
            "description": "Invalid negative amount"
        });

        let response = app
            .post("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .json(&transaction_data)
            .await;

        response.assert_status_bad_request();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_validates_type(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let transaction_data = json!({
            "user_id": user.id.to_string(),
            "transaction_type": "usage",
            "amount": "10.0",
            "description": "Invalid type"
        });

        let response = app
            .post("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .json(&transaction_data)
            .await;

        response.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_requires_user_id(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;

        let transaction_data = json!({
            "transaction_type": "admin_grant",
            "amount": "100.0",
            "source_id": billing_manager.id.to_string(),
            "description": "Missing user_id"
        });

        let response = app
            .post("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .json(&transaction_data)
            .await;

        response.assert_status_unprocessable_entity();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_transaction_insufficient_balance(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, user.id, "50.0").await;

        let transaction_data = json!({
            "user_id": user.id.to_string(),
            "transaction_type": "admin_removal",
            "amount": "100.0",
            "source_id": billing_manager.id.to_string(),
            "description": "Over removal"
        });

        let response = app
            .post("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .json(&transaction_data)
            .await;

        response.assert_status(axum::http::StatusCode::CREATED);
        let transaction: CreditTransactionResponse = response.json();
        assert_eq!(transaction.user_id, user.id);
        assert_eq!(transaction.amount, Decimal::from_str("100.0").unwrap());
        assert_eq!(transaction.transaction_type, CreditTransactionType::AdminRemoval);
        assert_eq!(transaction.source_id, billing_manager.id.to_string());
        assert_eq!(transaction.description, Some("Over removal".to_string()));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_own_transaction_as_request_viewer(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::RequestViewer).await;

        let transaction_id = create_initial_credit_transaction(&pool, user.id, "50.0").await;

        let response = app
            .get(&format!("/admin/api/v1/transactions/{}", transaction_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let transaction: CreditTransactionResponse = response.json();
        assert_eq!(transaction.user_id, user.id);
        assert_eq!(transaction.id, transaction_id);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_other_user_transaction_returns_404_request_viewer(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::RequestViewer).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        let transaction_id = create_initial_credit_transaction(&pool, user2.id, "50.0").await;

        let response = app
            .get(&format!("/admin/api/v1/transactions/{}", transaction_id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_not_found();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_can_view_any_transaction(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_user(&pool, Role::PlatformManager).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let transaction_id = create_initial_credit_transaction(&pool, user.id, "75.0").await;

        let response = app
            .get(&format!("/admin/api/v1/transactions/{}", transaction_id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_ok();
        let transaction: CreditTransactionResponse = response.json();
        assert_eq!(transaction.user_id, user.id);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_transactions_returns_own_for_request_viewer(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::RequestViewer).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, user1.id, "100.0").await;
        create_initial_credit_transaction(&pool, user2.id, "200.0").await;

        let response = app
            .get("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(transactions.iter().all(|t| t.user_id == user1.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_can_list_all_transactions(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_user(&pool, Role::PlatformManager).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, platform_manager.id, "50.0").await;
        create_initial_credit_transaction(&pool, user1.id, "100.0").await;
        create_initial_credit_transaction(&pool, user2.id, "200.0").await;

        let response = app
            .get("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(transactions.iter().all(|t| t.user_id == platform_manager.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_transactions_with_other_user_id_forbidden_request_viewer(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user1 = create_test_user(&pool, Role::RequestViewer).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        let response = app
            .get(&format!("/admin/api/v1/transactions?user_id={}", user2.id))
            .add_header(&add_auth_headers(&user1)[0].0, &add_auth_headers(&user1)[0].1)
            .add_header(&add_auth_headers(&user1)[1].0, &add_auth_headers(&user1)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_can_filter_by_user_id(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_user(&pool, Role::PlatformManager).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, user1.id, "100.0").await;
        create_initial_credit_transaction(&pool, user2.id, "200.0").await;

        let response = app
            .get(&format!("/admin/api/v1/transactions?user_id={}", user1.id))
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(transactions.iter().all(|t| t.user_id == user1.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_list_transactions_pagination(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        for i in 1..=5 {
            create_initial_credit_transaction(&pool, user.id, &format!("{}.0", i * 10)).await;
        }

        let response = app
            .get("/admin/api/v1/transactions?limit=2")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;
        assert_eq!(transactions.len(), 2);

        let response = app
            .get("/admin/api/v1/transactions?skip=2&limit=2")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;
        assert_eq!(transactions.len(), 2);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_billing_manager_without_params_returns_own(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, billing_manager.id, "50.0").await;
        create_initial_credit_transaction(&pool, user1.id, "100.0").await;
        create_initial_credit_transaction(&pool, user2.id, "200.0").await;

        let response = app
            .get("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(transactions.iter().all(|t| t.user_id == billing_manager.id));
        assert!(!transactions.is_empty());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_billing_manager_with_all_returns_all_transactions(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, billing_manager.id, "50.0").await;
        create_initial_credit_transaction(&pool, user1.id, "100.0").await;
        create_initial_credit_transaction(&pool, user2.id, "200.0").await;

        let response = app
            .get("/admin/api/v1/transactions?all=true")
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(transactions.iter().any(|t| t.user_id == billing_manager.id));
        assert!(transactions.iter().any(|t| t.user_id == user1.id));
        assert!(transactions.iter().any(|t| t.user_id == user2.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_standard_user_with_all_forbidden(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let response = app
            .get("/admin/api/v1/transactions?all=true")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_all_takes_precedence_over_user_id(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let billing_manager = create_test_user(&pool, Role::BillingManager).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, user1.id, "100.0").await;
        create_initial_credit_transaction(&pool, user2.id, "200.0").await;

        let response = app
            .get(&format!("/admin/api/v1/transactions?all=true&user_id={}", user1.id))
            .add_header(&add_auth_headers(&billing_manager)[0].0, &add_auth_headers(&billing_manager)[0].1)
            .add_header(&add_auth_headers(&billing_manager)[1].0, &add_auth_headers(&billing_manager)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(transactions.iter().any(|t| t.user_id == user1.id));
        assert!(transactions.iter().any(|t| t.user_id == user2.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_with_all_returns_all_transactions(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let platform_manager = create_test_user(&pool, Role::PlatformManager).await;
        let user1 = create_test_user(&pool, Role::StandardUser).await;
        let user2 = create_test_user(&pool, Role::StandardUser).await;

        create_initial_credit_transaction(&pool, platform_manager.id, "50.0").await;
        create_initial_credit_transaction(&pool, user1.id, "100.0").await;
        create_initial_credit_transaction(&pool, user2.id, "200.0").await;

        let response = app
            .get("/admin/api/v1/transactions?all=true")
            .add_header(&add_auth_headers(&platform_manager)[0].0, &add_auth_headers(&platform_manager)[0].1)
            .add_header(&add_auth_headers(&platform_manager)[1].0, &add_auth_headers(&platform_manager)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(transactions.iter().any(|t| t.user_id == platform_manager.id));
        assert!(transactions.iter().any(|t| t.user_id == user1.id));
        assert!(transactions.iter().any(|t| t.user_id == user2.id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_request_viewer_with_all_forbidden(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::RequestViewer).await;

        let response = app
            .get("/admin/api/v1/transactions?all=true")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_forbidden();
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_high_precision_decimal_serialization(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let transaction_id = create_initial_credit_transaction(&pool, user.id, "123.456789012345678").await;

        let response = app
            .get(&format!("/admin/api/v1/transactions/{}", transaction_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let transaction: CreditTransactionResponse = response.json();

        assert_eq!(transaction.id, transaction_id);
        assert_eq!(transaction.user_id, user.id);

        let response = app
            .get("/admin/api/v1/transactions")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert!(!transactions.is_empty());
        assert!(transactions.iter().any(|t| t.id == transaction_id));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_decimal_precision_preserved_in_json(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let precise_amount = "0.123456789012345";
        let transaction_id = create_initial_credit_transaction(&pool, user.id, precise_amount).await;

        let response = app
            .get(&format!("/admin/api/v1/transactions/{}", transaction_id))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();

        let json_text = response.text();

        let json_value: serde_json::Value = serde_json::from_str(&json_text).expect("Failed to parse JSON");

        println!("JSON amount field: {:?}", json_value["amount"]);

        match &json_value["amount"] {
            serde_json::Value::String(s) => {
                println!("✓ Amount serialized as string (arbitrary precision): {}", s);
                assert_eq!(s, precise_amount, "String representation should match exactly");
            }
            serde_json::Value::Number(n) => {
                println!("✗ Amount serialized as number (may lose precision): {}", n);
            }
            other => {
                panic!("Unexpected JSON type for amount: {:?}", other);
            }
        }

        let transaction: CreditTransactionResponse = serde_json::from_str(&json_text).expect("Failed to deserialize");
        assert_eq!(transaction.amount.to_string(), precise_amount);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_grouping_with_mixed_transactions(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits_repo = CreditsHandler::new(&mut conn);

        let grant_request = CreditTransactionCreateDBRequest::admin_grant(
            user.id,
            user.id,
            Decimal::from_str("1000.0").unwrap(),
            Some("Initial grant".to_string()),
        );
        credits_repo
            .create_transaction(&grant_request)
            .await
            .expect("Failed to create grant");

        let purchase_request = CreditTransactionCreateDBRequest {
            user_id: user.id,
            transaction_type: CreditTransactionType::Purchase,
            amount: Decimal::from_str("500.0").unwrap(),
            source_id: Uuid::new_v4().to_string(),
            description: Some("Purchase".to_string()),
            fusillade_batch_id: None,
            api_key_id: None,
        };
        credits_repo
            .create_transaction(&purchase_request)
            .await
            .expect("Failed to create purchase");

        let batch_id_1 = Uuid::new_v4();
        let batch_id_2 = Uuid::new_v4();

        for i in 0..5 {
            let analytics_record = sqlx::query!(
                r#"
                INSERT INTO http_analytics
                    (instance_id, correlation_id, timestamp, method, uri, model, user_id, fusillade_batch_id)
                VALUES ($1, $2, NOW(), $3, $4, $5, $6, $7)
                RETURNING id
                "#,
                Uuid::new_v4(),
                i as i64,
                "POST",
                "/ai/v1/chat/completions",
                "gpt-4",
                user.id,
                batch_id_1
            )
            .fetch_one(&pool)
            .await
            .expect("Failed to insert analytics");

            let usage_request = CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::from_str(&format!("{}.0", i + 1)).unwrap(),
                source_id: analytics_record.id.to_string(),
                description: Some(format!("Batch 1 request {}", i)),
                fusillade_batch_id: Some(batch_id_1),
                api_key_id: None,
            };
            credits_repo
                .create_transaction(&usage_request)
                .await
                .expect("Failed to create usage");
        }

        for i in 0..3 {
            let analytics_record = sqlx::query!(
                r#"
                INSERT INTO http_analytics
                    (instance_id, correlation_id, timestamp, method, uri, model, user_id, fusillade_batch_id)
                VALUES ($1, $2, NOW(), $3, $4, $5, $6, $7)
                RETURNING id
                "#,
                Uuid::new_v4(),
                (5 + i) as i64,
                "POST",
                "/ai/v1/chat/completions",
                "gpt-3.5-turbo",
                user.id,
                batch_id_2
            )
            .fetch_one(&pool)
            .await
            .expect("Failed to insert analytics");

            let usage_request = CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::from_str(&format!("{}.0", (i + 1) * 10)).unwrap(),
                source_id: analytics_record.id.to_string(),
                description: Some(format!("Batch 2 request {}", i)),
                fusillade_batch_id: Some(batch_id_2),
                api_key_id: None,
            };
            credits_repo
                .create_transaction(&usage_request)
                .await
                .expect("Failed to create usage");
        }

        for i in 0..2 {
            let analytics_record = sqlx::query!(
                r#"
                INSERT INTO http_analytics
                    (instance_id, correlation_id, timestamp, method, uri, model, user_id, fusillade_batch_id)
                VALUES ($1, $2, NOW(), $3, $4, $5, $6, NULL)
                RETURNING id
                "#,
                Uuid::new_v4(),
                (8 + i) as i64,
                "POST",
                "/ai/v1/chat/completions",
                "claude-3-sonnet",
                user.id
            )
            .fetch_one(&pool)
            .await
            .expect("Failed to insert analytics");

            let usage_request = CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::from_str(&format!("{}.0", i + 1)).unwrap(),
                source_id: analytics_record.id.to_string(),
                description: Some(format!("Individual request {}", i)),
                fusillade_batch_id: None,
                api_key_id: None,
            };
            credits_repo
                .create_transaction(&usage_request)
                .await
                .expect("Failed to create usage");
        }

        drop(conn);

        let response = app
            .get("/admin/api/v1/transactions?group_batches=false&limit=50")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert_eq!(transactions.len(), 12, "Should have 12 individual transactions without grouping");

        let grant_count = transactions
            .iter()
            .filter(|t| t.transaction_type == CreditTransactionType::AdminGrant)
            .count();
        let purchase_count = transactions
            .iter()
            .filter(|t| t.transaction_type == CreditTransactionType::Purchase)
            .count();
        let usage_count = transactions
            .iter()
            .filter(|t| t.transaction_type == CreditTransactionType::Usage)
            .count();

        assert_eq!(grant_count, 1, "Should have 1 admin grant");
        assert_eq!(purchase_count, 1, "Should have 1 purchase");
        assert_eq!(usage_count, 10, "Should have 10 usage transactions (5 + 3 + 2)");

        let response = app
            .get("/admin/api/v1/transactions?group_batches=true&limit=50")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let response_body: TransactionListResponse = response.json();
        let transactions = &response_body.data;

        assert_eq!(transactions.len(), 6, "Should have 6 transactions with batch grouping");

        let batch_1_txn = transactions
            .iter()
            .find(|t| t.description == Some("Batch".to_string()) && t.amount == Decimal::from_str("15.0").unwrap())
            .expect("Should have batch 1 aggregated transaction (amount 15.0)");

        let batch_2_txn = transactions
            .iter()
            .find(|t| t.description == Some("Batch".to_string()) && t.amount == Decimal::from_str("60.0").unwrap())
            .expect("Should have batch 2 aggregated transaction (amount 60.0)");

        assert!(batch_1_txn.batch_id.is_some(), "Batch 1 should have batch_id");

        assert!(batch_2_txn.batch_id.is_some(), "Batch 2 should have batch_id");

        let individual_usage_count = transactions
            .iter()
            .filter(|t| t.transaction_type == CreditTransactionType::Usage && t.batch_id.is_none())
            .count();
        assert_eq!(individual_usage_count, 2, "Should still have 2 individual usage transactions");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_batch_grouping_pagination(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits_repo = CreditsHandler::new(&mut conn);

        let grant_request = CreditTransactionCreateDBRequest::admin_grant(
            user.id,
            user.id,
            Decimal::from_str("10000.0").unwrap(),
            Some("Initial grant".to_string()),
        );
        credits_repo
            .create_transaction(&grant_request)
            .await
            .expect("Failed to create grant");

        for batch_num in 0..5 {
            let batch_id = Uuid::new_v4();
            for req_num in 0..10 {
                let analytics_record = sqlx::query!(
                    r#"
                    INSERT INTO http_analytics
                        (instance_id, correlation_id, timestamp, method, uri, model, user_id, fusillade_batch_id)
                    VALUES ($1, $2, NOW(), $3, $4, $5, $6, $7)
                    RETURNING id
                    "#,
                    Uuid::new_v4(),
                    (batch_num * 10 + req_num) as i64,
                    "POST",
                    "/ai/v1/chat/completions",
                    format!("model-{}", batch_num),
                    user.id,
                    batch_id
                )
                .fetch_one(&pool)
                .await
                .expect("Failed to insert analytics");

                let usage_request = CreditTransactionCreateDBRequest {
                    user_id: user.id,
                    transaction_type: CreditTransactionType::Usage,
                    amount: Decimal::from_str("1.0").unwrap(),
                    source_id: analytics_record.id.to_string(),
                    description: Some(format!("Batch {} request {}", batch_num, req_num)),
                    fusillade_batch_id: Some(batch_id),
                    api_key_id: None,
                };
                credits_repo
                    .create_transaction(&usage_request)
                    .await
                    .expect("Failed to create usage");
            }
        }

        drop(conn);


        let response = app
            .get("/admin/api/v1/transactions?group_batches=true&limit=3&skip=0")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let page1_body: TransactionListResponse = response.json();
        let page1 = &page1_body.data;
        assert_eq!(page1.len(), 3, "Page 1 should have 3 transactions");

        let response = app
            .get("/admin/api/v1/transactions?group_batches=true&limit=3&skip=3")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let page2_body: TransactionListResponse = response.json();
        let page2 = &page2_body.data;
        assert_eq!(page2.len(), 3, "Page 2 should have 3 transactions");

        let response = app
            .get("/admin/api/v1/transactions?group_batches=true&limit=3&skip=6")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let page3_body: TransactionListResponse = response.json();
        let page3 = &page3_body.data;
        assert_eq!(page3.len(), 0, "Page 3 should be empty");

        let mut all_ids = vec![];
        all_ids.extend(page1.iter().map(|t| t.id));
        all_ids.extend(page2.iter().map(|t| t.id));

        let unique_ids: std::collections::HashSet<_> = all_ids.iter().collect();
        assert_eq!(all_ids.len(), unique_ids.len(), "Should have no duplicate IDs across pages");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_page_start_balance_with_batch_grouping(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        let mut credits_repo = CreditsHandler::new(&mut conn);

        let grant_request = CreditTransactionCreateDBRequest::admin_grant(
            user.id,
            user.id,
            Decimal::from_str("1000.0").unwrap(),
            Some("Initial grant".to_string()),
        );
        credits_repo
            .create_transaction(&grant_request)
            .await
            .expect("Failed to create grant");

        let batch_id_1 = Uuid::new_v4();
        for i in 0..5 {
            let analytics_record = sqlx::query!(
                r#"
                INSERT INTO http_analytics
                    (instance_id, correlation_id, timestamp, method, uri, model, user_id, fusillade_batch_id)
                VALUES ($1, $2, NOW() - interval '3 hours', $3, $4, $5, $6, $7)
                RETURNING id
                "#,
                Uuid::new_v4(),
                i as i64,
                "POST",
                "/ai/v1/chat/completions",
                "gpt-4",
                user.id,
                batch_id_1
            )
            .fetch_one(&pool)
            .await
            .expect("Failed to insert analytics");

            let usage_request = CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::from_str("2.0").unwrap(),
                source_id: analytics_record.id.to_string(),
                description: Some(format!("Batch 1 request {}", i)),
                fusillade_batch_id: Some(batch_id_1),
                api_key_id: None,
            };
            credits_repo
                .create_transaction(&usage_request)
                .await
                .expect("Failed to create usage");
        }

        let batch_id_2 = Uuid::new_v4();
        for i in 0..10 {
            let analytics_record = sqlx::query!(
                r#"
                INSERT INTO http_analytics
                    (instance_id, correlation_id, timestamp, method, uri, model, user_id, fusillade_batch_id)
                VALUES ($1, $2, NOW() - interval '2 hours', $3, $4, $5, $6, $7)
                RETURNING id
                "#,
                Uuid::new_v4(),
                (5 + i) as i64,
                "POST",
                "/ai/v1/chat/completions",
                "gpt-4",
                user.id,
                batch_id_2
            )
            .fetch_one(&pool)
            .await
            .expect("Failed to insert analytics");

            let usage_request = CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::from_str("3.0").unwrap(),
                source_id: analytics_record.id.to_string(),
                description: Some(format!("Batch 2 request {}", i)),
                fusillade_batch_id: Some(batch_id_2),
                api_key_id: None,
            };
            credits_repo
                .create_transaction(&usage_request)
                .await
                .expect("Failed to create usage");
        }

        let batch_id_3 = Uuid::new_v4();
        for i in 0..3 {
            let analytics_record = sqlx::query!(
                r#"
                INSERT INTO http_analytics
                    (instance_id, correlation_id, timestamp, method, uri, model, user_id, fusillade_batch_id)
                VALUES ($1, $2, NOW() - interval '1 hour', $3, $4, $5, $6, $7)
                RETURNING id
                "#,
                Uuid::new_v4(),
                (15 + i) as i64,
                "POST",
                "/ai/v1/chat/completions",
                "gpt-4",
                user.id,
                batch_id_3
            )
            .fetch_one(&pool)
            .await
            .expect("Failed to insert analytics");

            let usage_request = CreditTransactionCreateDBRequest {
                user_id: user.id,
                transaction_type: CreditTransactionType::Usage,
                amount: Decimal::from_str("5.0").unwrap(),
                source_id: analytics_record.id.to_string(),
                description: Some(format!("Batch 3 request {}", i)),
                fusillade_batch_id: Some(batch_id_3),
                api_key_id: None,
            };
            credits_repo
                .create_transaction(&usage_request)
                .await
                .expect("Failed to create usage");
        }

        let payment_request = CreditTransactionCreateDBRequest {
            user_id: user.id,
            transaction_type: CreditTransactionType::Purchase,
            amount: Decimal::from_str("100.0").unwrap(),
            source_id: Uuid::new_v4().to_string(),
            description: Some("Dummy payment (test)".to_string()),
            fusillade_batch_id: None,
            api_key_id: None,
        };
        credits_repo
            .create_transaction(&payment_request)
            .await
            .expect("Failed to create payment");

        drop(conn);


        let response = app
            .get("/admin/api/v1/transactions?group_batches=true&limit=2&skip=0")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let page1: TransactionListResponse = response.json();

        assert_eq!(
            page1.page_start_balance,
            Decimal::from_str("1045.0").unwrap(),
            "Page 1 page_start_balance should be current balance $1045"
        );
        assert_eq!(page1.data.len(), 2, "Page 1 should have 2 items");

        assert_eq!(
            page1.data[0].description,
            Some("Dummy payment (test)".to_string()),
            "First item should be the payment"
        );
        assert_eq!(
            page1.data[0].amount,
            Decimal::from_str("100.0").unwrap(),
            "Payment amount should be $100"
        );

        assert_eq!(
            page1.data[1].description,
            Some("Batch".to_string()),
            "Second item should be Batch 3"
        );
        assert_eq!(
            page1.data[1].amount,
            Decimal::from_str("15.0").unwrap(),
            "Batch 3 amount should be $15"
        );

        let response = app
            .get("/admin/api/v1/transactions?group_batches=true&limit=2&skip=2")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let page2: TransactionListResponse = response.json();

        assert_eq!(
            page2.page_start_balance,
            Decimal::from_str("960.0").unwrap(),
            "Page 2 page_start_balance should be $960 (balance after Payment and Batch 3)"
        );
        assert_eq!(page2.data.len(), 2, "Page 2 should have 2 items");

        assert_eq!(
            page2.data[0].description,
            Some("Batch".to_string()),
            "First item on page 2 should be Batch 2"
        );
        assert_eq!(
            page2.data[0].amount,
            Decimal::from_str("30.0").unwrap(),
            "Batch 2 amount should be $30"
        );

        assert_eq!(
            page2.data[1].description,
            Some("Batch".to_string()),
            "Second item on page 2 should be Batch 1"
        );
        assert_eq!(
            page2.data[1].amount,
            Decimal::from_str("10.0").unwrap(),
            "Batch 1 amount should be $10"
        );

        let response = app
            .get("/admin/api/v1/transactions?group_batches=true&limit=2&skip=4")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let page3: TransactionListResponse = response.json();

        assert_eq!(
            page3.page_start_balance,
            Decimal::from_str("1000.0").unwrap(),
            "Page 3 page_start_balance should be $1000 (balance after initial grant only)"
        );
        assert_eq!(page3.data.len(), 1, "Page 3 should have 1 item (the initial grant)");

        assert_eq!(
            page3.data[0].description,
            Some("Initial grant".to_string()),
            "Only item on page 3 should be the initial grant"
        );
        assert_eq!(
            page3.data[0].amount,
            Decimal::from_str("1000.0").unwrap(),
            "Initial grant amount should be $1000"
        );

        assert!(page1.has_more, "Page 1 should report more grouped items");
        assert!(!page3.has_more, "Final page should report no more items");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_page_start_balance_with_date_filter(pool: PgPool) {
        let (app, _bg_services) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;

        sqlx::query!(
            r#"
            INSERT INTO credits_transactions (user_id, transaction_type, amount, source_id, description, created_at)
            VALUES ($1, 'admin_grant', $2, $3, $4, NOW() - interval '5 hours')
            "#,
            user.id,
            Decimal::from_str("1000.0").unwrap(),
            Uuid::new_v4().to_string(),
            "Initial grant",
        )
        .execute(&pool)
        .await
        .expect("Failed to create grant");

        sqlx::query!(
            r#"
            INSERT INTO credits_transactions (user_id, transaction_type, amount, source_id, description, created_at)
            VALUES ($1, 'usage', $2, $3, $4, NOW() - interval '3 hours')
            "#,
            user.id,
            Decimal::from_str("100.0").unwrap(),
            Uuid::new_v4().to_string(),
            "Usage 3 hours ago",
        )
        .execute(&pool)
        .await
        .expect("Failed to create transaction");

        sqlx::query!(
            r#"
            INSERT INTO credits_transactions (user_id, transaction_type, amount, source_id, description, created_at)
            VALUES ($1, 'usage', $2, $3, $4, NOW() - interval '2 hours')
            "#,
            user.id,
            Decimal::from_str("50.0").unwrap(),
            Uuid::new_v4().to_string(),
            "Usage 2 hours ago",
        )
        .execute(&pool)
        .await
        .expect("Failed to create transaction");

        sqlx::query!(
            r#"
            INSERT INTO credits_transactions (user_id, transaction_type, amount, source_id, description, created_at)
            VALUES ($1, 'usage', $2, $3, $4, NOW() - interval '1 hour')
            "#,
            user.id,
            Decimal::from_str("25.0").unwrap(),
            Uuid::new_v4().to_string(),
            "Usage 1 hour ago",
        )
        .execute(&pool)
        .await
        .expect("Failed to create transaction");

        sqlx::query!(
            r#"
            INSERT INTO credits_transactions (user_id, transaction_type, amount, source_id, description, created_at)
            VALUES ($1, 'purchase', $2, $3, $4, NOW() - interval '30 minutes')
            "#,
            user.id,
            Decimal::from_str("200.0").unwrap(),
            Uuid::new_v4().to_string(),
            "Purchase 30 minutes ago",
        )
        .execute(&pool)
        .await
        .expect("Failed to create transaction");

        sqlx::query!(
            r#"
            UPDATE user_balance_checkpoints c
            SET balance = (
                SELECT COALESCE(SUM(CASE WHEN transaction_type IN ('admin_grant', 'purchase') THEN amount ELSE -amount END), 0)
                FROM credits_transactions
                WHERE user_id = $1
            )
            WHERE c.user_id = $1
            "#,
            user.id
        )
        .execute(&pool)
        .await
        .expect("heal checkpoint from ledger");


        let response = app
            .get("/admin/api/v1/transactions?limit=10")
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let no_filter: TransactionListResponse = response.json();
        assert_eq!(
            no_filter.page_start_balance,
            Decimal::from_str("1025.0").unwrap(),
            "Without date filter, page_start_balance should be current balance $1025"
        );

        let end_date = (chrono::Utc::now() - chrono::Duration::minutes(90))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        let start_date = (chrono::Utc::now() - chrono::Duration::hours(4))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        let response = app
            .get(&format!(
                "/admin/api/v1/transactions?limit=10&start_date={}&end_date={}",
                start_date, end_date
            ))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let date_filtered: TransactionListResponse = response.json();

        assert_eq!(
            date_filtered.page_start_balance,
            Decimal::from_str("850.0").unwrap(),
            "With end_date filter, page_start_balance should be $850 (balance at that time)"
        );

        assert_eq!(date_filtered.data.len(), 2, "Should have 2 transactions in the filtered range");

        let end_date_earlier = (chrono::Utc::now() - chrono::Duration::minutes(150))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        let response = app
            .get(&format!(
                "/admin/api/v1/transactions?limit=10&start_date={}&end_date={}",
                start_date, end_date_earlier
            ))
            .add_header(&add_auth_headers(&user)[0].0, &add_auth_headers(&user)[0].1)
            .add_header(&add_auth_headers(&user)[1].0, &add_auth_headers(&user)[1].1)
            .await;

        response.assert_status_ok();
        let earlier_filtered: TransactionListResponse = response.json();

        assert_eq!(
            earlier_filtered.page_start_balance,
            Decimal::from_str("900.0").unwrap(),
            "With earlier end_date filter, page_start_balance should be $900"
        );

        assert_eq!(
            earlier_filtered.data.len(),
            1,
            "Should have 1 transaction in the earlier filtered range"
        );
    }
}
