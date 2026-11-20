
use crate::{
    AppState,
    api::models::{
        organizations::{
            AddMemberRequest, ApproveJoinRequestRequest, InviteDetailsResponse, InviteMemberRequest, InviteMemberResponse,
            ListOrganizationsQuery, OrganizationCreate, OrganizationMemberResponse, OrganizationResponse, OrganizationUpdate,
            PendingEmailChangeResponse, SetActiveOrganizationRequest, SetActiveOrganizationResponse, UpdateMemberRoleRequest,
        },
        pagination::PaginatedResponse,
        users::{CurrentUser, UserResponse},
    },
    auth::permissions::{
        can_manage_org_resource, can_read_all_resources, can_read_own_resource, can_update_all_resources, can_update_own_resource,
    },
    db::handlers::{Credits, Organizations, Repository, Users, api_keys::ApiKeys, organizations::OrganizationFilter},
    db::models::organizations::{OrganizationCreateDBRequest, OrganizationUpdateDBRequest},
    email::EmailService,
    errors::{Error, Result},
    types::{Operation, Permission, Resource, UserId, UserIdOrCurrent},
};
use chrono::Duration;
use rust_decimal::prelude::ToPrimitive;
use sha2::{Digest, Sha256};
use sqlx_pool_router::PoolProvider;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::Json,
};

const MAX_ORGS_PER_USER: i64 = 3;

fn org_email_name(display_name: Option<&str>, username: &str) -> String {
    display_name
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| username.split('~').next().unwrap_or(username).to_string())
}

fn person_email_name(display_name: Option<&str>, email: &str) -> String {
    display_name
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| email.to_string())
}

fn hash_invite_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

const VALID_ROLES: [&str; 3] = ["owner", "admin", "member"];

fn validate_role(role: &str) -> Result<()> {
    if !VALID_ROLES.contains(&role) {
        return Err(Error::BadRequest {
            message: format!("Invalid role '{}'. Must be one of: owner, admin, member", role),
        });
    }
    Ok(())
}

fn validate_contact_email(input: &str) -> Result<String> {
    let trimmed = input.trim().to_lowercase();
    if trimmed.is_empty() {
        return Err(Error::BadRequest {
            message: "Email address cannot be empty".to_string(),
        });
    }
    if trimmed.parse::<lettre::Address>().is_err() {
        return Err(Error::BadRequest {
            message: "Invalid email address".to_string(),
        });
    }
    Ok(trimmed)
}

const EMAIL_CHANGE_TOKEN_TTL_HOURS: i64 = 24;

static DNS_RESOLVER: tokio::sync::OnceCell<hickory_resolver::TokioResolver> = tokio::sync::OnceCell::const_new();

async fn dns_resolver() -> Result<&'static hickory_resolver::TokioResolver> {
    DNS_RESOLVER
        .get_or_try_init(|| async {
            hickory_resolver::TokioResolver::builder_tokio()
                .and_then(|b| b.build())
                .map_err(|e| Error::Internal {
                    operation: format!("initialize DNS resolver: {e}"),
                })
        })
        .await
}

async fn verify_email_deliverable(email: &str) -> Result<()> {
    let domain = email.rsplit_once('@').map(|(_, d)| d).ok_or_else(|| Error::BadRequest {
        message: "Invalid email address".to_string(),
    })?;
    if domain.is_empty() {
        return Err(Error::BadRequest {
            message: "Invalid email address".to_string(),
        });
    }

    let resolver = dns_resolver().await?;

    match resolver.mx_lookup(domain).await {
        Ok(mx) if !mx.answers().is_empty() => Ok(()),
        Ok(_) => {
            match resolver.lookup_ip(domain).await {
                Ok(ips) if ips.iter().next().is_some() => Ok(()),
                Ok(_) => Err(Error::BadRequest {
                    message: format!("Email domain '{domain}' has no mail servers and no address records"),
                }),
                Err(e) if e.is_no_records_found() => Err(Error::BadRequest {
                    message: format!("Email domain '{domain}' has no mail servers and no address records"),
                }),
                Err(e) => Err(Error::Internal {
                    operation: format!("DNS A lookup for {domain}: {e}"),
                }),
            }
        }
        Err(e) if e.is_no_records_found() => {
            match resolver.lookup_ip(domain).await {
                Ok(ips) if ips.iter().next().is_some() => Ok(()),
                Ok(_) => Err(Error::BadRequest {
                    message: format!("Email domain '{domain}' has no mail servers and no address records"),
                }),
                Err(ae) if ae.is_no_records_found() => Err(Error::BadRequest {
                    message: format!("Email domain '{domain}' has no mail servers and no address records"),
                }),
                Err(ae) => Err(Error::Internal {
                    operation: format!("DNS A lookup for {domain}: {ae}"),
                }),
            }
        }
        Err(e) => Err(Error::Internal {
            operation: format!("DNS MX lookup for {domain}: {e}"),
        }),
    }
}

async fn check_role_assignment_privilege(
    current_user: &CurrentUser,
    org_id: UserId,
    target_role: &str,
    is_platform_manager: bool,
    pool_conn: &mut sqlx::PgConnection,
) -> Result<()> {
    if target_role == "owner" && !is_platform_manager {
        let mut repo = Organizations::new(pool_conn);
        let caller_role = repo.get_user_org_role(current_user.id, org_id).await?;
        if caller_role.as_deref() != Some("owner") {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: "Only owners can assign the owner role".to_string(),
            });
        }
    }
    Ok(())
}

#[utoipa::path(
    post,
    path = "/organizations",
    tag = "organizations",
    summary = "Create organization",
    description = "Create a new organization. The authenticated user becomes the owner.",
    responses(
        (status = 201, description = "Organization created", body = OrganizationResponse),
        (status = 400, description = "Bad request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn create_organization<P: PoolProvider>(
    State(state): State<AppState<P>>,
    current_user: CurrentUser,
    Json(data): Json<OrganizationCreate>,
) -> Result<(StatusCode, Json<OrganizationResponse>)> {
    let is_platform_manager = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::CreateAll);

    if !is_platform_manager && !crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::CreateOwn) {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Organizations, Operation::CreateOwn),
            action: Operation::CreateOwn,
            resource: "Organizations".to_string(),
        });
    }

    if data.name.trim().is_empty() {
        return Err(Error::BadRequest {
            message: "Organization name cannot be empty".to_string(),
        });
    }

    let email = validate_contact_email(&data.email)?;

    let owner_id = if is_platform_manager {
        data.owner_id.unwrap_or(current_user.id)
    } else {
        if data.owner_id.is_some() {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::CreateAll),
                action: Operation::CreateAll,
                resource: "Organization owner assignment".to_string(),
            });
        }
        current_user.id
    };

    let owner_email = if owner_id == current_user.id {
        current_user.email.clone()
    } else {
        let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
        Users::new(&mut conn)
            .get_by_id(owner_id)
            .await?
            .ok_or(Error::NotFound {
                resource: "User".to_string(),
                id: owner_id.to_string(),
            })?
            .email
    };
    let config = state.current_config();
    let claimable_domain = crate::auth::utils::email_domain(&owner_email).filter(|d| !config.auth.is_personal_email_domain(d));

    let contact_domain = crate::auth::utils::email_domain(&email).ok_or_else(|| Error::BadRequest {
        message: "Contact email must include a domain".to_string(),
    })?;
    match claimable_domain.as_deref() {
        Some(domain) if contact_domain != domain => {
            return Err(Error::BadRequest {
                message: format!(
                    "Contact email must be at {domain}, the owner's email domain. Create the workspace with an address there, then change it afterwards - that sends a confirmation link to the new address."
                ),
            });
        }
        None if !email.eq_ignore_ascii_case(owner_email.trim()) => {
            return Err(Error::BadRequest {
                message: "Contact email must be the owner's own address, because their email domain is a personal one that no workspace can claim. Change it afterwards to use a different address - that sends a confirmation link to it.".to_string(),
            });
        }
        _ => {}
    }

    let username = match claimable_domain.as_deref() {
        Some(domain) => format!("{domain}~{}", &uuid::Uuid::new_v4().simple().to_string()[..8]),
        None => format!("user~{}", uuid::Uuid::new_v4().simple()),
    };

    let display_name = data.display_name.clone().or_else(|| Some(data.name.clone()));
    let db_request = OrganizationCreateDBRequest {
        name: username,
        email,
        display_name,
        avatar_url: None,
        created_by: owner_id,
    };

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Organizations::new(&mut pool_conn);

    let org_count = repo.count_user_organizations(owner_id).await?;
    if org_count >= MAX_ORGS_PER_USER {
        return Err(Error::BadRequest {
            message: format!("Cannot create organization: user is already a member of {MAX_ORGS_PER_USER} organizations (maximum)"),
        });
    }

    let org = repo.create(&db_request, &config.auth.default_user_roles).await?;

    if data.members_can_create_keys == Some(false) {
        repo.set_members_can_create_keys(org.id, false).await?;
    }
    let members_can_create_keys = data.members_can_create_keys.unwrap_or(true);

    let response = OrganizationResponse::from_user(UserResponse::from(org))
        .with_member_count(1)
        .with_members_can_create_keys(members_can_create_keys);

    Ok((StatusCode::CREATED, Json(response)))
}

#[utoipa::path(
    get,
    path = "/organizations",
    tag = "organizations",
    summary = "List organizations",
    description = "List organizations. Platform managers see all organizations; standard users see only those they belong to.",
    params(ListOrganizationsQuery),
    responses(
        (status = 200, description = "List of organizations", body = PaginatedResponse<OrganizationResponse>),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_organizations<P: PoolProvider>(
    State(state): State<AppState<P>>,
    current_user: CurrentUser,
    Query(query): Query<ListOrganizationsQuery>,
) -> Result<Json<PaginatedResponse<OrganizationResponse>>> {
    let can_all = can_read_all_resources(&current_user, Resource::Organizations);

    if can_all {
        let skip = query.pagination.skip();
        let limit = query.pagination.limit();
        let filter = OrganizationFilter::new(skip, limit);
        let filter = if let Some(search) = query.search {
            filter.with_search(search)
        } else {
            filter
        };

        let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let mut repo = Organizations::new(&mut pool_conn);
        let orgs = repo.list(&filter).await?;
        let total_count = repo.count(&filter).await?;

        let data = orgs
            .into_iter()
            .map(|o| {
                let toleration_preset = if can_update_all_resources(&current_user, Resource::Organizations) {
                    o.toleration_preset.clone()
                } else {
                    None
                };
                OrganizationResponse::from_user(UserResponse::from(o)).with_toleration_preset(toleration_preset)
            })
            .collect();

        Ok(Json(PaginatedResponse {
            data,
            total_count,
            skip,
            limit,
        }))
    } else {
        let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let mut repo = Organizations::new(&mut pool_conn);
        let memberships = repo.list_user_organizations(current_user.id).await?;

        let mut users_repo = Users::new(&mut pool_conn);
        let org_ids: Vec<UserId> = memberships.iter().map(|m| m.organization_id).collect();
        let org_map = users_repo.get_bulk(org_ids).await?;

        let data: Vec<OrganizationResponse> = memberships
            .iter()
            .filter_map(|m| {
                org_map
                    .get(&m.organization_id)
                    .map(|o| OrganizationResponse::from_user(UserResponse::from(o.clone())))
            })
            .collect();

        let total_count = data.len() as i64;

        Ok(Json(PaginatedResponse {
            data,
            total_count,
            skip: 0,
            limit: total_count,
        }))
    }
}

#[utoipa::path(
    get,
    path = "/organizations/{id}",
    tag = "organizations",
    summary = "Get organization",
    description = "Get organization details. Requires membership or platform manager access.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
    ),
    responses(
        (status = 200, description = "Organization details", body = OrganizationResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_organization<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(id): Path<UserId>,
    current_user: CurrentUser,
) -> Result<Json<OrganizationResponse>> {
    let can_all = can_read_all_resources(&current_user, Resource::Organizations);

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let mut repo = Organizations::new(&mut pool_conn);
        let role = repo.get_user_org_role(current_user.id, id).await?;
        if role.is_none() {
            return Err(Error::NotFound {
                resource: "Organization".to_string(),
                id: id.to_string(),
            });
        }
    }

    let mut users_repo = Users::new(&mut pool_conn);
    let org = users_repo.get_by_id(id).await?.ok_or_else(|| Error::NotFound {
        resource: "Organization".to_string(),
        id: id.to_string(),
    })?;

    if org.user_type != "organization" {
        return Err(Error::NotFound {
            resource: "Organization".to_string(),
            id: id.to_string(),
        });
    }

    let mut org_repo = Organizations::new(&mut pool_conn);
    let members = org_repo.list_members(id).await?;

    let pending_email_change = org_repo.find_pending_email_change_for_org(id).await?;
    let auto_join_enabled = org_repo.auto_join_enabled(id).await?;
    let members_can_create_keys = org_repo.members_can_create_keys(id).await?;
    let disabled_modalities = org_repo.disabled_modalities(id).await?;

    let can_update_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);
    let toleration_preset = if can_update_all { org.toleration_preset.clone() } else { None };

    let mut response = OrganizationResponse::from_user(UserResponse::from(org))
        .with_member_count(members.len() as i64)
        .with_auto_join_enabled(auto_join_enabled)
        .with_members_can_create_keys(members_can_create_keys)
        .with_disabled_modalities(disabled_modalities)
        .with_toleration_preset(toleration_preset);
    if let Some(pending) = pending_email_change {
        response = response.with_pending_email_change(PendingEmailChangeResponse::from(pending));
    }

    let can_view_billing = crate::auth::permissions::has_permission(&current_user, Resource::Credits, Operation::ReadAll)
        || crate::auth::permissions::has_permission(&current_user, Resource::Credits, Operation::ReadOwn);
    if can_view_billing {
        let mut credits_repo = Credits::new(&mut pool_conn);
        let balance = credits_repo.get_user_balance(id).await?.to_f64().unwrap_or(0.0);
        response.user = response.user.with_credit_balance(balance);
    }

    Ok(Json(response))
}

#[utoipa::path(
    patch,
    path = "/organizations/{id}",
    tag = "organizations",
    summary = "Update organization",
    description = "Update organization details as an owner/admin. Only owners may change zero data retention in either direction. PlatformManager platform keys and trusted SSO platform managers/admins may enable zero data retention for another organization only with an enable-only payload containing no other changes. The system credential has full administrative access. Other platform roles do not bypass membership.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
    ),
    responses(
        (status = 200, description = "Organization updated", body = OrganizationResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn update_organization<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(id): Path<UserId>,
    current_user: CurrentUser,
    Json(data): Json<OrganizationUpdate>,
) -> Result<Json<OrganizationResponse>> {
    let can_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);

    let support_zdr = current_user.can_manage_platform() && data.is_enable_zdr_only();
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let caller_org_role = if can_all || support_zdr {
        None
    } else {
        let mut org_repo = Organizations::new(&mut pool_conn);
        let role = org_repo.get_user_org_role(current_user.id, id).await?;
        if !matches!(role.as_deref(), Some("owner" | "admin")) {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: format!("Organization {id}"),
            });
        }
        role
    };

    if !can_all && !support_zdr && data.zero_data_retention.is_some() && caller_org_role.as_deref() != Some("owner") {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
            action: Operation::UpdateOwn,
            resource: format!("zero data retention for organization {id}"),
        });
    }
    if !can_all && (data.self_hosted_only.is_some() || data.toleration_preset.is_some()) {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Organizations, Operation::UpdateAll),
            action: Operation::UpdateAll,
            resource: format!("serving settings for organization {id}"),
        });
    }
    if let Some(Some(preset)) = &data.toleration_preset {
        super::validate_toleration_preset(&state, preset)?;
    }

    if !can_all && data.auto_join_enabled.is_some() && caller_org_role.as_deref() != Some("owner") {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
            action: Operation::UpdateOwn,
            resource: format!("domain auto-join for organization {id}"),
        });
    }

    if !can_all && data.disabled_modalities.is_some() && caller_org_role.as_deref() != Some("owner") {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
            action: Operation::UpdateOwn,
            resource: format!("disabled modalities for organization {id}"),
        });
    }

    let mut users_repo = Users::new(&mut pool_conn);
    let current_org = users_repo.get_by_id(id).await?.ok_or_else(|| Error::NotFound {
        resource: "Organization".to_string(),
        id: id.to_string(),
    })?;
    if current_org.user_type != "organization" {
        return Err(Error::NotFound {
            resource: "Organization".to_string(),
            id: id.to_string(),
        });
    }

    let mut pending_email_info: Option<PendingEmailChangeResponse> = None;
    if let Some(ref requested) = data.email {
        let normalized = validate_contact_email(requested)?;
        if normalized != current_org.email.to_lowercase() {
            verify_email_deliverable(&normalized).await?;

            let new_email_token = crate::auth::password::generate_reset_token();
            let old_email_token = crate::auth::password::generate_reset_token();
            let new_email_token_hash = hash_invite_token(&new_email_token);
            let old_email_token_hash = hash_invite_token(&old_email_token);
            let expires_at = chrono::Utc::now() + Duration::hours(EMAIL_CHANGE_TOKEN_TTL_HOURS);

            let mut org_repo = Organizations::new(&mut pool_conn);
            if let Some(prior) = org_repo.find_pending_email_change_for_org(id).await? {
                tracing::warn!(
                    org_id = %id,
                    superseded_requested_by = %prior.requested_by,
                    superseded_new_email = %prior.new_email,
                    superseded_created_at = %prior.created_at,
                    new_requested_by = %current_user.id,
                    new_email = %normalized,
                    kind = "org_email_change_superseded",
                    "PATCH superseded a prior pending org email change",
                );
            }
            let pending = org_repo
                .upsert_pending_email_change(
                    id,
                    &normalized,
                    current_user.id,
                    &new_email_token_hash,
                    &old_email_token_hash,
                    expires_at,
                )
                .await?;

            let config = state.current_config();
            let org_name = current_org.display_name.clone().unwrap_or_else(|| current_org.username.clone());
            let make_link = |tok: &str| {
                format!(
                    "{}/admin/api/v1/organizations/email-change/{}/confirm",
                    config.dashboard_url.trim_end_matches('/'),
                    tok,
                )
            };
            let new_email_link = make_link(&new_email_token);
            let old_email_link = make_link(&old_email_token);

            match EmailService::new(&config) {
                Ok(email_service) => {
                    if let Err(error) = email_service
                        .send_org_email_change_verify_new(&normalized, &org_name, &new_email_link)
                        .await
                    {
                        tracing::warn!(
                            org_id = %id,
                            new_email = %normalized,
                            error = %error,
                            kind = "org_email_change_verify_new_failed",
                            "Failed to send org email-change verify-new email",
                        );
                    }
                    if let Err(error) = email_service
                        .send_org_email_change_verify_old(
                            &current_org.email,
                            &org_name,
                            &normalized,
                            &old_email_link,
                            Some(&config.support_email),
                        )
                        .await
                    {
                        tracing::warn!(
                            org_id = %id,
                            old_email = %current_org.email,
                            new_email = %normalized,
                            error = %error,
                            kind = "org_email_change_verify_old_failed",
                            "Failed to send org email-change verify-old email — current owner cannot authorize",
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        org_id = %id,
                        error = %error,
                        kind = "org_email_change_email_service_unavailable",
                        "Email service unavailable for org email-change verification",
                    );
                }
            }

            pending_email_info = Some(PendingEmailChangeResponse::from(pending));
        }
    }

    let db_request = OrganizationUpdateDBRequest {
        display_name: data.display_name,
        avatar_url: None,
        email: None,
        batch_notifications_enabled: data.batch_notifications_enabled,
        low_balance_threshold: data.low_balance_threshold,
        zero_data_retention: data.zero_data_retention,
        self_hosted_only: data.self_hosted_only,
        toleration_preset: data.toleration_preset,
    };
    debug_assert!(
        db_request.email.is_none(),
        "user-facing PATCH must never write email directly — verification flow gates this field",
    );

    let mut repo = Organizations::new(&mut pool_conn);
    let org = repo.update(id, &db_request).await?;

    if let Some(enabled) = data.auto_join_enabled {
        repo.set_auto_join_enabled(id, enabled).await?;
    }
    let auto_join_enabled = repo.auto_join_enabled(id).await?;
    if let Some(enabled) = data.members_can_create_keys {
        repo.set_members_can_create_keys(id, enabled).await?;
    }
    let members_can_create_keys = repo.members_can_create_keys(id).await?;
    if let Some(disabled) = data.disabled_modalities.as_deref() {
        repo.set_disabled_modalities(id, disabled).await?;
    }
    let disabled_modalities = repo.disabled_modalities(id).await?;

    let pending_email_change = match pending_email_info {
        Some(info) => Some(info),
        None => repo
            .find_pending_email_change_for_org(id)
            .await?
            .map(PendingEmailChangeResponse::from),
    };

    if let Some(enabled) = db_request.zero_data_retention {
        tracing::info!(actor_id = %current_user.id, target_user_id = %id,
            zero_data_retention = enabled, "zero data retention updated");
    }
    let toleration_preset = if can_all { org.toleration_preset.clone() } else { None };

    let mut response = OrganizationResponse::from_user(UserResponse::from(org))
        .with_auto_join_enabled(auto_join_enabled)
        .with_members_can_create_keys(members_can_create_keys)
        .with_disabled_modalities(disabled_modalities)
        .with_toleration_preset(toleration_preset);
    if let Some(info) = pending_email_change {
        response = response.with_pending_email_change(info);
    }

    Ok(Json(response))
}

#[utoipa::path(
    delete,
    path = "/organizations/{id}",
    tag = "organizations",
    summary = "Delete organization",
    description = "Soft-delete an organization. Platform managers only.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
    ),
    responses(
        (status = 204, description = "Organization deleted"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn delete_organization<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(id): Path<UserId>,
    current_user: CurrentUser,
) -> Result<StatusCode> {
    if !crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::DeleteAll) {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Organizations, Operation::DeleteAll),
            action: Operation::DeleteAll,
            resource: format!("Organization {id}"),
        });
    }

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Organizations::new(&mut pool_conn);

    let deleted = repo.delete(id).await?;
    if !deleted {
        return Err(Error::NotFound {
            resource: "Organization".to_string(),
            id: id.to_string(),
        });
    }

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/organizations/{id}/members",
    tag = "organizations",
    summary = "List organization members",
    description = "List all members of an organization. Requires membership or platform manager access.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
    ),
    responses(
        (status = 200, description = "List of members", body = Vec<OrganizationMemberResponse>),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_members<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(id): Path<UserId>,
    current_user: CurrentUser,
) -> Result<Json<Vec<OrganizationMemberResponse>>> {
    let can_all = can_read_all_resources(&current_user, Resource::Organizations);

    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let mut repo = Organizations::new(&mut pool_conn);
        let role = repo.get_user_org_role(current_user.id, id).await?;
        if role.is_none() {
            return Err(Error::NotFound {
                resource: "Organization".to_string(),
                id: id.to_string(),
            });
        }
    }

    let mut repo = Organizations::new(&mut pool_conn);
    let memberships = repo.list_members(id).await?;
    let manage_keys_ids = repo.list_manage_keys_membership_ids(id).await?;
    let effective_manage_keys = |m: &crate::db::models::organizations::OrganizationMemberDBResponse| {
        matches!(m.role.as_str(), "owner" | "admin") || manage_keys_ids.contains(&m.id)
    };

    let user_ids: Vec<UserId> = memberships.iter().filter_map(|m| m.user_id).collect();
    let mut users_repo = Users::new(&mut pool_conn);
    let user_map = users_repo.get_bulk(user_ids).await?;

    let members: Vec<OrganizationMemberResponse> = memberships
        .iter()
        .filter_map(|m| {
            if let Some(uid) = m.user_id {
                user_map.get(&uid).map(|u| OrganizationMemberResponse {
                    id: m.id,
                    user: Some(UserResponse::from(u.clone())),
                    role: m.role.clone(),
                    status: m.status.clone(),
                    created_at: m.created_at,
                    invite_email: m.invite_email.clone(),
                    can_manage_keys: effective_manage_keys(m),
                })
            } else {
                Some(OrganizationMemberResponse {
                    id: m.id,
                    user: None,
                    role: m.role.clone(),
                    status: m.status.clone(),
                    created_at: m.created_at,
                    invite_email: m.invite_email.clone(),
                    can_manage_keys: effective_manage_keys(m),
                })
            }
        })
        .collect();

    Ok(Json(members))
}

#[utoipa::path(
    post,
    path = "/organizations/{id}/invites/{invite_id}/resend",
    tag = "organizations",
    summary = "Resend invite",
    description = "Issue a fresh token for a pending invite and email it again. The previous link stops working. Requires owner/admin role or platform manager access.",
    params(
        ("id" = String, Path, description = "Organization ID"),
        ("invite_id" = String, Path, description = "Invite (membership row) ID"),
    ),
    responses(
        (status = 204, description = "Invite re-sent"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Pending invite not found"),
    ),
    security(("BearerAuth" = []), ("CookieAuth" = []), ("X-Doubleword-User" = [])),
)]
#[tracing::instrument(skip_all)]
pub async fn resend_invite<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, invite_id)): Path<(UserId, uuid::Uuid)>,
    current_user: CurrentUser,
) -> Result<StatusCode> {
    let can_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let can_org = can_manage_org_resource(&current_user, id, &mut pool_conn).await?;
        if !can_org {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: format!("Organization {id} invites"),
            });
        }
    }

    let token = crate::auth::password::generate_reset_token();
    let token_hash = hash_invite_token(&token);
    let expires_at = chrono::Utc::now() + Duration::days(7);

    let mut org_repo = Organizations::new(&mut pool_conn);
    let Some((email, role)) = org_repo.refresh_invite_token(id, invite_id, &token_hash, expires_at).await? else {
        return Err(Error::NotFound {
            resource: "Pending invite".to_string(),
            id: format!("{invite_id} in organization {id}"),
        });
    };

    let mut users_repo = Users::new(&mut pool_conn);
    let org_user = users_repo.get_by_id(id).await?;
    let org_name = org_user
        .as_ref()
        .and_then(|u| u.display_name.clone())
        .unwrap_or_else(|| org_user.as_ref().map(|u| u.username.clone()).unwrap_or_default());

    let sender = users_repo.get_by_id(current_user.id).await?;
    let sender_name = sender
        .as_ref()
        .and_then(|u| u.display_name.clone())
        .unwrap_or_else(|| sender.as_ref().map(|u| u.username.clone()).unwrap_or_default());

    let config = state.current_config();
    let invite_link = format!("{}/org-invite?token={}", config.dashboard_url.trim_end_matches('/'), token);
    let email_service = EmailService::new(&config)?;
    if let Err(e) = email_service
        .send_org_invite_email(&email, &org_name, &sender_name, &role, &invite_link)
        .await
    {
        tracing::warn!("Failed to re-send invite email to {email}: {e}");
    }

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/organizations/{id}/join-requests",
    tag = "organizations",
    summary = "List join requests",
    description = "List users who have asked to join this organization and are awaiting a decision. Requires owner/admin role or platform manager access.",
    params(("id" = String, Path, description = "Organization ID")),
    responses(
        (status = 200, description = "Outstanding join requests", body = [OrganizationMemberResponse]),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Organization not found"),
    ),
    security(("BearerAuth" = []), ("CookieAuth" = []), ("X-Doubleword-User" = [])),
)]
#[tracing::instrument(skip_all)]
pub async fn list_join_requests<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(id): Path<UserId>,
    current_user: CurrentUser,
) -> Result<Json<Vec<OrganizationMemberResponse>>> {
    let can_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let can_org = can_manage_org_resource(&current_user, id, &mut pool_conn).await?;
        if !can_org {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: format!("Organization {id} join requests"),
            });
        }
    }

    let mut repo = Organizations::new(&mut pool_conn);
    let requests = repo.list_join_requests(id).await?;

    let user_ids: Vec<UserId> = requests.iter().filter_map(|m| m.user_id).collect();
    let mut users_repo = Users::new(&mut pool_conn);
    let user_map = users_repo.get_bulk(user_ids).await?;

    let responses: Vec<OrganizationMemberResponse> = requests
        .iter()
        .filter_map(|m| {
            let uid = m.user_id?;
            user_map.get(&uid).map(|u| OrganizationMemberResponse {
                id: m.id,
                user: Some(UserResponse::from(u.clone())),
                role: m.role.clone(),
                status: m.status.clone(),
                created_at: m.created_at,
                invite_email: m.invite_email.clone(),
                can_manage_keys: false,
            })
        })
        .collect();

    Ok(Json(responses))
}

#[utoipa::path(
    post,
    path = "/organizations/{id}/join-requests/{request_id}/approve",
    tag = "organizations",
    summary = "Approve join request",
    description = "Approve a pending join request, making the requester an active member. Requires owner/admin role or platform manager access.",
    params(
        ("id" = String, Path, description = "Organization ID"),
        ("request_id" = String, Path, description = "Join request ID"),
    ),
    request_body(content = ApproveJoinRequestRequest, description = "Optional role to grant; defaults to 'member'"),
    responses(
        (status = 204, description = "Request approved"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Join request not found"),
    ),
    security(("BearerAuth" = []), ("CookieAuth" = []), ("X-Doubleword-User" = [])),
)]
#[tracing::instrument(skip_all)]
pub async fn approve_join_request<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, request_id)): Path<(UserId, uuid::Uuid)>,
    current_user: CurrentUser,
    body: Option<Json<ApproveJoinRequestRequest>>,
) -> Result<StatusCode> {
    let can_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let can_org = can_manage_org_resource(&current_user, id, &mut pool_conn).await?;
        if !can_org {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: format!("Organization {id} join requests"),
            });
        }
    }

    let role = body.and_then(|b| b.0.role).unwrap_or_else(|| "member".to_string());
    validate_role(&role)?;
    check_role_assignment_privilege(&current_user, id, &role, can_all, &mut pool_conn).await?;

    let mut repo = Organizations::new(&mut pool_conn);
    let Some(approved_user_id) = repo.approve_join_request(id, request_id, &role).await? else {
        return Err(Error::NotFound {
            resource: "Join request".to_string(),
            id: format!("{request_id} in organization {id}"),
        });
    };
    repo.grant_default_member_roles(request_id).await?;

    let mut users_repo = Users::new(&mut pool_conn);
    let mut load = async |who: &str, user_id: UserId| match users_repo.get_by_id(user_id).await {
        Ok(found) => found,
        Err(e) => {
            tracing::warn!(org_id = %id, "Failed to load the {who} for a join-approval notification: {e}");
            None
        }
    };
    let org_user = load("organization", id).await;
    let approved_user = load("approved user", approved_user_id).await;

    match (org_user, approved_user) {
        (Some(org_user), Some(approved_user)) => {
            let config = state.current_config();
            let dashboard_link = config.dashboard_url.trim_end_matches('/').to_string();
            let org_name = org_email_name(org_user.display_name.as_deref(), &org_user.username);

            match EmailService::new(&config) {
                Ok(email_service) => {
                    if let Err(e) = email_service
                        .send_org_join_approved_email(&approved_user.email, &org_name, &role, &dashboard_link)
                        .await
                    {
                        tracing::warn!(org_id = %id, "Failed to send a join-approval notification: {e}");
                    }
                }
                Err(e) => {
                    tracing::warn!(org_id = %id, "Email service unavailable, join-approval notification not sent: {e}");
                }
            }
        }
        _ => tracing::warn!(org_id = %id, "Approved a join request but could not load the parties to notify"),
    }

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/organizations/{id}/join-requests/{request_id}/decline",
    tag = "organizations",
    summary = "Decline join request",
    description = "Decline a pending join request. The requester may ask again later. Requires owner/admin role or platform manager access.",
    params(
        ("id" = String, Path, description = "Organization ID"),
        ("request_id" = String, Path, description = "Join request ID"),
    ),
    responses(
        (status = 204, description = "Request declined"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Join request not found"),
    ),
    security(("BearerAuth" = []), ("CookieAuth" = []), ("X-Doubleword-User" = [])),
)]
#[tracing::instrument(skip_all)]
pub async fn decline_join_request<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, request_id)): Path<(UserId, uuid::Uuid)>,
    current_user: CurrentUser,
) -> Result<StatusCode> {
    let can_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let can_org = can_manage_org_resource(&current_user, id, &mut pool_conn).await?;
        if !can_org {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: format!("Organization {id} join requests"),
            });
        }
    }

    let mut repo = Organizations::new(&mut pool_conn);
    let declined = repo.decline_join_request(id, request_id).await?;
    if !declined {
        return Err(Error::NotFound {
            resource: "Join request".to_string(),
            id: format!("{request_id} in organization {id}"),
        });
    }

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/organizations/{id}/members",
    tag = "organizations",
    summary = "Add organization member",
    description = "Add a user as a member of an organization. Requires owner/admin role or platform manager access.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
    ),
    responses(
        (status = 201, description = "Member added", body = OrganizationMemberResponse),
        (status = 400, description = "Bad request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn add_member<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(id): Path<UserId>,
    current_user: CurrentUser,
    Json(data): Json<AddMemberRequest>,
) -> Result<(StatusCode, Json<OrganizationMemberResponse>)> {
    let can_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let can_org = can_manage_org_resource(&current_user, id, &mut pool_conn).await?;
        if !can_org {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: format!("Organization {id} members"),
            });
        }
    }

    let role = data.role.as_deref().unwrap_or("member");
    validate_role(role)?;

    check_role_assignment_privilege(&current_user, id, role, can_all, &mut pool_conn).await?;

    let mut repo = Organizations::new(&mut pool_conn);

    let org_count = repo.count_user_organizations(data.user_id).await?;
    if org_count >= MAX_ORGS_PER_USER {
        return Err(Error::BadRequest {
            message: format!("Cannot add member: user is already a member of {MAX_ORGS_PER_USER} organizations (maximum)"),
        });
    }

    let membership = repo.add_member(id, data.user_id, role).await?;

    let is_manager_role = matches!(role, "owner" | "admin");
    let granted = match data.can_manage_keys {
        _ if is_manager_role => false,
        Some(true) => {
            repo.set_membership_manage_keys(membership.id, true).await?;
            true
        }
        Some(false) => false,
        None => repo.grant_default_member_roles(membership.id).await?,
    };

    let mut users_repo = Users::new(&mut pool_conn);
    let user = users_repo.get_by_id(data.user_id).await?.ok_or_else(|| Error::NotFound {
        resource: "User".to_string(),
        id: data.user_id.to_string(),
    })?;

    Ok((
        StatusCode::CREATED,
        Json(OrganizationMemberResponse {
            id: membership.id,
            user: Some(UserResponse::from(user)),
            role: membership.role,
            status: membership.status,
            created_at: membership.created_at,
            invite_email: None,
            can_manage_keys: is_manager_role || granted,
        }),
    ))
}

#[utoipa::path(
    patch,
    path = "/organizations/{id}/members/{user_id}",
    tag = "organizations",
    summary = "Update member role",
    description = "Update a member's role in an organization. Requires owner/admin role or platform manager access.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
        ("user_id" = String, Path, description = "Member user ID (UUID)"),
    ),
    responses(
        (status = 200, description = "Role updated", body = OrganizationMemberResponse),
        (status = 400, description = "Bad request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn update_member_role<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, user_id)): Path<(UserId, UserId)>,
    current_user: CurrentUser,
    Json(data): Json<UpdateMemberRoleRequest>,
) -> Result<Json<OrganizationMemberResponse>> {
    let can_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let can_org = can_manage_org_resource(&current_user, id, &mut tx).await?;
        if !can_org {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: format!("Organization {id} members"),
            });
        }
    }

    validate_role(&data.role)?;

    check_role_assignment_privilege(&current_user, id, &data.role, can_all, &mut tx).await?;

    let mut repo = Organizations::new(&mut tx);
    if data.role != "owner" {
        let current_role = repo.get_user_org_role(user_id, id).await?;
        if current_role.as_deref() == Some("owner") {
            let members = repo.list_members(id).await?;
            let owner_count = members.iter().filter(|m| m.role == "owner").count();
            if owner_count <= 1 {
                return Err(Error::BadRequest {
                    message: "Cannot demote the last owner. Assign another owner first.".to_string(),
                });
            }
        }
    }

    let membership = repo.update_member_role(id, user_id, &data.role).await?;

    let is_manager_role = matches!(membership.role.as_str(), "owner" | "admin");
    let effective_manage_keys = if let Some(granted) = data.can_manage_keys {
        repo.set_membership_manage_keys(membership.id, granted).await?;
        is_manager_role || granted
    } else {
        is_manager_role || repo.membership_has_manage_keys(membership.id).await?
    };

    let mut users_repo = Users::new(&mut tx);
    let user = users_repo.get_by_id(user_id).await?.ok_or_else(|| Error::NotFound {
        resource: "User".to_string(),
        id: user_id.to_string(),
    })?;

    tx.commit().await.map_err(|e| Error::Database(e.into()))?;
    Ok(Json(OrganizationMemberResponse {
        id: membership.id,
        user: Some(UserResponse::from(user)),
        role: membership.role,
        status: membership.status,
        created_at: membership.created_at,
        invite_email: None,
        can_manage_keys: effective_manage_keys,
    }))
}

#[utoipa::path(
    delete,
    path = "/organizations/{id}/members/{user_id}",
    tag = "organizations",
    summary = "Remove organization member",
    description = "Remove a member from an organization. Requires owner/admin role or platform manager access.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
        ("user_id" = String, Path, description = "Member user ID (UUID)"),
    ),
    responses(
        (status = 204, description = "Member removed"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn remove_member<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, user_id)): Path<(UserId, UserId)>,
    current_user: CurrentUser,
) -> Result<StatusCode> {
    let can_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let can_org = can_manage_org_resource(&current_user, id, &mut tx).await?;
        if !can_org {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: format!("Organization {id} members"),
            });
        }
    }

    let mut repo = Organizations::new(&mut tx);

    let target_role = repo.get_user_org_role(user_id, id).await?;
    if let Some(ref role) = target_role
        && role == "owner"
    {
        let members = repo.list_members(id).await?;
        let owner_count = members.iter().filter(|m| m.role == "owner").count();
        if owner_count <= 1 {
            return Err(Error::BadRequest {
                message: "Cannot remove the last owner of an organization. Transfer ownership first.".to_string(),
            });
        }
    }

    let removed = repo.remove_member(id, user_id).await?;
    if !removed {
        return Err(Error::NotFound {
            resource: "Organization membership".to_string(),
            id: format!("{user_id} in organization {id}"),
        });
    }

    let mut api_keys_repo = ApiKeys::new(&mut tx);
    let keys_deleted = api_keys_repo.soft_delete_member_org_keys(id, user_id).await?;
    if keys_deleted > 0 {
        tracing::info!("Soft-deleted {keys_deleted} API key(s) for removed member {user_id} in org {id}");
    }

    tx.commit().await.map_err(|e| Error::Database(e.into()))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/organizations/{id}/leave",
    tag = "organizations",
    summary = "Leave organization",
    description = "Leave an organization voluntarily. Cannot leave if you are the last owner.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
    ),
    responses(
        (status = 204, description = "Left organization"),
        (status = 400, description = "Bad request - last owner cannot leave"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found - not a member"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn leave_organization<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(id): Path<UserId>,
    current_user: CurrentUser,
) -> Result<StatusCode> {
    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;

    let mut repo = Organizations::new(&mut tx);

    let role = repo.get_user_org_role(current_user.id, id).await?;
    if role.is_none() {
        return Err(Error::NotFound {
            resource: "Organization membership".to_string(),
            id: format!("{} in organization {id}", current_user.id),
        });
    }

    if role.as_deref() == Some("owner") {
        let members = repo.list_members(id).await?;
        let owner_count = members.iter().filter(|m| m.role == "owner").count();
        if owner_count <= 1 {
            return Err(Error::BadRequest {
                message: "Cannot leave as the last owner. Transfer ownership first.".to_string(),
            });
        }
    }

    repo.remove_member(id, current_user.id).await?;

    let mut api_keys_repo = ApiKeys::new(&mut tx);
    let keys_deleted = api_keys_repo.soft_delete_member_org_keys(id, current_user.id).await?;
    if keys_deleted > 0 {
        tracing::info!(
            "Soft-deleted {keys_deleted} API key(s) for user {} leaving org {id}",
            current_user.id
        );
    }

    tx.commit().await.map_err(|e| Error::Database(e.into()))?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/users/{user_id}/organizations",
    tag = "organizations",
    summary = "List user's organizations",
    description = "List organizations that a user belongs to.",
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
    ),
    responses(
        (status = 200, description = "List of organizations", body = Vec<crate::api::models::organizations::OrganizationSummary>),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_user_organizations<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(user_id): Path<UserIdOrCurrent>,
    current_user: CurrentUser,
) -> Result<Json<Vec<crate::api::models::organizations::OrganizationSummary>>> {
    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_all = can_read_all_resources(&current_user, Resource::Users);
    let can_own = can_read_own_resource(&current_user, Resource::Users, target_user_id);
    if !can_all && !can_own {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Users, Operation::ReadOwn),
            action: Operation::ReadOwn,
            resource: format!("Organizations for user {target_user_id}"),
        });
    }

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Organizations::new(&mut pool_conn);
    let memberships = repo.list_user_organizations(target_user_id).await?;
    let manage_keys_orgs = repo.user_manage_keys_org_ids(target_user_id).await?;

    let org_ids: Vec<UserId> = memberships.iter().map(|m| m.organization_id).collect();
    let mut users_repo = Users::new(&mut pool_conn);
    let org_map = users_repo.get_bulk(org_ids).await?;

    let summaries: Vec<crate::api::models::organizations::OrganizationSummary> = memberships
        .iter()
        .filter_map(|m| {
            org_map
                .get(&m.organization_id)
                .map(|o| crate::api::models::organizations::OrganizationSummary {
                    id: o.id,
                    name: o.username.clone(),
                    can_manage_keys: matches!(m.role.as_str(), "owner" | "admin") || manage_keys_orgs.contains(&o.id),
                    role: m.role.clone(),
                    zero_data_retention: o.zero_data_retention,
                    verified: o.verified,
                })
        })
        .collect();

    Ok(Json(summaries))
}

#[utoipa::path(
    get,
    path = "/users/{user_id}/join-requests",
    tag = "organizations",
    summary = "List user's pending join requests",
    description = "List organizations the user has asked to join and is awaiting a decision on. Readable by that user, or by a platform manager with read-all on users.",
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
    ),
    responses(
        (status = 200, description = "Pending join requests", body = Vec<crate::api::models::organizations::PendingJoinRequestResponse>),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn list_user_join_requests<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(user_id): Path<UserIdOrCurrent>,
    current_user: CurrentUser,
) -> Result<Json<Vec<crate::api::models::organizations::PendingJoinRequestResponse>>> {
    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_all = can_read_all_resources(&current_user, Resource::Users);
    let can_own = can_read_own_resource(&current_user, Resource::Users, target_user_id);
    if !can_all && !can_own {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Users, Operation::ReadOwn),
            action: Operation::ReadOwn,
            resource: format!("Join requests for user {target_user_id}"),
        });
    }

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut repo = Organizations::new(&mut pool_conn);
    let requests = repo.list_user_join_requests(target_user_id).await?;

    let org_ids: Vec<UserId> = requests.iter().map(|r| r.organization_id).collect();
    let mut users_repo = Users::new(&mut pool_conn);
    let org_map = users_repo.get_bulk(org_ids).await?;

    let responses: Vec<crate::api::models::organizations::PendingJoinRequestResponse> = requests
        .iter()
        .filter_map(|r| {
            org_map
                .get(&r.organization_id)
                .map(|o| crate::api::models::organizations::PendingJoinRequestResponse {
                    id: r.id,
                    organization_id: o.id,
                    organization_name: o.display_name.clone().unwrap_or_else(|| o.username.clone()),
                    requested_at: r.created_at,
                })
        })
        .collect();

    Ok(Json(responses))
}

#[utoipa::path(
    get,
    path = "/users/{user_id}/onboarding-context",
    tag = "organizations",
    summary = "Onboarding routing context",
    description = "Pending invitation, matching domain workspace, and outstanding join request for a user — the inputs to the onboarding routing decision.",
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
    ),
    responses(
        (status = 200, description = "Onboarding context", body = crate::api::models::organizations::OnboardingContextResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_onboarding_context<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(user_id): Path<UserIdOrCurrent>,
    current_user: CurrentUser,
) -> Result<Json<crate::api::models::organizations::OnboardingContextResponse>> {
    use crate::api::models::organizations::{
        DomainMatchResponse, OnboardingContextResponse, PendingInvitationResponse, PendingJoinRequestResponse,
    };

    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_all = can_read_all_resources(&current_user, Resource::Users);
    let can_own = can_read_own_resource(&current_user, Resource::Users, target_user_id);
    if !can_all && !can_own {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Users, Operation::ReadOwn),
            action: Operation::ReadOwn,
            resource: format!("Onboarding context for user {target_user_id}"),
        });
    }

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let mut users_repo = Users::new(&mut pool_conn);
    let target = users_repo.get_by_id(target_user_id).await?.ok_or_else(|| Error::NotFound {
        resource: "User".to_string(),
        id: target_user_id.to_string(),
    })?;
    let target_email = target.email.clone();

    let mut org_repo = Organizations::new(&mut pool_conn);

    let invite = org_repo.find_pending_invite_for_email(&target_email).await?;
    let invitation = match invite {
        Some(invite) => {
            let mut users_repo = Users::new(&mut pool_conn);
            let org = users_repo.get_by_id(invite.organization_id).await?;
            match org {
                Some(org) => {
                    let inviter_name = match invite.invited_by {
                        Some(inviter_id) => users_repo
                            .get_by_id(inviter_id)
                            .await?
                            .map(|u| u.display_name.unwrap_or(u.username)),
                        None => None,
                    };
                    Some(PendingInvitationResponse {
                        id: invite.id,
                        organization_id: invite.organization_id,
                        organization_name: org.display_name.unwrap_or(org.username),
                        role: invite.role.clone(),
                        inviter_name,
                        expires_at: invite.expires_at,
                    })
                }
                None => None,
            }
        }
        None => None,
    };

    let mut org_repo = Organizations::new(&mut pool_conn);
    let requests = org_repo.list_user_join_requests(target_user_id).await?;
    let join_request = match requests.first() {
        Some(request) => {
            let mut users_repo = Users::new(&mut pool_conn);
            users_repo
                .get_by_id(request.organization_id)
                .await?
                .map(|org| PendingJoinRequestResponse {
                    id: request.id,
                    organization_id: org.id,
                    organization_name: org.display_name.unwrap_or(org.username),
                    requested_at: request.created_at,
                })
        }
        None => None,
    };

    let domain = crate::auth::utils::email_domain(&target_email).filter(|d| !state.current_config().auth.is_personal_email_domain(d));
    let mut org_repo = Organizations::new(&mut pool_conn);
    let domain_match = match domain {
        Some(domain) => match org_repo.find_by_domain(&domain).await? {
            Some(org) => {
                let existing_role = org_repo.get_user_org_role(target_user_id, org.id).await?;
                if existing_role.is_some() {
                    None
                } else {
                    let auto_join_enabled = org_repo.auto_join_enabled(org.id).await?;
                    Some(DomainMatchResponse {
                        organization_id: org.id,
                        domain,
                        auto_join_enabled,
                    })
                }
            }
            None => None,
        },
        None => None,
    };

    Ok(Json(OnboardingContextResponse {
        invitation,
        domain_match,
        join_request,
    }))
}

#[utoipa::path(
    post,
    path = "/users/{user_id}/join-requests",
    tag = "organizations",
    summary = "Request access to the domain-matched workspace",
    description = "Ask to join the organization that has claimed the user's email domain. Becomes an immediate membership if that organization has auto-join enabled.",
    params(
        ("user_id" = String, Path, description = "User ID (UUID) or 'current' for current user"),
    ),
    responses(
        (status = 200, description = "Request filed or membership granted", body = crate::api::models::organizations::DomainJoinResponse),
        (status = 400, description = "No workspace claims this user's email domain"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn create_user_join_request<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(user_id): Path<UserIdOrCurrent>,
    current_user: CurrentUser,
) -> Result<Json<crate::api::models::organizations::DomainJoinResponse>> {
    use crate::api::models::organizations::{DomainJoinOutcome, DomainJoinResponse, PendingJoinRequestResponse};

    let target_user_id = match user_id {
        UserIdOrCurrent::Current(_) => current_user.id,
        UserIdOrCurrent::Id(uuid) => uuid,
    };

    let can_all = can_update_all_resources(&current_user, Resource::Users);
    let can_own = can_update_own_resource(&current_user, Resource::Users, target_user_id);
    if !can_all && !can_own {
        return Err(Error::InsufficientPermissions {
            required: Permission::Allow(Resource::Users, Operation::UpdateOwn),
            action: Operation::UpdateOwn,
            resource: format!("Join request for user {target_user_id}"),
        });
    }

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    let mut users_repo = Users::new(&mut pool_conn);
    let target = users_repo.get_by_id(target_user_id).await?.ok_or_else(|| Error::NotFound {
        resource: "User".to_string(),
        id: target_user_id.to_string(),
    })?;

    let domain = crate::auth::utils::email_domain(&target.email)
        .filter(|d| !state.current_config().auth.is_personal_email_domain(d))
        .ok_or_else(|| Error::BadRequest {
            message: "No workspace matches this account's email domain".to_string(),
        })?;

    let mut org_repo = Organizations::new(&mut pool_conn);
    let org = org_repo.find_by_domain(&domain).await?.ok_or_else(|| Error::BadRequest {
        message: "No workspace matches this account's email domain".to_string(),
    })?;

    if org_repo.get_user_org_role(target_user_id, org.id).await?.is_some() {
        return Ok(Json(DomainJoinResponse {
            outcome: DomainJoinOutcome::AlreadyMember,
            organization_id: org.id,
            join_request: None,
        }));
    }

    if org_repo.auto_join_enabled(org.id).await? {
        let org_count = org_repo.count_user_organizations(target_user_id).await?;
        if org_count >= MAX_ORGS_PER_USER {
            return Err(Error::BadRequest {
                message: format!("Cannot join: you are already a member of {MAX_ORGS_PER_USER} organizations (maximum)"),
            });
        }
        let membership = org_repo.add_member(org.id, target_user_id, "member").await?;
        org_repo.grant_default_member_roles(membership.id).await?;
        return Ok(Json(DomainJoinResponse {
            outcome: DomainJoinOutcome::Joined,
            organization_id: org.id,
            join_request: None,
        }));
    }

    let row = org_repo.create_join_request(org.id, target_user_id).await?;

    let outcome = match row.status.as_str() {
        "requested" => DomainJoinOutcome::Requested,
        "pending" => DomainJoinOutcome::Invited,
        _ => DomainJoinOutcome::AlreadyMember,
    };

    if outcome == DomainJoinOutcome::Requested {
        let recipients = match org_repo.list_admin_emails(org.id).await {
            Ok(emails) => emails,
            Err(e) => {
                tracing::warn!(org_id = %org.id, "Failed to list admins to notify of a join request: {e}");
                Vec::new()
            }
        };

        if recipients.is_empty() {
            tracing::warn!(
                org_id = %org.id,
                "Join request filed against a workspace with no active owner or admin to notify"
            );
        } else {
            let config = state.current_config();
            let requests_link = format!("{}/organization", config.dashboard_url.trim_end_matches('/'));
            let org_name = org_email_name(org.display_name.as_deref(), &org.username);
            let requester_name = person_email_name(target.display_name.as_deref(), &target.email);

            match EmailService::new(&config) {
                Ok(email_service) => {
                    for recipient in &recipients {
                        if let Err(e) = email_service
                            .send_org_join_request_email(recipient, &org_name, &requester_name, &target.email, &requests_link)
                            .await
                        {
                            tracing::warn!(org_id = %org.id, "Failed to send a join-request notification: {e}");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(org_id = %org.id, "Email service unavailable, join-request notification not sent: {e}");
                }
            }
        }
    }

    Ok(Json(DomainJoinResponse {
        outcome,
        organization_id: org.id,
        join_request: match outcome {
            DomainJoinOutcome::Requested => Some(PendingJoinRequestResponse {
                id: row.id,
                organization_id: org.id,
                organization_name: org.display_name.unwrap_or(org.username),
                requested_at: row.created_at,
            }),
            _ => None,
        },
    }))
}

#[utoipa::path(
    post,
    path = "/session/organization",
    tag = "organizations",
    summary = "Set active organization",
    description = "Validate organization membership and set a cookie for the active organization context.",
    responses(
        (status = 200, description = "Organization context validated", body = SetActiveOrganizationResponse),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn set_active_organization<P: PoolProvider>(
    State(state): State<AppState<P>>,
    current_user: CurrentUser,
    Json(data): Json<SetActiveOrganizationRequest>,
) -> Result<(HeaderMap, Json<SetActiveOrganizationResponse>)> {
    if let Some(org_id) = data.organization_id {
        let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
        let mut repo = Organizations::new(&mut pool_conn);
        let role = repo.get_user_org_role(current_user.id, org_id).await?;
        if role.is_none() {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::ReadOwn),
                action: Operation::ReadOwn,
                resource: format!("Organization {org_id}"),
            });
        }
    }

    let config = state.current_config();
    let session_config = &config.auth.native.session;
    let secure = if session_config.cookie_secure { "; Secure" } else { "" };
    let domain = session_config
        .cookie_domain
        .as_ref()
        .map(|d| format!("; Domain={d}"))
        .unwrap_or_default();
    let cookie = if let Some(org_id) = data.organization_id {
        format!(
            "dw_active_org={}; Path=/; HttpOnly{}{}; SameSite={}; Max-Age={}",
            org_id,
            secure,
            domain,
            session_config.cookie_same_site,
            30 * 24 * 60 * 60
        )
    } else {
        format!(
            "dw_active_org=; Path=/; HttpOnly{}{}; SameSite={}; Max-Age=0",
            secure, domain, session_config.cookie_same_site
        )
    };

    let mut headers = HeaderMap::new();
    headers.insert(header::SET_COOKIE, cookie.parse().unwrap());

    Ok((
        headers,
        Json(SetActiveOrganizationResponse {
            active_organization_id: data.organization_id,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/organizations/{id}/invites",
    tag = "organizations",
    summary = "Invite member by email",
    description = "Send an invitation email to join the organization. Requires owner/admin role or platform manager access.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
    ),
    responses(
        (status = 201, description = "Invite created", body = InviteMemberResponse),
        (status = 400, description = "Bad request"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 409, description = "Conflict - already a member or pending invite"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn invite_member<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(id): Path<UserId>,
    current_user: CurrentUser,
    Json(data): Json<InviteMemberRequest>,
) -> Result<(StatusCode, Json<InviteMemberResponse>)> {
    let can_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let can_org = can_manage_org_resource(&current_user, id, &mut pool_conn).await?;
        if !can_org {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: format!("Organization {id} members"),
            });
        }
    }

    let email = data.email.trim().to_lowercase();
    if !email.contains('@') || !email.contains('.') {
        return Err(Error::BadRequest {
            message: "Invalid email address".to_string(),
        });
    }

    let role = data.role.as_deref().unwrap_or("member");
    validate_role(role)?;

    check_role_assignment_privilege(&current_user, id, role, can_all, &mut pool_conn).await?;

    let mut users_repo = Users::new(&mut pool_conn);
    let existing_user = users_repo.get_user_by_email(&email).await?;
    let existing_user_id = existing_user.as_ref().map(|u| u.id);

    if let Some(ref user) = existing_user {
        let mut org_repo = Organizations::new(&mut pool_conn);
        let existing_role = org_repo.get_user_org_role(user.id, id).await?;
        if existing_role.is_some() {
            return Err(Error::Conflict {
                message: "User is already an active member of this organization".to_string(),
                conflicts: None,
            });
        }
    }

    let mut org_repo = Organizations::new(&mut pool_conn);
    let token = crate::auth::password::generate_reset_token();
    let token_hash = hash_invite_token(&token);
    let expires_at = chrono::Utc::now() + Duration::days(7);

    let invite = org_repo
        .create_invite(id, existing_user_id, &email, role, current_user.id, &token_hash, expires_at)
        .await?;

    if !matches!(role, "owner" | "admin") {
        match data.can_manage_keys {
            Some(true) => org_repo.set_membership_manage_keys(invite.id, true).await?,
            Some(false) => {}
            None => {
                org_repo.grant_default_member_roles(invite.id).await?;
            }
        }
    }

    let mut users_repo = Users::new(&mut pool_conn);
    let org_user = users_repo.get_by_id(id).await?;
    let org_name = org_user
        .as_ref()
        .and_then(|u| u.display_name.clone())
        .unwrap_or_else(|| org_user.as_ref().map(|u| u.username.clone()).unwrap_or_default());

    let inviter = users_repo.get_by_id(current_user.id).await?;
    let inviter_name = inviter
        .as_ref()
        .and_then(|u| u.display_name.clone())
        .unwrap_or_else(|| inviter.as_ref().map(|u| u.username.clone()).unwrap_or_default());

    let config = state.current_config();
    let invite_link = format!("{}/org-invite?token={}", config.dashboard_url.trim_end_matches('/'), token);
    let email_service = EmailService::new(&config)?;
    if let Err(e) = email_service
        .send_org_invite_email(&email, &org_name, &inviter_name, role, &invite_link)
        .await
    {
        tracing::warn!("Failed to send invite email to {email}: {e}");
    }

    Ok((
        StatusCode::CREATED,
        Json(InviteMemberResponse {
            id: invite.id,
            email,
            role: invite.role,
            status: invite.status,
            created_at: invite.created_at,
            expires_at: invite.expires_at.expect("invite must have expires_at"),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/organizations/invites/{token}",
    tag = "organizations",
    summary = "Get invite details",
    description = "Look up a pending invite by its token. Returns organization name, role, and inviter info.",
    params(
        ("token" = String, Path, description = "Invite token"),
    ),
    responses(
        (status = 200, description = "Invite details", body = InviteDetailsResponse),
        (status = 401, description = "Unauthorized"),
        (status = 400, description = "Bad request - invite has expired"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn get_invite_details<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(token): Path<String>,
    _current_user: CurrentUser,
) -> Result<Json<InviteDetailsResponse>> {
    let token_hash = hash_invite_token(&token);

    let mut pool_conn = state.db.read().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut org_repo = Organizations::new(&mut pool_conn);

    let invite = org_repo
        .find_invite_by_token_hash(&token_hash)
        .await?
        .ok_or_else(|| Error::NotFound {
            resource: "Invite".to_string(),
            id: "invalid or expired token".to_string(),
        })?;

    if let Some(expires_at) = invite.expires_at
        && expires_at < chrono::Utc::now()
    {
        return Err(Error::BadRequest {
            message: "This invite has expired".to_string(),
        });
    }

    let mut users_repo = Users::new(&mut pool_conn);
    let org_user = users_repo.get_by_id(invite.organization_id).await?;
    let org_name = org_user
        .as_ref()
        .and_then(|u| u.display_name.clone())
        .unwrap_or_else(|| org_user.as_ref().map(|u| u.username.clone()).unwrap_or_default());

    let inviter_name = if let Some(invited_by) = invite.invited_by {
        let inviter = users_repo.get_by_id(invited_by).await?;
        inviter.and_then(|u| u.display_name.or(Some(u.username)))
    } else {
        None
    };

    Ok(Json(InviteDetailsResponse {
        org_name,
        role: invite.role,
        inviter_name,
        expires_at: invite.expires_at.expect("invite must have expires_at"),
    }))
}

#[utoipa::path(
    post,
    path = "/organizations/invites/{token}/accept",
    tag = "organizations",
    summary = "Accept invite",
    description = "Accept a pending organization invite. The authenticated user's email must match the invite email.",
    params(
        ("token" = String, Path, description = "Invite token"),
    ),
    responses(
        (status = 200, description = "Invite accepted"),
        (status = 400, description = "Bad request - expired or email mismatch"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn accept_invite<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(token): Path<String>,
    current_user: CurrentUser,
) -> Result<Json<serde_json::Value>> {
    let token_hash = hash_invite_token(&token);

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut org_repo = Organizations::new(&mut pool_conn);

    let invite = org_repo
        .find_invite_by_token_hash(&token_hash)
        .await?
        .ok_or_else(|| Error::NotFound {
            resource: "Invite".to_string(),
            id: "invalid or expired token".to_string(),
        })?;

    if let Some(expires_at) = invite.expires_at
        && expires_at < chrono::Utc::now()
    {
        return Err(Error::BadRequest {
            message: "This invite has expired".to_string(),
        });
    }

    if let Some(ref invite_email) = invite.invite_email
        && current_user.email.to_lowercase() != invite_email.to_lowercase()
    {
        return Err(Error::BadRequest {
            message: "Your email address does not match this invite".to_string(),
        });
    }

    let org_count = org_repo.count_user_organizations(current_user.id).await?;
    if org_count >= MAX_ORGS_PER_USER {
        return Err(Error::BadRequest {
            message: format!("Cannot accept invite: you are already a member of {MAX_ORGS_PER_USER} organizations (maximum)"),
        });
    }

    org_repo.accept_invite(invite.id, current_user.id).await?;

    Ok(Json(serde_json::json!({ "message": "Invite accepted" })))
}

#[utoipa::path(
    post,
    path = "/organizations/invites/{token}/decline",
    tag = "organizations",
    summary = "Decline invite",
    description = "Decline a pending organization invite. The authenticated user's email must match the invite email.",
    params(
        ("token" = String, Path, description = "Invite token"),
    ),
    responses(
        (status = 200, description = "Invite declined"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn decline_invite<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(token): Path<String>,
    current_user: CurrentUser,
) -> Result<Json<serde_json::Value>> {
    let token_hash = hash_invite_token(&token);

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut org_repo = Organizations::new(&mut pool_conn);

    let invite = org_repo
        .find_invite_by_token_hash(&token_hash)
        .await?
        .ok_or_else(|| Error::NotFound {
            resource: "Invite".to_string(),
            id: "invalid or expired token".to_string(),
        })?;

    if let Some(ref invite_email) = invite.invite_email
        && current_user.email.to_lowercase() != invite_email.to_lowercase()
    {
        return Err(Error::BadRequest {
            message: "Your email address does not match this invite".to_string(),
        });
    }

    org_repo.cancel_invite(invite.organization_id, invite.id).await?;

    Ok(Json(serde_json::json!({ "message": "Invite declined" })))
}

#[utoipa::path(
    post,
    path = "/organizations/{id}/invites/{invite_id}/accept",
    tag = "organizations",
    summary = "Accept an invitation by id",
    description = "Accept a pending invitation addressed to the authenticated user, without its emailed token.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
        ("invite_id" = String, Path, description = "Invitation ID (UUID)"),
    ),
    responses(
        (status = 200, description = "Invitation accepted"),
        (status = 400, description = "Bad request - expired, or organization limit reached"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found, or not addressed to the caller"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn accept_invite_by_id<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, invite_id)): Path<(UserId, uuid::Uuid)>,
    current_user: CurrentUser,
) -> Result<Json<serde_json::Value>> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut org_repo = Organizations::new(&mut pool_conn);

    let invite = require_own_invite(&mut org_repo, id, invite_id, &current_user).await?;

    if let Some(expires_at) = invite.expires_at
        && expires_at < chrono::Utc::now()
    {
        return Err(Error::BadRequest {
            message: "This invite has expired".to_string(),
        });
    }

    let org_count = org_repo.count_user_organizations(current_user.id).await?;
    if org_count >= MAX_ORGS_PER_USER {
        return Err(Error::BadRequest {
            message: format!("Cannot accept invite: you are already a member of {MAX_ORGS_PER_USER} organizations (maximum)"),
        });
    }

    org_repo.accept_invite(invite.id, current_user.id).await?;

    Ok(Json(serde_json::json!({ "message": "Invite accepted" })))
}

#[utoipa::path(
    post,
    path = "/organizations/{id}/invites/{invite_id}/decline",
    tag = "organizations",
    summary = "Decline an invitation by id",
    description = "Decline a pending invitation addressed to the authenticated user, without its emailed token.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
        ("invite_id" = String, Path, description = "Invitation ID (UUID)"),
    ),
    responses(
        (status = 200, description = "Invitation declined"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Not found, or not addressed to the caller"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn decline_invite_by_id<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, invite_id)): Path<(UserId, uuid::Uuid)>,
    current_user: CurrentUser,
) -> Result<Json<serde_json::Value>> {
    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let mut org_repo = Organizations::new(&mut pool_conn);

    let invite = require_own_invite(&mut org_repo, id, invite_id, &current_user).await?;
    org_repo.cancel_invite(invite.organization_id, invite.id).await?;

    Ok(Json(serde_json::json!({ "message": "Invite declined" })))
}

async fn require_own_invite(
    org_repo: &mut Organizations<'_>,
    org_id: UserId,
    invite_id: uuid::Uuid,
    current_user: &CurrentUser,
) -> Result<crate::db::models::organizations::OrganizationMemberDBResponse> {
    let not_found = || Error::NotFound {
        resource: "Invite".to_string(),
        id: invite_id.to_string(),
    };

    let invite = org_repo.find_invite_by_id(org_id, invite_id).await?.ok_or_else(not_found)?;

    let addressed_to_caller = invite
        .invite_email
        .as_ref()
        .is_some_and(|email| email.to_lowercase() == current_user.email.to_lowercase());

    if !addressed_to_caller {
        return Err(not_found());
    }

    Ok(invite)
}

#[utoipa::path(
    delete,
    path = "/organizations/{id}/invites/{invite_id}",
    tag = "organizations",
    summary = "Cancel invite",
    description = "Cancel a pending invite. Requires owner/admin role or platform manager access.",
    params(
        ("id" = String, Path, description = "Organization ID (UUID)"),
        ("invite_id" = String, Path, description = "Invite row ID (UUID)"),
    ),
    responses(
        (status = 204, description = "Invite cancelled"),
        (status = 401, description = "Unauthorized"),
        (status = 403, description = "Forbidden"),
        (status = 404, description = "Not found"),
    ),
    security(
        ("BearerAuth" = []),
        ("CookieAuth" = []),
        ("X-Doubleword-User" = [])
    )
)]
#[tracing::instrument(skip_all)]
pub async fn cancel_invite<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path((id, invite_id)): Path<(UserId, UserId)>,
    current_user: CurrentUser,
) -> Result<StatusCode> {
    let can_all = crate::auth::permissions::has_permission(&current_user, Resource::Organizations, Operation::UpdateAll);

    let mut pool_conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;

    if !can_all {
        let can_org = can_manage_org_resource(&current_user, id, &mut pool_conn).await?;
        if !can_org {
            return Err(Error::InsufficientPermissions {
                required: Permission::Allow(Resource::Organizations, Operation::UpdateOwn),
                action: Operation::UpdateOwn,
                resource: format!("Organization {id} invites"),
            });
        }
    }

    let mut org_repo = Organizations::new(&mut pool_conn);
    let cancelled = org_repo.cancel_invite(id, invite_id).await?;
    if !cancelled {
        return Err(Error::NotFound {
            resource: "Pending invite".to_string(),
            id: format!("{invite_id} in organization {id}"),
        });
    }

    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/organizations/email-change/{token}/confirm",
    tag = "organizations",
    summary = "Confirm one side of an organization email change",
    description = "Mark one side (old or new) of a pending organization email change as confirmed. When both sides are confirmed, the change is applied. Returns an HTML page.",
    params(
        ("token" = String, Path, description = "Email change verification token"),
    ),
    responses(
        (status = 200, description = "Side recorded as confirmed, or change applied (HTML page)"),
        (status = 404, description = "Invalid, already-used, or expired token (HTML page)"),
    ),
)]
#[tracing::instrument(skip_all)]
pub async fn confirm_email_change<P: PoolProvider>(
    State(state): State<AppState<P>>,
    Path(token): Path<String>,
) -> Result<axum::response::Response> {
    let token_hash = hash_invite_token(&token);

    let mut tx = state.db.write().begin().await.map_err(|e| Error::Database(e.into()))?;
    let mut org_repo = Organizations::new(&mut tx);

    let (pending, just_confirmed_side) = if let Some(p) = org_repo.confirm_new_email_side(&token_hash).await? {
        (p, ConfirmedSide::New)
    } else if let Some(p) = org_repo.confirm_old_email_side(&token_hash).await? {
        (p, ConfirmedSide::Old)
    } else {
        tx.rollback().await.map_err(|e| Error::Database(e.into()))?;
        return Ok(email_change_html(
            StatusCode::NOT_FOUND,
            "This confirmation link is invalid, has already been used, or has expired.",
        ));
    };

    if pending.is_fully_confirmed() {
        let update = OrganizationUpdateDBRequest {
            display_name: None,
            avatar_url: None,
            email: Some(pending.new_email.clone()),
            batch_notifications_enabled: None,
            low_balance_threshold: None,
            zero_data_retention: None,
            self_hosted_only: None,
            toleration_preset: Default::default(),
        };
        org_repo.update(pending.organization_id, &update).await?;
        let deleted = org_repo.delete_pending_email_change(pending.id).await?;
        if !deleted {
            return Err(Error::Internal {
                operation: format!(
                    "delete_pending_email_change for {} returned false inside confirm transaction; logic bug",
                    pending.id,
                ),
            });
        }
        tx.commit().await.map_err(|e| Error::Database(e.into()))?;

        return Ok(email_change_html(
            StatusCode::OK,
            "Your organization's contact email has been updated. You can close this tab.",
        ));
    }

    tx.commit().await.map_err(|e| Error::Database(e.into()))?;

    let waiting_message = match just_confirmed_side {
        ConfirmedSide::New => {
            "Thanks — we've verified the new address. The change will take effect once the current contact also confirms via the link sent to their inbox."
        }
        ConfirmedSide::Old => {
            "Thanks — we've recorded the current owner's authorization. The change will take effect once the new address also confirms via the link sent to their inbox."
        }
    };
    Ok(email_change_html(StatusCode::OK, waiting_message))
}

enum ConfirmedSide {
    New,
    Old,
}

fn email_change_html(status: StatusCode, message: &str) -> axum::response::Response {
    let body = format!(
        r#"<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="referrer" content="no-referrer"><title>Email change</title></head>
<body style="font-family: system-ui, sans-serif; max-width: 540px; margin: 60px auto; padding: 20px; color: #333;">
<h2>Organization contact email</h2>
<p>{}</p>
</body></html>"#,
        html_escape(message),
    );
    axum::response::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::REFERRER_POLICY, "no-referrer")
        .header(header::CACHE_CONTROL, "no-store")
        .header("Pragma", "no-cache")
        .body(body.into())
        .expect("static HTML response builds")
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use crate::api::models::users::Role;
    use crate::test::utils::{
        add_auth_headers, create_test_admin_user, create_test_app, create_test_app_with_config, create_test_config, create_test_user,
        create_test_user_on_domain,
    };
    use serde_json::json;
    use sqlx::PgPool;


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_standard_user_can_create_organization(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let user_headers = add_auth_headers(&user);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&user_headers[0].0, &user_headers[0].1)
            .add_header(&user_headers[1].0, &user_headers[1].1)
            .json(&json!({ "name": "my-org", "email": "contact@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let body = resp.json::<serde_json::Value>();
        let username = body["username"].as_str().unwrap();
        assert!(username.starts_with("example.com~"), "got {username}");
        assert_eq!(body["display_name"].as_str().unwrap(), "my-org");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_contact_email_must_be_at_the_owners_domain(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({ "name": "Acme", "email": "victim@unrelated.test" }))
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({ "name": "Acme", "email": "billing@acme.test" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_personal_email_owner_must_use_their_own_address(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "gmail.com").await;
        let headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({ "name": "Acme", "email": "someone-else@gmail.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({ "name": "Acme", "email": owner.email.to_uppercase() }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_domain_claim_is_case_insensitive(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_on_domain(&pool, Role::StandardUser, "MixedCase.test").await;
        let headers = add_auth_headers(&user);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({ "name": "Mixed Case Ltd", "email": "billing@mixedcase.test" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let username = resp.json::<serde_json::Value>()["username"].as_str().unwrap().to_string();
        assert!(
            username.starts_with("mixedcase.test~"),
            "claim should be lowercased, got {username}"
        );

        let mut conn = pool.acquire().await.unwrap();
        assert!(
            crate::db::handlers::Organizations::new(&mut conn)
                .find_by_domain("mixedcase.test")
                .await
                .unwrap()
                .is_some(),
            "a differently-cased colleague must still match the workspace"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_domain_can_hold_several_workspaces(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let headers = add_auth_headers(&user);

        let mut ids = Vec::new();
        for name in ["Acme Prod", "Acme Dev"] {
            let resp = server
                .post("/admin/api/v1/organizations")
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({ "name": name, "email": "billing@acme.test" }))
                .await;
            resp.assert_status(axum::http::StatusCode::CREATED);
            let body = resp.json::<serde_json::Value>();
            assert!(body["username"].as_str().unwrap().starts_with("acme.test~"));
            ids.push(body["id"].as_str().unwrap().to_string());
        }
        assert_ne!(ids[0], ids[1], "both workspaces exist independently");

        let mut conn = pool.acquire().await.unwrap();
        let matched = crate::db::handlers::Organizations::new(&mut conn)
            .find_by_domain("acme.test")
            .await
            .unwrap()
            .expect("a workspace matches the domain");
        assert_eq!(matched.id.to_string(), ids[0], "the oldest surviving workspace wins");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_personal_email_owner_username_is_opaque(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "gmail.com").await;
        let headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({ "name": "acme.test", "email": owner.email }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let body = resp.json::<serde_json::Value>();
        let username = body["username"].as_str().unwrap();
        assert!(
            username.starts_with("user~") && username.len() == "user~".len() + 32,
            "personal-email fallback must use an opaque user~<uuid> username, got {username}"
        );
        assert_ne!(username, "acme.test", "the planted domain must not become the username");
        assert_eq!(body["display_name"].as_str().unwrap(), "acme.test");

        let mut conn = pool.acquire().await.unwrap();
        assert!(
            crate::db::handlers::Organizations::new(&mut conn)
                .find_by_domain("acme.test")
                .await
                .unwrap()
                .is_none(),
            "an opaque username must not match domain routing"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_personal_email_owner_cannot_hijack_unclaimed_domain(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;

        let attacker = create_test_user_on_domain(&pool, Role::StandardUser, "gmail.com").await;
        let attacker_headers = add_auth_headers(&attacker);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&attacker_headers[0].0, &attacker_headers[0].1)
            .add_header(&attacker_headers[1].0, &attacker_headers[1].1)
            .json(&json!({ "name": "acme.test", "email": attacker.email }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let body = resp.json::<serde_json::Value>();
        let org_id = body["id"].as_str().unwrap().to_string();
        assert_ne!(
            body["username"].as_str().unwrap(),
            "acme.test",
            "the planted domain must not be stored as the username"
        );

        set_auto_join(&pool, uuid::Uuid::parse_str(&org_id).unwrap(), true).await;

        {
            let mut conn = pool.acquire().await.unwrap();
            assert!(
                crate::db::handlers::Organizations::new(&mut conn)
                    .find_by_domain("acme.test")
                    .await
                    .unwrap()
                    .is_none(),
                "the planted workspace must not be reachable by domain routing"
            );
        }

        let victim_ext = format!("victim{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let victim_email = format!("{victim_ext}@acme.test");
        let resp = server
            .get("/admin/api/v1/users/current/onboarding-context")
            .add_header("x-doubleword-user", victim_ext.as_str())
            .add_header("x-doubleword-email", victim_email.as_str())
            .await;
        resp.assert_status_ok();
        let ctx = resp.json::<serde_json::Value>();
        assert!(
            ctx["domain_match"].is_null(),
            "the victim must be offered no domain match, got {}",
            ctx["domain_match"]
        );

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&attacker_headers[0].0, &attacker_headers[0].1)
            .add_header(&attacker_headers[1].0, &attacker_headers[1].1)
            .await;
        resp.assert_status_ok();
        let members = resp.json::<serde_json::Value>();
        let serialized = serde_json::to_string(&members).unwrap();
        assert!(
            !serialized.contains(&victim_email),
            "the attacker must not see the victim's email in the member list: {serialized}"
        );
        assert_eq!(members.as_array().unwrap().len(), 1, "no victim may be silently enrolled");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_personal_email_owner_cannot_plant_a_suffixed_domain(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let attacker = create_test_user_on_domain(&pool, Role::StandardUser, "gmail.com").await;
        let headers = add_auth_headers(&attacker);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({ "name": "acme.test~aaaaaaaa", "email": attacker.email }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);

        let mut conn = pool.acquire().await.unwrap();
        assert!(
            crate::db::handlers::Organizations::new(&mut conn)
                .find_by_domain("acme.test")
                .await
                .unwrap()
                .is_none(),
            "a suffixed plant must not match the LIKE arm either"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_business_email_owner_still_claims_domain(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({ "name": "Acme Corporation", "email": "billing@acme.test" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let body = resp.json::<serde_json::Value>();
        let username = body["username"].as_str().unwrap();
        assert!(
            username.starts_with("acme.test~"),
            "a business-email owner must still claim their domain, got {username}"
        );
        assert_eq!(body["display_name"].as_str().unwrap(), "Acme Corporation");

        let mut conn = pool.acquire().await.unwrap();
        let matched = crate::db::handlers::Organizations::new(&mut conn)
            .find_by_domain("acme.test")
            .await
            .unwrap()
            .expect("the business-email claim is still routable");
        assert_eq!(matched.id.to_string(), body["id"].as_str().unwrap());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_deleted_workspace_is_not_matched_by_domain(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user_on_domain(&pool, Role::StandardUser, "gone.test").await;
        let headers = add_auth_headers(&user);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({ "name": "Gone Ltd", "email": "billing@gone.test" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        {
            let mut conn = pool.acquire().await.unwrap();
            assert!(
                crate::db::handlers::Organizations::new(&mut conn)
                    .find_by_domain("gone.test")
                    .await
                    .unwrap()
                    .is_some(),
                "matches while it exists"
            );
        }

        crate::db::handlers::Organizations::new(&mut *pool.acquire().await.unwrap())
            .delete(uuid::Uuid::parse_str(&org_id).unwrap())
            .await
            .unwrap();

        let mut conn = pool.acquire().await.unwrap();
        assert!(
            crate::db::handlers::Organizations::new(&mut conn)
                .find_by_domain("gone.test")
                .await
                .unwrap()
                .is_none(),
            "a deleted workspace must not collect join requests"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_standard_user_becomes_owner_of_created_org(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let user_headers = add_auth_headers(&user);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&user_headers[0].0, &user_headers[0].1)
            .add_header(&user_headers[1].0, &user_headers[1].1)
            .json(&json!({ "name": "self-serve-org", "email": "contact@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&user_headers[0].0, &user_headers[0].1)
            .add_header(&user_headers[1].0, &user_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let members = resp.json::<Vec<serde_json::Value>>();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0]["role"].as_str().unwrap(), "owner");
        assert_eq!(members[0]["user"]["id"].as_str().unwrap(), user.id.to_string());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_user_organizations_summary_reflects_org_verification(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let user_headers = add_auth_headers(&user);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&user_headers[0].0, &user_headers[0].1)
            .add_header(&user_headers[1].0, &user_headers[1].1)
            .json(&json!({ "name": "verify-org", "email": "contact@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id: uuid::Uuid = resp.json::<serde_json::Value>()["id"].as_str().unwrap().parse().unwrap();

        let orgs_url = format!("/admin/api/v1/users/{}/organizations", user.id);
        let fetch_summary = async |server: &axum_test::TestServer| -> serde_json::Value {
            let resp = server
                .get(&orgs_url)
                .add_header(&user_headers[0].0, &user_headers[0].1)
                .add_header(&user_headers[1].0, &user_headers[1].1)
                .await;
            resp.assert_status(axum::http::StatusCode::OK);
            resp.json::<Vec<serde_json::Value>>().into_iter().next().expect("one org")
        };

        assert_eq!(fetch_summary(&server).await["verified"].as_bool(), Some(false));

        let mut conn = pool.acquire().await.unwrap();
        crate::db::handlers::users::Users::new(&mut conn)
            .set_verified(org_id)
            .await
            .unwrap();
        drop(conn);

        assert_eq!(fetch_summary(&server).await["verified"].as_bool(), Some(true));

        let resp = server
            .get("/admin/api/v1/users/current")
            .add_header(&user_headers[0].0, &user_headers[0].1)
            .add_header(&user_headers[1].0, &user_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        assert_eq!(
            resp.json::<serde_json::Value>()["verified"].as_bool(),
            Some(false),
            "the member is still unverified in their own right"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_user_organizations_summary_reflects_org_zero_data_retention(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let user_headers = add_auth_headers(&user);
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let admin_headers = add_auth_headers(&admin);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&user_headers[0].0, &user_headers[0].1)
            .add_header(&user_headers[1].0, &user_headers[1].1)
            .json(&json!({ "name": "zdr-org", "email": "contact@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let orgs_url = format!("/admin/api/v1/users/{}/organizations", user.id);

        let resp = server
            .get(&orgs_url)
            .add_header(&user_headers[0].0, &user_headers[0].1)
            .add_header(&user_headers[1].0, &user_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let orgs = resp.json::<Vec<serde_json::Value>>();
        assert_eq!(orgs.len(), 1);
        assert_eq!(orgs[0]["zero_data_retention"].as_bool(), Some(false));

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&admin_headers[0].0, &admin_headers[0].1)
            .add_header(&admin_headers[1].0, &admin_headers[1].1)
            .json(&json!({ "zero_data_retention": true }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);

        let resp = server
            .get(&orgs_url)
            .add_header(&user_headers[0].0, &user_headers[0].1)
            .add_header(&user_headers[1].0, &user_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let orgs = resp.json::<Vec<serde_json::Value>>();
        assert_eq!(orgs[0]["zero_data_retention"].as_bool(), Some(true));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_organization_owner_can_update_zero_data_retention(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "name": "owner-zdr-org", "email": "contact@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "zero_data_retention": true }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        assert_eq!(resp.json::<serde_json::Value>()["zero_data_retention"].as_bool(), Some(true));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_organization_owners_cannot_touch_serving_settings_or_customer_serving_settings(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "name": "owner-serving-org", "email": "contact@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        for body in [
            json!({ "self_hosted_only": true }),
            json!({ "display_name": "Renamed", "self_hosted_only": true }),
        ] {
            let resp = server
                .patch(&format!("/admin/api/v1/organizations/{org_id}"))
                .add_header(&owner_headers[0].0, &owner_headers[0].1)
                .add_header(&owner_headers[1].0, &owner_headers[1].1)
                .json(&body)
                .await;
            resp.assert_status(axum::http::StatusCode::FORBIDDEN);
        }
        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let body = resp.json::<serde_json::Value>();
        assert_ne!(body["display_name"].as_str(), Some("Renamed"), "a refused patch applies nothing");
        assert_eq!(body["self_hosted_only"].as_bool(), Some(false));

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "self_hosted_only": true }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_organization_non_owners_cannot_update_zero_data_retention(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);
        let org_admin = create_test_user(&pool, Role::StandardUser).await;
        let member = create_test_user(&pool, Role::StandardUser).await;
        let other_owner = create_test_user(&pool, Role::StandardUser).await;
        let other_owner_headers = add_auth_headers(&other_owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&other_owner_headers[0].0, &other_owner_headers[0].1)
            .add_header(&other_owner_headers[1].0, &other_owner_headers[1].1)
            .json(&json!({ "name": "other-owner-org", "email": "contact@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({
                "name": "non-owner-zdr-org",
                "email": "contact@example.com",
                "owner_id": owner.id,
            }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        for (user_id, role) in [(org_admin.id, "admin"), (member.id, "member")] {
            let resp = server
                .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
                .add_header(&owner_headers[0].0, &owner_headers[0].1)
                .add_header(&owner_headers[1].0, &owner_headers[1].1)
                .json(&json!({ "user_id": user_id, "role": role }))
                .await;
            resp.assert_status(axum::http::StatusCode::CREATED);
        }

        for user in [&org_admin, &member, &other_owner] {
            let headers = add_auth_headers(user);
            let resp = server
                .patch(&format!("/admin/api/v1/organizations/{org_id}"))
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({ "zero_data_retention": true }))
                .await;
            resp.assert_status(axum::http::StatusCode::FORBIDDEN);
        }

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        assert_eq!(resp.json::<serde_json::Value>()["zero_data_retention"].as_bool(), Some(false));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_standard_user_cannot_set_owner_id(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let user_headers = add_auth_headers(&user);
        let other = create_test_user(&pool, Role::StandardUser).await;

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&user_headers[0].0, &user_headers[0].1)
            .add_header(&user_headers[1].0, &user_headers[1].1)
            .json(&json!({ "name": "hijack-org", "email": "x@example.com", "owner_id": other.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_cannot_create_org_when_at_limit(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let user_headers = add_auth_headers(&user);

        for i in 0..super::MAX_ORGS_PER_USER {
            let resp = server
                .post("/admin/api/v1/organizations")
                .add_header(&user_headers[0].0, &user_headers[0].1)
                .add_header(&user_headers[1].0, &user_headers[1].1)
                .json(&json!({ "name": format!("org-{i}"), "email": format!("org{i}@example.com") }))
                .await;
            resp.assert_status(axum::http::StatusCode::CREATED);
        }

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&user_headers[0].0, &user_headers[0].1)
            .add_header(&user_headers[1].0, &user_headers[1].1)
            .json(&json!({ "name": "one-too-many", "email": "extra@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body = resp.text();
        assert!(body.contains("maximum"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_resend_invite_rotates_the_token(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "name": "resend-org", "email": "resend@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/invites"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "email": "invitee@example.com", "role": "member" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let invite_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let hash_before: Option<String> = sqlx::query_scalar!(
            "SELECT invite_token_hash FROM user_organizations WHERE id = $1",
            uuid::Uuid::parse_str(&invite_id).unwrap()
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/invites/{invite_id}/resend"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::NO_CONTENT);

        let hash_after: Option<String> = sqlx::query_scalar!(
            "SELECT invite_token_hash FROM user_organizations WHERE id = $1",
            uuid::Uuid::parse_str(&invite_id).unwrap()
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(hash_before.is_some() && hash_after.is_some());
        assert_ne!(hash_before, hash_after, "the old invite link must stop working");
    }


    async fn org_with_join_request(pool: &PgPool, owner_id: uuid::Uuid, joiner_id: uuid::Uuid) -> (uuid::Uuid, uuid::Uuid) {
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = crate::db::handlers::Organizations::new(&mut conn);
        let org = repo
            .create(
                &crate::db::models::organizations::OrganizationCreateDBRequest {
                    name: format!("jr-{}", &owner_id.to_string()[..8]),
                    email: format!("org-{}@example.com", &owner_id.to_string()[..8]),
                    display_name: None,
                    avatar_url: None,
                    created_by: owner_id,
                },
                &[Role::StandardUser],
            )
            .await
            .unwrap();
        let request = repo.create_join_request(org.id, joiner_id).await.unwrap();
        (org.id, request.id)
    }

    async fn org_claiming_domain(pool: &PgPool, owner_id: uuid::Uuid, domain: &str) -> uuid::Uuid {
        let mut conn = pool.acquire().await.unwrap();
        let org = crate::db::handlers::Organizations::new(&mut conn)
            .create(
                &crate::db::models::organizations::OrganizationCreateDBRequest {
                    name: domain.to_string(),
                    email: format!("billing@{domain}"),
                    display_name: Some("Acme Corporation".to_string()),
                    avatar_url: None,
                    created_by: owner_id,
                },
                &[Role::StandardUser],
            )
            .await
            .unwrap();
        org.id
    }

    async fn set_auto_join(pool: &PgPool, org_id: uuid::Uuid, enabled: bool) {
        let mut conn = pool.acquire().await.unwrap();
        crate::db::handlers::Organizations::new(&mut conn)
            .set_auto_join_enabled(org_id, enabled)
            .await
            .unwrap();
    }

    async fn invite_email_to_org(pool: &PgPool, org_id: uuid::Uuid, inviter_id: uuid::Uuid, email: &str) -> uuid::Uuid {
        let mut conn = pool.acquire().await.unwrap();
        crate::db::handlers::Organizations::new(&mut conn)
            .create_invite(
                org_id,
                None,
                email,
                "member",
                inviter_id,
                "test-token-hash",
                chrono::Utc::now() + chrono::Duration::days(7),
            )
            .await
            .unwrap()
            .id
    }

    fn onboarding_context(
        server: &axum_test::TestServer,
        user: &crate::api::models::users::UserResponse,
    ) -> impl Future<Output = serde_json::Value> {
        let headers = add_auth_headers(user);
        let request = server
            .get("/admin/api/v1/users/current/onboarding-context")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1);
        async move {
            let resp = request.await;
            resp.assert_status_ok();
            resp.json::<serde_json::Value>()
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_onboarding_context_offers_the_choice_on_a_domain_match(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let org_id = org_claiming_domain(&pool, owner.id, "acme.test").await;
        let joiner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;

        let body = onboarding_context(&server, &joiner).await;

        let matched = &body["domain_match"];
        assert_eq!(matched["organization_id"].as_str().unwrap(), org_id.to_string());
        assert_eq!(matched["domain"].as_str().unwrap(), "acme.test");
        assert_eq!(matched["auto_join_enabled"].as_bool().unwrap(), false);
        assert!(body["invitation"].is_null());
        assert!(body["join_request"].is_null(), "nothing may be filed on their behalf");

        let mut conn = pool.acquire().await.unwrap();
        assert!(
            crate::db::handlers::Organizations::new(&mut conn)
                .list_join_requests(org_id)
                .await
                .unwrap()
                .is_empty(),
            "reading the context must not queue anything"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_domain_match_names_only_the_domain(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        org_claiming_domain(&pool, owner.id, "acme.test").await;
        let joiner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;

        let body = onboarding_context(&server, &joiner).await;
        let matched = body["domain_match"].as_object().unwrap();

        let mut keys: Vec<&str> = matched.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["auto_join_enabled", "domain", "organization_id"]);
        assert!(
            !serde_json::to_string(&body).unwrap().contains("Acme Corporation"),
            "the workspace's display name must not reach a non-member"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_onboarding_context_ignores_personal_domains(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        org_claiming_domain(&pool, owner.id, "gmail.com").await;
        let consumer = create_test_user_on_domain(&pool, Role::StandardUser, "gmail.com").await;

        let body = onboarding_context(&server, &consumer).await;

        assert!(body["domain_match"].is_null());
        assert!(body["invitation"].is_null());
        assert!(body["join_request"].is_null());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_onboarding_context_surfaces_an_invitation(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let org_id = org_claiming_domain(&pool, owner.id, "elsewhere.test").await;
        let invitee = create_test_user_on_domain(&pool, Role::StandardUser, "unrelated.test").await;
        let invite_id = invite_email_to_org(&pool, org_id, owner.id, &invitee.email).await;

        let body = onboarding_context(&server, &invitee).await;

        let invitation = &body["invitation"];
        assert_eq!(invitation["id"].as_str().unwrap(), invite_id.to_string());
        assert_eq!(invitation["organization_id"].as_str().unwrap(), org_id.to_string());
        assert_eq!(invitation["organization_name"].as_str().unwrap(), "Acme Corporation");
        assert_eq!(invitation["role"].as_str().unwrap(), "member");
        assert!(body["domain_match"].is_null(), "their own domain claims nothing");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_onboarding_context_goes_quiet_for_a_member(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let org_id = org_claiming_domain(&pool, owner.id, "acme.test").await;
        let member = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;

        assert!(
            !onboarding_context(&server, &member).await["domain_match"].is_null(),
            "offered before they are in"
        );

        {
            let mut conn = pool.acquire().await.unwrap();
            crate::db::handlers::Organizations::new(&mut conn)
                .add_member(org_id, member.id, "member")
                .await
                .unwrap();
        }

        assert!(
            onboarding_context(&server, &member).await["domain_match"].is_null(),
            "and silent once they are"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_request_access_is_opt_in_and_idempotent(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let org_id = org_claiming_domain(&pool, owner.id, "acme.test").await;
        let joiner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let headers = add_auth_headers(&joiner);

        let ask = || async {
            let resp = server
                .post("/admin/api/v1/users/current/join-requests")
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .await;
            resp.assert_status_ok();
            resp.json::<serde_json::Value>()
        };

        let first = ask().await;
        assert_eq!(first["outcome"].as_str().unwrap(), "requested");
        assert_eq!(first["organization_id"].as_str().unwrap(), org_id.to_string());
        let request_id = first["join_request"]["id"].as_str().unwrap().to_string();

        let second = ask().await;
        assert_eq!(second["outcome"].as_str().unwrap(), "requested");
        assert_eq!(
            second["join_request"]["id"].as_str().unwrap(),
            request_id,
            "the same request, not a second one"
        );

        let mut conn = pool.acquire().await.unwrap();
        let queued = crate::db::handlers::Organizations::new(&mut conn)
            .list_join_requests(org_id)
            .await
            .unwrap();
        assert_eq!(queued.len(), 1);

        let body = onboarding_context(&server, &joiner).await;
        assert_eq!(body["join_request"]["id"].as_str().unwrap(), request_id);
        assert!(
            !body["domain_match"].is_null(),
            "still on the intercept screen, now showing the request as sent"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_request_access_joins_immediately_when_auto_join_is_on(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let org_id = org_claiming_domain(&pool, owner.id, "acme.test").await;
        let joiner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        set_auto_join(&pool, org_id, true).await;
        let headers = add_auth_headers(&joiner);

        assert_eq!(
            onboarding_context(&server, &joiner).await["domain_match"]["auto_join_enabled"]
                .as_bool()
                .unwrap(),
            true,
            "the client is told not to bother asking"
        );

        let resp = server
            .post("/admin/api/v1/users/current/join-requests")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        resp.assert_status_ok();
        let body = resp.json::<serde_json::Value>();
        assert_eq!(body["outcome"].as_str().unwrap(), "joined");
        assert!(body["join_request"].is_null());

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = crate::db::handlers::Organizations::new(&mut conn);
        assert_eq!(repo.get_user_org_role(joiner.id, org_id).await.unwrap(), Some("member".to_string()));
        assert!(
            repo.list_join_requests(org_id).await.unwrap().is_empty(),
            "nobody was queued — they were admitted"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_request_access_reports_an_outstanding_invitation(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let org_id = org_claiming_domain(&pool, owner.id, "acme.test").await;
        let invitee = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;

        {
            let mut conn = pool.acquire().await.unwrap();
            crate::db::handlers::Organizations::new(&mut conn)
                .create_invite(
                    org_id,
                    Some(invitee.id),
                    &invitee.email,
                    "member",
                    owner.id,
                    "test-token-hash",
                    chrono::Utc::now() + chrono::Duration::days(7),
                )
                .await
                .unwrap();
        }

        let headers = add_auth_headers(&invitee);
        let resp = server
            .post("/admin/api/v1/users/current/join-requests")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        resp.assert_status_ok();
        let body = resp.json::<serde_json::Value>();

        assert_eq!(body["outcome"].as_str().unwrap(), "invited");
        assert!(
            body["join_request"].is_null(),
            "an invitation's id must not be handed back as a join request"
        );

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = crate::db::handlers::Organizations::new(&mut conn);
        assert!(repo.list_join_requests(org_id).await.unwrap().is_empty(), "nothing was queued");
        assert!(
            repo.find_pending_invite_for_email(&invitee.email).await.unwrap().is_some(),
            "and the invitation survives, still acceptable"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_request_access_needs_a_matching_domain(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        org_claiming_domain(&pool, owner.id, "acme.test").await;
        let outsider = create_test_user_on_domain(&pool, Role::StandardUser, "elsewhere.test").await;
        let headers = add_auth_headers(&outsider);

        let resp = server
            .post("/admin/api/v1/users/current/join-requests")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_invitation_can_be_accepted_by_id_without_its_token(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let org_id = org_claiming_domain(&pool, owner.id, "acme.test").await;
        let invitee = create_test_user_on_domain(&pool, Role::StandardUser, "unrelated.test").await;
        let invite_id = invite_email_to_org(&pool, org_id, owner.id, &invitee.email).await;
        let headers = add_auth_headers(&invitee);

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/invites/{invite_id}/accept"))
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        resp.assert_status_ok();

        let mut conn = pool.acquire().await.unwrap();
        assert_eq!(
            crate::db::handlers::Organizations::new(&mut conn)
                .get_user_org_role(invitee.id, org_id)
                .await
                .unwrap(),
            Some("member".to_string())
        );

        assert!(onboarding_context(&server, &invitee).await["invitation"].is_null());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_invitation_by_id_is_refused_to_anyone_else(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let org_id = org_claiming_domain(&pool, owner.id, "acme.test").await;
        let invitee = create_test_user_on_domain(&pool, Role::StandardUser, "unrelated.test").await;
        let invite_id = invite_email_to_org(&pool, org_id, owner.id, &invitee.email).await;

        let stranger = create_test_user_on_domain(&pool, Role::StandardUser, "stranger.test").await;
        let headers = add_auth_headers(&stranger);

        for action in ["accept", "decline"] {
            let resp = server
                .post(&format!("/admin/api/v1/organizations/{org_id}/invites/{invite_id}/{action}"))
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .await;
            resp.assert_status(axum::http::StatusCode::NOT_FOUND);
        }

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = crate::db::handlers::Organizations::new(&mut conn);
        assert_eq!(repo.get_user_org_role(stranger.id, org_id).await.unwrap(), None);
        assert!(
            repo.find_invite_by_id(org_id, invite_id).await.unwrap().is_some(),
            "the real invitee's invitation must survive the attempt"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_invitation_can_be_declined_by_id(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let org_id = org_claiming_domain(&pool, owner.id, "acme.test").await;
        let invitee = create_test_user_on_domain(&pool, Role::StandardUser, "unrelated.test").await;
        let invite_id = invite_email_to_org(&pool, org_id, owner.id, &invitee.email).await;
        let headers = add_auth_headers(&invitee);

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/invites/{invite_id}/decline"))
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        resp.assert_status_ok();

        let body = onboarding_context(&server, &invitee).await;
        assert!(body["invitation"].is_null());

        let mut conn = pool.acquire().await.unwrap();
        assert_eq!(
            crate::db::handlers::Organizations::new(&mut conn)
                .get_user_org_role(invitee.id, org_id)
                .await
                .unwrap(),
            None,
            "declining must not admit them"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_auto_join_is_owner_only(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let org_id = org_claiming_domain(&pool, owner.id, "acme.test").await;
        let admin = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        {
            let mut conn = pool.acquire().await.unwrap();
            crate::db::handlers::Organizations::new(&mut conn)
                .add_member(org_id, admin.id, "admin")
                .await
                .unwrap();
        }

        let admin_headers = add_auth_headers(&admin);
        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&admin_headers[0].0, &admin_headers[0].1)
            .add_header(&admin_headers[1].0, &admin_headers[1].1)
            .json(&json!({ "auto_join_enabled": true }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);

        let owner_headers = add_auth_headers(&owner);
        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "auto_join_enabled": true }))
            .await;
        resp.assert_status_ok();
        assert_eq!(resp.json::<serde_json::Value>()["auto_join_enabled"].as_bool().unwrap(), true);

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status_ok();
        assert_eq!(
            resp.json::<serde_json::Value>()["auto_join_enabled"].as_bool().unwrap(),
            true,
            "and it survives the round trip"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_auto_join_defaults_off(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let org_id = org_claiming_domain(&pool, owner.id, "acme.test").await;
        let headers = add_auth_headers(&owner);

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        resp.assert_status_ok();
        assert_eq!(resp.json::<serde_json::Value>()["auto_join_enabled"].as_bool().unwrap(), false);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_onboarding_context_is_not_readable_for_others(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let target = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let snooper = create_test_user_on_domain(&pool, Role::StandardUser, "elsewhere.test").await;
        let headers = add_auth_headers(&snooper);

        for path in [
            format!("/admin/api/v1/users/{}/onboarding-context", target.id),
            format!("/admin/api/v1/users/{}/join-requests", target.id),
        ] {
            let resp = server
                .get(&path)
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .await;
            resp.assert_status(axum::http::StatusCode::FORBIDDEN);
        }

        let resp = server
            .post(&format!("/admin/api/v1/users/{}/join-requests", target.id))
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_join_requests_hidden_from_ordinary_members(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let joiner = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, _) = org_with_join_request(&pool, owner.id, joiner.id).await;

        let member = create_test_user(&pool, Role::StandardUser).await;
        {
            let mut conn = pool.acquire().await.unwrap();
            crate::db::handlers::Organizations::new(&mut conn)
                .add_member(org_id, member.id, "member")
                .await
                .unwrap();
        }
        let member_headers = add_auth_headers(&member);

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}/join-requests"))
            .add_header(&member_headers[0].0, &member_headers[0].1)
            .add_header(&member_headers[1].0, &member_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);

        let owner_headers = add_auth_headers(&owner);
        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}/join-requests"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status_ok();
        assert_eq!(resp.json::<serde_json::Value>().as_array().unwrap().len(), 1);
    }

    #[dwctl_test_macros::test]
    async fn test_list_admin_emails_covers_only_those_who_can_decide(pool: PgPool) {
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let admin = create_test_user(&pool, Role::StandardUser).await;
        let member = create_test_user(&pool, Role::StandardUser).await;
        let joiner = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, _request_id) = org_with_join_request(&pool, owner.id, joiner.id).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = crate::db::handlers::Organizations::new(&mut conn);
        repo.add_member(org_id, admin.id, "admin").await.unwrap();
        repo.add_member(org_id, member.id, "member").await.unwrap();

        let emails = repo.list_admin_emails(org_id).await.unwrap();

        assert!(emails.contains(&owner.email), "the owner decides");
        assert!(emails.contains(&admin.email), "admins decide");
        assert!(!emails.contains(&member.email), "a plain member cannot approve");
        assert!(
            !emails.contains(&joiner.email),
            "the requester is not notified of their own request"
        );
        assert_eq!(emails.len(), 2);
    }

    #[dwctl_test_macros::test]
    async fn test_list_admin_emails_skips_deleted_accounts(pool: PgPool) {
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let admin = create_test_user(&pool, Role::StandardUser).await;
        let joiner = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, _request_id) = org_with_join_request(&pool, owner.id, joiner.id).await;

        let mut conn = pool.acquire().await.unwrap();
        crate::db::handlers::Organizations::new(&mut conn)
            .add_member(org_id, admin.id, "admin")
            .await
            .unwrap();
        use crate::db::handlers::Repository as _;
        crate::db::handlers::Users::new(&mut conn).delete(admin.id).await.unwrap();

        let emails = crate::db::handlers::Organizations::new(&mut conn)
            .list_admin_emails(org_id)
            .await
            .unwrap();

        assert_eq!(emails, vec![owner.email], "only the live owner is left to tell");
    }

    #[dwctl_test_macros::test]
    async fn test_approve_join_request_returns_the_approved_user_once(pool: PgPool) {
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let joiner = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, request_id) = org_with_join_request(&pool, owner.id, joiner.id).await;

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = crate::db::handlers::Organizations::new(&mut conn);

        let approved = repo.approve_join_request(org_id, request_id, "member").await.unwrap();
        assert_eq!(approved, Some(joiner.id), "the caller learns who to tell");

        let again = repo.approve_join_request(org_id, request_id, "member").await.unwrap();
        assert_eq!(again, None, "a second approval has nobody new to notify");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_approve_join_request_makes_an_active_member(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let joiner = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, request_id) = org_with_join_request(&pool, owner.id, joiner.id).await;
        let owner_headers = add_auth_headers(&owner);

        {
            let mut conn = pool.acquire().await.unwrap();
            let role = crate::db::handlers::Organizations::new(&mut conn)
                .get_user_org_role(joiner.id, org_id)
                .await
                .unwrap();
            assert_eq!(role, None);
        }

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/join-requests/{request_id}/approve"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::NO_CONTENT);

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = crate::db::handlers::Organizations::new(&mut conn);
        assert_eq!(repo.get_user_org_role(joiner.id, org_id).await.unwrap(), Some("member".to_string()));
        assert!(repo.list_join_requests(org_id).await.unwrap().is_empty(), "queue is cleared");

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/join-requests/{request_id}/approve"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::NOT_FOUND);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_decline_join_request_allows_asking_again(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let joiner = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, request_id) = org_with_join_request(&pool, owner.id, joiner.id).await;
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/join-requests/{request_id}/decline"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::NO_CONTENT);

        let mut conn = pool.acquire().await.unwrap();
        let mut repo = crate::db::handlers::Organizations::new(&mut conn);
        assert!(repo.list_join_requests(org_id).await.unwrap().is_empty());
        assert_eq!(repo.get_user_org_role(joiner.id, org_id).await.unwrap(), None);

        repo.create_join_request(org_id, joiner.id).await.expect("can ask again");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_join_request_decisions_are_scoped_to_their_org(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner_a = create_test_user(&pool, Role::StandardUser).await;
        let owner_b = create_test_user(&pool, Role::StandardUser).await;
        let joiner = create_test_user(&pool, Role::StandardUser).await;

        let (_org_a, _) = org_with_join_request(&pool, owner_a.id, joiner.id).await;
        let other = create_test_user(&pool, Role::StandardUser).await;
        let (org_b, request_b) = org_with_join_request(&pool, owner_b.id, other.id).await;

        let a_headers = add_auth_headers(&owner_a);
        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_b}/join-requests/{request_b}/approve"))
            .add_header(&a_headers[0].0, &a_headers[0].1)
            .add_header(&a_headers[1].0, &a_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);

        let mut conn = pool.acquire().await.unwrap();
        let role = crate::db::handlers::Organizations::new(&mut conn)
            .get_user_org_role(other.id, org_b)
            .await
            .unwrap();
        assert_eq!(role, None, "the request must still be outstanding");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_user_sees_their_own_pending_join_request(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let joiner = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, request_id) = org_with_join_request(&pool, owner.id, joiner.id).await;
        let joiner_headers = add_auth_headers(&joiner);

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}/join-requests"))
            .add_header(&joiner_headers[0].0, &joiner_headers[0].1)
            .add_header(&joiner_headers[1].0, &joiner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);

        let resp = server
            .get("/admin/api/v1/users/current/join-requests")
            .add_header(&joiner_headers[0].0, &joiner_headers[0].1)
            .add_header(&joiner_headers[1].0, &joiner_headers[1].1)
            .await;
        resp.assert_status_ok();
        let body = resp.json::<serde_json::Value>();
        let items = body.as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"].as_str().unwrap(), request_id.to_string());
        assert_eq!(items[0]["organization_id"].as_str().unwrap(), org_id.to_string());
        assert!(items[0]["organization_name"].as_str().is_some_and(|n| !n.is_empty()));

        assert!(items[0].get("user").is_none());
        assert!(items[0].get("member_count").is_none());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_own_join_requests_clear_once_decided(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let joiner = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, request_id) = org_with_join_request(&pool, owner.id, joiner.id).await;
        let joiner_headers = add_auth_headers(&joiner);
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .get("/admin/api/v1/users/current/join-requests")
            .add_header(&joiner_headers[0].0, &joiner_headers[0].1)
            .add_header(&joiner_headers[1].0, &joiner_headers[1].1)
            .await;
        resp.assert_status_ok();
        assert_eq!(
            resp.json::<serde_json::Value>().as_array().unwrap().len(),
            1,
            "outstanding before the decision"
        );

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/join-requests/{request_id}/approve"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::NO_CONTENT);

        let resp = server
            .get("/admin/api/v1/users/current/join-requests")
            .add_header(&joiner_headers[0].0, &joiner_headers[0].1)
            .add_header(&joiner_headers[1].0, &joiner_headers[1].1)
            .await;
        resp.assert_status_ok();
        assert!(
            resp.json::<serde_json::Value>().as_array().unwrap().is_empty(),
            "an approved request is no longer pending"
        );

        let resp = server
            .get("/admin/api/v1/users/current/organizations")
            .add_header(&joiner_headers[0].0, &joiner_headers[0].1)
            .add_header(&joiner_headers[1].0, &joiner_headers[1].1)
            .await;
        resp.assert_status_ok();
        assert_eq!(resp.json::<serde_json::Value>().as_array().unwrap().len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_own_join_requests_are_not_readable_by_other_users(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let joiner = create_test_user(&pool, Role::StandardUser).await;
        let (_org_id, _) = org_with_join_request(&pool, owner.id, joiner.id).await;

        let snooper = create_test_user(&pool, Role::StandardUser).await;
        let snooper_headers = add_auth_headers(&snooper);
        let resp = server
            .get(&format!("/admin/api/v1/users/{}/join-requests", joiner.id))
            .add_header(&snooper_headers[0].0, &snooper_headers[0].1)
            .add_header(&snooper_headers[1].0, &snooper_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);

        let resp = server
            .get("/admin/api/v1/users/current/join-requests")
            .add_header(&snooper_headers[0].0, &snooper_headers[0].1)
            .add_header(&snooper_headers[1].0, &snooper_headers[1].1)
            .await;
        resp.assert_status_ok();
        assert!(resp.json::<serde_json::Value>().as_array().unwrap().is_empty());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_member_can_leave_organization(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);
        let member = create_test_user(&pool, Role::StandardUser).await;
        let member_headers = add_auth_headers(&member);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "name": "leave-org", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "user_id": member.id, "role": "member" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/leave"))
            .add_header(&member_headers[0].0, &member_headers[0].1)
            .add_header(&member_headers[1].0, &member_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::NO_CONTENT);

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let members = resp.json::<Vec<serde_json::Value>>();
        assert_eq!(members.len(), 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_last_owner_cannot_leave(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "name": "solo-org", "email": "solo@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/leave"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body = resp.text();
        assert!(body.contains("last owner"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_non_member_cannot_leave(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let outsider = create_test_user(&pool, Role::StandardUser).await;
        let outsider_headers = add_auth_headers(&outsider);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "name": "private-org", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/leave"))
            .add_header(&outsider_headers[0].0, &outsider_headers[0].1)
            .add_header(&outsider_headers[1].0, &outsider_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::NOT_FOUND);
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_cannot_remove_last_owner(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let admin_headers = add_auth_headers(&admin);

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&admin_headers[0].0, &admin_headers[0].1)
            .add_header(&admin_headers[1].0, &admin_headers[1].1)
            .json(&json!({ "name": "test-org-last-owner", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .delete(&format!("/admin/api/v1/organizations/{org_id}/members/{}", owner.id))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body = resp.text();
        assert!(body.contains("last owner"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_can_remove_owner_when_another_exists(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let admin_headers = add_auth_headers(&admin);

        let owner1 = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner1);
        let owner2 = create_test_user(&pool, Role::StandardUser).await;

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&admin_headers[0].0, &admin_headers[0].1)
            .add_header(&admin_headers[1].0, &admin_headers[1].1)
            .json(&json!({ "name": "test-org-two-owners", "email": "org@example.com", "owner_id": owner1.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "user_id": owner2.id, "role": "owner" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);

        let resp = server
            .delete(&format!("/admin/api/v1/organizations/{org_id}/members/{}", owner1.id))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::NO_CONTENT);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_cannot_demote_last_owner(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let admin = create_test_admin_user(&pool, Role::PlatformManager).await;
        let admin_headers = add_auth_headers(&admin);

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&admin_headers[0].0, &admin_headers[0].1)
            .add_header(&admin_headers[1].0, &admin_headers[1].1)
            .json(&json!({ "name": "test-org-demote", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}/members/{}", owner.id))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "role": "member" }))
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
        let body = resp.text();
        assert!(body.contains("last owner"));
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_admin_cannot_assign_owner_role(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);
        let org_admin = create_test_user(&pool, Role::StandardUser).await;
        let org_admin_headers = add_auth_headers(&org_admin);
        let member = create_test_user(&pool, Role::StandardUser).await;

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "name": "test-org-priv-esc", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "user_id": org_admin.id, "role": "admin" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "user_id": member.id, "role": "member" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}/members/{}", member.id))
            .add_header(&org_admin_headers[0].0, &org_admin_headers[0].1)
            .add_header(&org_admin_headers[1].0, &org_admin_headers[1].1)
            .json(&json!({ "role": "owner" }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_admin_cannot_add_member_as_owner(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);
        let org_admin = create_test_user(&pool, Role::StandardUser).await;
        let org_admin_headers = add_auth_headers(&org_admin);
        let new_user = create_test_user(&pool, Role::StandardUser).await;

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "name": "test-org-add-owner", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "user_id": org_admin.id, "role": "admin" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&org_admin_headers[0].0, &org_admin_headers[0].1)
            .add_header(&org_admin_headers[1].0, &org_admin_headers[1].1)
            .json(&json!({ "user_id": new_user.id, "role": "owner" }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_owner_can_assign_owner_role(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);
        let new_user = create_test_user(&pool, Role::StandardUser).await;

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "name": "test-org-owner-assign", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "user_id": new_user.id, "role": "owner" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let body = resp.json::<serde_json::Value>();
        assert_eq!(body["role"].as_str().unwrap(), "owner");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_cannot_assign_customer_org_owner_role(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;

        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let new_user = create_test_user(&pool, Role::StandardUser).await;

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "name": "test-org-pm-assign", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "user_id": new_user.id, "role": "owner" }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_set_active_organization_validates_membership(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);
        let non_member = create_test_user(&pool, Role::StandardUser).await;
        let non_member_headers = add_auth_headers(&non_member);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "name": "test-org-session", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post("/admin/api/v1/session/organization")
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "organization_id": org_id }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let body = resp.json::<serde_json::Value>();
        assert_eq!(body["active_organization_id"].as_str().unwrap(), org_id);

        let resp = server
            .post("/admin/api/v1/session/organization")
            .add_header(&non_member_headers[0].0, &non_member_headers[0].1)
            .add_header(&non_member_headers[1].0, &non_member_headers[1].1)
            .json(&json!({ "organization_id": org_id }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);

        let resp = server
            .post("/admin/api/v1/session/organization")
            .add_header(&non_member_headers[0].0, &non_member_headers[0].1)
            .add_header(&non_member_headers[1].0, &non_member_headers[1].1)
            .json(&json!({ "organization_id": null }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_set_active_org_cookie_includes_domain_when_configured(pool: PgPool) {
        let mut config = create_test_config();
        config.auth.native.session.cookie_domain = Some(".example.com".to_string());
        let (server, _bg) = create_test_app_with_config(pool.clone(), config, false).await;

        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "name": "domain-test-org", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post("/admin/api/v1/session/organization")
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "organization_id": org_id }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let cookie = resp.headers().get("set-cookie").unwrap().to_str().unwrap();
        assert!(cookie.contains("Domain=.example.com"), "set cookie should include Domain: {cookie}");

        let resp = server
            .post("/admin/api/v1/session/organization")
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "organization_id": null }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let cookie = resp.headers().get("set-cookie").unwrap().to_str().unwrap();
        assert!(
            cookie.contains("Domain=.example.com"),
            "clear cookie should include Domain: {cookie}"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_set_active_org_cookie_omits_domain_when_not_configured(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;

        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "name": "no-domain-org", "email": "org@example.com", "owner_id": owner.id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();

        let resp = server
            .post("/admin/api/v1/session/organization")
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "organization_id": org_id }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let cookie = resp.headers().get("set-cookie").unwrap().to_str().unwrap();
        assert!(!cookie.contains("Domain="), "cookie should not include Domain: {cookie}");
    }


    async fn create_org_for(
        server: &axum_test::TestServer,
        pm_headers: &[(String, String)],
        name: &str,
        email: &str,
        owner_id: crate::types::UserId,
    ) -> String {
        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "name": name, "email": email, "owner_id": owner_id }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string()
    }

    async fn plant_pending_email_change(
        pool: &PgPool,
        org_id: uuid::Uuid,
        new_email: &str,
        requested_by: crate::types::UserId,
        expires_at_relative_seconds: i64,
    ) -> (String, String) {
        let new_token = crate::auth::password::generate_reset_token();
        let old_token = crate::auth::password::generate_reset_token();
        let new_hash = super::hash_invite_token(&new_token);
        let old_hash = super::hash_invite_token(&old_token);
        sqlx::query(
            "INSERT INTO pending_org_email_changes
                (organization_id, new_email, requested_by,
                 new_email_token_hash, old_email_token_hash, expires_at)
             VALUES ($1, $2, $3, $4, $5, NOW() + ($6 || ' seconds')::interval)",
        )
        .bind(org_id)
        .bind(new_email)
        .bind(requested_by)
        .bind(&new_hash)
        .bind(&old_hash)
        .bind(expires_at_relative_seconds.to_string())
        .execute(pool)
        .await
        .unwrap();
        (new_token, old_token)
    }

    async fn click_confirm(server: &axum_test::TestServer, token: &str) -> axum_test::TestResponse {
        server
            .get(&format!("/admin/api/v1/organizations/email-change/{token}/confirm"))
            .await
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_patch_org_with_invalid_email_rejected(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "validate-email-org", "billing@example.com", owner.id).await;

        for bad in ["not-an-email", "missing-at.com", "  ", "spaces in@addr.com"] {
            let resp = server
                .patch(&format!("/admin/api/v1/organizations/{org_id}"))
                .add_header(&owner_headers[0].0, &owner_headers[0].1)
                .add_header(&owner_headers[1].0, &owner_headers[1].1)
                .json(&json!({ "email": bad }))
                .await;
            resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
        }

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let body = resp.json::<serde_json::Value>();
        assert_eq!(body["email"].as_str().unwrap(), "billing@example.com");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_patch_org_email_does_not_apply_immediately(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "verify-flow-org", "billing@example.com", owner.id).await;

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "email": "attacker@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let body = resp.json::<serde_json::Value>();
        assert_eq!(
            body["email"].as_str().unwrap(),
            "billing@example.com",
            "old email must remain until both sides confirm"
        );
        assert_eq!(body["pending_email_change"]["new_email"].as_str().unwrap(), "attacker@example.com");
        assert!(body["pending_email_change"]["expires_at"].is_string());
        assert!(body["pending_email_change"]["new_email_confirmed_at"].is_null());
        assert!(body["pending_email_change"]["old_email_confirmed_at"].is_null());

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let body = resp.json::<serde_json::Value>();
        assert_eq!(body["email"].as_str().unwrap(), "billing@example.com");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_patch_org_display_name_still_applies_when_email_pending(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "rename-org", "billing@example.com", owner.id).await;

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "display_name": "Renamed Org", "email": "new@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let body = resp.json::<serde_json::Value>();
        assert_eq!(body["display_name"].as_str().unwrap(), "Renamed Org");
        assert_eq!(body["email"].as_str().unwrap(), "billing@example.com");
        assert_eq!(body["pending_email_change"]["new_email"].as_str().unwrap(), "new@example.com");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_patch_org_with_same_email_is_noop(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "same-email-org", "billing@example.com", owner.id).await;

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "email": "Billing@Example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let body = resp.json::<serde_json::Value>();
        assert!(
            body.get("pending_email_change").is_none() || body["pending_email_change"].is_null(),
            "no pending change when email did not actually change: {body}"
        );

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_org_email_changes WHERE organization_id = $1")
            .bind(uuid::Uuid::parse_str(&org_id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_neither_side_alone_applies_email_change(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "single-side-org", "billing@example.com", owner.id).await;
        let org_uuid = uuid::Uuid::parse_str(&org_id).unwrap();

        let (new_token, _old_token) = plant_pending_email_change(&pool, org_uuid, "new@example.com", owner.id, 3600).await;

        let resp = click_confirm(&server, &new_token).await;
        resp.assert_status(axum::http::StatusCode::OK);
        let body = resp.text();
        assert!(
            body.contains("current contact"),
            "expected a 'waiting on the other side' message; got: {body}"
        );

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        assert_eq!(resp.json::<serde_json::Value>()["email"].as_str().unwrap(), "billing@example.com");

        let row: (Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
            "SELECT new_email_confirmed_at, old_email_confirmed_at FROM pending_org_email_changes WHERE organization_id = $1",
        )
        .bind(org_uuid)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(row.0.is_some(), "new side should be confirmed");
        assert!(row.1.is_none(), "old side should NOT be confirmed");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_both_sides_confirm_applies_change_in_any_order(pool: PgPool) {
        for &order in &[("new_first", true), ("old_first", false)] {
            let _ = order;
        }
        for &new_first in &[true, false] {
            let (server, _bg) = create_test_app(pool.clone(), false).await;
            let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
            let pm_headers = add_auth_headers(&pm);
            let owner = create_test_user(&pool, Role::StandardUser).await;
            let owner_headers = add_auth_headers(&owner);

            let label = if new_first { "new-first" } else { "old-first" };
            let org_id = create_org_for(&server, &pm_headers, &format!("apply-{label}-org"), "billing@example.com", owner.id).await;
            let org_uuid = uuid::Uuid::parse_str(&org_id).unwrap();

            let (new_token, old_token) = plant_pending_email_change(&pool, org_uuid, "new@example.com", owner.id, 3600).await;

            let (first, second) = if new_first {
                (&new_token, &old_token)
            } else {
                (&old_token, &new_token)
            };

            click_confirm(&server, first).await.assert_status(axum::http::StatusCode::OK);
            let resp = server
                .get(&format!("/admin/api/v1/organizations/{org_id}"))
                .add_header(&owner_headers[0].0, &owner_headers[0].1)
                .add_header(&owner_headers[1].0, &owner_headers[1].1)
                .await;
            assert_eq!(
                resp.json::<serde_json::Value>()["email"].as_str().unwrap(),
                "billing@example.com",
                "{label}: email must remain after only the first confirmation",
            );

            let resp = click_confirm(&server, second).await;
            resp.assert_status(axum::http::StatusCode::OK);
            assert!(
                resp.text().contains("contact email has been updated"),
                "{label}: expected applied-message on second click",
            );

            let resp = server
                .get(&format!("/admin/api/v1/organizations/{org_id}"))
                .add_header(&owner_headers[0].0, &owner_headers[0].1)
                .add_header(&owner_headers[1].0, &owner_headers[1].1)
                .await;
            assert_eq!(resp.json::<serde_json::Value>()["email"].as_str().unwrap(), "new@example.com");

            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_org_email_changes WHERE organization_id = $1")
                .bind(org_uuid)
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(count, 0, "{label}: pending row should be deleted once both sides confirmed");
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_confirm_email_change_invalid_token(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;

        let resp = click_confirm(&server, "not-a-real-token").await;
        resp.assert_status(axum::http::StatusCode::NOT_FOUND);
        assert!(resp.text().contains("invalid"));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_expired_token_returns_not_found(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "expired-org", "billing@example.com", owner.id).await;
        let org_uuid = uuid::Uuid::parse_str(&org_id).unwrap();

        let (new_token, old_token) = plant_pending_email_change(&pool, org_uuid, "new@example.com", owner.id, -3600).await;

        for token in [&new_token, &old_token] {
            let resp = click_confirm(&server, token).await;
            resp.assert_status(axum::http::StatusCode::NOT_FOUND);
        }

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        assert_eq!(resp.json::<serde_json::Value>()["email"].as_str().unwrap(), "billing@example.com");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_second_email_change_supersedes_first(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "supersede-org", "billing@example.com", owner.id).await;

        for new_email in ["one@example.com", "two@example.com"] {
            let resp = server
                .patch(&format!("/admin/api/v1/organizations/{org_id}"))
                .add_header(&owner_headers[0].0, &owner_headers[0].1)
                .add_header(&owner_headers[1].0, &owner_headers[1].1)
                .json(&json!({ "email": new_email }))
                .await;
            resp.assert_status(axum::http::StatusCode::OK);
        }

        let rows: Vec<(String, uuid::Uuid)> =
            sqlx::query_as("SELECT new_email, requested_by FROM pending_org_email_changes WHERE organization_id = $1")
                .bind(uuid::Uuid::parse_str(&org_id).unwrap())
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows, vec![("two@example.com".to_string(), owner.id)]);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_superseded_tokens_become_inert(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "stale-token-org", "billing@example.com", owner.id).await;
        let org_uuid = uuid::Uuid::parse_str(&org_id).unwrap();

        let (stale_new, stale_old) = plant_pending_email_change(&pool, org_uuid, "first@example.com", owner.id, 3600).await;

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "email": "second@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);

        for token in [&stale_new, &stale_old] {
            click_confirm(&server, token).await.assert_status(axum::http::StatusCode::NOT_FOUND);
        }

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        assert_eq!(resp.json::<serde_json::Value>()["email"].as_str().unwrap(), "billing@example.com");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_confirm_token_cannot_be_replayed(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;

        let org_id = create_org_for(&server, &pm_headers, "replay-org", "billing@example.com", owner.id).await;
        let org_uuid = uuid::Uuid::parse_str(&org_id).unwrap();

        let (new_token, _old_token) = plant_pending_email_change(&pool, org_uuid, "new@example.com", owner.id, 3600).await;

        click_confirm(&server, &new_token).await.assert_status(axum::http::StatusCode::OK);

        click_confirm(&server, &new_token)
            .await
            .assert_status(axum::http::StatusCode::NOT_FOUND);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_create_org_with_invalid_email_rejected(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let user = create_test_user(&pool, Role::StandardUser).await;
        let headers = add_auth_headers(&user);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&headers[0].0, &headers[0].1)
            .add_header(&headers[1].0, &headers[1].1)
            .json(&json!({ "name": "bad-email-org", "email": "not-an-email" }))
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_plain_member_cannot_patch_org_email(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);
        let plain_member = create_test_user(&pool, Role::StandardUser).await;
        let plain_member_headers = add_auth_headers(&plain_member);

        let org_id = create_org_for(&server, &pm_headers, "auth-member-org", "billing@example.com", owner.id).await;

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "user_id": plain_member.id, "role": "member" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&plain_member_headers[0].0, &plain_member_headers[0].1)
            .add_header(&plain_member_headers[1].0, &plain_member_headers[1].1)
            .json(&json!({ "email": "attacker@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_org_email_changes WHERE organization_id = $1")
            .bind(uuid::Uuid::parse_str(&org_id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_non_member_cannot_patch_org_email(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let outsider = create_test_user(&pool, Role::StandardUser).await;
        let outsider_headers = add_auth_headers(&outsider);

        let org_id = create_org_for(&server, &pm_headers, "auth-outsider-org", "billing@example.com", owner.id).await;

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&outsider_headers[0].0, &outsider_headers[0].1)
            .add_header(&outsider_headers[1].0, &outsider_headers[1].1)
            .json(&json!({ "email": "attacker@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_org_email_changes WHERE organization_id = $1")
            .bind(uuid::Uuid::parse_str(&org_id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_platform_manager_can_patch_org_email_without_membership(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;

        let org_id = create_org_for(&server, &pm_headers, "auth-pm-org", "billing@example.com", owner.id).await;

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&pm_headers[0].0, &pm_headers[0].1)
            .add_header(&pm_headers[1].0, &pm_headers[1].1)
            .json(&json!({ "email": "new-contact@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::FORBIDDEN);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_token_for_one_org_cannot_affect_another(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner_a = create_test_user(&pool, Role::StandardUser).await;
        let owner_a_headers = add_auth_headers(&owner_a);
        let owner_b = create_test_user(&pool, Role::StandardUser).await;
        let owner_b_headers = add_auth_headers(&owner_b);

        let org_a_id = create_org_for(&server, &pm_headers, "tenancy-org-a", "a@example.com", owner_a.id).await;
        let org_b_id = create_org_for(&server, &pm_headers, "tenancy-org-b", "b@example.com", owner_b.id).await;
        let org_a_uuid = uuid::Uuid::parse_str(&org_a_id).unwrap();

        let (new_a, old_a) = plant_pending_email_change(&pool, org_a_uuid, "new-a@example.com", owner_a.id, 3600).await;

        click_confirm(&server, &new_a).await.assert_status(axum::http::StatusCode::OK);
        click_confirm(&server, &old_a).await.assert_status(axum::http::StatusCode::OK);

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_a_id}"))
            .add_header(&owner_a_headers[0].0, &owner_a_headers[0].1)
            .add_header(&owner_a_headers[1].0, &owner_a_headers[1].1)
            .await;
        assert_eq!(resp.json::<serde_json::Value>()["email"].as_str().unwrap(), "new-a@example.com");

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_b_id}"))
            .add_header(&owner_b_headers[0].0, &owner_b_headers[0].1)
            .add_header(&owner_b_headers[1].0, &owner_b_headers[1].1)
            .await;
        assert_eq!(resp.json::<serde_json::Value>()["email"].as_str().unwrap(), "b@example.com");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_soft_deleted_org_cannot_have_email_changed_via_token(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;

        let org_id = create_org_for(&server, &pm_headers, "soft-delete-org", "billing@example.com", owner.id).await;
        let org_uuid = uuid::Uuid::parse_str(&org_id).unwrap();

        let (new_token, old_token) = plant_pending_email_change(&pool, org_uuid, "new@example.com", owner.id, 3600).await;

        sqlx::query("UPDATE users SET is_deleted = true WHERE id = $1")
            .bind(org_uuid)
            .execute(&pool)
            .await
            .unwrap();

        for token in [&new_token, &old_token] {
            click_confirm(&server, token).await.assert_status(axum::http::StatusCode::NOT_FOUND);
        }

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_org_email_changes WHERE organization_id = $1")
            .bind(org_uuid)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_patch_writes_both_verification_emails(pool: PgPool) {
        let scratch = std::env::temp_dir().join(format!("dwctl-test-emails-patch-{}-{}", std::process::id(), uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&scratch).unwrap();

        let mut config = create_test_config();
        config.email.transport = crate::config::EmailTransportConfig::File {
            path: scratch.to_string_lossy().to_string(),
        };

        let (server, _bg) = create_test_app_with_config(pool.clone(), config, false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "email-send-org", "current@example.com", owner.id).await;

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "email": "new@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);

        let mut to_addresses: Vec<String> = Vec::new();
        let mut bodies: Vec<String> = Vec::new();
        for entry in std::fs::read_dir(&scratch).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|s| s.to_str()) == Some("eml") {
                let body = std::fs::read_to_string(&path).unwrap();
                for line in body.lines() {
                    if let Some(rest) = line.strip_prefix("To: ") {
                        to_addresses.push(rest.trim().to_string());
                    }
                }
                bodies.push(body);
            }
        }
        to_addresses.sort();
        assert_eq!(
            to_addresses,
            vec!["current@example.com".to_string(), "new@example.com".to_string()],
            "expected verification email to both addresses; got {to_addresses:?}",
        );
        let confirm_link_count = bodies
            .iter()
            .filter(|b| b.contains("/admin/api/v1/organizations/email-change/"))
            .count();
        assert_eq!(confirm_link_count, 2, "both verification emails must carry a confirmation link");
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_mx_check_rejects_undeliverable_domain(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "mx-check-org", "billing@example.com", owner.id).await;

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "email": "bounce@no-such-host-please.invalid" }))
            .await;
        resp.assert_status(axum::http::StatusCode::BAD_REQUEST);

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pending_org_email_changes WHERE organization_id = $1")
            .bind(uuid::Uuid::parse_str(&org_id).unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_get_org_surfaces_pending_email_change(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "pending-visible-org", "billing@example.com", owner.id).await;

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "email": "next@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let body = resp.json::<serde_json::Value>();
        assert_eq!(body["email"].as_str().unwrap(), "billing@example.com");
        assert_eq!(
            body["pending_email_change"]["new_email"].as_str().unwrap(),
            "next@example.com",
            "GET should surface in-flight pending change; got: {body}",
        );
        assert!(body["pending_email_change"]["expires_at"].is_string());
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_patch_other_fields_still_reports_pending_email_change(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let pm = create_test_admin_user(&pool, Role::PlatformManager).await;
        let pm_headers = add_auth_headers(&pm);
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let org_id = create_org_for(&server, &pm_headers, "pending-survives-org", "billing@example.com", owner.id).await;

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "email": "next@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "display_name": "Renamed Org" }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let body = resp.json::<serde_json::Value>();
        assert_eq!(body["display_name"].as_str().unwrap(), "Renamed Org");
        assert_eq!(
            body["pending_email_change"]["new_email"].as_str().unwrap(),
            "next@example.com",
            "non-email PATCH must still surface the in-flight change; got: {body}",
        );
    }


    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_member_manage_keys_grant_lifecycle(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let defaulted = create_test_user(&pool, Role::StandardUser).await;
        let restricted = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let resp = server
            .post("/admin/api/v1/organizations")
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "name": "keyrole-org", "email": "contact@example.com" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let org_id = resp.json::<serde_json::Value>()["id"].as_str().unwrap().to_string();
        let org_uuid: uuid::Uuid = org_id.parse().unwrap();
        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "user_id": restricted.id, "role": "member", "can_manage_keys": false }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        assert_eq!(resp.json::<serde_json::Value>()["can_manage_keys"].as_bool(), Some(false));

        let resp = server
            .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "user_id": defaulted.id, "role": "member" }))
            .await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        assert_eq!(resp.json::<serde_json::Value>()["can_manage_keys"].as_bool(), Some(true));

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}/members"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        let members = resp.json::<Vec<serde_json::Value>>();
        let by_user = |uid: &str| {
            members
                .iter()
                .find(|m| m["user"]["id"].as_str() == Some(uid))
                .unwrap_or_else(|| panic!("member {uid} missing"))
        };
        assert_eq!(by_user(&owner.id.to_string())["can_manage_keys"].as_bool(), Some(true));
        assert_eq!(by_user(&defaulted.id.to_string())["can_manage_keys"].as_bool(), Some(true));
        assert_eq!(by_user(&restricted.id.to_string())["can_manage_keys"].as_bool(), Some(false));

        let restricted_headers = add_auth_headers(&restricted);
        server
            .post(&format!("/admin/api/v1/users/{org_uuid}/api-keys"))
            .add_header(&restricted_headers[0].0, &restricted_headers[0].1)
            .add_header(&restricted_headers[1].0, &restricted_headers[1].1)
            .json(&json!({ "name": "Nope", "purpose": "realtime" }))
            .await
            .assert_status_forbidden();

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}/members/{}", restricted.id))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "role": "member", "can_manage_keys": true }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        assert_eq!(resp.json::<serde_json::Value>()["can_manage_keys"].as_bool(), Some(true));

        server
            .post(&format!("/admin/api/v1/users/{org_uuid}/api-keys"))
            .add_header(&restricted_headers[0].0, &restricted_headers[0].1)
            .add_header(&restricted_headers[1].0, &restricted_headers[1].1)
            .json(&json!({ "name": "Now allowed", "purpose": "realtime" }))
            .await
            .assert_status(axum::http::StatusCode::CREATED);

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}/members/{}", restricted.id))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "role": "member", "can_manage_keys": false }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        assert_eq!(resp.json::<serde_json::Value>()["can_manage_keys"].as_bool(), Some(false));

        let resp = server
            .patch(&format!("/admin/api/v1/organizations/{org_id}/members/{}", restricted.id))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "role": "admin" }))
            .await;
        resp.assert_status(axum::http::StatusCode::OK);
        assert_eq!(resp.json::<serde_json::Value>()["can_manage_keys"].as_bool(), Some(true));
    }


    async fn membership_has_manage_keys(pool: &PgPool, org_id: uuid::Uuid, user_id: uuid::Uuid) -> bool {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM organization_member_roles omr \
             JOIN user_organizations uo ON uo.id = omr.user_organization_id \
             WHERE uo.organization_id = $1 AND uo.user_id = $2 AND omr.role = 'manage_keys')",
        )
        .bind(org_id)
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn set_members_can_create_keys(pool: &PgPool, org_id: uuid::Uuid, enabled: bool) {
        let mut conn = pool.acquire().await.unwrap();
        assert!(
            crate::db::handlers::Organizations::new(&mut conn)
                .set_members_can_create_keys(org_id, enabled)
                .await
                .unwrap()
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_new_members_can_create_keys_by_default_on_every_join_path(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;

        let owner = create_test_user(&pool, Role::StandardUser).await;
        let requester = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, request_id) = org_with_join_request(&pool, owner.id, requester.id).await;
        let owner_headers = add_auth_headers(&owner);
        assert!(
            !membership_has_manage_keys(&pool, org_id, requester.id).await,
            "nothing is granted before approval"
        );
        server
            .post(&format!("/admin/api/v1/organizations/{org_id}/join-requests/{request_id}/approve"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await
            .assert_status(axum::http::StatusCode::NO_CONTENT);
        assert!(membership_has_manage_keys(&pool, org_id, requester.id).await);

        let invitee = create_test_user(&pool, Role::StandardUser).await;
        server
            .post(&format!("/admin/api/v1/organizations/{org_id}/invites"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "email": invitee.email, "role": "member" }))
            .await
            .assert_status(axum::http::StatusCode::CREATED);
        assert!(membership_has_manage_keys(&pool, org_id, invitee.id).await);

        let domain_owner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        let domain_org_id = org_claiming_domain(&pool, domain_owner.id, "acme.test").await;
        let joiner = create_test_user_on_domain(&pool, Role::StandardUser, "acme.test").await;
        set_auto_join(&pool, domain_org_id, true).await;
        let joiner_headers = add_auth_headers(&joiner);
        server
            .post("/admin/api/v1/users/current/join-requests")
            .add_header(&joiner_headers[0].0, &joiner_headers[0].1)
            .add_header(&joiner_headers[1].0, &joiner_headers[1].1)
            .await
            .assert_status_ok();
        assert!(membership_has_manage_keys(&pool, domain_org_id, joiner.id).await);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_default_key_role_is_not_written_for_an_approved_admin(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let requester = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, request_id) = org_with_join_request(&pool, owner.id, requester.id).await;
        let owner_headers = add_auth_headers(&owner);

        server
            .post(&format!("/admin/api/v1/organizations/{org_id}/join-requests/{request_id}/approve"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .json(&json!({ "role": "admin" }))
            .await
            .assert_status(axum::http::StatusCode::NO_CONTENT);
        assert!(!membership_has_manage_keys(&pool, org_id, requester.id).await);
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_members_can_create_keys_off_applies_to_later_joiners_only(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let earlier = create_test_user(&pool, Role::StandardUser).await;
        let requester = create_test_user(&pool, Role::StandardUser).await;
        let added = create_test_user(&pool, Role::StandardUser).await;
        let opted_in = create_test_user(&pool, Role::StandardUser).await;
        let (org_id, request_id) = org_with_join_request(&pool, owner.id, requester.id).await;
        let owner_headers = add_auth_headers(&owner);
        let add_member = |body: serde_json::Value| {
            server
                .post(&format!("/admin/api/v1/organizations/{org_id}/members"))
                .add_header(&owner_headers[0].0, &owner_headers[0].1)
                .add_header(&owner_headers[1].0, &owner_headers[1].1)
                .json(&body)
        };

        add_member(json!({ "user_id": earlier.id, "role": "member" }))
            .await
            .assert_status(axum::http::StatusCode::CREATED);
        assert!(membership_has_manage_keys(&pool, org_id, earlier.id).await);

        set_members_can_create_keys(&pool, org_id, false).await;
        assert!(
            membership_has_manage_keys(&pool, org_id, earlier.id).await,
            "the setting is a default for new memberships, not a revocation"
        );

        server
            .post(&format!("/admin/api/v1/organizations/{org_id}/join-requests/{request_id}/approve"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await
            .assert_status(axum::http::StatusCode::NO_CONTENT);
        assert!(!membership_has_manage_keys(&pool, org_id, requester.id).await);

        let resp = add_member(json!({ "user_id": added.id, "role": "member" })).await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        assert_eq!(resp.json::<serde_json::Value>()["can_manage_keys"].as_bool(), Some(false));

        let resp = add_member(json!({ "user_id": opted_in.id, "role": "member", "can_manage_keys": true })).await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        assert_eq!(resp.json::<serde_json::Value>()["can_manage_keys"].as_bool(), Some(true));
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_members_can_create_keys_setting_round_trips(pool: PgPool) {
        let (server, _bg) = create_test_app(pool.clone(), false).await;
        let owner = create_test_user(&pool, Role::StandardUser).await;
        let admin = create_test_user(&pool, Role::StandardUser).await;
        let member = create_test_user(&pool, Role::StandardUser).await;
        let owner_headers = add_auth_headers(&owner);

        let create = |name: &'static str, extra: serde_json::Value| {
            let mut body = json!({ "name": name, "email": owner.email });
            body.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
            server
                .post("/admin/api/v1/organizations")
                .add_header(&owner_headers[0].0, &owner_headers[0].1)
                .add_header(&owner_headers[1].0, &owner_headers[1].1)
                .json(&body)
        };

        let resp = create("default-keys-org", json!({})).await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        assert_eq!(resp.json::<serde_json::Value>()["members_can_create_keys"].as_bool(), Some(true));

        let resp = create("restricted-keys-org", json!({ "members_can_create_keys": false })).await;
        resp.assert_status(axum::http::StatusCode::CREATED);
        let body = resp.json::<serde_json::Value>();
        assert_eq!(body["members_can_create_keys"].as_bool(), Some(false));
        let org_id: uuid::Uuid = body["id"].as_str().unwrap().parse().unwrap();

        crate::test::utils::add_org_member(&pool, org_id, admin.id, "admin").await;
        crate::test::utils::add_org_member(&pool, org_id, member.id, "member").await;
        let patch = |who: &crate::api::models::users::UserResponse, enabled: bool| {
            let headers = add_auth_headers(who);
            server
                .patch(&format!("/admin/api/v1/organizations/{org_id}"))
                .add_header(&headers[0].0, &headers[0].1)
                .add_header(&headers[1].0, &headers[1].1)
                .json(&json!({ "members_can_create_keys": enabled }))
        };

        patch(&member, true).await.assert_status(axum::http::StatusCode::FORBIDDEN);

        let resp = patch(&admin, true).await;
        resp.assert_status_ok();
        assert_eq!(resp.json::<serde_json::Value>()["members_can_create_keys"].as_bool(), Some(true));

        let resp = server
            .get(&format!("/admin/api/v1/organizations/{org_id}"))
            .add_header(&owner_headers[0].0, &owner_headers[0].1)
            .add_header(&owner_headers[1].0, &owner_headers[1].1)
            .await;
        resp.assert_status_ok();
        assert_eq!(resp.json::<serde_json::Value>()["members_can_create_keys"].as_bool(), Some(true));
    }
}
