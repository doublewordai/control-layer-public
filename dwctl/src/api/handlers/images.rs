
use crate::AppState;
use crate::api::models::users::CurrentUser;
use crate::crypto::hash_api_key;
use crate::errors::{Error, Result};
use crate::image_normalizer::{ImageToken, TokenParseError};
use axum::{
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header::LOCATION},
    response::{IntoResponse, Response},
};
use sqlx_pool_router::PoolProvider;
use std::time::Duration;
use tracing::warn;

#[tracing::instrument(skip_all)]
pub async fn get_image<P: PoolProvider + Clone + Send + Sync>(
    State(state): State<AppState<P>>,
    current_user: CurrentUser,
    Path(sha256_hex): Path<String>,
) -> Result<Response> {
    let token: ImageToken = sha256_hex.parse().map_err(|e: TokenParseError| Error::BadRequest {
        message: format!("invalid image hash: {e}"),
    })?;

    let config = state.current_config();
    if !config.image_normalizer.enabled {
        return Err(Error::NotFound {
            resource: "image".to_string(),
            id: sha256_hex,
        });
    }

    let sha_bytes: Vec<u8> = token.0.to_vec();
    let mut conn = state.db.write().acquire().await.map_err(|e| Error::Database(e.into()))?;
    let authorized = is_authorized_to_view(&mut conn, &sha_bytes, current_user.id, current_user.active_organization)
        .await
        .map_err(|e: sqlx::Error| Error::Database(e.into()))?;
    drop(conn);

    if !authorized {
        return Err(Error::NotFound {
            resource: "image".to_string(),
            id: sha256_hex,
        });
    }

    let ttl = Duration::from_secs(config.image_normalizer.signing.dashboard_ttl_secs);
    let signed = state.image_normalizer.sign(token, ttl).await.map_err(|e| match e {
        crate::image_normalizer::NormalizeError::NotFound => Error::NotFound {
            resource: "image".to_string(),
            id: sha256_hex.clone(),
        },
        e => {
            warn!(error = %e, "image_normalizer.sign failed for dashboard view");
            Error::Internal {
                operation: format!("image signing failed: {e}"),
            }
        }
    })?;

    let mut response = (StatusCode::FOUND, "").into_response();
    response.headers_mut().insert(
        LOCATION,
        HeaderValue::from_str(&signed.url).map_err(|e| Error::Internal {
            operation: format!("invalid signed URL: {e}"),
        })?,
    );
    response
        .headers_mut()
        .insert(axum::http::header::CACHE_CONTROL, HeaderValue::from_static("no-store, private"));
    Ok(response)
}

#[derive(Debug, Clone, Copy)]
pub struct ImageAttribution {
    pub user_id: uuid::Uuid,
    pub organization_id: Option<uuid::Uuid>,
}

pub async fn resolve_image_attribution(pool: &sqlx::PgPool, api_key: &str) -> Option<ImageAttribution> {
    try_resolve_caller(pool, &hash_api_key(api_key))
        .await
        .ok()
        .flatten()
        .map(|c| c.attribution)
}

#[derive(Debug, Clone, Copy)]
pub struct ResolvedCaller {
    pub attribution: ImageAttribution,
    pub is_daemon_dispatch: bool,
}

pub async fn try_resolve_caller(
    pool: &sqlx::PgPool,
    api_key: &crate::crypto::ApiKeyHash,
) -> std::result::Result<Option<ResolvedCaller>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT
            created_by AS "created_by!",
            user_id AS "user_id!",
            (purpose = 'batch' AND hidden) AS "is_daemon_dispatch!"
        FROM api_keys
        WHERE secret_hash = $1 AND is_deleted = FALSE
        LIMIT 1
        "#,
        &api_key[..],
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|row| {
        let organization_id = (row.created_by != row.user_id).then_some(row.user_id);
        ResolvedCaller {
            attribution: ImageAttribution {
                user_id: row.user_id,
                organization_id,
            },
            is_daemon_dispatch: row.is_daemon_dispatch,
        }
    }))
}

pub async fn record_image_access(pool: &sqlx::PgPool, attribution: ImageAttribution, token: ImageToken, mime: &str, bytes_len: u64) {
    if let Err(e) = try_record_image_access(pool, attribution, token, mime, bytes_len).await {
        warn!(error = %e, "failed to record image_access row (non-fatal)");
    }
}

pub async fn try_record_image_access(
    pool: &sqlx::PgPool,
    attribution: ImageAttribution,
    token: ImageToken,
    mime: &str,
    bytes_len: u64,
) -> std::result::Result<(), sqlx::Error> {
    let sha_bytes: Vec<u8> = token.0.to_vec();
    let bytes_len_i64 = bytes_len as i64;
    sqlx::query!(
        r#"
        INSERT INTO image_access (user_id, organization_id, sha256, mime, bytes_len, first_seen_at, last_seen_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
        ON CONFLICT (user_id, sha256) DO UPDATE
        SET last_seen_at = NOW(),
            mime = EXCLUDED.mime,
            bytes_len = EXCLUDED.bytes_len,
            organization_id = COALESCE(EXCLUDED.organization_id, image_access.organization_id)
        "#,
        attribution.user_id,
        attribution.organization_id,
        sha_bytes,
        mime,
        bytes_len_i64,
    )
    .execute(pool)
    .await
    .map(|_| ())
}

pub async fn is_token_accessible(
    pool: &sqlx::PgPool,
    attribution: &ImageAttribution,
    token: ImageToken,
) -> std::result::Result<bool, sqlx::Error> {
    Ok(accessible_tokens(pool, attribution, &[token]).await?.contains(&token))
}

pub async fn accessible_tokens(
    pool: &sqlx::PgPool,
    attribution: &ImageAttribution,
    tokens: &[ImageToken],
) -> std::result::Result<std::collections::HashSet<ImageToken>, sqlx::Error> {
    if tokens.is_empty() {
        return Ok(std::collections::HashSet::new());
    }
    let hashes: Vec<Vec<u8>> = tokens.iter().map(|t| t.0.to_vec()).collect();
    let rows = sqlx::query!(
        r#"
        SELECT DISTINCT sha256
        FROM image_access
        WHERE sha256 = ANY($1)
          AND (user_id = $2 OR organization_id = $3)
        "#,
        &hashes,
        attribution.user_id,
        attribution.organization_id,
    )
    .fetch_all(pool)
    .await?;
    let granted: std::collections::HashSet<[u8; 32]> = rows
        .into_iter()
        .filter_map(|row| <[u8; 32]>::try_from(row.sha256.as_slice()).ok())
        .collect();
    Ok(tokens.iter().filter(|t| granted.contains(&t.0)).copied().collect())
}

async fn is_authorized_to_view(
    conn: &mut sqlx::PgConnection,
    sha256: &[u8],
    viewer: uuid::Uuid,
    active_org: Option<uuid::Uuid>,
) -> std::result::Result<bool, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT 1 AS "exists!"
        FROM image_access
        WHERE sha256 = $1
          AND (user_id = $2 OR organization_id = $3)
        LIMIT 1
        "#,
        sha256,
        viewer,
        active_org,
    )
    .fetch_optional(conn)
    .await?;
    Ok(row.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::api_keys::ApiKeyCreate;
    use crate::api::models::users::Role;
    use crate::db::handlers::Organizations;
    use crate::db::handlers::api_keys::ApiKeys;
    use crate::db::handlers::repository::Repository;
    use crate::db::models::api_keys::{ApiKeyCreateDBRequest, ApiKeyPurpose};
    use crate::test::utils::{create_test_api_key_for_user, create_test_org, create_test_user};
    use sqlx::PgPool;

    fn token(b: u8) -> ImageToken {
        ImageToken([b; 32], None)
    }

    async fn can_view(pool: &PgPool, tok: ImageToken, viewer: uuid::Uuid, active_org: Option<uuid::Uuid>) -> bool {
        let mut conn = pool.acquire().await.unwrap();
        is_authorized_to_view(&mut conn, &tok.0.to_vec(), viewer, active_org).await.unwrap()
    }

    async fn create_org_api_key(pool: &PgPool, org_id: uuid::Uuid, member_id: uuid::Uuid) -> String {
        let mut conn = pool.acquire().await.unwrap();
        let create = ApiKeyCreate {
            name: format!("Org key {}", uuid::Uuid::new_v4().simple()),
            description: None,
            purpose: ApiKeyPurpose::Realtime,
            member_id: None,
            spend_limit: None,
            spend_limit_interval: None,
        };
        let req = ApiKeyCreateDBRequest::new(org_id, member_id, create);
        ApiKeys::new(&mut conn).create(&req).await.unwrap().secret
    }

    #[dwctl_test_macros::test]
    async fn the_same_image_submitted_under_two_organizations_grants_both(pool: PgPool) {
        let alice = create_test_user(&pool, Role::StandardUser).await;
        let bob = create_test_user(&pool, Role::StandardUser).await;
        let carol = create_test_user(&pool, Role::StandardUser).await;
        let dave = create_test_user(&pool, Role::StandardUser).await;
        let org_a = create_test_org(&pool, alice.id).await;
        let org_b = create_test_org(&pool, alice.id).await;
        {
            let mut conn = pool.acquire().await.unwrap();
            Organizations::new(&mut conn).add_member(org_a.id, bob.id, "member").await.unwrap();
            Organizations::new(&mut conn)
                .add_member(org_b.id, carol.id, "member")
                .await
                .unwrap();
        }
        let img = token(7);

        let as_org = |org: uuid::Uuid| ImageAttribution {
            user_id: org,
            organization_id: Some(org),
        };
        let personally = |user: uuid::Uuid| ImageAttribution {
            user_id: user,
            organization_id: None,
        };

        for attribution in [as_org(org_a.id), as_org(org_b.id), personally(alice.id)] {
            record_image_access(&pool, attribution, img, "image/png", 10).await;
        }

        assert!(
            is_token_accessible(&pool, &as_org(org_a.id), img).await.unwrap(),
            "org A (bob's key)"
        );
        assert!(
            is_token_accessible(&pool, &as_org(org_b.id), img).await.unwrap(),
            "org B (carol's key)"
        );
        assert!(
            is_token_accessible(&pool, &personally(alice.id), img).await.unwrap(),
            "alice, personally"
        );
        assert!(!is_token_accessible(&pool, &personally(dave.id), img).await.unwrap(), "a stranger");
        assert!(can_view(&pool, img, bob.id, Some(org_a.id)).await, "bob in org A");
        assert!(can_view(&pool, img, carol.id, Some(org_b.id)).await, "carol in org B");
        assert!(!can_view(&pool, img, dave.id, None).await, "dave");
    }

    #[dwctl_test_macros::test]
    async fn personal_image_is_private_org_image_is_org_visible(pool: PgPool) {
        let p = create_test_user(&pool, Role::StandardUser).await;
        let q = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, p.id).await;
        {
            let mut conn = pool.acquire().await.unwrap();
            Organizations::new(&mut conn).add_member(org.id, q.id, "member").await.unwrap();
        }

        let personal = token(1);
        let org_img = token(2);
        record_image_access(
            &pool,
            ImageAttribution {
                user_id: p.id,
                organization_id: None,
            },
            personal,
            "image/png",
            10,
        )
        .await;
        record_image_access(
            &pool,
            ImageAttribution {
                user_id: q.id,
                organization_id: Some(org.id),
            },
            org_img,
            "image/png",
            20,
        )
        .await;

        assert!(can_view(&pool, personal, p.id, None).await, "P views own personal image");
        assert!(
            can_view(&pool, personal, p.id, Some(org.id)).await,
            "P views own personal even in org context"
        );
        assert!(!can_view(&pool, personal, q.id, None).await, "Q cannot view P's personal image");
        assert!(
            !can_view(&pool, personal, q.id, Some(org.id)).await,
            "a personal image must never be visible via the org"
        );

        assert!(
            can_view(&pool, org_img, q.id, None).await,
            "submitter sees own org image without org context too"
        );
        assert!(
            can_view(&pool, org_img, q.id, Some(org.id)).await,
            "submitter sees own org image in org context"
        );
        assert!(
            can_view(&pool, org_img, p.id, Some(org.id)).await,
            "another org member (in org context) sees it"
        );
        assert!(
            !can_view(&pool, org_img, p.id, None).await,
            "org member WITHOUT org context does not see it"
        );
        let outsider = create_test_user(&pool, Role::StandardUser).await;
        assert!(
            !can_view(&pool, org_img, outsider.id, None).await,
            "a non-member never sees the org image"
        );
    }

    #[dwctl_test_macros::test]
    async fn resolve_attribution_distinguishes_personal_and_org_keys(pool: PgPool) {
        let person = create_test_user(&pool, Role::StandardUser).await;
        let personal_key = create_test_api_key_for_user(&pool, person.id).await;
        let attr = resolve_image_attribution(&pool, &personal_key.secret).await.expect("known key");
        assert_eq!(attr.user_id, person.id);
        assert_eq!(attr.organization_id, None);

        let member = create_test_user(&pool, Role::StandardUser).await;
        let org = create_test_org(&pool, member.id).await;
        let org_secret = create_org_api_key(&pool, org.id, member.id).await;
        let attr = resolve_image_attribution(&pool, &org_secret).await.expect("known org key");
        assert_eq!(attr.user_id, org.id, "the principal is the key owner (user_id): the org");
        assert_eq!(attr.organization_id, Some(org.id), "repeated for the console's org branch");

        assert!(resolve_image_attribution(&pool, "sk-does-not-exist").await.is_none());
    }
}
