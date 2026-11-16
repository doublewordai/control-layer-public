
use crate::{
    AppState,
    api::models::users::{AuthMethod, CurrentUser, Role},
    errors::Error,
    types::{Operation, Resource, UserId},
};
use axum::{extract::FromRequestParts, http::request::Parts};
use std::marker::PhantomData;

pub mod resource {
    use crate::types::Resource;

    #[derive(Default)]
    pub struct Users;

    #[derive(Default)]
    pub struct Groups;

    #[derive(Default)]
    pub struct Models;

    #[derive(Default)]
    pub struct Endpoints;

    #[derive(Default)]
    pub struct ApiKeys;

    #[derive(Default)]
    pub struct Analytics;

    #[derive(Default)]
    pub struct Requests;

    #[derive(Default)]
    pub struct Pricing;

    #[derive(Default)]
    pub struct ModelRateLimits;

    #[derive(Default)]
    pub struct Files;

    #[derive(Default)]
    pub struct Credits;

    #[derive(Default)]
    pub struct Probes;

    #[derive(Default)]
    pub struct Batches;

    #[derive(Default)]
    pub struct CompositeModels;

    #[derive(Default)]
    pub struct System;

    #[derive(Default)]
    pub struct Webhooks;

    #[derive(Default)]
    pub struct Connections;

    impl From<Users> for Resource {
        fn from(_: Users) -> Resource {
            Resource::Users
        }
    }
    impl From<Groups> for Resource {
        fn from(_: Groups) -> Resource {
            Resource::Groups
        }
    }
    impl From<Models> for Resource {
        fn from(_: Models) -> Resource {
            Resource::Models
        }
    }
    impl From<Endpoints> for Resource {
        fn from(_: Endpoints) -> Resource {
            Resource::Endpoints
        }
    }
    impl From<ApiKeys> for Resource {
        fn from(_: ApiKeys) -> Resource {
            Resource::ApiKeys
        }
    }
    impl From<Analytics> for Resource {
        fn from(_: Analytics) -> Resource {
            Resource::Analytics
        }
    }
    impl From<Requests> for Resource {
        fn from(_: Requests) -> Resource {
            Resource::Requests
        }
    }
    impl From<Pricing> for Resource {
        fn from(_: Pricing) -> Resource {
            Resource::Pricing
        }
    }
    impl From<ModelRateLimits> for Resource {
        fn from(_: ModelRateLimits) -> Resource {
            Resource::ModelRateLimits
        }
    }
    impl From<Files> for Resource {
        fn from(_: Files) -> Resource {
            Resource::Files
        }
    }
    impl From<Credits> for Resource {
        fn from(_: Credits) -> Resource {
            Resource::Credits
        }
    }
    impl From<Probes> for Resource {
        fn from(_: Probes) -> Resource {
            Resource::Probes
        }
    }
    impl From<Batches> for Resource {
        fn from(_: Batches) -> Resource {
            Resource::Batches
        }
    }
    impl From<CompositeModels> for Resource {
        fn from(_: CompositeModels) -> Resource {
            Resource::CompositeModels
        }
    }
    impl From<System> for Resource {
        fn from(_: System) -> Resource {
            Resource::System
        }
    }
    impl From<Webhooks> for Resource {
        fn from(_: Webhooks) -> Resource {
            Resource::Webhooks
        }
    }

    #[derive(Default)]
    pub struct Organizations;

    impl From<Organizations> for Resource {
        fn from(_: Organizations) -> Resource {
            Resource::Organizations
        }
    }

    impl From<Connections> for Resource {
        fn from(_: Connections) -> Resource {
            Resource::Connections
        }
    }
}

pub mod operation {
    use crate::types::Operation;

    #[derive(Default)]
    pub struct CreateAll;

    #[derive(Default)]
    pub struct CreateOwn;

    #[derive(Default)]
    pub struct ReadAll;

    #[derive(Default)]
    pub struct ReadOwn;

    #[derive(Default)]
    pub struct UpdateAll;

    #[derive(Default)]
    pub struct UpdateOwn;

    #[derive(Default)]
    pub struct DeleteAll;

    #[derive(Default)]
    pub struct DeleteOwn;

    #[derive(Default)]
    pub struct SystemAccess;

    impl From<CreateAll> for Operation {
        fn from(_: CreateAll) -> Operation {
            Operation::CreateAll
        }
    }
    impl From<CreateOwn> for Operation {
        fn from(_: CreateOwn) -> Operation {
            Operation::CreateOwn
        }
    }
    impl From<ReadAll> for Operation {
        fn from(_: ReadAll) -> Operation {
            Operation::ReadAll
        }
    }
    impl From<ReadOwn> for Operation {
        fn from(_: ReadOwn) -> Operation {
            Operation::ReadOwn
        }
    }
    impl From<UpdateAll> for Operation {
        fn from(_: UpdateAll) -> Operation {
            Operation::UpdateAll
        }
    }
    impl From<UpdateOwn> for Operation {
        fn from(_: UpdateOwn) -> Operation {
            Operation::UpdateOwn
        }
    }
    impl From<DeleteAll> for Operation {
        fn from(_: DeleteAll) -> Operation {
            Operation::DeleteAll
        }
    }
    impl From<DeleteOwn> for Operation {
        fn from(_: DeleteOwn) -> Operation {
            Operation::DeleteOwn
        }
    }
    impl From<SystemAccess> for Operation {
        fn from(_: SystemAccess) -> Operation {
            Operation::SystemAccess
        }
    }
}

pub struct RequiresPermission<R, O>
where
    R: Into<Resource> + Default,
    O: Into<Operation> + Default,
{
    pub current_user: CurrentUser,
    _marker: PhantomData<(R, O)>,
}

impl<R, O> FromRequestParts<AppState> for RequiresPermission<R, O>
where
    R: Into<Resource> + Default,
    O: Into<Operation> + Default,
{
    type Rejection = Error;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let current_user = CurrentUser::from_request_parts(parts, state).await?;

        let resource = R::default().into();
        let operation = O::default().into();

        if has_permission(&current_user, resource, operation) {
            Ok(RequiresPermission {
                current_user,
                _marker: PhantomData,
            })
        } else {
            Err(Error::InsufficientPermissions {
                required: crate::types::Permission::Allow(resource, operation),
                action: operation,
                resource: format!("{resource:?}"),
            })
        }
    }
}

impl<R, O> std::ops::Deref for RequiresPermission<R, O>
where
    R: Into<Resource> + Default,
    O: Into<Operation> + Default,
{
    type Target = CurrentUser;

    fn deref(&self) -> &Self::Target {
        &self.current_user
    }
}

pub fn has_permission(user: &CurrentUser, resource: Resource, operation: Operation) -> bool {
    if user.is_system() {
        return true;
    }
    if matches!(
        resource,
        Resource::Files | Resource::Batches | Resource::ApiKeys | Resource::Connections | Resource::Webhooks
    ) && matches!(
        operation,
        Operation::CreateAll | Operation::ReadAll | Operation::UpdateAll | Operation::DeleteAll | Operation::SystemAccess
    ) {
        return false;
    }
    if (resource == Resource::Users && matches!(operation, Operation::UpdateAll | Operation::DeleteAll))
        || (resource == Resource::Organizations && matches!(operation, Operation::UpdateAll | Operation::DeleteAll))
    {
        return false;
    }
    if user.auth_method != AuthMethod::TrustedSso {
        return role_has_permission(&Role::StandardUser, resource, operation)
            || user
                .roles
                .iter()
                .filter(|role| match role {
                    Role::PlatformManager => user.auth_method == AuthMethod::PlatformApiKey,
                    Role::BillingManager | Role::RequestViewer => false,
                    _ => true,
                })
                .any(|role| role_has_permission(role, resource, operation));
    }
    if user.is_admin {
        return true;
    }

    user.roles.iter().any(|role| role_has_permission(role, resource, operation))
}

pub fn can_run_background_inference(user: &CurrentUser) -> bool {
    user.roles.contains(&Role::BackgroundInferenceUser)
}

pub fn role_has_permission(role: &Role, resource: Resource, operation: Operation) -> bool {
    if operation == Operation::SystemAccess {
        return false;
    }

    match role {
        Role::PlatformManager => {
            !matches!(resource, Resource::Requests)
        }
        Role::StandardUser => {
            matches!(
                (resource, operation),
                (Resource::Models, Operation::ReadOwn)
                    | (Resource::CompositeModels, Operation::ReadOwn)
                    | (Resource::ApiKeys, Operation::ReadOwn)
                    | (Resource::ApiKeys, Operation::CreateOwn)
                    | (Resource::ApiKeys, Operation::UpdateOwn)
                    | (Resource::ApiKeys, Operation::DeleteOwn)
                    | (Resource::Users, Operation::ReadOwn)
                    | (Resource::Users, Operation::UpdateOwn)
                    | (Resource::Credits, Operation::ReadOwn)
                    | (Resource::Webhooks, Operation::CreateOwn)
                    | (Resource::Webhooks, Operation::ReadOwn)
                    | (Resource::Webhooks, Operation::UpdateOwn)
                    | (Resource::Webhooks, Operation::DeleteOwn)
                    | (Resource::Organizations, Operation::ReadOwn)
                    | (Resource::Organizations, Operation::UpdateOwn)
                    | (Resource::Organizations, Operation::CreateOwn)
            )
        }
        Role::RequestViewer => {
            matches!(
                (resource, operation),
                (Resource::Requests, Operation::ReadAll)
                    | (Resource::Requests, Operation::ReadOwn)
                    | (Resource::Analytics, Operation::ReadAll)
                    | (Resource::Analytics, Operation::ReadOwn)
                    | (Resource::Users, Operation::ReadOwn)
                    | (Resource::Groups, Operation::ReadOwn)
                    | (Resource::Credits, Operation::ReadOwn)
            )
        }
        Role::BillingManager => {
            matches!(
                (resource, operation),
                (Resource::Credits, _) | (Resource::Users, Operation::ReadAll)
            )
        }
        Role::BatchAPIUser => {
            matches!(
                (resource, operation),
                (Resource::Files, Operation::CreateOwn)
                    | (Resource::Files, Operation::ReadOwn)
                    | (Resource::Files, Operation::DeleteOwn)
                    | (Resource::Batches, Operation::CreateOwn)
                    | (Resource::Batches, Operation::ReadOwn)
                    | (Resource::Batches, Operation::UpdateOwn)
                    | (Resource::Batches, Operation::DeleteOwn)
            )
        }
        Role::BackgroundInferenceUser => false,
        Role::ConnectionsUser => {
            matches!(
                (resource, operation),
                (Resource::Connections, Operation::CreateOwn)
                    | (Resource::Connections, Operation::ReadOwn)
                    | (Resource::Connections, Operation::UpdateOwn)
                    | (Resource::Connections, Operation::DeleteOwn)
                    | (Resource::Files, Operation::ReadOwn)
                    | (Resource::Batches, Operation::ReadOwn)
            )
        }
    }
}

fn can_perform_own_operation(user: &CurrentUser, resource: Resource, operation: Operation, target_user_id: UserId) -> bool {
    user.id == target_user_id && has_permission(user, resource, operation)
}

fn can_perform_all_operation(user: &CurrentUser, resource: Resource, operation: Operation) -> bool {
    has_permission(user, resource, operation)
}

macro_rules! generate_permission_helpers {
    ($operation_name:ident, $all_operation:expr, $own_operation:expr) => {
        paste::paste! {
            pub fn [<can_ $operation_name:lower _own_resource>](user: &CurrentUser, resource: Resource, target_user_id: UserId) -> bool {
                can_perform_own_operation(user, resource, $own_operation, target_user_id)
            }

            pub fn [<can_ $operation_name:lower _all_resources>](user: &CurrentUser, resource: Resource) -> bool {
                can_perform_all_operation(user, resource, $all_operation)
            }
        }
    };
}

generate_permission_helpers!(read, Operation::ReadAll, Operation::ReadOwn);
generate_permission_helpers!(create, Operation::CreateAll, Operation::CreateOwn);
generate_permission_helpers!(update, Operation::UpdateAll, Operation::UpdateOwn);
generate_permission_helpers!(delete, Operation::DeleteAll, Operation::DeleteOwn);

pub async fn can_manage_org_resource(
    current_user: &CurrentUser,
    target_user_id: UserId,
    db: &mut sqlx::PgConnection,
) -> std::result::Result<bool, crate::db::errors::DbError> {
    let mut repo = crate::db::handlers::Organizations::new(db);
    match repo.get_user_org_role(current_user.id, target_user_id).await? {
        Some(role) if role == "owner" || role == "admin" => Ok(true),
        _ => Ok(false),
    }
}

pub async fn is_org_member(
    current_user: &CurrentUser,
    target_user_id: UserId,
    db: &mut sqlx::PgConnection,
) -> std::result::Result<bool, crate::db::errors::DbError> {
    let mut repo = crate::db::handlers::Organizations::new(db);
    Ok(repo.get_user_org_role(current_user.id, target_user_id).await?.is_some())
}

#[derive(Debug, Clone, Copy)]
pub struct KeyCapabilities {
    pub target_is_org: bool,
    pub is_org_manager: bool,
    pub has_manage_keys_role: bool,
}

impl KeyCapabilities {
    pub fn can_self_manage_keys(&self) -> bool {
        !self.target_is_org || self.is_org_manager || self.has_manage_keys_role
    }

    pub fn requires_batch_key_selection(&self) -> bool {
        self.target_is_org && !self.is_org_manager && !self.has_manage_keys_role
    }
}

pub async fn resolve_key_capabilities(
    current_user: &CurrentUser,
    target_user_id: UserId,
    db: &mut sqlx::PgConnection,
) -> std::result::Result<KeyCapabilities, crate::db::errors::DbError> {
    let row = sqlx::query!(
        r#"
        SELECT u.user_type AS "user_type!",
               uo.role AS "role?",
               EXISTS(
                   SELECT 1 FROM organization_member_roles omr
                   WHERE omr.user_organization_id = uo.id AND omr.role = 'manage_keys'
               ) AS "has_manage_keys!"
        FROM users u
        LEFT JOIN user_organizations uo
               ON uo.organization_id = u.id
              AND uo.user_id = $2
              AND uo.status = 'active'
        WHERE u.id = $1
        "#,
        target_user_id,
        current_user.id
    )
    .fetch_optional(&mut *db)
    .await?;

    let Some(row) = row else {
        return Ok(KeyCapabilities {
            target_is_org: false,
            is_org_manager: false,
            has_manage_keys_role: false,
        });
    };

    let target_is_org = row.user_type == "organization";
    let is_org_manager = target_is_org && matches!(row.role.as_deref(), Some("owner") | Some("admin"));

    Ok(KeyCapabilities {
        target_is_org,
        is_org_manager,
        has_manage_keys_role: target_is_org && row.has_manage_keys,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn create_user_with_roles(roles: Vec<Role>, is_admin: bool) -> CurrentUser {
        CurrentUser {
            auth_method: crate::api::models::users::AuthMethod::TrustedSso,
            id: Uuid::new_v4(),
            username: "test".to_string(),
            email: "test@example.com".to_string(),
            is_admin,
            roles,
            display_name: None,
            avatar_url: None,
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        }
    }

    #[test]
    fn test_admin_bypass() {
        let admin = create_user_with_roles(vec![Role::StandardUser], true);

        assert!(has_permission(&admin, Resource::Users, Operation::CreateAll));
        assert!(has_permission(&admin, Resource::Requests, Operation::ReadAll));
        assert!(has_permission(&admin, Resource::Models, Operation::DeleteAll));
    }

    #[test]
    fn test_standard_user_role() {
        let user = create_user_with_roles(vec![Role::StandardUser], false);

        assert!(has_permission(&user, Resource::Users, Operation::ReadOwn));
        assert!(has_permission(&user, Resource::ApiKeys, Operation::CreateOwn));
        assert!(has_permission(&user, Resource::Models, Operation::ReadOwn));

        assert!(!has_permission(&user, Resource::Users, Operation::CreateAll));
        assert!(!has_permission(&user, Resource::Requests, Operation::ReadAll));
        assert!(!has_permission(&user, Resource::Analytics, Operation::ReadAll));
    }

    #[test]
    fn test_request_viewer_role() {
        let viewer = create_user_with_roles(vec![Role::RequestViewer], false);

        assert!(has_permission(&viewer, Resource::Requests, Operation::ReadAll));
        assert!(has_permission(&viewer, Resource::Analytics, Operation::ReadAll));
        assert!(has_permission(&viewer, Resource::Users, Operation::ReadOwn));

        assert!(!has_permission(&viewer, Resource::Users, Operation::CreateAll));
        assert!(!has_permission(&viewer, Resource::Models, Operation::UpdateAll));
    }

    #[test]
    fn test_platform_manager_role() {
        let pm = create_user_with_roles(vec![Role::PlatformManager], false);

        assert!(has_permission(&pm, Resource::Users, Operation::CreateAll));
        assert!(has_permission(&pm, Resource::Models, Operation::DeleteAll));
        assert!(has_permission(&pm, Resource::Analytics, Operation::ReadAll));

        assert!(!has_permission(&pm, Resource::Requests, Operation::ReadAll));
        assert!(!has_permission(&pm, Resource::Requests, Operation::ReadOwn));
    }

    #[test]
    fn test_multi_role_additive_permissions() {
        let multi_user = create_user_with_roles(vec![Role::StandardUser, Role::RequestViewer], false);

        assert!(has_permission(&multi_user, Resource::ApiKeys, Operation::CreateOwn));
        assert!(has_permission(&multi_user, Resource::Requests, Operation::ReadAll));
        assert!(has_permission(&multi_user, Resource::Analytics, Operation::ReadAll));

        assert!(!has_permission(&multi_user, Resource::Users, Operation::CreateAll));
    }

    #[test]
    fn test_no_roles_no_permissions() {
        let no_roles = create_user_with_roles(vec![], false);

        assert!(!has_permission(&no_roles, Resource::Users, Operation::ReadOwn));
        assert!(!has_permission(&no_roles, Resource::ApiKeys, Operation::CreateOwn));
        assert!(!has_permission(&no_roles, Resource::Requests, Operation::ReadAll));
    }

    #[test]
    fn test_system_access_restricted() {
        let admin = create_user_with_roles(vec![Role::PlatformManager], true);
        let pm = create_user_with_roles(vec![Role::PlatformManager], false);

        assert!(has_permission(&admin, Resource::Models, Operation::SystemAccess));

        assert!(!has_permission(&pm, Resource::Models, Operation::SystemAccess));
        assert!(!role_has_permission(
            &Role::PlatformManager,
            Resource::Models,
            Operation::SystemAccess
        ));
    }

    #[test]
    fn test_permission_helpers() {
        let user = create_user_with_roles(vec![Role::StandardUser], false);
        let other_id = Uuid::new_v4();

        assert!(can_read_own_resource(&user, Resource::Users, user.id));
        assert!(!can_read_own_resource(&user, Resource::Users, other_id));

        assert!(!can_read_all_resources(&user, Resource::Users));

        let admin = create_user_with_roles(vec![], true);
        assert!(can_read_all_resources(&admin, Resource::Users));
    }

    #[test]
    fn test_requires_permission_deref() {
        let user = create_user_with_roles(vec![Role::StandardUser], false);
        let requires_permission = RequiresPermission::<resource::Users, operation::ReadOwn> {
            current_user: user.clone(),
            _marker: PhantomData,
        };

        assert_eq!(requires_permission.id, user.id);
        assert_eq!(requires_permission.username, user.username);
        assert_eq!(requires_permission.is_admin(), user.is_admin());
    }

    #[test]
    fn test_batch_api_user_role() {
        let batch_user = create_user_with_roles(vec![Role::BatchAPIUser], false);

        assert!(has_permission(&batch_user, Resource::Files, Operation::CreateOwn));
        assert!(has_permission(&batch_user, Resource::Files, Operation::ReadOwn));
        assert!(has_permission(&batch_user, Resource::Files, Operation::DeleteOwn));

        assert!(!has_permission(&batch_user, Resource::Files, Operation::ReadAll));
        assert!(!has_permission(&batch_user, Resource::ApiKeys, Operation::CreateOwn));
        assert!(!has_permission(&batch_user, Resource::Models, Operation::ReadOwn));
        assert!(!has_permission(&batch_user, Resource::Requests, Operation::ReadAll));
    }

    #[test]
    fn test_batch_api_user_with_standard_user() {
        let user = create_user_with_roles(vec![Role::StandardUser, Role::BatchAPIUser], false);

        assert!(has_permission(&user, Resource::ApiKeys, Operation::CreateOwn));
        assert!(has_permission(&user, Resource::Models, Operation::ReadOwn));
        assert!(has_permission(&user, Resource::Users, Operation::ReadOwn));

        assert!(has_permission(&user, Resource::Files, Operation::CreateOwn));
        assert!(has_permission(&user, Resource::Files, Operation::ReadOwn));
        assert!(has_permission(&user, Resource::Files, Operation::DeleteOwn));

        assert!(!has_permission(&user, Resource::Files, Operation::ReadAll));
        assert!(!has_permission(&user, Resource::Users, Operation::CreateAll));
    }

    #[test]
    fn background_inference_requires_exact_role() {
        let allowed = create_user_with_roles(vec![Role::BackgroundInferenceUser], false);
        let platform_manager = create_user_with_roles(vec![Role::PlatformManager], false);
        let batch_user = create_user_with_roles(vec![Role::BatchAPIUser], false);
        let legacy_admin = create_user_with_roles(vec![], true);

        assert!(can_run_background_inference(&allowed));
        assert!(!can_run_background_inference(&platform_manager));
        assert!(!can_run_background_inference(&batch_user));
        assert!(!can_run_background_inference(&legacy_admin));
    }

    #[test]
    fn background_inference_role_uses_public_api_name() {
        assert_eq!(
            serde_json::to_value(Role::BackgroundInferenceUser).expect("serialize role"),
            serde_json::json!("BackgroundInferenceUser")
        );
    }

    #[test]
    fn test_platform_manager_has_file_access() {
        let pm = create_user_with_roles(vec![Role::PlatformManager], false);

        assert!(!has_permission(&pm, Resource::Files, Operation::CreateAll));
        assert!(!has_permission(&pm, Resource::Files, Operation::ReadAll));
        assert!(!has_permission(&pm, Resource::Files, Operation::UpdateAll));
        assert!(!has_permission(&pm, Resource::Files, Operation::DeleteAll));
    }

    #[test]
    fn test_admin_has_no_system_override_for_files() {
        let admin = create_user_with_roles(vec![Role::BatchAPIUser], true);
        let regular_user = create_user_with_roles(vec![Role::BatchAPIUser], false);

        assert!(!has_permission(&admin, Resource::Files, Operation::SystemAccess));

        assert!(!has_permission(&regular_user, Resource::Files, Operation::SystemAccess));
    }

    #[test]
    fn test_connections_user_role() {
        let conn_user = create_user_with_roles(vec![Role::ConnectionsUser], false);

        assert!(has_permission(&conn_user, Resource::Connections, Operation::CreateOwn));
        assert!(has_permission(&conn_user, Resource::Connections, Operation::ReadOwn));
        assert!(has_permission(&conn_user, Resource::Connections, Operation::UpdateOwn));
        assert!(has_permission(&conn_user, Resource::Connections, Operation::DeleteOwn));

        assert!(has_permission(&conn_user, Resource::Files, Operation::ReadOwn));
        assert!(has_permission(&conn_user, Resource::Batches, Operation::ReadOwn));

        assert!(!has_permission(&conn_user, Resource::Connections, Operation::ReadAll));
        assert!(!has_permission(&conn_user, Resource::Files, Operation::CreateOwn));
        assert!(!has_permission(&conn_user, Resource::Batches, Operation::CreateOwn));
        assert!(!has_permission(&conn_user, Resource::ApiKeys, Operation::CreateOwn));
        assert!(!has_permission(&conn_user, Resource::Models, Operation::ReadOwn));
        assert!(!has_permission(&conn_user, Resource::Requests, Operation::ReadAll));
        assert!(!has_permission(&conn_user, Resource::Users, Operation::ReadAll));
    }

    #[test]
    fn test_connections_user_with_standard_user() {
        let user = create_user_with_roles(vec![Role::StandardUser, Role::ConnectionsUser], false);

        assert!(has_permission(&user, Resource::ApiKeys, Operation::CreateOwn));
        assert!(has_permission(&user, Resource::Models, Operation::ReadOwn));
        assert!(has_permission(&user, Resource::Users, Operation::ReadOwn));

        assert!(has_permission(&user, Resource::Connections, Operation::CreateOwn));
        assert!(has_permission(&user, Resource::Connections, Operation::ReadOwn));
        assert!(has_permission(&user, Resource::Connections, Operation::DeleteOwn));
        assert!(has_permission(&user, Resource::Files, Operation::ReadOwn));
        assert!(has_permission(&user, Resource::Batches, Operation::ReadOwn));

        assert!(!has_permission(&user, Resource::Connections, Operation::ReadAll));
        assert!(!has_permission(&user, Resource::Users, Operation::CreateAll));
    }

    #[test]
    fn test_system_resource_permissions() {
        let admin = create_user_with_roles(vec![Role::StandardUser], true);
        let pm = create_user_with_roles(vec![Role::PlatformManager], false);
        let request_viewer = create_user_with_roles(vec![Role::RequestViewer], false);
        let standard_user = create_user_with_roles(vec![Role::StandardUser], false);

        assert!(has_permission(&admin, Resource::System, Operation::ReadAll));

        assert!(has_permission(&pm, Resource::System, Operation::ReadAll));

        assert!(!has_permission(&request_viewer, Resource::System, Operation::ReadAll));

        assert!(!has_permission(&standard_user, Resource::System, Operation::ReadAll));
        assert!(!has_permission(&standard_user, Resource::System, Operation::ReadOwn));
    }


    use crate::api::models::users::UserCreate;
    use crate::db::handlers::{Organizations, Repository, Users};
    use crate::db::models::{organizations::OrganizationCreateDBRequest, users::UserCreateDBRequest};
    use sqlx::PgPool;

    async fn create_db_user(pool: &PgPool, username: &str, email: &str) -> CurrentUser {
        let mut conn = pool.acquire().await.unwrap();
        let mut repo = Users::new(&mut conn);
        let user = repo
            .create(&UserCreateDBRequest::from(UserCreate {
                username: username.to_string(),
                email: email.to_string(),
                display_name: None,
                avatar_url: None,
                roles: vec![Role::StandardUser],
            }))
            .await
            .unwrap();
        let user: crate::db::models::users::UserDBResponse = user;
        CurrentUser {
            auth_method: crate::api::models::users::AuthMethod::TrustedSso,
            id: user.id,
            username: user.username,
            email: user.email,
            is_admin: false,
            roles: vec![Role::StandardUser],
            display_name: None,
            avatar_url: None,
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_is_org_member_returns_true_for_owner(pool: PgPool) {
        let alice = create_db_user(&pool, "alice", "alice@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme".to_string(),
                    email: "org@acme.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: alice.id,
                },
                &[
                    crate::api::models::users::Role::StandardUser,
                    crate::api::models::users::Role::BatchAPIUser,
                ],
            )
            .await
            .unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let result = is_org_member(&alice, org.id, &mut conn).await.unwrap();
        assert!(result, "Owner should be recognized as org member");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_is_org_member_returns_true_for_member(pool: PgPool) {
        let alice = create_db_user(&pool, "alice", "alice@example.com").await;
        let bob = create_db_user(&pool, "bob", "bob@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme".to_string(),
                    email: "org@acme.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: alice.id,
                },
                &[
                    crate::api::models::users::Role::StandardUser,
                    crate::api::models::users::Role::BatchAPIUser,
                ],
            )
            .await
            .unwrap();
        orgs.add_member(org.id, bob.id, "member").await.unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let result = is_org_member(&bob, org.id, &mut conn).await.unwrap();
        assert!(result, "Member should be recognized as org member");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_is_org_member_returns_true_for_admin(pool: PgPool) {
        let alice = create_db_user(&pool, "alice", "alice@example.com").await;
        let charlie = create_db_user(&pool, "charlie", "charlie@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme".to_string(),
                    email: "org@acme.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: alice.id,
                },
                &[
                    crate::api::models::users::Role::StandardUser,
                    crate::api::models::users::Role::BatchAPIUser,
                ],
            )
            .await
            .unwrap();
        orgs.add_member(org.id, charlie.id, "admin").await.unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let result = is_org_member(&charlie, org.id, &mut conn).await.unwrap();
        assert!(result, "Admin should be recognized as org member");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_is_org_member_returns_false_for_non_member(pool: PgPool) {
        let alice = create_db_user(&pool, "alice", "alice@example.com").await;
        let outsider = create_db_user(&pool, "outsider", "outsider@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme".to_string(),
                    email: "org@acme.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: alice.id,
                },
                &[
                    crate::api::models::users::Role::StandardUser,
                    crate::api::models::users::Role::BatchAPIUser,
                ],
            )
            .await
            .unwrap();

        let mut conn = pool.acquire().await.unwrap();
        let result = is_org_member(&outsider, org.id, &mut conn).await.unwrap();
        assert!(!result, "Non-member should not be recognized as org member");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_can_manage_org_resource_owner_and_admin_only(pool: PgPool) {
        let alice = create_db_user(&pool, "alice", "alice@example.com").await;
        let bob = create_db_user(&pool, "bob", "bob@example.com").await;
        let charlie = create_db_user(&pool, "charlie", "charlie@example.com").await;

        let mut conn = pool.acquire().await.unwrap();
        let mut orgs = Organizations::new(&mut conn);
        let org = orgs
            .create(
                &OrganizationCreateDBRequest {
                    name: "acme".to_string(),
                    email: "org@acme.com".to_string(),
                    display_name: None,
                    avatar_url: None,
                    created_by: alice.id,
                },
                &[
                    crate::api::models::users::Role::StandardUser,
                    crate::api::models::users::Role::BatchAPIUser,
                ],
            )
            .await
            .unwrap();
        orgs.add_member(org.id, bob.id, "member").await.unwrap();
        orgs.add_member(org.id, charlie.id, "admin").await.unwrap();

        let mut conn = pool.acquire().await.unwrap();
        assert!(can_manage_org_resource(&alice, org.id, &mut conn).await.unwrap());

        let mut conn = pool.acquire().await.unwrap();
        assert!(can_manage_org_resource(&charlie, org.id, &mut conn).await.unwrap());

        let mut conn = pool.acquire().await.unwrap();
        assert!(!can_manage_org_resource(&bob, org.id, &mut conn).await.unwrap());
    }
}
