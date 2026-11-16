
use super::pagination::Pagination;
use crate::api::models::deployments::DeployedModelResponse;
use crate::api::models::users::UserResponse;
use crate::db::models::groups::GroupDBResponse;
use crate::types::{GroupId, UserId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ListGroupsQuery {
    #[serde(flatten)]
    #[param(inline)]
    pub pagination: Pagination,

    pub include: Option<String>,

    pub search: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupCreate {
    #[schema(example = "Engineering Team")]
    pub name: String,
    #[schema(example = "Backend and frontend engineers")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupUpdate {
    #[schema(example = "Engineering Team - Updated")]
    pub name: Option<String>,
    #[schema(example = "Updated description")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupResponse {
    #[schema(value_type = String, format = "uuid")]
    pub id: GroupId,
    pub name: String,
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>, format = "uuid")]
    pub created_by: Option<UserId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub users: Option<Vec<UserResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(no_recursion)]
    pub models: Option<Vec<DeployedModelResponse>>,
    pub source: String,
}

impl From<GroupDBResponse> for GroupResponse {
    fn from(db: GroupDBResponse) -> Self {
        Self {
            id: db.id,
            name: db.name,
            description: db.description,
            created_by: Some(db.created_by),
            created_at: db.created_at,
            updated_at: db.updated_at,
            source: db.source,
            users: None,
            models: None,
        }
    }
}

impl GroupResponse {
    pub fn with_relationships(mut self, users: Option<Vec<UserResponse>>, models: Option<Vec<DeployedModelResponse>>) -> Self {
        if let Some(users) = users {
            self.users = Some(users);
        }
        if let Some(models) = models {
            self.models = Some(models);
        }
        self
    }

    pub fn mask_created_by(mut self) -> Self {
        self.created_by = None;
        self
    }
}
