
use chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::types::{DeploymentId, ModelServingClassId};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct ModelAlias {
    pub alias: String,
    pub deployed_model_id: DeploymentId,
    pub serving_class_id: ModelServingClassId,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
