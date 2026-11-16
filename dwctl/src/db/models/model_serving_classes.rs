
use chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::types::{DeploymentId, InferenceEndpointId, ModelServingClassId};

#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct ModelServingClass {
    pub id: ModelServingClassId,
    pub deployed_model_id: DeploymentId,
    pub class_key: String,
    pub display_name: String,
    pub inference_endpoint_id: InferenceEndpointId,
    pub upstream_model_name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
