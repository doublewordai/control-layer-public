
use serde::Deserialize;
use std::fmt;
use uuid::Uuid;

pub type UserId = Uuid;
pub type ApiKeyId = Uuid;
pub type DeploymentId = Uuid;
pub type ModelServingClassId = Uuid;
pub type GroupId = Uuid;
pub type InferenceEndpointId = Uuid;
#[allow(dead_code)]
pub type FileId = Uuid;

pub fn abbrev_uuid(uuid: &Uuid) -> String {
    uuid.to_string().chars().take(8).collect()
}

#[derive(Debug, Clone, Deserialize)]
pub enum CurrentKeyword {
    #[serde(rename = "current")]
    Current,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum UserIdOrCurrent {
    Current(CurrentKeyword),
    Id(UserId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    CreateAll,
    CreateOwn,
    ReadAll,
    ReadOwn,
    UpdateAll,
    UpdateOwn,
    DeleteAll,
    DeleteOwn,
    SystemAccess,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Resource {
    Users,
    Groups,
    Models,
    CompositeModels,
    Endpoints,
    ApiKeys,
    Analytics,
    Requests,
    Pricing,
    ModelRateLimits,
    Credits,
    Probes,
    Files,
    Batches,
    Webhooks,
    System,
    Organizations,
    Connections,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    Allow(Resource, Operation),
    Granted,
    Any(Vec<Permission>),
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operation::CreateAll | Operation::CreateOwn => write!(f, "Create"),
            Operation::ReadAll | Operation::ReadOwn => write!(f, "Read"),
            Operation::UpdateAll | Operation::UpdateOwn => write!(f, "Update"),
            Operation::DeleteAll | Operation::DeleteOwn => write!(f, "Delete"),
            Operation::SystemAccess => write!(f, "Access"),
        }
    }
}
