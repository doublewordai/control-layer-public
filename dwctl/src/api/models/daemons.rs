
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DaemonStatus {
    Initializing,
    Running,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStats {
    pub requests_processed: u64,
    pub requests_failed: u64,
    pub requests_in_flight: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonResponse {
    pub id: String,
    pub status: DaemonStatus,
    pub hostname: String,
    pub pid: i32,
    pub version: String,
    pub started_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_heartbeat: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stopped_at: Option<i64>,
    pub stats: DaemonStats,
    pub config: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct ListDaemonsQuery {
    #[serde(default)]
    pub status: Option<DaemonStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListDaemonsResponse {
    pub daemons: Vec<DaemonResponse>,
}
