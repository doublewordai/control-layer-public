
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::request::DaemonId;

pub trait DaemonState: Send + Sync {}

#[derive(Debug, Clone, Serialize)]
pub struct DaemonRecord<T: DaemonState> {
    pub state: T,
    pub data: DaemonData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaemonData {
    pub id: DaemonId,
    pub hostname: String,
    pub pid: i32,
    pub version: String,
    pub config_snapshot: serde_json::Value,
}


#[derive(Debug, Clone, Serialize)]
pub struct Initializing {
    pub started_at: DateTime<Utc>,
}

impl DaemonState for Initializing {}

#[derive(Debug, Clone, Serialize)]
pub struct Running {
    pub started_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub stats: DaemonStats,
}

impl DaemonState for Running {}

#[derive(Debug, Clone, Serialize)]
pub struct Dead {
    pub started_at: DateTime<Utc>,
    pub stopped_at: DateTime<Utc>,
    pub final_stats: DaemonStats,
}

impl DaemonState for Dead {}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DaemonStats {
    pub requests_processed: u64,
    pub requests_failed: u64,
    pub requests_in_flight: usize,
}


#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", content = "daemon")]
pub enum AnyDaemonRecord {
    Initializing(DaemonRecord<Initializing>),
    Running(DaemonRecord<Running>),
    Dead(DaemonRecord<Dead>),
}

impl AnyDaemonRecord {
    pub fn id(&self) -> DaemonId {
        match self {
            AnyDaemonRecord::Initializing(d) => d.data.id,
            AnyDaemonRecord::Running(d) => d.data.id,
            AnyDaemonRecord::Dead(d) => d.data.id,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, AnyDaemonRecord::Dead(_))
    }

    pub fn status(&self) -> DaemonStatus {
        match self {
            AnyDaemonRecord::Initializing(_) => DaemonStatus::Initializing,
            AnyDaemonRecord::Running(_) => DaemonStatus::Running,
            AnyDaemonRecord::Dead(_) => DaemonStatus::Dead,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonStatus {
    Initializing,
    Running,
    Dead,
}

impl DaemonStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            DaemonStatus::Initializing => "initializing",
            DaemonStatus::Running => "running",
            DaemonStatus::Dead => "dead",
        }
    }
}

impl std::str::FromStr for DaemonStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "initializing" => Ok(DaemonStatus::Initializing),
            "running" => Ok(DaemonStatus::Running),
            "dead" => Ok(DaemonStatus::Dead),
            _ => Err(format!("Invalid daemon status: {}", s)),
        }
    }
}


impl From<DaemonRecord<Initializing>> for AnyDaemonRecord {
    fn from(d: DaemonRecord<Initializing>) -> Self {
        AnyDaemonRecord::Initializing(d)
    }
}

impl From<DaemonRecord<Running>> for AnyDaemonRecord {
    fn from(d: DaemonRecord<Running>) -> Self {
        AnyDaemonRecord::Running(d)
    }
}

impl From<DaemonRecord<Dead>> for AnyDaemonRecord {
    fn from(d: DaemonRecord<Dead>) -> Self {
        AnyDaemonRecord::Dead(d)
    }
}
