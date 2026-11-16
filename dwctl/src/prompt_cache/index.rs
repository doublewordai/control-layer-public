
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::types::UserId;

pub type PrefixHash = Vec<u8>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlTier {
    FiveMinutes,
    OneHour,
    TwentyFourHours,
}

impl TtlTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FiveMinutes => "5m",
            Self::OneHour => "1h",
            Self::TwentyFourHours => "24h",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "5m" => Some(Self::FiveMinutes),
            "1h" => Some(Self::OneHour),
            "24h" => Some(Self::TwentyFourHours),
            _ => None,
        }
    }

    pub fn duration(self) -> chrono::Duration {
        match self {
            Self::FiveMinutes => chrono::Duration::minutes(5),
            Self::OneHour => chrono::Duration::hours(1),
            Self::TwentyFourHours => chrono::Duration::hours(24),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TierPolicy {
    default_ttl: TtlTier,
    enabled: Vec<TtlTier>,
}

impl TierPolicy {
    pub fn from_config(enabled_ttls: &[String], default_ttl: &str) -> Self {
        Self {
            default_ttl: TtlTier::parse(default_ttl).expect("default_ttl must be a known tier (validated in config)"),
            enabled: enabled_ttls
                .iter()
                .map(|s| TtlTier::parse(s).expect("enabled_ttls must contain only known tiers (validated in config)"))
                .collect(),
        }
    }

    pub fn default_ttl(&self) -> TtlTier {
        self.default_ttl
    }

    pub fn is_enabled(&self, tier: TtlTier) -> bool {
        self.enabled.contains(&tier)
    }

    pub fn enabled_strs(&self) -> Vec<&'static str> {
        self.enabled.iter().map(|t| t.as_str()).collect()
    }
}

#[derive(Debug, Clone)]
pub struct IndexScope {
    pub principal_id: UserId,
    pub virtual_model: String,
    pub tokenizer_version: String,
}

#[derive(Debug, Clone)]
pub struct CacheMatch {
    pub prefix_hash: PrefixHash,
    pub cumulative_token_count: u32,
    pub ttl_tier: TtlTier,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub scope: IndexScope,
    pub prefix_hash: PrefixHash,
    pub cumulative_token_count: u32,
    pub ttl_tier: TtlTier,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("cache index database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("cache principal lookup failed: {0}")]
    Principal(#[from] crate::db::errors::DbError),
    #[error("invalid cache data: {0}")]
    Invalid(String),
}

pub type CacheResult<T> = std::result::Result<T, CacheError>;

#[async_trait]
pub trait CacheIndex: Send + Sync {
    async fn lookup(&self, scope: &IndexScope, candidate_hashes: &[PrefixHash]) -> CacheResult<Vec<CacheMatch>>;

    async fn write(&self, entry: &CacheEntry) -> CacheResult<()>;

    async fn refresh(&self, scope: &IndexScope, prefix_hash: &PrefixHash, new_expires_at: DateTime<Utc>) -> CacheResult<()>;
}
