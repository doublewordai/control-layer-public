
use std::sync::{Arc, OnceLock};

use chrono::{DateTime, Utc};

use super::index::{CacheEntry, IndexScope, PrefixHash, TtlTier};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheReadSource {
    Module,
    Engine,
}

impl CacheReadSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Module => "module",
            Self::Engine => "engine",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BilledCache {
    pub stats: CacheStats,
    pub read_source: Option<CacheReadSource>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CacheBilling(Arc<OnceLock<BilledCache>>);

impl CacheBilling {
    pub(crate) fn get(&self) -> Option<BilledCache> {
        self.0.get().copied()
    }

    pub(crate) fn set(&self, billed: BilledCache) {
        let _ = self.0.set(billed);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub read: u64,
    pub render_total: Option<u64>,
    pub creation_5m: u64,
    pub creation_1h: u64,
    pub creation_24h: u64,
}

impl CacheStats {
    pub fn creation_total(&self) -> u64 {
        self.creation_5m.saturating_add(self.creation_1h).saturating_add(self.creation_24h)
    }

    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }

    pub fn add_creation(&mut self, tier: TtlTier, tokens: u64) {
        let slot = match tier {
            TtlTier::FiveMinutes => &mut self.creation_5m,
            TtlTier::OneHour => &mut self.creation_1h,
            TtlTier::TwentyFourHours => &mut self.creation_24h,
        };
        *slot = slot.saturating_add(tokens);
    }
}

#[derive(Debug, Clone, Default)]
pub struct PendingWrite {
    pub writes: Vec<CacheEntry>,
    pub refresh: Option<(IndexScope, PrefixHash, DateTime<Utc>)>,
}

impl PendingWrite {
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty() && self.refresh.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_creation_routes_to_tier() {
        let mut s = CacheStats::default();
        s.add_creation(TtlTier::OneHour, 100);
        s.add_creation(TtlTier::OneHour, 50);
        s.add_creation(TtlTier::FiveMinutes, 7);
        assert_eq!(s.creation_1h, 150);
        assert_eq!(s.creation_5m, 7);
        assert_eq!(s.creation_24h, 0);
        assert_eq!(s.creation_total(), 157);
        assert!(!s.is_zero());
    }

    #[test]
    fn default_is_zero_and_empty() {
        assert!(CacheStats::default().is_zero());
        assert!(PendingWrite::default().is_empty());
    }
}
