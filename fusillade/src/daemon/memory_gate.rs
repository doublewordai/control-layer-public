
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use metrics::{counter, gauge};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct MemoryReading {
    pub working_set: u64,
    pub limit: u64,
}

pub(super) trait MemorySource: Send + Sync {
    fn read(&self) -> Option<MemoryReading>;
}

pub(super) struct CgroupMemorySource;

impl CgroupMemorySource {
    const UNLIMITED_ABOVE: u64 = 1 << 50;

    fn read_u64(path: &str) -> Option<u64> {
        std::fs::read_to_string(path).ok()?.trim().parse().ok()
    }

    fn read_stat_key(path: &str, key: &str) -> Option<u64> {
        let contents = std::fs::read_to_string(path).ok()?;
        contents.lines().find_map(|line| {
            let mut parts = line.split_whitespace();
            (parts.next()? == key).then(|| parts.next()?.parse().ok())?
        })
    }

    fn working_set(current: u64, inactive_file: u64, file: u64) -> u64 {
        current.saturating_sub(inactive_file.min(file))
    }

    fn read_v2() -> Option<MemoryReading> {
        const STAT: &str = "/sys/fs/cgroup/memory.stat";
        let limit_raw = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
        let limit = match limit_raw.trim() {
            "max" => return None,
            other => other.parse::<u64>().ok()?,
        };
        let current = Self::read_u64("/sys/fs/cgroup/memory.current")?;
        let inactive_file = Self::read_stat_key(STAT, "inactive_file").unwrap_or(0);
        let file = Self::read_stat_key(STAT, "file").unwrap_or(0);
        Some(MemoryReading {
            working_set: Self::working_set(current, inactive_file, file),
            limit,
        })
    }

    fn read_v1() -> Option<MemoryReading> {
        const STAT: &str = "/sys/fs/cgroup/memory/memory.stat";
        let limit = Self::read_u64("/sys/fs/cgroup/memory/memory.limit_in_bytes")?;
        if limit >= Self::UNLIMITED_ABOVE {
            return None;
        }
        let usage = Self::read_u64("/sys/fs/cgroup/memory/memory.usage_in_bytes")?;
        let inactive_file = Self::read_stat_key(STAT, "total_inactive_file").unwrap_or(0);
        let file = Self::read_stat_key(STAT, "total_cache").unwrap_or(0);
        Some(MemoryReading {
            working_set: Self::working_set(usage, inactive_file, file),
            limit,
        })
    }
}

impl MemorySource for CgroupMemorySource {
    fn read(&self) -> Option<MemoryReading> {
        Self::read_v2().or_else(Self::read_v1)
    }
}

pub(super) struct MemoryGate {
    source: Box<dyn MemorySource>,
    high: f64,
    low: f64,
    engaged: AtomicBool,
    warned_unavailable: AtomicBool,
    peak_in_flight_at_gate: AtomicU64,
}

impl MemoryGate {
    pub(super) fn new(high: f64, low: f64, source: Box<dyn MemorySource>) -> Option<Self> {
        if high <= 0.0 || high > 1.0 || low <= 0.0 || low >= high {
            return None;
        }
        Some(Self {
            source,
            high,
            low,
            engaged: AtomicBool::new(false),
            warned_unavailable: AtomicBool::new(false),
            peak_in_flight_at_gate: AtomicU64::new(0),
        })
    }

    pub(super) fn should_block(&self, in_flight: usize) -> bool {
        let Some(reading) = self.source.read() else {
            if !self.warned_unavailable.swap(true, Ordering::Relaxed) {
                tracing::info!(
                    "Memory gate configured but this process has no readable cgroup limit; \
                     claiming will not be suppressed on memory pressure"
                );
            }
            return false;
        };

        let used = reading.working_set as f64 / reading.limit as f64;
        gauge!("fusillade_memory_working_set_ratio").set(used);

        let was_engaged = self.engaged.load(Ordering::Relaxed);
        let engaged = if was_engaged {
            used > self.low
        } else {
            used >= self.high
        };
        self.engaged.store(engaged, Ordering::Relaxed);
        gauge!("fusillade_memory_gate_engaged").set(u8::from(engaged));

        if engaged && !was_engaged {
            counter!("fusillade_memory_gate_engagements_total").increment(1);
            let in_flight = in_flight as u64;
            self.peak_in_flight_at_gate
                .fetch_max(in_flight, Ordering::Relaxed);
            gauge!("fusillade_in_flight_at_gate").set(in_flight as f64);
            tracing::warn!(
                working_set_bytes = reading.working_set,
                limit_bytes = reading.limit,
                used_fraction = used,
                in_flight,
                "Memory gate engaged; suspending claims until usage falls"
            );
        } else if !engaged && was_engaged {
            tracing::info!(
                used_fraction = used,
                "Memory gate released; resuming claims"
            );
        }

        engaged
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct FixedSource(Mutex<Vec<MemoryReading>>);

    impl FixedSource {
        fn new(readings: Vec<MemoryReading>) -> Box<Self> {
            Box::new(Self(Mutex::new(readings)))
        }
    }

    impl MemorySource for FixedSource {
        fn read(&self) -> Option<MemoryReading> {
            let mut readings = self.0.lock().unwrap();
            if readings.len() > 1 {
                Some(readings.remove(0))
            } else {
                readings.first().copied()
            }
        }
    }

    struct NoSource;
    impl MemorySource for NoSource {
        fn read(&self) -> Option<MemoryReading> {
            None
        }
    }

    fn at(fraction: f64) -> MemoryReading {
        MemoryReading {
            working_set: (1000.0 * fraction) as u64,
            limit: 1000,
        }
    }

    #[test]
    fn stays_open_below_the_high_mark() {
        let gate = MemoryGate::new(0.75, 0.65, FixedSource::new(vec![at(0.5)])).unwrap();
        assert!(!gate.should_block(100));
    }

    #[test]
    fn engages_at_the_high_mark() {
        let gate = MemoryGate::new(0.75, 0.65, FixedSource::new(vec![at(0.8)])).unwrap();
        assert!(gate.should_block(100));
    }

    #[test]
    fn stays_engaged_between_the_marks() {
        let gate = MemoryGate::new(
            0.75,
            0.65,
            FixedSource::new(vec![at(0.8), at(0.7), at(0.7)]),
        )
        .unwrap();
        assert!(gate.should_block(100), "engages at 0.8");
        assert!(
            gate.should_block(100),
            "still engaged at 0.7, above the low mark"
        );
    }

    #[test]
    fn releases_below_the_low_mark() {
        let gate = MemoryGate::new(
            0.75,
            0.65,
            FixedSource::new(vec![at(0.8), at(0.6), at(0.6)]),
        )
        .unwrap();
        assert!(gate.should_block(100));
        assert!(!gate.should_block(100), "released once under the low mark");
    }

    #[test]
    fn an_unreadable_source_never_blocks() {
        let gate = MemoryGate::new(0.75, 0.65, Box::new(NoSource)).unwrap();
        assert!(!gate.should_block(100));
        assert!(!gate.should_block(100));
    }

    #[test]
    fn disabled_or_nonsensical_thresholds_produce_no_gate() {
        let src = || FixedSource::new(vec![at(0.9)]);
        assert!(MemoryGate::new(0.0, 0.0, src()).is_none(), "zero disables");
        assert!(
            MemoryGate::new(0.65, 0.75, src()).is_none(),
            "low above high"
        );
        assert!(
            MemoryGate::new(0.75, 0.75, src()).is_none(),
            "equal marks flap"
        );
        assert!(
            MemoryGate::new(1.5, 0.5, src()).is_none(),
            "high above the limit"
        );
    }

    #[test]
    fn records_the_peak_in_flight_when_engaging() {
        let gate = MemoryGate::new(
            0.75,
            0.65,
            FixedSource::new(vec![at(0.8), at(0.5), at(0.9)]),
        )
        .unwrap();
        gate.should_block(4000);
        gate.should_block(10);
        gate.should_block(1200);
        assert_eq!(gate.peak_in_flight_at_gate.load(Ordering::Relaxed), 4000);
    }

    #[test]
    fn cache_is_excluded_from_the_working_set() {
        assert_eq!(
            CgroupMemorySource::working_set(1000, 300, 400),
            700,
            "the inactive part of a real cache is not the process's memory"
        );
    }

    #[test]
    fn lazily_freed_anonymous_pages_are_not_mistaken_for_cache() {
        assert_eq!(
            CgroupMemorySource::working_set(1000, 300, 0),
            1000,
            "no page cache means nothing to exclude"
        );
        assert_eq!(
            CgroupMemorySource::working_set(1000, 300, 100),
            900,
            "only the cache that exists is excluded"
        );
    }

    #[test]
    fn page_cache_does_not_count_toward_the_gate() {
        let reading = MemoryReading {
            working_set: 400,
            limit: 1000,
        };
        let gate = MemoryGate::new(0.75, 0.65, FixedSource::new(vec![reading])).unwrap();
        assert!(!gate.should_block(100));
    }
}
