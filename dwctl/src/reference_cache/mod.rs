
pub mod assurance;
pub mod layer;
pub mod store;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use metrics::counter;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

use crate::crypto::ApiKeyHash;
use crate::metrics::errors::component::REFERENCE_CACHE;
use crate::prefix_chain::{ModelFilter, deserialize_model_list};
use crate::prompt_cache::{
    BilledCache, CacheReadSource, ParseError, PrincipalResolver, TelemetryPolicy, TierPolicy, parse_chat_completions,
};
use crate::types::UserId;

pub use assurance::{AssuranceBook, CacheAssurance};
pub use layer::FirstTokenDetector;
pub use store::{EntryWrite, LiveEntry, ReferenceStore};

const MAX_IN_FLIGHT_WRITES: usize = 256;
const SWEEP_BATCH: i64 = 10_000;
const SWEEP_GRACE: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReferenceCacheConfig {
    pub enabled: bool,
    pub ttl_secs: u64,
    #[serde(deserialize_with = "deserialize_uuid_list")]
    pub orgs: Vec<Uuid>,
    #[serde(deserialize_with = "deserialize_model_list")]
    pub models: Option<Vec<String>>,
    pub template_allowance_tokens: HashMap<String, u32>,
    pub estimate_timeout_ms: u64,
    pub join_deadline_ms: u64,
    pub sweep_interval_secs: u64,
    pub max_connections: u32,
}

impl Default for ReferenceCacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            ttl_secs: 300,
            orgs: Vec::new(),
            models: None,
            template_allowance_tokens: HashMap::new(),
            estimate_timeout_ms: 2_000,
            join_deadline_ms: 50,
            sweep_interval_secs: 60,
            max_connections: 4,
        }
    }
}

fn deserialize_uuid_list<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Uuid>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Seq(Vec<Uuid>),
        Str(String),
    }
    match Option::<Raw>::deserialize(d)? {
        None => Ok(Vec::new()),
        Some(Raw::Seq(v)) => Ok(v),
        Some(Raw::Str(s)) => s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| Uuid::parse_str(s).map_err(serde::de::Error::custom))
            .collect(),
    }
}

impl ReferenceCacheConfig {
    pub fn validate(&self, analytics_enabled: bool, cache_enabled: bool) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if !analytics_enabled {
            return Err("reference_cache.enabled is true but enable_analytics is false; the estimate is an analytics column".to_string());
        }
        if !cache_enabled {
            return Err("reference_cache.enabled is true but cache.enabled is false; the estimate runs in the cache layer".to_string());
        }
        if self.join_deadline_ms == 0 {
            return Err("reference_cache.join_deadline_ms must be greater than 0; 0 would record NULL for every request".to_string());
        }
        if self.ttl_secs == 0 {
            return Err("reference_cache.ttl_secs must be greater than 0".to_string());
        }
        if self.estimate_timeout_ms == 0 {
            return Err("reference_cache.estimate_timeout_ms must be greater than 0; 0 would record NULL for every request".to_string());
        }
        if self.sweep_interval_secs == 0 {
            return Err("reference_cache.sweep_interval_secs must be greater than 0".to_string());
        }
        if self.max_connections == 0 {
            return Err("reference_cache.max_connections must be greater than 0".to_string());
        }
        if let Some(models) = &self.models
            && models.is_empty()
        {
            return Err("reference_cache.models is an empty list; omit it to estimate every model".to_string());
        }
        Ok(())
    }

    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.ttl_secs)
    }

    pub fn join_deadline(&self) -> Duration {
        Duration::from_millis(self.join_deadline_ms)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Boundaries {
    pub hashes: Vec<Vec<u8>>,
    pub cumulative_bytes: Vec<i64>,
}

impl Boundaries {
    pub fn total_bytes(&self) -> i64 {
        self.cumulative_bytes.last().copied().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    pub fn tokens_per_byte(&self, prompt_tokens: u64) -> Option<f32> {
        let bytes = self.total_bytes();
        (bytes > 0).then(|| prompt_tokens as f32 / bytes as f32)
    }
}

pub fn boundaries(body: &[u8], tier_policy: &TierPolicy, telemetry: &TelemetryPolicy) -> Result<Boundaries, ParseError> {
    let parsed = parse_chat_completions(body, tier_policy, telemetry)?;
    let mut total = 0i64;
    let cumulative_bytes = parsed
        .blocks
        .iter()
        .map(|b| {
            total = total.saturating_add(b.text.len() as i64);
            total
        })
        .collect();
    Ok(Boundaries {
        hashes: parsed.cumulative_hashes,
        cumulative_bytes,
    })
}

pub fn estimate(b: &Boundaries, live: &[LiveEntry], arrival: DateTime<Utc>, prompt_tokens: u64, template_allowance: u32) -> u64 {
    let by_hash: HashMap<&[u8], &LiveEntry> = live.iter().map(|e| (e.prefix_hash.as_slice(), e)).collect();
    let matched = b
        .hashes
        .iter()
        .rev()
        .filter_map(|h| by_hash.get(h.as_slice()))
        .find(|e| e.visible_at <= arrival);
    let Some(entry) = matched else {
        return 0;
    };
    let tokens = match entry.exact_tokens {
        Some(exact) => u64::try_from(exact).unwrap_or(0).saturating_sub(u64::from(template_allowance)),
        None => {
            let tpb = entry.tokens_per_byte.or_else(|| b.tokens_per_byte(prompt_tokens)).unwrap_or(0.0);
            (entry.cumulative_bytes.max(0) as f64 * f64::from(tpb)).round() as u64
        }
    };
    tokens.min(prompt_tokens)
}

fn needs_write(b: &Boundaries, live: &[LiveEntry]) -> Vec<usize> {
    let by_hash: HashMap<&[u8], &LiveEntry> = live.iter().map(|e| (e.prefix_hash.as_slice(), e)).collect();
    let last = b.hashes.len().saturating_sub(1);
    (0..b.hashes.len())
        .filter(|&i| match by_hash.get(b.hashes[i].as_slice()) {
            None => true,
            Some(e) => e.tokens_per_byte.is_none() || (i == last && e.exact_tokens.is_none()),
        })
        .collect()
}

fn estimate_outcome(outcome: &'static str) {
    counter!("dwctl_reference_cache_estimates_total", "outcome" => outcome).increment(1);
}

fn write_outcome(phase: &'static str, outcome: &'static str) {
    counter!("dwctl_reference_cache_writes_total", "phase" => phase, "outcome" => outcome).increment(1);
}

#[derive(Debug, Clone, PartialEq)]
pub struct Coverage {
    pub principal: UserId,
    pub terms: Option<CacheAssurance>,
}

#[derive(Debug)]
enum Unavailable {
    Parse(&'static str),
    Backend(sqlx::Error),
    Principal,
    Timeout,
}

impl Unavailable {
    fn outcome(&self) -> &'static str {
        match self {
            Self::Parse(_) => "parse_error",
            Self::Backend(_) => "backend_error",
            Self::Principal => "principal_error",
            Self::Timeout => "timeout",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Lookup {
    pub coverage: Coverage,
    requested: String,
    b: Boundaries,
    live: Vec<LiveEntry>,
    arrival: DateTime<Utc>,
    allowance: u32,
}

impl Lookup {
    pub fn estimate(&self, prompt_tokens: u64) -> u64 {
        estimate(&self.b, &self.live, self.arrival, prompt_tokens, self.allowance)
    }
}

#[derive(Debug, Clone)]
pub enum Joined {
    Uncovered,
    Ready(Lookup),
    Unavailable {
        coverage: Option<Coverage>,
        outcome: &'static str,
    },
}

impl Joined {
    pub fn coverage(&self) -> Option<&Coverage> {
        match self {
            Self::Uncovered => None,
            Self::Ready(l) => Some(&l.coverage),
            Self::Unavailable { coverage, .. } => coverage.as_ref(),
        }
    }

    pub fn terms(&self) -> Option<CacheAssurance> {
        self.coverage().and_then(|c| c.terms)
    }

    pub fn estimate(&self, prompt_tokens: u64) -> Option<u64> {
        match self {
            Self::Ready(l) => Some(l.estimate(prompt_tokens)),
            _ => None,
        }
    }

    fn outcome(&self) -> Option<&'static str> {
        match self {
            Self::Unavailable { outcome, .. } => Some(outcome),
            _ => None,
        }
    }
}

#[cfg(test)]
impl Joined {
    pub(crate) fn ready_with(principal: UserId, terms: Option<CacheAssurance>, tokens: i32) -> Self {
        let past = Utc::now() - chrono::Duration::seconds(1);
        Self::Ready(Lookup {
            coverage: Coverage { principal, terms },
            requested: "model".to_string(),
            b: Boundaries {
                hashes: vec![b"h".to_vec()],
                cumulative_bytes: vec![100],
            },
            live: vec![LiveEntry {
                prefix_hash: b"h".to_vec(),
                cumulative_bytes: 100,
                tokens_per_byte: Some(1.0),
                exact_tokens: Some(tokens),
                visible_at: past,
            }],
            arrival: Utc::now(),
            allowance: 0,
        })
    }
}

type LookupTask = JoinHandle<Result<Option<Lookup>, Unavailable>>;

pub struct PendingReference {
    cache: Arc<ReferenceCache>,
    api_key: ApiKeyHash,
    requested: String,
    model: String,
    body: Bytes,
    coverage: Arc<OnceLock<Option<Coverage>>>,
    task: Option<LookupTask>,
    joined: Option<Joined>,
    unattributed: Option<BillingRecord>,
}

#[derive(Debug, Clone, Copy)]
struct BillingRecord {
    billed: BilledCache,
    engine: Option<u64>,
    prompt_tokens: u64,
    unmarked: bool,
    injected: bool,
    outcome: Option<&'static str>,
}

impl BillingRecord {
    fn emit(&self, coverage: &Coverage, requested: &str) {
        if let Some(reference) = self.billed.reference {
            estimate_outcome(if reference > 0 { "hit" } else { "miss" });
        }
        let org = coverage.principal.to_string();
        let tokens = |kind: &'static str, value: u64| {
            counter!("dwctl_cache_assurance_tokens_total", "org" => org.clone(), "model" => requested.to_string(), "kind" => kind)
                .increment(value);
        };
        tokens("prompt", self.prompt_tokens);
        tokens("engine", self.engine.unwrap_or(0).min(self.prompt_tokens));
        if let Some(reference) = self.billed.reference {
            tokens("reference", reference);
        }
        tokens("billed", self.billed.stats.read);
        if coverage.terms.is_none() || !self.unmarked {
            return;
        }
        let fallback = if !self.injected {
            Some("inactive")
        } else if self.billed.reference.is_none() {
            Some(self.outcome.unwrap_or("unavailable"))
        } else {
            None
        };
        if let Some(reason) = fallback {
            counter!("dwctl_cache_assurance_fallbacks_total", "org" => org.clone(), "model" => requested.to_string(), "reason" => reason)
                .increment(1);
        }
        let outcome = match (self.billed.read_source, fallback) {
            (Some(CacheReadSource::Reference), _) => "raised",
            (_, Some(_)) => "fallback",
            _ => "engine",
        };
        counter!("dwctl_cache_assurance_requests_total", "org" => org, "model" => requested.to_string(), "outcome" => outcome).increment(1);
    }
}

impl Drop for PendingReference {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl PendingReference {
    pub fn first_token(&self) {
        if matches!(self.joined, Some(Joined::Uncovered)) || matches!(self.coverage.get(), Some(None)) {
            return;
        }
        self.cache
            .observe_first_token(self.api_key, self.requested.clone(), self.model.clone(), self.body.clone());
    }

    pub async fn join(&mut self) -> &Joined {
        let deadline = self.cache.join_deadline;
        self.join_within(deadline).await
    }

    pub(crate) async fn join_within(&mut self, deadline: Duration) -> &Joined {
        if self.joined.is_none() {
            let joined = self.join_once(deadline).await;
            if let Some(outcome) = joined.outcome() {
                estimate_outcome(outcome);
            }
            self.joined = Some(joined);
        }
        self.joined.as_ref().expect("set above")
    }

    async fn join_once(&mut self, deadline: Duration) -> Joined {
        let Some(task) = self.task.as_mut() else {
            return Joined::Uncovered;
        };
        let result = match tokio::time::timeout(deadline, &mut *task).await {
            Ok(result) => result,
            Err(_) => {
                return match self.coverage.get() {
                    Some(None) => Joined::Uncovered,
                    known => Joined::Unavailable {
                        coverage: known.cloned().flatten(),
                        outcome: "deadline_exceeded",
                    },
                };
            }
        };
        self.task = None;
        match result {
            Ok(Ok(Some(lookup))) => Joined::Ready(lookup),
            Ok(Ok(None)) => Joined::Uncovered,
            Ok(Err(e)) => {
                match &e {
                    Unavailable::Backend(err) => {
                        crate::background_error!(REFERENCE_CACHE, "lookup", Warning, error = %err, "reference cache: lookup failed; estimate recorded as NULL");
                    }
                    Unavailable::Parse(kind) => debug!(kind, "reference cache: prompt not parseable; estimate recorded as NULL"),
                    Unavailable::Principal | Unavailable::Timeout => {}
                }
                Joined::Unavailable {
                    coverage: self.coverage.get().cloned().flatten(),
                    outcome: e.outcome(),
                }
            }
            Err(e) => Joined::Unavailable {
                coverage: self.coverage.get().cloned().flatten(),
                outcome: if e.is_panic() { "panicked" } else { "cancelled" },
            },
        }
    }

    pub fn joined(&self) -> Option<&Joined> {
        self.joined.as_ref()
    }

    pub fn record(&mut self, billed: &BilledCache, engine: Option<u64>, prompt_tokens: Option<u64>, unmarked: bool, injected: bool) {
        let Some(joined) = &self.joined else {
            return;
        };
        let Some(prompt_tokens) = prompt_tokens else {
            if matches!(joined, Joined::Ready(_)) {
                estimate_outcome("no_usage");
            }
            return;
        };
        let record = BillingRecord {
            billed: *billed,
            engine,
            prompt_tokens,
            unmarked,
            injected,
            outcome: joined.outcome(),
        };
        match joined.coverage() {
            Some(coverage) => record.emit(coverage, &self.requested),
            None if matches!(joined, Joined::Unavailable { .. }) => self.unattributed = Some(record),
            None => {}
        }
    }

    pub fn finish(mut self, prompt_tokens: Option<u64>, success: bool) {
        let task = self.task.take();
        let joined = self.joined.take();
        let Some(prompt_tokens) = prompt_tokens.filter(|&t| t > 0 && success) else {
            if let Some(task) = task {
                task.abort();
            }
            if let (Some(record), Some(Some(c))) = (self.unattributed.take(), self.coverage.get()) {
                record.emit(c, &self.requested);
            }
            let covered = joined.as_ref().is_some_and(|j| j.coverage().is_some()) || matches!(self.coverage.get(), Some(Some(_)));
            if !success && covered {
                estimate_outcome("not_served");
            }
            return;
        };
        match (joined, task) {
            (Some(Joined::Ready(lookup)), _) => self.cache.spawn_completion_write(lookup, prompt_tokens),
            (_, Some(task)) => {
                let cache = Arc::clone(&self.cache);
                let coverage = Arc::clone(&self.coverage);
                let unattributed = self.unattributed.take();
                let requested = std::mem::take(&mut self.requested);
                tokio::spawn(async move {
                    let result = task.await;
                    if let (Some(record), Some(Some(c))) = (unattributed, coverage.get()) {
                        record.emit(c, &requested);
                    }
                    if let Ok(Ok(Some(lookup))) = result {
                        cache.spawn_completion_write(lookup, prompt_tokens);
                    }
                });
            }
            _ => {}
        }
    }
}

pub struct ReferenceCache {
    store: ReferenceStore,
    principal: PrincipalResolver,
    orgs: HashSet<Uuid>,
    models: ModelFilter,
    assurance: Arc<AssuranceBook>,
    template_allowance: HashMap<String, u32>,
    estimate_timeout: Duration,
    join_deadline: Duration,
    tier_policy: TierPolicy,
    telemetry: TelemetryPolicy,
    in_flight: Arc<Semaphore>,
}

impl std::fmt::Debug for ReferenceCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReferenceCache")
            .field("orgs", &self.orgs.len())
            .field("models", &self.models)
            .field("assurance", &self.assurance)
            .finish_non_exhaustive()
    }
}

impl ReferenceCache {
    pub fn new(
        config: &ReferenceCacheConfig,
        pool: impl sqlx_pool_router::PoolProvider,
        telemetry: TelemetryPolicy,
        assurance: Arc<AssuranceBook>,
    ) -> Self {
        Self {
            store: ReferenceStore::new(pool.clone(), config.ttl(), config.max_connections as usize),
            principal: PrincipalResolver::new(pool),
            orgs: config.orgs.iter().copied().collect(),
            models: ModelFilter::from_config(&config.models),
            assurance,
            template_allowance: config.template_allowance_tokens.clone(),
            estimate_timeout: Duration::from_millis(config.estimate_timeout_ms),
            join_deadline: config.join_deadline(),
            tier_policy: TierPolicy::from_config(&["5m".into(), "1h".into(), "24h".into()], "5m"),
            telemetry,
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT_WRITES)),
        }
    }

    pub fn assurance(&self) -> &Arc<AssuranceBook> {
        &self.assurance
    }

    pub fn begin(
        self: &Arc<Self>,
        api_key: ApiKeyHash,
        requested: &str,
        model: &str,
        body: Bytes,
        arrival: DateTime<Utc>,
    ) -> Option<PendingReference> {
        if !self.may_cover(requested, model) {
            return None;
        }
        let coverage = Arc::new(OnceLock::new());
        let task = {
            let this = Arc::clone(self);
            let coverage = Arc::clone(&coverage);
            let (requested, model, body) = (requested.to_string(), model.to_string(), body.clone());
            tokio::spawn(async move {
                tokio::time::timeout(
                    this.estimate_timeout,
                    this.lookup(&api_key, &requested, &model, &body, arrival, &coverage),
                )
                .await
                .unwrap_or(Err(Unavailable::Timeout))
            })
        };
        Some(PendingReference {
            cache: Arc::clone(self),
            api_key,
            requested: requested.to_string(),
            model: model.to_string(),
            body,
            coverage,
            task: Some(task),
            joined: None,
            unattributed: None,
        })
    }

    fn may_cover(&self, requested: &str, model: &str) -> bool {
        self.assurance.names_model(model) || (!self.orgs.is_empty() && self.models.allows(requested))
    }

    async fn coverage(&self, api_key: &ApiKeyHash, requested: &str, model: &str) -> Result<Option<Coverage>, Unavailable> {
        let resolved = match self.principal.cached(api_key).await {
            Some(memoised) => Ok(memoised),
            None => {
                let _permit = self.store.connection_permit().await;
                self.principal.resolve(api_key).await
            }
        };
        let principal = match resolved {
            Ok(Some(p)) => p,
            Ok(None) => return Ok(None),
            Err(e) => {
                debug!(error = %e, "reference cache: principal lookup failed");
                return Err(Unavailable::Principal);
            }
        };
        let terms = self.assurance.terms(principal, model);
        let shadow = self.orgs.contains(&principal) && self.models.allows(requested);
        Ok((terms.is_some() || shadow).then_some(Coverage { principal, terms }))
    }

    async fn lookup(
        &self,
        api_key: &ApiKeyHash,
        requested: &str,
        model: &str,
        body: &[u8],
        arrival: DateTime<Utc>,
        coverage_cell: &OnceLock<Option<Coverage>>,
    ) -> Result<Option<Lookup>, Unavailable> {
        let coverage = self.coverage(api_key, requested, model).await?;
        let _ = coverage_cell.set(coverage.clone());
        let Some(coverage) = coverage else {
            return Ok(None);
        };
        let b = self.boundaries(body)?;
        let live = self
            .store
            .lookup(coverage.principal, requested, &b.hashes, arrival)
            .await
            .map_err(Unavailable::Backend)?;
        let allowance = self.template_allowance.get(requested).copied().unwrap_or(0);
        Ok(Some(Lookup {
            coverage,
            requested: requested.to_string(),
            b,
            live,
            arrival,
            allowance,
        }))
    }

    fn boundaries(&self, body: &[u8]) -> Result<Boundaries, Unavailable> {
        boundaries(body, &self.tier_policy, &self.telemetry).map_err(|e| Unavailable::Parse(crate::prefix_chain::parse_error_kind(&e)))
    }

    pub fn observe_first_token(self: &Arc<Self>, api_key: ApiKeyHash, requested: String, model: String, body: Bytes) {
        let Ok(permit) = Arc::clone(&self.in_flight).try_acquire_owned() else {
            write_outcome("first_token", "backpressure");
            return;
        };
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let _permit = permit;
            let write = this.first_token_write(&api_key, &requested, &model, &body);
            if tokio::time::timeout(this.estimate_timeout, write).await.is_err() {
                write_outcome("first_token", "timeout");
            }
        });
    }

    async fn first_token_write(&self, api_key: &ApiKeyHash, requested: &str, model: &str, body: &[u8]) {
        let principal_id = match self.coverage(api_key, requested, model).await {
            Ok(Some(c)) => c.principal,
            Ok(None) => return,
            Err(_) => {
                write_outcome("first_token", "principal_error");
                return;
            }
        };
        let b = match self.boundaries(body) {
            Ok(b) if !b.is_empty() => b,
            Ok(_) => return,
            Err(_) => {
                write_outcome("first_token", "parse_error");
                return;
            }
        };
        let exact = vec![None; b.hashes.len()];
        let write = EntryWrite {
            principal_id,
            virtual_model: requested,
            prefix_hashes: &b.hashes,
            cumulative_bytes: &b.cumulative_bytes,
            exact_tokens: &exact,
            tokens_per_byte: None,
        };
        match self.store.upsert(&write).await {
            Ok(_) => write_outcome("first_token", "ok"),
            Err(e) => {
                crate::background_error!(REFERENCE_CACHE, "first_token_write", Warning, error = %e, "reference cache: first-token write failed");
                write_outcome("first_token", "error");
            }
        }
    }

    fn spawn_completion_write(self: &Arc<Self>, lookup: Lookup, prompt_tokens: u64) {
        let Lookup {
            coverage,
            requested,
            b,
            live,
            ..
        } = lookup;
        let idx = needs_write(&b, &live);
        if idx.is_empty() {
            return;
        }
        let Ok(permit) = Arc::clone(&self.in_flight).try_acquire_owned() else {
            write_outcome("completion", "backpressure");
            return;
        };
        let last = b.hashes.len() - 1;
        let tokens_per_byte = b.tokens_per_byte(prompt_tokens);
        let exact_final = i32::try_from(prompt_tokens).ok();
        let hashes: Vec<Vec<u8>> = idx.iter().map(|&i| b.hashes[i].clone()).collect();
        let bytes: Vec<i64> = idx.iter().map(|&i| b.cumulative_bytes[i]).collect();
        let exact: Vec<Option<i32>> = idx.iter().map(|&i| if i == last { exact_final } else { None }).collect();
        let store = self.store.clone();
        let timeout = self.estimate_timeout;
        let principal_id = coverage.principal;
        tokio::spawn(async move {
            let _permit = permit;
            let write = EntryWrite {
                principal_id,
                virtual_model: &requested,
                prefix_hashes: &hashes,
                cumulative_bytes: &bytes,
                exact_tokens: &exact,
                tokens_per_byte,
            };
            match tokio::time::timeout(timeout, store.upsert(&write)).await {
                Ok(Ok(_)) => write_outcome("completion", "ok"),
                Ok(Err(e)) => {
                    crate::background_error!(REFERENCE_CACHE, "completion_write", Warning, error = %e, "reference cache: completion write failed");
                    write_outcome("completion", "error");
                }
                Err(_) => write_outcome("completion", "timeout"),
            }
        });
    }
}

pub async fn run_sweeper(store: ReferenceStore, interval: Duration, shutdown: CancellationToken) {
    info!(interval_s = interval.as_secs(), "Reference cache sweeper started");
    let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }
        loop {
            match store.sweep_expired(SWEEP_BATCH, SWEEP_GRACE).await {
                Ok(n) if n < SWEEP_BATCH as u64 => break,
                Ok(_) if shutdown.is_cancelled() => break,
                Ok(_) => continue,
                Err(e) => {
                    crate::background_error!(REFERENCE_CACHE, "sweep", Warning, error = %e, "reference cache: sweep failed");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod billing_tests;
#[cfg(test)]
mod tests;
