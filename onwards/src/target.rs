use crate::affinity::AffinityConfig;
use crate::aimd::AimdConfig;
use crate::auth::{API_KEY_ID_LABEL, KeyHash, KeySet};
use crate::inflight::InflightLimits;
use crate::load_balancer::{Provider, ProviderPool};
use crate::reasoning::ReasoningTranslationConfig;
use crate::serving::{AccountServing, ProviderKind, ServingOverlay};
use anyhow::anyhow;
use async_trait::async_trait;
use bon::Builder;
use dashmap::DashMap;
use futures_util::{Stream, StreamExt};
use governor::{DefaultDirectRateLimiter, Quota};
use notify::{Config as NotifyConfig, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::HashMap, num::NonZeroU32, path::PathBuf, pin::Pin, sync::Arc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tracing::{debug, error, info, trace};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitParameters {
    pub requests_per_second: NonZeroU32,
    pub burst_size: Option<NonZeroU32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyLimitParameters {
    pub max_concurrent_requests: usize,
}

pub const DEFAULT_POOL: &str = "default";

pub const COMPLETIONS_POOL: &str = "completions";

pub const KNOWN_POOLS: [&str; 2] = [DEFAULT_POOL, COMPLETIONS_POOL];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RequestClass {
    #[default]
    Normal,
    Completions,
}

impl RequestClass {
    pub fn from_path(path: &str) -> Self {
        let path = path.split('?').next().unwrap_or(path);
        let path = path.trim_end_matches('/');
        if path.ends_with("/completions") && !path.ends_with("/chat/completions") {
            RequestClass::Completions
        } else {
            RequestClass::Normal
        }
    }

    pub fn pool_name(self) -> &'static str {
        match self {
            RequestClass::Normal => DEFAULT_POOL,
            RequestClass::Completions => COMPLETIONS_POOL,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct ProviderSpec {
    pub url: Url,
    pub onwards_key: Option<String>,
    pub onwards_model: Option<String>,
    pub rate_limit: Option<RateLimitParameters>,
    pub concurrency_limit: Option<ConcurrencyLimitParameters>,
    #[serde(default)]
    pub upstream_auth_header_name: Option<String>,
    #[serde(default)]
    pub upstream_auth_header_prefix: Option<String>,

    #[serde(default)]
    pub response_headers: Option<HashMap<String, String>>,

    #[serde(default = "default_weight")]
    #[builder(default = default_weight())]
    pub weight: u32,

    #[serde(default)]
    pub sanitize_response: bool,

    #[serde(default)]
    pub request_timeout_secs: Option<u64>,

    #[serde(default)]
    pub trusted: Option<bool>,

    #[serde(default)]
    pub propagate_trace_context: Option<bool>,

    #[serde(default)]
    pub reasoning_translation: Option<ReasoningTranslationConfig>,

    #[serde(default)]
    pub accepts_scheduling_priority: bool,

    #[serde(default)]
    pub kind: ProviderKind,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalanceStrategy {
    #[default]
    WeightedRandom,
    Priority,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FallbackConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default)]
    pub on_status: Vec<u16>,

    #[serde(default)]
    pub realtime_on_status: Vec<u16>,

    #[serde(default)]
    pub on_rate_limit: bool,

    #[serde(default)]
    pub with_replacement: bool,

    #[serde(default)]
    pub max_attempts: Option<usize>,

    #[serde(default)]
    pub backoff: Option<BackoffConfig>,

    #[serde(default)]
    pub max_total_backoff_ms: Option<u64>,

    #[serde(default)]
    pub first_token_timeout_ms: Option<u64>,
    #[serde(default)]
    pub aimd: Option<AimdConfig>,
    #[serde(default)]
    pub affinity: Option<AffinityConfig>,
}

impl FallbackConfig {
    pub fn should_fallback_on_status(&self, status: u16) -> bool {
        self.enabled && status_matches(&self.on_status, status)
    }

    pub fn should_fallback_on_realtime_status(&self, status: u16) -> bool {
        self.enabled
            && (status_matches(&self.on_status, status)
                || status_matches(&self.realtime_on_status, status))
    }
}

fn status_matches(patterns: &[u16], status: u16) -> bool {
    patterns.iter().any(|&pattern| {
        if pattern < 10 {
            status / 100 == pattern
        } else if pattern < 100 {
            status / 10 == pattern
        } else {
            status == pattern
        }
    })
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JitterStrategy {
    None,
    #[default]
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackoffConfig {
    pub initial_ms: u64,

    #[serde(default = "default_max_backoff_ms")]
    pub max_ms: u64,

    #[serde(default = "default_backoff_factor")]
    pub factor: f64,

    #[serde(default)]
    pub jitter: JitterStrategy,
}

fn default_max_backoff_ms() -> u64 {
    5_000
}

fn default_backoff_factor() -> f64 {
    2.0
}

impl BackoffConfig {
    fn base_delay_ms(&self, retry_index: u32) -> u64 {
        if retry_index == 0 {
            return 0;
        }
        let exponent = retry_index.saturating_sub(1);
        let scaled = match i32::try_from(exponent) {
            Ok(e) => (self.initial_ms as f64) * self.factor.powi(e),
            Err(_) => f64::INFINITY,
        };
        if !scaled.is_finite() || scaled >= self.max_ms as f64 {
            self.max_ms
        } else if scaled <= 0.0 {
            0
        } else {
            scaled as u64
        }
    }

    pub fn delay(&self, retry_index: u32) -> std::time::Duration {
        let base = self.base_delay_ms(retry_index);
        let jittered = match self.jitter {
            JitterStrategy::None => base,
            JitterStrategy::Full => {
                if base == 0 {
                    0
                } else {
                    use rand::Rng;
                    rand::rng().random_range(0..=base)
                }
            }
        };
        std::time::Duration::from_millis(jittered)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct PoolSpec {
    #[serde(default)]
    pub keys: Option<KeySet>,

    #[serde(default)]
    pub rate_limit: Option<RateLimitParameters>,

    #[serde(default)]
    pub concurrency_limit: Option<ConcurrencyLimitParameters>,

    #[serde(default)]
    pub response_headers: Option<HashMap<String, String>>,

    #[serde(default)]
    pub fallback: Option<FallbackConfig>,

    #[serde(default)]
    pub strategy: LoadBalanceStrategy,

    #[serde(default)]
    #[builder(default)]
    pub sanitize_response: bool,

    #[serde(default)]
    #[builder(default)]
    pub trusted: bool,
    #[serde(default)]
    pub routing_rules: Vec<RoutingRule>,

    #[serde(default)]
    pub overlays: HashMap<String, ServingOverlay>,

    #[serde(default)]
    pub inflight_limit: Option<u32>,

    #[serde(default)]
    pub batch_inflight_limit: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class_route_of: Option<String>,

    #[serde(default)]
    #[builder(default)]
    pub account_inflight_limits: HashMap<String, u32>,

    pub providers: Vec<ProviderSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct TargetSpec {
    pub url: Url,
    pub keys: Option<KeySet>,
    pub onwards_key: Option<String>,
    pub onwards_model: Option<String>,
    pub rate_limit: Option<RateLimitParameters>,
    pub concurrency_limit: Option<ConcurrencyLimitParameters>,
    #[serde(default)]
    pub upstream_auth_header_name: Option<String>,
    #[serde(default)]
    pub upstream_auth_header_prefix: Option<String>,

    #[serde(default)]
    pub response_headers: Option<HashMap<String, String>>,

    #[serde(default = "default_weight")]
    #[builder(default = default_weight())]
    pub weight: u32,

    #[serde(default)]
    #[builder(default)]
    pub sanitize_response: bool,

    #[serde(default)]
    #[builder(default)]
    pub trusted: bool,

    #[serde(default)]
    pub propagate_trace_context: Option<bool>,

    #[serde(default)]
    #[builder(default)]
    pub accepts_scheduling_priority: bool,

    #[serde(default)]
    #[builder(default)]
    pub kind: ProviderKind,

    #[serde(default)]
    pub request_timeout_secs: Option<u64>,

    #[serde(default)]
    pub reasoning_translation: Option<ReasoningTranslationConfig>,
}

fn default_weight() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolsSpec {
    pub pools: HashMap<String, PoolSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TargetSpecOrList {
    Pools(PoolsSpec),
    Pool(PoolSpec),
    List(Vec<TargetSpec>),
    Single(TargetSpec),
}

#[derive(Debug)]
pub struct PoolConfig {
    pub keys: Option<KeySet>,
    pub rate_limit: Option<RateLimitParameters>,
    pub concurrency_limit: Option<ConcurrencyLimitParameters>,
    pub response_headers: Option<HashMap<String, String>>,
    pub fallback: Option<FallbackConfig>,
    pub strategy: LoadBalanceStrategy,
    pub sanitize_response: bool,
    pub trusted: bool,
    pub routing_rules: Vec<RoutingRule>,
    pub overlays: HashMap<String, ServingOverlay>,
    pub inflight_limit: Option<u32>,
    pub batch_inflight_limit: Option<u32>,
    pub class_route_of: Option<String>,
    pub account_inflight_limits: HashMap<String, u32>,
    pub providers: Vec<ProviderSpec>,
}

impl From<PoolSpec> for PoolConfig {
    fn from(pool: PoolSpec) -> Self {
        PoolConfig {
            keys: pool.keys,
            rate_limit: pool.rate_limit,
            concurrency_limit: pool.concurrency_limit,
            response_headers: pool.response_headers,
            fallback: pool.fallback,
            strategy: pool.strategy,
            sanitize_response: pool.sanitize_response,
            trusted: pool.trusted,
            routing_rules: pool.routing_rules,
            overlays: pool.overlays,
            inflight_limit: pool.inflight_limit,
            batch_inflight_limit: pool.batch_inflight_limit,
            class_route_of: pool.class_route_of,
            account_inflight_limits: pool.account_inflight_limits,
            providers: pool.providers,
        }
    }
}

pub fn inheritable_routing_rules(rules: &[RoutingRule]) -> Vec<RoutingRule> {
    rules
        .iter()
        .filter(|rule| matches!(rule.action, RoutingAction::Deny))
        .cloned()
        .collect()
}

impl TargetSpecOrList {
    pub fn into_pool_configs(self) -> Result<Vec<(String, PoolConfig)>, anyhow::Error> {
        let TargetSpecOrList::Pools(spec) = self else {
            return Ok(vec![(DEFAULT_POOL.to_string(), self.into_pool_config()?)]);
        };

        let mut pools = spec.pools;
        for name in pools.keys() {
            if !KNOWN_POOLS.contains(&name.as_str()) {
                return Err(anyhow!(
                    "Unknown pool name '{}'. Known pools: {}.",
                    name,
                    KNOWN_POOLS.join(", ")
                ));
            }
        }
        let default: PoolConfig = pools
            .remove(DEFAULT_POOL)
            .ok_or_else(|| anyhow!("A `pools` target must define a '{DEFAULT_POOL}' pool."))?
            .into();

        let mut configs = Vec::with_capacity(pools.len() + 1);
        for (name, pool) in pools {
            let mut config: PoolConfig = pool.into();
            if config.keys.is_none() {
                config.keys = default.keys.clone();
            }
            if config.routing_rules.is_empty() {
                config.routing_rules = inheritable_routing_rules(&default.routing_rules);
            }
            configs.push((name, config));
        }
        configs.sort_by(|(a, _), (b, _)| a.cmp(b));
        configs.insert(0, (DEFAULT_POOL.to_string(), default));
        Ok(configs)
    }

    pub fn into_pool_config(self) -> Result<PoolConfig, anyhow::Error> {
        match self {
            TargetSpecOrList::Pools(spec) => spec
                .pools
                .into_iter()
                .find(|(name, _)| name == DEFAULT_POOL)
                .map(|(_, pool)| pool.into())
                .ok_or_else(|| anyhow!("A `pools` target must define a '{DEFAULT_POOL}' pool.")),
            TargetSpecOrList::Pool(pool) => Ok(pool.into()),
            TargetSpecOrList::List(list) => {
                let keys = list.first().and_then(|t| t.keys.clone());
                let trusted = list.first().map(|t| t.trusted).unwrap_or(false);

                if list.iter().any(|t| t.trusted != trusted) {
                    return Err(anyhow::anyhow!(
                        "All providers in a legacy list format must have the same 'trusted' value. \
                         Use pool config format if you need different trust levels per provider."
                    ));
                }

                let providers = list
                    .into_iter()
                    .map(|t| ProviderSpec {
                        url: t.url,
                        onwards_key: t.onwards_key,
                        onwards_model: t.onwards_model,
                        rate_limit: t.rate_limit,
                        concurrency_limit: t.concurrency_limit,
                        upstream_auth_header_name: t.upstream_auth_header_name,
                        upstream_auth_header_prefix: t.upstream_auth_header_prefix,
                        response_headers: t.response_headers,
                        weight: t.weight,
                        sanitize_response: t.sanitize_response,
                        request_timeout_secs: t.request_timeout_secs,
                        trusted: None,
                        propagate_trace_context: t.propagate_trace_context,
                        reasoning_translation: t.reasoning_translation,
                        accepts_scheduling_priority: t.accepts_scheduling_priority,
                        kind: t.kind,
                    })
                    .collect();
                Ok(PoolConfig {
                    keys,
                    rate_limit: None,
                    concurrency_limit: None,
                    response_headers: None,
                    fallback: None,
                    strategy: LoadBalanceStrategy::default(),
                    sanitize_response: false,
                    trusted,
                    routing_rules: Vec::new(),
                    overlays: HashMap::new(),
                    inflight_limit: None,
                    batch_inflight_limit: None,
                    class_route_of: None,
                    account_inflight_limits: HashMap::new(),
                    providers,
                })
            }
            TargetSpecOrList::Single(spec) => {
                let keys = spec.keys.clone();
                let sanitize_response = spec.sanitize_response;
                let trusted = spec.trusted;
                let provider = ProviderSpec {
                    url: spec.url,
                    onwards_key: spec.onwards_key,
                    onwards_model: spec.onwards_model,
                    rate_limit: spec.rate_limit,
                    concurrency_limit: spec.concurrency_limit,
                    upstream_auth_header_name: spec.upstream_auth_header_name,
                    upstream_auth_header_prefix: spec.upstream_auth_header_prefix,
                    response_headers: spec.response_headers,
                    weight: spec.weight,
                    sanitize_response: false,
                    request_timeout_secs: spec.request_timeout_secs,
                    trusted: None,
                    propagate_trace_context: spec.propagate_trace_context,
                    reasoning_translation: spec.reasoning_translation,
                    accepts_scheduling_priority: spec.accepts_scheduling_priority,
                    kind: spec.kind,
                };
                Ok(PoolConfig {
                    keys,
                    rate_limit: None,
                    concurrency_limit: None,
                    response_headers: None,
                    fallback: None,
                    strategy: LoadBalanceStrategy::default(),
                    sanitize_response,
                    trusted,
                    routing_rules: Vec::new(),
                    overlays: HashMap::new(),
                    inflight_limit: None,
                    batch_inflight_limit: None,
                    class_route_of: None,
                    account_inflight_limits: HashMap::new(),
                    providers: vec![provider],
                })
            }
        }
    }
}

fn normalize_url(mut url: Url) -> Url {
    let path = url.path();
    if !path.ends_with('/') {
        url.set_path(&format!("{}/", path));
    }
    url
}

impl From<TargetSpec> for Target {
    fn from(value: TargetSpec) -> Self {
        Target {
            url: normalize_url(value.url),
            keys: value.keys,
            onwards_key: value.onwards_key,
            onwards_model: value.onwards_model,
            limiter: value.rate_limit.map(|rl| {
                Arc::new(governor::RateLimiter::direct(
                    Quota::per_second(rl.requests_per_second)
                        .allow_burst(rl.burst_size.unwrap_or(rl.requests_per_second)),
                )) as Arc<dyn RateLimiter>
            }),
            upstream_auth_header_name: value.upstream_auth_header_name,
            upstream_auth_header_prefix: value.upstream_auth_header_prefix,
            response_headers: value.response_headers,
            sanitize_response: value.sanitize_response,
            request_timeout_secs: value.request_timeout_secs,
            trusted: None,
            propagate_trace_context: value.propagate_trace_context,
            reasoning_translation: value.reasoning_translation,
            accepts_scheduling_priority: value.accepts_scheduling_priority,
            kind: value.kind,
        }
    }
}

impl From<ProviderSpec> for Target {
    fn from(value: ProviderSpec) -> Self {
        Target {
            url: normalize_url(value.url),
            keys: None,
            onwards_key: value.onwards_key,
            onwards_model: value.onwards_model,
            limiter: value.rate_limit.map(|rl| {
                Arc::new(governor::RateLimiter::direct(
                    Quota::per_second(rl.requests_per_second)
                        .allow_burst(rl.burst_size.unwrap_or(rl.requests_per_second)),
                )) as Arc<dyn RateLimiter>
            }),
            upstream_auth_header_name: value.upstream_auth_header_name,
            upstream_auth_header_prefix: value.upstream_auth_header_prefix,
            response_headers: value.response_headers,
            sanitize_response: value.sanitize_response,
            request_timeout_secs: value.request_timeout_secs,
            trusted: value.trusted,
            propagate_trace_context: value.propagate_trace_context,
            reasoning_translation: value.reasoning_translation,
            accepts_scheduling_priority: value.accepts_scheduling_priority,
            kind: value.kind,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RateLimitExceeded;

pub trait RateLimiter: std::fmt::Debug + Send + Sync {
    fn check(&self) -> Result<(), RateLimitExceeded>;
}

impl RateLimiter for DefaultDirectRateLimiter {
    fn check(&self) -> Result<(), RateLimitExceeded> {
        self.check().map_err(|_| RateLimitExceeded)
    }
}

#[derive(Debug)]
pub struct ConcurrencyGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::Release);
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrencyLimiter {
    active: Arc<AtomicUsize>,
    limit: Option<usize>,
}

impl Default for ConcurrencyLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcurrencyLimiter {
    pub fn new() -> Self {
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            limit: None,
        }
    }

    pub fn with_limit(limit: usize) -> Self {
        Self {
            active: Arc::new(AtomicUsize::new(0)),
            limit: Some(limit),
        }
    }

    pub fn try_acquire(&self) -> Option<ConcurrencyGuard> {
        loop {
            let current = self.active.load(Ordering::Acquire);
            if let Some(max) = self.limit
                && current >= max
            {
                return None;
            }
            if self
                .active
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(ConcurrencyGuard {
                    active: Arc::clone(&self.active),
                });
            }
        }
    }

    pub fn at_capacity(&self) -> bool {
        match self.limit {
            Some(max) => self.active.load(Ordering::Acquire) >= max,
            None => false,
        }
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn adopt_active_counter(&mut self, old: &ConcurrencyLimiter) {
        self.active = Arc::clone(&old.active);
    }
}

#[derive(Debug, Clone, Builder)]
#[builder(derive(Clone))]
pub struct Target {
    pub url: Url,
    pub keys: Option<KeySet>,
    pub onwards_key: Option<String>,
    pub onwards_model: Option<String>,
    pub limiter: Option<Arc<dyn RateLimiter>>,
    pub upstream_auth_header_name: Option<String>,
    pub upstream_auth_header_prefix: Option<String>,
    pub response_headers: Option<HashMap<String, String>>,
    #[builder(default)]
    pub sanitize_response: bool,
    pub request_timeout_secs: Option<u64>,
    pub trusted: Option<bool>,
    pub propagate_trace_context: Option<bool>,
    pub reasoning_translation: Option<ReasoningTranslationConfig>,
    #[builder(default)]
    pub accepts_scheduling_priority: bool,
    #[builder(default)]
    pub kind: ProviderKind,
}

impl Target {
    pub fn into_pool(self) -> TargetPools {
        let keys = self.keys.clone();
        ProviderPool::with_config(
            vec![Provider::new(self, 1)],
            keys,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            false,
            Vec::new(),
        )
        .into()
    }
}

#[derive(Debug, Clone)]
pub struct TargetPools {
    default: ProviderPool,
    extra: HashMap<String, ProviderPool>,
}

impl From<ProviderPool> for TargetPools {
    fn from(default: ProviderPool) -> Self {
        Self {
            default,
            extra: HashMap::new(),
        }
    }
}

impl TargetPools {
    pub fn with_pools(default: ProviderPool, extra: HashMap<String, ProviderPool>) -> Self {
        Self { default, extra }
    }

    pub fn resolve(&self, class: RequestClass) -> &ProviderPool {
        self.named_pool(class).unwrap_or(&self.default)
    }

    pub fn resolved_name(&self, class: RequestClass) -> Option<&'static str> {
        self.named_pool(class).map(|_| class.pool_name())
    }

    fn named_pool(&self, class: RequestClass) -> Option<&ProviderPool> {
        self.extra
            .get(class.pool_name())
            .filter(|pool| !pool.is_empty())
    }

    pub fn default_pool(&self) -> &ProviderPool {
        &self.default
    }

    pub fn first_target(&self) -> Option<&Target> {
        self.default.first_target()
    }

    pub fn evaluate_routing_rules(
        &self,
        labels: &HashMap<String, String>,
    ) -> Option<&RoutingAction> {
        self.default.evaluate_routing_rules(labels)
    }

    pub fn get(&self, name: &str) -> Option<&ProviderPool> {
        if name == DEFAULT_POOL {
            Some(&self.default)
        } else {
            self.extra.get(name)
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &ProviderPool)> {
        std::iter::once((DEFAULT_POOL, &self.default))
            .chain(self.extra.iter().map(|(name, pool)| (name.as_str(), pool)))
    }

    pub fn pool_count(&self) -> usize {
        self.extra.len() + 1
    }

    pub fn adopt_provider_state(&mut self, old: &TargetPools) {
        self.default.adopt_provider_state(&old.default);
        for (name, pool) in &old.extra {
            if !self.extra.contains_key(name) {
                pool.retire_aimd();
            }
        }
        for (name, pool) in self.extra.iter_mut() {
            if let Some(old_pool) = old.extra.get(name) {
                pool.adopt_provider_state(old_pool);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyDefinition {
    pub key: KeyHash,
    pub rate_limit: Option<RateLimitParameters>,
    pub concurrency_limit: Option<ConcurrencyLimitParameters>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    pub match_labels: HashMap<String, String>,
    pub action: RoutingAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RoutingAction {
    Deny,
    Redirect { target: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct Auth {
    global_keys: KeySet,
    pub key_definitions: Option<HashMap<String, KeyDefinition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpPoolConfig {
    #[serde(default = "default_pool_max_idle_per_host")]
    pub max_idle_per_host: usize,
    #[serde(default = "default_pool_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
}

fn default_pool_max_idle_per_host() -> usize {
    100
}

fn default_pool_idle_timeout_secs() -> u64 {
    90
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigFile {
    pub targets: HashMap<String, TargetSpecOrList>,
    pub auth: Option<Auth>,
    #[serde(default)]
    pub strict_mode: bool,
    #[serde(default)]
    pub http_pool: Option<HttpPoolConfig>,
    #[serde(default)]
    pub accounts: HashMap<String, AccountServing>,
}

#[derive(Debug, Clone, Default)]
pub struct Targets {
    pub targets: Arc<DashMap<String, TargetPools>>,
    pub key_rate_limiters: Arc<DashMap<KeyHash, Arc<DefaultDirectRateLimiter>>>,
    pub key_concurrency_limiters: Arc<DashMap<KeyHash, ConcurrencyLimiter>>,
    pub key_labels: Arc<DashMap<KeyHash, HashMap<String, String>>>,
    pub key_ids: Arc<DashMap<String, KeyHash>>,
    pub accounts: Arc<DashMap<String, AccountServing>>,
    pub strict_mode: bool,
    pub http_pool_config: Option<HttpPoolConfig>,
}

#[async_trait]
pub trait TargetsStream {
    type Error: Into<anyhow::Error> + Send + Sync + 'static;

    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Targets, Self::Error>> + Send>>, Self::Error>;
}

pub struct WatchedFile(pub PathBuf);

#[async_trait]
impl TargetsStream for WatchedFile {
    type Error = anyhow::Error;

    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Targets, Self::Error>> + Send>>, Self::Error> {
        let (targets_tx, targets_rx) = mpsc::channel(100);
        let (file_tx, mut file_rx) = mpsc::channel(100);

        let mut watcher = RecommendedWatcher::new(
            move |res| {
                let _ = file_tx.blocking_send(res);
            },
            NotifyConfig::default(),
        )?;

        watcher.watch(&self.0, RecursiveMode::NonRecursive)?;

        let config_path = self.0.clone();
        tokio::spawn(async move {
            while let Some(res) = file_rx.recv().await {
                match res {
                    Ok(event) => {
                        if event.kind.is_modify() {
                            info!("Config file changed, reloading targets...");
                            match Targets::from_config_file(&config_path).await {
                                Ok(new_targets) => {
                                    if targets_tx.send(Ok(new_targets)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to reload config: {}", e);
                                    if targets_tx.send(Err(e)).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Watch error: {}", e);
                        if targets_tx
                            .send(Err(anyhow!("Watch error: {}", e)))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
            }
        });

        std::mem::forget(watcher);

        Ok(Box::pin(ReceiverStream::new(targets_rx)))
    }
}

pub struct WatchTargetsStream {
    receiver: tokio::sync::watch::Receiver<Targets>,
}

impl WatchTargetsStream {
    pub fn new(receiver: tokio::sync::watch::Receiver<Targets>) -> Self {
        Self { receiver }
    }
}

#[async_trait]
impl TargetsStream for WatchTargetsStream {
    type Error = std::convert::Infallible;

    async fn stream(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Targets, Self::Error>> + Send>>, Self::Error> {
        let stream = WatchStream::from_changes(self.receiver.clone()).map(Ok);
        Ok(Box::pin(stream))
    }
}

fn build_pool(
    alias: &str,
    pool_name: &str,
    mut pool_config: PoolConfig,
    global_keys: &KeySet,
) -> Result<ProviderPool, anyhow::Error> {
    if pool_name != DEFAULT_POOL
        && let Some(fallback) = &mut pool_config.fallback
    {
        fallback.aimd = Some(crate::aimd::AimdConfig {
            enabled: false,
            ..crate::aimd::AimdConfig::default()
        });
        fallback.affinity = None;
    }
    if let Some(fallback) = &pool_config.fallback
        && let Some(config) = &fallback.affinity
        && config.enabled
    {
        config.validate().map_err(|e| anyhow!(e))?;
        if !fallback.enabled || pool_config.strategy != LoadBalanceStrategy::Priority {
            return Err(anyhow!(
                "affinity requires enabled fallback and priority strategy"
            ));
        }
    }
    if let Some(fallback) = &pool_config.fallback
        && let Some(config) = &fallback.aimd
        && config.enabled
    {
        config.validate().map_err(|e| anyhow!(e))?;
        if !fallback.enabled || pool_config.strategy != LoadBalanceStrategy::Priority {
            return Err(anyhow!(
                "AIMD requires enabled fallback and priority strategy"
            ));
        }
        if !fallback
            .first_token_timeout_ms
            .is_some_and(|ms| ms == 0 || (ms >= config.latency_budget_ms && ms <= 3_600_000))
        {
            return Err(anyhow!(
                "AIMD requires an explicit first_token_timeout_ms of 0 or at least latency_budget_ms"
            ));
        }
    }
    for (index, provider) in pool_config.providers.iter().enumerate() {
        if let Some(config) = provider.reasoning_translation.as_ref() {
            config.validate().map_err(|error| {
                anyhow!(
                    "Invalid reasoning translation for target '{}' pool '{}' provider {}: {}",
                    alias,
                    pool_name,
                    index,
                    error.message()
                )
            })?;
        }
    }

    let merged_keys = if let Some(mut keys) = pool_config.keys {
        debug!(
            "Pool '{}' ({}) has {} keys configured",
            alias,
            pool_name,
            keys.len()
        );
        keys.extend(global_keys.clone());
        Some(keys)
    } else if !global_keys.is_empty() {
        Some(global_keys.clone())
    } else {
        None
    };

    let pool_limiter: Option<Arc<dyn RateLimiter>> = pool_config.rate_limit.map(|rl| {
        Arc::new(governor::RateLimiter::direct(
            Quota::per_second(rl.requests_per_second)
                .allow_burst(rl.burst_size.unwrap_or(rl.requests_per_second)),
        )) as Arc<dyn RateLimiter>
    });

    let pool_concurrency_limiter = pool_config
        .concurrency_limit
        .map(|cl| ConcurrencyLimiter::with_limit(cl.max_concurrent_requests));

    let pool_sanitize = pool_config.sanitize_response;
    let providers: Vec<Provider> = pool_config
        .providers
        .into_iter()
        .map(|mut spec| {
            let weight = spec.weight;
            let concurrency_limit = spec
                .concurrency_limit
                .as_ref()
                .map(|cl| cl.max_concurrent_requests);
            spec.sanitize_response = pool_sanitize || spec.sanitize_response;
            let target: Target = spec.into();
            match concurrency_limit {
                Some(limit) => Provider::with_concurrency_limit(target, weight, limit),
                None => Provider::new(target, weight),
            }
        })
        .collect();

    Ok(ProviderPool::with_config(
        providers,
        merged_keys,
        pool_limiter,
        pool_concurrency_limiter,
        pool_config.fallback,
        pool_config.strategy,
        pool_config.trusted,
        pool_config.routing_rules,
    )
    .with_overlays(pool_config.overlays)
    .with_inflight_limits(pool_config.inflight_limit.map(|default| InflightLimits {
        default,
        accounts: pool_config.account_inflight_limits,
    }))
    .with_batch_inflight_limit(pool_config.batch_inflight_limit)
    .with_class_route_of(pool_config.class_route_of))
}

pub const STANDARD_SERVING_CLASS: &str = "standard";

pub fn serving_metric_labels(alias: &str, class_route_of: Option<&str>) -> (String, String) {
    if let Some(model) = class_route_of
        && let Some(class) = alias
            .strip_prefix(model)
            .and_then(|rest| rest.strip_prefix(':'))
    {
        return (model.to_owned(), class.to_owned());
    }
    (alias.to_owned(), STANDARD_SERVING_CLASS.to_owned())
}

impl Targets {
    pub fn configured_alias<'m>(&self, model: &'m str) -> Option<&'m str> {
        Some(model).filter(|alias| self.targets.contains_key(*alias))
    }

    pub fn class_route_of(&self, alias: &str) -> Option<String> {
        self.targets
            .get(alias)
            .and_then(|pools| pools.default_pool().class_route_of().map(str::to_owned))
    }

    pub fn metric_labels(&self, alias: &str) -> (String, String) {
        serving_metric_labels(alias, self.class_route_of(alias).as_deref())
    }

    pub async fn from_config_file(config_path: &PathBuf) -> Result<Self, anyhow::Error> {
        let contents = tokio::fs::read_to_string(config_path).await.map_err(|e| {
            anyhow!(
                "Failed to read config file {}: {}",
                config_path.display(),
                e
            )
        })?;

        let config_file: ConfigFile = serde_json::from_str(&contents).map_err(|e| {
            anyhow!(
                "Failed to parse config file {}: {}",
                config_path.display(),
                e
            )
        })?;

        let targets = Self::from_config(config_file)?;

        info!(
            "Loaded {} targets from {}",
            targets.targets.len(),
            config_path.display()
        );
        Ok(targets)
    }

    pub fn from_config(mut config_file: ConfigFile) -> Result<Self, anyhow::Error> {
        let (global_keys, key_definitions) = config_file
            .auth
            .take()
            .map(|auth| (auth.global_keys, auth.key_definitions.unwrap_or_default()))
            .unwrap_or_default();
        debug!("{} global keys configured", global_keys.len());
        debug!("{} key definitions configured", key_definitions.len());

        let key_rate_limiters = Arc::new(DashMap::new());
        let key_concurrency_limiters = Arc::new(DashMap::new());
        let key_labels = Arc::new(DashMap::new());
        let key_ids = Arc::new(DashMap::new());

        for (_key_id, key_def) in key_definitions {
            if let Some(ref rate_limit) = key_def.rate_limit {
                let limiter = Arc::new(governor::RateLimiter::direct(
                    Quota::per_second(rate_limit.requests_per_second).allow_burst(
                        rate_limit
                            .burst_size
                            .unwrap_or(rate_limit.requests_per_second),
                    ),
                ));
                key_rate_limiters.insert(key_def.key, limiter);
            }
            if let Some(ref concurrency_limit) = key_def.concurrency_limit {
                let limiter =
                    ConcurrencyLimiter::with_limit(concurrency_limit.max_concurrent_requests);
                key_concurrency_limiters.insert(key_def.key, limiter);
            }
            if let Some(id) = key_def.labels.get(API_KEY_ID_LABEL) {
                key_ids.insert(id.clone(), key_def.key);
            }
            if !key_def.labels.is_empty() {
                key_labels.insert(key_def.key, key_def.labels);
            }
        }

        let accounts = Arc::new(DashMap::new());
        for (account_id, policy) in config_file.accounts {
            if !policy.is_empty() {
                accounts.insert(account_id, policy);
            }
        }

        let targets = Arc::new(DashMap::new());
        for (name, target_spec_or_list) in config_file.targets {
            let mut pool_configs = target_spec_or_list.into_pool_configs()?.into_iter();
            let (_, default_config) = pool_configs
                .next()
                .expect("into_pool_configs always yields the default pool first");
            let default_pool = build_pool(&name, DEFAULT_POOL, default_config, &global_keys)?;
            let extra = pool_configs
                .map(|(pool_name, config)| {
                    let pool = build_pool(&name, &pool_name, config, &global_keys)?;
                    Ok::<_, anyhow::Error>((pool_name, pool))
                })
                .collect::<Result<HashMap<_, _>, _>>()?;

            let pools = TargetPools::with_pools(default_pool, extra);
            for (pool_name, pool) in pools.iter() {
                debug!(
                    "Created provider pool '{}' ({}) with {} provider(s), fallback enabled: {}, strategy: {:?}",
                    name,
                    pool_name,
                    pool.len(),
                    pool.fallback_enabled(),
                    pool.strategy()
                );
            }
            targets.insert(name, pools);
        }

        Ok(Targets {
            targets,
            key_rate_limiters,
            key_concurrency_limiters,
            key_labels,
            key_ids,
            accounts,
            strict_mode: config_file.strict_mode,
            http_pool_config: config_file.http_pool,
        })
    }

    pub async fn receive_updates<W>(&self, targets_stream: W) -> Result<(), W::Error>
    where
        W: TargetsStream + Send + 'static,
        W::Error: Into<anyhow::Error>,
    {
        let targets = Arc::clone(&self.targets);
        let key_rate_limiters = Arc::clone(&self.key_rate_limiters);
        let key_concurrency_limiters = Arc::clone(&self.key_concurrency_limiters);
        let key_labels = Arc::clone(&self.key_labels);
        let key_ids = Arc::clone(&self.key_ids);
        let accounts = Arc::clone(&self.accounts);

        let mut stream = targets_stream.stream().await?;

        tokio::spawn(async move {
            while let Some(result) = stream.next().await {
                match result {
                    Ok(new_targets) => {
                        info!("Config file changed, updating targets...");
                        trace!("{:?}", new_targets);

                        let current_target_keys: Vec<String> =
                            targets.iter().map(|entry| entry.key().clone()).collect();


                        for key in current_target_keys {
                            if !new_targets.targets.contains_key(&key)
                                && let Some((_, removed)) = targets.remove(&key)
                            {
                                for (_, pool) in removed.iter() {
                                    pool.retire_aimd();
                                }
                            }
                        }

                        for entry in new_targets.targets.iter() {
                            let alias = entry.key().clone();
                            let mut new_pool = entry.value().clone();
                            if let Some(old_pool) = targets.get(&alias) {
                                new_pool.adopt_provider_state(&old_pool);
                            }
                            targets.insert(alias, new_pool);
                        }

                        let current_rate_limiter_keys: Vec<KeyHash> = key_rate_limiters
                            .iter()
                            .map(|entry| entry.key().clone())
                            .collect();

                        for key in current_rate_limiter_keys {
                            if !new_targets.key_rate_limiters.contains_key(&key) {
                                key_rate_limiters.remove(&key);
                            }
                        }

                        for entry in new_targets.key_rate_limiters.iter() {
                            key_rate_limiters.insert(entry.key().clone(), entry.value().clone());
                        }

                        let current_concurrency_limiter_keys: Vec<KeyHash> =
                            key_concurrency_limiters
                                .iter()
                                .map(|entry| entry.key().clone())
                                .collect();

                        for key in current_concurrency_limiter_keys {
                            if !new_targets.key_concurrency_limiters.contains_key(&key) {
                                key_concurrency_limiters.remove(&key);
                            }
                        }

                        for entry in new_targets.key_concurrency_limiters.iter() {
                            let key = entry.key().clone();
                            let mut new_limiter = entry.value().clone();
                            if let Some(old_limiter) = key_concurrency_limiters.get(&key) {
                                new_limiter.adopt_active_counter(&old_limiter);
                            }
                            key_concurrency_limiters.insert(key, new_limiter);
                        }

                        let current_label_keys: Vec<KeyHash> =
                            key_labels.iter().map(|entry| entry.key().clone()).collect();

                        for key in current_label_keys {
                            if !new_targets.key_labels.contains_key(&key) {
                                key_labels.remove(&key);
                            }
                        }

                        for entry in new_targets.key_labels.iter() {
                            key_labels.insert(entry.key().clone(), entry.value().clone());
                        }

                        let current_key_ids: Vec<String> =
                            key_ids.iter().map(|entry| entry.key().clone()).collect();
                        for id in current_key_ids {
                            if !new_targets.key_ids.contains_key(&id) {
                                key_ids.remove(&id);
                            }
                        }
                        for entry in new_targets.key_ids.iter() {
                            key_ids.insert(entry.key().clone(), *entry.value());
                        }

                        let current_accounts: Vec<String> =
                            accounts.iter().map(|entry| entry.key().clone()).collect();
                        for account in current_accounts {
                            if !new_targets.accounts.contains_key(&account) {
                                accounts.remove(&account);
                            }
                        }
                        for entry in new_targets.accounts.iter() {
                            accounts.insert(entry.key().clone(), entry.value().clone());
                        }
                    }
                    Err(e) => {
                        let err: anyhow::Error = e.into();
                        error!("Failed to reload config: {}", err);
                    }
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn class_routes_are_labelled_as_their_model_and_class() {
        use super::{STANDARD_SERVING_CLASS, serving_metric_labels};
        assert_eq!(
            serving_metric_labels("org/model:fast", Some("org/model")),
            ("org/model".to_owned(), "fast".to_owned())
        );
        assert_eq!(
            serving_metric_labels("org/model", None),
            ("org/model".to_owned(), STANDARD_SERVING_CLASS.to_owned())
        );
        assert_eq!(
            serving_metric_labels("other:fast", Some("org/model")),
            ("other:fast".to_owned(), STANDARD_SERVING_CLASS.to_owned())
        );
    }
    use super::*;
    use crate::auth::KeyHash;
    use dashmap::DashMap;
    use std::collections::HashSet;

    fn default_pool(targets: &Targets, alias: &str) -> ProviderPool {
        targets.targets.get(alias).unwrap().default_pool().clone()
    }
    use std::sync::Arc;
    pub struct MockConfigWatcher {
        configs: Vec<Result<Targets, String>>,
    }

    impl MockConfigWatcher {
        pub fn with_targets(targets_list: Vec<Targets>) -> Self {
            Self {
                configs: targets_list.into_iter().map(Ok).collect(),
            }
        }

        pub fn with_error(error: String) -> Self {
            Self {
                configs: vec![Err(error)],
            }
        }
    }

    #[async_trait]
    impl TargetsStream for MockConfigWatcher {
        type Error = anyhow::Error;

        async fn stream(
            &self,
        ) -> Result<Pin<Box<dyn Stream<Item = Result<Targets, Self::Error>> + Send>>, Self::Error>
        {
            use tokio_stream::wrappers::ReceiverStream;

            let (tx, rx) = mpsc::channel(100);

            let configs = self.configs.clone();
            tokio::spawn(async move {
                for config in configs {
                    let result = config.map_err(|e| anyhow::anyhow!(e));
                    if tx.send(result).await.is_err() {
                        break;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
            });

            Ok(Box::pin(ReceiverStream::new(rx)))
        }
    }

    fn create_test_targets(models: Vec<(&str, &str)>) -> Targets {
        let targets_map = Arc::new(DashMap::new());
        for (model, url) in models {
            targets_map.insert(
                model.to_string(),
                Target::builder()
                    .url(url.parse().unwrap())
                    .onwards_key(format!("key-{model}"))
                    .build()
                    .into_pool(),
            );
        }
        Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        }
    }

    #[tokio::test]
    async fn test_config_watcher_updates_targets() {
        let initial_targets = create_test_targets(vec![("gpt-4", "https://api.openai.com")]);

        let updated_targets = create_test_targets(vec![
            ("gpt-4", "https://api.openai.com"),
            ("claude-3", "https://api.anthropic.com"),
        ]);

        let mock_watcher = MockConfigWatcher::with_targets(vec![updated_targets]);

        initial_targets.receive_updates(mock_watcher).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        assert_eq!(initial_targets.targets.len(), 2);
        assert!(initial_targets.targets.contains_key("gpt-4"));
        assert!(initial_targets.targets.contains_key("claude-3"));
    }

    #[tokio::test]
    async fn test_config_watcher_removes_deleted_targets() {
        let initial_targets = create_test_targets(vec![
            ("gpt-4", "https://api.openai.com"),
            ("claude-3", "https://api.anthropic.com"),
        ]);

        let updated_targets = create_test_targets(vec![("gpt-4", "https://api.openai.com")]);

        let mock_watcher = MockConfigWatcher::with_targets(vec![updated_targets]);

        initial_targets.receive_updates(mock_watcher).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        assert_eq!(initial_targets.targets.len(), 1);
        assert!(initial_targets.targets.contains_key("gpt-4"));
        assert!(!initial_targets.targets.contains_key("claude-3"));
    }

    #[tokio::test]
    async fn test_config_watcher_multiple_updates() {
        let targets = Targets {
            targets: Arc::new(DashMap::new()),
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let update1 = create_test_targets(vec![("gpt-4", "https://api.openai.com")]);
        let update2 = create_test_targets(vec![
            ("gpt-4", "https://api.openai.com"),
            ("claude-3", "https://api.anthropic.com"),
        ]);
        let update3 = create_test_targets(vec![("claude-3", "https://api.anthropic.com")]);

        let mock_watcher = MockConfigWatcher::with_targets(vec![update1, update2, update3]);

        targets.receive_updates(mock_watcher).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert_eq!(targets.targets.len(), 1);
        assert!(!targets.targets.contains_key("gpt-4"));
        assert!(targets.targets.contains_key("claude-3"));
    }

    #[tokio::test]
    async fn test_config_watcher_handles_errors() {
        let targets = create_test_targets(vec![("gpt-4", "https://api.openai.com")]);

        let mock_watcher = MockConfigWatcher::with_error("Invalid config file".to_string());

        targets.receive_updates(mock_watcher).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        assert_eq!(targets.targets.len(), 1);
        assert!(targets.targets.contains_key("gpt-4"));
    }

    #[tokio::test]
    async fn test_config_watcher_updates_target_properties() {
        let initial_targets = create_test_targets(vec![("gpt-4", "https://api.openai.com")]);

        let targets_map = Arc::new(DashMap::new());
        targets_map.insert(
            "gpt-4".to_string(),
            Target::builder()
                .url("https://api.openai.com/v2".parse().unwrap())
                .onwards_key("new-key".to_string())
                .onwards_model("gpt-4-turbo".to_string())
                .build()
                .into_pool(),
        );
        let updated_targets = Targets {
            targets: targets_map,
            key_rate_limiters: Arc::new(DashMap::new()),
            key_concurrency_limiters: Arc::new(DashMap::new()),
            key_labels: Arc::new(DashMap::new()),
            key_ids: Arc::new(DashMap::new()),
            accounts: Arc::new(DashMap::new()),
            strict_mode: false,
            http_pool_config: None,
        };

        let mock_watcher = MockConfigWatcher::with_targets(vec![updated_targets]);

        initial_targets.receive_updates(mock_watcher).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let pool = default_pool(&initial_targets, "gpt-4");
        let target = pool.first_target().unwrap();
        assert_eq!(target.url.as_str(), "https://api.openai.com/v2");
        assert_eq!(target.onwards_key, Some("new-key".to_string()));
        assert_eq!(target.onwards_model, Some("gpt-4-turbo".to_string()));
    }

    #[test]
    fn test_from_config_merges_global_keys_with_target_keys() {
        let mut target_keys = HashSet::new();
        target_keys.insert(KeyHash::from("target-key-1".to_string()));
        target_keys.insert(KeyHash::from("target-key-2".to_string()));
        target_keys.insert(KeyHash::from("shared-key".to_string()));

        let mut global_keys = HashSet::new();
        global_keys.insert(KeyHash::from("global-key-1".to_string()));
        global_keys.insert(KeyHash::from("global-key-2".to_string()));
        global_keys.insert(KeyHash::from("shared-key".to_string()));

        let mut targets = HashMap::new();
        targets.insert(
            "test-model".to_string(),
            TargetSpecOrList::Single(
                TargetSpec::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .onwards_key("test-key".to_string())
                    .keys(target_keys)
                    .build(),
            ),
        );

        let config_file = ConfigFile {
            targets,
            auth: Some(Auth {
                global_keys,
                key_definitions: None,
            }),
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();
        let pool = default_pool(&targets, "test-model");

        let pool_keys = pool.keys().unwrap();
        assert_eq!(pool_keys.len(), 5);
        assert!(pool_keys.contains(&KeyHash::from("target-key-1".to_string())));
        assert!(pool_keys.contains(&KeyHash::from("target-key-2".to_string())));
        assert!(pool_keys.contains(&KeyHash::from("global-key-1".to_string())));
        assert!(pool_keys.contains(&KeyHash::from("global-key-2".to_string())));
        assert!(pool_keys.contains(&KeyHash::from("shared-key".to_string())));
    }

    #[test]
    fn test_from_config_target_without_keys_gets_global_keys() {
        let mut global_keys = HashSet::new();
        global_keys.insert(KeyHash::from("global-key-1".to_string()));
        global_keys.insert(KeyHash::from("global-key-2".to_string()));

        let mut targets = HashMap::new();
        targets.insert(
            "test-model".to_string(),
            TargetSpecOrList::Single(
                TargetSpec::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .onwards_key("test-key".to_string())
                    .build(),
            ),
        );

        let config_file = ConfigFile {
            targets,
            auth: Some(Auth {
                global_keys,
                key_definitions: None,
            }),
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();
        let pool = default_pool(&targets, "test-model");

        let pool_keys = pool.keys().unwrap();
        assert_eq!(pool_keys.len(), 2);
        assert!(pool_keys.contains(&KeyHash::from("global-key-1".to_string())));
        assert!(pool_keys.contains(&KeyHash::from("global-key-2".to_string())));
    }

    #[test]
    fn test_target_with_rate_limit_config() {
        let mut targets = HashMap::new();
        targets.insert(
            "test-model".to_string(),
            TargetSpecOrList::Single(
                TargetSpec::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .rate_limit(RateLimitParameters {
                        requests_per_second: NonZeroU32::new(10).unwrap(),
                        burst_size: Some(NonZeroU32::new(20).unwrap()),
                    })
                    .build(),
            ),
        );

        let config_file = ConfigFile {
            targets,
            auth: None,
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();
        let pool = default_pool(&targets, "test-model");
        let target = pool.first_target().unwrap();

        assert!(target.limiter.is_some());
    }

    #[test]
    fn test_target_without_rate_limit_config() {
        let mut targets = HashMap::new();
        targets.insert(
            "test-model".to_string(),
            TargetSpecOrList::Single(
                TargetSpec::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .build(),
            ),
        );

        let config_file = ConfigFile {
            targets,
            auth: None,
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();
        let pool = default_pool(&targets, "test-model");
        let target = pool.first_target().unwrap();

        assert!(target.limiter.is_none());
    }

    #[derive(Debug)]
    struct MockRateLimiter {
        should_allow: std::sync::Mutex<bool>,
    }

    impl MockRateLimiter {
        fn new(should_allow: bool) -> Self {
            Self {
                should_allow: std::sync::Mutex::new(should_allow),
            }
        }

        fn set_should_allow(&self, allow: bool) {
            *self.should_allow.lock().unwrap() = allow;
        }
    }

    impl RateLimiter for MockRateLimiter {
        fn check(&self) -> Result<(), RateLimitExceeded> {
            if *self.should_allow.lock().unwrap() {
                Ok(())
            } else {
                Err(RateLimitExceeded)
            }
        }
    }

    #[test]
    fn test_rate_limiter_trait_allows_requests() {
        let limiter = MockRateLimiter::new(true);
        assert!(limiter.check().is_ok());
    }

    #[test]
    fn test_rate_limiter_trait_blocks_requests() {
        let limiter = MockRateLimiter::new(false);
        assert!(limiter.check().is_err());
    }

    #[test]
    fn test_rate_limiter_trait_can_change_state() {
        let limiter = MockRateLimiter::new(true);
        assert!(limiter.check().is_ok());

        limiter.set_should_allow(false);
        assert!(limiter.check().is_err());

        limiter.set_should_allow(true);
        assert!(limiter.check().is_ok());
    }

    #[test]
    fn test_per_key_rate_limiting_with_actual_key() {
        use std::collections::HashMap;
        use std::num::NonZeroU32;

        let mut key_definitions = HashMap::new();
        key_definitions.insert(
            "basic_user".to_string(),
            KeyDefinition {
                key: KeyHash::of("sk-user-123"),
                rate_limit: Some(RateLimitParameters {
                    requests_per_second: NonZeroU32::new(10).unwrap(),
                    burst_size: Some(NonZeroU32::new(20).unwrap()),
                }),
                concurrency_limit: None,
                labels: HashMap::new(),
            },
        );

        let config_file = ConfigFile {
            targets: HashMap::new(),
            auth: Some(Auth {
                global_keys: std::collections::HashSet::new(),
                key_definitions: Some(key_definitions),
            }),
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();

        assert!(
            targets
                .key_rate_limiters
                .contains_key(&KeyHash::of("sk-user-123"))
        );

        let limiter = targets
            .key_rate_limiters
            .get(&KeyHash::of("sk-user-123"))
            .unwrap();
        assert!(limiter.check().is_ok());
    }

    #[test]
    fn test_per_key_rate_limiting_with_literal_key() {
        use std::collections::HashMap;

        let config_file = ConfigFile {
            targets: HashMap::new(),
            auth: None,
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();

        assert!(
            !targets
                .key_rate_limiters
                .contains_key(&KeyHash::of("sk-literal-key"))
        );
    }

    #[test]
    fn test_per_key_rate_limiting_no_rate_limit_configured() {
        use std::collections::HashMap;

        let mut key_definitions = HashMap::new();
        key_definitions.insert(
            "unlimited_user".to_string(),
            KeyDefinition {
                key: KeyHash::of("sk-unlimited-123"),
                rate_limit: None,
                concurrency_limit: None,
                labels: HashMap::new(),
            },
        );

        let config_file = ConfigFile {
            targets: HashMap::new(),
            auth: Some(Auth {
                global_keys: std::collections::HashSet::new(),
                key_definitions: Some(key_definitions),
            }),
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();

        assert!(
            !targets
                .key_rate_limiters
                .contains_key(&KeyHash::of("sk-unlimited-123"))
        );
    }

    #[test]
    fn test_from_config_no_global_keys() {
        let mut target_keys = HashSet::new();
        target_keys.insert(KeyHash::from("target-key-1".to_string()));
        target_keys.insert(KeyHash::from("target-key-2".to_string()));

        let mut targets = HashMap::new();
        targets.insert(
            "model-with-keys".to_string(),
            TargetSpecOrList::Single(
                TargetSpec::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .onwards_key("test-key".to_string())
                    .keys(target_keys)
                    .build(),
            ),
        );
        targets.insert(
            "model-without-keys".to_string(),
            TargetSpecOrList::Single(
                TargetSpec::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .onwards_key("test-key".to_string())
                    .build(),
            ),
        );

        let config_file = ConfigFile {
            targets,
            auth: None,
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();

        let pool_with_keys = default_pool(&targets, "model-with-keys");
        let pool_keys = pool_with_keys.keys().unwrap();
        assert_eq!(pool_keys.len(), 2);
        assert!(pool_keys.contains(&KeyHash::from("target-key-1".to_string())));
        assert!(pool_keys.contains(&KeyHash::from("target-key-2".to_string())));

        let pool_without_keys = default_pool(&targets, "model-without-keys");
        assert!(pool_without_keys.keys().is_none());
    }

    #[test]
    fn test_normalize_url_adds_trailing_slash() {
        let url_without_slash: Url = "https://api.example.com/v1".parse().unwrap();
        let normalized = super::normalize_url(url_without_slash);
        assert_eq!(normalized.as_str(), "https://api.example.com/v1/");

        let url_with_slash: Url = "https://api.example.com/v1/".parse().unwrap();
        let normalized = super::normalize_url(url_with_slash);
        assert_eq!(normalized.as_str(), "https://api.example.com/v1/");

        let root_url: Url = "https://api.example.com".parse().unwrap();
        let normalized = super::normalize_url(root_url);
        assert_eq!(normalized.as_str(), "https://api.example.com/");
    }

    #[test]
    fn test_url_joining_after_normalization() {
        let base_url: Url = "https://api.example.com/v1".parse().unwrap();
        let normalized = super::normalize_url(base_url);

        let joined = normalized.join("messages").unwrap();
        assert_eq!(joined.as_str(), "https://api.example.com/v1/messages");

        let normalized_again: Url = "https://api.example.com/v1".parse().unwrap();
        let normalized_again = super::normalize_url(normalized_again);
        let joined_with_slash = normalized_again.join("messages/create").unwrap();
        assert_eq!(
            joined_with_slash.as_str(),
            "https://api.example.com/v1/messages/create"
        );
    }

    #[test]
    fn test_target_spec_conversion_normalizes_url() {
        let target_spec = TargetSpec::builder()
            .url("https://api.example.com/v1".parse().unwrap())
            .onwards_key("test-key".to_string())
            .build();

        let target: Target = target_spec.into();

        assert_eq!(target.url.as_str(), "https://api.example.com/v1/");
    }

    #[test]
    fn test_target_with_concurrency_limit_config() {
        let mut targets = HashMap::new();
        targets.insert(
            "test-model".to_string(),
            TargetSpecOrList::Single(
                TargetSpec::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .concurrency_limit(ConcurrencyLimitParameters {
                        max_concurrent_requests: 5,
                    })
                    .weight(1)
                    .build(),
            ),
        );

        let config_file = ConfigFile {
            targets,
            auth: None,
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();
        let pool = default_pool(&targets, "test-model");

        let guards: Vec<_> = (0..5).filter_map(|_| pool.select()).collect();
        assert_eq!(guards.len(), 5);
        assert!(pool.select().is_none());
    }

    #[test]
    fn test_target_without_concurrency_limit_config() {
        let mut targets = HashMap::new();
        targets.insert(
            "test-model".to_string(),
            TargetSpecOrList::Single(
                TargetSpec::builder()
                    .url("https://api.example.com".parse().unwrap())
                    .weight(1)
                    .build(),
            ),
        );

        let config_file = ConfigFile {
            targets,
            auth: None,
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();
        let pool = default_pool(&targets, "test-model");

        let guards: Vec<_> = (0..100).filter_map(|_| pool.select()).collect();
        assert_eq!(guards.len(), 100);
    }

    #[test]
    fn test_concurrency_limiter_allows_requests() {
        let limiter = ConcurrencyLimiter::with_limit(2);

        let guard1 = limiter.try_acquire();
        assert!(guard1.is_some());

        let guard2 = limiter.try_acquire();
        assert!(guard2.is_some());
    }

    #[test]
    fn test_concurrency_limiter_blocks_at_capacity() {
        let limiter = ConcurrencyLimiter::with_limit(2);

        let _guard1 = limiter.try_acquire().unwrap();
        let _guard2 = limiter.try_acquire().unwrap();

        let guard3 = limiter.try_acquire();
        assert!(guard3.is_none());
    }

    #[test]
    fn test_concurrency_limiter_releases_on_drop() {
        let limiter = ConcurrencyLimiter::with_limit(1);

        {
            let _guard = limiter.try_acquire().unwrap();

            assert!(limiter.try_acquire().is_none());
        }

        let guard = limiter.try_acquire();
        assert!(guard.is_some());
    }

    #[test]
    fn test_per_key_concurrency_limiting_configured() {
        use std::collections::HashMap;

        let mut key_definitions = HashMap::new();
        key_definitions.insert(
            "limited_user".to_string(),
            KeyDefinition {
                key: KeyHash::of("sk-limited-123"),
                rate_limit: None,
                concurrency_limit: Some(ConcurrencyLimitParameters {
                    max_concurrent_requests: 3,
                }),
                labels: HashMap::new(),
            },
        );

        let config_file = ConfigFile {
            targets: HashMap::new(),
            auth: Some(Auth {
                global_keys: std::collections::HashSet::new(),
                key_definitions: Some(key_definitions),
            }),
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();

        assert!(
            targets
                .key_concurrency_limiters
                .contains_key(&KeyHash::of("sk-limited-123"))
        );
    }

    #[test]
    fn test_per_key_concurrency_limiting_not_configured() {
        use std::collections::HashMap;

        let mut key_definitions = HashMap::new();
        key_definitions.insert(
            "unlimited_user".to_string(),
            KeyDefinition {
                key: KeyHash::of("sk-unlimited-456"),
                rate_limit: None,
                concurrency_limit: None,
                labels: HashMap::new(),
            },
        );

        let config_file = ConfigFile {
            targets: HashMap::new(),
            auth: Some(Auth {
                global_keys: std::collections::HashSet::new(),
                key_definitions: Some(key_definitions),
            }),
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();

        assert!(
            !targets
                .key_concurrency_limiters
                .contains_key(&KeyHash::of("sk-unlimited-456"))
        );
    }

    #[test]
    fn test_fallback_config_wildcard_status_codes() {
        let config = FallbackConfig {
            enabled: true,
            on_status: vec![5, 429],
            on_rate_limit: false,
            ..Default::default()
        };

        assert!(config.should_fallback_on_status(500));
        assert!(config.should_fallback_on_status(502));
        assert!(config.should_fallback_on_status(503));
        assert!(config.should_fallback_on_status(599));

        assert!(config.should_fallback_on_status(429));

        assert!(!config.should_fallback_on_status(400));
        assert!(!config.should_fallback_on_status(404));
        assert!(!config.should_fallback_on_status(200));
    }

    #[test]
    fn test_fallback_config_two_digit_wildcard() {
        let config = FallbackConfig {
            enabled: true,
            on_status: vec![50, 52],
            on_rate_limit: false,
            ..Default::default()
        };

        assert!(config.should_fallback_on_status(500));
        assert!(config.should_fallback_on_status(504));
        assert!(config.should_fallback_on_status(509));
        assert!(!config.should_fallback_on_status(510));

        assert!(config.should_fallback_on_status(520));
        assert!(config.should_fallback_on_status(522));
        assert!(!config.should_fallback_on_status(530));
    }

    #[test]
    fn test_fallback_config_disabled_ignores_status() {
        let config = FallbackConfig {
            enabled: false,
            on_status: vec![5, 429],
            on_rate_limit: true,
            ..Default::default()
        };

        assert!(!config.should_fallback_on_status(500));
        assert!(!config.should_fallback_on_status(429));
    }

    #[test]
    fn test_backwards_compat_single_target_config() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "url": "https://api.openai.com",
                    "onwards_key": "sk-test"
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "gpt-4");
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.strategy(), LoadBalanceStrategy::WeightedRandom);
        assert!(!pool.fallback_enabled());
    }

    #[test]
    fn test_backwards_compat_list_format_config() {
        let json = r#"{
            "targets": {
                "gpt-4": [
                    { "url": "https://api1.example.com", "weight": 3 },
                    { "url": "https://api2.example.com", "weight": 1 }
                ]
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "gpt-4");
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.strategy(), LoadBalanceStrategy::WeightedRandom);
    }

    #[test]
    fn test_pool_config_with_strategy() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "strategy": "priority",
                    "fallback": {
                        "enabled": true,
                        "on_status": [429, 5],
                        "on_rate_limit": true
                    },
                    "providers": [
                        { "url": "https://primary.example.com" },
                        { "url": "https://backup.example.com" }
                    ]
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "gpt-4");
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.strategy(), LoadBalanceStrategy::Priority);
        assert!(pool.fallback_enabled());
        assert!(pool.should_fallback_on_status(429));
        assert!(pool.should_fallback_on_status(500));
        assert!(pool.should_fallback_on_rate_limit());
    }

    #[test]
    fn test_pool_config_weighted_random_strategy() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "strategy": "weighted_random",
                    "providers": [
                        { "url": "https://api1.example.com", "weight": 3 },
                        { "url": "https://api2.example.com", "weight": 1 }
                    ]
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "gpt-4");
        assert_eq!(pool.strategy(), LoadBalanceStrategy::WeightedRandom);
    }

    #[test]
    fn test_into_pool_preserves_sanitize_response() {
        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .sanitize_response(true)
            .build();

        let pool = target.into_pool();

        let (_, first_target, _guard) = pool.default_pool().select_iter().next().unwrap();
        assert!(
            first_target.sanitize_response,
            "into_pool should preserve sanitize_response setting"
        );
    }

    #[test]
    fn test_into_pool_preserves_sanitize_response_disabled() {
        let target = Target::builder()
            .url("https://api.example.com".parse().unwrap())
            .build();

        let pool = target.into_pool();

        let (_, first_target, _guard) = pool.default_pool().select_iter().next().unwrap();
        assert!(
            !first_target.sanitize_response,
            "into_pool should preserve default sanitize_response setting"
        );
    }

    #[test]
    fn test_single_target_config_with_timeout() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "url": "https://api.openai.com/v1/",
                    "onwards_key": "sk-test-key",
                    "request_timeout_secs": 30
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "gpt-4");
        let target = pool.first_target().unwrap();
        assert_eq!(target.request_timeout_secs, Some(30));
    }

    #[test]
    fn test_trusted_field_defaults_to_false() {
        let json = r#"{
            "targets": {
                "test-model": {
                    "url": "https://api.example.com",
                    "onwards_key": "sk-test"
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "test-model");
        assert!(!pool.is_trusted(), "trusted should default to false");
    }

    #[test]
    fn test_trusted_field_set_to_true() {
        let json = r#"{
            "targets": {
                "test-model": {
                    "url": "https://api.example.com",
                    "onwards_key": "sk-test",
                    "trusted": true
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "test-model");
        assert!(
            pool.is_trusted(),
            "trusted should be true when explicitly set"
        );
    }

    #[test]
    fn inflight_limits_parse_onto_the_alias() {
        let config: ConfigFile = serde_json::from_value(serde_json::json!({
            "auth": {
                "global_keys": [],
                "key_definitions": {
                    "acme_backend": { "key": "sk-acme-1", "labels": { "account": "acme" } }
                }
            },
            "targets": {
                "gpt-4": {
                    "inflight_limit": 20,
                    "batch_inflight_limit": 7,
                    "account_inflight_limits": { "acme": 200 },
                    "providers": [{ "url": "https://api.openai.com", "onwards_key": "sk-upstream" }]
                },
                "unlimited": {
                    "providers": [{ "url": "https://api.openai.com" }]
                }
            }
        }))
        .unwrap();
        let targets = Targets::from_config(config).unwrap();

        let limits = targets
            .targets
            .get("gpt-4")
            .and_then(|pools| pools.default_pool().inflight_limits().cloned())
            .unwrap();
        assert_eq!(limits.for_account("acme"), 200);
        assert_eq!(limits.for_account("someone-else"), 20);
        assert_eq!(
            targets
                .targets
                .get("gpt-4")
                .unwrap()
                .default_pool()
                .batch_inflight_limit(),
            Some(7)
        );
        assert!(
            targets
                .targets
                .get("unlimited")
                .unwrap()
                .default_pool()
                .inflight_limits()
                .is_none()
        );
        assert!(
            targets
                .targets
                .get("unlimited")
                .unwrap()
                .default_pool()
                .batch_inflight_limit()
                .is_none()
        );
    }

    #[test]
    fn test_trusted_field_preserved_in_pool_conversion() {
        let pool_spec = PoolSpec {
            keys: None,
            rate_limit: None,
            concurrency_limit: None,
            response_headers: None,
            fallback: None,
            strategy: LoadBalanceStrategy::default(),
            sanitize_response: false,
            trusted: true,
            routing_rules: Vec::new(),
            providers: vec![ProviderSpec {
                url: "https://api.example.com".parse().unwrap(),
                onwards_key: None,
                onwards_model: None,
                rate_limit: None,
                concurrency_limit: None,
                upstream_auth_header_name: None,
                upstream_auth_header_prefix: None,
                response_headers: None,
                weight: 1,
                sanitize_response: false,
                request_timeout_secs: None,
                trusted: None,
                propagate_trace_context: None,
                reasoning_translation: None,
                accepts_scheduling_priority: false,
                kind: Default::default(),
            }],
            overlays: Default::default(),
            inflight_limit: None,
            batch_inflight_limit: None,
            class_route_of: None,
            account_inflight_limits: Default::default(),
        };

        let pool_config = TargetSpecOrList::Pool(pool_spec)
            .into_pool_config()
            .unwrap();
        assert!(
            pool_config.trusted,
            "PoolSpec conversion should preserve trusted field"
        );

        let json = r#"{
            "targets": {
                "test-model": {
                    "url": "https://api.example.com",
                    "trusted": true
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "test-model");
        assert!(
            pool.is_trusted(),
            "Single TargetSpec with trusted: true should create trusted pool"
        );
    }

    #[test]
    fn test_legacy_list_mixed_trusted_values_rejected() {
        let json = r#"{
            "targets": {
                "test-model": [
                    {
                        "url": "https://api.example1.com",
                        "trusted": true
                    },
                    {
                        "url": "https://api.example2.com",
                        "trusted": false
                    }
                ]
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let result = Targets::from_config(config);

        assert!(
            result.is_err(),
            "Config with mixed trusted values in legacy list should be rejected"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("same 'trusted' value"),
            "Error message should mention trusted value mismatch, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_legacy_list_carries_per_provider_propagate_trace_context() {
        let list: TargetSpecOrList = serde_json::from_str(
            r#"[
                { "url": "https://provider1.example.com", "propagate_trace_context": true },
                { "url": "https://provider2.example.com", "propagate_trace_context": false },
                { "url": "https://provider3.example.com" }
            ]"#,
        )
        .unwrap();

        let pool_config = list.into_pool_config().unwrap();
        assert_eq!(pool_config.providers.len(), 3);
        assert_eq!(
            pool_config.providers[0].propagate_trace_context,
            Some(true),
            "explicit true must survive"
        );
        assert_eq!(
            pool_config.providers[1].propagate_trace_context,
            Some(false),
            "explicit false must survive"
        );
        assert_eq!(
            pool_config.providers[2].propagate_trace_context, None,
            "unset stays None (inherits resolved trusted)"
        );
    }

    #[test]
    fn test_single_target_config_without_timeout() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "url": "https://api.openai.com/v1/",
                    "onwards_key": "sk-test-key"
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "gpt-4");
        let target = pool.first_target().unwrap();
        assert_eq!(target.request_timeout_secs, None);
    }

    #[test]
    fn test_pool_config_with_timeout() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "providers": [
                        {
                            "url": "https://api.openai.com/v1/",
                            "onwards_key": "sk-test-key-1",
                            "request_timeout_secs": 30,
                            "weight": 1
                        },
                        {
                            "url": "https://api.azure.com/v1/",
                            "onwards_key": "sk-test-key-2",
                            "request_timeout_secs": 60,
                            "weight": 1
                        }
                    ],
                    "fallback": {
                        "enabled": true,
                        "on_status": [502, 503]
                    }
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "gpt-4");
        assert_eq!(pool.len(), 2);

        let providers = pool.providers();

        let provider1 = providers
            .iter()
            .find(|p| p.target.url.host_str() == Some("api.openai.com"))
            .unwrap();
        assert_eq!(provider1.target.request_timeout_secs, Some(30));

        let provider2 = providers
            .iter()
            .find(|p| p.target.url.host_str() == Some("api.azure.com"))
            .unwrap();
        assert_eq!(provider2.target.request_timeout_secs, Some(60));
    }

    #[test]
    fn test_pool_config_mixed_timeouts() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "providers": [
                        {
                            "url": "https://api.openai.com/v1/",
                            "onwards_key": "sk-test-key-1",
                            "request_timeout_secs": 30,
                            "weight": 1
                        },
                        {
                            "url": "https://api.azure.com/v1/",
                            "onwards_key": "sk-test-key-2",
                            "weight": 1
                        }
                    ]
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "gpt-4");
        let providers = pool.providers();

        assert!(
            providers
                .iter()
                .any(|p| p.target.request_timeout_secs == Some(30))
        );
        assert!(
            providers
                .iter()
                .any(|p| p.target.request_timeout_secs.is_none())
        );
    }

    #[test]
    fn test_trusted_field_in_pool_config() {
        let json_trusted = r#"{
            "targets": {
                "trusted-pool": {
                    "trusted": true,
                    "providers": [
                        {
                            "url": "https://api1.example.com"
                        },
                        {
                            "url": "https://api2.example.com"
                        }
                    ]
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json_trusted).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "trusted-pool");
        assert!(
            pool.is_trusted(),
            "Pool with trusted: true should be trusted"
        );
        assert_eq!(pool.len(), 2, "Pool should have 2 providers");

        let json_untrusted = r#"{
            "targets": {
                "untrusted-pool": {
                    "providers": [
                        {
                            "url": "https://api1.example.com"
                        },
                        {
                            "url": "https://api2.example.com"
                        }
                    ]
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json_untrusted).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "untrusted-pool");
        assert!(
            !pool.is_trusted(),
            "Pool without trusted flag should default to false"
        );
    }

    #[test]
    fn test_provider_spec_trusted_defaults_to_none() {
        let json = r#"{"url": "https://example.com"}"#;
        let spec: ProviderSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.trusted, None);
    }

    #[test]
    fn test_provider_spec_trusted_some_true() {
        let json = r#"{"url": "https://example.com", "trusted": true}"#;
        let spec: ProviderSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.trusted, Some(true));
    }

    #[test]
    fn test_provider_spec_trusted_some_false() {
        let json = r#"{"url": "https://example.com", "trusted": false}"#;
        let spec: ProviderSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.trusted, Some(false));
    }

    #[test]
    fn test_target_from_provider_spec_propagates_trusted() {
        let spec: ProviderSpec =
            serde_json::from_str(r#"{"url": "https://example.com", "trusted": true}"#).unwrap();
        let target: Target = spec.into();
        assert_eq!(target.trusted, Some(true));
    }

    #[test]
    fn test_target_from_provider_spec_trusted_none() {
        let spec: ProviderSpec = serde_json::from_str(r#"{"url": "https://example.com"}"#).unwrap();
        let target: Target = spec.into();
        assert_eq!(target.trusted, None);
    }

    #[test]
    fn test_per_provider_trusted_parsed_from_config() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "trusted": false,
                    "providers": [
                        {"url": "https://internal.example.com", "trusted": true},
                        {"url": "https://external.example.com"}
                    ]
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();
        let pool = default_pool(&targets, "gpt-4");

        assert!(!pool.is_trusted(), "Pool-level trusted should be false");

        let providers = pool.providers();
        assert_eq!(
            providers[0].target.trusted,
            Some(true),
            "First provider should have trusted=Some(true)"
        );
        assert_eq!(
            providers[1].target.trusted, None,
            "Second provider should have trusted=None (inherits pool)"
        );
    }

    #[test]
    fn test_routing_rules_deserialized_from_config() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "routing_rules": [
                        {
                            "match_labels": {"purpose": "playground"},
                            "action": {"type": "deny"}
                        },
                        {
                            "match_labels": {"purpose": "batch"},
                            "action": {"type": "redirect", "target": "gpt-4o-mini"}
                        }
                    ],
                    "providers": [
                        { "url": "https://api.openai.com/v1/" }
                    ]
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "gpt-4");
        let rules = pool.routing_rules();
        assert_eq!(rules.len(), 2);

        assert_eq!(rules[0].match_labels.get("purpose").unwrap(), "playground");
        assert!(matches!(rules[0].action, RoutingAction::Deny));

        assert_eq!(rules[1].match_labels.get("purpose").unwrap(), "batch");
        match &rules[1].action {
            RoutingAction::Redirect { target } => assert_eq!(target, "gpt-4o-mini"),
            other => panic!("Expected Redirect, got {:?}", other),
        }
    }

    #[test]
    fn test_key_labels_wired_through_config() {
        let mut key_definitions = HashMap::new();
        key_definitions.insert(
            "batch_user".to_string(),
            KeyDefinition {
                key: KeyHash::of("sk-batch-123"),
                rate_limit: None,
                concurrency_limit: None,
                labels: HashMap::from([("purpose".to_string(), "batch".to_string())]),
            },
        );
        key_definitions.insert(
            "playground_user".to_string(),
            KeyDefinition {
                key: KeyHash::of("sk-play-456"),
                rate_limit: None,
                concurrency_limit: None,
                labels: HashMap::from([("purpose".to_string(), "playground".to_string())]),
            },
        );
        key_definitions.insert(
            "no_labels_user".to_string(),
            KeyDefinition {
                key: KeyHash::of("sk-nolabel-789"),
                rate_limit: None,
                concurrency_limit: None,
                labels: HashMap::new(),
            },
        );

        let config_file = ConfigFile {
            targets: HashMap::new(),
            auth: Some(Auth {
                global_keys: std::collections::HashSet::new(),
                key_definitions: Some(key_definitions),
            }),
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        };

        let targets = Targets::from_config(config_file).unwrap();

        let batch_labels = targets
            .key_labels
            .get(&KeyHash::of("sk-batch-123"))
            .unwrap();
        assert_eq!(batch_labels.get("purpose").unwrap(), "batch");

        let play_labels = targets.key_labels.get(&KeyHash::of("sk-play-456")).unwrap();
        assert_eq!(play_labels.get("purpose").unwrap(), "playground");

        assert!(
            !targets
                .key_labels
                .contains_key(&KeyHash::of("sk-nolabel-789"))
        );
    }

    fn targets_with_labelled_keys(keys: &[(&str, &str)]) -> Targets {
        let key_definitions = keys
            .iter()
            .map(|(key, id)| {
                (
                    id.to_string(),
                    KeyDefinition {
                        key: KeyHash::of(key),
                        rate_limit: None,
                        concurrency_limit: None,
                        labels: HashMap::from([(API_KEY_ID_LABEL.to_string(), id.to_string())]),
                    },
                )
            })
            .collect();
        Targets::from_config(ConfigFile {
            targets: HashMap::new(),
            auth: Some(Auth {
                global_keys: std::collections::HashSet::new(),
                key_definitions: Some(key_definitions),
            }),
            strict_mode: false,
            http_pool: None,
            accounts: Default::default(),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn test_key_ids_index_keys_and_follow_updates() {
        let targets = targets_with_labelled_keys(&[("sk-a", "id-a"), ("sk-b", "id-b")]);
        assert_eq!(
            targets.key_ids.get("id-a").map(|h| *h),
            Some(KeyHash::of("sk-a"))
        );
        assert_eq!(
            targets.key_ids.get("id-b").map(|h| *h),
            Some(KeyHash::of("sk-b"))
        );

        let updated = targets_with_labelled_keys(&[("sk-a", "id-a"), ("sk-c", "id-c")]);
        targets
            .receive_updates(MockConfigWatcher::with_targets(vec![updated]))
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
        while !targets.key_ids.contains_key("id-c") {
            assert!(
                tokio::time::Instant::now() < deadline,
                "update never applied"
            );
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        }
        assert_eq!(
            targets.key_ids.get("id-a").map(|h| *h),
            Some(KeyHash::of("sk-a"))
        );
        assert_eq!(
            targets.key_ids.get("id-c").map(|h| *h),
            Some(KeyHash::of("sk-c"))
        );
        assert!(!targets.key_ids.contains_key("id-b"));
    }

    #[test]
    fn test_routing_rules_empty_by_default() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "url": "https://api.openai.com",
                    "onwards_key": "sk-test"
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let targets = Targets::from_config(config).unwrap();

        let pool = default_pool(&targets, "gpt-4");
        assert!(pool.routing_rules().is_empty());
    }

    fn backoff(initial_ms: u64, max_ms: u64, factor: f64, jitter: JitterStrategy) -> BackoffConfig {
        BackoffConfig {
            initial_ms,
            max_ms,
            factor,
            jitter,
        }
    }

    #[test]
    fn test_backoff_first_retry_is_initial() {
        let b = backoff(100, 5_000, 2.0, JitterStrategy::None);
        assert_eq!(b.base_delay_ms(1), 100);
    }

    #[test]
    fn test_backoff_exponential_growth() {
        let b = backoff(100, 5_000, 2.0, JitterStrategy::None);
        assert_eq!(b.base_delay_ms(1), 100);
        assert_eq!(b.base_delay_ms(2), 200);
        assert_eq!(b.base_delay_ms(3), 400);
        assert_eq!(b.base_delay_ms(4), 800);
        assert_eq!(b.base_delay_ms(5), 1_600);
        assert_eq!(b.base_delay_ms(6), 3_200);
    }

    #[test]
    fn test_backoff_clamped_to_max_ms() {
        let b = backoff(100, 1_000, 2.0, JitterStrategy::None);
        assert_eq!(b.base_delay_ms(5), 1_000);
        assert_eq!(b.base_delay_ms(20), 1_000);
    }

    #[test]
    fn test_backoff_zero_retry_index_is_zero() {
        let b = backoff(100, 5_000, 2.0, JitterStrategy::None);
        assert_eq!(b.base_delay_ms(0), 0);
    }

    #[test]
    fn test_backoff_extreme_exponent_stays_bounded() {
        let b = backoff(1_000, 10_000, 10.0, JitterStrategy::None);
        assert_eq!(b.base_delay_ms(100), 10_000);
        assert_eq!(b.base_delay_ms(u32::MAX), 10_000);
    }

    #[test]
    fn test_backoff_factor_one_is_constant() {
        let b = backoff(250, 5_000, 1.0, JitterStrategy::None);
        for retry in 1..=10 {
            assert_eq!(b.base_delay_ms(retry), 250);
        }
    }

    #[test]
    fn test_backoff_full_jitter_within_bounds() {
        let b = backoff(200, 5_000, 2.0, JitterStrategy::Full);
        for _ in 0..256 {
            let d = b.delay(3).as_millis() as u64;
            assert!(d <= 800, "jittered delay {d} exceeded base 800");
        }
    }

    #[test]
    fn test_backoff_none_jitter_is_deterministic() {
        let b = backoff(150, 5_000, 2.0, JitterStrategy::None);
        let first = b.delay(2);
        for _ in 0..16 {
            assert_eq!(b.delay(2), first);
        }
        assert_eq!(first, std::time::Duration::from_millis(300));
    }

    #[test]
    fn test_backoff_serde_defaults() {
        let json = r#"{ "initial_ms": 100 }"#;
        let b: BackoffConfig = serde_json::from_str(json).unwrap();
        assert_eq!(b.initial_ms, 100);
        assert_eq!(b.max_ms, 5_000);
        assert!((b.factor - 2.0).abs() < f64::EPSILON);
        assert_eq!(b.jitter, JitterStrategy::Full);
    }

    #[test]
    fn test_fallback_config_backoff_unset_by_default() {
        let f: FallbackConfig = serde_json::from_str("{}").unwrap();
        assert!(f.backoff.is_none());
        assert!(f.max_total_backoff_ms.is_none());
    }

    #[test]
    fn test_fallback_config_with_backoff_and_budget() {
        let json = r#"{
            "enabled": true,
            "on_status": [502, 503],
            "max_attempts": 4,
            "max_total_backoff_ms": 3000,
            "backoff": {
                "initial_ms": 100,
                "max_ms": 1500,
                "factor": 2.0,
                "jitter": "none"
            }
        }"#;
        let f: FallbackConfig = serde_json::from_str(json).unwrap();
        assert!(f.enabled);
        assert_eq!(f.max_total_backoff_ms, Some(3000));
        let b = f.backoff.expect("backoff parsed");
        assert_eq!(b.initial_ms, 100);
        assert_eq!(b.max_ms, 1500);
        assert_eq!(b.jitter, JitterStrategy::None);
    }

    #[test]
    fn test_from_config_rejects_invalid_reasoning_translation() {
        let config: ConfigFile = serde_json::from_value(serde_json::json!({
            "targets": {
                "reasoning-model": {
                    "url": "https://provider.example.com/v1",
                    "reasoning_translation": {
                        "chat_completions": {
                            "unsupported_efforts": ["minimal", "low", "medium", "high", "xhigh", "max"],
                            "writes": [{
                                "target_path": "/thinking_token_budget",
                                "values": {"none": 0}
                            }]
                        }
                    }
                }
            }
        }))
        .unwrap();

        let error = Targets::from_config(config).unwrap_err();

        assert!(error.to_string().contains("thinking_token_budget"));
        assert!(error.to_string().contains("reasoning_effort"));
    }


    #[test]
    fn an_existing_config_becomes_a_single_default_pool() {
        let json = r#"{
            "targets": {
                "gpt-4": {
                    "providers": [
                        {"url": "https://dynamo.example.com/"},
                        {"url": "https://openrouter.example.com/"}
                    ]
                }
            }
        }"#;
        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let pools = config.targets["gpt-4"].clone().into_pool_configs().unwrap();
        assert_eq!(pools.len(), 1);
        assert_eq!(pools[0].0, DEFAULT_POOL);
        assert_eq!(pools[0].1.providers.len(), 2);

        let targets = Targets::from_config(serde_json::from_str(json).unwrap()).unwrap();
        let entry = targets.targets.get("gpt-4").unwrap();
        assert_eq!(entry.pool_count(), 1);
        for class in [RequestClass::Normal, RequestClass::Completions] {
            assert_eq!(entry.resolve(class).len(), 2);
            assert!(entry.resolved_name(class).is_none());
        }
    }

    #[test]
    fn an_existing_config_re_serializes_without_a_pools_wrapper() {
        let json = r#"{
            "targets": {
                "pool-form": {
                    "keys": ["sk-a"],
                    "strategy": "priority",
                    "providers": [
                        {"url": "https://dynamo.example.com/"},
                        {"url": "https://openrouter.example.com/"}
                    ]
                },
                "list-form": [{"url": "https://a.example.com/"}],
                "single-form": {"url": "https://b.example.com/"}
            },
            "strict_mode": false
        }"#;
        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_value(&config).unwrap();

        let targets = &serialized["targets"];
        assert!(targets["pool-form"]["providers"].is_array());
        assert!(targets["pool-form"].get("pools").is_none());
        assert!(targets["list-form"].is_array());
        assert!(targets["single-form"]["url"].is_string());
        assert!(
            !serialized.to_string().contains("\"pools\""),
            "no existing target may serialize as a named-pool map: {serialized}"
        );

        let reparsed: ConfigFile = serde_json::from_value(serialized).unwrap();
        for name in ["pool-form", "list-form", "single-form"] {
            let pools = reparsed.targets[name].clone().into_pool_configs().unwrap();
            assert_eq!(pools.len(), 1, "{name}");
            assert_eq!(pools[0].0, DEFAULT_POOL, "{name}");
        }
        let pool_form = reparsed.targets["pool-form"]
            .clone()
            .into_pool_configs()
            .unwrap();
        let urls: Vec<_> = pool_form[0]
            .1
            .providers
            .iter()
            .map(|p| p.url.to_string())
            .collect();
        assert_eq!(
            urls,
            vec![
                "https://dynamo.example.com/",
                "https://openrouter.example.com/"
            ]
        );
        assert_eq!(pool_form[0].1.strategy, LoadBalanceStrategy::Priority);
        assert_eq!(pool_form[0].1.keys.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn a_pools_target_parses_into_named_pools() {
        let json = r#"{
            "targets": {
                "dsv4-flash": {
                    "pools": {
                        "default": {
                            "strategy": "priority",
                            "providers": [
                                {"url": "https://dynamo.example.com/"},
                                {"url": "https://openrouter.example.com/"}
                            ]
                        },
                        "completions": {
                            "strategy": "priority",
                            "providers": [
                                {"url": "https://dynamo.example.com/"},
                                {"url": "https://fireworks.example.com/"}
                            ]
                        }
                    }
                }
            }
        }"#;
        let targets = Targets::from_config(serde_json::from_str(json).unwrap()).unwrap();
        let entry = targets.targets.get("dsv4-flash").unwrap();
        assert_eq!(entry.pool_count(), 2);

        let urls = |class| {
            entry
                .resolve(class)
                .providers()
                .iter()
                .map(|p| p.target.url.to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            urls(RequestClass::Normal),
            vec![
                "https://dynamo.example.com/",
                "https://openrouter.example.com/"
            ]
        );
        assert_eq!(
            urls(RequestClass::Completions),
            vec![
                "https://dynamo.example.com/",
                "https://fireworks.example.com/"
            ]
        );
        assert_eq!(
            entry.resolved_name(RequestClass::Completions),
            Some(COMPLETIONS_POOL)
        );
        assert!(entry.resolved_name(RequestClass::Normal).is_none());
    }

    #[test]
    fn affinity_is_validated_and_kept_only_on_the_default_pool() {
        let config = |strategy: &str, affinity: &str| {
            let pool = format!(
                r#"{{
                    "strategy": "{strategy}",
                    "fallback": {{"enabled": true, "on_status": [5], "affinity": {affinity}}},
                    "providers": [
                        {{"url": "https://hosted.example.com/"}},
                        {{"url": "https://overflow.example.com/"}}
                    ]
                }}"#
            );
            format!(
                r#"{{"targets": {{"m": {{"pools": {{"default": {pool}, "completions": {pool}}}}}}}}}"#
            )
        };
        let build = |json: String| Targets::from_config(serde_json::from_str(&json).unwrap());

        let targets = build(config("priority", r#"{"target_conversations": 10}"#)).unwrap();
        let entry = targets.targets.get("m").unwrap();
        assert!(entry.resolve(RequestClass::Normal).affinity_enabled());
        assert!(!entry.resolve(RequestClass::Completions).affinity_enabled());

        assert!(build(config("priority", r#"{"target_conversations": 0}"#)).is_err());
        assert!(build(config("weighted_random", r#"{"target_conversations": 10}"#)).is_err());
        assert!(build(config("weighted_random", r#"{"enabled": false}"#)).is_ok());
    }

    #[test]
    fn a_named_pool_inherits_the_default_pools_access_control() {
        let json = r#"{
            "targets": {
                "dsv4-flash": {
                    "pools": {
                        "default": {
                            "keys": ["sk-alias"],
                            "routing_rules": [
                                {"match_labels": {"purpose": "batch"}, "action": {"type": "deny"}}
                            ],
                            "providers": [{"url": "https://dynamo.example.com/"}]
                        },
                        "completions": {
                            "providers": [{"url": "https://fireworks.example.com/"}]
                        }
                    }
                }
            }
        }"#;
        let targets = Targets::from_config(serde_json::from_str(json).unwrap()).unwrap();
        let entry = targets.targets.get("dsv4-flash").unwrap();
        let completions = entry.resolve(RequestClass::Completions);
        assert_eq!(
            completions.keys().unwrap().len(),
            1,
            "completions pool must be behind the same keys"
        );
        assert_eq!(completions.routing_rules().len(), 1);
    }

    #[test]
    fn a_named_pool_inherits_deny_rules() {
        let json = r#"{
            "targets": {
                "dsv4-flash": {
                    "pools": {
                        "default": {
                            "routing_rules": [
                                {"match_labels": {"purpose": "batch"}, "action": {"type": "deny"}}
                            ],
                            "providers": [{"url": "https://dynamo.example.com/"}]
                        },
                        "completions": {
                            "providers": [{"url": "https://fireworks.example.com/"}]
                        }
                    }
                }
            }
        }"#;
        let targets = Targets::from_config(serde_json::from_str(json).unwrap()).unwrap();
        let entry = targets.targets.get("dsv4-flash").unwrap();
        let batch = HashMap::from([("purpose".to_string(), "batch".to_string())]);

        for class in [RequestClass::Normal, RequestClass::Completions] {
            assert!(
                matches!(
                    entry.resolve(class).evaluate_routing_rules(&batch),
                    Some(RoutingAction::Deny)
                ),
                "{class:?} traffic must hit the alias's deny rule"
            );
        }
        let realtime = HashMap::from([("purpose".to_string(), "realtime".to_string())]);
        assert!(
            entry
                .resolve(RequestClass::Completions)
                .evaluate_routing_rules(&realtime)
                .is_none()
        );
    }

    #[test]
    fn a_named_pool_does_not_inherit_redirect_rules() {
        let json = r#"{
            "targets": {
                "dsv4-flash": {
                    "pools": {
                        "default": {
                            "routing_rules": [
                                {"match_labels": {"purpose": "batch"}, "action": {"type": "redirect", "target": "dsv4-flash-batch"}}
                            ],
                            "providers": [{"url": "https://dynamo.example.com/"}]
                        },
                        "completions": {
                            "providers": [{"url": "https://fireworks.example.com/"}]
                        }
                    }
                },
                "dsv4-flash-batch": {"url": "https://batch.example.com/"}
            }
        }"#;
        let targets = Targets::from_config(serde_json::from_str(json).unwrap()).unwrap();
        let entry = targets.targets.get("dsv4-flash").unwrap();
        let batch = HashMap::from([("purpose".to_string(), "batch".to_string())]);

        assert!(matches!(
            entry
                .resolve(RequestClass::Normal)
                .evaluate_routing_rules(&batch),
            Some(RoutingAction::Redirect { target }) if target == "dsv4-flash-batch"
        ));
        let completions = entry.resolve(RequestClass::Completions);
        assert!(completions.routing_rules().is_empty());
        assert!(completions.evaluate_routing_rules(&batch).is_none());
        assert_eq!(
            completions
                .providers()
                .iter()
                .map(|p| p.target.url.to_string())
                .collect::<Vec<_>>(),
            vec!["https://fireworks.example.com/"]
        );
    }

    #[test]
    fn a_named_pool_keeps_its_own_routing_rules() {
        let json = r#"{
            "targets": {
                "dsv4-flash": {
                    "pools": {
                        "default": {
                            "routing_rules": [
                                {"match_labels": {"purpose": "batch"}, "action": {"type": "deny"}}
                            ],
                            "providers": [{"url": "https://dynamo.example.com/"}]
                        },
                        "completions": {
                            "routing_rules": [
                                {"match_labels": {"purpose": "batch"}, "action": {"type": "redirect", "target": "dsv4-flash-batch"}}
                            ],
                            "providers": [{"url": "https://fireworks.example.com/"}]
                        }
                    }
                },
                "dsv4-flash-batch": {"url": "https://batch.example.com/"}
            }
        }"#;
        let targets = Targets::from_config(serde_json::from_str(json).unwrap()).unwrap();
        let entry = targets.targets.get("dsv4-flash").unwrap();
        let batch = HashMap::from([("purpose".to_string(), "batch".to_string())]);

        let completions = entry.resolve(RequestClass::Completions);
        assert_eq!(
            completions.routing_rules().len(),
            1,
            "no inherited deny is added"
        );
        assert!(matches!(
            completions.evaluate_routing_rules(&batch),
            Some(RoutingAction::Redirect { target }) if target == "dsv4-flash-batch"
        ));
        assert!(matches!(
            entry
                .resolve(RequestClass::Normal)
                .evaluate_routing_rules(&batch),
            Some(RoutingAction::Deny)
        ));
    }

    #[test]
    fn a_named_pool_keeps_its_own_access_control() {
        let json = r#"{
            "targets": {
                "dsv4-flash": {
                    "pools": {
                        "default": {
                            "keys": ["sk-alias"],
                            "providers": [{"url": "https://dynamo.example.com/"}]
                        },
                        "completions": {
                            "keys": ["sk-continuation", "sk-other"],
                            "providers": [{"url": "https://fireworks.example.com/"}]
                        }
                    }
                }
            }
        }"#;
        let targets = Targets::from_config(serde_json::from_str(json).unwrap()).unwrap();
        let entry = targets.targets.get("dsv4-flash").unwrap();
        assert_eq!(
            entry
                .resolve(RequestClass::Completions)
                .keys()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(entry.resolve(RequestClass::Normal).keys().unwrap().len(), 1);
    }

    #[test]
    fn a_pools_target_without_a_default_is_rejected() {
        let json = r#"{
            "targets": {
                "dsv4-flash": {
                    "pools": {
                        "completions": {"providers": [{"url": "https://fireworks.example.com/"}]}
                    }
                }
            }
        }"#;
        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let error = config.targets["dsv4-flash"]
            .clone()
            .into_pool_configs()
            .unwrap_err();
        assert!(error.to_string().contains("default"), "{error}");
    }

    #[test]
    fn an_unknown_pool_name_is_rejected() {
        let json = r#"{
            "targets": {
                "dsv4-flash": {
                    "pools": {
                        "default": {"providers": [{"url": "https://dynamo.example.com/"}]},
                        "completion": {"providers": [{"url": "https://fireworks.example.com/"}]}
                    }
                }
            }
        }"#;
        let config: ConfigFile = serde_json::from_str(json).unwrap();
        let error = config.targets["dsv4-flash"]
            .clone()
            .into_pool_configs()
            .unwrap_err();
        assert!(error.to_string().contains("completion"), "{error}");
    }

    #[test]
    fn request_class_is_derived_from_the_path_alone() {
        for path in [
            "/completions",
            "/v1/completions",
            "/ai/v1/completions",
            "/v1/completions?stream=true",
            "/v1/completions/",
        ] {
            assert_eq!(
                RequestClass::from_path(path),
                RequestClass::Completions,
                "{path}"
            );
        }
        for path in [
            "/chat/completions",
            "/v1/chat/completions",
            "/ai/v1/chat/completions",
            "/v1/chat/completions?stream=true",
            "/v1/responses",
            "/v1/embeddings",
            "/v1/models",
            "/",
            "",
        ] {
            assert_eq!(
                RequestClass::from_path(path),
                RequestClass::Normal,
                "{path}"
            );
        }
        assert_eq!(RequestClass::Completions.pool_name(), COMPLETIONS_POOL);
        assert_eq!(RequestClass::Normal.pool_name(), DEFAULT_POOL);
    }
}
