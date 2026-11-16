
use crate::affinity::{self, AffinityConfig, Tracker};
use crate::aimd::{AimdConfig, Controller, Observation};
use crate::auth::KeySet;
use crate::inflight::InflightLimits;
use crate::serving::ServingOverlay;
use crate::target::{
    ConcurrencyGuard, ConcurrencyLimiter, FallbackConfig, LoadBalanceStrategy, RateLimiter,
    RoutingAction, RoutingRule, Target,
};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

fn configured_aimd(
    fallback: Option<&FallbackConfig>,
    strategy: LoadBalanceStrategy,
) -> Option<AimdConfig> {
    fallback
        .filter(|f| f.enabled)
        .map(|f| f.aimd.clone().unwrap_or_default())
        .filter(|c| c.enabled && c.validate().is_ok() && strategy == LoadBalanceStrategy::Priority)
}

fn configured_affinity(
    fallback: Option<&FallbackConfig>,
    strategy: LoadBalanceStrategy,
) -> Option<AffinityConfig> {
    fallback
        .filter(|f| f.enabled)
        .and_then(|f| f.affinity.clone())
        .filter(|c| c.enabled && c.validate().is_ok() && strategy == LoadBalanceStrategy::Priority)
}

fn provider_identity(target: &Target) -> (&url::Url, Option<&str>, Option<&str>) {
    (
        &target.url,
        target.onwards_key.as_deref(),
        target.onwards_model.as_deref(),
    )
}

#[derive(Debug, Clone)]
struct ParkedController {
    controller: Arc<Mutex<Controller>>,
    preferred: Target,
    first_token_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ProviderPool {
    providers: Vec<Provider>,
    keys: Option<Arc<KeySet>>,
    pool_limiter: Option<Arc<dyn RateLimiter>>,
    pool_concurrency_limiter: Option<ConcurrencyLimiter>,
    fallback: Option<FallbackConfig>,
    strategy: LoadBalanceStrategy,
    controller: Option<Arc<Mutex<Controller>>>,
    parked: Option<ParkedController>,
    affinity: Option<Arc<Mutex<Tracker>>>,
    trusted: bool,
    routing_rules: Vec<RoutingRule>,
    serving: AliasServing,
    inflight: Option<Arc<InflightLimits>>,
    batch_inflight: Option<u32>,
    class_route_of: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct AliasServing {
    overlays: Arc<HashMap<String, ServingOverlay>>,
}

impl AliasServing {
    pub fn new(overlays: HashMap<String, ServingOverlay>) -> Self {
        Self {
            overlays: Arc::new(overlays),
        }
    }

    pub fn overlays(&self) -> &HashMap<String, ServingOverlay> {
        &self.overlays
    }
}

#[derive(Debug, Clone)]
pub struct Provider {
    pub target: Target,
    pub weight: u32,
    limiter: ConcurrencyLimiter,
}

impl Provider {
    pub fn new(target: Target, weight: u32) -> Self {
        Self {
            target,
            weight,
            limiter: ConcurrencyLimiter::new(),
        }
    }

    pub fn with_concurrency_limit(target: Target, weight: u32, limit: usize) -> Self {
        Self {
            target,
            weight,
            limiter: ConcurrencyLimiter::with_limit(limit),
        }
    }

    pub fn active_connections(&self) -> usize {
        self.limiter.active()
    }
}

impl ProviderPool {
    pub fn new(providers: Vec<Provider>) -> Self {
        Self {
            providers,
            keys: None,
            pool_limiter: None,
            pool_concurrency_limiter: None,
            fallback: None,
            strategy: LoadBalanceStrategy::default(),
            controller: None,
            parked: None,
            affinity: None,
            trusted: false,
            routing_rules: Vec::new(),
            serving: AliasServing::default(),
            inflight: None,
            batch_inflight: None,
            class_route_of: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_config(
        providers: Vec<Provider>,
        keys: Option<KeySet>,
        pool_limiter: Option<Arc<dyn RateLimiter>>,
        pool_concurrency_limiter: Option<ConcurrencyLimiter>,
        fallback: Option<FallbackConfig>,
        strategy: LoadBalanceStrategy,
        trusted: bool,
        routing_rules: Vec<RoutingRule>,
    ) -> Self {
        let controller = configured_aimd(fallback.as_ref(), strategy)
            .filter(|_| providers.len() > 1)
            .map(|c| Arc::new(Mutex::new(Controller::new(c, Instant::now()))));
        let affinity = configured_affinity(fallback.as_ref(), strategy)
            .filter(|_| providers.len() > 1)
            .map(|c| Arc::new(Mutex::new(Tracker::new(c))));
        Self {
            controller,
            parked: None,
            affinity,
            providers,
            keys: keys.map(Arc::new),
            pool_limiter,
            pool_concurrency_limiter,
            fallback,
            strategy,
            trusted,
            routing_rules,
            serving: AliasServing::default(),
            inflight: None,
            batch_inflight: None,
            class_route_of: None,
        }
    }

    pub fn with_overlays(mut self, overlays: HashMap<String, ServingOverlay>) -> Self {
        self.serving = AliasServing::new(overlays);
        self
    }

    pub fn with_inflight_limits(mut self, limits: Option<InflightLimits>) -> Self {
        self.inflight = limits.map(Arc::new);
        self
    }

    pub fn inflight_limits(&self) -> Option<&Arc<InflightLimits>> {
        self.inflight.as_ref()
    }

    pub fn with_batch_inflight_limit(mut self, limit: Option<u32>) -> Self {
        self.batch_inflight = limit;
        self
    }

    pub fn batch_inflight_limit(&self) -> Option<u32> {
        self.batch_inflight
    }

    pub fn with_class_route_of(mut self, model: Option<String>) -> Self {
        self.class_route_of = model;
        self
    }

    pub fn class_route_of(&self) -> Option<&str> {
        self.class_route_of.as_deref()
    }

    pub fn overlays(&self) -> &HashMap<String, ServingOverlay> {
        self.serving.overlays()
    }

    pub fn alias_serving(&self) -> &AliasServing {
        &self.serving
    }

    pub fn single(target: Target, weight: u32) -> Self {
        Self::new(vec![Provider::new(target, weight)])
    }

    pub fn select(&self) -> Option<(usize, &Target, ConcurrencyGuard)> {
        self.select_excluding(&HashSet::new())
    }

    pub fn select_iter(&self) -> SelectIter<'_> {
        let with_replacement = self.fallback.as_ref().is_some_and(|f| f.with_replacement);
        let max_attempts = self
            .fallback
            .as_ref()
            .and_then(|f| f.max_attempts)
            .unwrap_or(self.providers.len());

        SelectIter {
            pool: self,
            excluded: HashSet::new(),
            ineligible: HashSet::new(),
            refused: HashSet::new(),
            max_attempts,
            attempts: 0,
            with_replacement,
            alternate_first: false,
        }
    }

    pub(crate) fn retire_aimd(&self) {
        if let Some(controller) = &self.controller {
            controller.lock().unwrap().retire();
        }
        if let Some(parked) = &self.parked {
            parked.controller.lock().unwrap().retire();
        }
    }

    pub(crate) fn aimd_enabled(&self) -> bool {
        self.controller.is_some()
    }

    pub(crate) fn affinity_enabled(&self) -> bool {
        self.affinity.is_some()
    }

    fn adopt_affinity(&mut self, old: &ProviderPool) {
        let (Some(new), Some(old)) = (&self.affinity, &old.affinity) else {
            return;
        };
        if new.lock().unwrap().config() == old.lock().unwrap().config() {
            self.affinity = Some(old.clone());
        }
    }

    fn adopt_aimd(&mut self, old: &ProviderPool) {
        let candidate = old
            .controller
            .as_ref()
            .zip(old.providers.first())
            .map(|(controller, preferred)| ParkedController {
                controller: controller.clone(),
                preferred: preferred.target.clone(),
                first_token_timeout_ms: old
                    .fallback
                    .as_ref()
                    .and_then(|f| f.first_token_timeout_ms),
            })
            .or_else(|| old.parked.clone());
        let Some(candidate) = candidate else {
            return;
        };
        let compatible = configured_aimd(self.fallback.as_ref(), self.strategy)
            .is_some_and(|config| *candidate.controller.lock().unwrap().config() == config)
            && candidate.first_token_timeout_ms
                == self
                    .fallback
                    .as_ref()
                    .and_then(|f| f.first_token_timeout_ms);
        let same_preferred = self.providers.first().is_some_and(|p| {
            provider_identity(&p.target) == provider_identity(&candidate.preferred)
        });
        match (self.controller.is_some(), compatible, same_preferred) {
            (true, true, true) => {
                candidate.controller.lock().unwrap().resume();
                self.controller = Some(candidate.controller);
            }
            (false, true, _) => {
                candidate.controller.lock().unwrap().park();
                self.parked = Some(candidate);
            }
            _ => candidate.controller.lock().unwrap().retire(),
        }
    }

    pub(crate) fn select_iter_routed(
        &self,
        aimd_eligible: bool,
        conversation: Option<u64>,
        model: &str,
        pool: &str,
    ) -> SelectIter<'_> {
        self.select_iter_routed_at(aimd_eligible, conversation, model, pool, affinity::now_ms())
    }

    pub(crate) fn select_iter_routed_at(
        &self,
        aimd_eligible: bool,
        conversation: Option<u64>,
        model: &str,
        pool: &str,
        now_ms: u64,
    ) -> SelectIter<'_> {
        let mut iter = self.select_iter();
        let aimd_share = if aimd_eligible {
            self.aimd_share(model, pool)
        } else {
            None
        };
        match (conversation, &self.affinity) {
            (Some(key), Some(tracker)) => {
                let mut tracker = tracker.lock().unwrap();
                let (_, updated) = tracker.route(key, now_ms);
                if updated {
                    let snapshot = tracker.snapshot();
                    metrics::gauge!("onwards_affinity_share", "model" => model.to_string(), "pool" => pool.to_string())
                        .set(snapshot.share);
                    metrics::gauge!("onwards_affinity_active_conversations", "model" => model.to_string(), "pool" => pool.to_string())
                        .set(snapshot.active as f64);
                    metrics::gauge!("onwards_affinity_admitted_conversations", "model" => model.to_string(), "pool" => pool.to_string())
                        .set(snapshot.admitted as f64);
                }
                let threshold = tracker.share().min(aimd_share.unwrap_or(1.0));
                let preferred = affinity::point(key) < threshold;
                iter.alternate_first = !preferred;
                metrics::counter!("onwards_affinity_routed_total", "model" => model.to_string(), "pool" => pool.to_string(), "side" => if preferred { "preferred" } else { "alternate" })
                    .increment(1);
            }
            _ => {
                if let Some(share) = aimd_share {
                    iter.alternate_first = rand::rng().random::<f64>() >= share;
                }
            }
        }
        iter
    }

    fn aimd_share(&self, model: &str, pool: &str) -> Option<f64> {
        let controller = self.controller.as_ref()?;
        let mut controller = controller.lock().unwrap();
        if !controller.active() {
            return None;
        }
        controller.remember_labels(model, pool);
        if let Some(direction) = controller.tick(Instant::now()) {
            metrics::counter!("onwards_share_adjustments_total", "model" => model.to_string(), "pool" => pool.to_string(), "direction" => direction).increment(1);
        }
        controller.publish();
        Some(controller.share())
    }

    pub(crate) fn observe(
        &self,
        eligible: bool,
        member: usize,
        model: &str,
        pool: &str,
        start: Instant,
    ) -> Option<Observation> {
        if !eligible || member != 0 {
            return None;
        }
        self.controller
            .as_ref()
            .and_then(|c| Observation::new(c.clone(), model, pool, start))
    }

    fn select_excluding(
        &self,
        exclude: &HashSet<usize>,
    ) -> Option<(usize, &Target, ConcurrencyGuard)> {
        if self.providers.is_empty() {
            return None;
        }

        match self.strategy {
            LoadBalanceStrategy::Priority => self.select_priority(exclude),
            LoadBalanceStrategy::WeightedRandom => self.select_least_connections(exclude),
        }
    }

    fn select_priority(
        &self,
        exclude: &HashSet<usize>,
    ) -> Option<(usize, &Target, ConcurrencyGuard)> {
        for (idx, provider) in self.providers.iter().enumerate() {
            if exclude.contains(&idx) {
                continue;
            }
            if let Some(guard) = provider.limiter.try_acquire() {
                return Some((idx, &provider.target, guard));
            }
        }
        None
    }

    fn select_least_connections(
        &self,
        exclude: &HashSet<usize>,
    ) -> Option<(usize, &Target, ConcurrencyGuard)> {
        let mut best_score = f64::INFINITY;
        let mut candidates: Vec<usize> = Vec::new();

        for (idx, provider) in self.providers.iter().enumerate() {
            if exclude.contains(&idx) {
                continue;
            }
            if provider.limiter.at_capacity() {
                continue;
            }

            let score = provider.limiter.active() as f64 / provider.weight as f64;

            if score < best_score - f64::EPSILON {
                best_score = score;
                candidates.clear();
                candidates.push(idx);
            } else if (score - best_score).abs() < f64::EPSILON {
                candidates.push(idx);
            }
        }

        if candidates.is_empty() {
            return None;
        }

        let selected = if candidates.len() == 1 {
            candidates[0]
        } else {
            let mut rng = rand::rng();
            let total_weight: u32 = candidates
                .iter()
                .map(|&idx| self.providers[idx].weight)
                .sum();
            let r: u32 = rng.random_range(0..total_weight);
            let mut cumulative = 0;
            let mut picked = candidates[0];
            for &idx in &candidates {
                cumulative += self.providers[idx].weight;
                if r < cumulative {
                    picked = idx;
                    break;
                }
            }
            picked
        };

        let provider = &self.providers[selected];
        match provider.limiter.try_acquire() {
            Some(guard) => Some((selected, &provider.target, guard)),
            None => {
                let mut new_exclude = exclude.clone();
                new_exclude.insert(selected);
                self.select_least_connections(&new_exclude)
            }
        }
    }

    pub fn providers(&self) -> &[Provider] {
        &self.providers
    }

    pub fn len(&self) -> usize {
        self.providers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    pub fn first_target(&self) -> Option<&Target> {
        self.providers.first().map(|p| &p.target)
    }

    pub fn keys(&self) -> Option<&KeySet> {
        self.keys.as_deref()
    }

    pub fn pool_limiter(&self) -> Option<&Arc<dyn RateLimiter>> {
        self.pool_limiter.as_ref()
    }

    pub fn pool_concurrency_limiter(&self) -> Option<&ConcurrencyLimiter> {
        self.pool_concurrency_limiter.as_ref()
    }

    pub fn fallback(&self) -> Option<&FallbackConfig> {
        self.fallback.as_ref()
    }

    pub fn fallback_enabled(&self) -> bool {
        self.fallback.as_ref().is_some_and(|f| f.enabled)
    }

    pub fn should_fallback_on_status(&self, status_code: u16) -> bool {
        self.fallback
            .as_ref()
            .is_some_and(|f| f.should_fallback_on_status(status_code))
    }

    pub fn should_fallback_on_realtime_status(&self, status_code: u16) -> bool {
        self.fallback
            .as_ref()
            .is_some_and(|f| f.should_fallback_on_realtime_status(status_code))
    }

    pub fn should_fallback_on_rate_limit(&self) -> bool {
        self.fallback
            .as_ref()
            .is_some_and(|f| f.enabled && f.on_rate_limit)
    }

    pub fn fallback_max_attempts(&self) -> usize {
        self.fallback
            .as_ref()
            .and_then(|f| f.max_attempts)
            .unwrap_or(self.providers.len())
    }

    pub fn strategy(&self) -> LoadBalanceStrategy {
        self.strategy
    }

    pub fn is_trusted(&self) -> bool {
        self.trusted
    }

    pub fn routing_rules(&self) -> &[RoutingRule] {
        &self.routing_rules
    }

    pub fn evaluate_routing_rules(
        &self,
        key_labels: &HashMap<String, String>,
    ) -> Option<&RoutingAction> {
        self.routing_rules.iter().find_map(|rule| {
            let matches = rule
                .match_labels
                .iter()
                .all(|(k, v)| key_labels.get(k).is_some_and(|kv| kv == v));
            matches.then_some(&rule.action)
        })
    }

    pub fn adopt_provider_state(&mut self, old: &ProviderPool) {
        self.adopt_aimd(old);
        self.adopt_affinity(old);
        for new_provider in &mut self.providers {
            if let Some(old_provider) = old.providers.iter().find(|old_p| {
                old_p.target.url == new_provider.target.url
                    && old_p.target.onwards_key == new_provider.target.onwards_key
                    && old_p.target.onwards_model == new_provider.target.onwards_model
            }) {
                new_provider
                    .limiter
                    .adopt_active_counter(&old_provider.limiter);
            }
        }

        if let (Some(new_limiter), Some(old_limiter)) = (
            &mut self.pool_concurrency_limiter,
            &old.pool_concurrency_limiter,
        ) {
            new_limiter.adopt_active_counter(old_limiter);
        }
    }
}

pub struct SelectIter<'a> {
    pool: &'a ProviderPool,
    excluded: HashSet<usize>,
    ineligible: HashSet<usize>,
    refused: HashSet<usize>,
    max_attempts: usize,
    attempts: usize,
    with_replacement: bool,
    alternate_first: bool,
}

impl SelectIter<'_> {
    pub fn excluding_members(mut self, members: impl IntoIterator<Item = usize>) -> Self {
        self.ineligible.extend(members);
        self
    }

    pub(crate) fn mark_refused(&mut self, member: usize) {
        self.refused.insert(member);
    }

    fn exclusions(&self) -> HashSet<usize> {
        self.excluded
            .iter()
            .chain(&self.ineligible)
            .chain(&self.refused)
            .copied()
            .collect()
    }

    pub(crate) fn has_available_alternative(&self, current_member: usize) -> bool {
        self.attempts < self.max_attempts
            && self
                .pool
                .providers
                .iter()
                .enumerate()
                .any(|(index, provider)| {
                    index != current_member
                        && !self.ineligible.contains(&index)
                        && !self.refused.contains(&index)
                        && !self.excluded.contains(&index)
                        && !provider.limiter.at_capacity()
                })
    }
}

impl<'a> Iterator for SelectIter<'a> {
    type Item = (usize, &'a Target, ConcurrencyGuard);

    fn next(&mut self) -> Option<Self::Item> {
        if self.attempts >= self.max_attempts {
            return None;
        }
        self.attempts += 1;

        let first_override = if self.attempts == 1 && self.alternate_first {
            let mut exclude = self.ineligible.clone();
            exclude.insert(0);
            self.pool.select_priority(&exclude)
        } else {
            None
        };
        let result = match first_override.or_else(|| self.pool.select_excluding(&self.exclusions()))
        {
            Some(result) => result,
            None if self.excluded.is_empty() => return None,
            None => {
                self.excluded.clear();
                self.pool.select_excluding(&self.exclusions())?
            }
        };

        let should_exclude = match self.pool.strategy {
            LoadBalanceStrategy::Priority => true,
            LoadBalanceStrategy::WeightedRandom => !self.with_replacement,
        };
        if should_exclude {
            self.excluded.insert(result.0);
        }

        Some(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::target::Target;
    use std::collections::HashMap;

    fn create_test_target(url: &str) -> Target {
        Target::builder().url(url.parse().unwrap()).build()
    }

    fn aimd_test_config() -> crate::aimd::AimdConfig {
        crate::aimd::AimdConfig {
            latency_budget_ms: 100,
            breach_rate_target: 0.2,
            recovery_breach_rate: 0.1,
            window_samples: 10,
            min_samples: 2,
            share_step: 0.1,
            share_decay: 0.5,
            share_floor: 0.1,
            dwell_ms: 1,
            ..crate::aimd::AimdConfig::default()
        }
    }

    fn aimd_pool_from(urls: &[&str], aimd: Option<crate::aimd::AimdConfig>) -> ProviderPool {
        let providers = urls
            .iter()
            .enumerate()
            .map(|(index, url)| {
                if index == 1 {
                    Provider::with_concurrency_limit(create_test_target(url), 1, 1)
                } else {
                    Provider::new(create_test_target(url), 1)
                }
            })
            .collect();
        ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            Some(FallbackConfig {
                enabled: true,
                first_token_timeout_ms: Some(0),
                aimd,
                max_attempts: Some(4),
                ..Default::default()
            }),
            LoadBalanceStrategy::Priority,
            false,
            vec![],
        )
    }

    fn aimd_pool() -> ProviderPool {
        aimd_pool_from(
            &["https://preferred.example", "https://alternate.example"],
            Some(aimd_test_config()),
        )
    }

    fn demote(pool: &ProviderPool) {
        let controller = pool.controller.as_ref().unwrap();
        for _ in 0..2 {
            Observation::new(controller.clone(), "model", "default", Instant::now())
                .unwrap()
                .status(529);
        }
    }

    fn affinity_pool(
        target: usize,
        aimd: Option<crate::aimd::AimdConfig>,
        strategy: LoadBalanceStrategy,
    ) -> ProviderPool {
        ProviderPool::with_config(
            vec![
                Provider::new(create_test_target("https://preferred.example"), 1),
                Provider::new(create_test_target("https://alternate.example"), 1),
            ],
            None,
            None,
            None,
            Some(FallbackConfig {
                enabled: true,
                first_token_timeout_ms: Some(0),
                aimd,
                affinity: Some(AffinityConfig {
                    target_conversations: target,
                    margin: Some(1),
                    ..AffinityConfig::default()
                }),
                ..Default::default()
            }),
            strategy,
            false,
            vec![],
        )
    }

    fn conversation_keys(n: usize) -> Vec<u64> {
        (0..n)
            .map(|i| {
                affinity::conversation_key(
                    Some(&format!("conversation-{i}")),
                    &serde_json::json!({}),
                )
                .unwrap()
            })
            .collect()
    }

    fn first_member(pool: &ProviderPool, aimd: bool, key: Option<u64>, now_ms: u64) -> usize {
        pool.select_iter_routed_at(aimd, key, "model", "default", now_ms)
            .next()
            .unwrap()
            .0
    }

    #[test]
    fn affinity_sends_each_conversation_to_one_side() {
        let disabled = crate::aimd::AimdConfig {
            enabled: false,
            ..crate::aimd::AimdConfig::default()
        };
        let pool = affinity_pool(10, Some(disabled), LoadBalanceStrategy::Priority);
        let keys = conversation_keys(100);
        for key in &keys {
            first_member(&pool, false, Some(*key), 1_000);
        }
        let first: Vec<usize> = keys
            .iter()
            .map(|k| first_member(&pool, false, Some(*k), 61_000))
            .collect();
        assert_eq!(first.iter().filter(|m| **m == 0).count(), 10);
        let later: Vec<usize> = keys
            .iter()
            .map(|k| first_member(&pool, false, Some(*k), 121_000))
            .collect();
        assert_eq!(first, later);
        assert_eq!(first_member(&pool, false, None, 121_000), 0);
    }

    #[tokio::test]
    async fn aimd_overload_caps_the_affinity_share() {
        let pool = affinity_pool(
            1_000,
            Some(aimd_test_config()),
            LoadBalanceStrategy::Priority,
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        demote(&pool);
        let share = pool.controller.as_ref().unwrap().lock().unwrap().share();
        assert!((share - 0.5).abs() < 1e-9, "share after demotion: {share}");
        for key in conversation_keys(200) {
            let expected = if affinity::point(key) < share { 0 } else { 1 };
            assert_eq!(first_member(&pool, true, Some(key), 1_000), expected);
            assert_eq!(first_member(&pool, false, Some(key), 1_000), 0);
        }
    }

    #[test]
    fn affinity_tracker_survives_a_reload_with_the_same_configuration() {
        let old = affinity_pool(10, None, LoadBalanceStrategy::Priority);
        let tracker = old.affinity.clone().unwrap();
        let mut same = affinity_pool(10, None, LoadBalanceStrategy::Priority);
        same.adopt_provider_state(&old);
        assert!(Arc::ptr_eq(same.affinity.as_ref().unwrap(), &tracker));
        let mut changed = affinity_pool(20, None, LoadBalanceStrategy::Priority);
        changed.adopt_provider_state(&old);
        assert!(!Arc::ptr_eq(changed.affinity.as_ref().unwrap(), &tracker));
    }

    #[test]
    fn affinity_needs_a_priority_pool_with_an_alternate() {
        assert!(affinity_pool(10, None, LoadBalanceStrategy::Priority).affinity_enabled());
        assert!(!affinity_pool(10, None, LoadBalanceStrategy::WeightedRandom).affinity_enabled());
        let single = ProviderPool::with_config(
            vec![Provider::new(
                create_test_target("https://preferred.example"),
                1,
            )],
            None,
            None,
            None,
            Some(FallbackConfig {
                enabled: true,
                affinity: Some(AffinityConfig {
                    target_conversations: 10,
                    ..AffinityConfig::default()
                }),
                ..Default::default()
            }),
            LoadBalanceStrategy::Priority,
            false,
            vec![],
        );
        assert!(!single.affinity_enabled());
    }

    #[tokio::test(start_paused = true)]
    async fn aimd_parks_while_the_preferred_provider_is_disabled_and_resumes_on_return() {
        let active = aimd_pool();
        tokio::time::advance(std::time::Duration::from_millis(5)).await;
        demote(&active);
        let share = active.controller.as_ref().unwrap().lock().unwrap().share();
        assert!((share - 0.5).abs() < 1e-9);

        let mut single = aimd_pool_from(&["https://alternate.example"], Some(aimd_test_config()));
        single.adopt_provider_state(&active);
        assert!(!single.aimd_enabled());
        let parked = single.parked.as_ref().unwrap().controller.clone();
        assert!(!parked.lock().unwrap().active());

        let mut still_single =
            aimd_pool_from(&["https://alternate.example"], Some(aimd_test_config()));
        still_single.adopt_provider_state(&single);
        assert!(Arc::ptr_eq(
            &still_single.parked.as_ref().unwrap().controller,
            &parked
        ));

        let mut restored = aimd_pool();
        restored.adopt_provider_state(&still_single);
        let resumed = restored.controller.as_ref().unwrap();
        assert!(Arc::ptr_eq(resumed, &parked));
        assert!(resumed.lock().unwrap().active());
        assert!((resumed.lock().unwrap().share() - 0.5).abs() < 1e-9);
    }

    #[tokio::test(start_paused = true)]
    async fn aimd_parked_controller_is_retired_when_a_different_preferred_returns() {
        let active = aimd_pool();
        let mut single = aimd_pool_from(&["https://alternate.example"], Some(aimd_test_config()));
        single.adopt_provider_state(&active);
        let parked = single.parked.as_ref().unwrap().controller.clone();

        let mut different = aimd_pool_from(
            &["https://other.example", "https://alternate.example"],
            Some(aimd_test_config()),
        );
        different.adopt_provider_state(&single);
        assert!(!Arc::ptr_eq(
            different.controller.as_ref().unwrap(),
            &parked
        ));
        assert!(!parked.lock().unwrap().active());
        assert!(!parked.lock().unwrap().resume(), "retired, not parked");
    }

    #[tokio::test(start_paused = true)]
    async fn aimd_opt_out_retires_rather_than_parks() {
        let active = aimd_pool();
        let controller = active.controller.clone().unwrap();
        let mut disabled = aimd_pool_from(
            &["https://alternate.example"],
            Some(crate::aimd::AimdConfig {
                enabled: false,
                ..aimd_test_config()
            }),
        );
        disabled.adopt_provider_state(&active);
        assert!(disabled.parked.is_none());
        assert!(!controller.lock().unwrap().resume(), "retired, not parked");
    }

    #[test]
    fn aimd_is_default_on_only_for_eligible_pools_and_has_explicit_opt_out() {
        let make = |strategy, providers, fallback| {
            ProviderPool::with_config(
                providers,
                None,
                None,
                None,
                Some(fallback),
                strategy,
                false,
                vec![],
            )
        };
        let providers = aimd_pool().providers;
        let default_fallback = FallbackConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(
            make(
                LoadBalanceStrategy::Priority,
                providers.clone(),
                default_fallback.clone()
            )
            .aimd_enabled()
        );
        assert!(
            !make(
                LoadBalanceStrategy::WeightedRandom,
                providers.clone(),
                default_fallback.clone()
            )
            .aimd_enabled()
        );
        assert!(
            !make(
                LoadBalanceStrategy::Priority,
                vec![providers[0].clone()],
                default_fallback
            )
            .aimd_enabled()
        );
        assert!(
            !make(
                LoadBalanceStrategy::Priority,
                providers.clone(),
                FallbackConfig::default()
            )
            .aimd_enabled()
        );
        let disabled =
            serde_json::from_value(serde_json::json!({"enabled":true,"aimd":{"enabled":false}}))
                .unwrap();
        assert!(!make(LoadBalanceStrategy::Priority, providers, disabled).aimd_enabled());
    }

    #[test]
    fn aimd_first_override_preserves_cascade_and_guards() {
        let pool = aimd_pool();
        let mut iter = pool.select_iter();
        iter.alternate_first = true;
        let indices: Vec<_> = iter.map(|(index, _, _guard)| index).collect();
        assert_eq!(indices, [1, 0, 0, 1]);
        assert!(pool.providers.iter().all(|p| p.active_connections() == 0));
        let held = pool.providers[1].limiter.try_acquire().unwrap();
        let mut iter = pool.select_iter();
        iter.alternate_first = true;
        assert_eq!(iter.next().unwrap().0, 0);
        drop(held);
    }

    #[test]
    fn aimd_reload_preserves_inherited_and_explicit_default_equivalence() {
        let urls = ["https://preferred.example", "https://alternate.example"];
        let old = aimd_pool_from(&urls, None);
        let mut new = aimd_pool_from(&urls, Some(crate::aimd::AimdConfig::default()));
        new.adopt_provider_state(&old);
        assert!(Arc::ptr_eq(
            new.controller.as_ref().unwrap(),
            old.controller.as_ref().unwrap()
        ));
        assert!(old.controller.as_ref().unwrap().lock().unwrap().active());
    }

    #[test]
    fn aimd_reload_adopts_only_same_preferred_and_configuration() {
        let old = aimd_pool();
        let mut new = aimd_pool();
        new.adopt_provider_state(&old);
        assert!(Arc::ptr_eq(
            new.controller.as_ref().unwrap(),
            old.controller.as_ref().unwrap()
        ));
        let mut reordered = aimd_pool();
        reordered.providers.swap(0, 1);
        reordered.adopt_provider_state(&old);
        assert!(!Arc::ptr_eq(
            reordered.controller.as_ref().unwrap(),
            old.controller.as_ref().unwrap()
        ));
        assert!(!old.controller.as_ref().unwrap().lock().unwrap().active());
        assert!(
            reordered
                .controller
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .active()
        );
        assert!(
            old.observe(true, 0, "model", "default", Instant::now())
                .is_none()
        );
        let mut changed = aimd_pool();
        changed
            .fallback
            .as_mut()
            .unwrap()
            .aimd
            .as_mut()
            .unwrap()
            .dwell_ms = 2;
        changed.adopt_provider_state(&old);
        assert!(!Arc::ptr_eq(
            changed.controller.as_ref().unwrap(),
            old.controller.as_ref().unwrap()
        ));
        assert!(Arc::ptr_eq(
            old.clone().controller.as_ref().unwrap(),
            old.controller.as_ref().unwrap()
        ));
        assert!(
            old.observe(true, 1, "model", "default", Instant::now())
                .is_none()
        );
        assert!(
            old.observe(false, 0, "model", "default", Instant::now())
                .is_none()
        );
        assert_eq!(
            old.select_iter_routed(false, None, "model", "default")
                .next()
                .unwrap()
                .0,
            0
        );
    }

    #[test]
    fn test_single_provider_pool() {
        let target = create_test_target("https://api.example.com");
        let pool = ProviderPool::single(target.clone(), 1);

        assert_eq!(pool.len(), 1);
        assert!(!pool.is_empty());

        let selected = pool.select();
        assert!(selected.is_some());
        let (_, target, _guard) = selected.unwrap();
        assert_eq!(target.url.as_str(), "https://api.example.com/");
    }

    #[test]
    fn test_empty_pool_returns_none() {
        let pool = ProviderPool::new(vec![]);

        assert!(pool.is_empty());
        assert!(pool.select().is_none());
    }

    #[test]
    fn test_weighted_selection_distribution() {
        let providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 3),
            Provider::new(create_test_target("https://api2.example.com"), 1),
        ];
        let pool = ProviderPool::new(providers);

        let mut counts: HashMap<String, usize> = HashMap::new();
        for _ in 0..1000 {
            if let Some((_, target, _guard)) = pool.select() {
                *counts.entry(target.url.to_string()).or_insert(0) += 1;
            }
        }

        let count1 = *counts.get("https://api1.example.com/").unwrap_or(&0);
        let count2 = *counts.get("https://api2.example.com/").unwrap_or(&0);

        let ratio = count1 as f64 / count2 as f64;
        assert!(
            ratio > 1.5 && ratio < 6.0,
            "Expected ratio around 3.0, got {}",
            ratio
        );
    }

    #[test]
    fn test_least_connections_prefers_less_loaded() {
        let providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 1),
            Provider::new(create_test_target("https://api2.example.com"), 1),
        ];
        let pool = ProviderPool::new(providers);

        let (idx1, _, guard1) = pool.select().unwrap();

        let (idx2, _, _guard2) = pool.select().unwrap();
        assert_ne!(idx1, idx2, "Should pick the less loaded provider");

        drop(guard1);

        let (idx3, _, _guard3) = pool.select().unwrap();
        assert_eq!(
            idx3, idx1,
            "Should pick the provider whose guard was dropped"
        );
    }

    #[test]
    fn test_weighted_least_connections_respects_weights() {
        let providers = vec![
            Provider::new(create_test_target("https://heavy.example.com"), 3),
            Provider::new(create_test_target("https://light.example.com"), 1),
        ];
        let pool = ProviderPool::new(providers);

        let mut guards = Vec::new();
        for _ in 0..40 {
            if let Some((_, _, guard)) = pool.select() {
                guards.push(guard);
            }
        }

        let heavy_active = pool.providers()[0].active_connections();
        let light_active = pool.providers()[1].active_connections();

        let ratio = heavy_active as f64 / light_active as f64;
        assert!(
            ratio > 2.0 && ratio < 5.0,
            "Expected ratio around 3.0, got {} (heavy={}, light={})",
            ratio,
            heavy_active,
            light_active
        );
    }

    #[test]
    fn test_concurrency_limit_skips_full_provider() {
        let providers = vec![
            Provider::with_concurrency_limit(
                create_test_target("https://limited.example.com"),
                1,
                1,
            ),
            Provider::new(create_test_target("https://unlimited.example.com"), 1),
        ];
        let pool = ProviderPool::new(providers);

        let mut guard_on_limited = None;
        for _ in 0..100 {
            let (idx, _, guard) = pool.select().unwrap();
            if idx == 0 {
                guard_on_limited = Some(guard);
                break;
            }
        }
        assert!(
            guard_on_limited.is_some(),
            "Should eventually select the limited provider"
        );

        let (idx, _, _guard) = pool.select().unwrap();
        assert_eq!(idx, 1, "Should skip the full provider");
    }

    #[test]
    fn test_all_at_capacity_returns_none() {
        let providers = vec![
            Provider::with_concurrency_limit(create_test_target("https://a.example.com"), 1, 1),
            Provider::with_concurrency_limit(create_test_target("https://b.example.com"), 1, 1),
        ];
        let pool = ProviderPool::new(providers);

        let (_, _, _g1) = pool.select().unwrap();
        let (_, _, _g2) = pool.select().unwrap();

        assert!(pool.select().is_none());
    }

    #[test]
    fn test_first_target() {
        let providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 1),
            Provider::new(create_test_target("https://api2.example.com"), 2),
        ];
        let pool = ProviderPool::new(providers);

        let first = pool.first_target();
        assert!(first.is_some());
        assert_eq!(first.unwrap().url.as_str(), "https://api1.example.com/");
    }

    #[test]
    fn test_fallback_max_attempts() {
        use crate::target::FallbackConfig;

        let two_providers = || {
            vec![
                Provider::new(create_test_target("https://a.example.com"), 1),
                Provider::new(create_test_target("https://b.example.com"), 1),
            ]
        };

        let pool = ProviderPool::new(two_providers());
        assert_eq!(pool.fallback_max_attempts(), 2);

        let pool = ProviderPool::with_config(
            two_providers(),
            None,
            None,
            None,
            Some(FallbackConfig {
                enabled: true,
                ..Default::default()
            }),
            LoadBalanceStrategy::default(),
            false,
            Vec::new(),
        );
        assert_eq!(pool.fallback_max_attempts(), 2);

        for strategy in [
            LoadBalanceStrategy::WeightedRandom,
            LoadBalanceStrategy::Priority,
        ] {
            let pool = ProviderPool::with_config(
                two_providers(),
                None,
                None,
                None,
                Some(FallbackConfig {
                    enabled: true,
                    max_attempts: Some(10),
                    ..Default::default()
                }),
                strategy,
                false,
                Vec::new(),
            );
            assert_eq!(pool.fallback_max_attempts(), 10);
            assert_eq!(pool.select_iter().count(), 10);
        }
    }

    #[test]
    fn test_providers_accessor() {
        let providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 1),
            Provider::new(create_test_target("https://api2.example.com"), 2),
        ];
        let pool = ProviderPool::new(providers);

        assert_eq!(pool.providers().len(), 2);
        assert_eq!(pool.providers()[0].weight, 1);
        assert_eq!(pool.providers()[1].weight, 2);
    }

    #[test]
    fn test_select_iter_priority_strategy() {
        use crate::target::LoadBalanceStrategy;

        let providers = vec![
            Provider::new(create_test_target("https://primary.example.com"), 1),
            Provider::new(create_test_target("https://secondary.example.com"), 10),
            Provider::new(create_test_target("https://tertiary.example.com"), 5),
        ];

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::Priority,
            false,
            Vec::new(),
        );

        let order: Vec<_> = pool.select_iter().collect();
        assert_eq!(order.len(), 3);
        assert_eq!(order[0].0, 0);
        assert_eq!(order[1].0, 1);
        assert_eq!(order[2].0, 2);
        assert_eq!(order[0].1.url.as_str(), "https://primary.example.com/");
        assert_eq!(order[1].1.url.as_str(), "https://secondary.example.com/");
        assert_eq!(order[2].1.url.as_str(), "https://tertiary.example.com/");
    }

    #[test]
    fn test_select_iter_priority_with_replacement_still_advances() {
        use crate::target::{FallbackConfig, LoadBalanceStrategy};

        let providers = vec![
            Provider::new(create_test_target("https://primary.example.com"), 1),
            Provider::new(create_test_target("https://secondary.example.com"), 1),
            Provider::new(create_test_target("https://tertiary.example.com"), 1),
        ];

        let fallback = Some(FallbackConfig {
            enabled: true,
            with_replacement: true,
            max_attempts: Some(3),
            ..Default::default()
        });

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            fallback,
            LoadBalanceStrategy::Priority,
            false,
            Vec::new(),
        );

        let order: Vec<_> = pool.select_iter().collect();
        assert_eq!(order.len(), 3);
        assert_eq!(order[0].1.url.as_str(), "https://primary.example.com/");
        assert_eq!(order[1].1.url.as_str(), "https://secondary.example.com/");
        assert_eq!(order[2].1.url.as_str(), "https://tertiary.example.com/");
    }

    #[test]
    fn test_select_iter_weighted_random_includes_all() {
        use crate::target::LoadBalanceStrategy;

        let providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 3),
            Provider::new(create_test_target("https://api2.example.com"), 1),
        ];

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::WeightedRandom,
            false,
            Vec::new(),
        );

        let order: Vec<_> = pool.select_iter().collect();
        assert_eq!(order.len(), 2);

        let urls: std::collections::HashSet<_> =
            order.iter().map(|(_, t, _)| t.url.as_str()).collect();
        assert!(urls.contains("https://api1.example.com/"));
        assert!(urls.contains("https://api2.example.com/"));
    }

    #[test]
    fn test_select_iter_weighted_random_distribution() {
        use crate::target::LoadBalanceStrategy;

        let providers = vec![
            Provider::new(create_test_target("https://heavy.example.com"), 9),
            Provider::new(create_test_target("https://light.example.com"), 1),
        ];

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::WeightedRandom,
            false,
            Vec::new(),
        );

        let mut heavy_first = 0;
        let iterations = 1000;
        for _ in 0..iterations {
            let order: Vec<_> = pool.select_iter().collect();
            if order[0].1.url.as_str() == "https://heavy.example.com/" {
                heavy_first += 1;
            }
        }

        let percentage = (heavy_first * 100) / iterations;
        assert!(
            (80..=98).contains(&percentage),
            "Expected heavy to be first ~90% of the time, got {}% ({}/{})",
            percentage,
            heavy_first,
            iterations
        );
    }

    #[test]
    fn test_select_iter_empty_pool() {
        let pool = ProviderPool::new(vec![]);
        let order: Vec<_> = pool.select_iter().collect();
        assert!(order.is_empty());
    }

    #[test]
    fn test_select_iter_single_provider() {
        use crate::target::LoadBalanceStrategy;

        let providers = vec![Provider::new(
            create_test_target("https://only.example.com"),
            1,
        )];

        for strategy in [
            LoadBalanceStrategy::Priority,
            LoadBalanceStrategy::WeightedRandom,
        ] {
            let pool = ProviderPool::with_config(
                providers.clone(),
                None,
                None,
                None,
                None,
                strategy,
                false,
                Vec::new(),
            );

            let order: Vec<_> = pool.select_iter().collect();
            assert_eq!(order.len(), 1);
            assert_eq!(order[0].1.url.as_str(), "https://only.example.com/");
        }
    }

    #[test]
    fn test_select_iter_with_replacement_allows_duplicates() {
        use crate::target::{FallbackConfig, LoadBalanceStrategy};

        let providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 9),
            Provider::new(create_test_target("https://api2.example.com"), 1),
        ];

        let fallback = Some(FallbackConfig {
            enabled: true,
            with_replacement: true,
            max_attempts: Some(5),
            ..Default::default()
        });

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            fallback,
            LoadBalanceStrategy::WeightedRandom,
            false,
            Vec::new(),
        );

        let order: Vec<_> = pool.select_iter().collect();
        assert_eq!(order.len(), 5);

        let mut found_duplicate = false;
        for _ in 0..100 {
            let order: Vec<_> = pool.select_iter().collect();
            let indices: Vec<usize> = order.iter().map(|(idx, _, _)| *idx).collect();
            let unique: std::collections::HashSet<_> = indices.iter().collect();
            if unique.len() < indices.len() {
                found_duplicate = true;
                break;
            }
        }
        assert!(
            found_duplicate,
            "With replacement should allow the same provider to appear multiple times"
        );
    }

    #[test]
    fn test_select_iter_max_attempts_controls_length() {
        use crate::target::{FallbackConfig, LoadBalanceStrategy};

        let providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 1),
            Provider::new(create_test_target("https://api2.example.com"), 1),
            Provider::new(create_test_target("https://api3.example.com"), 1),
        ];

        let fallback = Some(FallbackConfig {
            enabled: true,
            max_attempts: Some(2),
            ..Default::default()
        });

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            fallback,
            LoadBalanceStrategy::WeightedRandom,
            false,
            Vec::new(),
        );

        let order: Vec<_> = pool.select_iter().collect();
        assert_eq!(
            order.len(),
            2,
            "max_attempts should cap the ordering length"
        );

        let indices: std::collections::HashSet<_> = order.iter().map(|(idx, _, _)| *idx).collect();
        assert_eq!(
            indices.len(),
            2,
            "Without replacement, all entries should be unique"
        );
    }

    #[test]
    fn test_select_iter_max_attempts_with_priority() {
        use crate::target::{FallbackConfig, LoadBalanceStrategy};

        let providers = vec![
            Provider::new(create_test_target("https://primary.example.com"), 1),
            Provider::new(create_test_target("https://secondary.example.com"), 1),
            Provider::new(create_test_target("https://tertiary.example.com"), 1),
        ];

        let fallback = Some(FallbackConfig {
            enabled: true,
            max_attempts: Some(2),
            ..Default::default()
        });

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            fallback,
            LoadBalanceStrategy::Priority,
            false,
            Vec::new(),
        );

        let order: Vec<_> = pool.select_iter().collect();
        assert_eq!(order.len(), 2);
        assert_eq!(order[0].1.url.as_str(), "https://primary.example.com/");
        assert_eq!(order[1].1.url.as_str(), "https://secondary.example.com/");
    }

    #[test]
    fn test_select_iter_refused_member_sits_out_later_passes() {
        use crate::target::{FallbackConfig, LoadBalanceStrategy};

        let providers = vec![
            Provider::new(create_test_target("https://primary.example.com"), 1),
            Provider::new(create_test_target("https://secondary.example.com"), 1),
        ];
        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            Some(FallbackConfig {
                enabled: true,
                max_attempts: Some(6),
                ..Default::default()
            }),
            LoadBalanceStrategy::Priority,
            false,
            Vec::new(),
        );

        let mut iter = pool.select_iter();
        let (first, _, guard) = iter.next().unwrap();
        drop(guard);
        assert_eq!(first, 0);
        iter.mark_refused(first);

        let rest: Vec<usize> = std::iter::from_fn(|| iter.next().map(|(idx, _, _)| idx)).collect();
        assert_eq!(rest, vec![1, 1, 1, 1, 1]);

        let mut iter = pool.select_iter();
        iter.mark_refused(0);
        iter.mark_refused(1);
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_select_iter_retries_above_strategy() {
        use crate::target::{FallbackConfig, LoadBalanceStrategy};

        let pool = ProviderPool::with_config(
            vec![Provider::new(
                create_test_target("https://only.example.com"),
                1,
            )],
            None,
            None,
            None,
            Some(FallbackConfig {
                enabled: true,
                max_attempts: Some(3),
                ..Default::default()
            }),
            LoadBalanceStrategy::Priority,
            false,
            Vec::new(),
        );
        let indices: Vec<_> = pool.select_iter().map(|(idx, _, _)| idx).collect();
        assert_eq!(
            indices,
            vec![0, 0, 0],
            "single provider retried up to the budget"
        );

        let pool = ProviderPool::with_config(
            vec![
                Provider::new(create_test_target("https://p0.example.com"), 1),
                Provider::new(create_test_target("https://p1.example.com"), 1),
            ],
            None,
            None,
            None,
            Some(FallbackConfig {
                enabled: true,
                max_attempts: Some(5),
                ..Default::default()
            }),
            LoadBalanceStrategy::Priority,
            false,
            Vec::new(),
        );
        let indices: Vec<_> = pool.select_iter().map(|(idx, _, _)| idx).collect();
        assert_eq!(indices, vec![0, 1, 0, 1, 0], "cascade, restart, cascade...");
    }

    #[test]
    fn test_select_iter_defaults_preserve_behavior() {
        use crate::target::LoadBalanceStrategy;

        let providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 3),
            Provider::new(create_test_target("https://api2.example.com"), 1),
        ];

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::WeightedRandom,
            false,
            Vec::new(),
        );

        let order: Vec<_> = pool.select_iter().collect();
        assert_eq!(order.len(), 2);

        let urls: std::collections::HashSet<_> =
            order.iter().map(|(_, t, _)| t.url.as_str()).collect();
        assert!(urls.contains("https://api1.example.com/"));
        assert!(urls.contains("https://api2.example.com/"));
    }

    #[test]
    fn test_select_iter_with_replacement_single_provider() {
        use crate::target::{FallbackConfig, LoadBalanceStrategy};

        let providers = vec![Provider::new(
            create_test_target("https://only.example.com"),
            1,
        )];

        let fallback = Some(FallbackConfig {
            enabled: true,
            with_replacement: true,
            max_attempts: Some(3),
            ..Default::default()
        });

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            fallback,
            LoadBalanceStrategy::WeightedRandom,
            false,
            Vec::new(),
        );

        let order: Vec<_> = pool.select_iter().collect();
        assert_eq!(
            order.len(),
            3,
            "Single provider with replacement should repeat"
        );
        for (idx, target, _) in &order {
            assert_eq!(*idx, 0);
            assert_eq!(target.url.as_str(), "https://only.example.com/");
        }
    }

    #[test]
    fn test_select_iter_with_replacement_respects_weights() {
        use crate::target::{FallbackConfig, LoadBalanceStrategy};

        let providers = vec![
            Provider::new(create_test_target("https://heavy.example.com"), 99),
            Provider::new(create_test_target("https://light.example.com"), 1),
        ];

        let fallback = Some(FallbackConfig {
            enabled: true,
            with_replacement: true,
            max_attempts: Some(10),
            ..Default::default()
        });

        let pool = ProviderPool::with_config(
            providers,
            None,
            None,
            None,
            fallback,
            LoadBalanceStrategy::WeightedRandom,
            false,
            Vec::new(),
        );

        let mut heavy_count = 0;
        let iterations = 100;
        for _ in 0..iterations {
            let order: Vec<_> = pool.select_iter().collect();
            heavy_count += order
                .iter()
                .filter(|(_, t, _)| t.url.as_str() == "https://heavy.example.com/")
                .count();
        }

        let total = iterations * 10;
        let percentage = (heavy_count * 100) / total;
        assert!(
            percentage > 85,
            "Heavy provider (99:1 weight) should appear >85% of the time, got {}%",
            percentage
        );
    }

    #[test]
    fn test_evaluate_routing_rules_no_rules() {
        let pool = ProviderPool::new(vec![Provider::new(
            create_test_target("https://api.example.com"),
            1,
        )]);

        let labels = HashMap::from([("purpose".to_string(), "batch".to_string())]);
        assert!(pool.evaluate_routing_rules(&labels).is_none());
    }

    #[test]
    fn test_evaluate_routing_rules_deny() {
        use crate::target::{RoutingAction, RoutingRule};

        let rules = vec![RoutingRule {
            match_labels: HashMap::from([("purpose".to_string(), "playground".to_string())]),
            action: RoutingAction::Deny,
        }];

        let pool = ProviderPool::with_config(
            vec![Provider::new(
                create_test_target("https://api.example.com"),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            false,
            rules,
        );

        let labels = HashMap::from([("purpose".to_string(), "playground".to_string())]);
        assert!(matches!(
            pool.evaluate_routing_rules(&labels),
            Some(RoutingAction::Deny)
        ));

        let labels = HashMap::from([("purpose".to_string(), "batch".to_string())]);
        assert!(pool.evaluate_routing_rules(&labels).is_none());

        assert!(pool.evaluate_routing_rules(&HashMap::new()).is_none());
    }

    #[test]
    fn test_evaluate_routing_rules_redirect() {
        use crate::target::{RoutingAction, RoutingRule};

        let rules = vec![RoutingRule {
            match_labels: HashMap::from([("purpose".to_string(), "batch".to_string())]),
            action: RoutingAction::Redirect {
                target: "gpt-4o-mini".to_string(),
            },
        }];

        let pool = ProviderPool::with_config(
            vec![Provider::new(
                create_test_target("https://api.example.com"),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            false,
            rules,
        );

        let labels = HashMap::from([("purpose".to_string(), "batch".to_string())]);
        match pool.evaluate_routing_rules(&labels) {
            Some(RoutingAction::Redirect { target }) => {
                assert_eq!(target, "gpt-4o-mini");
            }
            other => panic!("Expected Redirect, got {:?}", other),
        }
    }

    #[test]
    fn test_evaluate_routing_rules_first_match_wins() {
        use crate::target::{RoutingAction, RoutingRule};

        let rules = vec![
            RoutingRule {
                match_labels: HashMap::from([("purpose".to_string(), "batch".to_string())]),
                action: RoutingAction::Deny,
            },
            RoutingRule {
                match_labels: HashMap::from([("purpose".to_string(), "batch".to_string())]),
                action: RoutingAction::Redirect {
                    target: "other".to_string(),
                },
            },
        ];

        let pool = ProviderPool::with_config(
            vec![Provider::new(
                create_test_target("https://api.example.com"),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            false,
            rules,
        );

        let labels = HashMap::from([("purpose".to_string(), "batch".to_string())]);
        assert!(matches!(
            pool.evaluate_routing_rules(&labels),
            Some(RoutingAction::Deny)
        ));
    }

    #[test]
    fn test_evaluate_routing_rules_multiple_label_conditions() {
        use crate::target::{RoutingAction, RoutingRule};

        let rules = vec![RoutingRule {
            match_labels: HashMap::from([
                ("purpose".to_string(), "batch".to_string()),
                ("tier".to_string(), "free".to_string()),
            ]),
            action: RoutingAction::Deny,
        }];

        let pool = ProviderPool::with_config(
            vec![Provider::new(
                create_test_target("https://api.example.com"),
                1,
            )],
            None,
            None,
            None,
            None,
            LoadBalanceStrategy::default(),
            false,
            rules,
        );

        let labels = HashMap::from([
            ("purpose".to_string(), "batch".to_string()),
            ("tier".to_string(), "free".to_string()),
        ]);
        assert!(matches!(
            pool.evaluate_routing_rules(&labels),
            Some(RoutingAction::Deny)
        ));

        let labels = HashMap::from([("purpose".to_string(), "batch".to_string())]);
        assert!(pool.evaluate_routing_rules(&labels).is_none());

        let labels = HashMap::from([
            ("purpose".to_string(), "batch".to_string()),
            ("tier".to_string(), "free".to_string()),
            ("org".to_string(), "acme".to_string()),
        ]);
        assert!(matches!(
            pool.evaluate_routing_rules(&labels),
            Some(RoutingAction::Deny)
        ));
    }

    #[test]
    fn test_adopt_provider_state_preserves_active_counts() {
        let providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 3),
            Provider::new(create_test_target("https://api2.example.com"), 1),
        ];
        let old_pool = ProviderPool::new(providers);

        let mut guards = Vec::new();
        for _ in 0..8 {
            if let Some((_, _, guard)) = old_pool.select() {
                guards.push(guard);
            }
        }

        let old_active_0 = old_pool.providers()[0].active_connections();
        let old_active_1 = old_pool.providers()[1].active_connections();
        assert!(
            old_active_0 > 0,
            "Should have active connections on provider 0"
        );
        assert!(
            old_active_1 > 0,
            "Should have active connections on provider 1"
        );

        let new_providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 3),
            Provider::new(create_test_target("https://api2.example.com"), 1),
        ];
        let mut new_pool = ProviderPool::new(new_providers);

        assert_eq!(new_pool.providers()[0].active_connections(), 0);
        assert_eq!(new_pool.providers()[1].active_connections(), 0);

        new_pool.adopt_provider_state(&old_pool);

        assert_eq!(new_pool.providers()[0].active_connections(), old_active_0);
        assert_eq!(new_pool.providers()[1].active_connections(), old_active_1);

        guards.pop();
        let total_after = new_pool.providers()[0].active_connections()
            + new_pool.providers()[1].active_connections();
        assert_eq!(total_after, old_active_0 + old_active_1 - 1);
    }

    #[test]
    fn test_adopt_provider_state_new_provider_starts_at_zero() {
        let old_providers = vec![Provider::new(
            create_test_target("https://api1.example.com"),
            1,
        )];
        let old_pool = ProviderPool::new(old_providers);

        let _guards: Vec<_> = (0..5)
            .filter_map(|_| old_pool.select().map(|(_, _, g)| g))
            .collect();

        let new_providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 1),
            Provider::new(create_test_target("https://api2.example.com"), 1),
        ];
        let mut new_pool = ProviderPool::new(new_providers);
        new_pool.adopt_provider_state(&old_pool);

        assert_eq!(new_pool.providers()[0].active_connections(), 5);
        assert_eq!(new_pool.providers()[1].active_connections(), 0);
    }

    #[test]
    fn test_adopt_provider_state_removed_provider_ignored() {
        let old_providers = vec![
            Provider::new(create_test_target("https://api1.example.com"), 1),
            Provider::new(create_test_target("https://api2.example.com"), 1),
        ];
        let old_pool = ProviderPool::new(old_providers);

        let _guards: Vec<_> = (0..4)
            .filter_map(|_| old_pool.select().map(|(_, _, g)| g))
            .collect();

        let new_providers = vec![Provider::new(
            create_test_target("https://api1.example.com"),
            1,
        )];
        let mut new_pool = ProviderPool::new(new_providers);
        new_pool.adopt_provider_state(&old_pool);

        assert!(new_pool.providers()[0].active_connections() > 0);
        assert_eq!(new_pool.providers().len(), 1);
    }

    #[test]
    fn test_adopt_provider_state_preserves_pool_concurrency_limiter() {
        use crate::target::ConcurrencyLimiter;

        let old_pool = ProviderPool::with_config(
            vec![Provider::new(
                create_test_target("https://api1.example.com"),
                1,
            )],
            None,
            None,
            Some(ConcurrencyLimiter::with_limit(100)),
            None,
            LoadBalanceStrategy::default(),
            false,
            Vec::new(),
        );

        let _guard1 = old_pool.pool_concurrency_limiter().unwrap().try_acquire();
        let _guard2 = old_pool.pool_concurrency_limiter().unwrap().try_acquire();
        assert_eq!(old_pool.pool_concurrency_limiter().unwrap().active(), 2);

        let mut new_pool = ProviderPool::with_config(
            vec![Provider::new(
                create_test_target("https://api1.example.com"),
                1,
            )],
            None,
            None,
            Some(ConcurrencyLimiter::with_limit(200)),
            None,
            LoadBalanceStrategy::default(),
            false,
            Vec::new(),
        );

        new_pool.adopt_provider_state(&old_pool);

        assert_eq!(new_pool.pool_concurrency_limiter().unwrap().active(), 2);
        assert_eq!(
            new_pool.pool_concurrency_limiter().unwrap().limit(),
            Some(200)
        );
    }
}
