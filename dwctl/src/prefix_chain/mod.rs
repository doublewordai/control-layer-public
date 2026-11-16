
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use metrics::counter;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::Sha256;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::clickhouse::{ClickhouseConfig, SinkHandle, SinkOptions};
use crate::inference::serving_name::split_served;
use crate::metrics::errors::component::PREFIX_CHAIN;
use crate::prompt_cache::{
    ParseError, PrincipalResolver, TelemetryPolicy, TierPolicy, TokenizerClient, TokenizerError, parse_chat_completions,
};

pub const RECORD_VERSION: u8 = 1;
const HASH_BYTES: usize = 16;

const TABLE: &str = "prompt_chains";
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const MAX_BATCH_ROWS: usize = 5_000;
const QUEUE_CAPACITY: usize = 20_000;
const RETRY_DELAY: Duration = Duration::from_millis(500);
const MAX_IN_FLIGHT: usize = 256;

#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PrefixChainConfig {
    pub enabled: bool,
    #[serde(deserialize_with = "deserialize_model_list")]
    pub models: Option<Vec<String>>,
    pub hmac_key: String,
}

impl fmt::Debug for PrefixChainConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefixChainConfig")
            .field("enabled", &self.enabled)
            .field("models", &self.models)
            .field("hmac_key", &if self.hmac_key.is_empty() { "<unset>" } else { "<redacted>" })
            .finish()
    }
}

pub(crate) fn deserialize_model_list<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<String>>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Seq(Vec<String>),
        Str(String),
    }
    Ok(match Option::<Raw>::deserialize(d)? {
        None => None,
        Some(Raw::Seq(v)) => Some(v),
        Some(Raw::Str(s)) => Some(s.split(',').map(|m| m.trim().to_string()).filter(|m| !m.is_empty()).collect()),
    })
}

impl PrefixChainConfig {
    pub fn validate(&self, clickhouse: Option<&ClickhouseConfig>, tokenizer_url: &str) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if clickhouse.is_none() {
            return Err("prefix_chain.enabled is true but there is no clickhouse section".to_string());
        }
        if tokenizer_url.trim().is_empty() {
            return Err("prefix_chain.enabled is true but cache.tokenizer_url is empty".to_string());
        }
        self.key_bytes()?;
        if let Some(models) = &self.models
            && models.is_empty()
        {
            return Err(
                "prefix_chain.models is an empty list; omit it to capture every model, or set prefix_chain.enabled: false".to_string(),
            );
        }
        Ok(())
    }

    pub fn key_bytes(&self) -> Result<[u8; 32], String> {
        let raw =
            hex::decode(self.hmac_key.trim()).map_err(|_| "prefix_chain.hmac_key is not hex (expected 64 hex characters)".to_string())?;
        <[u8; 32]>::try_from(raw).map_err(|v| format!("prefix_chain.hmac_key decodes to {} bytes, expected 32", v.len()))
    }
}

pub fn sink_options() -> SinkOptions {
    SinkOptions {
        table: TABLE.to_string(),
        flush_interval: FLUSH_INTERVAL,
        max_batch_rows: MAX_BATCH_ROWS,
        queue_capacity: QUEUE_CAPACITY,
        retry_delay: RETRY_DELAY,
    }
}

pub fn key_id(key: &[u8; 32]) -> u32 {
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(key);
    u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]])
}

#[derive(Debug, Clone)]
pub enum ModelFilter {
    All,
    Only(HashSet<String>),
}

impl ModelFilter {
    pub fn from_config(models: &Option<Vec<String>>) -> Self {
        match models {
            None => Self::All,
            Some(list) => Self::Only(list.iter().cloned().collect()),
        }
    }

    pub fn allows(&self, model: &str) -> bool {
        match self {
            Self::All => true,
            Self::Only(set) => set.contains(model),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PromptChainRow {
    pub ts: DateTime<Utc>,
    pub instance_id: Uuid,
    pub correlation_id: i64,
    pub principal_id: Uuid,
    pub model: String,
    pub chain: Vec<String>,
    pub block_roles: Vec<String>,
    pub block_tokens: Vec<u32>,
    pub tokenizer_version: String,
    pub key_version: u32,
    pub v: u8,
}

pub struct Chain {
    pub hashes: Vec<String>,
    pub roles: Vec<String>,
    texts: Vec<String>,
}

impl Chain {
    pub fn len(&self) -> usize {
        self.hashes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }
}

pub struct Observation {
    pub instance_id: Uuid,
    pub correlation_id: i64,
    pub timestamp: DateTime<Utc>,
    pub model: Option<String>,
    pub api_key: Option<crate::crypto::ApiKeyHash>,
    pub path: String,
    pub body: Option<Bytes>,
}

pub struct ChainHasher {
    key: [u8; 32],
    tier_policy: TierPolicy,
    telemetry: TelemetryPolicy,
}

impl ChainHasher {
    pub fn new(key: [u8; 32], tier_policy: TierPolicy, telemetry: TelemetryPolicy) -> Self {
        Self {
            key,
            tier_policy,
            telemetry,
        }
    }

    pub fn chain_for(&self, body: &[u8]) -> Result<Chain, ParseError> {
        let parsed = parse_chat_completions(body, &self.tier_policy, &self.telemetry)?;
        let hashes = parsed
            .cumulative_hashes
            .iter()
            .map(|h| hex::encode(&self.mac(h)[..HASH_BYTES]))
            .collect();
        let roles = parsed.blocks.iter().map(|b| b.role.clone()).collect();
        let texts = parsed.blocks.into_iter().map(|b| b.text).collect();
        Ok(Chain { hashes, roles, texts })
    }

    fn mac(&self, data: &[u8]) -> [u8; 32] {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("a 32-byte key is always a valid HMAC key");
        mac.update(data);
        mac.finalize().into_bytes().into()
    }
}

pub struct PrefixChainRecorder {
    hasher: ChainHasher,
    key_id: u32,
    filter: ModelFilter,
    tokenizer: TokenizerClient,
    principal: PrincipalResolver,
    sink: SinkHandle<PromptChainRow>,
    in_flight: Arc<Semaphore>,
}

impl fmt::Debug for PrefixChainRecorder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrefixChainRecorder")
            .field("key_id", &self.key_id)
            .field("filter", &self.filter)
            .finish_non_exhaustive()
    }
}

pub(crate) fn parse_error_kind(e: &ParseError) -> &'static str {
    match e {
        ParseError::Json(_) => "json",
        ParseError::TooManyBreakpoints => "too_many_breakpoints",
        ParseError::InvalidTtl(_) => "invalid_ttl",
        ParseError::UnsupportedType(_) => "unsupported_type",
        ParseError::DisabledTier(_) => "disabled_tier",
        ParseError::MalformedCacheControl => "malformed_cache_control",
        ParseError::AutomaticTtlConflict => "automatic_ttl_conflict",
        ParseError::NoAutomaticSlot => "automatic_no_slot",
    }
}

fn skipped(reason: &'static str) {
    counter!("dwctl_prefix_chain_skipped_total", "reason" => reason).increment(1);
}

impl PrefixChainRecorder {
    pub fn new(
        key: [u8; 32],
        filter: ModelFilter,
        tier_policy: TierPolicy,
        telemetry: TelemetryPolicy,
        tokenizer: TokenizerClient,
        principal: PrincipalResolver,
        sink: SinkHandle<PromptChainRow>,
    ) -> Self {
        Self {
            key_id: key_id(&key),
            hasher: ChainHasher::new(key, tier_policy, telemetry),
            filter,
            tokenizer,
            principal,
            sink,
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        }
    }

    pub fn observe(self: &Arc<Self>, obs: Observation) {
        if !obs.path.ends_with("/chat/completions") {
            skipped("not_chat");
            return;
        }
        let Some(model) = obs.model else {
            skipped("no_model");
            return;
        };
        if !self.filter.allows(split_served(&model).0) {
            skipped("model_filtered");
            return;
        }
        let Some(body) = obs.body else {
            skipped("no_body");
            return;
        };
        let Some(api_key) = obs.api_key else {
            skipped("no_api_key");
            return;
        };
        let chain = match self.chain_for(&body) {
            Ok(c) if !c.is_empty() => c,
            Ok(_) => {
                skipped("empty_prompt");
                return;
            }
            Err(e) => {
                let kind = parse_error_kind(&e);
                debug!(
                    correlation_id = obs.correlation_id,
                    kind, "prefix chain: prompt not parseable; skipped"
                );
                skipped("parse_error");
                return;
            }
        };
        let Ok(permit) = Arc::clone(&self.in_flight).try_acquire_owned() else {
            skipped("backpressure");
            return;
        };
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let _permit = permit;
            this.finish(model, api_key, chain, obs.instance_id, obs.correlation_id, obs.timestamp)
                .await;
        });
    }

    pub fn chain_for(&self, body: &[u8]) -> Result<Chain, ParseError> {
        self.hasher.chain_for(body)
    }

    async fn finish(
        &self,
        model: String,
        api_key: crate::crypto::ApiKeyHash,
        chain: Chain,
        instance_id: Uuid,
        correlation_id: i64,
        ts: DateTime<Utc>,
    ) {
        let principal_id = match self.principal.resolve(&api_key).await {
            Ok(Some(p)) => p,
            Ok(None) => {
                skipped("unknown_principal");
                return;
            }
            Err(e) => {
                crate::background_error!(PREFIX_CHAIN, "principal_lookup", Error, correlation_id, error = %e, "prefix chain: principal lookup failed; skipped");
                skipped("principal_error");
                return;
            }
        };

        let (block_tokens, tokenizer_version) = match self.tokenizer.tokenize(split_served(&model).0, &chain.texts).await {
            Ok(r) if r.segment_counts.len() == chain.len() => {
                counter!("dwctl_prefix_chain_tokenize_total", "outcome" => "ok").increment(1);
                (r.segment_counts, r.tokenizer_version)
            }
            Ok(r) => {
                warn!(
                    correlation_id,
                    got = r.segment_counts.len(),
                    want = chain.len(),
                    "prefix chain: tokenizer returned the wrong number of counts; recorded without counts"
                );
                counter!("dwctl_prefix_chain_tokenize_total", "outcome" => "mismatch").increment(1);
                (Vec::new(), String::new())
            }
            Err(TokenizerError::Unmapped(_)) => {
                counter!("dwctl_prefix_chain_tokenize_total", "outcome" => "unmapped").increment(1);
                (Vec::new(), String::new())
            }
            Err(e) => {
                crate::background_error!(PREFIX_CHAIN, "tokenize", Warning, correlation_id, error = %e, "prefix chain: tokenizer call failed; recorded without counts");
                counter!("dwctl_prefix_chain_tokenize_total", "outcome" => "error").increment(1);
                (Vec::new(), String::new())
            }
        };

        let row = PromptChainRow {
            ts,
            instance_id,
            correlation_id,
            principal_id,
            model,
            chain: chain.hashes,
            block_roles: chain.roles,
            block_tokens,
            tokenizer_version,
            key_version: self.key_id,
            v: RECORD_VERSION,
        };
        let outcome = if self.sink.send(row) { "recorded" } else { "dropped" };
        counter!("dwctl_prefix_chain_records_total", "outcome" => outcome).increment(1);
    }
}

pub struct PrefixChainHandler {
    recorder: Arc<PrefixChainRecorder>,
    instance_id: Uuid,
    config: crate::config::Config,
    internal_auth: crate::auth::internal_credential::InternalAuthState,
}

impl PrefixChainHandler {
    pub fn new(
        recorder: Arc<PrefixChainRecorder>,
        instance_id: Uuid,
        config: crate::config::Config,
        internal_auth: crate::auth::internal_credential::InternalAuthState,
    ) -> Self {
        Self {
            recorder,
            instance_id,
            config,
            internal_auth,
        }
    }
}

impl outlet::RequestHandler for PrefixChainHandler {
    async fn handle_request(&self, _data: outlet::RequestData) {}

    async fn handle_response(&self, request_data: outlet::RequestData, response_data: outlet::ResponseData) {
        if !response_data.status.is_success() {
            return;
        }
        if !request_data.uri.path().ends_with("/chat/completions") {
            skipped("not_chat");
            return;
        }
        let bearer = match crate::request_logging::serializers::Auth::from_request(&request_data, &self.config) {
            crate::request_logging::serializers::Auth::ApiKey { bearer_token } => Some(bearer_token),
            crate::request_logging::serializers::Auth::None => None,
        };
        let carried = crate::auth::internal_credential::carried_key_hash(&response_data.extensions);
        let api_key = self.internal_auth.captured_key_hash(bearer.as_deref(), carried);
        let model = request_data
            .body
            .as_ref()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok())
            .and_then(|v| v.get("model").and_then(|m| m.as_str()).map(String::from));
        self.recorder.observe(Observation {
            instance_id: self.instance_id,
            correlation_id: request_data.correlation_id as i64,
            timestamp: DateTime::<Utc>::from(request_data.timestamp),
            model,
            api_key,
            path: request_data.uri.path().to_string(),
            body: request_data.body.clone(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::ClickhouseConfig;
    use tokio::sync::mpsc;

    fn key(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    fn policies() -> (TierPolicy, TelemetryPolicy) {
        (
            TierPolicy::from_config(&["5m".into(), "1h".into(), "24h".into()], "5m"),
            TelemetryPolicy::from_config(false, &[]),
        )
    }

    fn recorder_with(pool: sqlx::PgPool, key: [u8; 32], filter: ModelFilter) -> (Arc<PrefixChainRecorder>, mpsc::Receiver<PromptChainRow>) {
        let (tx, rx) = mpsc::channel(8);
        let (tier, telemetry) = policies();
        let rec = PrefixChainRecorder::new(
            key,
            filter,
            tier,
            telemetry,
            TokenizerClient::new("http://127.0.0.1:9"),
            PrincipalResolver::new(pool),
            SinkHandle::for_test(tx, "prompt_chains"),
        );
        (Arc::new(rec), rx)
    }

    fn hasher(key: [u8; 32]) -> ChainHasher {
        let (tier, telemetry) = policies();
        ChainHasher::new(key, tier, telemetry)
    }

    fn body(messages: &[(&str, &str)], tools: usize) -> Vec<u8> {
        let msgs: Vec<serde_json::Value> = messages.iter().map(|(r, c)| serde_json::json!({"role": r, "content": c})).collect();
        let tools: Vec<serde_json::Value> = (0..tools)
            .map(|i| serde_json::json!({"type": "function", "function": {"name": format!("t{i}"), "parameters": {"type": "object"}}}))
            .collect();
        let mut v = serde_json::json!({"model": "m", "messages": msgs});
        if !tools.is_empty() {
            v["tools"] = serde_json::Value::Array(tools);
        }
        serde_json::to_vec(&v).unwrap()
    }

    #[test]
    fn chain_is_one_entry_per_block_in_parse_order() {
        let rec = hasher(key(1));
        let c = rec
            .chain_for(&body(&[("system", "s"), ("user", "u"), ("assistant", "a")], 2))
            .unwrap();
        assert_eq!(c.roles, vec!["tool_definition", "tool_definition", "system", "user", "assistant"]);
        assert_eq!(c.hashes.len(), 5);
        assert!(c.hashes.iter().all(|h| h.len() == 32 && h.chars().all(|ch| ch.is_ascii_hexdigit())));
        assert_eq!(c.hashes.iter().collect::<HashSet<_>>().len(), 5, "cumulative hashes are distinct");
    }

    #[test]
    fn extending_a_conversation_extends_the_chain() {
        let rec = hasher(key(1));
        let one = rec.chain_for(&body(&[("system", "s"), ("user", "u1")], 0)).unwrap();
        let two = rec
            .chain_for(&body(&[("system", "s"), ("user", "u1"), ("assistant", "a1"), ("user", "u2")], 0))
            .unwrap();
        assert_eq!(&two.hashes[..2], &one.hashes[..]);
        let other = rec
            .chain_for(&body(
                &[("system", "s"), ("user", "DIFFERENT"), ("assistant", "a1"), ("user", "u2")],
                0,
            ))
            .unwrap();
        assert_eq!(other.hashes[0], two.hashes[0], "shared system prompt");
        assert!(
            other.hashes[1..].iter().zip(&two.hashes[1..]).all(|(a, b)| a != b),
            "everything after the divergence differs"
        );
    }

    #[test]
    fn same_body_same_key_is_deterministic_and_a_different_key_is_unrelated() {
        let a = hasher(key(1));
        let b = hasher(key(1));
        let c = hasher(key(2));
        let payload = body(&[("user", "hello")], 0);
        assert_eq!(a.chain_for(&payload).unwrap().hashes, b.chain_for(&payload).unwrap().hashes);
        assert_ne!(a.chain_for(&payload).unwrap().hashes, c.chain_for(&payload).unwrap().hashes);
    }

    #[test]
    fn chain_never_contains_content() {
        let rec = hasher(key(1));
        let c = rec.chain_for(&body(&[("user", "the secret phrase")], 0)).unwrap();
        let serialized = serde_json::to_string(&c.hashes).unwrap() + &serde_json::to_string(&c.roles).unwrap();
        assert!(!serialized.contains("secret"));
    }

    #[test]
    fn unparseable_bodies_are_errors_not_panics() {
        let rec = hasher(key(1));
        assert!(rec.chain_for(b"not json").is_err());
        assert!(rec.chain_for(&body(&[], 0)).unwrap().is_empty(), "no blocks, empty chain");
    }

    #[test]
    fn model_filter_semantics() {
        assert!(ModelFilter::from_config(&None).allows("anything"));
        let only = ModelFilter::from_config(&Some(vec!["vendor/model-a".into()]));
        assert!(only.allows("vendor/model-a"));
        assert!(!only.allows("Vendor/model-a"), "exact, case-sensitive");
        assert!(!only.allows("other"));
    }

    fn ch() -> ClickhouseConfig {
        ClickhouseConfig {
            endpoint_url: "https://x.clickhouse.cloud:8443".into(),
            database: "clay".into(),
            user: "dwctl".into(),
            password: "pw".into(),
        }
    }

    fn enabled() -> PrefixChainConfig {
        PrefixChainConfig {
            enabled: true,
            hmac_key: "ab".repeat(32),
            ..Default::default()
        }
    }

    #[test]
    fn key_id_is_derived_from_the_key() {
        assert_eq!(key_id(&key(1)), key_id(&key(1)));
        assert_ne!(key_id(&key(1)), key_id(&key(2)));
    }

    #[test]
    fn disabled_config_ignores_everything_else() {
        let mut c = PrefixChainConfig::default();
        c.hmac_key = "garbage".into();
        c.models = Some(vec![]);
        assert!(c.validate(None, "").is_ok());
    }

    #[test]
    fn enabled_config_requires_the_connection_the_tokenizer_and_a_real_key() {
        assert!(enabled().validate(Some(&ch()), "http://tokenizer-svc:8088").is_ok());
        assert!(
            enabled()
                .validate(None, "http://tokenizer-svc:8088")
                .unwrap_err()
                .contains("clickhouse section")
        );
        assert!(enabled().validate(Some(&ch()), "").unwrap_err().contains("tokenizer_url"));
        let mut c = enabled();
        c.hmac_key = "abc".into();
        assert!(c.validate(Some(&ch()), "http://t").unwrap_err().contains("hmac_key"));
        let mut c = enabled();
        c.hmac_key = "00".repeat(16);
        assert!(c.validate(Some(&ch()), "http://t").unwrap_err().contains("expected 32"));
    }

    #[test]
    fn empty_model_list_is_rejected_but_null_and_lists_are_fine() {
        let mut c = enabled();
        c.models = Some(vec![]);
        assert!(c.validate(Some(&ch()), "http://t").unwrap_err().contains("empty list"));
        c.models = None;
        assert!(c.validate(Some(&ch()), "http://t").is_ok());
        c.models = Some(vec!["a".into()]);
        assert!(c.validate(Some(&ch()), "http://t").is_ok());
    }

    #[test]
    fn models_deserializes_from_a_list_or_a_comma_string() {
        let from_yaml: PrefixChainConfig = serde_json::from_str(r#"{"models": ["a", "b"]}"#).unwrap();
        assert_eq!(from_yaml.models, Some(vec!["a".into(), "b".into()]));
        let from_env: PrefixChainConfig = serde_json::from_str(r#"{"models": "a, b ,c"}"#).unwrap();
        assert_eq!(from_env.models, Some(vec!["a".into(), "b".into(), "c".into()]));
        let empty: PrefixChainConfig = serde_json::from_str(r#"{"models": ""}"#).unwrap();
        assert_eq!(
            empty.models,
            Some(vec![]),
            "an empty string is the empty list, which validation rejects"
        );
        let absent: PrefixChainConfig = serde_json::from_str(r#"{"enabled": false}"#).unwrap();
        assert_eq!(absent.models, None);
        let null: PrefixChainConfig = serde_json::from_str(r#"{"models": null}"#).unwrap();
        assert_eq!(null.models, None);
    }

    #[test]
    fn parse_errors_log_a_fixed_kind_not_the_client_value() {
        let (tier, telemetry) = policies();
        let err = parse_chat_completions(
            br#"{"model":"m","tools":[{"type":"function","function":{"name":"t"},"cache_control":{"type":"SECRET-VALUE"}}],"messages":[{"role":"user","content":"x"}]}"#,
            &tier,
            &telemetry,
        )
        .unwrap_err();
        assert!(err.to_string().contains("SECRET-VALUE"), "Display echoes the client value: {err}");
        assert!(!parse_error_kind(&err).contains("SECRET"));
    }

    #[test]
    fn debug_output_redacts_the_key() {
        let s = format!("{:?}", enabled());
        assert!(s.contains("<redacted>"));
        assert!(!s.contains("abab"), "{s}");
    }

    fn observation(path: &str, model: Option<&str>, api_key: Option<&str>, body: Option<Vec<u8>>) -> Observation {
        Observation {
            instance_id: Uuid::nil(),
            correlation_id: 7,
            timestamp: Utc::now(),
            model: model.map(String::from),
            api_key: api_key.map(crate::crypto::hash_api_key),
            path: path.to_string(),
            body: body.map(Bytes::from),
        }
    }

    #[dwctl_test_macros::test]
    async fn observe_filters_before_spawning(pool: sqlx::PgPool) {
        let (rec, mut rx) = recorder_with(pool, key(1), ModelFilter::from_config(&Some(vec!["m".into()])));
        let b = body(&[("user", "hi")], 0);
        rec.observe(observation("/embeddings", Some("m"), Some("sk"), Some(b.clone())));
        rec.observe(observation("/chat/completions", Some("other"), Some("sk"), Some(b.clone())));
        rec.observe(observation("/chat/completions", None, Some("sk"), Some(b.clone())));
        rec.observe(observation("/chat/completions", Some("m"), None, Some(b.clone())));
        rec.observe(observation("/chat/completions", Some("m"), Some("sk"), None));
        rec.observe(observation("/chat/completions", Some("m"), Some("sk"), Some(b"nope".to_vec())));
        let nothing = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await;
        assert!(nothing.is_err(), "nothing recorded");
    }
}
