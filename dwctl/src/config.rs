
use clap::Parser;
use dashmap::DashMap;
use figment::{
    Figment,
    providers::{Env, Format, Yaml},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    ffi::OsString,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use url::Url;

use crate::api::models::users::Role;
use crate::errors::Error;
use crate::sample_files::SampleFilesConfig;

pub static ONWARDS_CONFIG_CHANGED_CHANNEL: &str = "auth_config_changed";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short = 'f', long, env = "DWCTL_CONFIG", default_value = "config.yaml")]
    pub config: PathBuf,

    #[arg(long)]
    pub validate: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub dashboard_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_replica_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_pooled_url: Option<String>,
    pub database: DatabaseConfig,
    pub slow_statement_threshold_ms: u64,
    pub admin_email: String,
    pub admin_password: Option<String>,
    pub secret_key: Option<String>,
    pub model_sources: Vec<ModelSource>,
    pub model_provisioning: ModelProvisioningConfig,
    pub migrations: MigrationsConfig,
    pub metadata: Metadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payment: Option<PaymentConfig>,
    pub auth: AuthConfig,
    pub batches: BatchConfig,
    pub background_services: BackgroundServicesConfig,
    pub enable_metrics: bool,
    pub enable_request_logging: bool,
    pub enable_analytics: bool,
    #[serde(default)]
    pub analytics: AnalyticsConfig,
    pub enable_otel_export: bool,
    pub log_format: LogFormat,
    pub credits: CreditsConfig,
    pub sample_files: SampleFilesConfig,
    pub limits: LimitsConfig,
    pub email: EmailConfig,
    pub onwards: OnwardsConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub onboarding_url: Option<String>,
    pub support_email: String,
    #[serde(default)]
    pub connections: ConnectionsConfig,
    #[serde(default)]
    pub image_normalizer: crate::image_normalizer::ImageNormalizerConfig,
    #[serde(default)]
    pub keystore: Option<crate::keystore::KeystoreConfig>,
    #[serde(default)]
    pub openapi: OpenApiConfig,
    #[serde(default)]
    pub discovery: DiscoveryConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub clickhouse: Option<crate::clickhouse::ClickhouseConfig>,
    #[serde(default)]
    pub prefix_chain: crate::prefix_chain::PrefixChainConfig,
    #[serde(default)]
    pub reference_cache: crate::reference_cache::ReferenceCacheConfig,
    #[serde(default)]
    pub continuation: ContinuationConfig,
    #[serde(default)]
    pub heap_profiling: crate::profiling::HeapProfilingConfig,
    #[serde(default)]
    pub scheduling: SchedulingConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchedulingConfig {
    pub toleration_presets: Vec<TolerationPreset>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TolerationPreset {
    pub id: String,
    pub label: String,
    pub description: String,
    pub tolerations: Vec<crate::scheduling::SchedulingToleration>,
}

impl SchedulingConfig {
    pub fn default_preset() -> TolerationPreset {
        TolerationPreset {
            id: "no-overflow".to_string(),
            label: "No overflow capacity".to_string(),
            description: "Never placed on capacity the backend marks with any taint, such as overflow capacity; \
                          requests may be refused when dedicated capacity is full."
                .to_string(),
            tolerations: Vec::new(),
        }
    }

    pub fn preset(&self, id: &str) -> Option<&TolerationPreset> {
        self.toleration_presets.iter().find(|preset| preset.id == id)
    }

    pub fn resolve(&self, id: Option<&str>) -> Option<&[crate::scheduling::SchedulingToleration]> {
        id.and_then(|id| self.preset(id)).map(|preset| preset.tolerations.as_slice())
    }

    pub fn validate(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for (index, preset) in self.toleration_presets.iter().enumerate() {
            if preset.id.trim().is_empty() {
                return Err(format!("scheduling.toleration_presets[{index}] has an empty id"));
            }
            if preset.id != preset.id.trim() {
                return Err(format!(
                    "scheduling.toleration_presets[{index}] id {:?} has leading or trailing whitespace",
                    preset.id
                ));
            }
            if preset.id == crate::scheduling::DEFAULT_PRESET_SENTINEL {
                return Err(format!(
                    "scheduling.toleration_presets[{index}] id {:?} is reserved for the dashboard's \
                     \"Default\" option",
                    preset.id
                ));
            }
            if !seen.insert(preset.id.as_str()) {
                return Err(format!("scheduling.toleration_presets has a duplicate id {:?}", preset.id));
            }
            if preset.label.trim().is_empty() {
                return Err(format!(
                    "scheduling.toleration_presets[{index}] ({:?}) has an empty label",
                    preset.id
                ));
            }
            if preset.description.trim().is_empty() {
                return Err(format!(
                    "scheduling.toleration_presets[{index}] ({:?}) has an empty description",
                    preset.id
                ));
            }
            crate::scheduling::PinnedTolerations(preset.tolerations.clone())
                .validate()
                .map_err(|error| format!("scheduling.toleration_presets[{:?}] is invalid: {error}", preset.id))?;
        }
        Ok(())
    }
}

impl Default for SchedulingConfig {
    fn default() -> Self {
        Self {
            toleration_presets: vec![Self::default_preset()],
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OpenApiConfig {
    pub admin_enabled: bool,
    pub ai_enabled: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiscoveryConfig {
    pub list_class_routes: bool,
}

impl Default for OpenApiConfig {
    fn default() -> Self {
        Self {
            admin_enabled: true,
            ai_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolSettings {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
}

impl Default for PoolSettings {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_connections: 0,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 600,
            max_lifetime_secs: 1800,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PooledSchemaMode {
    #[default]
    RoleDefault,
    Transaction,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ComponentDb {
    Schema {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pooled_url: Option<String>,
        #[serde(default)]
        pooled_schema_mode: PooledSchemaMode,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replica_url: Option<String>,
        #[serde(default)]
        pool: PoolSettings,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replica_pool: Option<PoolSettings>,
        #[serde(default = "default_direct_pool")]
        direct_pool: PoolSettings,
    },
    Dedicated {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replica_url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pooled_url: Option<String>,
        #[serde(default)]
        pool: PoolSettings,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replica_pool: Option<PoolSettings>,
        #[serde(default = "default_direct_pool")]
        direct_pool: PoolSettings,
    },
}

impl ComponentDb {
    pub(crate) fn validate_schema_pooling(&self, component_name: &str, main_is_split: bool) -> Result<(), String> {
        if let Self::Schema {
            url,
            pooled_url,
            pooled_schema_mode,
            ..
        } = self
        {
            if *pooled_schema_mode == PooledSchemaMode::Transaction && component_name != "fusillade" {
                return Err(format!(
                    "database.{component_name}.pooled_schema_mode: transaction is supported only for fusillade"
                ));
            }
            if url.is_some() && main_is_split && pooled_url.is_none() {
                return Err(format!(
                    "database.{component_name}.pooled_url is required with an explicit schema url when main uses a pooled endpoint"
                ));
            }
        }
        Ok(())
    }

    pub fn pool_settings(&self) -> &PoolSettings {
        match self {
            ComponentDb::Schema { pool, .. } => pool,
            ComponentDb::Dedicated { pool, .. } => pool,
        }
    }

    pub fn direct_pool_settings(&self) -> &PoolSettings {
        match self {
            ComponentDb::Schema { direct_pool, .. } => direct_pool,
            ComponentDb::Dedicated { direct_pool, .. } => direct_pool,
        }
    }

    pub fn pooled_url(&self) -> Option<&str> {
        match self {
            ComponentDb::Schema { pooled_url, .. } | ComponentDb::Dedicated { pooled_url, .. } => pooled_url.as_deref(),
        }
    }

    pub fn replica_pool_settings(&self) -> &PoolSettings {
        match self {
            ComponentDb::Schema { pool, replica_pool, .. } => replica_pool.as_ref().unwrap_or(pool),
            ComponentDb::Dedicated { pool, replica_pool, .. } => replica_pool.as_ref().unwrap_or(pool),
        }
    }
}

pub fn default_fusillade_component() -> ComponentDb {
    ComponentDb::Schema {
        url: None,
        pooled_schema_mode: PooledSchemaMode::default(),
        pooled_url: None,
        replica_url: None,
        name: "fusillade".into(),
        pool: PoolSettings {
            max_connections: 20,
            min_connections: 2,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 600,
            max_lifetime_secs: 1800,
        },
        replica_pool: None,
        direct_pool: default_direct_pool(),
    }
}

pub fn default_outlet_component() -> ComponentDb {
    ComponentDb::Schema {
        url: None,
        pooled_schema_mode: PooledSchemaMode::default(),
        pooled_url: None,
        replica_url: None,
        name: "outlet".into(),
        pool: PoolSettings {
            max_connections: 5,
            min_connections: 0,
            acquire_timeout_secs: 30,
            idle_timeout_secs: 600,
            max_lifetime_secs: 1800,
        },
        replica_pool: None,
        direct_pool: default_direct_pool(),
    }
}

pub fn default_direct_pool() -> PoolSettings {
    PoolSettings {
        max_connections: 8,
        min_connections: 0,
        ..Default::default()
    }
}

pub fn default_underway_pool() -> PoolSettings {
    PoolSettings {
        max_connections: 100,
        min_connections: 0,
        ..Default::default()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DatabaseConfig {
    Embedded {
        #[serde(skip_serializing_if = "Option::is_none")]
        data_dir: Option<PathBuf>,
        #[serde(default)]
        persistent: bool,
        #[serde(default)]
        pool: PoolSettings,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replica_pool: Option<PoolSettings>,
        #[serde(default = "default_fusillade_component")]
        fusillade: ComponentDb,
        #[serde(default = "default_outlet_component")]
        outlet: ComponentDb,
        #[serde(default = "default_underway_pool")]
        underway_pool: PoolSettings,
        #[serde(default = "default_direct_pool")]
        direct_pool: PoolSettings,
    },
    External {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replica_url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pooled_url: Option<String>,
        #[serde(default)]
        pool: PoolSettings,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replica_pool: Option<PoolSettings>,
        #[serde(default = "default_fusillade_component")]
        fusillade: ComponentDb,
        #[serde(default = "default_outlet_component")]
        outlet: ComponentDb,
        #[serde(default = "default_underway_pool")]
        underway_pool: PoolSettings,
        #[serde(default = "default_direct_pool")]
        direct_pool: PoolSettings,
    },
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        #[cfg(feature = "embedded-db")]
        {
            DatabaseConfig::Embedded {
                data_dir: None,
                persistent: false,
                pool: PoolSettings::default(),
                replica_pool: None,
                fusillade: default_fusillade_component(),
                outlet: default_outlet_component(),
                underway_pool: default_underway_pool(),
                direct_pool: default_direct_pool(),
            }
        }
        #[cfg(not(feature = "embedded-db"))]
        {
            DatabaseConfig::External {
                url: "postgres://localhost:5432/control_layer".to_string(),
                replica_url: None,
                pooled_url: None,
                pool: PoolSettings::default(),
                replica_pool: None,
                fusillade: default_fusillade_component(),
                outlet: default_outlet_component(),
                underway_pool: default_underway_pool(),
                direct_pool: default_direct_pool(),
            }
        }
    }
}

impl DatabaseConfig {
    pub fn is_embedded(&self) -> bool {
        matches!(self, DatabaseConfig::Embedded { .. })
    }

    pub fn external_url(&self) -> Option<&str> {
        match self {
            DatabaseConfig::External { url, .. } => Some(url),
            DatabaseConfig::Embedded { .. } => None,
        }
    }

    pub fn external_replica_url(&self) -> Option<&str> {
        match self {
            DatabaseConfig::External { replica_url, .. } => replica_url.as_deref(),
            DatabaseConfig::Embedded { .. } => None,
        }
    }

    pub fn external_pooled_url(&self) -> Option<&str> {
        match self {
            DatabaseConfig::External { pooled_url, .. } => pooled_url.as_deref(),
            DatabaseConfig::Embedded { .. } => None,
        }
    }

    pub fn direct_pool_settings(&self) -> &PoolSettings {
        match self {
            DatabaseConfig::Embedded { direct_pool, .. } => direct_pool,
            DatabaseConfig::External { direct_pool, .. } => direct_pool,
        }
    }

    pub fn embedded_data_dir(&self) -> Option<PathBuf> {
        match self {
            DatabaseConfig::Embedded { data_dir, .. } => data_dir.clone(),
            DatabaseConfig::External { .. } => None,
        }
    }

    pub fn embedded_persistent(&self) -> bool {
        match self {
            DatabaseConfig::Embedded { persistent, .. } => *persistent,
            DatabaseConfig::External { .. } => false,
        }
    }

    pub fn main_pool_settings(&self) -> &PoolSettings {
        match self {
            DatabaseConfig::Embedded { pool, .. } => pool,
            DatabaseConfig::External { pool, .. } => pool,
        }
    }

    pub fn main_replica_pool_settings(&self) -> &PoolSettings {
        match self {
            DatabaseConfig::Embedded { pool, replica_pool, .. } => replica_pool.as_ref().unwrap_or(pool),
            DatabaseConfig::External { pool, replica_pool, .. } => replica_pool.as_ref().unwrap_or(pool),
        }
    }

    pub fn fusillade(&self) -> &ComponentDb {
        match self {
            DatabaseConfig::Embedded { fusillade, .. } => fusillade,
            DatabaseConfig::External { fusillade, .. } => fusillade,
        }
    }

    pub fn outlet(&self) -> &ComponentDb {
        match self {
            DatabaseConfig::Embedded { outlet, .. } => outlet,
            DatabaseConfig::External { outlet, .. } => outlet,
        }
    }

    pub fn underway_pool_settings(&self) -> &PoolSettings {
        match self {
            DatabaseConfig::Embedded { underway_pool, .. } => underway_pool,
            DatabaseConfig::External { underway_pool, .. } => underway_pool,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PaymentConfig {
    Stripe(StripeConfig),
    Dummy(DummyConfig),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StripeConfig {
    pub api_key: String,
    pub webhook_secret: String,
    pub price_id: String,
    #[serde(default)]
    pub enable_invoice_creation: bool,
    pub auto_topup_terms_of_service_text: Option<String>,
    #[serde(default)]
    pub setup_terms_of_service_text: Option<String>,
    pub tax_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_base_url: Option<String>,
}

impl StripeConfig {
    pub fn api_base(&self) -> Result<Option<String>, String> {
        let Some(raw) = &self.api_base_url else {
            return Ok(None);
        };
        let mut url = Url::parse(raw).map_err(|e| format!("payment.stripe.api_base_url {raw:?} is not a valid URL: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!("payment.stripe.api_base_url {raw:?} must be an http or https URL"));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(format!("payment.stripe.api_base_url {raw:?} must not have a query or fragment"));
        }
        if !url.path().ends_with('/') {
            let path = format!("{}/", url.path());
            url.set_path(&path);
        }
        Ok(Some(url.into()))
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DummyConfig {
    pub amount: rust_decimal::Decimal,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Metadata {
    pub region: Option<String>,
    pub organization: Option<String>,
    pub docs_url: String,

    pub docs_jsonl_url: Option<String>,

    pub title: Option<String>,

    pub ai_api_base_url: Option<String>,
}

impl Default for Metadata {
    fn default() -> Self {
        Self {
            region: None,
            organization: None,
            docs_url: "https://doublewordai.github.io/control-layer/".to_string(),
            docs_jsonl_url: None,
            title: None,
            ai_api_base_url: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModelSource {
    pub name: String,
    pub url: Url,
    pub api_key: Option<String>,
    #[serde(default = "ModelSource::default_sync_interval")]
    #[serde(with = "humantime_serde")]
    pub sync_interval: Duration,
    #[serde(default)]
    pub default_models: Option<Vec<DefaultModel>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DefaultModel {
    pub name: String,
    pub add_to_everyone_group: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    pub native: NativeAuthConfig,
    pub proxy_header: ProxyHeaderAuthConfig,
    pub security: SecurityConfig,
    pub default_user_roles: Vec<Role>,
    #[serde(rename = "rate_limits", skip_serializing)]
    pub ignored_rate_limits: serde::de::IgnoredAny,
    pub personal_email_domains: Vec<String>,
}

impl AuthConfig {
    pub fn is_personal_email_domain(&self, domain: &str) -> bool {
        crate::auth::utils::is_builtin_personal_email_domain(domain)
            || self
                .personal_email_domains
                .iter()
                .any(|configured| configured.trim().eq_ignore_ascii_case(domain))
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            native: NativeAuthConfig::default(),
            proxy_header: ProxyHeaderAuthConfig::default(),
            security: SecurityConfig::default(),
            default_user_roles: vec![Role::StandardUser, Role::BackgroundInferenceUser],
            ignored_rate_limits: serde::de::IgnoredAny,
            personal_email_domains: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NativeAuthConfig {
    pub enabled: bool,
    pub allow_registration: bool,
    pub password: PasswordConfig,
    pub session: SessionConfig,
    #[serde(with = "humantime_serde")]
    pub password_reset_token_duration: Duration,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxyHeaderAuthConfig {
    pub enabled: bool,
    pub header_name: String,
    pub email_header_name: String,
    pub groups_field_name: String,
    pub import_idp_groups: bool,
    pub blacklisted_sso_groups: Vec<String>,
    pub provider_field_name: String,
    pub auto_create_users: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SessionConfig {
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub cookie_name: String,
    pub cookie_secure: bool,
    pub cookie_same_site: String,
    pub cookie_domain: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PasswordConfig {
    pub min_length: usize,
    pub max_length: usize,
    pub argon2_memory_kib: u32,
    pub argon2_iterations: u32,
    pub argon2_parallelism: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SecurityConfig {
    #[serde(with = "humantime_serde")]
    pub jwt_expiry: Duration,
    pub cors: CorsConfig,
    pub headers: SecurityHeadersConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SecurityHeadersConfig {
    pub enabled: bool,
    pub frame_options: String,
    pub referrer_policy: String,
    pub permissions_policy: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub content_security_policy: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub content_security_policy_report_only: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub strict_transport_security: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CorsConfig {
    pub allowed_origins: Vec<CorsOrigin>,
    pub allow_credentials: bool,
    pub max_age: Option<u64>,
    pub exposed_headers: Vec<String>,
    pub allow_any_origin_without_credentials: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct EmailConfig {
    #[serde(flatten)]
    pub transport: EmailTransportConfig,
    pub from_email: String,
    pub from_name: String,
    pub reply_to: Option<String>,
    pub templates_dir: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum EmailTransportConfig {
    Smtp {
        host: String,
        port: u16,
        username: String,
        password: String,
        use_tls: bool,
    },
    File {
        path: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FilesConfig {
    pub default_expiry_seconds: i64,
    pub min_expiry_seconds: i64,
    pub max_expiry_seconds: i64,
    pub upload_buffer_size: usize,
    pub download_buffer_size: usize,
    pub batch_insert_size: usize,
}

impl Default for FilesConfig {
    fn default() -> Self {
        Self {
            default_expiry_seconds: 30 * 24 * 60 * 60,
            min_expiry_seconds: 60 * 60,
            max_expiry_seconds: 30 * 24 * 60 * 60,
            upload_buffer_size: 100,
            download_buffer_size: 100,
            batch_insert_size: 5000,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsConfig {
    pub files: FileLimitsConfig,
    pub requests: RequestLimitsConfig,
    pub realtime_inflight: RealtimeInflightLimitsConfig,
    pub batch_inflight: BatchInflightLimitsConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RealtimeInflightLimitsConfig {
    pub enforce: bool,
    pub redis_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BatchInflightLimitsConfig {
    pub enforce: bool,
    #[serde(
        default = "default_batch_inflight_capacity",
        deserialize_with = "deserialize_batch_inflight_default_capacity"
    )]
    pub default_capacity: Option<u32>,
}

pub const DEFAULT_BATCH_INFLIGHT_CAPACITY: u32 = 200;

fn default_batch_inflight_capacity() -> Option<u32> {
    Some(DEFAULT_BATCH_INFLIGHT_CAPACITY)
}

fn deserialize_batch_inflight_default_capacity<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let value: Option<i64> = Option::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(value) if value < 0 => Err(D::Error::custom(format!(
            "limits.batch_inflight.default_capacity must not be negative, got {value}"
        ))),
        Some(0) => Ok(None),
        Some(value) => u32::try_from(value)
            .map(Some)
            .map_err(|_| D::Error::custom(format!("limits.batch_inflight.default_capacity is too large, got {value}"))),
    }
}

impl Default for BatchInflightLimitsConfig {
    fn default() -> Self {
        Self {
            enforce: false,
            default_capacity: default_batch_inflight_capacity(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RequestLimitsConfig {
    pub max_body_size: u64,
}

impl Default for RequestLimitsConfig {
    fn default() -> Self {
        Self {
            max_body_size: 10 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OnwardsConfig {
    pub sse_buffer_limit: usize,
    pub strict_mode: bool,
    pub upstream_rate_limit_message: String,
    pub first_token_timeout_ms: u64,
    pub rejected_params: Vec<String>,
    pub model_allowed_params: std::collections::BTreeMap<String, Vec<String>>,
}

impl Default for OnwardsConfig {
    fn default() -> Self {
        Self {
            sse_buffer_limit: onwards::sse::DEFAULT_SSE_BUFFER_LIMIT,
            strict_mode: false,
            upstream_rate_limit_message:
                "This is a shared best-effort endpoint, rate limited under load – retry with backoff. For production workloads that aren't latency-sensitive, try our async or batch tiers (https://docs.doubleword.ai/inference-api/batch-inference); for a dedicated real-time endpoint with SLAs, higher rate limits, and volume pricing, contact support@doubleword.ai."
                    .to_string(),
            first_token_timeout_ms: 20_000,
            rejected_params: Vec::new(),
            model_allowed_params: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    pub enabled: bool,

    pub tokenizer_url: String,

    pub pricing: CachePricingConfig,

    pub enabled_ttls: Vec<String>,

    pub default_ttl: String,

    pub classify_deadline_secs: u64,

    pub index_conn_retries: u32,

    #[serde(default)]
    pub render_counting: bool,

    pub telemetry_blocks: TelemetryBlockConfig,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tokenizer_url: "http://localhost:8088".to_string(),
            pricing: CachePricingConfig::default(),
            enabled_ttls: vec!["5m".to_string(), "1h".to_string()],
            default_ttl: "5m".to_string(),
            classify_deadline_secs: 5,
            index_conn_retries: 1,
            render_counting: false,
            telemetry_blocks: TelemetryBlockConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TelemetryBlockConfig {
    pub strip_from_prompt: bool,

    pub prefixes: Vec<String>,
}

impl Default for TelemetryBlockConfig {
    fn default() -> Self {
        Self {
            strip_from_prompt: true,
            prefixes: vec!["x-anthropic-billing-header:".to_string()],
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContinuationConfig {
    pub enabled: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokenizer_url: Option<String>,

    pub origins: ContinuationOriginsConfig,

    pub max_attempts: u32,

    pub resume_deadline_secs: u64,

    pub stall_timeout_secs: u64,

    pub max_buffer_bytes: usize,

    pub priority: i32,

    pub max_inflight_per_model: u32,

    pub model_reconstructors: HashMap<String, String>,
}

impl Default for ContinuationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            tokenizer_url: None,
            origins: ContinuationOriginsConfig::default(),
            max_attempts: 2,
            resume_deadline_secs: 20,
            stall_timeout_secs: 540,
            max_buffer_bytes: 2 * 1024 * 1024,
            priority: 100,
            max_inflight_per_model: 8,
            model_reconstructors: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContinuationOriginsConfig {
    pub realtime: bool,
    pub batch: bool,
    pub playground: bool,
}

impl Default for ContinuationOriginsConfig {
    fn default() -> Self {
        Self {
            realtime: true,
            batch: false,
            playground: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CachePricingConfig {
    pub default_write_multiplier_5m: rust_decimal::Decimal,
    pub default_write_multiplier_1h: rust_decimal::Decimal,
    pub default_write_multiplier_24h: rust_decimal::Decimal,
    pub default_read_multiplier: rust_decimal::Decimal,
    pub default_min_prefix_tokens: i32,
}

impl Default for CachePricingConfig {
    fn default() -> Self {
        use rust_decimal::Decimal;
        Self {
            default_write_multiplier_5m: Decimal::new(125, 2),
            default_write_multiplier_1h: Decimal::new(2, 0),
            default_write_multiplier_24h: Decimal::new(25, 1),
            default_read_multiplier: Decimal::new(1, 1),
            default_min_prefix_tokens: 1024,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileLimitsConfig {
    pub max_file_size: u64,
    pub max_requests_per_file: usize,
    pub max_concurrent_uploads: usize,
    pub max_waiting_uploads: usize,
    pub max_upload_wait_secs: u64,
}

impl Default for FileLimitsConfig {
    fn default() -> Self {
        Self {
            max_file_size: 100 * 1024 * 1024,
            max_requests_per_file: 0,
            max_concurrent_uploads: 0,
            max_waiting_uploads: 20,
            max_upload_wait_secs: 60,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BatchConfig {
    pub enabled: bool,
    pub allowed_completion_windows: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_relaxation_factors")]
    pub window_relaxation_factors: HashMap<String, f32>,
    pub allowed_url_paths: Vec<String>,
    #[serde(default)]
    pub async_requests: AsyncRequestsConfig,
    pub files: FilesConfig,
    #[serde(default = "default_batch_throughput", deserialize_with = "deserialize_positive_throughput")]
    pub default_throughput: f32,
    #[serde(
        default = "default_reservation_ttl_secs",
        deserialize_with = "deserialize_positive_reservation_ttl"
    )]
    pub reservation_ttl_secs: i64,
    pub unverified_requests_per_completion_hour: usize,

    #[serde(default)]
    pub pending_capacity_counts_enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AsyncRequestsConfig {
    pub enabled: bool,
    pub completion_window: String,
}

impl Default for AsyncRequestsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            completion_window: "1h".to_string(),
        }
    }
}

fn default_batch_throughput() -> f32 {
    100.0
}

fn default_reservation_ttl_secs() -> i64 {
    10 * 60
}

fn deserialize_relaxation_factors<'de, D>(deserializer: D) -> Result<HashMap<String, f32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let map: HashMap<String, f32> = HashMap::deserialize(deserializer)?;
    for (window, &factor) in &map {
        if !factor.is_finite() {
            return Err(D::Error::custom(format!(
                "window_relaxation_factors[{}] must be a finite number, got {}",
                window, factor
            )));
        }
        if factor < 0.0 {
            return Err(D::Error::custom(format!(
                "window_relaxation_factors[{}] must be >= 0.0, got {}",
                window, factor
            )));
        }
    }
    Ok(map)
}

impl BatchConfig {
    pub fn relaxation_factor(&self, completion_window: &str) -> f32 {
        self.window_relaxation_factors.get(completion_window).copied().unwrap_or(1.0)
    }
}

fn deserialize_positive_reservation_ttl<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let opt: Option<i64> = Option::deserialize(deserializer)?;

    match opt {
        None => Ok(default_reservation_ttl_secs()),
        Some(value) if value <= 0 => Err(D::Error::custom(format!(
            "reservation_ttl_secs must be positive (> 0), got {}",
            value
        ))),
        Some(value) => Ok(value),
    }
}

fn deserialize_positive_throughput<'de, D>(deserializer: D) -> Result<f32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let opt: Option<f32> = Option::deserialize(deserializer)?;

    match opt {
        None => Ok(default_batch_throughput()),
        Some(value) if value <= 0.0 => Err(D::Error::custom(format!(
            "default_throughput must be positive (> 0), got {}",
            value
        ))),
        Some(value) => Ok(value),
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allowed_completion_windows: vec!["24h".to_string()],
            window_relaxation_factors: HashMap::new(),
            allowed_url_paths: vec![
                "/v1/chat/completions".to_string(),
                "/v1/completions".to_string(),
                "/v1/embeddings".to_string(),
                "/v1/responses".to_string(),
                "/v1/messages".to_string(),
            ],
            async_requests: AsyncRequestsConfig::default(),
            files: FilesConfig::default(),
            default_throughput: default_batch_throughput(),
            reservation_ttl_secs: default_reservation_ttl_secs(),
            unverified_requests_per_completion_hour: 1000,
            pending_capacity_counts_enabled: false,
        }
    }
}

impl BatchConfig {
    pub fn validate(&self) {
        if self.async_requests.enabled && !self.allowed_completion_windows.contains(&self.async_requests.completion_window) {
            tracing::error!(
                async_window = %self.async_requests.completion_window,
                allowed = ?self.allowed_completion_windows,
                "async_requests.completion_window is not in allowed_completion_windows — async requests will fail"
            );
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Compact,
    Json,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonMode {
    #[default]
    Both,
    RequestOnly,
    BatchOnly,
}

#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct SensitiveDatabaseUrl(String);

impl SensitiveDatabaseUrl {
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SensitiveDatabaseUrl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl Serialize for SensitiveDatabaseUrl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str("<redacted>")
    }
}

impl From<DaemonMode> for fusillade::DaemonMode {
    fn from(mode: DaemonMode) -> Self {
        match mode {
            DaemonMode::Both => fusillade::DaemonMode::Both,
            DaemonMode::RequestOnly => fusillade::DaemonMode::RequestOnly,
            DaemonMode::BatchOnly => fusillade::DaemonMode::BatchOnly,
        }
    }
}

fn default_memory_gate_low_fraction() -> f64 {
    0.65
}

fn default_adaptive_growth_factor() -> f64 {
    1.5
}

fn default_adaptive_cut_factor() -> f64 {
    0.8
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DaemonConfig {
    pub enabled: DaemonEnabled,

    #[serde(default)]
    pub mode: DaemonMode,

    pub claim_batch_size: usize,

    pub default_model_concurrency: usize,

    #[serde(default)]
    pub adaptive_concurrency: bool,

    #[serde(default = "default_adaptive_growth_factor")]
    pub adaptive_growth_factor: f64,

    #[serde(default = "default_adaptive_cut_factor")]
    pub adaptive_cut_factor: f64,

    #[serde(default)]
    pub memory_gate_high_fraction: f64,

    #[serde(default = "default_memory_gate_low_fraction")]
    pub memory_gate_low_fraction: f64,

    pub claim_interval_ms: u64,

    pub max_retries: Option<u32>,

    pub stop_before_deadline_ms: Option<i64>,

    pub backoff_ms: u64,

    pub backoff_factor: u64,

    pub max_backoff_ms: u64,

    #[serde(default = "default_additional_retryable_statuses")]
    pub additional_retryable_statuses: Vec<u16>,

    pub timeout_ms: Option<u64>,

    pub first_chunk_timeout_ms: u64,

    pub chunk_timeout_ms: u64,

    pub body_timeout_ms: u64,

    pub upload_stall_timeout_ms: u64,

    pub upload_chunk_bytes: usize,

    pub upload_stall_poll_ms: u64,

    pub status_log_interval_ms: Option<u64>,

    pub claim_timeout_ms: u64,

    pub processing_timeout_ms: u64,

    pub pending_request_counts_timeout_ms: u64,

    pub model_escalations: HashMap<String, fusillade::ModelEscalationConfig>,

    #[serde(default = "default_batch_metadata_fields_dwctl")]
    pub batch_metadata_fields: Vec<String>,

    pub purge_interval_ms: u64,

    pub purge_batch_size: i64,

    pub purge_throttle_ms: u64,

    #[serde(default)]
    pub retention: fusillade::RetentionPolicy,

    #[serde(default)]
    pub batchless_archive_sweep_enabled: bool,

    #[serde(default)]
    pub batchless_archive_backfill_enabled: bool,

    #[serde(default = "default_batchless_archive_groups_per_tick")]
    pub batchless_archive_groups_per_tick: i64,

    #[serde(default = "default_batchless_archive_bytes_per_tick")]
    pub batchless_archive_bytes_per_tick: i64,

    #[serde(default = "default_batchless_archive_backfill_concurrency")]
    pub batchless_archive_backfill_concurrency: usize,

    #[serde(default = "default_retained_response_partitions_days_ahead")]
    pub retained_response_partitions_days_ahead: i32,

    #[serde(default)]
    pub retained_response_retirement_enabled: bool,

    #[serde(default)]
    pub batch_archive_retirement_enabled: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_generation_writes_enabled: Option<bool>,

    #[serde(default)]
    pub template_retirement_enabled: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_retention_days: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_archive_retention_days: Option<u32>,

    #[serde(default, skip_serializing)]
    pub retained_response_partition_maintenance_url: Option<SensitiveDatabaseUrl>,

    #[serde(default)]
    pub streamable_endpoints: Vec<String>,

    #[serde(default = "default_urgency_weight", deserialize_with = "deserialize_urgency_weight")]
    pub urgency_weight: f64,

    #[serde(default)]
    pub inject_deadline_priority: bool,

    #[serde(default)]
    pub background_concurrency_limit: usize,

    #[serde(default)]
    pub batch_claim_size: usize,

    #[serde(default = "default_batch_claim_batch_size")]
    pub batch_claim_batch_size: usize,

    #[serde(default)]
    pub batch_claim_interval_ms: u64,

    #[serde(default)]
    pub batch_claim_require_live: bool,

    #[serde(default = "default_claim_ramp_exponent", deserialize_with = "deserialize_claim_ramp_exponent")]
    pub claim_ramp_exponent: f64,

    #[serde(flatten)]
    pub leak: fusillade::daemon::LeakConfig,

    #[serde(default = "default_claim_loop_max_consecutive_failures")]
    pub claim_loop_max_consecutive_failures: u32,

    #[serde(default = "default_claim_query_timeout_ms")]
    pub claim_query_timeout_ms: u64,

    #[serde(default)]
    pub batch_archive_sweep_enabled: bool,
    #[serde(default = "default_batch_archive_sweep_interval_ms")]
    pub batch_archive_sweep_interval_ms: u64,
    #[serde(default = "default_batch_archive_moves_per_tick")]
    pub batch_archive_sweep_moves_per_tick: i64,
    #[serde(default, deserialize_with = "deserialize_non_negative_secs")]
    pub batch_archive_sweep_dwell_secs: f64,
    #[serde(
        default = "default_batch_archive_cancel_grace_secs",
        deserialize_with = "deserialize_cancel_grace_secs"
    )]
    pub batch_archive_cancel_grace_secs: f64,
    #[serde(default)]
    pub batch_archive_backfill_enabled: bool,
    #[serde(default = "default_batch_archive_backfill_interval_ms")]
    pub batch_archive_backfill_interval_ms: u64,
    #[serde(default = "default_batch_archive_moves_per_tick")]
    pub batch_archive_backfill_moves_per_tick: i64,
    #[serde(default = "default_batch_archive_backfill_concurrency")]
    pub batch_archive_backfill_concurrency: usize,
    #[serde(default = "default_batch_archive_partitions_weeks_ahead")]
    pub batch_archive_partitions_weeks_ahead: i32,
}

fn default_batch_archive_sweep_interval_ms() -> u64 {
    5_000
}

fn default_batchless_archive_groups_per_tick() -> i64 {
    fusillade::RetentionMaintenanceConfig::default().batchless_archive_groups_per_tick()
}

fn default_batchless_archive_bytes_per_tick() -> i64 {
    fusillade::RetentionMaintenanceConfig::default().batchless_archive_bytes_per_tick()
}

fn default_batchless_archive_backfill_concurrency() -> usize {
    fusillade::RetentionMaintenanceConfig::default().batchless_archive_backfill_concurrency()
}

fn default_retained_response_partitions_days_ahead() -> i32 {
    fusillade::RetentionMaintenanceConfig::default().retained_response_partitions_days_ahead()
}

fn default_batch_archive_moves_per_tick() -> i64 {
    4
}

fn default_batch_archive_cancel_grace_secs() -> f64 {
    600.0
}

fn default_batch_archive_backfill_interval_ms() -> u64 {
    1_000
}

fn default_batch_archive_backfill_concurrency() -> usize {
    8
}

fn default_batch_archive_partitions_weeks_ahead() -> i32 {
    4
}

fn default_claim_loop_max_consecutive_failures() -> u32 {
    10
}

fn default_additional_retryable_statuses() -> Vec<u16> {
    vec![499]
}

fn default_claim_query_timeout_ms() -> u64 {
    180_000
}

fn default_batch_claim_batch_size() -> usize {
    4
}

fn default_claim_ramp_exponent() -> f64 {
    0.56
}

fn default_urgency_weight() -> f64 {
    0.5
}

fn deserialize_urgency_weight<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let opt: Option<f64> = Option::deserialize(deserializer)?;

    match opt {
        None => Ok(default_urgency_weight()),
        Some(value) if !value.is_finite() => Err(D::Error::custom(format!("urgency_weight must be a finite number, got {}", value))),
        Some(value) if !(0.0..=1.0).contains(&value) => Err(D::Error::custom(format!(
            "urgency_weight must be between 0.0 and 1.0, got {}",
            value
        ))),
        Some(value) => Ok(value),
    }
}

fn deserialize_claim_ramp_exponent<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let opt: Option<f64> = Option::deserialize(deserializer)?;

    match opt {
        None => Ok(default_claim_ramp_exponent()),
        Some(value) if !value.is_finite() => Err(D::Error::custom(format!(
            "claim_ramp_exponent must be a finite number, got {}",
            value
        ))),
        Some(value) if value < 0.0 => Err(D::Error::custom(format!("claim_ramp_exponent must be non-negative, got {}", value))),
        Some(value) => Ok(value),
    }
}

fn deserialize_non_negative_secs<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let opt: Option<f64> = Option::deserialize(deserializer)?;
    match opt {
        None => Ok(0.0),
        Some(value) if !value.is_finite() => Err(D::Error::custom(format!("seconds value must be a finite number, got {}", value))),
        Some(value) if value < 0.0 => Err(D::Error::custom(format!("seconds value must be non-negative, got {}", value))),
        Some(value) => Ok(value),
    }
}

fn deserialize_cancel_grace_secs<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let opt: Option<f64> = Option::deserialize(deserializer)?;
    match opt {
        None => Ok(default_batch_archive_cancel_grace_secs()),
        Some(value) if !value.is_finite() => Err(D::Error::custom(format!(
            "batch_archive_cancel_grace_secs must be a finite number, got {}",
            value
        ))),
        Some(value) if value < 0.0 => Err(D::Error::custom(format!(
            "batch_archive_cancel_grace_secs must be non-negative, got {}",
            value
        ))),
        Some(value) => Ok(value),
    }
}

fn default_batch_metadata_fields_dwctl() -> Vec<String> {
    vec![
        "id".to_string(),
        "endpoint".to_string(),
        "created_at".to_string(),
        "completion_window".to_string(),
        "request_source".to_string(),
        "dw_user_agent".to_string(),
    ]
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            enabled: DaemonEnabled::Leader,
            mode: DaemonMode::Both,
            claim_batch_size: 100,
            default_model_concurrency: 10,
            adaptive_concurrency: false,
            adaptive_growth_factor: default_adaptive_growth_factor(),
            adaptive_cut_factor: default_adaptive_cut_factor(),
            memory_gate_high_fraction: 0.0,
            memory_gate_low_fraction: default_memory_gate_low_fraction(),
            claim_interval_ms: 1000,
            max_retries: Some(1000),
            stop_before_deadline_ms: Some(900_000),
            backoff_ms: 1000,
            backoff_factor: 2,
            max_backoff_ms: 10000,
            additional_retryable_statuses: default_additional_retryable_statuses(),
            timeout_ms: None,
            first_chunk_timeout_ms: 86_400_000,
            chunk_timeout_ms: 86_400_000,
            body_timeout_ms: 86_400_000,
            upload_stall_timeout_ms: 60_000,
            upload_chunk_bytes: 64 * 1024,
            upload_stall_poll_ms: 100,
            status_log_interval_ms: Some(2000),
            claim_timeout_ms: 60000,
            processing_timeout_ms: 600000,
            pending_request_counts_timeout_ms: 60000,
            batch_metadata_fields: default_batch_metadata_fields_dwctl(),
            model_escalations: HashMap::new(),
            purge_interval_ms: 600_000,
            purge_batch_size: 1000,
            purge_throttle_ms: 100,
            retention: fusillade::RetentionPolicy::default(),
            batchless_archive_sweep_enabled: false,
            batchless_archive_backfill_enabled: false,
            batchless_archive_groups_per_tick: default_batchless_archive_groups_per_tick(),
            batchless_archive_bytes_per_tick: default_batchless_archive_bytes_per_tick(),
            batchless_archive_backfill_concurrency: default_batchless_archive_backfill_concurrency(),
            retained_response_partitions_days_ahead: default_retained_response_partitions_days_ahead(),
            retained_response_retirement_enabled: false,
            batch_archive_retirement_enabled: false,
            batch_archive_retention_days: None,
            template_generation_writes_enabled: None,
            template_retirement_enabled: false,
            template_retention_days: None,
            retained_response_partition_maintenance_url: None,
            streamable_endpoints: Vec::new(),
            urgency_weight: default_urgency_weight(),
            inject_deadline_priority: false,
            background_concurrency_limit: 0,
            batch_claim_size: 0,
            batch_claim_batch_size: default_batch_claim_batch_size(),
            batch_claim_interval_ms: 0,
            batch_claim_require_live: false,
            claim_ramp_exponent: default_claim_ramp_exponent(),
            leak: fusillade::daemon::LeakConfig::default(),
            claim_loop_max_consecutive_failures: default_claim_loop_max_consecutive_failures(),
            claim_query_timeout_ms: default_claim_query_timeout_ms(),
            batch_archive_sweep_enabled: false,
            batch_archive_sweep_interval_ms: default_batch_archive_sweep_interval_ms(),
            batch_archive_sweep_moves_per_tick: default_batch_archive_moves_per_tick(),
            batch_archive_sweep_dwell_secs: 0.0,
            batch_archive_cancel_grace_secs: default_batch_archive_cancel_grace_secs(),
            batch_archive_backfill_enabled: false,
            batch_archive_backfill_interval_ms: default_batch_archive_backfill_interval_ms(),
            batch_archive_backfill_moves_per_tick: default_batch_archive_moves_per_tick(),
            batch_archive_backfill_concurrency: default_batch_archive_backfill_concurrency(),
            batch_archive_partitions_weeks_ahead: default_batch_archive_partitions_weeks_ahead(),
        }
    }
}

pub fn content_retention_days(max_expiry_seconds: i64) -> u32 {
    const SECONDS_PER_DAY: i64 = 24 * 60 * 60;
    let days = max_expiry_seconds.max(1).saturating_add(SECONDS_PER_DAY - 1) / SECONDS_PER_DAY;
    u32::try_from(days.min(i64::from(i32::MAX))).unwrap_or(i32::MAX as u32)
}

impl DaemonConfig {
    pub fn to_fusillade_retention_maintenance_config(&self, max_expiry_seconds: i64) -> fusillade::RetentionMaintenanceConfig {
        fusillade::RetentionMaintenanceConfig::new(self.retention.clone())
            .with_batchless_archive_sweep_enabled(self.batchless_archive_sweep_enabled)
            .with_batchless_archive_backfill_enabled(self.batchless_archive_backfill_enabled)
            .with_batchless_archive_limits(self.batchless_archive_groups_per_tick, self.batchless_archive_bytes_per_tick)
            .with_batchless_archive_backfill_concurrency(self.batchless_archive_backfill_concurrency)
            .with_retained_response_partitions_days_ahead(self.retained_response_partitions_days_ahead)
            .with_retained_response_retirement_enabled(self.retained_response_retirement_enabled)
            .with_batch_archive_retirement_enabled(self.batch_archive_retirement_enabled)
            .with_template_retirement_enabled(self.template_retirement_enabled)
            .with_content_retention_days(Some(content_retention_days(max_expiry_seconds)))
    }

    pub fn to_fusillade_config(&self) -> fusillade::daemon::DaemonConfig {
        self.to_fusillade_config_with_limits(None)
    }

    pub fn to_fusillade_config_with_limits(
        &self,
        model_capacity_limits: Option<std::sync::Arc<dashmap::DashMap<String, usize>>>,
    ) -> fusillade::daemon::DaemonConfig {
        let (first_chunk_timeout_ms, chunk_timeout_ms, body_timeout_ms) = if let Some(timeout) = self.timeout_ms {
            if self.first_chunk_timeout_ms == 86_400_000 && self.chunk_timeout_ms == 86_400_000 && self.body_timeout_ms == 86_400_000 {
                tracing::warn!(
                    timeout_ms = timeout,
                    "batch_daemon.timeout_ms is deprecated; \
                         use first_chunk_timeout_ms, chunk_timeout_ms, and body_timeout_ms instead"
                );
                (timeout * 9 / 10, 86_400_000, timeout / 10)
            } else {
                (self.first_chunk_timeout_ms, self.chunk_timeout_ms, self.body_timeout_ms)
            }
        } else {
            (self.first_chunk_timeout_ms, self.chunk_timeout_ms, self.body_timeout_ms)
        };

        fusillade::daemon::DaemonConfig {
            mode: self.mode.into(),
            claim_batch_size: self.claim_batch_size,
            model_concurrency_limits: model_capacity_limits.unwrap_or_else(|| std::sync::Arc::new(dashmap::DashMap::new())),
            adaptive_concurrency: self.adaptive_concurrency,
            adaptive_growth_factor: self.adaptive_growth_factor,
            adaptive_cut_factor: self.adaptive_cut_factor,
            memory_gate_high_fraction: self.memory_gate_high_fraction,
            memory_gate_low_fraction: self.memory_gate_low_fraction,
            model_escalations: Arc::new(DashMap::from_iter(self.model_escalations.clone())),
            claim_interval_ms: self.claim_interval_ms,
            max_retries: self.max_retries,
            stop_before_deadline_ms: self.stop_before_deadline_ms,
            backoff_ms: self.backoff_ms,
            backoff_factor: self.backoff_factor,
            max_backoff_ms: self.max_backoff_ms,
            additional_retryable_statuses: self.additional_retryable_statuses.clone(),
            first_chunk_timeout_ms,
            chunk_timeout_ms,
            body_timeout_ms,
            upload_stall_timeout_ms: self.upload_stall_timeout_ms,
            upload_chunk_bytes: self.upload_chunk_bytes,
            upload_stall_poll_ms: self.upload_stall_poll_ms,
            status_log_interval_ms: self.status_log_interval_ms,
            claim_timeout_ms: self.claim_timeout_ms,
            processing_timeout_ms: self.processing_timeout_ms,
            pending_request_counts_timeout_ms: self.pending_request_counts_timeout_ms,
            batch_metadata_fields: self.batch_metadata_fields.clone(),
            purge_interval_ms: self.purge_interval_ms,
            purge_batch_size: self.purge_batch_size,
            purge_throttle_ms: self.purge_throttle_ms,
            urgency_weight: self.urgency_weight,
            inject_deadline_priority: self.inject_deadline_priority,
            background_concurrency_limit: self.background_concurrency_limit,
            batch_claim_size: self.batch_claim_size,
            batch_claim_batch_size: self.batch_claim_batch_size,
            batch_claim_interval_ms: self.batch_claim_interval_ms,
            batch_claim_require_live: self.batch_claim_require_live,
            claim_ramp_exponent: self.claim_ramp_exponent,
            claim_loop_max_consecutive_failures: self.claim_loop_max_consecutive_failures,
            claim_query_timeout_ms: self.claim_query_timeout_ms,
            batch_archive_sweep_enabled: self.batch_archive_sweep_enabled,
            batch_archive_sweep_interval_ms: self.batch_archive_sweep_interval_ms,
            batch_archive_sweep_moves_per_tick: self.batch_archive_sweep_moves_per_tick,
            batch_archive_sweep_dwell_secs: self.batch_archive_sweep_dwell_secs,
            batch_archive_cancel_grace_secs: self.batch_archive_cancel_grace_secs,
            batch_archive_backfill_enabled: self.batch_archive_backfill_enabled,
            batch_archive_backfill_interval_ms: self.batch_archive_backfill_interval_ms,
            batch_archive_backfill_moves_per_tick: self.batch_archive_backfill_moves_per_tick,
            batch_archive_backfill_concurrency: self.batch_archive_backfill_concurrency,
            batch_archive_partitions_weeks_ahead: self.batch_archive_partitions_weeks_ahead,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DaemonEnabled {
    Always,
    Never,
    Leader,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LeaderElectionConfig {
    pub enabled: bool,
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NotificationsConfig {
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,
    pub webhooks: WebhookConfig,
}

impl Default for NotificationsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval: Duration::from_secs(30),
            webhooks: WebhookConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct BackgroundServicesConfig {
    pub onwards_sync: OnwardsSyncConfig,
    pub key_policy_sync: KeyPolicySyncConfig,
    pub usage_refresh: UsageRefreshConfig,
    pub probe_scheduler: ProbeSchedulerConfig,
    pub batch_daemon: DaemonConfig,
    pub leader_election: LeaderElectionConfig,
    pub pool_metrics: PoolMetricsSamplerConfig,
    pub batch_sla_metrics: crate::metrics::batch_sla::BatchSlaMetricsConfig,
    pub allocator_metrics: crate::metrics::allocator::AllocatorMetricsConfig,
    pub notifications: NotificationsConfig,
    pub sync_workers: SyncWorkersConfig,
    pub task_workers: TaskWorkersConfig,
    pub task_retention: TaskRetentionConfig,
    pub prompt_cache_retention: PromptCacheRetentionConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TaskRetentionConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub batch_size: u32,
    pub batch_pause_milliseconds: u64,
    pub min_age_days: u64,
}

impl Default for TaskRetentionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: 300,
            batch_size: 1000,
            batch_pause_milliseconds: 2000,
            min_age_days: 14,
        }
    }
}

impl TaskRetentionConfig {
    pub const MAX_MIN_AGE_DAYS: u64 = 36_500;

    pub fn min_age(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.min_age_days.min(Self::MAX_MIN_AGE_DAYS) * 86_400)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.batch_size == 0 {
            return Err("batch_size must be at least 1".to_string());
        }
        if self.min_age_days > Self::MAX_MIN_AGE_DAYS {
            return Err(format!("min_age_days must be at most {}", Self::MAX_MIN_AGE_DAYS));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PromptCacheRetentionConfig {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub batch_size: u32,
    pub batch_pause_milliseconds: u64,
    pub grace_days: u64,
}

impl Default for PromptCacheRetentionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_seconds: 300,
            batch_size: 1000,
            batch_pause_milliseconds: 2000,
            grace_days: crate::recompute::report::CACHE_GRACE_DAYS as u64,
        }
    }
}

impl PromptCacheRetentionConfig {
    pub const MAX_GRACE_DAYS: u64 = 36_500;

    pub fn grace(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.grace_days.min(Self::MAX_GRACE_DAYS) * 86_400)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.batch_size == 0 {
            return Err("batch_size must be at least 1".to_string());
        }
        let minimum = crate::recompute::report::CACHE_GRACE_DAYS as u64;
        if self.grace_days < minimum {
            return Err(format!(
                "grace_days must be at least {minimum}: usage recompute re-derives cache splits from that much history"
            ));
        }
        if self.grace_days > Self::MAX_GRACE_DAYS {
            return Err(format!("grace_days must be at most {}", Self::MAX_GRACE_DAYS));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolMetricsSamplerConfig {
    #[serde(with = "humantime_serde")]
    pub sample_interval: Duration,
}

impl Default for PoolMetricsSamplerConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OnwardsSyncConfig {
    pub enabled: bool,
    pub fallback_interval_milliseconds: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct KeyPolicySyncConfig {
    pub enabled: bool,
    pub fallback_interval_milliseconds: u64,
}

impl Default for KeyPolicySyncConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            fallback_interval_milliseconds: 300_000,
        }
    }
}

impl Default for OnwardsSyncConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            fallback_interval_milliseconds: 300_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct UsageRefreshConfig {
    pub enabled: bool,
    pub fallback_interval_milliseconds: u64,
    pub min_interval_milliseconds: u64,
}

impl Default for UsageRefreshConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            fallback_interval_milliseconds: 60_000,
            min_interval_milliseconds: 30_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProbeSchedulerConfig {
    pub enabled: bool,
}

impl Default for ProbeSchedulerConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WebhookConfig {
    pub enabled: bool,
    pub timeout_secs: u64,
    pub retry_schedule_secs: Vec<i64>,
    pub circuit_breaker_threshold: i32,
    pub claim_batch_size: i64,
    pub max_concurrent_sends: usize,
    pub channel_capacity: usize,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout_secs: 30,
            retry_schedule_secs: vec![0, 5, 300, 1800, 7200, 28800, 86400],
            circuit_breaker_threshold: 10,
            claim_batch_size: 50,
            max_concurrent_sends: 20,
            channel_capacity: 200,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum CorsOrigin {
    #[serde(deserialize_with = "parse_wildcard")]
    Wildcard,
    #[serde(deserialize_with = "parse_url")]
    Url(Url),
}

fn parse_wildcard<'de, D>(deserializer: D) -> Result<(), D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    if s == "*" {
        Ok(())
    } else {
        Err(serde::de::Error::custom("Expected '*'"))
    }
}

fn parse_url<'de, D>(deserializer: D) -> Result<Url, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    Url::parse(&s).map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CreditsConfig {
    pub initial_credits_for_standard_users: rust_decimal::Decimal,
    #[serde(default)]
    pub first_payment_match_up_to: rust_decimal::Decimal,
    #[serde(default)]
    pub verification_credits: rust_decimal::Decimal,
    #[serde(with = "humantime_serde")]
    pub auto_topup_retry_delay: Duration,
}

impl Default for CreditsConfig {
    fn default() -> Self {
        Self {
            initial_credits_for_standard_users: rust_decimal::Decimal::ZERO,
            first_payment_match_up_to: rust_decimal::Decimal::ZERO,
            verification_credits: rust_decimal::Decimal::ZERO,
            auto_topup_retry_delay: Duration::from_secs(24 * 60 * 60),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AnalyticsConfig {
    pub batch_size: usize,
    pub max_retries: u32,
    pub retry_base_delay_ms: u64,
    pub balance_notification_interval_milliseconds: u64,
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            max_retries: 3,
            retry_base_delay_ms: 100,
            balance_notification_interval_milliseconds: 5000,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConnectionsConfig {
    pub encryption_key: Option<String>,
    pub sync: SyncPipelineConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncPipelineConfig {
    pub default_completion_window: String,
    pub default_endpoint: String,
}

impl Default for SyncPipelineConfig {
    fn default() -> Self {
        Self {
            default_completion_window: "24h".to_string(),
            default_endpoint: "/v1/chat/completions".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncWorkersConfig {
    pub enabled: bool,
    pub ingest_workers: usize,
    pub activate_workers: usize,
    #[serde(alias = "sync_workers")]
    pub discovery_workers: usize,
}

impl Default for SyncWorkersConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ingest_workers: 4,
            activate_workers: 1,
            discovery_workers: 1,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TaskWorkersConfig {
    pub create_batch_workers: usize,
    pub cascade_batch_state_workers: usize,
    pub purge_user_data_workers: usize,
    pub response_writer_batch_size: usize,
}

impl Default for TaskWorkersConfig {
    fn default() -> Self {
        Self {
            create_batch_workers: 1,
            cascade_batch_state_workers: 1,
            purge_user_data_workers: 1,
            response_writer_batch_size: 100,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 3001,
            dashboard_url: "http://localhost:5173".to_string(),
            database_url: None,
            database_replica_url: None,
            database_pooled_url: None,
            database: DatabaseConfig::default(),
            slow_statement_threshold_ms: 1000,
            admin_email: "test@doubleword.ai".to_string(),
            admin_password: Some("hunter2".to_string()),
            secret_key: None,
            model_sources: vec![],
            model_provisioning: ModelProvisioningConfig::default(),
            migrations: MigrationsConfig::default(),
            metadata: Metadata::default(),
            payment: None,
            auth: AuthConfig::default(),
            batches: BatchConfig::default(),
            background_services: BackgroundServicesConfig::default(),
            enable_metrics: true,
            enable_request_logging: true,
            enable_analytics: true,
            analytics: AnalyticsConfig::default(),
            enable_otel_export: false,
            log_format: LogFormat::default(),
            credits: CreditsConfig::default(),
            sample_files: SampleFilesConfig::default(),
            limits: LimitsConfig::default(),
            email: EmailConfig::default(),
            onwards: OnwardsConfig::default(),
            onboarding_url: None,
            support_email: "support@doubleword.ai".to_string(),
            connections: ConnectionsConfig::default(),
            image_normalizer: crate::image_normalizer::ImageNormalizerConfig::default(),
            keystore: None,
            openapi: OpenApiConfig::default(),
            discovery: DiscoveryConfig::default(),
            cache: CacheConfig::default(),
            clickhouse: None,
            prefix_chain: crate::prefix_chain::PrefixChainConfig::default(),
            reference_cache: crate::reference_cache::ReferenceCacheConfig::default(),
            continuation: ContinuationConfig::default(),
            heap_profiling: crate::profiling::HeapProfilingConfig::default(),
            scheduling: SchedulingConfig::default(),
        }
    }
}

impl Default for ModelSource {
    fn default() -> Self {
        Self {
            name: String::new(),
            url: Url::parse("http://localhost:8080").unwrap(),
            api_key: None,
            sync_interval: Duration::from_secs(10),
            default_models: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MigrationsMode {
    #[default]
    Run,
    Check,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct MigrationsConfig {
    pub mode: MigrationsMode,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModelProvisioningConfig {
    pub enabled: bool,
    pub directory: PathBuf,
    pub org_overlays_directory: PathBuf,
    pub account_limits_directory: PathBuf,
}

impl Default for ModelProvisioningConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: PathBuf::from("/app/model-provisioning.d"),
            org_overlays_directory: PathBuf::from("/app/org-overlays.d"),
            account_limits_directory: PathBuf::from("/app/account-limits.d"),
        }
    }
}

impl Default for NativeAuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allow_registration: false,
            password: PasswordConfig::default(),
            session: SessionConfig::default(),
            password_reset_token_duration: Duration::from_secs(30 * 60),
        }
    }
}

impl Default for ProxyHeaderAuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            header_name: "x-doubleword-user".to_string(),
            email_header_name: "x-doubleword-email".to_string(),
            groups_field_name: "x-doubleword-user-groups".to_string(),
            provider_field_name: "x-doubleword-sso-provider".to_string(),
            auto_create_users: true,
            blacklisted_sso_groups: Vec::new(),
            import_idp_groups: false,
        }
    }
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(24 * 60 * 60),
            cookie_name: "dwctl_session".to_string(),
            cookie_secure: true,
            cookie_same_site: "strict".to_string(),
            cookie_domain: None,
        }
    }
}

impl Default for PasswordConfig {
    fn default() -> Self {
        Self {
            min_length: 8,
            max_length: 64,
            argon2_memory_kib: 19456,
            argon2_iterations: 2,
            argon2_parallelism: 1,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            jwt_expiry: Duration::from_secs(24 * 60 * 60),
            cors: CorsConfig::default(),
            headers: SecurityHeadersConfig::default(),
        }
    }
}

impl Default for SecurityHeadersConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            frame_options: "DENY".to_string(),
            referrer_policy: "strict-origin-when-cross-origin".to_string(),
            permissions_policy:
                "accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()".to_string(),
            content_security_policy: String::new(),
            content_security_policy_report_only: String::new(),
            strict_transport_security: String::new(),
        }
    }
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            allowed_origins: vec![
                CorsOrigin::Url(Url::parse("http://localhost:3001").unwrap()),
            ],
            allow_credentials: true,
            max_age: Some(3600),
            exposed_headers: vec!["location".to_string()],
            allow_any_origin_without_credentials: false,
        }
    }
}

impl Default for EmailConfig {
    fn default() -> Self {
        Self {
            transport: EmailTransportConfig::default(),
            from_email: "noreply@example.com".to_string(),
            from_name: "Control Layer".to_string(),
            reply_to: None,
            templates_dir: None,
        }
    }
}

impl Default for EmailTransportConfig {
    fn default() -> Self {
        Self::File {
            path: "./emails".to_string(),
        }
    }
}

impl ModelSource {
    fn default_sync_interval() -> Duration {
        Duration::from_secs(10)
    }
}

impl Config {
    #[allow(clippy::result_large_err)]
    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self, figment::Error> {
        Self::load(&Args {
            config: path.as_ref().to_path_buf(),
            validate: false,
        })
    }

    #[allow(clippy::result_large_err)]
    pub fn load(args: &Args) -> Result<Self, figment::Error> {
        let mut config: Self = Self::figment(args).extract()?;

        if let Some(url) = config.database_url.take() {
            let pool = config.database.main_pool_settings().clone();
            let fusillade = config.database.fusillade().clone();
            let outlet = config.database.outlet().clone();
            let underway_pool = config.database.underway_pool_settings().clone();
            let direct_pool = config.database.direct_pool_settings().clone();
            let pooled_url = config.database.external_pooled_url().map(str::to_string);

            let original_replica_pool = match &config.database {
                DatabaseConfig::External { replica_pool, .. } => replica_pool.clone(),
                DatabaseConfig::Embedded { replica_pool, .. } => replica_pool.clone(),
            };

            let replica_url = config.database_replica_url.take();

            config.database = DatabaseConfig::External {
                url,
                replica_url,
                pooled_url,
                pool,
                replica_pool: original_replica_pool,
                fusillade,
                outlet,
                underway_pool,
                direct_pool,
            };
        } else if let Some(replica_url) = config.database_replica_url.take() {
            match &mut config.database {
                DatabaseConfig::External {
                    replica_url: current_replica,
                    ..
                } => {
                    *current_replica = Some(replica_url);
                }
                DatabaseConfig::Embedded { .. } => {
                }
            }
        }

        if let Some(url) = config.database_pooled_url.take() {
            match &mut config.database {
                DatabaseConfig::External { pooled_url, .. } => *pooled_url = Some(url),
                DatabaseConfig::Embedded { .. } => {
                    return Err(figment::Error::from("a pooled endpoint requires an external database"));
                }
            }
        }

        if config.auth.native.session.cookie_domain.as_deref() == Some("") {
            config.auth.native.session.cookie_domain = None;
        }

        config.validate().map_err(|e| figment::Error::from(e.to_string()))?;
        Ok(config)
    }

    pub fn database_url(&self) -> Option<&str> {
        self.database.external_url()
    }

    pub fn validate(&self) -> Result<(), Error> {
        self.background_services
            .batch_sla_metrics
            .validate()
            .map_err(|operation| Error::Internal { operation })?;
        for (name, component) in [("fusillade", self.database.fusillade()), ("outlet", self.database.outlet())] {
            component
                .validate_schema_pooling(name, self.database.external_pooled_url().is_some())
                .map_err(|operation| Error::Internal {
                    operation: format!("Config validation: {operation}"),
                })?;
        }
        if let Err(error) = self.background_services.batch_daemon.retention.validate() {
            return Err(Error::Internal {
                operation: format!("Config validation: batch retention is invalid: {error}"),
            });
        }
        if let Err(error) = self.background_services.task_retention.validate() {
            return Err(Error::Internal {
                operation: format!("Config validation: task retention is invalid: {error}"),
            });
        }
        if let Err(error) = self.background_services.prompt_cache_retention.validate() {
            return Err(Error::Internal {
                operation: format!("Config validation: prompt-cache retention is invalid: {error}"),
            });
        }
        if let Some(name) = self
            .onwards
            .rejected_params
            .iter()
            .find(|name| onwards::unsupported_params::known_param(name).is_none())
        {
            return Err(Error::Internal {
                operation: format!("Config validation: onwards.rejected_params names an unknown parameter: {name}"),
            });
        }
        if let Some(model) = self
            .onwards
            .model_allowed_params
            .keys()
            .find(|model| model.contains(crate::inference::serving_name::CLASS_SEPARATOR))
        {
            return Err(Error::Internal {
                operation: format!(
                    "Config validation: onwards.model_allowed_params.{model} names a serving class; key the exceptions by the model alias, which covers its classes"
                ),
            });
        }
        if let Some((model, name)) = self.onwards.model_allowed_params.iter().find_map(|(model, params)| {
            params
                .iter()
                .find(|name| onwards::unsupported_params::known_param(name).is_none())
                .map(|name| (model, name))
        }) {
            return Err(Error::Internal {
                operation: format!("Config validation: onwards.model_allowed_params.{model} names an unknown parameter: {name}"),
            });
        }
        if let Some(PaymentConfig::Stripe(stripe)) = &self.payment
            && let Err(error) = stripe.api_base()
        {
            return Err(Error::Internal {
                operation: format!("Config validation: {error}"),
            });
        }
        if let Err(error) = self.scheduling.validate() {
            return Err(Error::Internal {
                operation: format!("Config validation: {error}"),
            });
        }
        if self.background_services.batch_daemon.retention.expire_files
            || self.background_services.batch_daemon.retention.terminal_batch_seconds.is_some()
        {
            return Err(Error::Internal {
                operation: "Config validation: scheduled file and batch retention is not supported by retained-response maintenance"
                    .to_string(),
            });
        }
        let retention_enabled = self.background_services.batch_daemon.retention.is_enabled();
        let daemon = &self.background_services.batch_daemon;
        let owns_archive_maintenance = !matches!(daemon.mode, DaemonMode::RequestOnly) && daemon.enabled != DaemonEnabled::Never;
        let batchless_policy_configured = !daemon.retention.batchless_seconds_by_service_tier.is_empty();
        let batchless_movement_enabled = daemon.batchless_archive_sweep_enabled || daemon.batchless_archive_backfill_enabled;
        if owns_archive_maintenance
            && (batchless_movement_enabled || daemon.retained_response_retirement_enabled)
            && !batchless_policy_configured
        {
            return Err(Error::Internal {
                operation: "Config validation: batchless retention policy is required for retained-response maintenance".to_string(),
            });
        }
        if owns_archive_maintenance
            && (daemon.retained_response_retirement_enabled
                || daemon.batch_archive_retirement_enabled
                || daemon.template_retirement_enabled)
            && matches!(self.database.fusillade(), ComponentDb::Dedicated { .. })
            && daemon.retained_response_partition_maintenance_url.is_none()
        {
            return Err(Error::Internal {
                operation: "Config validation: partition retirement on a dedicated database requires an explicit direct session endpoint using the same role as fusillade query traffic".to_string(),
            });
        }
        if owns_archive_maintenance
            && batchless_movement_enabled
            && (daemon.batchless_archive_groups_per_tick <= 0 || daemon.batchless_archive_bytes_per_tick <= 0)
        {
            return Err(Error::Internal {
                operation: "Config validation: batchless archive group and byte budgets must be positive".to_string(),
            });
        }
        if owns_archive_maintenance && daemon.batchless_archive_backfill_enabled {
            let fusillade_pool_max = match self.database.fusillade() {
                ComponentDb::Schema { pool, .. } | ComponentDb::Dedicated { pool, .. } => pool.max_connections,
            };
            if daemon.batchless_archive_backfill_concurrency as u64 >= u64::from(fusillade_pool_max) {
                return Err(Error::Internal {
                    operation: format!(
                        "Config validation: batchless archive backfill concurrency ({}) must be below the fusillade pool max_connections ({})",
                        daemon.batchless_archive_backfill_concurrency, fusillade_pool_max
                    ),
                });
            }
        }
        if owns_archive_maintenance && daemon.batchless_archive_backfill_enabled && daemon.batchless_archive_backfill_concurrency == 0 {
            return Err(Error::Internal {
                operation: "Config validation: batchless archive backfill concurrency must be at least 1 when the backfill is enabled"
                    .to_string(),
            });
        }
        if owns_archive_maintenance && batchless_policy_configured && daemon.retained_response_partitions_days_ahead <= 0 {
            return Err(Error::Internal {
                operation: "Config validation: retained-response partition runway must be positive".to_string(),
            });
        }
        if owns_archive_maintenance
            && batchless_movement_enabled
            && daemon
                .retention
                .batchless_seconds_by_service_tier
                .values()
                .any(|seconds| daemon.batch_archive_sweep_dwell_secs >= *seconds as f64)
        {
            return Err(Error::Internal {
                operation: "Config validation: batchless archive sweep dwell must be shorter than every configured retention period"
                    .to_string(),
            });
        }
        if owns_archive_maintenance
            && batchless_movement_enabled
            && daemon
                .retention
                .batchless_seconds_by_service_tier
                .values()
                .any(|seconds| daemon.batch_archive_cancel_grace_secs >= *seconds as f64)
        {
            return Err(Error::Internal {
                operation: "Config validation: batchless archive cancellation grace must be shorter than every configured retention period"
                    .to_string(),
            });
        }
        if retention_enabled && daemon.enabled == DaemonEnabled::Never && !matches!(daemon.mode, DaemonMode::RequestOnly) {
            tracing::warn!(
                "automated retention is configured but the batch daemon never runs on this instance; \
                 another instance must run it for retention to be enforced"
            );
        }
        if owns_archive_maintenance
            && retention_enabled
            && (self.background_services.batch_daemon.purge_interval_ms == 0 || self.background_services.batch_daemon.purge_batch_size < 1)
        {
            return Err(Error::Internal {
                operation: "Config validation: automated retention requires an enabled orphan purge and a positive purge batch size"
                    .to_string(),
            });
        }
        if self.auth.native.enabled {
            if self.secret_key.is_none() {
                return Err(Error::Internal {
                    operation: "Config validation: Native authentication is enabled but secret_key is not configured. \
                     Please set DWCTL_SECRET_KEY environment variable or add secret_key to config file."
                        .to_string(),
                });
            }

            if self.auth.native.password.min_length > self.auth.native.password.max_length {
                return Err(Error::Internal {
                    operation: format!(
                        "Config validation: Invalid password configuration: min_length ({}) cannot be greater than max_length ({})",
                        self.auth.native.password.min_length, self.auth.native.password.max_length
                    ),
                });
            }

            if self.auth.native.password.min_length < 1 {
                return Err(Error::Internal {
                    operation: "Config validation: Invalid password configuration: min_length must be at least 1".to_string(),
                });
            }
        }

        if let Err(e) = self.prefix_chain.validate(self.clickhouse.as_ref(), &self.cache.tokenizer_url) {
            return Err(Error::Internal {
                operation: format!("Config validation: {e}"),
            });
        }
        if let Err(e) = self.reference_cache.validate(self.enable_analytics) {
            return Err(Error::Internal {
                operation: format!("Config validation: {e}"),
            });
        }
        if let Some(ch) = &self.clickhouse
            && let Err(e) = ch.validate()
        {
            return Err(Error::Internal {
                operation: format!("Config validation: {e}"),
            });
        }
        if self.cache.enabled && self.cache.tokenizer_url.trim().is_empty() {
            return Err(Error::Internal {
                operation: "Config validation: cache.enabled is true but cache.tokenizer_url is empty. \
                     Set DWCTL_CACHE__TOKENIZER_URL to the tokenizer-svc base URL, or disable caching."
                    .to_string(),
            });
        }

        if self.cache.enabled || self.prefix_chain.enabled {
            for ttl in &self.cache.enabled_ttls {
                if crate::prompt_cache::TtlTier::parse(ttl).is_none() {
                    return Err(Error::Internal {
                        operation: format!(
                            "Config validation: cache.enabled_ttls contains an unknown tier {ttl:?}; allowed values are \"5m\", \"1h\", \"24h\"."
                        ),
                    });
                }
            }
            if self.cache.enabled_ttls.is_empty() {
                return Err(Error::Internal {
                    operation: "Config validation: cache.enabled_ttls is empty; enable at least one tier (\"5m\", \"1h\", \"24h\")."
                        .to_string(),
                });
            }
            if !self.cache.enabled_ttls.iter().any(|t| t == &self.cache.default_ttl) {
                return Err(Error::Internal {
                    operation: format!(
                        "Config validation: cache.default_ttl {:?} is not in cache.enabled_ttls {:?}.",
                        self.cache.default_ttl, self.cache.enabled_ttls
                    ),
                });
            }
        }

        if self.auth.security.jwt_expiry.as_secs() < 300 {
            return Err(Error::Internal {
                operation: "Config validation: JWT expiry duration is too short (minimum 5 minutes)".to_string(),
            });
        }

        if self.auth.security.jwt_expiry.as_secs() > 86400 * 30 {
            return Err(Error::Internal {
                operation: "Config validation: JWT expiry duration is too long (maximum 30 days)".to_string(),
            });
        }

        if !self.auth.native.enabled && !self.auth.proxy_header.enabled {
            return Err(Error::Internal {
                operation:
                    "Config validation: No authentication methods are enabled. Please enable either native or proxy_header authentication."
                        .to_string(),
            });
        }

        if let Some(ref domain) = self.auth.native.session.cookie_domain {
            let invalid = domain.is_empty() || domain.chars().any(|c| c.is_whitespace() || c.is_control()) || domain.contains(';');
            if invalid {
                return Err(Error::Internal {
                    operation: format!(
                        "Config validation: Invalid cookie_domain '{domain}'. \
                         Must not be empty or contain semicolons, whitespace, or control characters."
                    ),
                });
            }
            let fragment = format!("; Domain={domain}");
            if axum::http::HeaderValue::from_str(&fragment).is_err() {
                return Err(Error::Internal {
                    operation: format!("Config validation: cookie_domain '{domain}' produces an invalid HTTP header value."),
                });
            }
        }

        if self.auth.security.cors.allowed_origins.is_empty() {
            return Err(Error::Internal {
                operation: "Config validation: CORS allowed_origins cannot be empty. Add at least one allowed origin.".to_string(),
            });
        }

        let has_wildcard = self
            .auth
            .security
            .cors
            .allowed_origins
            .iter()
            .any(|origin| matches!(origin, CorsOrigin::Wildcard));
        if has_wildcard && self.auth.security.cors.allow_credentials {
            return Err(Error::Internal {
                operation: "Config validation: CORS cannot use wildcard origin '*' with allow_credentials=true. Specify explicit origins."
                    .to_string(),
            });
        }

        if self.background_services.batch_daemon.upload_chunk_bytes == 0 {
            return Err(Error::Internal {
                operation: "Config validation: upload_chunk_bytes cannot be 0. Set a positive integer value (default: 65536).".to_string(),
            });
        }

        if self.background_services.batch_daemon.upload_stall_poll_ms == 0 {
            return Err(Error::Internal {
                operation: "Config validation: upload_stall_poll_ms cannot be 0. Set a positive integer value (default: 100).".to_string(),
            });
        }

        let batch_daemon = &self.background_services.batch_daemon;
        if batch_daemon.background_concurrency_limit > 0 && !batch_daemon.inject_deadline_priority {
            return Err(Error::Internal {
                operation: "Config validation: background_concurrency_limit requires inject_deadline_priority=true so background work is always lower priority than SLA work."
                    .to_string(),
            });
        }

        let daemon_can_run = self.background_services.batch_daemon.enabled != DaemonEnabled::Never;
        let validate_request_manager_config = self.batches.enabled || daemon_can_run;

        if validate_request_manager_config {
            if self.batches.files.batch_insert_size == 0 {
                return Err(Error::Internal {
                    operation: "Config validation: batch_insert_size cannot be 0. Set a positive integer value (recommended: 1000-10000). \
                               This setting is used by the request manager when batches are enabled or the daemon runs."
                        .to_string(),
                });
            }

            if self.batches.files.download_buffer_size == 0 {
                return Err(Error::Internal {
                    operation: "Config validation: download_buffer_size cannot be 0. Set a positive integer value (default: 100). \
                               This setting is used by the request manager when batches are enabled or the daemon runs."
                        .to_string(),
                });
            }
        }

        if self.batches.enabled {
            let unknown_windows: Vec<&str> = self
                .batches
                .window_relaxation_factors
                .keys()
                .filter(|w| !self.batches.allowed_completion_windows.contains(w))
                .map(|w| w.as_str())
                .collect();

            if !unknown_windows.is_empty() {
                return Err(Error::Internal {
                    operation: format!(
                        "Config validation: window_relaxation_factors contains window(s) not in \
                        allowed_completion_windows: {}. Add them to allowed_completion_windows or \
                        remove them from window_relaxation_factors.",
                        unknown_windows.join(", ")
                    ),
                });
            }

            if self.batches.allowed_url_paths.is_empty() {
                return Err(Error::Internal {
                    operation: "Config validation: batches.allowed_url_paths cannot be empty. Add at least one supported URL path."
                        .to_string(),
                });
            }

            if self.batches.files.upload_buffer_size == 0 {
                return Err(Error::Internal {
                    operation: "Config validation: upload_buffer_size cannot be 0. Set a positive integer value (default: 100)."
                        .to_string(),
                });
            }

        }

        if self.batches.files.min_expiry_seconds <= 0 {
            return Err(Error::Internal {
                operation: "Config validation: min_expiry_seconds must be positive (default: 3600 = 1 hour).".to_string(),
            });
        }

        if self.batches.files.default_expiry_seconds <= 0 {
            return Err(Error::Internal {
                operation: "Config validation: default_expiry_seconds must be positive (default: 2592000 = 30 days).".to_string(),
            });
        }

        if self.batches.files.max_expiry_seconds <= 0 {
            return Err(Error::Internal {
                operation: "Config validation: max_expiry_seconds must be positive (default: 2592000 = 30 days).".to_string(),
            });
        }

        if self.batches.files.min_expiry_seconds > self.batches.files.default_expiry_seconds {
            return Err(Error::Internal {
                operation: format!(
                    "Config validation: min_expiry_seconds ({}) cannot be greater than default_expiry_seconds ({})",
                    self.batches.files.min_expiry_seconds, self.batches.files.default_expiry_seconds
                ),
            });
        }

        if self.batches.files.default_expiry_seconds > self.batches.files.max_expiry_seconds {
            return Err(Error::Internal {
                operation: format!(
                    "Config validation: default_expiry_seconds ({}) cannot be greater than max_expiry_seconds ({})",
                    self.batches.files.default_expiry_seconds, self.batches.files.max_expiry_seconds
                ),
            });
        }

        if self.batches.files.min_expiry_seconds > self.batches.files.max_expiry_seconds {
            return Err(Error::Internal {
                operation: format!(
                    "Config validation: min_expiry_seconds ({}) cannot be greater than max_expiry_seconds ({})",
                    self.batches.files.min_expiry_seconds, self.batches.files.max_expiry_seconds
                ),
            });
        }

        Ok(())
    }

    pub fn figment(args: &Args) -> Figment {
        let config_path: OsString = args.config.as_os_str().to_owned();
        Figment::new()
            .merge(Yaml::file(config_path))
            .merge(Env::prefixed("DWCTL_").split("__"))
            .merge(Env::raw().only(&["DATABASE_URL", "DATABASE_REPLICA_URL", "DATABASE_POOLED_URL"]))
            .merge(
                Env::raw()
                    .only(&["DWCTL_DATABASE_REPLICA_URL"])
                    .map(|_| "database_replica_url".into()),
            )
            .merge(
                Env::raw()
                    .only(&["DWCTL_DATABASE_POOLED_URL"])
                    .map(|_| "database_pooled_url".into()),
            )
    }

    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn configured_personal_domains_extend_the_builtin_list() {
        let mut auth = super::AuthConfig::default();
        assert!(auth.is_personal_email_domain("gmail.com"), "built-in list still applies");
        assert!(!auth.is_personal_email_domain("example-isp.net"));

        auth.personal_email_domains = vec!["example-isp.net".to_string()];
        assert!(auth.is_personal_email_domain("example-isp.net"), "configured domain is personal");
        assert!(
            auth.is_personal_email_domain("EXAMPLE-ISP.NET"),
            "and matches case-insensitively, like the built-in check"
        );
        assert!(auth.is_personal_email_domain("gmail.com"), "without displacing the built-in list");
        assert!(!auth.is_personal_email_domain("acme.com"), "and without catching company domains");
    }

    use super::*;
    use figment::Jail;

    #[test]
    fn auto_topup_retry_delay_defaults_to_24_hours() {
        assert_eq!(CreditsConfig::default().auto_topup_retry_delay, Duration::from_secs(24 * 60 * 60));
    }

    #[test]
    fn default_auth_roles_include_background_inference() {
        assert!(AuthConfig::default().default_user_roles.contains(&Role::BackgroundInferenceUser));
    }

    #[test]
    fn batch_inflight_enforcement_defaults_off_and_parses_on() {
        assert!(!LimitsConfig::default().batch_inflight.enforce);

        let parsed: LimitsConfig = serde_json::from_value(serde_json::json!({
            "batch_inflight": { "enforce": true }
        }))
        .unwrap();
        assert!(parsed.batch_inflight.enforce);
    }

    #[test]
    fn batch_inflight_default_capacity_defaults_to_200() {
        assert_eq!(LimitsConfig::default().batch_inflight.default_capacity, Some(200));
    }

    #[test]
    fn batch_inflight_default_capacity_parses_an_explicit_positive_value() {
        let parsed: LimitsConfig = serde_json::from_value(serde_json::json!({
            "batch_inflight": { "default_capacity": 500 }
        }))
        .unwrap();
        assert_eq!(parsed.batch_inflight.default_capacity, Some(500));
    }

    #[test]
    fn batch_inflight_default_capacity_accepts_the_off_forms() {
        for off in [serde_json::json!(null), serde_json::json!(0)] {
            let parsed: LimitsConfig = serde_json::from_value(serde_json::json!({
                "batch_inflight": { "default_capacity": off }
            }))
            .unwrap();
            assert_eq!(parsed.batch_inflight.default_capacity, None, "{off} should mean no default");
        }
    }

    #[test]
    fn batch_inflight_default_capacity_rejects_negative_values() {
        let error = serde_json::from_value::<LimitsConfig>(serde_json::json!({
            "batch_inflight": { "default_capacity": -1 }
        }))
        .expect_err("a negative default capacity must be rejected");
        assert!(error.to_string().contains("must not be negative"), "unexpected error: {error}");
    }

    #[test]
    fn batch_inflight_default_capacity_rejects_out_of_range_values() {
        let error = serde_json::from_value::<LimitsConfig>(serde_json::json!({
            "batch_inflight": { "default_capacity": 5_000_000_000i64 }
        }))
        .expect_err("a default capacity beyond u32 must be rejected");
        assert!(error.to_string().contains("too large"), "unexpected error: {error}");
    }

    #[test]
    fn obsolete_retention_sweep_interval_is_rejected_on_a_complete_config() {
        let mut serialized = serde_json::to_value(DaemonConfig::default()).unwrap();
        serialized["retention_sweep_interval_ms"] = serde_json::json!(1_000);

        let error = serde_json::from_value::<DaemonConfig>(serialized).expect_err("the removed standalone scheduler key must be rejected");

        assert!(error.to_string().contains("retention_sweep_interval_ms"));
    }

    #[test]
    fn retention_maintenance_controls_default_off_and_map_explicitly() {
        let mut serialized = serde_json::to_value(DaemonConfig::default()).unwrap();
        for key in [
            "batchless_archive_sweep_enabled",
            "batchless_archive_backfill_enabled",
            "batchless_archive_groups_per_tick",
            "batchless_archive_bytes_per_tick",
            "batchless_archive_backfill_concurrency",
            "retained_response_partitions_days_ahead",
            "retained_response_retirement_enabled",
            "batch_archive_retirement_enabled",
            "batch_archive_retention_days",
            "template_retirement_enabled",
            "template_retention_days",
        ] {
            serialized.as_object_mut().unwrap().remove(key);
        }

        let daemon: DaemonConfig = serde_json::from_value(serialized).unwrap();
        assert!(!daemon.batchless_archive_sweep_enabled);
        assert!(!daemon.batchless_archive_backfill_enabled);
        assert!(!daemon.retained_response_retirement_enabled);
        assert!(daemon.retained_response_partition_maintenance_url.is_none());
        assert!(daemon.batchless_archive_groups_per_tick > 0);
        assert!(daemon.batchless_archive_bytes_per_tick > 0);
        assert_eq!(daemon.batchless_archive_backfill_concurrency, 1);
        assert!(daemon.retained_response_partitions_days_ahead > 0);

        let mapped = daemon.to_fusillade_retention_maintenance_config(30 * 24 * 60 * 60 + 1);
        assert_eq!(mapped.policy(), &daemon.retention);
        assert_eq!(mapped.batchless_archive_sweep_enabled(), daemon.batchless_archive_sweep_enabled);
        assert_eq!(
            mapped.batchless_archive_backfill_enabled(),
            daemon.batchless_archive_backfill_enabled
        );
        assert_eq!(mapped.batchless_archive_groups_per_tick(), daemon.batchless_archive_groups_per_tick);
        assert_eq!(mapped.batchless_archive_bytes_per_tick(), daemon.batchless_archive_bytes_per_tick);
        assert_eq!(
            mapped.batchless_archive_backfill_concurrency(),
            daemon.batchless_archive_backfill_concurrency
        );
        assert_eq!(
            mapped.retained_response_partitions_days_ahead(),
            daemon.retained_response_partitions_days_ahead
        );
        assert_eq!(
            mapped.retained_response_retirement_enabled(),
            daemon.retained_response_retirement_enabled
        );
        assert_eq!(mapped.batch_archive_retirement_enabled(), daemon.batch_archive_retirement_enabled);
        assert_eq!(mapped.template_retirement_enabled(), daemon.template_retirement_enabled);
        assert_eq!(
            mapped.content_retention_days(),
            Some(31),
            "retention rounds max expiry up to whole days"
        );
    }

    #[test]
    fn retention_rejects_unknown_batchless_tiers() {
        let mut config = Config::default();
        config
            .background_services
            .batch_daemon
            .retention
            .batchless_seconds_by_service_tier
            .insert("unknown".to_string(), 60);
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("unsupported service tier"));
    }

    fn stripe_config_with_base(api_base_url: Option<&str>) -> StripeConfig {
        StripeConfig {
            api_key: "sk_test_fake".to_string(),
            webhook_secret: "whsec_test".to_string(),
            price_id: "price_ExampleCredits".to_string(),
            enable_invoice_creation: false,
            auto_topup_terms_of_service_text: None,
            setup_terms_of_service_text: None,
            tax_code: None,
            api_base_url: api_base_url.map(str::to_string),
        }
    }

    #[test]
    fn stripe_api_base_is_normalised_to_a_trailing_slash() {
        for (configured, expected) in [
            (None, None),
            (Some("http://localhost:12111"), Some("http://localhost:12111/")),
            (Some("http://localhost:12111/"), Some("http://localhost:12111/")),
            (
                Some("https://egress.example.test/stripe"),
                Some("https://egress.example.test/stripe/"),
            ),
            (
                Some("https://egress.example.test/stripe/"),
                Some("https://egress.example.test/stripe/"),
            ),
        ] {
            assert_eq!(
                stripe_config_with_base(configured).api_base().unwrap().as_deref(),
                expected,
                "{configured:?}"
            );
        }
    }

    #[test]
    fn stripe_api_base_rejects_unusable_urls() {
        for configured in [
            "localhost:12111",
            "not a url",
            "ftp://files.example.test/",
            "https://egress.example.test/stripe?region=a",
            "https://egress.example.test/stripe#frag",
        ] {
            assert!(
                stripe_config_with_base(Some(configured)).api_base().is_err(),
                "{configured:?} should be rejected"
            );
        }
    }

    #[test]
    fn config_validation_rejects_a_bad_stripe_api_base() {
        let mut config = Config::default();
        config.secret_key = Some("test-secret-key".to_string());
        config.payment = Some(PaymentConfig::Stripe(stripe_config_with_base(Some(
            "https://egress.example.test/stripe",
        ))));
        config.validate().unwrap();

        config.payment = Some(PaymentConfig::Stripe(stripe_config_with_base(Some("not a url"))));
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("api_base_url"), "got {error}");
    }

    #[test]
    fn rejected_params_must_be_known() {
        let mut config = Config::default();
        config.secret_key = Some("test-secret-key".to_string());
        config.onwards.rejected_params = vec!["logprobs".to_string()];
        config.validate().unwrap();

        config.onwards.rejected_params.push("top_k".to_string());
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("onwards.rejected_params names an unknown parameter: top_k"));
    }

    #[test]
    fn model_allowed_params_must_be_known() {
        let mut config = Config::default();
        config.secret_key = Some("test-secret-key".to_string());
        config
            .onwards
            .model_allowed_params
            .insert("example/model".to_string(), vec!["logprobs".to_string()]);
        config.validate().unwrap();

        config
            .onwards
            .model_allowed_params
            .insert("example/model".to_string(), vec!["logprob".to_string()]);
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("onwards.model_allowed_params.example/model names an unknown parameter: logprob"));
    }

    #[test]
    fn model_allowed_params_are_keyed_by_model_not_class() {
        let mut config = Config::default();
        config.secret_key = Some("test-secret-key".to_string());
        config
            .onwards
            .model_allowed_params
            .insert("example/model:fast".to_string(), vec!["logprobs".to_string()]);
        let error = config.validate().unwrap_err().to_string();
        assert!(
            error.contains("onwards.model_allowed_params.example/model:fast names a serving class"),
            "{error}"
        );
    }

    #[test]
    fn scheduled_file_and_batch_retention_fail_closed() {
        let mut config = Config::default();
        config.background_services.batch_daemon.retention.expire_files = true;
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("file and batch retention is not supported"));

        let mut config = Config::default();
        config.background_services.batch_daemon.retention.terminal_batch_seconds = Some(60);
        let error = config.validate().unwrap_err().to_string();
        assert!(error.contains("file and batch retention is not supported"));
    }

    fn configure_batchless_retention(config: &mut Config) {
        config.secret_key = Some("test-secret-key".to_string());
        let daemon = &mut config.background_services.batch_daemon;
        daemon.retention.batchless_seconds_by_service_tier.insert("flex".to_string(), 120);
        daemon.retention.max_late_writer_seconds = Some(30);
    }

    #[test]
    fn batchless_retention_validation_matrix_is_fail_closed() {
        let mut config = Config::default();
        config.background_services.batch_daemon.batchless_archive_sweep_enabled = true;
        assert!(config.validate().unwrap_err().to_string().contains("batchless retention policy"));

        let mut config = Config::default();
        configure_batchless_retention(&mut config);
        assert!(
            config.validate().is_ok(),
            "a configured policy must permit reader-safe runway preflight"
        );

        config.background_services.batch_daemon.retained_response_partitions_days_ahead = 0;
        assert!(config.validate().unwrap_err().to_string().contains("partition runway"));

        let mut config = Config::default();
        configure_batchless_retention(&mut config);
        let daemon = &mut config.background_services.batch_daemon;
        daemon.batchless_archive_backfill_enabled = true;
        daemon.batchless_archive_groups_per_tick = 0;
        assert!(config.validate().unwrap_err().to_string().contains("group and byte budgets"));

        let mut config = Config::default();
        configure_batchless_retention(&mut config);
        let daemon = &mut config.background_services.batch_daemon;
        daemon.batchless_archive_backfill_concurrency = 0;
        assert!(config.validate().is_ok(), "a disabled backfill ignores its concurrency");
        let daemon = &mut config.background_services.batch_daemon;
        daemon.batchless_archive_backfill_enabled = true;
        daemon.batch_archive_cancel_grace_secs = 60.0;
        assert!(config.validate().unwrap_err().to_string().contains("backfill concurrency"));
        config.background_services.batch_daemon.batchless_archive_backfill_concurrency = 16;
        assert!(config.validate().is_ok());
        let pool_max = match config.database.fusillade() {
            ComponentDb::Schema { pool, .. } | ComponentDb::Dedicated { pool, .. } => pool.max_connections,
        };
        config.background_services.batch_daemon.batchless_archive_backfill_concurrency = pool_max as usize;
        assert!(config.validate().unwrap_err().to_string().contains("below the fusillade pool"));
        config.background_services.batch_daemon.batchless_archive_backfill_concurrency = pool_max as usize - 1;
        assert!(config.validate().is_ok());

        let mut config = Config::default();
        config.background_services.batch_daemon.retained_response_retirement_enabled = true;
        assert!(config.validate().unwrap_err().to_string().contains("batchless retention policy"));
    }

    #[test]
    fn retained_response_retirement_accepts_a_valid_batchless_policy_in_schema_mode() {
        let mut config = Config::default();
        configure_batchless_retention(&mut config);
        config.background_services.batch_daemon.retained_response_retirement_enabled = true;

        assert!(config.validate().is_ok());
    }

    #[test]
    fn maintenance_endpoint_is_redacted_and_never_serialized() {
        let mut daemon = DaemonConfig::default();
        daemon.retained_response_partition_maintenance_url = Some(SensitiveDatabaseUrl(
            "postgres://secret-user:secret-password@db.invalid/fusillade".to_string(),
        ));
        let debug = format!("{daemon:?}");
        assert!(!debug.contains("secret-user"));
        assert!(!debug.contains("secret-password"));
        let serialized = serde_json::to_value(&daemon).unwrap();
        assert!(serialized.get("retained_response_partition_maintenance_url").is_none());

        let mut input = serde_json::to_value(DaemonConfig::default()).unwrap();
        input["retained_response_partition_maintenance_url"] = serde_json::json!("postgres://configured.invalid/fusillade");
        let parsed: DaemonConfig = serde_json::from_value(input).unwrap();
        assert_eq!(
            parsed
                .retained_response_partition_maintenance_url
                .as_ref()
                .map(SensitiveDatabaseUrl::expose),
            Some("postgres://configured.invalid/fusillade")
        );
    }

    #[test]
    fn request_only_ignores_shared_retirement_selection_configuration() {
        let mut config = Config::default();
        config.secret_key = Some("test-secret-key".to_string());
        config.background_services.batch_daemon.mode = DaemonMode::RequestOnly;
        config.background_services.batch_daemon.retained_response_retirement_enabled = true;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn batchless_movement_windows_precede_retention() {
        let mut config = Config::default();
        configure_batchless_retention(&mut config);
        let daemon = &mut config.background_services.batch_daemon;
        daemon.batchless_archive_sweep_enabled = true;
        daemon.batch_archive_sweep_dwell_secs = 120.0;
        assert!(config.validate().unwrap_err().to_string().contains("sweep dwell"));

        let mut backfill = Config::default();
        configure_batchless_retention(&mut backfill);
        let daemon = &mut backfill.background_services.batch_daemon;
        daemon.batchless_archive_backfill_enabled = true;
        daemon.batch_archive_sweep_dwell_secs = 120.0;
        assert!(backfill.validate().unwrap_err().to_string().contains("sweep dwell"));

        let daemon = &mut config.background_services.batch_daemon;
        daemon.batch_archive_sweep_dwell_secs = 119.0;
        daemon.batch_archive_cancel_grace_secs = 120.0;
        assert!(config.validate().unwrap_err().to_string().contains("cancellation grace"));

        config.background_services.batch_daemon.batch_archive_cancel_grace_secs = 119.0;
        assert!(config.validate().is_ok());
    }
    #[test]
    fn forwarded_batch_metadata_covers_every_column_analytics_fills_from_it() {
        let forwarded = DaemonConfig::default().batch_metadata_fields;
        for (key, column) in [
            ("id", "fusillade_batch_id"),
            ("completion_window", "batch_sla"),
            ("created_at", "batch_created_at (batch-creation pricing)"),
            ("request_source", "batch_request_source"),
            ("dw_user_agent", "user_agent"),
        ] {
            assert!(
                forwarded.contains(&key.to_string()),
                "batch metadata key '{key}' is not forwarded, so http_analytics.{column} will never be populated for batch requests"
            );
        }
    }

    #[test]
    fn test_model_sources_config() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: hello
model_sources:
  - name: openai
    url: https://api.openai.com
    api_key: sk-test
    sync_interval: 30s
  - name: internal
    url: http://internal:8080
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;

            assert_eq!(config.model_sources.len(), 2);

            let openai = &config.model_sources[0];
            assert_eq!(openai.name, "openai");
            assert_eq!(openai.url.as_str(), "https://api.openai.com/");
            assert_eq!(openai.api_key.as_deref(), Some("sk-test"));
            assert_eq!(openai.sync_interval, Duration::from_secs(30));

            let internal = &config.model_sources[1];
            assert_eq!(internal.name, "internal");
            assert_eq!(internal.sync_interval, Duration::from_secs(10));

            Ok(())
        });
    }

    #[test]
    fn cors_allow_any_origin_without_credentials_parses() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: hello
auth:
  security:
    cors:
      allowed_origins:
        - "https://app.doubleword.ai"
      allow_credentials: true
      allow_any_origin_without_credentials: true
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;

            assert!(config.auth.security.cors.allow_any_origin_without_credentials);
            assert!(config.auth.security.cors.allow_credentials);

            Ok(())
        });
    }

    #[test]
    fn cors_allow_any_origin_without_credentials_defaults_false() {
        Jail::expect_with(|jail| {
            jail.create_file("test.yaml", "secret_key: hello\n")?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;

            assert!(!config.auth.security.cors.allow_any_origin_without_credentials);

            Ok(())
        });
    }

    #[test]
    fn pooled_url_and_direct_pool_parse_and_fold_from_env() {
        Jail::expect_with(|jail| {
            jail.clear_env();
            jail.create_file(
                "test.yaml",
                r#"
secret_key: hello
database:
  type: external
  url: postgres://direct/db
  pooled_url: postgres://pooled/db
  pool:
    max_connections: 400
  direct_pool:
    max_connections: 6
  fusillade:
    mode: schema
    name: fusillade
    direct_pool:
      max_connections: 3
"#,
            )?;
            let config = Config::load(&Args {
                config: "test.yaml".into(),
                validate: false,
            })?;
            assert_eq!(config.database.external_url(), Some("postgres://direct/db"));
            assert_eq!(config.database.external_pooled_url(), Some("postgres://pooled/db"));
            assert_eq!(config.database.main_pool_settings().max_connections, 400);
            assert_eq!(config.database.direct_pool_settings().max_connections, 6);
            assert_eq!(config.database.fusillade().direct_pool_settings().max_connections, 3);
            assert_eq!(
                config.database.outlet().direct_pool_settings().max_connections,
                default_direct_pool().max_connections
            );

            jail.set_env("DATABASE_URL", "postgres://env-direct/db");
            jail.set_env("DWCTL_DATABASE_POOLED_URL", "postgres://env-pooled/db");
            let config = Config::load(&Args {
                config: "test.yaml".into(),
                validate: false,
            })?;
            assert_eq!(config.database.external_url(), Some("postgres://env-direct/db"));
            assert_eq!(config.database.external_pooled_url(), Some("postgres://env-pooled/db"));
            assert_eq!(config.database.direct_pool_settings().max_connections, 6);
            assert!(config.database_pooled_url.is_none(), "folded into database.pooled_url");
            Ok(())
        });
    }

    #[test]
    fn pooled_schema_retirement_reuses_direct_session_endpoint() {
        for component_only in [false, true] {
            let mut config = Config::default();
            configure_batchless_retention(&mut config);
            config.database = serde_json::from_value(serde_json::json!({
                "type": "external", "url": "postgres://main@localhost/db",
                "pooled_url": if component_only { None } else { Some("postgres://main@pooler/db") },
                "fusillade": { "mode": "schema", "name": "fusillade",
                    "pooled_url": "postgres://main@pooler/db" }
            }))
            .unwrap();
            config.background_services.batch_daemon.retained_response_retirement_enabled = true;
            config.validate().unwrap();
        }
    }

    #[test]
    fn pooled_url_override_without_direct_url_override() {
        for alias in ["DATABASE_POOLED_URL", "DWCTL_DATABASE_POOLED_URL"] {
            for nested_direct in [false, true] {
                Jail::expect_with(|jail| {
                    jail.clear_env();
                    jail.create_file("test.yaml", "secret_key: hello\ndatabase:\n  type: external\n  url: postgres://direct/db\n  pooled_url: postgres://old-pool/db\n")?;
                    if nested_direct {
                        jail.set_env("DWCTL_DATABASE__URL", "postgres://nested-direct/db");
                    }
                    jail.set_env(alias, "postgres://pooled/db");
                    let config = Config::load_from_path("test.yaml")?;
                    assert_eq!(
                        config.database.external_url(),
                        Some(if nested_direct {
                            "postgres://nested-direct/db"
                        } else {
                            "postgres://direct/db"
                        })
                    );
                    assert_eq!(config.database.external_pooled_url(), Some("postgres://pooled/db"));
                    assert!(config.database_pooled_url.is_none());
                    Ok(())
                });
            }
        }
    }

    #[test]
    fn no_pooled_url_by_default() {
        let config = Config::default();
        assert!(config.database.external_pooled_url().is_none());
        assert_eq!(config.database.direct_pool_settings().max_connections, 8);
    }

    #[test]
    fn test_env_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: hello
metadata:
  region: US East
  organization: Test Corp
"#,
            )?;

            jail.set_env("DWCTL_HOST", "127.0.0.1");
            jail.set_env("DWCTL_PORT", "8080");

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;

            assert_eq!(config.host, "127.0.0.1");
            assert_eq!(config.port, 8080);

            assert_eq!(config.metadata.region, Some("US East".to_string()));
            assert_eq!(config.metadata.organization, Some("Test Corp".to_string()));

            Ok(())
        });
    }

    #[test]
    fn test_auth_config_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key-for-testing"
auth:
  native:
    enabled: true
    allow_registration: false
    password:
      min_length: 12
  proxy_header:
    enabled: false
    header_name: "x-custom-user"
  security:
    jwt_expiry: "2h"
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;

            assert!(config.auth.native.enabled);
            assert!(!config.auth.native.allow_registration);
            assert_eq!(config.auth.native.password.min_length, 12);
            assert_eq!(config.auth.native.password.max_length, 64);

            assert!(!config.auth.proxy_header.enabled);
            assert_eq!(config.auth.proxy_header.header_name, "x-custom-user");

            assert_eq!(config.auth.security.jwt_expiry, Duration::from_secs(2 * 60 * 60));

            Ok(())
        });
    }

    #[test]
    fn test_onwards_sse_buffer_limit_config() {
        let defaults: OnwardsConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(defaults.sse_buffer_limit, 65536);
        let configured: OnwardsConfig = serde_json::from_str(r#"{"sse_buffer_limit":1048576}"#).unwrap();
        assert_eq!(configured.sse_buffer_limit, 1048576);
    }

    #[test]
    fn test_config_validation_native_auth_missing_secret() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = None;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("secret_key is not configured"));
    }

    #[test]
    fn test_config_validation_invalid_password_length() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-key".to_string());
        config.auth.native.password.min_length = 10;
        config.auth.native.password.max_length = 5;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("min_length"));
    }

    #[test]
    fn test_config_validation_no_auth_methods_enabled() {
        let mut config = Config::default();
        config.auth.native.enabled = false;
        config.auth.proxy_header.enabled = false;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No authentication methods"));
    }

    #[test]
    fn test_config_validation_valid_config() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());

        let result = config.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_batch_insert_size_default() {
        let config = Config::default();
        assert_eq!(config.batches.files.batch_insert_size, 5000);
    }

    #[test]
    fn test_batch_insert_size_yaml_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  files:
    batch_insert_size: 10000
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.batches.files.batch_insert_size, 10000);

            Ok(())
        });
    }

    #[test]
    fn test_batch_insert_size_env_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
"#,
            )?;

            jail.set_env("DWCTL_BATCHES__FILES__BATCH_INSERT_SIZE", "7500");

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.batches.files.batch_insert_size, 7500);

            Ok(())
        });
    }

    #[test]
    fn test_batch_insert_size_zero_validation() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = true;
        config.batches.files.batch_insert_size = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("batch_insert_size cannot be 0"));
    }

    #[test]
    fn test_upload_buffer_size_zero_validation() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = true;
        config.batches.files.upload_buffer_size = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("upload_buffer_size cannot be 0"));
    }

    #[test]
    fn test_download_buffer_size_zero_validation() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = true;
        config.batches.files.download_buffer_size = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("download_buffer_size cannot be 0"));
    }

    #[test]
    fn test_expiry_times_positive_validation() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = true;

        config.batches.files.min_expiry_seconds = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("min_expiry_seconds must be positive"));

        config.batches.files.min_expiry_seconds = 3600;
        config.batches.files.default_expiry_seconds = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("default_expiry_seconds must be positive"));

        config.batches.files.default_expiry_seconds = 86400;
        config.batches.files.max_expiry_seconds = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max_expiry_seconds must be positive"));
    }

    #[test]
    fn test_expiry_times_order_validation() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = true;

        config.batches.files.min_expiry_seconds = 86400;
        config.batches.files.default_expiry_seconds = 3600;
        config.batches.files.max_expiry_seconds = 2592000;
        let result = config.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("min_expiry_seconds") && err_msg.contains("default_expiry_seconds"));

        config.batches.files.min_expiry_seconds = 3600;
        config.batches.files.default_expiry_seconds = 2592000;
        config.batches.files.max_expiry_seconds = 86400;
        let result = config.validate();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("default_expiry_seconds") && err_msg.contains("max_expiry_seconds"));

        config.batches.files.min_expiry_seconds = 2592000;
        config.batches.files.default_expiry_seconds = 86400;
        config.batches.files.max_expiry_seconds = 3600;
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn content_retention_rounds_max_expiry_up_to_whole_days() {
        let day = 24 * 60 * 60;
        assert_eq!(content_retention_days(1), 1);
        assert_eq!(content_retention_days(day), 1);
        assert_eq!(content_retention_days(day + 1), 2);
        assert_eq!(content_retention_days(30 * day), 30);
        assert_eq!(content_retention_days(i64::MAX), i32::MAX as u32);
    }

    #[test]
    fn retention_config_is_accepted_on_an_instance_that_never_runs_the_daemon() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.background_services.batch_daemon.enabled = DaemonEnabled::Never;
        let daemon = &mut config.background_services.batch_daemon;
        daemon.retention.batchless_seconds_by_service_tier.insert("flex".to_string(), 60);
        daemon.retention.max_late_writer_seconds = Some(60);
        daemon.batchless_archive_sweep_enabled = true;
        daemon.retained_response_retirement_enabled = true;
        daemon.batch_archive_retirement_enabled = true;
        daemon.batch_archive_retention_days = Some(1);
        daemon.template_generation_writes_enabled = Some(true);
        daemon.template_retirement_enabled = true;
        daemon.template_retention_days = Some(1);
        assert!(config.validate().is_ok(), "{:?}", config.validate().err());
    }

    #[test]
    fn test_batch_validation_skipped_when_disabled() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = false;
        config.background_services.batch_daemon.enabled = DaemonEnabled::Never;
        config.batches.files.batch_insert_size = 0;

        let result = config.validate();
        assert!(result.is_ok());
    }

    #[test]
    fn expiry_validated_when_batches_and_daemon_are_disabled() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = false;
        config.background_services.batch_daemon.enabled = DaemonEnabled::Never;
        assert!(config.validate().is_ok(), "{:?}", config.validate().err());

        config.batches.files.default_expiry_seconds = config.batches.files.max_expiry_seconds + 1;
        let err_msg = config.validate().unwrap_err().to_string();
        assert!(
            err_msg.contains("default_expiry_seconds") && err_msg.contains("max_expiry_seconds"),
            "{err_msg}"
        );

        config.batches.files.default_expiry_seconds = 0;
        let err_msg = config.validate().unwrap_err().to_string();
        assert!(err_msg.contains("default_expiry_seconds must be positive"), "{err_msg}");
    }

    fn cache_test_config() -> Config {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.cache.enabled = true;
        config
    }

    #[test]
    fn test_cache_tiers_not_validated_when_disabled() {
        let mut config = cache_test_config();
        config.cache.enabled = false;
        config.cache.enabled_ttls = vec!["99h".to_string()];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_cache_valid_tiers_ok() {
        let mut config = cache_test_config();
        config.cache.enabled_ttls = vec!["5m".to_string(), "1h".to_string()];
        config.cache.default_ttl = "5m".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_cache_unknown_tier_rejected() {
        let mut config = cache_test_config();
        config.cache.enabled_ttls = vec!["5m".to_string(), "99h".to_string()];
        config.cache.default_ttl = "5m".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("unknown tier"), "{err}");
    }

    #[test]
    fn test_cache_empty_tiers_rejected() {
        let mut config = cache_test_config();
        config.cache.enabled_ttls = vec![];
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("enabled_ttls is empty"), "{err}");
    }

    #[test]
    fn test_cache_default_ttl_must_be_enabled() {
        let mut config = cache_test_config();
        config.cache.enabled_ttls = vec!["5m".to_string()];
        config.cache.default_ttl = "1h".to_string();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("default_ttl"), "{err}");
    }

    #[test]
    fn test_batch_insert_size_validated_when_daemon_enabled() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = false;
        config.background_services.batch_daemon.enabled = DaemonEnabled::Leader;
        config.batches.files.batch_insert_size = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("batch_insert_size cannot be 0"));
    }

    #[test]
    fn test_download_buffer_validated_when_daemon_enabled() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = false;
        config.background_services.batch_daemon.enabled = DaemonEnabled::Always;
        config.batches.files.download_buffer_size = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("download_buffer_size cannot be 0"));
    }

    #[test]
    fn test_batch_insert_size_validated_when_batches_enabled_daemon_never() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = true;
        config.background_services.batch_daemon.enabled = DaemonEnabled::Never;
        config.batches.files.batch_insert_size = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("batch_insert_size cannot be 0"));
    }

    #[test]
    fn test_download_buffer_validated_when_batches_enabled_daemon_never() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.batches.enabled = true;
        config.background_services.batch_daemon.enabled = DaemonEnabled::Never;
        config.batches.files.download_buffer_size = 0;

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("download_buffer_size cannot be 0"));
    }

    #[test]
    fn test_default_throughput_default_value() {
        let config = Config::default();
        assert_eq!(config.batches.default_throughput, 100.0);
    }

    #[test]
    fn test_default_throughput_yaml_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  default_throughput: 100.0
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.batches.default_throughput, 100.0);

            Ok(())
        });
    }

    #[test]
    fn test_default_throughput_null_uses_default() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  default_throughput: null
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.batches.default_throughput, 100.0);

            Ok(())
        });
    }

    #[test]
    fn test_default_throughput_missing_uses_default() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  enabled: true
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.batches.default_throughput, 100.0);

            Ok(())
        });
    }

    #[test]
    fn test_default_throughput_zero_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  default_throughput: 0
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let result = Config::load(&args);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("default_throughput must be positive"));

            Ok(())
        });
    }

    #[test]
    fn test_default_throughput_negative_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  default_throughput: -10.0
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let result = Config::load(&args);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("default_throughput must be positive"));

            Ok(())
        });
    }

    #[test]
    fn test_default_throughput_env_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
"#,
            )?;

            jail.set_env("DWCTL_BATCHES__DEFAULT_THROUGHPUT", "75.5");

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.batches.default_throughput, 75.5);

            Ok(())
        });
    }

    #[test]
    fn test_batch_inflight_enforce_env_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            assert!(!Config::load(&args)?.limits.batch_inflight.enforce, "default is off");

            jail.set_env("DWCTL_LIMITS__BATCH_INFLIGHT__ENFORCE", "true");
            let config = Config::load(&args)?;
            assert!(config.limits.batch_inflight.enforce);

            Ok(())
        });
    }

    #[test]
    fn test_batch_inflight_default_capacity_env_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            assert_eq!(
                Config::load(&args)?.limits.batch_inflight.default_capacity,
                Some(200),
                "default is 200"
            );

            jail.set_env("DWCTL_LIMITS__BATCH_INFLIGHT__DEFAULT_CAPACITY", "250");
            assert_eq!(Config::load(&args)?.limits.batch_inflight.default_capacity, Some(250));

            jail.set_env("DWCTL_LIMITS__BATCH_INFLIGHT__DEFAULT_CAPACITY", "0");
            assert_eq!(Config::load(&args)?.limits.batch_inflight.default_capacity, None);

            jail.set_env("DWCTL_LIMITS__BATCH_INFLIGHT__DEFAULT_CAPACITY", "-1");
            assert!(Config::load(&args).is_err(), "a negative env override must be rejected");

            Ok(())
        });
    }

    #[test]
    fn test_reservation_ttl_zero_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  reservation_ttl_secs: 0
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let result = Config::load(&args);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("reservation_ttl_secs must be positive"));

            Ok(())
        });
    }

    #[test]
    fn test_reservation_ttl_negative_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  reservation_ttl_secs: -60
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let result = Config::load(&args);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("reservation_ttl_secs must be positive"));

            Ok(())
        });
    }

    #[test]
    fn test_reservation_ttl_null_uses_default() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  reservation_ttl_secs: null
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;
            assert_eq!(config.batches.reservation_ttl_secs, 600);

            Ok(())
        });
    }

    #[test]
    fn test_reservation_ttl_default() {
        let config = Config::default();
        assert_eq!(config.batches.reservation_ttl_secs, 600);
    }

    #[test]
    fn test_pending_capacity_counts_default_disabled() {
        let config = Config::default();
        assert!(!config.batches.pending_capacity_counts_enabled);
    }

    #[test]
    fn test_relaxation_factor_defaults_to_one() {
        let config = Config::default();
        assert_eq!(config.batches.relaxation_factor("1h"), 1.0);
        assert_eq!(config.batches.relaxation_factor("24h"), 1.0);
    }

    #[test]
    fn test_relaxation_factor_explicit_value() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  allowed_completion_windows: ["1h", "24h"]
  window_relaxation_factors:
    "1h": 1.0
    "24h": 1.5
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;
            assert_eq!(config.batches.relaxation_factor("1h"), 1.0);
            assert_eq!(config.batches.relaxation_factor("24h"), 1.5);
            Ok(())
        });
    }

    #[test]
    fn test_relaxation_factor_unknown_window_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  allowed_completion_windows: ["1h", "24h"]
  window_relaxation_factors:
    "12h": 1.5
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let result = Config::load(&args);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("12h"));
            Ok(())
        });
    }

    #[test]
    fn test_relaxation_factor_negative_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  allowed_completion_windows: ["1h", "24h"]
  window_relaxation_factors:
    "24h": -0.5
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let result = Config::load(&args);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("window_relaxation_factors"));
            Ok(())
        });
    }

    #[test]
    fn test_relaxation_factor_zero_allowed() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  allowed_completion_windows: ["1h", "24h"]
  window_relaxation_factors:
    "1h": 0.0
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;
            assert_eq!(config.batches.relaxation_factor("1h"), 0.0);
            Ok(())
        });
    }

    #[test]
    fn test_relaxation_factor_empty_map_backwards_compatible() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
batches:
  allowed_completion_windows: ["1h", "24h"]
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;
            assert_eq!(config.batches.relaxation_factor("1h"), 1.0);
            assert_eq!(config.batches.relaxation_factor("24h"), 1.0);
            Ok(())
        });
    }

    #[test]
    fn test_empty_cookie_domain_env_override_normalized_to_none() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
auth:
  native:
    session:
      cookie_domain: ".doubleword.ai"
"#,
            )?;

            jail.set_env("DWCTL_AUTH__NATIVE__SESSION__COOKIE_DOMAIN", "");

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.auth.native.session.cookie_domain, None);

            Ok(())
        });
    }

    #[test]
    fn test_additional_retryable_statuses_default_override_and_mapping() {
        Jail::expect_with(|jail| {
            jail.create_file("test.yaml", "secret_key: test-secret-key\n")?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.additional_retryable_statuses, vec![499]);
            assert_eq!(
                config
                    .background_services
                    .batch_daemon
                    .to_fusillade_config()
                    .additional_retryable_statuses,
                vec![499]
            );

            jail.create_file(
                "test.yaml",
                r#"
secret_key: test-secret-key
background_services:
  batch_daemon:
    additional_retryable_statuses: []
"#,
            )?;
            let config = Config::load(&args)?;
            assert!(config.background_services.batch_daemon.additional_retryable_statuses.is_empty());
            assert!(
                config
                    .background_services
                    .batch_daemon
                    .to_fusillade_config()
                    .additional_retryable_statuses
                    .is_empty()
            );

            Ok(())
        });
    }

    #[test]
    fn test_adaptive_concurrency_default_override_and_mapping() {
        Jail::expect_with(|jail| {
            jail.create_file("test.yaml", "secret_key: test-secret-key\n")?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert!(!config.background_services.batch_daemon.adaptive_concurrency);
            let fusillade_config = config.background_services.batch_daemon.to_fusillade_config();
            assert!(!fusillade_config.adaptive_concurrency);
            assert_eq!(fusillade_config.adaptive_growth_factor, 1.5);
            assert_eq!(fusillade_config.adaptive_cut_factor, 0.8);
            assert_eq!(fusillade_config.memory_gate_high_fraction, 0.0);

            jail.create_file(
                "test.yaml",
                r#"
secret_key: test-secret-key
background_services:
  batch_daemon:
    adaptive_concurrency: true
    adaptive_growth_factor: 2.0
    adaptive_cut_factor: 0.5
    memory_gate_high_fraction: 0.75
"#,
            )?;
            let config = Config::load(&args)?;
            let fusillade_config = config.background_services.batch_daemon.to_fusillade_config();
            assert!(fusillade_config.adaptive_concurrency);
            assert_eq!(fusillade_config.adaptive_growth_factor, 2.0);
            assert_eq!(fusillade_config.adaptive_cut_factor, 0.5);
            assert_eq!(fusillade_config.memory_gate_high_fraction, 0.75);

            Ok(())
        });
    }

    #[test]
    fn test_background_concurrency_limit_yaml_override_and_mapping() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: test-secret-key
background_services:
  batch_daemon:
    background_concurrency_limit: 37
    inject_deadline_priority: true
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(
                config
                    .background_services
                    .batch_daemon
                    .to_fusillade_config()
                    .background_concurrency_limit,
                37
            );
            assert!(
                config
                    .background_services
                    .batch_daemon
                    .to_fusillade_config()
                    .inject_deadline_priority
            );

            Ok(())
        });
    }

    #[test]
    fn test_urgency_weight_default() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.urgency_weight, 0.5);

            Ok(())
        });
    }

    #[test]
    fn test_urgency_weight_yaml_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    urgency_weight: 0.8
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.urgency_weight, 0.8);

            Ok(())
        });
    }

    #[test]
    fn test_urgency_weight_negative_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    urgency_weight: -0.1
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let result = Config::load(&args);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("urgency_weight must be between 0.0 and 1.0"));

            Ok(())
        });
    }

    #[test]
    fn test_urgency_weight_above_one_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    urgency_weight: 1.5
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let result = Config::load(&args);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("urgency_weight must be between 0.0 and 1.0"));

            Ok(())
        });
    }

    #[test]
    fn test_daemon_mode_default_and_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.mode, DaemonMode::Both);
            assert_eq!(
                fusillade::DaemonMode::from(config.background_services.batch_daemon.mode),
                fusillade::DaemonMode::Both
            );

            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    mode: request_only
"#,
            )?;
            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.mode, DaemonMode::RequestOnly);
            assert_eq!(
                fusillade::DaemonMode::from(config.background_services.batch_daemon.mode),
                fusillade::DaemonMode::RequestOnly
            );

            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    mode: batch_only
"#,
            )?;
            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.mode, DaemonMode::BatchOnly);
            assert_eq!(
                fusillade::DaemonMode::from(config.background_services.batch_daemon.mode),
                fusillade::DaemonMode::BatchOnly
            );

            Ok(())
        });
    }

    #[test]
    fn test_upload_watchdog_defaults_override_and_mapping() {
        Jail::expect_with(|jail| {
            jail.create_file("test.yaml", "secret_key: test-secret-key\n")?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            let daemon = &config.background_services.batch_daemon;
            assert_eq!(daemon.upload_stall_timeout_ms, 60_000);
            assert_eq!(daemon.upload_chunk_bytes, 64 * 1024);
            assert_eq!(daemon.upload_stall_poll_ms, 100);

            jail.create_file(
                "test.yaml",
                r#"
secret_key: test-secret-key
background_services:
  batch_daemon:
    upload_stall_timeout_ms: 30000
    upload_chunk_bytes: 8192
    upload_stall_poll_ms: 25
"#,
            )?;
            let config = Config::load(&args)?;
            let daemon = &config.background_services.batch_daemon;
            assert_eq!(daemon.upload_stall_timeout_ms, 30_000);
            assert_eq!(daemon.upload_chunk_bytes, 8 * 1024);
            assert_eq!(daemon.upload_stall_poll_ms, 25);

            let fusillade = daemon.to_fusillade_config();
            assert_eq!(fusillade.upload_stall_timeout_ms, 30_000);
            assert_eq!(fusillade.upload_chunk_bytes, 8 * 1024);
            assert_eq!(fusillade.upload_stall_poll_ms, 25);

            Ok(())
        });
    }

    #[test]
    fn test_upload_watchdog_zero_values_rejected() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());

        config.background_services.batch_daemon.upload_chunk_bytes = 0;
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("upload_chunk_bytes cannot be 0"), "{err}");

        config.background_services.batch_daemon.upload_chunk_bytes = 64 * 1024;
        config.background_services.batch_daemon.upload_stall_poll_ms = 0;
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("upload_stall_poll_ms cannot be 0"), "{err}");
    }

    #[test]
    fn test_background_processing_requires_priority_injection() {
        let mut config = Config::default();
        config.auth.native.enabled = true;
        config.secret_key = Some("test-secret-key".to_string());
        config.background_services.batch_daemon.background_concurrency_limit = 1;
        config.background_services.batch_daemon.inject_deadline_priority = false;

        let err = config.validate().unwrap_err().to_string();
        assert!(
            err.contains("background_concurrency_limit requires inject_deadline_priority"),
            "{err}"
        );
    }

    #[test]
    fn test_leak_interval_defaults_and_overrides() {
        Jail::expect_with(|jail| {
            jail.create_file("test.yaml", "secret_key: test-secret-key")?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;
            assert!(config.background_services.batch_daemon.leak.leak_enabled);
            assert_eq!(config.background_services.batch_daemon.leak.leak_interval_seconds.get(), 60);
            jail.create_file(
                "test.yaml",
                r#"
secret_key: test-secret-key
background_services:
  batch_daemon:
    leak_enabled: false
    leak_interval_seconds: 30
    model_leak_interval_seconds:
      large/model: 10
"#,
            )?;
            let config = Config::load(&args)?;
            assert!(!config.background_services.batch_daemon.leak.leak_enabled);
            assert_eq!(config.background_services.batch_daemon.leak.leak_interval_seconds.get(), 30);
            assert_eq!(
                config.background_services.batch_daemon.leak.model_leak_interval_seconds["large/model"].get(),
                10
            );
            jail.set_env("DWCTL_BACKGROUND_SERVICES__BATCH_DAEMON__LEAK_ENABLED", "true");
            jail.set_env("DWCTL_BACKGROUND_SERVICES__BATCH_DAEMON__LEAK_INTERVAL_SECONDS", "15");
            let config = Config::load(&args)?;
            assert!(config.background_services.batch_daemon.leak.leak_enabled);
            assert_eq!(config.background_services.batch_daemon.leak.leak_interval_seconds.get(), 15);
            Ok(())
        });
    }

    #[test]
    fn test_leak_interval_rejects_invalid_values() {
        Jail::expect_with(|jail| {
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            for value in ["0", "-1", "1.5", ".nan", "4294967296"] {
                for setting in [
                    format!("leak_interval_seconds: {value}"),
                    format!("model_leak_interval_seconds: {{large/model: {value}}}"),
                ] {
                    jail.create_file(
                        "test.yaml",
                        &format!("secret_key: test-secret-key\nbackground_services:\n  batch_daemon:\n    {setting}\n"),
                    )?;
                    assert!(Config::load(&args).is_err(), "accepted {setting}");
                }
            }
            Ok(())
        });
    }

    #[test]
    fn test_claim_ramp_exponent_default_and_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.claim_ramp_exponent, 0.56);

            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    claim_ramp_exponent: 0.9
"#,
            )?;
            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.claim_ramp_exponent, 0.9);

            Ok(())
        });
    }

    #[test]
    fn test_claim_loop_max_consecutive_failures_default_and_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.claim_loop_max_consecutive_failures, 10);

            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    claim_loop_max_consecutive_failures: 3
"#,
            )?;
            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.claim_loop_max_consecutive_failures, 3);

            Ok(())
        });
    }

    #[test]
    fn prompt_cache_retention_keeps_at_least_the_recompute_grace() {
        let config = PromptCacheRetentionConfig::default();
        assert_eq!(config.grace_days, crate::recompute::report::CACHE_GRACE_DAYS as u64);
        assert!(config.validate().is_ok());
        let shorter = PromptCacheRetentionConfig {
            grace_days: config.grace_days - 1,
            ..config.clone()
        };
        assert!(shorter.validate().unwrap_err().contains("at least"));
        let longer = PromptCacheRetentionConfig { grace_days: 30, ..config };
        assert!(longer.validate().is_ok());
    }

    #[test]
    fn test_claim_query_timeout_default_and_override() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.claim_query_timeout_ms, 180_000);

            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    claim_query_timeout_ms: 60000
"#,
            )?;
            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.claim_query_timeout_ms, 60_000);

            Ok(())
        });
    }

    #[test]
    fn test_claim_ramp_exponent_negative_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    claim_ramp_exponent: -0.5
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let result = Config::load(&args);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("claim_ramp_exponent must be non-negative"));

            Ok(())
        });
    }

    #[test]
    fn test_claim_ramp_exponent_non_finite_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    claim_ramp_exponent: .nan
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let result = Config::load(&args);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("claim_ramp_exponent must be a finite number") || err.contains("invalid"),
                "unexpected error: {err}"
            );

            Ok(())
        });
    }

    #[test]
    fn test_urgency_weight_null_uses_default() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    urgency_weight: null
"#,
            )?;

            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };

            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.urgency_weight, 0.5);

            Ok(())
        });
    }

    #[test]
    fn test_batch_archive_cancel_grace_secs_negative_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    batch_archive_cancel_grace_secs: -1.0
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let result = Config::load(&args);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("batch_archive_cancel_grace_secs must be non-negative"),
                "unexpected error: {err}"
            );

            Ok(())
        });
    }

    #[test]
    fn test_batch_archive_cancel_grace_secs_non_finite_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    batch_archive_cancel_grace_secs: .inf
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let result = Config::load(&args);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("batch_archive_cancel_grace_secs must be a finite number") || err.contains("invalid"),
                "unexpected error: {err}"
            );

            Ok(())
        });
    }

    #[test]
    fn test_batch_archive_cancel_grace_secs_null_uses_default() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    batch_archive_cancel_grace_secs: null
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let config = Config::load(&args)?;
            assert_eq!(config.background_services.batch_daemon.batch_archive_cancel_grace_secs, 600.0);

            Ok(())
        });
    }

    #[test]
    fn test_batch_archive_sweep_dwell_secs_negative_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    batch_archive_sweep_dwell_secs: -0.001
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let result = Config::load(&args);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(err.contains("seconds value must be non-negative"), "unexpected error: {err}");

            Ok(())
        });
    }

    #[test]
    fn test_batch_archive_sweep_dwell_secs_non_finite_rejected() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "test.yaml",
                r#"
secret_key: "test-secret-key"
background_services:
  batch_daemon:
    batch_archive_sweep_dwell_secs: .nan
"#,
            )?;
            let args = Args {
                config: "test.yaml".into(),
                validate: false,
            };
            let result = Config::load(&args);
            assert!(result.is_err());
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("seconds value must be a finite number") || err.contains("invalid"),
                "unexpected error: {err}"
            );

            Ok(())
        });
    }
}
