use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImageNormalizerConfig {
    #[serde(default)]
    pub enabled: bool,

    pub backend: Option<BackendConfig>,

    #[serde(default)]
    pub fetcher: FetcherConfig,

    #[serde(default)]
    pub signing: SigningConfig,

    #[serde(default)]
    pub unique_upload_keys: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendConfig {
    Memory,
    Gcs {
        bucket: String,
        #[serde(default = "default_gcs_region")]
        region: String,
    },
    S3Compatible {
        bucket: String,
        endpoint_url: String,
        #[serde(default = "default_s3_region")]
        region: String,
        #[serde(default = "default_true")]
        force_path_style: bool,
        #[serde(default = "default_s3_reuse_max_age_secs")]
        reuse_max_age_secs: u64,
    },
}

fn default_s3_reuse_max_age_secs() -> u64 {
    5 * 24 * 60 * 60
}

fn default_gcs_region() -> String {
    "europe-west4".to_string()
}

fn default_s3_region() -> String {
    "auto".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetcherConfig {
    pub max_bytes: u64,

    pub timeout_secs: u64,

    pub max_redirects: u8,

    pub max_retries: u8,

    pub retry_base_delay_ms: u64,

    pub allowed_mime: Vec<String>,
}

impl Default for FetcherConfig {
    fn default() -> Self {
        Self {
            max_bytes: 20 * 1024 * 1024,
            timeout_secs: 30,
            max_redirects: 3,
            max_retries: 3,
            retry_base_delay_ms: 250,
            allowed_mime: vec![
                "image/png".to_string(),
                "image/jpeg".to_string(),
                "image/webp".to_string(),
                "image/gif".to_string(),
            ],
        }
    }
}

impl FetcherConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
    pub fn retry_base_delay(&self) -> Duration {
        Duration::from_millis(self.retry_base_delay_ms)
    }
    pub fn mime_allowed(&self, mime: &str) -> bool {
        let mime = mime.to_ascii_lowercase();
        self.allowed_mime.iter().any(|m| m.eq_ignore_ascii_case(&mime))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SigningConfig {
    pub realtime_ttl_secs: u64,
    pub dispatch_ttl_secs: Option<u64>,
    pub dispatch_ttl_headroom_secs: u64,
    pub dashboard_ttl_secs: u64,
}

impl Default for SigningConfig {
    fn default() -> Self {
        Self {
            realtime_ttl_secs: 900,
            dispatch_ttl_secs: None,
            dispatch_ttl_headroom_secs: 300,
            dashboard_ttl_secs: 300,
        }
    }
}

impl SigningConfig {
    pub fn realtime_ttl(&self) -> Duration {
        Duration::from_secs(self.realtime_ttl_secs)
    }

    pub fn dispatch_ttl(&self, processing_timeout: Duration) -> Duration {
        match self.dispatch_ttl_secs {
            Some(s) => Duration::from_secs(s),
            None => processing_timeout + Duration::from_secs(self.dispatch_ttl_headroom_secs),
        }
    }

    pub fn dashboard_ttl(&self) -> Duration {
        Duration::from_secs(self.dashboard_ttl_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_reuse_max_age_defaults_to_five_days_and_can_be_disabled() {
        let cfg: BackendConfig = serde_yaml::from_str("type: s3_compatible\nbucket: b\nendpoint_url: https://example.invalid\n").unwrap();
        let BackendConfig::S3Compatible { reuse_max_age_secs, .. } = cfg else {
            panic!("expected s3_compatible");
        };
        assert_eq!(reuse_max_age_secs, 5 * 24 * 60 * 60);

        let cfg: BackendConfig =
            serde_yaml::from_str("type: s3_compatible\nbucket: b\nendpoint_url: https://example.invalid\nreuse_max_age_secs: 0\n").unwrap();
        let BackendConfig::S3Compatible { reuse_max_age_secs, .. } = cfg else {
            panic!("expected s3_compatible");
        };
        assert_eq!(reuse_max_age_secs, 0);
    }

    #[test]
    fn defaults_disabled_with_no_backend() {
        let c = ImageNormalizerConfig::default();
        assert!(!c.enabled);
        assert!(c.backend.is_none(), "no default backend — must be set explicitly when enabled");
    }

    #[test]
    fn fetcher_default_mime_includes_common_image_types() {
        let f = FetcherConfig::default();
        assert!(f.mime_allowed("image/png"));
        assert!(f.mime_allowed("IMAGE/JPEG"));
        assert!(!f.mime_allowed("application/json"));
        assert!(!f.mime_allowed("text/html"));
    }

    #[test]
    fn json_round_trip_with_gcs_backend() {
        let cfg = ImageNormalizerConfig {
            enabled: true,
            backend: Some(BackendConfig::Gcs {
                bucket: "my-bucket".to_string(),
                region: "europe-west4".to_string(),
            }),
            fetcher: FetcherConfig {
                max_bytes: 1_048_576,
                timeout_secs: 10,
                max_redirects: 1,
                max_retries: 0,
                retry_base_delay_ms: 100,
                allowed_mime: vec!["image/png".to_string()],
            },
            signing: SigningConfig {
                realtime_ttl_secs: 60,
                dispatch_ttl_secs: Some(120),
                dispatch_ttl_headroom_secs: 300,
                dashboard_ttl_secs: 30,
            },
            unique_upload_keys: true,
        };
        let json = serde_json::to_string(&cfg).expect("serialize");
        let back: ImageNormalizerConfig = serde_json::from_str(&json).expect("deserialize");
        assert!(back.enabled);
        match back.backend {
            Some(BackendConfig::Gcs { ref bucket, .. }) => assert_eq!(bucket, "my-bucket"),
            _ => panic!("expected gcs backend"),
        }
        assert_eq!(back.fetcher.max_bytes, 1_048_576);
        assert!(back.fetcher.mime_allowed("image/png"));
        assert!(!back.fetcher.mime_allowed("image/jpeg"));
        assert_eq!(back.signing.realtime_ttl().as_secs(), 60);
        assert!(back.unique_upload_keys);
    }

    #[test]
    fn unique_upload_keys_defaults_off() {
        let c: ImageNormalizerConfig = serde_yaml::from_str("enabled: true\nbackend:\n  type: memory\n").unwrap();
        assert!(!c.unique_upload_keys);
    }

    #[test]
    fn dispatch_ttl_explicit_override_used_verbatim() {
        let signing = SigningConfig {
            realtime_ttl_secs: 900,
            dispatch_ttl_secs: Some(3600),
            dispatch_ttl_headroom_secs: 300,
            dashboard_ttl_secs: 300,
        };
        assert_eq!(signing.dispatch_ttl(Duration::from_secs(60)).as_secs(), 3600);
        assert_eq!(signing.dispatch_ttl(Duration::from_secs(99999)).as_secs(), 3600);
    }

    #[test]
    fn dispatch_ttl_derived_from_processing_timeout_plus_headroom() {
        let signing = SigningConfig::default();
        assert_eq!(signing.dispatch_ttl(Duration::from_secs(600)).as_secs(), 900);
        assert_eq!(signing.dispatch_ttl(Duration::from_secs(1800)).as_secs(), 2100);
    }

    #[test]
    fn dispatch_ttl_headroom_is_configurable() {
        let signing = SigningConfig {
            realtime_ttl_secs: 900,
            dispatch_ttl_secs: None,
            dispatch_ttl_headroom_secs: 60,
            dashboard_ttl_secs: 300,
        };
        assert_eq!(signing.dispatch_ttl(Duration::from_secs(600)).as_secs(), 660);
    }
}
