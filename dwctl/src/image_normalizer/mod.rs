use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

pub mod config;
pub mod data_uri;
pub mod fetcher;
pub mod ip_filter;
pub mod sealed;
pub mod store;
pub mod token;
pub mod walker;

pub use config::{BackendConfig, FetcherConfig, ImageNormalizerConfig, SigningConfig};
pub use store::{ImageStore, MemoryStore, SignedImageUrl, StoreError};
pub use token::{ImageToken, TokenParseError};
pub use walker::Mode;

#[derive(Debug, Clone)]
pub enum ImageInput {
    HttpUrl(String),
    DataUri(String),
}

#[derive(Debug, thiserror::Error)]
pub enum NormalizeError {
    #[error("bad input: {0}")]
    BadInput(String),
    #[error("the provided image URL could not be retrieved: {0}; ensure it is publicly accessible and does not require authentication")]
    Unfetchable(String),
    #[error("fetch failed: {0}")]
    FetchFailed(String),
    #[error("transient failure: {0}")]
    Transient(String),
    #[error("store failed: {0}")]
    StoreFailed(String),
    #[error("token not found in store")]
    NotFound,
    #[error("image token is not accessible to this caller")]
    Forbidden,
    #[error("image access store unavailable")]
    AccessUnavailable,
}

impl NormalizeError {
    pub fn for_image(self, label: &str) -> Self {
        match self {
            NormalizeError::BadInput(m) => NormalizeError::BadInput(format!("{label}: {m}")),
            NormalizeError::Unfetchable(m) => NormalizeError::Unfetchable(format!("{label}: {m}")),
            NormalizeError::FetchFailed(m) => NormalizeError::FetchFailed(format!("{label}: {m}")),
            NormalizeError::Transient(m) => NormalizeError::Transient(format!("{label}: {m}")),
            other => other,
        }
    }
}

pub fn image_label(position: usize, url: &str) -> String {
    const MAX_URL_CHARS: usize = 120;
    match url::Url::parse(url) {
        Ok(u) if matches!(u.scheme(), "http" | "https") => {
            let mut shown = format!("{}://{}{}", u.scheme(), u.host_str().unwrap_or(""), u.path());
            if shown.chars().count() > MAX_URL_CHARS {
                shown = shown.chars().take(MAX_URL_CHARS - 3).collect::<String>() + "...";
            }
            format!("image {position} ({shown})")
        }
        _ => format!("image {position}"),
    }
}

impl From<fetcher::FetchError> for NormalizeError {
    fn from(e: fetcher::FetchError) -> Self {
        match e {
            fetcher::FetchError::BadInput(m) => NormalizeError::BadInput(m),
            fetcher::FetchError::Unfetchable(m) => NormalizeError::Unfetchable(m),
            fetcher::FetchError::FetchFailed(m) => NormalizeError::FetchFailed(m),
            fetcher::FetchError::Transient(m) => NormalizeError::Transient(m),
        }
    }
}

impl From<data_uri::DataUriError> for NormalizeError {
    fn from(e: data_uri::DataUriError) -> Self {
        NormalizeError::BadInput(e.to_string())
    }
}

impl From<StoreError> for NormalizeError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::NotFound => NormalizeError::NotFound,
            StoreError::Backend(m) => NormalizeError::StoreFailed(m),
            StoreError::Unimplemented => NormalizeError::StoreFailed("backend not implemented".into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngestResult {
    pub token: ImageToken,
    pub mime: String,
    pub bytes_len: u64,
}

#[async_trait]
pub trait ImageNormalizer: Send + Sync {
    async fn ingest(&self, input: ImageInput) -> Result<IngestResult, NormalizeError>;

    async fn sign(&self, token: ImageToken, ttl: Duration) -> Result<SignedImageUrl, NormalizeError>;

    async fn read(&self, token: ImageToken) -> Result<(String, Bytes), NormalizeError>;

    async fn load(&self, _input: ImageInput) -> Result<(String, Bytes), NormalizeError> {
        Err(NormalizeError::BadInput("image normalisation is disabled".into()))
    }

    async fn ingest_sealed(&self, _input: ImageInput, _key: &[u8]) -> Result<IngestResult, NormalizeError> {
        Err(NormalizeError::BadInput("image normalisation is disabled".into()))
    }

    async fn read_sealed(&self, token: ImageToken) -> Result<Option<Bytes>, NormalizeError> {
        let (_, bytes) = self.read(token).await?;
        Ok(sealed::is_sealed(&bytes).then_some(bytes))
    }

    fn owns_url(&self, _url: &str) -> bool {
        false
    }
}

pub struct DisabledNormalizer;

#[async_trait]
impl ImageNormalizer for DisabledNormalizer {
    async fn ingest(&self, _input: ImageInput) -> Result<IngestResult, NormalizeError> {
        Err(NormalizeError::BadInput("image normalisation is disabled".into()))
    }
    async fn sign(&self, _token: ImageToken, _ttl: Duration) -> Result<SignedImageUrl, NormalizeError> {
        Err(NormalizeError::BadInput("image normalisation is disabled".into()))
    }
    async fn read(&self, _token: ImageToken) -> Result<(String, Bytes), NormalizeError> {
        Err(NormalizeError::BadInput("image normalisation is disabled".into()))
    }
}

pub struct DefaultImageNormalizer<S: ImageStore> {
    fetcher: fetcher::ImageFetcher,
    store: Arc<S>,
    unique_upload_keys: bool,
}

impl<S: ImageStore> DefaultImageNormalizer<S> {
    pub fn new(fetcher_cfg: FetcherConfig, store: Arc<S>) -> Self {
        Self {
            fetcher: fetcher::ImageFetcher::new(fetcher_cfg),
            store,
            unique_upload_keys: false,
        }
    }

    pub fn with_unique_upload_keys(mut self, enabled: bool) -> Self {
        self.unique_upload_keys = enabled;
        self
    }
}

#[async_trait]
impl<S: ImageStore + 'static> ImageNormalizer for DefaultImageNormalizer<S> {
    async fn ingest(&self, input: ImageInput) -> Result<IngestResult, NormalizeError> {
        let (mime, bytes) = self.load(input).await?;
        let bytes_len = bytes.len() as u64;
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let digest = hasher.finalize();
        let mut sha = [0u8; 32];
        sha.copy_from_slice(&digest);
        let token = if self.unique_upload_keys {
            let token = ImageToken::new_unique(sha);
            self.store.put(token, &mime, bytes).await?;
            token
        } else {
            let token = ImageToken(sha, None);
            let exists = match self.store.exists(token).await {
                Ok(exists) => exists,
                Err(e) => {
                    tracing::warn!(error = %e, "image existence check failed, uploading instead");
                    false
                }
            };
            if !exists {
                self.store.put(token, &mime, bytes).await?;
            }
            token
        };
        Ok(IngestResult { token, mime, bytes_len })
    }

    async fn load(&self, input: ImageInput) -> Result<(String, Bytes), NormalizeError> {
        let (mime, bytes) = match input {
            ImageInput::HttpUrl(url) => {
                let fetched = self.fetcher.fetch(&url).await?;
                (fetched.mime, fetched.bytes)
            }
            ImageInput::DataUri(uri) => {
                let max_encoded_len = self.fetcher.max_bytes().div_ceil(3).saturating_mul(4);
                if uri
                    .split_once(',')
                    .is_some_and(|(_, payload)| payload.len() as u64 > max_encoded_len)
                {
                    return Err(NormalizeError::BadInput(format!(
                        "data: URI payload exceeds cap {}",
                        self.fetcher.max_bytes()
                    )));
                }
                let decoded = data_uri::parse(&uri)?;
                let len = decoded.bytes.len() as u64;
                if len > self.fetcher.max_bytes() {
                    return Err(NormalizeError::BadInput(format!(
                        "data: URI payload {len} bytes exceeds cap {}",
                        self.fetcher.max_bytes()
                    )));
                }
                if !self.fetcher.mime_allowed(&decoded.mime) {
                    return Err(NormalizeError::BadInput(format!("mime not allowed: {}", decoded.mime)));
                }
                (decoded.mime, Bytes::from(decoded.bytes))
            }
        };
        if sealed::is_sealed(&bytes) {
            return Err(NormalizeError::BadInput("payload is not a supported image".into()));
        }
        Ok((mime, bytes))
    }

    async fn ingest_sealed(&self, input: ImageInput, key: &[u8]) -> Result<IngestResult, NormalizeError> {
        let (mime, bytes) = self.load(input).await?;
        let bytes_len = bytes.len() as u64;
        let blob = sealed::seal(key, &mime, &bytes).map_err(|e| NormalizeError::StoreFailed(format!("sealing image failed: {e}")))?;
        let sha: [u8; 32] = Sha256::digest(&blob).into();
        let token = ImageToken::new_unique(sha);
        self.store.put(token, sealed::SEALED_CONTENT_TYPE, Bytes::from(blob)).await?;
        Ok(IngestResult { token, mime, bytes_len })
    }

    async fn sign(&self, token: ImageToken, ttl: Duration) -> Result<SignedImageUrl, NormalizeError> {
        Ok(self.store.sign(token, ttl).await?)
    }

    async fn read(&self, token: ImageToken) -> Result<(String, Bytes), NormalizeError> {
        Ok(self.store.read(token).await?)
    }

    fn owns_url(&self, url: &str) -> bool {
        self.store.owns_url(url)
    }
}

pub fn from_config(cfg: &ImageNormalizerConfig) -> Result<Arc<dyn ImageNormalizer>, anyhow::Error> {
    if !cfg.enabled {
        return Ok(Arc::new(DisabledNormalizer));
    }
    let backend = cfg
        .backend
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("image_normalizer.enabled = true but image_normalizer.backend is not set"))?;
    Ok(match backend {
        BackendConfig::Memory => {
            tracing::warn!(
                "image_normalizer enabled with the in-memory backend: stored image bytes are lost on \
                 restart and are not shared across replicas — use gcs or s3_compatible in production"
            );
            let store = Arc::new(MemoryStore::new());
            Arc::new(DefaultImageNormalizer::new(cfg.fetcher.clone(), store).with_unique_upload_keys(cfg.unique_upload_keys))
        }
        BackendConfig::Gcs { bucket, region } => {
            let store = Arc::new(store::GcsStore::new(bucket.clone(), region.clone()));
            Arc::new(DefaultImageNormalizer::new(cfg.fetcher.clone(), store).with_unique_upload_keys(cfg.unique_upload_keys))
        }
        BackendConfig::S3Compatible {
            bucket,
            endpoint_url,
            region,
            force_path_style,
            reuse_max_age_secs,
        } => {
            let access_key_id = std::env::var("IMAGE_NORMALIZER_S3_ACCESS_KEY_ID").map_err(|_| {
                anyhow::anyhow!(
                    "image_normalizer.backend.type = s3_compatible requires the \
                     IMAGE_NORMALIZER_S3_ACCESS_KEY_ID environment variable"
                )
            })?;
            let secret_access_key = std::env::var("IMAGE_NORMALIZER_S3_SECRET_ACCESS_KEY").map_err(|_| {
                anyhow::anyhow!(
                    "image_normalizer.backend.type = s3_compatible requires the \
                     IMAGE_NORMALIZER_S3_SECRET_ACCESS_KEY environment variable"
                )
            })?;
            let store = Arc::new(
                store::S3CompatStore::new(
                    bucket.clone(),
                    endpoint_url.clone(),
                    region.clone(),
                    *force_path_style,
                    access_key_id,
                    secret_access_key,
                )
                .with_reuse_max_age((*reuse_max_age_secs > 0).then(|| Duration::from_secs(*reuse_max_age_secs))),
            );
            Arc::new(DefaultImageNormalizer::new(cfg.fetcher.clone(), store).with_unique_upload_keys(cfg.unique_upload_keys))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TINY_PNG_DATA_URI: &str =
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=";

    #[tokio::test]
    async fn ingest_data_uri_then_sign_round_trip() {
        let store = Arc::new(MemoryStore::new());
        let n = DefaultImageNormalizer::new(FetcherConfig::default(), store.clone());

        let result = n.ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string())).await.unwrap();
        let token = result.token;
        assert_eq!(result.mime, "image/png");
        assert!(result.bytes_len > 0, "bytes_len should be the actual decoded length, got 0");
        assert_eq!(token.1, None, "unique_upload_keys is off by default: content-addressed token");

        let result_again = n.ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string())).await.unwrap();
        assert_eq!(token, result_again.token);
        assert_eq!(result.bytes_len, result_again.bytes_len);

        let signed = n.sign(token, Duration::from_secs(60)).await.unwrap();
        assert!(signed.url.contains(&token.to_hex()));

        let (mime, bytes) = n.read(token).await.unwrap();
        assert_eq!(mime, "image/png");
        assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n");
    }

    #[tokio::test]
    async fn ingest_with_unique_upload_keys_stores_each_ingest_separately() {
        let store = Arc::new(MemoryStore::new());
        let n = DefaultImageNormalizer::new(FetcherConfig::default(), store).with_unique_upload_keys(true);
        let a = n.ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string())).await.unwrap().token;
        let b = n.ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string())).await.unwrap().token;
        assert_eq!(a.0, b.0);
        assert!(a.1.is_some() && b.1.is_some());
        assert_ne!(a, b);
        assert_eq!(n.read(a).await.unwrap().1, n.read(b).await.unwrap().1);
        let legacy = DefaultImageNormalizer::new(FetcherConfig::default(), Arc::new(MemoryStore::new()));
        let t = legacy
            .ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string()))
            .await
            .unwrap()
            .token;
        assert_eq!(t, ImageToken(a.0, None));
    }

    struct FailingExistsStore(MemoryStore);

    #[async_trait]
    impl ImageStore for FailingExistsStore {
        async fn put(&self, token: ImageToken, mime: &str, bytes: Bytes) -> Result<bool, StoreError> {
            self.0.put(token, mime, bytes).await
        }
        async fn sign(&self, token: ImageToken, ttl: Duration) -> Result<SignedImageUrl, StoreError> {
            self.0.sign(token, ttl).await
        }
        async fn read(&self, token: ImageToken) -> Result<(String, Bytes), StoreError> {
            self.0.read(token).await
        }
        async fn exists(&self, _token: ImageToken) -> Result<bool, StoreError> {
            Err(StoreError::Backend("HEAD: service unavailable".into()))
        }
    }

    #[tokio::test]
    async fn ingest_uploads_when_existence_check_fails() {
        let n = DefaultImageNormalizer::new(FetcherConfig::default(), Arc::new(FailingExistsStore(MemoryStore::new())));
        let token = n.ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string())).await.unwrap().token;
        assert_eq!(token.1, None);
        assert_eq!(n.read(token).await.unwrap().0, "image/png");
    }

    #[tokio::test]
    async fn ingest_sealed_never_stores_plaintext() {
        let store = Arc::new(MemoryStore::new());
        let n = DefaultImageNormalizer::new(FetcherConfig::default(), store.clone());
        let key = crate::keystore::generate_key();

        let sealed_result = n
            .ingest_sealed(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string()), &key)
            .await
            .unwrap();
        assert_eq!(sealed_result.mime, "image/png");
        assert!(sealed_result.token.1.is_some(), "a sealed object is always its own upload");

        let objects = store.objects();
        assert_eq!(objects.len(), 1);
        assert!(sealed::is_sealed(&objects[0]));
        assert!(!objects[0].windows(4).any(|w| w == b"\x89PNG"));

        let plain = DefaultImageNormalizer::new(FetcherConfig::default(), Arc::new(MemoryStore::new()))
            .ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string()))
            .await
            .unwrap();
        assert_ne!(sealed_result.token.0, plain.token.0);

        let blob = n.read_sealed(sealed_result.token).await.unwrap().expect("sealed object");
        let (mime, bytes) = sealed::open(&key, &blob).unwrap();
        assert_eq!(mime, "image/png");
        assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n");
        assert!(sealed::open(&crate::keystore::generate_key(), &blob).is_err());
    }

    #[tokio::test]
    async fn a_payload_that_looks_sealed_is_refused() {
        use base64::Engine as _;
        let mut spoof = sealed::SEALED_MAGIC.to_vec();
        spoof.extend_from_slice(b"not really sealed");
        let uri = format!("data:image/png;base64,{}", base64::engine::general_purpose::STANDARD.encode(&spoof));
        let store = Arc::new(MemoryStore::new());
        let n = DefaultImageNormalizer::new(FetcherConfig::default(), store.clone());
        let err = n.ingest(ImageInput::DataUri(uri)).await.unwrap_err();
        assert!(matches!(err, NormalizeError::BadInput(_)), "got {err:?}");
        assert!(store.objects().is_empty());
    }

    #[tokio::test]
    async fn an_oversized_data_uri_is_refused_before_decoding() {
        let cfg = FetcherConfig {
            max_bytes: 8,
            ..FetcherConfig::default()
        };
        let n = DefaultImageNormalizer::new(cfg, Arc::new(MemoryStore::new()));
        let uri = format!("data:image/png;base64,{}", "!".repeat(64));
        let err = n.load(ImageInput::DataUri(uri)).await.unwrap_err();
        match err {
            NormalizeError::BadInput(m) => assert!(m.contains("exceeds cap"), "{m}"),
            other => panic!("got {other:?}"),
        }
        let ok = format!("data:image/png;base64,{}", "AAAAAAAAAAA=");
        assert!(n.load(ImageInput::DataUri(ok)).await.is_ok());
    }

    #[tokio::test]
    async fn read_sealed_is_none_for_plaintext_objects() {
        let n = DefaultImageNormalizer::new(FetcherConfig::default(), Arc::new(MemoryStore::new()));
        let token = n.ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string())).await.unwrap().token;
        assert!(n.read_sealed(token).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn load_validates_without_storing() {
        let store = Arc::new(MemoryStore::new());
        let n = DefaultImageNormalizer::new(FetcherConfig::default(), store.clone());
        let (mime, bytes) = n.load(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string())).await.unwrap();
        assert_eq!(mime, "image/png");
        assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n");
        assert!(store.objects().is_empty());
        let err = n
            .load(ImageInput::DataUri("data:text/html;base64,PGgxPmhpPC9oMT4=".to_string()))
            .await
            .unwrap_err();
        assert!(matches!(err, NormalizeError::BadInput(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn ingest_bad_data_uri_returns_bad_input() {
        let store = Arc::new(MemoryStore::new());
        let n = DefaultImageNormalizer::new(FetcherConfig::default(), store);
        let err = n.ingest(ImageInput::DataUri("data:image/png,raw".to_string())).await.unwrap_err();
        assert!(matches!(err, NormalizeError::BadInput(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn ingest_oversized_data_uri_is_rejected() {
        let store = Arc::new(MemoryStore::new());
        let cfg = FetcherConfig {
            max_bytes: 8,
            ..FetcherConfig::default()
        };
        let n = DefaultImageNormalizer::new(cfg, store);
        let err = n.ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string())).await.unwrap_err();
        assert!(matches!(err, NormalizeError::BadInput(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn ingest_data_uri_with_disallowed_mime_is_rejected() {
        let store = Arc::new(MemoryStore::new());
        let n = DefaultImageNormalizer::new(FetcherConfig::default(), store);
        let err = n
            .ingest(ImageInput::DataUri("data:text/html;base64,PGgxPmhpPC9oMT4=".to_string()))
            .await
            .unwrap_err();
        assert!(matches!(err, NormalizeError::BadInput(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn disabled_normalizer_errors_predictably() {
        let n = DisabledNormalizer;
        let err = n.ingest(ImageInput::DataUri(TINY_PNG_DATA_URI.to_string())).await.unwrap_err();
        assert!(matches!(err, NormalizeError::BadInput(_)));
    }

    #[test]
    fn from_config_disabled_returns_disabled_normalizer() {
        let cfg = ImageNormalizerConfig::default();
        let _: Arc<dyn ImageNormalizer> = from_config(&cfg).expect("disabled config must build cleanly");
    }

    #[test]
    fn from_config_memory_backend_when_enabled() {
        let cfg = ImageNormalizerConfig {
            enabled: true,
            backend: Some(BackendConfig::Memory),
            fetcher: FetcherConfig::default(),
            signing: SigningConfig::default(),
            unique_upload_keys: false,
        };
        let _: Arc<dyn ImageNormalizer> = from_config(&cfg).expect("memory backend must build");
    }

    #[test]
    fn from_config_enabled_without_backend_errors() {
        let cfg = ImageNormalizerConfig {
            enabled: true,
            backend: None,
            fetcher: FetcherConfig::default(),
            signing: SigningConfig::default(),
            unique_upload_keys: false,
        };
        match from_config(&cfg) {
            Ok(_) => panic!("enabled + no backend must error"),
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("backend"), "error should mention 'backend': {msg}");
            }
        }
    }

    #[test]
    fn image_label_names_position_and_strips_query() {
        assert_eq!(
            image_label(2, "https://user:pw@cdn.example.com/a/b.png?sig=secret#frag"),
            "image 2 (https://cdn.example.com/a/b.png)"
        );
        assert_eq!(image_label(1, "data:image/png;base64,AAAA"), "image 1");
        assert_eq!(image_label(3, "dw-img://abc"), "image 3");
    }

    #[test]
    fn image_label_truncates_long_urls() {
        let label = image_label(1, &format!("https://cdn.example.com/{}", "x".repeat(300)));
        assert!(label.ends_with("...)"), "{label}");
        assert!(label.chars().count() < 140, "{label}");
    }

    #[test]
    fn for_image_prefixes_per_image_errors_only() {
        let msg = NormalizeError::Unfetchable("origin 403 Forbidden".into())
            .for_image("image 2 (https://cdn.example.com/b.png)")
            .to_string();
        assert!(
            msg.starts_with("the provided image URL could not be retrieved: image 2 (https://cdn.example.com/b.png): origin 403 Forbidden"),
            "{msg}"
        );
        assert!(matches!(NormalizeError::Forbidden.for_image("image 1"), NormalizeError::Forbidden));
    }
}
