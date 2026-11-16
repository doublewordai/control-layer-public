use async_trait::async_trait;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use bytes::Bytes;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use metrics::counter;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tracing::debug;
use url::Url;

use super::token::ImageToken;

#[derive(Debug, Clone)]
pub struct SignedImageUrl {
    pub url: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("object not found in store")]
    NotFound,
    #[error("object-store backend error: {0}")]
    Backend(String),
    #[error("not implemented for this backend")]
    Unimplemented,
}

#[async_trait]
pub trait ImageStore: Send + Sync {
    async fn put(&self, token: ImageToken, mime: &str, bytes: Bytes) -> Result<bool, StoreError>;

    async fn sign(&self, token: ImageToken, ttl: Duration) -> Result<SignedImageUrl, StoreError>;

    async fn read(&self, token: ImageToken) -> Result<(String, Bytes), StoreError>;

    async fn exists(&self, token: ImageToken) -> Result<bool, StoreError>;

    fn owns_url(&self, _url: &str) -> bool {
        false
    }
}


pub struct MemoryStore {
    inner: Mutex<HashMap<ImageToken, (String, Bytes)>>,
    base_url: String,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            base_url: "http://localhost/dw-img".to_string(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    #[cfg(test)]
    pub fn objects(&self) -> Vec<Bytes> {
        let map = self.inner.lock().expect("MemoryStore mutex poisoned");
        map.values().map(|(_, bytes)| bytes.clone()).collect()
    }
}

#[async_trait]
impl ImageStore for MemoryStore {
    async fn put(&self, token: ImageToken, mime: &str, bytes: Bytes) -> Result<bool, StoreError> {
        let mut map = self.inner.lock().expect("MemoryStore mutex poisoned");
        if map.contains_key(&token) {
            return Ok(false);
        }
        map.insert(token, (mime.to_string(), bytes));
        Ok(true)
    }

    async fn sign(&self, token: ImageToken, ttl: Duration) -> Result<SignedImageUrl, StoreError> {
        let map = self.inner.lock().expect("MemoryStore mutex poisoned");
        if !map.contains_key(&token) {
            return Err(StoreError::NotFound);
        }
        let expires_at = Utc::now() + ChronoDuration::from_std(ttl).unwrap_or(ChronoDuration::seconds(900));
        let url = format!(
            "{}/{}?expires={}",
            self.base_url.trim_end_matches('/'),
            token.to_hex(),
            expires_at.timestamp()
        );
        Ok(SignedImageUrl { url, expires_at })
    }

    async fn read(&self, token: ImageToken) -> Result<(String, Bytes), StoreError> {
        let map = self.inner.lock().expect("MemoryStore mutex poisoned");
        map.get(&token).cloned().ok_or(StoreError::NotFound)
    }

    async fn exists(&self, token: ImageToken) -> Result<bool, StoreError> {
        let map = self.inner.lock().expect("MemoryStore mutex poisoned");
        Ok(map.contains_key(&token))
    }

    fn owns_url(&self, url: &str) -> bool {
        let (Ok(base), Ok(u)) = (Url::parse(&self.base_url), Url::parse(url)) else {
            return false;
        };
        if base.scheme() != u.scheme() || base.host_str() != u.host_str() || base.port_or_known_default() != u.port_or_known_default() {
            return false;
        }
        let prefix = base.path().trim_end_matches('/');
        match u.path().strip_prefix(prefix) {
            Some(rest) => rest.is_empty() || rest.starts_with('/'),
            None => false,
        }
    }
}


pub struct GcsStore {
    pub bucket: String,
    pub region: String,
    client_cell: tokio::sync::OnceCell<google_cloud_storage::client::Storage>,
    signer_cell: tokio::sync::OnceCell<google_cloud_auth::signer::Signer>,
}

impl GcsStore {
    pub fn new(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            client_cell: tokio::sync::OnceCell::new(),
            signer_cell: tokio::sync::OnceCell::new(),
        }
    }

    pub(crate) fn key(token: ImageToken) -> String {
        let hex = token.to_hex();
        format!("images/{}/{}/{}", &hex[..2], &hex[2..4], hex)
    }

    fn bucket_resource(&self) -> String {
        format!("projects/_/buckets/{}", self.bucket)
    }

    async fn client(&self) -> Result<&google_cloud_storage::client::Storage, StoreError> {
        self.client_cell
            .get_or_try_init(|| async {
                google_cloud_storage::client::Storage::builder()
                    .build()
                    .await
                    .map_err(|e| StoreError::Backend(format!("GCS client init: {e}")))
            })
            .await
    }

    async fn signer(&self) -> Result<&google_cloud_auth::signer::Signer, StoreError> {
        self.signer_cell
            .get_or_try_init(|| async {
                google_cloud_auth::credentials::Builder::default()
                    .build_signer()
                    .map_err(|e| StoreError::Backend(format!("ADC signer init: {e}")))
            })
            .await
    }
}

#[async_trait]
impl ImageStore for GcsStore {
    async fn put(&self, token: ImageToken, mime: &str, bytes: Bytes) -> Result<bool, StoreError> {
        let client = self.client().await?;
        let key = Self::key(token);
        client
            .write_object(self.bucket_resource(), &key, bytes)
            .set_content_type(mime)
            .send_buffered()
            .await
            .map_err(|e| StoreError::Backend(format!("GCS put {key}: {e}")))?;
        Ok(true)
    }

    async fn sign(&self, token: ImageToken, ttl: Duration) -> Result<SignedImageUrl, StoreError> {
        let signer = self.signer().await?;
        let key = Self::key(token);
        let url = google_cloud_storage::builder::storage::SignedUrlBuilder::for_object(self.bucket_resource(), &key)
            .with_method(google_cloud_storage::http::Method::GET)
            .with_expiration(ttl)
            .sign_with(signer)
            .await
            .map_err(|e| StoreError::Backend(format!("GCS sign {key}: {e}")))?;
        let expires_at = Utc::now() + ChronoDuration::from_std(ttl).unwrap_or(ChronoDuration::seconds(900));
        Ok(SignedImageUrl { url, expires_at })
    }

    async fn read(&self, token: ImageToken) -> Result<(String, Bytes), StoreError> {
        let client = self.client().await?;
        let key = Self::key(token);
        let mut resp = client
            .read_object(self.bucket_resource(), &key)
            .send()
            .await
            .map_err(|e| StoreError::Backend(format!("GCS read {key}: {e}")))?;
        let mime = resp.object().content_type.clone();
        let mut bytes_vec: Vec<u8> = Vec::new();
        while let Some(chunk) = resp.next().await {
            let chunk = chunk.map_err(|e| StoreError::Backend(format!("GCS read body: {e}")))?;
            bytes_vec.extend_from_slice(&chunk);
        }
        Ok((mime, Bytes::from(bytes_vec)))
    }

    async fn exists(&self, token: ImageToken) -> Result<bool, StoreError> {
        let client = self.client().await?;
        let key = Self::key(token);
        match client.read_object(self.bucket_resource(), &key).send().await {
            Ok(_) => Ok(true),
            Err(e) => match e.http_status_code() {
                Some(404) => Ok(false),
                _ => Err(StoreError::Backend(format!("GCS exists {key}: {e}"))),
            },
        }
    }

    fn owns_url(&self, url: &str) -> bool {
        let Ok(u) = Url::parse(url) else {
            return false;
        };
        u.scheme() == "https"
            && u.host_str() == Some("storage.googleapis.com")
            && u.path().starts_with(&format!("/{}/images/", self.bucket))
    }
}


fn stale_for_reuse(last_modified_secs: Option<i64>, now_secs: i64, max_age: Option<Duration>) -> bool {
    match (last_modified_secs, max_age) {
        (Some(modified), Some(max_age)) => {
            let max_age_secs = i64::try_from(max_age.as_secs()).unwrap_or(i64::MAX);
            now_secs.saturating_sub(modified) >= max_age_secs
        }
        _ => false,
    }
}

fn head_last_modified_secs(head: &aws_sdk_s3::operation::head_object::HeadObjectOutput) -> Option<i64> {
    head.last_modified().map(|t| t.secs())
}

const S3_MAX_PRESIGN: Duration = Duration::from_secs(7 * 24 * 60 * 60 - 60);

const S3_MAX_ATTEMPTS: u32 = 5;

const S3_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

pub struct S3CompatStore {
    bucket: String,
    reuse_max_age: Option<Duration>,
    endpoint: String,
    client: aws_sdk_s3::Client,
}

impl S3CompatStore {
    pub fn new(
        bucket: impl Into<String>,
        endpoint_url: impl Into<String>,
        region: impl Into<String>,
        force_path_style: bool,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        let creds =
            aws_credential_types::Credentials::new(access_key_id.into(), secret_access_key.into(), None, None, "dwctl-image-normalizer");
        let endpoint = endpoint_url.into();
        let s3_config = aws_sdk_s3::config::Builder::new()
            .region(aws_sdk_s3::config::Region::new(region.into()))
            .credentials_provider(creds)
            .endpoint_url(endpoint.clone())
            .force_path_style(force_path_style)
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .retry_config(RetryConfig::standard().with_max_attempts(S3_MAX_ATTEMPTS))
            .timeout_config(TimeoutConfig::builder().operation_timeout(S3_OPERATION_TIMEOUT).build())
            .build();
        Self {
            bucket: bucket.into(),
            reuse_max_age: None,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            client: aws_sdk_s3::Client::from_conf(s3_config),
        }
    }

    pub fn with_reuse_max_age(mut self, max_age: Option<Duration>) -> Self {
        self.reuse_max_age = max_age;
        self
    }

    fn key(token: ImageToken) -> String {
        let hex = token.to_hex();
        format!("images/{}/{}/{}", &hex[..2], &hex[2..4], hex)
    }
}

#[async_trait]
impl ImageStore for S3CompatStore {
    async fn put(&self, token: ImageToken, mime: &str, bytes: Bytes) -> Result<bool, StoreError> {
        let key = Self::key(token);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .content_type(mime)
            .body(aws_sdk_s3::primitives::ByteStream::from(bytes))
            .send()
            .await
            .map_err(|e| StoreError::Backend(format!("S3 put {key}: {}", e.into_service_error())))?;
        Ok(true)
    }

    async fn sign(&self, token: ImageToken, ttl: Duration) -> Result<SignedImageUrl, StoreError> {
        let key = Self::key(token);
        let ttl = ttl.min(S3_MAX_PRESIGN);
        let presign = aws_sdk_s3::presigning::PresigningConfig::expires_in(ttl)
            .map_err(|e| StoreError::Backend(format!("S3 presign config: {e}")))?;
        let req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .presigned(presign)
            .await
            .map_err(|e| StoreError::Backend(format!("S3 sign {key}: {}", e.into_service_error())))?;
        let expires_at = Utc::now() + ChronoDuration::from_std(ttl).unwrap_or(ChronoDuration::seconds(900));
        Ok(SignedImageUrl {
            url: req.uri().to_string(),
            expires_at,
        })
    }

    async fn read(&self, token: ImageToken) -> Result<(String, Bytes), StoreError> {
        let key = Self::key(token);
        let resp = self.client.get_object().bucket(&self.bucket).key(&key).send().await.map_err(|e| {
            let svc = e.into_service_error();
            if svc.is_no_such_key() {
                StoreError::NotFound
            } else {
                StoreError::Backend(format!("S3 read {key}: {svc}"))
            }
        })?;
        let mime = resp.content_type().unwrap_or("application/octet-stream").to_string();
        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| StoreError::Backend(format!("S3 read body {key}: {e}")))?
            .into_bytes();
        Ok((mime, bytes))
    }

    async fn exists(&self, token: ImageToken) -> Result<bool, StoreError> {
        let key = Self::key(token);
        match self.client.head_object().bucket(&self.bucket).key(&key).send().await {
            Ok(head) => {
                let last_modified_secs = head_last_modified_secs(&head);
                if stale_for_reuse(last_modified_secs, Utc::now().timestamp(), self.reuse_max_age) {
                    counter!("dwctl_image_normalizer_refresh_total").increment(1);
                    debug!(key, ?last_modified_secs, "image object older than reuse_max_age; re-uploading");
                    return Ok(false);
                }
                Ok(true)
            }
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_not_found() {
                    Ok(false)
                } else {
                    Err(StoreError::Backend(format!("S3 exists {key}: {svc}")))
                }
            }
        }
    }

    fn owns_url(&self, url: &str) -> bool {
        let (Ok(ep), Ok(u)) = (Url::parse(&self.endpoint), Url::parse(url)) else {
            return false;
        };
        if u.scheme() != ep.scheme() {
            return false;
        }
        let (Some(ep_host), Some(u_host)) = (ep.host_str(), u.host_str()) else {
            return false;
        };
        if u_host == ep_host && u.port_or_known_default() == ep.port_or_known_default() {
            return u.path().starts_with(&format!("/{}/images/", self.bucket));
        }
        if u_host == format!("{}.{}", self.bucket, ep_host) {
            return u.path().starts_with("/images/");
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(b: u8) -> ImageToken {
        ImageToken([b; 32], None)
    }

    #[tokio::test]
    async fn memory_store_dedups_puts() {
        let s = MemoryStore::new();
        let first = s.put(tok(1), "image/png", Bytes::from_static(b"hello")).await.unwrap();
        let second = s.put(tok(1), "image/png", Bytes::from_static(b"hello")).await.unwrap();
        assert!(first);
        assert!(!second);
    }

    #[tokio::test]
    async fn memory_store_sign_returns_url_with_token_hex() {
        let s = MemoryStore::new().with_base_url("http://test.local/img");
        s.put(tok(7), "image/png", Bytes::from_static(b"x")).await.unwrap();
        let signed = s.sign(tok(7), Duration::from_secs(60)).await.unwrap();
        let token_hex = tok(7).to_hex();
        assert!(signed.url.contains(&token_hex));
        assert!(signed.url.starts_with("http://test.local/img/"));
        assert!(signed.expires_at > Utc::now());
    }

    #[tokio::test]
    async fn memory_store_sign_missing_object_returns_not_found() {
        let s = MemoryStore::new();
        let err = s.sign(tok(99), Duration::from_secs(60)).await.unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
    }

    #[tokio::test]
    async fn memory_store_exists_round_trip() {
        let s = MemoryStore::new();
        assert!(!s.exists(tok(2)).await.unwrap());
        s.put(tok(2), "image/png", Bytes::from_static(b"x")).await.unwrap();
        assert!(s.exists(tok(2)).await.unwrap());
    }

    #[tokio::test]
    async fn memory_store_read_returns_bytes_and_mime() {
        let s = MemoryStore::new();
        s.put(tok(3), "image/jpeg", Bytes::from_static(b"jpegbytes")).await.unwrap();
        let (mime, bytes) = s.read(tok(3)).await.unwrap();
        assert_eq!(mime, "image/jpeg");
        assert_eq!(bytes.as_ref(), b"jpegbytes");
    }

    #[test]
    fn gcs_key_uses_two_level_prefix() {
        let key = GcsStore::key(tok(0xab));
        assert!(key.starts_with("images/ab/ab/abab"));
    }

    #[test]
    fn gcs_bucket_resource_format() {
        let s = GcsStore::new("my-bucket", "europe-west4");
        assert_eq!(s.bucket_resource(), "projects/_/buckets/my-bucket");
    }

    #[test]
    fn stale_for_reuse_only_when_older_than_max_age() {
        let day = 24 * 60 * 60;
        let now = 1_790_000_000;
        let five_days = Some(Duration::from_secs(5 * day as u64));
        assert!(!stale_for_reuse(Some(now - 100 * day), now, None));
        assert!(!stale_for_reuse(None, now, five_days));
        assert!(!stale_for_reuse(Some(now - 4 * day), now, five_days));
        assert!(stale_for_reuse(Some(now - 5 * day), now, five_days));
        assert!(stale_for_reuse(Some(now - 14 * day), now, five_days));
        assert!(!stale_for_reuse(Some(now + day), now, five_days));
        assert!(!stale_for_reuse(Some(now - 100 * day), now, Some(Duration::from_secs(u64::MAX))));
    }

    #[test]
    fn head_last_modified_maps_to_epoch_seconds() {
        use aws_sdk_s3::operation::head_object::HeadObjectOutput;
        use aws_sdk_s3::primitives::DateTime;

        let with = HeadObjectOutput::builder()
            .last_modified(DateTime::from_secs(1_790_000_000))
            .build();
        assert_eq!(head_last_modified_secs(&with), Some(1_790_000_000));

        let without = HeadObjectOutput::builder().build();
        assert_eq!(head_last_modified_secs(&without), None);
    }

    #[test]
    fn s3_key_uses_two_level_prefix() {
        let key = S3CompatStore::key(tok(0xab));
        assert!(key.starts_with("images/ab/ab/abab"));
    }

    #[test]
    fn copies_of_the_same_image_get_distinct_keys_under_its_prefix() {
        let a = ImageToken::new_unique([0xab; 32]);
        let b = ImageToken::new_unique([0xab; 32]);
        let (ka, kb) = (S3CompatStore::key(a), S3CompatStore::key(b));
        assert_ne!(ka, kb);
        assert!(ka.starts_with(&format!("images/ab/ab/{}.", hex::encode([0xab; 32]))));
        assert_eq!(GcsStore::key(a), ka);
        assert_eq!(S3CompatStore::key(tok(0xab)), format!("images/ab/ab/{}", hex::encode([0xab; 32])));
    }

    #[test]
    fn s3_compat_store_builds_client() {
        let s = S3CompatStore::new(
            "imgs",
            "https://example.r2.cloudflarestorage.com",
            "auto",
            true,
            "AKIDEXAMPLE",
            "secret",
        );
        assert_eq!(s.bucket, "imgs");
    }

    #[test]
    fn s3_owns_url_matches_own_presigned_urls_only() {
        let s = S3CompatStore::new(
            "images-bucket",
            "https://acct.r2.cloudflarestorage.com",
            "auto",
            true,
            "AKIDEXAMPLE",
            "secret",
        );
        let hex = tok(0xab).to_hex();
        let ours = format!(
            "https://acct.r2.cloudflarestorage.com/images-bucket/images/{}/{}/{}?x-id=GetObject&X-Amz-Signature=deadbeef",
            &hex[..2],
            &hex[2..4],
            hex
        );
        assert!(s.owns_url(&ours));
        let spoofed = format!("https://evil.example.com/images-bucket/images/{}/{}/{}", &hex[..2], &hex[2..4], hex);
        assert!(!s.owns_url(&spoofed));
        assert!(!s.owns_url("https://acct.r2.cloudflarestorage.com.evil.com/images-bucket/images/ab/cd/abcd"));
        assert!(!s.owns_url("https://evil.example.com/x?next=/images-bucket/images/ab/cd/abcd"));
        assert!(!s.owns_url("https://example.com/cat.png"));
        assert!(!s.owns_url("data:image/png;base64,AAAA"));
    }

    #[test]
    fn s3_owns_url_matches_virtual_hosted_urls() {
        let s = S3CompatStore::new(
            "images-bucket",
            "https://acct.r2.cloudflarestorage.com",
            "auto",
            false,
            "AKIDEXAMPLE",
            "secret",
        );
        assert!(s.owns_url("https://images-bucket.acct.r2.cloudflarestorage.com/images/ab/cd/abcd?X-Amz-Signature=deadbeef"));
        assert!(!s.owns_url("https://other.acct.r2.cloudflarestorage.com/images/ab/cd/abcd"));
        assert!(!s.owns_url("https://images-bucket.acct.r2.cloudflarestorage.com.evil.com/images/ab/cd/abcd"));
    }

    #[test]
    fn s3_owns_url_endpoint_trailing_slash_trimmed() {
        let s = S3CompatStore::new(
            "imgs",
            "https://acct.r2.cloudflarestorage.com/",
            "auto",
            true,
            "AKIDEXAMPLE",
            "secret",
        );
        assert_eq!(s.endpoint, "https://acct.r2.cloudflarestorage.com");
        assert!(s.owns_url("https://acct.r2.cloudflarestorage.com/imgs/images/ab/cd/abcd"));
    }

    #[test]
    fn memory_store_owns_url() {
        let s = MemoryStore::new().with_base_url("http://test.local/img");
        assert!(s.owns_url("http://test.local/img/abcd?expires=123"));
        assert!(!s.owns_url("https://example.com/cat.png"));
        assert!(!s.owns_url("http://test.local/img2/abcd"));
        assert!(!s.owns_url("http://test.local/img.evil/abcd"));
        assert!(!s.owns_url("https://test.local/img/abcd"));
        assert!(!s.owns_url("http://evil.test/img/abcd"));
    }

    #[test]
    fn gcs_owns_url_matches_bucket_and_key_prefix() {
        let s = GcsStore::new("images-bucket", "europe-west4");
        assert!(s.owns_url("https://storage.googleapis.com/images-bucket/images/ab/cd/abcd?X-Goog-Signature=x"));
        assert!(!s.owns_url("https://storage.googleapis.com/other-bucket/images/ab/cd/abcd"));
        assert!(!s.owns_url("https://example.com/cat.png"));
        assert!(!s.owns_url("https://storage.googleapis.com.evil.com/images-bucket/images/ab/cd/abcd"));
        assert!(!s.owns_url("https://evil.example.com/x?u=/images-bucket/images/ab/cd/abcd"));
        assert!(!s.owns_url("http://storage.googleapis.com/images-bucket/images/ab/cd/abcd"));
    }

    #[tokio::test]
    async fn s3_presign_caps_ttl_at_seven_days() {
        let s = S3CompatStore::new(
            "imgs",
            "https://example.r2.cloudflarestorage.com",
            "auto",
            true,
            "AKIDEXAMPLE",
            "secret",
        );
        let signed = s
            .sign(tok(5), Duration::from_secs(30 * 24 * 60 * 60))
            .await
            .expect("presign with clamped ttl should succeed");
        assert!(signed.url.starts_with("https://example.r2.cloudflarestorage.com"));
        assert!(signed.expires_at <= Utc::now() + ChronoDuration::days(7));
    }
}
