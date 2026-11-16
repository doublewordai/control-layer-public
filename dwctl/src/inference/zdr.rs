
use uuid::Uuid;

use crate::keystore::{self, KeystoreError};

const ZDR_BODY_PREFIX: &str = "dwzdr1:";

pub const ZDR_MARKER_KEY: &str = "zdr";

pub const ZDR_MARKER_HEADER: &str = "x-fusillade-batch-zdr";

pub fn is_zdr_request(zdr_cache: &crate::sync::key_policy::KeyPolicyCache, key: Option<&crate::crypto::ApiKeyHash>) -> bool {
    key.is_some_and(|key| zdr_cache.is_zdr(key))
}

pub struct FlexKeys {
    pub request: [u8; 32],
    pub response: [u8; 32],
}

impl FlexKeys {
    pub fn generate() -> Self {
        Self {
            request: keystore::generate_key(),
            response: keystore::generate_key(),
        }
    }
}

pub async fn prepare_flex_submit(
    keystore: &crate::keystore::Keystore,
    request_id: &Uuid,
    keys: &FlexKeys,
    request_value: &mut serde_json::Value,
) -> Result<String, KeystoreError> {
    if let Some(obj) = request_value.as_object_mut() {
        obj.shift_remove("service_tier");
        obj.shift_remove("background");
    }
    let body = encrypt_body(&keys.request, &request_value.to_string())?;
    keystore.put(&key_id(request_id, KeyKind::Request), &keys.request, None).await?;
    keystore.put(&key_id(request_id, KeyKind::Response), &keys.response, None).await?;
    Ok(body)
}

pub fn is_zdr_body(stored: &str) -> bool {
    stored.starts_with(ZDR_BODY_PREFIX)
}

pub fn encrypt_body(key: &[u8], plaintext: &str) -> Result<String, KeystoreError> {
    Ok(format!("{ZDR_BODY_PREFIX}{}", keystore::encrypt_value(key, plaintext)?))
}

pub fn decrypt_body(key: &[u8], stored: &str) -> Result<String, KeystoreError> {
    let envelope = stored.strip_prefix(ZDR_BODY_PREFIX).ok_or(KeystoreError::MalformedEnvelope)?;
    keystore::decrypt_value(key, envelope)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    Request,
    Response,
}

impl KeyKind {
    fn suffix(self) -> &'static str {
        match self {
            KeyKind::Request => "req",
            KeyKind::Response => "resp",
        }
    }
}

pub fn key_id(request_id: &Uuid, kind: KeyKind) -> String {
    format!("zdr:{}:{}", request_id, kind.suffix())
}

#[derive(Debug, PartialEq, Eq)]
pub enum DecryptOutcome {
    Unchanged,
    Decrypted(String),
    Gone,
}

pub async fn decrypt_response_body(
    keystore: &crate::keystore::Keystore,
    request_id: &Uuid,
    response_body: Option<&str>,
) -> Result<DecryptOutcome, KeystoreError> {
    let Some(body) = response_body else {
        return Ok(DecryptOutcome::Unchanged);
    };
    if !is_zdr_body(body) {
        return Ok(DecryptOutcome::Unchanged);
    }
    let response_key_id = key_id(request_id, KeyKind::Response);
    match keystore.take(&response_key_id).await? {
        Some(key) => {
            let plaintext = decrypt_body(&key, body)?;
            Ok(DecryptOutcome::Decrypted(plaintext))
        }
        None => Ok(DecryptOutcome::Gone),
    }
}

pub struct ZdrResponseEncryptor {
    keystore: crate::keystore::Keystore,
}

impl ZdrResponseEncryptor {
    pub fn new(keystore: crate::keystore::Keystore) -> Self {
        Self { keystore }
    }
}

#[async_trait::async_trait]
impl fusillade::ResponseTransformer for ZdrResponseEncryptor {
    async fn transform(&self, request: &fusillade::RequestData, body: &str) -> fusillade::Result<String> {
        let is_zdr = request.batch_metadata.get(ZDR_MARKER_KEY).is_some_and(|v| v == "1");
        if !is_zdr {
            return Ok(body.to_string());
        }
        let request_id = &request.id.0;
        match self.keystore.get(&key_id(request_id, KeyKind::Response)).await {
            Ok(Some(key)) => {
                let encrypted = encrypt_body(&key, body)
                    .map_err(|e| fusillade::FusilladeError::Other(anyhow::anyhow!("ZDR response encrypt failed: {e}")))?;
                if let Err(e) = self.keystore.delete(&key_id(request_id, KeyKind::Request)).await {
                    tracing::warn!(error = %e, "ZDR request key shred-on-terminal failed; relying on TTL");
                }
                Ok(encrypted)
            }
            Ok(None) => {
                tracing::warn!(request_id = %request_id, "ZDR response key gone at persist; storing blank (response not retained)");
                Ok(String::new())
            }
            Err(e) => Err(fusillade::FusilladeError::Other(anyhow::anyhow!(
                "ZDR keystore error during response encrypt: {e}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_ids_are_distinct_per_kind() {
        let id = Uuid::nil();
        let req = key_id(&id, KeyKind::Request);
        let resp = key_id(&id, KeyKind::Response);
        assert_ne!(req, resp);
        assert!(req.ends_with(":req"));
        assert!(resp.ends_with(":resp"));
        assert!(req.starts_with("zdr:"));
    }

    #[test]
    fn body_envelope_is_self_describing() {
        let key = keystore::generate_key();
        let plaintext = r#"{"model":"gpt","input":"hello"}"#;
        let envelope = encrypt_body(&key, plaintext).unwrap();
        assert!(!is_zdr_body(plaintext));
        assert!(is_zdr_body(&envelope));
        assert_eq!(decrypt_body(&key, &envelope).unwrap(), plaintext);
    }

    #[test]
    fn decrypt_body_rejects_unprefixed() {
        let key = keystore::generate_key();
        assert!(matches!(decrypt_body(&key, "{}"), Err(KeystoreError::MalformedEnvelope)));
    }

    #[test]
    fn is_zdr_request_reads_the_key_map() {
        let cache = crate::sync::key_policy::KeyPolicyCache::from_pairs([("sk-on".to_string(), true), ("sk-off".to_string(), false)]);
        assert!(is_zdr_request(&cache, Some(&crate::crypto::hash_api_key("sk-on"))));
        assert!(!is_zdr_request(&cache, Some(&crate::crypto::hash_api_key("sk-off"))));
        assert!(!is_zdr_request(&cache, Some(&crate::crypto::hash_api_key("sk-unknown"))));
        assert!(!is_zdr_request(&cache, None));
    }
}
