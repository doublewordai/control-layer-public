
use std::collections::HashMap;
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose};
use deadpool_redis::{Config as RedisConfig, Pool, Runtime};
use rand::prelude::RngExt;
use rand::rng;
use serde::{Deserialize, Serialize};

use crate::encryption::{self, EncryptionError};

const VALUE_ENVELOPE_VERSION: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum KeystoreError {
    #[error("crypto error: {0}")]
    Crypto(#[from] EncryptionError),

    #[error("unknown wrap-key id {0:?}; cannot unwrap (key retired too early?)")]
    UnknownWrapKeyId(String),

    #[error("malformed wrapped value in keystore")]
    MalformedWrappedValue,

    #[error("malformed value envelope")]
    MalformedEnvelope,

    #[error("keystore unreachable: {0}")]
    Unreachable(String),

    #[error("keystore configuration error: {0}")]
    Config(String),
}

impl KeystoreError {
    pub fn is_unreachable(&self) -> bool {
        matches!(self, KeystoreError::Unreachable(_))
    }
}

pub fn generate_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    rng().fill(&mut key);
    key
}

#[derive(Clone)]
pub struct WrapKeyring {
    current_id: String,
    keys: HashMap<String, Vec<u8>>,
}

impl std::fmt::Debug for WrapKeyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WrapKeyring")
            .field("current_id", &self.current_id)
            .field("key_ids", &self.keys.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl WrapKeyring {
    pub fn new(current_id: String, keys: HashMap<String, Vec<u8>>) -> Result<Self, KeystoreError> {
        if !keys.contains_key(&current_id) {
            return Err(KeystoreError::Config(format!(
                "current wrap-key id {current_id:?} is not present in the keyring"
            )));
        }
        if let Some(id) = keys.keys().find(|id| id.len() > u8::MAX as usize) {
            return Err(KeystoreError::Config(format!("wrap-key id {id:?} is too long (max 255 bytes)")));
        }
        Ok(Self { current_id, keys })
    }

    pub fn wrap(&self, value: &[u8]) -> Result<Vec<u8>, KeystoreError> {
        let wrap_key = self
            .keys
            .get(&self.current_id)
            .ok_or_else(|| KeystoreError::Config("current wrap key missing from keyring".into()))?;
        let sealed = encryption::encrypt(wrap_key, value)?;
        let id = self.current_id.as_bytes();
        let mut out = Vec::with_capacity(1 + id.len() + sealed.len());
        out.push(id.len() as u8);
        out.extend_from_slice(id);
        out.extend_from_slice(&sealed);
        Ok(out)
    }

    pub fn unwrap(&self, blob: &[u8]) -> Result<Vec<u8>, KeystoreError> {
        let (id, sealed) = split_wrapped(blob)?;
        let wrap_key = self.keys.get(id).ok_or_else(|| KeystoreError::UnknownWrapKeyId(id.to_string()))?;
        Ok(encryption::decrypt(wrap_key, sealed)?)
    }

    pub fn is_current(&self, blob: &[u8]) -> bool {
        split_wrapped(blob).is_ok_and(|(id, _)| id == self.current_id)
    }
}

fn split_wrapped(blob: &[u8]) -> Result<(&str, &[u8]), KeystoreError> {
    let (&id_len, rest) = blob.split_first().ok_or(KeystoreError::MalformedWrappedValue)?;
    let id_len = id_len as usize;
    if rest.len() < id_len {
        return Err(KeystoreError::MalformedWrappedValue);
    }
    let (id_bytes, sealed) = rest.split_at(id_len);
    let id = std::str::from_utf8(id_bytes).map_err(|_| KeystoreError::MalformedWrappedValue)?;
    Ok((id, sealed))
}

pub fn encrypt_value(key: &[u8], plaintext: &str) -> Result<String, KeystoreError> {
    let blob = encryption::encrypt(key, plaintext.as_bytes())?;
    let mut framed = Vec::with_capacity(1 + blob.len());
    framed.push(VALUE_ENVELOPE_VERSION);
    framed.extend_from_slice(&blob);
    Ok(general_purpose::STANDARD.encode(framed))
}

pub fn decrypt_value(key: &[u8], ciphertext: &str) -> Result<String, KeystoreError> {
    let framed = general_purpose::STANDARD
        .decode(ciphertext)
        .map_err(|_| KeystoreError::MalformedEnvelope)?;
    let (&version, blob) = framed.split_first().ok_or(KeystoreError::MalformedEnvelope)?;
    if version != VALUE_ENVELOPE_VERSION {
        return Err(KeystoreError::MalformedEnvelope);
    }
    let plaintext = encryption::decrypt(key, blob)?;
    String::from_utf8(plaintext).map_err(|_| KeystoreError::MalformedEnvelope)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeystoreConfig {
    pub redis_url: String,
    #[serde(default = "default_keystore_ttl_seconds")]
    pub default_ttl_seconds: u64,
    pub current_wrap_key_id: String,
    pub wrap_keys: HashMap<String, String>,
}

fn default_keystore_ttl_seconds() -> u64 {
    7200
}

#[derive(Clone)]
pub struct Keystore {
    pool: Pool,
    keyring: WrapKeyring,
    default_ttl: Duration,
}

impl Keystore {
    pub fn from_config(cfg: &KeystoreConfig) -> Result<Self, KeystoreError> {
        let mut keys = HashMap::with_capacity(cfg.wrap_keys.len());
        for (id, secret) in &cfg.wrap_keys {
            if secret.trim().is_empty() {
                return Err(KeystoreError::Config(format!(
                    "wrap key {id:?} is empty or whitespace; refusing to derive a key from an empty secret \
                     (SHA-256(\"\") is a publicly-known constant and would disable crypto-shredding)"
                )));
            }
            keys.insert(id.clone(), encryption::derive_encryption_key(secret));
        }
        let keyring = WrapKeyring::new(cfg.current_wrap_key_id.clone(), keys)?;
        let pool = RedisConfig::from_url(cfg.redis_url.clone())
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| KeystoreError::Config(format!("failed to create redis pool: {e}")))?;
        Ok(Self {
            pool,
            keyring,
            default_ttl: Duration::from_secs(cfg.default_ttl_seconds),
        })
    }

    pub fn wrap(&self, value: &[u8]) -> Result<Vec<u8>, KeystoreError> {
        self.keyring.wrap(value)
    }

    pub fn unwrap(&self, blob: &[u8]) -> Result<Vec<u8>, KeystoreError> {
        self.keyring.unwrap(blob)
    }

    pub fn is_current(&self, blob: &[u8]) -> bool {
        self.keyring.is_current(blob)
    }

    pub fn default_ttl(&self) -> Duration {
        self.default_ttl
    }

    pub async fn put(&self, id: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), KeystoreError> {
        let wrapped = self.keyring.wrap(value)?;
        let secs = ttl.unwrap_or(self.default_ttl).as_secs();
        let mut conn = self.pool.get().await.map_err(|e| KeystoreError::Unreachable(e.to_string()))?;
        deadpool_redis::redis::cmd("SET")
            .arg(id)
            .arg(wrapped)
            .arg("EX")
            .arg(secs)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| KeystoreError::Unreachable(e.to_string()))?;
        Ok(())
    }

    pub async fn get(&self, id: &str) -> Result<Option<Vec<u8>>, KeystoreError> {
        let mut conn = self.pool.get().await.map_err(|e| KeystoreError::Unreachable(e.to_string()))?;
        let wrapped: Option<Vec<u8>> = deadpool_redis::redis::cmd("GET")
            .arg(id)
            .query_async(&mut conn)
            .await
            .map_err(|e| KeystoreError::Unreachable(e.to_string()))?;
        match wrapped {
            None => Ok(None),
            Some(blob) => Ok(Some(self.keyring.unwrap(&blob)?)),
        }
    }

    pub async fn take(&self, id: &str) -> Result<Option<Vec<u8>>, KeystoreError> {
        let mut conn = self.pool.get().await.map_err(|e| KeystoreError::Unreachable(e.to_string()))?;
        let wrapped: Option<Vec<u8>> = deadpool_redis::redis::cmd("GETDEL")
            .arg(id)
            .query_async(&mut conn)
            .await
            .map_err(|e| KeystoreError::Unreachable(e.to_string()))?;
        match wrapped {
            None => Ok(None),
            Some(blob) => Ok(Some(self.keyring.unwrap(&blob)?)),
        }
    }

    pub async fn delete(&self, id: &str) -> Result<(), KeystoreError> {
        let mut conn = self.pool.get().await.map_err(|e| KeystoreError::Unreachable(e.to_string()))?;
        deadpool_redis::redis::cmd("DEL")
            .arg(id)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| KeystoreError::Unreachable(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn test_keystore_config(current_id: &str, wrap_keys: &[(&str, &str)]) -> KeystoreConfig {
    KeystoreConfig {
        redis_url: "redis://127.0.0.1:1".into(),
        default_ttl_seconds: 300,
        current_wrap_key_id: current_id.into(),
        wrap_keys: wrap_keys.iter().map(|(id, secret)| (id.to_string(), secret.to_string())).collect(),
    }
}

#[cfg(test)]
pub(crate) fn test_keystore(current_id: &str, wrap_keys: &[(&str, &str)]) -> Keystore {
    Keystore::from_config(&test_keystore_config(current_id, wrap_keys)).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyring() -> WrapKeyring {
        let mut keys = HashMap::new();
        keys.insert("k1".to_string(), encryption::derive_encryption_key("secret-one"));
        keys.insert("k2".to_string(), encryption::derive_encryption_key("secret-two"));
        WrapKeyring::new("k2".to_string(), keys).unwrap()
    }

    #[test]
    fn wrap_unwrap_roundtrip() {
        let kr = keyring();
        let value = generate_key();
        let wrapped = kr.wrap(&value).unwrap();
        assert_ne!(wrapped.as_slice(), value.as_slice());
        assert_eq!(kr.unwrap(&wrapped).unwrap(), value.to_vec());
    }

    #[test]
    fn unwrap_with_old_key_after_rotation_still_works() {
        let sealed = keyring().wrap(&generate_key()).unwrap();
        let mut keys = HashMap::new();
        keys.insert("k2".to_string(), encryption::derive_encryption_key("secret-two"));
        keys.insert("k3".to_string(), encryption::derive_encryption_key("secret-three"));
        let rotated = WrapKeyring::new("k3".to_string(), keys).unwrap();
        assert!(rotated.unwrap(&sealed).is_ok());
    }

    #[test]
    fn is_current_tracks_the_sealing_key() {
        let sealed = keyring().wrap(&generate_key()).unwrap();
        assert!(keyring().is_current(&sealed));
        let mut keys = HashMap::new();
        keys.insert("k2".to_string(), encryption::derive_encryption_key("secret-two"));
        keys.insert("k3".to_string(), encryption::derive_encryption_key("secret-three"));
        let rotated = WrapKeyring::new("k3".to_string(), keys).unwrap();
        assert!(!rotated.is_current(&sealed));
        assert!(!rotated.is_current(&[]));
    }

    #[test]
    fn unwrap_after_old_key_retired_errors() {
        let sealed = keyring().wrap(&generate_key()).unwrap();
        let mut keys = HashMap::new();
        keys.insert("k3".to_string(), encryption::derive_encryption_key("secret-three"));
        let retired = WrapKeyring::new("k3".to_string(), keys).unwrap();
        assert!(matches!(retired.unwrap(&sealed).unwrap_err(), KeystoreError::UnknownWrapKeyId(_)));
    }

    #[test]
    fn value_roundtrip() {
        let key = generate_key();
        let value = r#"{"model":"gpt","input":"hello"}"#;
        let ciphertext = encrypt_value(&key, value).unwrap();
        assert_ne!(ciphertext, value);
        assert_eq!(decrypt_value(&key, &ciphertext).unwrap(), value);
    }

    #[test]
    fn value_decrypt_with_wrong_key_fails() {
        let ciphertext = encrypt_value(&generate_key(), "secret prompt").unwrap();
        assert!(decrypt_value(&generate_key(), &ciphertext).is_err());
    }

    #[test]
    fn current_id_must_be_in_keyring() {
        let mut keys = HashMap::new();
        keys.insert("a".to_string(), encryption::derive_encryption_key("x"));
        assert!(matches!(
            WrapKeyring::new("missing".to_string(), keys).unwrap_err(),
            KeystoreError::Config(_)
        ));
    }

    fn keystore_config(secret: &str) -> KeystoreConfig {
        KeystoreConfig {
            redis_url: "redis://localhost:6379".into(),
            default_ttl_seconds: 300,
            current_wrap_key_id: "dev-1".into(),
            wrap_keys: HashMap::from([("dev-1".into(), secret.into())]),
        }
    }

    #[test]
    fn from_config_rejects_empty_wrap_key() {
        let cfg = keystore_config("");
        assert!(matches!(Keystore::from_config(&cfg), Err(KeystoreError::Config(_))));
    }

    #[test]
    fn from_config_rejects_whitespace_only_wrap_key() {
        for ws in [" ", "\t", "\n", "   \t\n "] {
            let cfg = keystore_config(ws);
            assert!(
                matches!(Keystore::from_config(&cfg), Err(KeystoreError::Config(_))),
                "whitespace-only secret {ws:?} should be rejected",
            );
        }
    }

    #[test]
    fn from_config_accepts_nonempty_wrap_key() {
        let cfg = keystore_config("a-real-secret");
        let store = Keystore::from_config(&cfg).unwrap();
        let key = store.keyring.keys.get("dev-1").unwrap();
        assert_eq!(key.as_slice(), encryption::derive_encryption_key("a-real-secret").as_slice());
        assert_eq!(store.default_ttl(), Duration::from_secs(300));
    }
}
