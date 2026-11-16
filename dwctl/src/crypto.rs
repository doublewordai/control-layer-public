
use base64::{Engine as _, engine::general_purpose};
use rand::prelude::RngExt;
use rand::rng;
use sha2::{Digest, Sha256};

pub const API_KEY_PREFIX_LEN: usize = 8;

pub fn generate_api_key() -> String {
    let mut key_bytes = [0u8; 32];
    rng().fill(&mut key_bytes);

    format!("sk-{}", general_purpose::URL_SAFE_NO_PAD.encode(key_bytes))
}

pub type ApiKeyHash = [u8; 32];

pub fn hash_api_key(secret: &str) -> ApiKeyHash {
    Sha256::digest(secret.as_bytes()).into()
}

pub fn api_key_prefix(secret: &str) -> String {
    secret.chars().take(API_KEY_PREFIX_LEN).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_generate_api_key_format() {
        let key = generate_api_key();

        assert!(key.starts_with("sk-"));

        assert_eq!(key.len(), 46);

        let key_part = &key[3..];
        assert!(key_part.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn test_generate_api_key_uniqueness() {
        let mut keys = HashSet::new();

        for _ in 0..1000 {
            let key = generate_api_key();
            assert!(keys.insert(key), "Generated duplicate API key");
        }
    }

    #[test]
    fn test_hash_api_key_is_sha256() {
        assert_eq!(
            hex::encode(hash_api_key("abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let key = generate_api_key();
        assert_eq!(hash_api_key(&key), hash_api_key(&key));
        assert_ne!(hash_api_key(&key), hash_api_key(&generate_api_key()));
    }

    #[test]
    fn test_api_key_prefix() {
        let key = generate_api_key();
        let prefix = api_key_prefix(&key);
        assert_eq!(prefix.len(), API_KEY_PREFIX_LEN);
        assert!(prefix.starts_with("sk-"));
        assert!(key.starts_with(&prefix));
        assert_eq!(api_key_prefix("sk-ab"), "sk-ab");
    }

    #[test]
    fn test_generate_api_key_no_padding() {
        let key = generate_api_key();

        assert!(!key.contains('='));
    }
}
