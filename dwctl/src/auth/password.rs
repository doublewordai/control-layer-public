
use argon2::{
    Algorithm, Argon2, Params, Version,
    password_hash::{PasswordHasher, PasswordVerifier, phc::PasswordHash},
};
use base64::{Engine as _, engine::general_purpose};
use rand::prelude::RngExt;
use rand::rng;

use crate::errors::Error;

#[derive(Debug, Clone, Copy)]
pub struct Argon2Params {
    pub memory_kib: u32,
    pub iterations: u32,
    pub parallelism: u32,
}

impl Argon2Params {
    fn to_argon2(self) -> Result<Argon2<'static>, Error> {
        let params = Params::new(self.memory_kib, self.iterations, self.parallelism, None).map_err(|e| Error::Internal {
            operation: format!("create argon2 params: {e}"),
        })?;

        Ok(Argon2::new(Algorithm::Argon2id, Version::V0x13, params))
    }
}

impl Default for Argon2Params {
    fn default() -> Self {
        Self {
            memory_kib: 19456,
            iterations: 2,
            parallelism: 1,
        }
    }
}

pub fn hash_string_with_params(input: &str, params: Option<Argon2Params>) -> Result<String, Error> {
    let argon2 = if let Some(p) = params {
        p.to_argon2()?
    } else {
        Argon2Params::default().to_argon2()?
    };

    let hash = argon2.hash_password(input.as_bytes()).map_err(|e| Error::Internal {
        operation: format!("hash string: {e}"),
    })?;

    Ok(hash.to_string())
}

pub fn verify_string(input: &str, hash: &str) -> Result<bool, Error> {
    let parsed_hash = PasswordHash::new(hash).map_err(|e| Error::Internal {
        operation: format!("parse hash: {e}"),
    })?;

    let argon2 = Argon2::default();
    Ok(argon2.verify_password(input.as_bytes(), &parsed_hash).is_ok())
}

pub fn generate_reset_token() -> String {
    let mut token_bytes = [0u8; 32];
    rng().fill(&mut token_bytes);

    general_purpose::URL_SAFE_NO_PAD.encode(token_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_hashing() {
        let input = "test_password_123";
        let hash = hash_string_with_params(input, None).unwrap();

        assert!(!hash.is_empty());

        assert!(verify_string(input, &hash).unwrap());

        assert!(!verify_string("wrong_password", &hash).unwrap());
    }

    #[test]
    fn test_different_inputs_different_hashes() {
        let input1 = "password1";
        let input2 = "password2";

        let hash1 = hash_string_with_params(input1, None).unwrap();
        let hash2 = hash_string_with_params(input2, None).unwrap();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_same_input_different_hashes() {
        let input = "same_password";

        let hash1 = hash_string_with_params(input, None).unwrap();
        let hash2 = hash_string_with_params(input, None).unwrap();

        assert_ne!(hash1, hash2);

        assert!(verify_string(input, &hash1).unwrap());
        assert!(verify_string(input, &hash2).unwrap());
    }

    #[test]
    fn test_generate_reset_token() {
        let token1 = generate_reset_token();
        let token2 = generate_reset_token();

        assert_ne!(token1, token2);

        assert_eq!(token1.len(), 43);
        assert_eq!(token2.len(), 43);

        assert!(token1.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        assert!(token2.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));

        assert!(!token1.contains('='));
        assert!(!token2.contains('='));
    }
}
