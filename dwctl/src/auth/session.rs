
use chrono::Utc;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};

use crate::{api::models::users::CurrentUser, config::Config, errors::Error, types::UserId};

#[derive(Debug, Serialize, Deserialize)]
pub struct SessionClaims {
    pub sub: UserId,
    pub exp: i64,
    pub iat: i64,
}

impl SessionClaims {
    pub fn new(user: &CurrentUser, config: &Config) -> Self {
        let now = Utc::now();
        let exp = now + config.auth.security.jwt_expiry;

        Self {
            sub: user.id,
            exp: exp.timestamp(),
            iat: now.timestamp(),
        }
    }

    pub fn user_id(&self) -> UserId {
        self.sub
    }
}

pub fn create_session_token(user: &CurrentUser, config: &Config) -> Result<String, Error> {
    let claims = SessionClaims::new(user, config);
    let secret_key = config.secret_key.as_ref().ok_or_else(|| Error::Internal {
        operation: "JWT sessions: secret_key is required".to_string(),
    })?;

    let key = EncodingKey::from_secret(secret_key.as_bytes());
    encode(&Header::default(), &claims, &key).map_err(|e| Error::Internal {
        operation: format!("create JWT: {e}"),
    })
}

pub fn verify_session_token(token: &str, config: &Config) -> Result<UserId, Error> {
    let secret_key = config.secret_key.as_ref().ok_or_else(|| Error::Internal {
        operation: "JWT sessions: secret_key is required".to_string(),
    })?;

    let key = DecodingKey::from_secret(secret_key.as_bytes());
    let validation = Validation::default();

    let token_data = decode::<SessionClaims>(token, &key, &validation).map_err(|e| match e.kind() {
        jsonwebtoken::errors::ErrorKind::InvalidToken
        | jsonwebtoken::errors::ErrorKind::InvalidSignature
        | jsonwebtoken::errors::ErrorKind::ExpiredSignature
        | jsonwebtoken::errors::ErrorKind::MissingRequiredClaim(_)
        | jsonwebtoken::errors::ErrorKind::InvalidClaimFormat(_)
        | jsonwebtoken::errors::ErrorKind::InvalidIssuer
        | jsonwebtoken::errors::ErrorKind::InvalidAudience
        | jsonwebtoken::errors::ErrorKind::InvalidSubject
        | jsonwebtoken::errors::ErrorKind::ImmatureSignature
        | jsonwebtoken::errors::ErrorKind::Base64(_)
        | jsonwebtoken::errors::ErrorKind::InvalidAlgorithm => Error::Unauthenticated { message: None },

        jsonwebtoken::errors::ErrorKind::InvalidEcdsaKey
        | jsonwebtoken::errors::ErrorKind::InvalidEddsaKey
        | jsonwebtoken::errors::ErrorKind::InvalidRsaKey(_)
        | jsonwebtoken::errors::ErrorKind::RsaFailedSigning
        | jsonwebtoken::errors::ErrorKind::Signing(_)
        | jsonwebtoken::errors::ErrorKind::Provider(_)
        | jsonwebtoken::errors::ErrorKind::InvalidAlgorithmName
        | jsonwebtoken::errors::ErrorKind::InvalidKeyFormat
        | jsonwebtoken::errors::ErrorKind::MissingAlgorithm
        | jsonwebtoken::errors::ErrorKind::Json(_)
        | jsonwebtoken::errors::ErrorKind::Utf8(_) => Error::Internal {
            operation: format!("JWT verification: {e}"),
        },

        _ => Error::Internal {
            operation: format!("JWT verification (unknown error): {e}"),
        },
    })?;

    Ok(token_data.claims.user_id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::models::users::Role,
        config::{AuthConfig, SecurityConfig},
    };
    use std::time::Duration;
    use uuid::Uuid;

    fn create_test_config() -> Config {
        Config {
            secret_key: Some("test-secret-key-for-jwt".to_string()),
            auth: AuthConfig {
                security: SecurityConfig {
                    jwt_expiry: Duration::from_secs(3600),
                    cors: crate::config::CorsConfig::default(),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn create_test_user() -> CurrentUser {
        CurrentUser {
            auth_method: crate::api::models::users::AuthMethod::TrustedSso,
            id: Uuid::new_v4(),
            email: "test@example.com".to_string(),
            username: "testuser".to_string(),
            roles: vec![Role::StandardUser],
            is_admin: false,
            display_name: Some("Test User".to_string()),
            avatar_url: None,
            payment_provider_id: None,
            organizations: vec![],
            active_organization: None,
            api_key_id: None,
        }
    }

    #[test]
    fn test_create_and_verify_session_token() {
        let config = create_test_config();
        let user = create_test_user();

        let token = create_session_token(&user, &config).unwrap();
        assert!(!token.is_empty());

        let user_id = verify_session_token(&token, &config).unwrap();
        assert_eq!(user_id, user.id);
    }

    #[test]
    fn test_verify_invalid_token() {
        let config = create_test_config();

        let result = verify_session_token("invalid.token.here", &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_token_wrong_secret() {
        let mut config = create_test_config();
        let user = create_test_user();

        let token = create_session_token(&user, &config).unwrap();

        config.secret_key = Some("different-secret".to_string());
        let result = verify_session_token(&token, &config);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Unauthenticated { .. }));
    }

    #[test]
    fn test_verify_expired_token() {
        let config = create_test_config();
        let user = create_test_user();

        let now = Utc::now();
        let claims = SessionClaims {
            sub: user.id,
            exp: (now - chrono::Duration::seconds(3600)).timestamp(),
            iat: now.timestamp(),
        };

        let secret_key = config.secret_key.as_ref().unwrap();
        let key = EncodingKey::from_secret(secret_key.as_bytes());
        let token = encode(&Header::default(), &claims, &key).unwrap();

        let result = verify_session_token(&token, &config);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Unauthenticated { .. }));
    }

    #[test]
    fn test_verify_malformed_token() {
        let config = create_test_config();

        let malformed_tokens = vec!["not.a.token", "invalid", "", "too.many.parts.in.this.token"];

        for token in malformed_tokens {
            let result = verify_session_token(token, &config);
            assert!(result.is_err());
            assert!(
                matches!(result.unwrap_err(), Error::Unauthenticated { .. }),
                "Expected Unauthenticated error for token: {}",
                token
            );
        }
    }

    #[test]
    fn test_jwt_only_contains_user_id() {
        let config = create_test_config();
        let user = create_test_user();

        let token = create_session_token(&user, &config).unwrap();

        let secret_key = config.secret_key.as_ref().unwrap();
        let key = DecodingKey::from_secret(secret_key.as_bytes());
        let token_data = decode::<SessionClaims>(&token, &key, &Validation::default()).unwrap();

        assert_eq!(token_data.claims.sub, user.id);
        assert!(token_data.claims.exp > 0);
        assert!(token_data.claims.iat > 0);
    }
}
