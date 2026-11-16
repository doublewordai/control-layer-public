
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub const SECRET_PREFIX: &str = "whsec_";

pub fn generate_secret() -> String {
    use rand::Rng;

    let mut secret_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut secret_bytes);

    format!("{}{}", SECRET_PREFIX, BASE64_STANDARD.encode(secret_bytes))
}

pub fn decode_secret(secret: &str) -> Option<Vec<u8>> {
    let encoded = secret.strip_prefix(SECRET_PREFIX)?;
    BASE64_STANDARD.decode(encoded).ok()
}

pub fn sign_payload(msg_id: &str, timestamp: i64, payload: &str, secret: &str) -> Option<String> {
    let secret_bytes = decode_secret(secret)?;

    let signed_content = format!("{}.{}.{}", msg_id, timestamp, payload);

    let mut mac = HmacSha256::new_from_slice(&secret_bytes).ok()?;
    mac.update(signed_content.as_bytes());
    let signature = mac.finalize().into_bytes();

    Some(format!("v1,{}", BASE64_STANDARD.encode(signature)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_secret() {
        let secret = generate_secret();
        assert!(secret.starts_with(SECRET_PREFIX));

        let decoded = decode_secret(&secret);
        assert!(decoded.is_some());
        assert_eq!(decoded.unwrap().len(), 32);
    }

    #[test]
    fn test_decode_secret_invalid_prefix() {
        assert!(decode_secret("invalid_secret").is_none());
    }

    #[test]
    fn test_decode_secret_invalid_base64() {
        assert!(decode_secret("whsec_not-valid-base64!!!").is_none());
    }

    #[test]
    fn test_sign_payload() {
        let secret = generate_secret();
        let msg_id = "msg_123";
        let timestamp = 1704067200;
        let payload = r#"{"type":"batch.completed","data":{}}"#;

        let signature = sign_payload(msg_id, timestamp, payload, &secret).expect("should sign");
        assert!(signature.starts_with("v1,"));
    }

    #[test]
    fn test_sign_payload_deterministic() {
        let secret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
        let msg_id = "msg_p5jXN8AQM9LWM0D4loKWxJek";
        let timestamp = 1614265330;
        let payload = r#"{"test": 2432232314}"#;

        let sig1 = sign_payload(msg_id, timestamp, payload, secret).expect("should sign");
        let sig2 = sign_payload(msg_id, timestamp, payload, secret).expect("should sign");
        assert_eq!(sig1, sig2);
    }
}
