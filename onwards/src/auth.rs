use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use subtle::ConstantTimeEq;

const HASHED_KEY_PREFIX: &str = "sha256:";

#[derive(Clone, Copy)]
pub struct KeyHash([u8; 32]);

impl KeyHash {
    pub fn of(key: &str) -> Self {
        Self(Sha256::digest(key.as_bytes()).into())
    }

    pub const fn from_bytes(hash: [u8; 32]) -> Self {
        Self(hash)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn to_hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn parse_hashed(s: &str) -> Option<Self> {
        let hex = s.strip_prefix(HASHED_KEY_PREFIX)?;
        if hex.len() != 64 || !hex.is_ascii() {
            return None;
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
        }
        Some(Self(out))
    }
}

impl From<&str> for KeyHash {
    fn from(key: &str) -> Self {
        Self::of(key)
    }
}

impl From<String> for KeyHash {
    fn from(key: String) -> Self {
        Self::of(&key)
    }
}

impl TryFrom<&[u8]> for KeyHash {
    type Error = std::array::TryFromSliceError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(bytes.try_into()?))
    }
}

impl PartialEq for KeyHash {
    fn eq(&self, other: &Self) -> bool {
        self.0.ct_eq(&other.0).into()
    }
}

impl Eq for KeyHash {}

impl Hash for KeyHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl fmt::Debug for KeyHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KeyHash({}…)", &self.to_hex()[..8])
    }
}

impl Serialize for KeyHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{HASHED_KEY_PREFIX}{}", self.to_hex()))
    }
}

impl<'de> Deserialize<'de> for KeyHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(Self::parse_hashed(&s).unwrap_or_else(|| Self::of(&s)))
    }
}

pub type KeySet = HashSet<KeyHash>;

pub fn validate_bearer_token(keys: &KeySet, token: &KeyHash) -> bool {
    keys.contains(token)
}

pub const API_KEY_ID_LABEL: &str = "api_key_id";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PresentedKey(pub KeyHash);

pub fn presented_key<B>(req: &axum::http::Request<B>) -> Option<KeyHash> {
    presented_key_from(req.headers(), req.extensions())
}

pub fn presented_key_from(
    headers: &axum::http::HeaderMap,
    extensions: &axum::http::Extensions,
) -> Option<KeyHash> {
    if let Some(PresentedKey(hash)) = extensions.get::<PresentedKey>() {
        return Some(*hash);
    }
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(KeyHash::of)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_plaintext_with_sha256() {
        assert_eq!(
            KeyHash::of("abc").to_hex(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(KeyHash::from("abc"), KeyHash::of("abc"));
        assert_ne!(KeyHash::of("abc"), KeyHash::of("abd"));
    }

    #[test]
    fn deserializes_plaintext_and_hashed_forms() {
        let plain: KeyHash = serde_json::from_str("\"sk-test\"").unwrap();
        assert_eq!(plain, KeyHash::of("sk-test"));

        let serialized = serde_json::to_string(&plain).unwrap();
        assert!(serialized.starts_with("\"sha256:"));
        assert!(!serialized.contains("sk-test"));
        let round_tripped: KeyHash = serde_json::from_str(&serialized).unwrap();
        assert_eq!(round_tripped, plain);

        let odd: KeyHash = serde_json::from_str("\"sha256:zz\"").unwrap();
        assert_eq!(odd, KeyHash::of("sha256:zz"));
    }

    #[test]
    fn validates_by_hash() {
        let keys: KeySet = [KeyHash::of("good")].into_iter().collect();
        assert!(validate_bearer_token(&keys, &KeyHash::of("good")));
        assert!(!validate_bearer_token(&keys, &KeyHash::of("bad")));
    }

    #[test]
    fn presented_key_prefers_the_extension_over_the_bearer() {
        let request = |bearer: Option<&str>| {
            let mut builder = axum::http::Request::builder();
            if let Some(bearer) = bearer {
                builder = builder.header("authorization", format!("Bearer {bearer}"));
            }
            builder.body(()).unwrap()
        };

        assert_eq!(presented_key(&request(None)), None);
        assert_eq!(
            presented_key(&request(Some("sk-a"))),
            Some(KeyHash::of("sk-a"))
        );

        let mut pre_authenticated = request(Some("not-a-key"));
        pre_authenticated
            .extensions_mut()
            .insert(PresentedKey(KeyHash::of("sk-b")));
        assert_eq!(presented_key(&pre_authenticated), Some(KeyHash::of("sk-b")));
    }

    #[test]
    fn debug_does_not_print_the_full_hash() {
        let hash = KeyHash::of("sk-test");
        let debug = format!("{hash:?}");
        assert!(debug.starts_with(&format!("KeyHash({}", &hash.to_hex()[..8])));
        assert!(!debug.contains(&hash.to_hex()[..9]), "{debug}");
    }
}
