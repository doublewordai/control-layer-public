use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

const SCHEME: &str = "dw-img://";
const SHA_HEX_LEN: usize = 64;
const UPLOAD_ID_HEX_LEN: usize = 32;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ImageToken(pub [u8; 32], pub Option<[u8; 16]>);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TokenParseError {
    #[error("token hex must be exactly 64 characters (got {0})")]
    WrongLength(usize),
    #[error("token hex contains non-hex characters")]
    InvalidHex,
    #[error("token upload ID must be exactly 32 hex characters (got {0})")]
    WrongUploadIdLength(usize),
}

impl ImageToken {
    pub fn new_unique(sha256: [u8; 32]) -> Self {
        ImageToken(sha256, Some(*uuid::Uuid::new_v4().as_bytes()))
    }

    pub fn to_dw_img_uri(self) -> String {
        format!("{SCHEME}{}", self.to_hex())
    }

    pub fn to_hex(self) -> String {
        match self.1 {
            Some(upload_id) => format!("{}.{}", hex::encode(self.0), hex::encode(upload_id)),
            None => hex::encode(self.0),
        }
    }

    pub fn looks_like_token(s: &str) -> bool {
        s.starts_with(SCHEME)
    }
}

impl fmt::Debug for ImageToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ImageToken({})", self.to_dw_img_uri())
    }
}

impl fmt::Display for ImageToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_dw_img_uri())
    }
}

impl FromStr for ImageToken {
    type Err = TokenParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hex_str = s.strip_prefix(SCHEME).unwrap_or(s);
        let (sha_hex, upload_id_hex) = match hex_str.split_once('.') {
            Some((sha, upload_id)) => (sha, Some(upload_id)),
            None => (hex_str, None),
        };
        if sha_hex.len() != SHA_HEX_LEN {
            return Err(TokenParseError::WrongLength(sha_hex.len()));
        }
        let mut sha = [0u8; 32];
        hex::decode_to_slice(sha_hex, &mut sha).map_err(|_| TokenParseError::InvalidHex)?;
        let upload_id = match upload_id_hex {
            None => None,
            Some(n) if n.len() != UPLOAD_ID_HEX_LEN => return Err(TokenParseError::WrongUploadIdLength(n.len())),
            Some(n) => {
                let mut upload_id = [0u8; 16];
                hex::decode_to_slice(n, &mut upload_id).map_err(|_| TokenParseError::InvalidHex)?;
                Some(upload_id)
            }
        };
        Ok(ImageToken(sha, upload_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ImageToken {
        let mut bytes = [0u8; 32];
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }
        ImageToken(bytes, None)
    }

    #[test]
    fn round_trip_via_dw_img_uri() {
        let t = sample();
        let s = t.to_dw_img_uri();
        assert!(s.starts_with("dw-img://"));
        let parsed: ImageToken = s.parse().unwrap();
        assert_eq!(parsed, t);
    }

    #[test]
    fn round_trip_via_bare_hex() {
        let t = sample();
        let parsed: ImageToken = t.to_hex().parse().unwrap();
        assert_eq!(parsed, t);
    }

    #[test]
    fn round_trip_with_upload_id() {
        let t = ImageToken::new_unique(sample().0);
        let s = t.to_dw_img_uri();
        assert_eq!(s.len(), "dw-img://".len() + 64 + 1 + 32);
        assert_eq!(s.parse::<ImageToken>().unwrap(), t);
        assert_eq!(t.to_hex().parse::<ImageToken>().unwrap(), t);
    }

    #[test]
    fn new_unique_keeps_content_hash_and_varies_upload_id() {
        let a = ImageToken::new_unique(sample().0);
        let b = ImageToken::new_unique(sample().0);
        assert_eq!(a.0, b.0);
        assert_ne!(a, b);
        assert_ne!(a.to_hex(), b.to_hex());
    }

    #[test]
    fn rejects_wrong_length() {
        let err: TokenParseError = "dw-img://abcd".parse::<ImageToken>().unwrap_err();
        assert!(matches!(err, TokenParseError::WrongLength(4)));
    }

    #[test]
    fn rejects_non_hex_chars() {
        let bad = format!("dw-img://{}", "z".repeat(64));
        let err = bad.parse::<ImageToken>().unwrap_err();
        assert_eq!(err, TokenParseError::InvalidHex);
    }

    #[test]
    fn rejects_bad_upload_id() {
        let sha = sample().to_hex();
        assert_eq!(
            format!("dw-img://{sha}.abcd").parse::<ImageToken>().unwrap_err(),
            TokenParseError::WrongUploadIdLength(4)
        );
        assert_eq!(
            format!("dw-img://{sha}.{}", "z".repeat(32)).parse::<ImageToken>().unwrap_err(),
            TokenParseError::InvalidHex
        );
    }

    #[test]
    fn looks_like_token_only_matches_scheme() {
        assert!(ImageToken::looks_like_token("dw-img://anything"));
        assert!(!ImageToken::looks_like_token("https://example.com/foo"));
        assert!(!ImageToken::looks_like_token("data:image/png;base64,iVB="));
        assert!(!ImageToken::looks_like_token(""));
    }
}
