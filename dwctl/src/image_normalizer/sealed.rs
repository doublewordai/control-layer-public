
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;

use crate::encryption::{self, EncryptionError};

pub const SEALED_MAGIC: &[u8] = b"dwzdrimg1\0";

pub const SEALED_CONTENT_TYPE: &str = "application/octet-stream";

#[derive(Debug, thiserror::Error)]
pub enum SealError {
    #[error("crypto error: {0}")]
    Crypto(#[from] EncryptionError),
    #[error("mime type too long to seal")]
    MimeTooLong,
    #[error("malformed sealed image")]
    Malformed,
}

pub fn is_sealed(blob: &[u8]) -> bool {
    blob.starts_with(SEALED_MAGIC)
}

pub fn seal(key: &[u8], mime: &str, bytes: &[u8]) -> Result<Vec<u8>, SealError> {
    let mime_len = u8::try_from(mime.len()).map_err(|_| SealError::MimeTooLong)?;
    let mut framed = Vec::with_capacity(1 + mime.len() + bytes.len());
    framed.push(mime_len);
    framed.extend_from_slice(mime.as_bytes());
    framed.extend_from_slice(bytes);
    let sealed = encryption::encrypt(key, &framed)?;
    let mut out = Vec::with_capacity(SEALED_MAGIC.len() + sealed.len());
    out.extend_from_slice(SEALED_MAGIC);
    out.extend_from_slice(&sealed);
    Ok(out)
}

pub fn open(key: &[u8], blob: &[u8]) -> Result<(String, Bytes), SealError> {
    let sealed = blob.strip_prefix(SEALED_MAGIC).ok_or(SealError::Malformed)?;
    let framed = encryption::decrypt(key, sealed)?;
    let (&mime_len, rest) = framed.split_first().ok_or(SealError::Malformed)?;
    let mime_len = mime_len as usize;
    if rest.len() < mime_len {
        return Err(SealError::Malformed);
    }
    let (mime, bytes) = rest.split_at(mime_len);
    let mime = std::str::from_utf8(mime).map_err(|_| SealError::Malformed)?.to_string();
    Ok((mime, Bytes::copy_from_slice(bytes)))
}

pub fn to_data_uri(mime: &str, bytes: &[u8]) -> String {
    format!("data:{mime};base64,{}", general_purpose::STANDARD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keystore::generate_key;

    #[test]
    fn seal_open_roundtrip() {
        let key = generate_key();
        let png = b"\x89PNG\r\n\x1a\nrest-of-image";
        let blob = seal(&key, "image/png", png).unwrap();
        assert!(is_sealed(&blob));
        assert!(!blob.windows(8).any(|w| w == &png[..8]));
        let (mime, bytes) = open(&key, &blob).unwrap();
        assert_eq!(mime, "image/png");
        assert_eq!(bytes.as_ref(), png);
    }

    #[test]
    fn open_with_wrong_key_fails() {
        let blob = seal(&generate_key(), "image/png", b"img").unwrap();
        assert!(matches!(open(&generate_key(), &blob), Err(SealError::Crypto(_))));
    }

    #[test]
    fn plaintext_images_are_not_sealed() {
        assert!(!is_sealed(b"\x89PNG\r\n\x1a\n"));
        assert!(!is_sealed(b"\xff\xd8\xff\xe0"));
        assert!(matches!(open(&generate_key(), b"\x89PNG"), Err(SealError::Malformed)));
    }

    #[test]
    fn data_uri_shape() {
        assert_eq!(to_data_uri("image/png", b"hi"), "data:image/png;base64,aGk=");
    }
}
