
use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    extract::{Request, State},
    http::{Extensions, StatusCode},
    middleware::Next,
    response::Response,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use onwards::{auth::PresentedKey, target::Targets};
use rand::prelude::RngExt;
use sha2::Sha256;
use uuid::Uuid;

use crate::{
    crypto::{ApiKeyHash, hash_api_key},
    inference::rejections,
    types::ApiKeyId,
};

type HmacSha256 = Hmac<Sha256>;

pub const TOKEN_PREFIX: &str = "dwi-";

pub fn is_internal(token: &str) -> bool {
    token.starts_with(TOKEN_PREFIX)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidCredential {
    #[error("malformed internal credential")]
    Malformed,
    #[error("internal credential signature does not match")]
    BadSignature,
    #[error("internal credential has expired")]
    Expired,
}

#[derive(Clone)]
pub struct InternalCredentials {
    key: Arc<[u8; 32]>,
}

impl fmt::Debug for InternalCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InternalCredentials(<redacted>)")
    }
}

impl Default for InternalCredentials {
    fn default() -> Self {
        Self::generate()
    }
}

impl InternalCredentials {
    pub fn generate() -> Self {
        let mut key = [0u8; 32];
        rand::rng().fill(&mut key);
        Self { key: Arc::new(key) }
    }

    pub fn mint(&self, api_key_id: ApiKeyId, ttl: Duration) -> String {
        self.sign(api_key_id, unix_now().saturating_add(ttl.as_secs()))
    }

    #[cfg(test)]
    pub(crate) fn mint_expiring_at(&self, api_key_id: ApiKeyId, expiry: u64) -> String {
        self.sign(api_key_id, expiry)
    }

    fn sign(&self, api_key_id: ApiKeyId, expiry: u64) -> String {
        let payload = format!("{TOKEN_PREFIX}{}.{expiry}", api_key_id.simple());
        let tag = self.mac(&payload).finalize().into_bytes();
        format!("{payload}.{}", URL_SAFE_NO_PAD.encode(tag))
    }

    pub fn verify(&self, token: &str) -> Result<ApiKeyId, InvalidCredential> {
        let (id, expiry) = self.verify_signature(token)?;
        if unix_now() >= expiry {
            return Err(InvalidCredential::Expired);
        }
        Ok(id)
    }

    fn verify_signature(&self, token: &str) -> Result<(ApiKeyId, u64), InvalidCredential> {
        let (payload, tag) = token.rsplit_once('.').ok_or(InvalidCredential::Malformed)?;
        if !is_internal(payload) {
            return Err(InvalidCredential::Malformed);
        }
        let tag = URL_SAFE_NO_PAD.decode(tag).map_err(|_| InvalidCredential::Malformed)?;
        self.mac(payload).verify_slice(&tag).map_err(|_| InvalidCredential::BadSignature)?;

        let (id, expiry) = payload[TOKEN_PREFIX.len()..].split_once('.').ok_or(InvalidCredential::Malformed)?;
        let id = Uuid::try_parse(id).map_err(|_| InvalidCredential::Malformed)?;
        let expiry: u64 = expiry.parse().map_err(|_| InvalidCredential::Malformed)?;
        Ok((id, expiry))
    }

    fn mac(&self, payload: &str) -> HmacSha256 {
        let mut mac = HmacSha256::new_from_slice(&self.key[..]).expect("a 32-byte key is always a valid HMAC key");
        mac.update(payload.as_bytes());
        mac
    }
}

fn unix_now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_secs())
}

pub fn presented_key_hash(extensions: &Extensions, bearer: Option<&str>) -> Option<ApiKeyHash> {
    match extensions.get::<PresentedKey>() {
        Some(PresentedKey(hash)) => Some(*hash.as_bytes()),
        None => bearer.map(hash_api_key),
    }
}

#[derive(Clone)]
pub struct InternalAuthState {
    pub credentials: InternalCredentials,
    pub targets: Targets,
}

impl InternalAuthState {
    pub fn captured_key_hash(&self, bearer: Option<&str>, authenticated: Option<ApiKeyHash>) -> Option<ApiKeyHash> {
        match bearer {
            Some(token) if is_internal(token) => authenticated.or_else(|| {
                let (id, _expiry) = self.credentials.verify_signature(token).ok()?;
                self.targets.key_ids.get(&id.to_string()).map(|hash| *hash.as_bytes())
            }),
            Some(token) => Some(hash_api_key(token)),
            None => None,
        }
    }

    fn resolve(&self, token: &str) -> Result<ApiKeyHash, &'static str> {
        let id = self.credentials.verify(token).map_err(|error| match error {
            InvalidCredential::Malformed => "malformed",
            InvalidCredential::BadSignature => "bad_signature",
            InvalidCredential::Expired => "expired",
        })?;
        self.targets
            .key_ids
            .get(&id.to_string())
            .map(|hash| *hash.as_bytes())
            .ok_or("unknown_key")
    }
}

pub async fn carry_presented_key(request: Request, next: Next) -> Response {
    let key = request.extensions().get::<PresentedKey>().copied();
    let mut response = next.run(request).await;
    if let Some(key) = key {
        response.extensions_mut().insert(key);
    }
    response
}

pub fn carried_key_hash(extensions: &Extensions) -> Option<ApiKeyHash> {
    extensions.get::<PresentedKey>().map(|PresentedKey(hash)| *hash.as_bytes())
}

pub async fn internal_credential_middleware(State(state): State<InternalAuthState>, mut request: Request, next: Next) -> Response {
    let Some(token) = rejections::bearer_token(request.headers()).filter(|token| is_internal(token)) else {
        return next.run(request).await;
    };
    match state.resolve(token) {
        Ok(hash) => {
            let hash = onwards::auth::KeyHash::from_bytes(hash);
            request.extensions_mut().insert(PresentedKey(hash));
            next.run(request).await
        }
        Err(reason) => {
            tracing::debug!(reason, "Refused internal credential");
            let response = Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "error": {
                            "message": "Invalid or expired internal credential",
                            "type": "invalid_request_error",
                            "code": "invalid_api_key",
                        }
                    })
                    .to_string(),
                ))
                .expect("static response always builds");
            rejections::refuse(&request, response, "invalid_api_key", None, None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::{Router, routing::post};
    use axum_test::TestServer;

    use super::*;

    #[test]
    fn mint_then_verify_round_trips() {
        let credentials = InternalCredentials::generate();
        let id = Uuid::new_v4();
        let token = credentials.mint(id, Duration::from_secs(60));
        assert!(is_internal(&token));
        assert!(!token.contains(&id.to_string()), "id is written without hyphens");
        assert_eq!(credentials.verify(&token), Ok(id));
    }

    #[test]
    fn rejects_expired_tokens() {
        let credentials = InternalCredentials::generate();
        let token = credentials.mint_expiring_at(Uuid::new_v4(), unix_now() - 1);
        assert_eq!(credentials.verify(&token), Err(InvalidCredential::Expired));
    }

    #[test]
    fn rejects_tokens_signed_by_another_process() {
        let ours = InternalCredentials::generate();
        let theirs = InternalCredentials::generate();
        let token = theirs.mint(Uuid::new_v4(), Duration::from_secs(60));
        assert_eq!(ours.verify(&token), Err(InvalidCredential::BadSignature));
    }

    #[test]
    fn rejects_forged_and_malformed_tokens() {
        let credentials = InternalCredentials::generate();
        let token = credentials.mint(Uuid::new_v4(), Duration::from_secs(60));
        let (payload, tag) = token.rsplit_once('.').unwrap();

        let other_id = format!("{TOKEN_PREFIX}{}.{}", Uuid::new_v4().simple(), unix_now() + 60);
        let later = format!("{}.{}", payload.rsplit_once('.').unwrap().0, unix_now() + 3600);
        for forged in [other_id, later] {
            assert_eq!(
                credentials.verify(&format!("{forged}.{tag}")),
                Err(InvalidCredential::BadSignature),
                "{forged}"
            );
        }

        for malformed in ["", "dwi-", "sk-abc", "dwi-abc.def", &format!("{payload}.not base64!")] {
            assert_eq!(credentials.verify(malformed), Err(InvalidCredential::Malformed), "{malformed}");
        }
        let unparseable = credentials
            .mint_expiring_at(Uuid::nil(), 0)
            .replace(&Uuid::nil().simple().to_string(), "x");
        assert!(credentials.verify(&unparseable).is_err());
    }

    #[test]
    fn captured_key_hash_checks_the_signature_not_the_expiry() {
        let state = InternalAuthState {
            credentials: InternalCredentials::generate(),
            targets: Targets::default(),
        };
        let id = Uuid::new_v4();
        state.targets.key_ids.insert(id.to_string(), onwards::auth::KeyHash::of("sk-live"));
        let live = Some(hash_api_key("sk-live"));

        assert_eq!(
            state.captured_key_hash(Some(&state.credentials.mint(id, Duration::from_secs(60))), None),
            live
        );
        assert_eq!(
            state.captured_key_hash(Some(&state.credentials.mint_expiring_at(id, 1)), None),
            live
        );
        let foreign = InternalCredentials::generate().mint(id, Duration::from_secs(60));
        assert_eq!(state.captured_key_hash(Some(&foreign), None), None);
        assert_eq!(state.captured_key_hash(Some("sk-other"), None), Some(hash_api_key("sk-other")));
        assert_eq!(state.captured_key_hash(None, None), None);

        let deleted = Uuid::new_v4();
        let carried = Some(hash_api_key("sk-deleted"));
        let token = state.credentials.mint(deleted, Duration::from_secs(60));
        assert_eq!(state.captured_key_hash(Some(&token), None), None);
        assert_eq!(state.captured_key_hash(Some(&token), carried), carried);
        assert_eq!(state.captured_key_hash(Some("sk-other"), carried), Some(hash_api_key("sk-other")));
    }

    #[tokio::test]
    async fn carry_presented_key_puts_the_key_on_the_response() {
        use tower::ServiceExt;

        async fn refuse() -> StatusCode {
            StatusCode::BAD_REQUEST
        }
        let app = Router::new()
            .route("/ai", post(refuse))
            .layer(axum::middleware::from_fn(carry_presented_key));
        let key = onwards::auth::KeyHash::of("sk-live");
        let mut request = Request::builder().method("POST").uri("/ai").body(Body::empty()).unwrap();
        request.extensions_mut().insert(PresentedKey(key));
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(carried_key_hash(response.extensions()), Some(hash_api_key("sk-live")));

        let plain = Request::builder().method("POST").uri("/ai").body(Body::empty()).unwrap();
        assert_eq!(carried_key_hash(app.oneshot(plain).await.unwrap().extensions()), None);
    }

    #[test]
    fn debug_hides_the_key() {
        assert_eq!(format!("{:?}", InternalCredentials::generate()), "InternalCredentials(<redacted>)");
    }

    #[test]
    fn presented_key_hash_prefers_the_authenticated_key() {
        let mut extensions = Extensions::new();
        assert_eq!(presented_key_hash(&extensions, None), None);
        assert_eq!(presented_key_hash(&extensions, Some("sk-a")), Some(hash_api_key("sk-a")));
        extensions.insert(PresentedKey(onwards::auth::KeyHash::of("sk-b")));
        assert_eq!(presented_key_hash(&extensions, Some("sk-a")), Some(hash_api_key("sk-b")));
    }

    #[tokio::test]
    async fn middleware_admits_only_valid_tokens_for_live_keys() {
        let credentials = InternalCredentials::generate();
        let live = Uuid::new_v4();
        let deleted = Uuid::new_v4();
        let targets = Targets::default();
        targets.key_ids.insert(live.to_string(), onwards::auth::KeyHash::of("sk-live"));

        async fn echo(request: Request) -> String {
            match request.extensions().get::<PresentedKey>() {
                Some(PresentedKey(hash)) if *hash == onwards::auth::KeyHash::of("sk-live") => "live".to_string(),
                Some(_) => "other".to_string(),
                None => "none".to_string(),
            }
        }
        let app = Router::new().route("/ai", post(echo)).layer(axum::middleware::from_fn_with_state(
            InternalAuthState {
                credentials: credentials.clone(),
                targets,
            },
            internal_credential_middleware,
        ));
        let server = TestServer::new(app).unwrap();

        let ok = server
            .post("/ai")
            .authorization_bearer(credentials.mint(live, Duration::from_secs(60)))
            .await;
        ok.assert_status_ok();
        assert_eq!(ok.text(), "live");

        let plain = server.post("/ai").authorization_bearer("sk-live").await;
        assert_eq!(plain.text(), "none");

        let refused: HashMap<&str, String> = HashMap::from([
            ("deleted key", credentials.mint(deleted, Duration::from_secs(60))),
            ("expired", credentials.mint_expiring_at(live, unix_now() - 1)),
            ("other process", InternalCredentials::generate().mint(live, Duration::from_secs(60))),
            ("garbage", format!("{TOKEN_PREFIX}garbage")),
        ]);
        for (case, token) in refused {
            let response = server.post("/ai").authorization_bearer(token).await;
            assert_eq!(response.status_code(), StatusCode::UNAUTHORIZED, "{case}");
            assert_eq!(response.json::<serde_json::Value>()["error"]["code"], "invalid_api_key", "{case}");
        }
    }
}
