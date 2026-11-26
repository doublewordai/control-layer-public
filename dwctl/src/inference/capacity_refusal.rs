
use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use uuid::Uuid;

use crate::inference::rejections::DISPATCHED_HEADER;

const CAPACITY_REFUSAL: u16 = 529;

#[derive(Debug, Clone, Default)]
pub struct CapacityRefusalStatuses(Arc<HashMap<String, StatusCode>>);

impl CapacityRefusalStatuses {
    pub fn new(statuses: HashMap<Uuid, u16>) -> Self {
        Self(Arc::new(
            statuses
                .into_iter()
                .filter_map(|(account, status)| Some((account.to_string(), StatusCode::from_u16(status).ok()?)))
                .collect(),
        ))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn for_account(&self, account: &str) -> Option<StatusCode> {
        self.0.get(account).copied()
    }
}

pub async fn capacity_refusal_status_middleware(State(statuses): State<CapacityRefusalStatuses>, request: Request, next: Next) -> Response {
    let dispatched = request.headers().contains_key(DISPATCHED_HEADER);
    let mut response = next.run(request).await;
    if dispatched || response.status().as_u16() != CAPACITY_REFUSAL {
        return response;
    }
    let Some(status) = response
        .extensions()
        .get::<onwards::RequestIdentity>()
        .and_then(|identity| identity.account_id.as_deref())
        .and_then(|account| statuses.for_account(account))
    else {
        return response;
    };
    metrics::counter!("dwctl_capacity_refusal_status_rewrites_total", "status" => status.as_str().to_string()).increment(1);
    *response.status_mut() = status;
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::Body, middleware, response::IntoResponse, routing::get};
    use tower::ServiceExt;

    const ACCOUNT: Uuid = Uuid::from_u128(0x1234);

    fn router(status: u16) -> Router {
        let statuses = CapacityRefusalStatuses::new(HashMap::from([(ACCOUNT, 429)]));
        Router::new()
            .route(
                "/",
                get(move |request: Request| async move {
                    let account = request
                        .headers()
                        .get("x-test-account")
                        .map(|value| value.to_str().unwrap().to_string());
                    let mut response = (
                        StatusCode::from_u16(status).unwrap(),
                        [("retry-after", "1")],
                        r#"{"error":{"code":"overloaded"}}"#,
                    )
                        .into_response();
                    response.extensions_mut().insert(onwards::RequestIdentity {
                        account_id: account,
                        ..Default::default()
                    });
                    response
                }),
            )
            .layer(middleware::from_fn_with_state(statuses, capacity_refusal_status_middleware))
    }

    async fn call(status: u16, account: Option<Uuid>, dispatched: bool) -> Response {
        let mut request = Request::builder().uri("/");
        if let Some(account) = account {
            request = request.header("x-test-account", account.to_string());
        }
        if dispatched {
            request = request.header(DISPATCHED_HEADER, "1700000000");
        }
        router(status).oneshot(request.body(Body::empty()).unwrap()).await.unwrap()
    }

    #[tokio::test]
    async fn realtime_refusal_for_a_listed_account_takes_its_status() {
        let response = call(529, Some(ACCOUNT), false).await;
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers()["retry-after"], "1");
        assert!(response.extensions().get::<onwards::RequestIdentity>().is_some());
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], br#"{"error":{"code":"overloaded"}}"#);
    }

    #[tokio::test]
    async fn other_accounts_keep_529() {
        let response = call(529, Some(Uuid::from_u128(0x5678)), false).await;
        assert_eq!(response.status().as_u16(), 529);
        let response = call(529, None, false).await;
        assert_eq!(response.status().as_u16(), 529);
    }

    #[tokio::test]
    async fn batch_dispatch_keeps_529() {
        let response = call(529, Some(ACCOUNT), true).await;
        assert_eq!(response.status().as_u16(), 529);
    }

    #[tokio::test]
    async fn other_statuses_pass_through() {
        for status in [200, 429, 500, 503] {
            let response = call(status, Some(ACCOUNT), false).await;
            assert_eq!(response.status().as_u16(), status);
        }
    }
}
