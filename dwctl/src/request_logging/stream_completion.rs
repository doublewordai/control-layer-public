
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;
use http_body::{Frame, SizeHint};

pub const CLIENT_DISCONNECTED: &str = "client_disconnected";

#[derive(Debug, Clone, Default)]
pub struct StreamCompletion(Arc<AtomicBool>);

impl StreamCompletion {
    pub fn ended(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub(crate) fn mark_ended(&self) {
        self.0.store(true, Ordering::Release);
    }
}

pub fn client_disconnected(response: &outlet::ResponseData) -> bool {
    response.extensions.get::<StreamCompletion>().is_some_and(|c| !c.ended())
}

pub async fn track_stream_completion(request: Request, next: Next) -> Response {
    let (mut parts, body) = next.run(request).await.into_parts();
    let completion = StreamCompletion::default();
    parts.extensions.insert(completion.clone());
    Response::from_parts(parts, Body::new(TrackedBody { inner: body, completion }))
}

struct TrackedBody {
    inner: Body,
    completion: StreamCompletion,
}

impl http_body::Body for TrackedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let poll = Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(poll, Poll::Ready(None | Some(Err(_)))) {
            self.completion.mark_ended();
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        let end = self.inner.is_end_stream();
        if end {
            self.completion.mark_ended();
        }
        end
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::routing::get;
    use futures::StreamExt;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn respond(body: Body) -> (StreamCompletion, Body) {
        let body = Arc::new(std::sync::Mutex::new(Some(body)));
        let app = Router::new()
            .route(
                "/",
                get(move || {
                    let body = body.clone();
                    async move { body.lock().unwrap().take().unwrap() }
                }),
            )
            .layer(axum::middleware::from_fn(track_stream_completion));
        let response = app.oneshot(Request::get("/").body(Body::empty()).unwrap()).await.unwrap();
        let completion = response.extensions().get::<StreamCompletion>().unwrap().clone();
        (completion, response.into_body())
    }

    fn chunks(n: usize) -> Body {
        Body::from_stream(futures::stream::iter(
            (0..n).map(|i| Ok::<_, std::io::Error>(Bytes::from(format!("data: {i}\n\n")))),
        ))
    }

    #[tokio::test]
    async fn body_read_to_the_end_is_ended() {
        let (completion, body) = respond(chunks(3)).await;
        assert!(!completion.ended());
        body.collect().await.unwrap();
        assert!(completion.ended());
    }

    #[tokio::test]
    async fn body_dropped_part_way_is_not_ended() {
        let (completion, body) = respond(chunks(3)).await;
        let mut stream = body.into_data_stream();
        stream.next().await.unwrap().unwrap();
        drop(stream);
        assert!(!completion.ended());
    }

    #[tokio::test]
    async fn body_that_errors_is_ended() {
        let failing = futures::stream::iter([Ok(Bytes::from("data: 0\n\n")), Err(std::io::Error::other("reset"))]);
        let (completion, body) = respond(Body::from_stream(failing)).await;
        assert!(body.collect().await.is_err());
        assert!(completion.ended());
    }

    struct Recorder(tokio::sync::mpsc::UnboundedSender<bool>);

    impl outlet::RequestHandler for Recorder {
        async fn handle_request(&self, _: outlet::RequestData) {}

        async fn handle_response(&self, _: outlet::RequestData, response: outlet::ResponseData) {
            let _ = self.0.send(client_disconnected(&response));
        }
    }

    async fn seen_by_outlet(read_to_end: bool) -> bool {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let config = outlet::RequestLoggerConfig {
            capture_request_body: true,
            capture_response_body: true,
            ..Default::default()
        };
        let app = Router::new()
            .route("/", get(|| async { chunks(3) }))
            .layer(axum::middleware::from_fn(track_stream_completion))
            .layer(outlet::RequestLoggerLayer::new(config, Recorder(tx)));
        let body = app
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap()
            .into_body();
        if read_to_end {
            body.collect().await.unwrap();
        } else {
            let mut stream = body.into_data_stream();
            stream.next().await.unwrap().unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn outlet_handlers_see_a_disconnect() {
        assert!(!seen_by_outlet(true).await);
        assert!(seen_by_outlet(false).await);
    }

    #[tokio::test]
    async fn size_hint_is_preserved() {
        let (_, body) = respond(Body::from("hello")).await;
        assert_eq!(http_body::Body::size_hint(&body).exact(), Some(5));
    }
}
