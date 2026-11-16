
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use http_body_util::BodyExt;
use tokio::net::TcpListener;
use tokio::sync::{Notify, watch};

use crate::payload;

#[derive(Clone, Copy, Debug)]
pub enum Reply {
    Hold,
    Complete { tokens: usize },
    StreamThenHold { tokens: usize },
}

#[derive(Clone, Default)]
pub struct Delivered {
    events: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}

impl Delivered {
    pub fn add(&self, events: usize) {
        if events > 0 {
            self.events.fetch_add(events, Ordering::SeqCst);
            self.changed.notify_waiters();
        }
    }

    async fn at_least(&self, events: usize) {
        loop {
            let changed = self.changed.notified();
            if self.events.load(Ordering::SeqCst) >= events {
                return;
            }
            changed.await;
        }
    }
}

#[derive(Clone)]
struct Shared {
    model: String,
    reply: Arc<Mutex<Reply>>,
    requests: Arc<AtomicUsize>,
    arrived: Arc<AtomicUsize>,
    parked: Arc<AtomicUsize>,
    streamed: Arc<AtomicUsize>,
    delivered: Delivered,
    release: watch::Receiver<u64>,
}

pub struct Upstream {
    pub base_url: String,
    shared: Shared,
    release: watch::Sender<u64>,
}

impl Upstream {
    pub async fn start(model: &str) -> Upstream {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock upstream");
        let addr = listener.local_addr().unwrap();
        let (release, release_rx) = watch::channel(0u64);
        let shared = Shared {
            model: model.to_string(),
            reply: Arc::new(Mutex::new(Reply::Complete { tokens: 1 })),
            requests: Arc::new(AtomicUsize::new(1)),
            arrived: Arc::new(AtomicUsize::new(0)),
            parked: Arc::new(AtomicUsize::new(0)),
            streamed: Arc::new(AtomicUsize::new(0)),
            delivered: Delivered::default(),
            release: release_rx,
        };
        let router = Router::new()
            .route("/v1/chat/completions", post(completion))
            .with_state(shared.clone());
        tokio::spawn(async move { axum::serve(listener, router).await.expect("mock upstream server") });
        Upstream {
            base_url: format!("http://{addr}/v1"),
            shared,
            release,
        }
    }

    pub fn prepare(&self, reply: Reply, requests: usize) {
        *self.shared.reply.lock().unwrap() = reply;
        self.shared.requests.store(requests, Ordering::SeqCst);
        self.shared.arrived.store(0, Ordering::SeqCst);
        self.shared.parked.store(0, Ordering::SeqCst);
        self.shared.streamed.store(0, Ordering::SeqCst);
        self.shared.delivered.events.store(0, Ordering::SeqCst);
    }

    pub fn delivered(&self) -> Delivered {
        self.shared.delivered.clone()
    }

    pub fn arrived(&self) -> usize {
        self.shared.arrived.load(Ordering::SeqCst)
    }

    pub fn parked(&self) -> usize {
        self.shared.parked.load(Ordering::SeqCst)
    }

    pub fn streamed(&self) -> usize {
        self.shared.streamed.load(Ordering::SeqCst)
    }

    pub fn release_all(&self) {
        self.release.send_modify(|generation| *generation += 1);
    }
}

async fn completion(State(shared): State<Shared>, request: Request) -> Response {
    let mut body = request.into_body();
    while let Some(frame) = body.frame().await {
        if frame.is_err() {
            return StatusCode::BAD_REQUEST.into_response();
        }
    }

    let reply = *shared.reply.lock().unwrap();
    let mut release = shared.release.clone();
    let generation = *release.borrow_and_update();
    shared.arrived.fetch_add(1, Ordering::SeqCst);

    match reply {
        Reply::Hold => {
            shared.parked.fetch_add(1, Ordering::SeqCst);
            wait_for_release(&mut release, generation).await;
            complete(payload::completion(&shared.model, 1))
        }
        Reply::Complete { tokens } => complete(payload::completion(&shared.model, tokens)),
        Reply::StreamThenHold { tokens } => {
            shared.parked.fetch_add(1, Ordering::SeqCst);
            let requests = shared.requests.load(Ordering::SeqCst);
            let payload::Stream { content, finish } = payload::stream(&shared.model, tokens);
            let stream = async_stream::stream! {
                let events = content.len();
                for (event, bytes) in content.into_iter().enumerate() {
                    shared.delivered.at_least(event * requests).await;
                    yield Ok::<_, std::convert::Infallible>(bytes);
                }
                shared.delivered.at_least(events * requests).await;
                shared.streamed.fetch_add(1, Ordering::SeqCst);
                wait_for_release(&mut release, generation).await;
                for bytes in finish {
                    yield Ok(bytes);
                }
            };
            Response::builder()
                .header(header::CONTENT_TYPE, "text/event-stream")
                .header(header::CONNECTION, "close")
                .body(Body::from_stream(stream))
                .unwrap()
        }
    }
}

async fn wait_for_release(release: &mut watch::Receiver<u64>, generation: u64) {
    let _ = release.wait_for(|current| *current > generation).await;
}

fn complete(body: Bytes) -> Response {
    Response::builder()
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CONNECTION, "close")
        .body(Body::from(body))
        .unwrap()
}
