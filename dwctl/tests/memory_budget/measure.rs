
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::http::{Request, header};
use http_body_util::{BodyExt, Full};
use tokio::sync::watch;

use crate::alloc;
use crate::harness::Harness;
use crate::upstream::Reply;

pub const CONCURRENCY: usize = 32;

const ATTEMPTS: usize = 3;

#[derive(Clone, Copy)]
pub enum Target {
    Application,
    Upstream,
}

pub struct Load {
    pub target: Target,
    pub path: &'static str,
    pub body: Bytes,
    pub reply: Reply,
    pub size: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct Round {
    pub size: usize,
    pub held: f64,
    pub peak: f64,
    pub retained: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct Scaling {
    pub copies: f64,
    pub fixed: f64,
    pub idle_copies: f64,
    pub leaked: f64,
    pub small: Round,
    pub large: Round,
}

impl std::fmt::Display for Scaling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kib = |bytes: f64| bytes / 1024.0;
        write!(
            f,
            "copies {:.2}, fixed {:.1} KiB, idle copies {:.2}, leaked {:.1} KiB per request \
             (held {:.1}/{:.1} KiB, peak {:.1}/{:.1} KiB per request at {:.0}/{:.0} KiB)",
            self.copies,
            kib(self.fixed),
            self.idle_copies,
            kib(self.leaked),
            kib(self.small.held),
            kib(self.large.held),
            kib(self.small.peak),
            kib(self.large.peak),
            kib(self.small.size as f64),
            kib(self.large.size as f64),
        )
    }
}

pub fn scaling(harness: &Harness, load: impl Fn(usize) -> Load, [small, large]: [usize; 2]) -> Scaling {
    round(harness, load(small));
    let small = quietest(harness, || load(small));
    let first_large = round(harness, load(large));
    let large = quietest(harness, || load(large));
    let size_delta = (large.size - small.size) as f64;
    let copies = (large.held - small.held) / size_delta;
    Scaling {
        copies,
        fixed: small.held - copies * small.size as f64,
        idle_copies: first_large.retained / size_delta,
        leaked: small.retained.max(large.retained),
        small,
        large,
    }
}

pub fn quietest(harness: &Harness, load: impl Fn() -> Load) -> Round {
    rounds(harness, load)
        .min_by(|a, b| a.held.total_cmp(&b.held))
        .expect("at least one attempt")
}

pub fn rounds<'a>(harness: &'a Harness, load: impl Fn() -> Load + 'a) -> impl Iterator<Item = Round> + 'a {
    (0..ATTEMPTS).map(move |_| round(harness, load()))
}

pub fn round(harness: &Harness, load: Load) -> Round {
    let upstream = &harness.upstream;
    upstream.prepare(load.reply, CONCURRENCY);

    let baseline = settle();
    alloc::reset_peak();

    let url = match load.target {
        Target::Application => format!("{}{}", harness.base_url, load.path),
        Target::Upstream => format!("{}/chat/completions", upstream.base_url),
    };
    let authorization = format!("Bearer {}", harness.api_key);
    let client = harness.client.clone();
    let body = load.body;
    let delivered = upstream.delivered();
    let hold_response = matches!(load.reply, Reply::Complete { .. });
    let responding = Arc::new(AtomicUsize::new(0));
    let (release_responses, responses_released) = watch::channel(false);
    let load_responding = responding.clone();
    let requests = harness.driver.spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..CONCURRENCY {
            let (client, url, authorization, body) = (client.clone(), url.clone(), authorization.clone(), body.clone());
            let (delivered, responding) = (delivered.clone(), load_responding.clone());
            let mut responses_released = responses_released.clone();
            tasks.spawn(async move {
                let request = Request::post(url)
                    .header(header::AUTHORIZATION, authorization)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Full::new(body))
                    .expect("request");
                let response = client.request(request).await.expect("send request");
                responding.fetch_add(1, Ordering::SeqCst);
                if hold_response {
                    let _ = responses_released.wait_for(|released| *released).await;
                }
                let status = response.status();
                let mut response = response.into_body();
                let mut error = Vec::new();
                let mut previous = 0u8;
                while let Some(frame) = response.frame().await {
                    let Ok(chunk) = frame.expect("read response").into_data() else {
                        continue;
                    };
                    let mut events = 0;
                    for &byte in chunk.iter() {
                        if byte == b'\n' && previous == b'\n' {
                            events += 1;
                        }
                        previous = byte;
                    }
                    delivered.add(events);
                    if !status.is_success() && error.len() < 2048 {
                        error.extend_from_slice(&chunk);
                    }
                }
                (status, String::from_utf8_lossy(&error).into_owned())
            });
        }
        tasks.join_all().await
    });

    match load.reply {
        Reply::Hold => wait_until("every request to reach the upstream", Duration::from_secs(60), || {
            upstream.parked() >= CONCURRENCY
        }),
        Reply::StreamThenHold { .. } => wait_until("every stream to deliver its content", Duration::from_secs(60), || {
            upstream.streamed() >= CONCURRENCY
        }),
        Reply::Complete { .. } => wait_until("every response to reach the load generator", Duration::from_secs(60), || {
            responding.load(Ordering::SeqCst) >= CONCURRENCY
        }),
    }
    let held = settle() - baseline;
    let peak = alloc::peak_bytes() - baseline;
    assert_eq!(upstream.arrived(), CONCURRENCY, "requests other than the load reached the upstream");

    upstream.release_all();
    release_responses.send_replace(true);
    let results = harness.driver.block_on(requests).expect("request tasks");
    for (status, error) in &results {
        assert!(status.is_success(), "request failed with {status}: {error}");
    }
    let retained = settle() - baseline;

    let per_request = |bytes: isize| bytes as f64 / CONCURRENCY as f64;
    Round {
        size: load.size,
        held: per_request(held),
        peak: per_request(peak),
        retained: per_request(retained),
    }
}

fn settle() -> isize {
    const WINDOW: usize = 12;
    const INTERVAL: Duration = Duration::from_millis(25);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut recent = Vec::with_capacity(WINDOW);
    loop {
        recent.push(alloc::live_bytes());
        if recent.len() > WINDOW {
            recent.remove(0);
        }
        if recent.len() == WINDOW {
            let (low, high) = (recent.iter().min().unwrap(), recent.iter().max().unwrap());
            let band = (high.abs() / 200).max(64 * 1024);
            if high - low <= band || Instant::now() > deadline {
                return recent[WINDOW - 1];
            }
        }
        std::thread::sleep(INTERVAL);
    }
}

fn wait_until(what: &str, timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out after {timeout:?} waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}
