
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics::counter;
use serde_json::Value;
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::SharedConfig;
use crate::config::OpenRouterIntegrationConfig;
use crate::metrics::errors::component;

const REFRESHES: &str = "dwctl_openrouter_listing_refreshes_total";

const MAX_SCHEDULED: usize = 1024;

#[derive(Default)]
struct Pending {
    now: bool,
    scheduled: BTreeSet<Instant>,
}

struct Shared {
    pending: Mutex<Pending>,
    wake: Notify,
}

#[derive(Clone, Default)]
pub struct ListingRefresher {
    shared: Option<Arc<Shared>>,
}

impl ListingRefresher {
    pub fn spawn(config: SharedConfig, shutdown: CancellationToken) -> (Self, impl Future<Output = anyhow::Result<()>> + Send + 'static) {
        let shared = Arc::new(Shared {
            pending: Mutex::new(Pending::default()),
            wake: Notify::new(),
        });
        (
            Self {
                shared: Some(shared.clone()),
            },
            run(config, shared, shutdown),
        )
    }

    pub fn request_at(&self, at: chrono::DateTime<chrono::Utc>) {
        let (Some(shared), Ok(wait)) = (&self.shared, (at - chrono::Utc::now()).to_std()) else {
            return;
        };
        if wait.is_zero() {
            return;
        }
        let at = Instant::now() + Duration::from_secs(wait.as_secs() + 2);
        {
            let mut pending = shared.pending.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            pending.scheduled.insert(at);
            if pending.scheduled.len() > MAX_SCHEDULED {
                pending.scheduled.pop_last();
            }
        }
        shared.wake.notify_one();
    }

    pub fn request(&self) {
        if let Some(shared) = &self.shared {
            shared.pending.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).now = true;
            shared.wake.notify_one();
        }
    }
}

async fn run(config: SharedConfig, shared: Arc<Shared>, shutdown: CancellationToken) -> anyhow::Result<()> {
    let mut client = match credentials(&config.snapshot().integrations.openrouter) {
        Some(_) => Some(reqwest::Client::builder().build()?),
        None => None,
    };
    let lock = || shared.pending.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    loop {
        loop {
            let (now, next) = {
                let pending = lock();
                (pending.now, pending.scheduled.first().copied())
            };
            if now || next.is_some_and(|at| at <= Instant::now()) {
                break;
            }
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = shared.wake.notified() => {}
                _ = tokio::time::sleep_until(next.unwrap_or_else(Instant::now)), if next.is_some() => {}
            }
        }
        let coalesce = config.snapshot().integrations.openrouter.refresh_coalesce;
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            _ = tokio::time::sleep(coalesce) => {}
        }
        {
            let mut pending = lock();
            pending.now = false;
            let now = Instant::now();
            pending.scheduled.retain(|at| *at > now);
        }
        let settings = config.snapshot().integrations.openrouter.clone();
        if client.is_none() && credentials(&settings).is_some() {
            client = Some(reqwest::Client::builder().build()?);
        }
        match &client {
            Some(client) => refresh(client, &settings).await,
            None => skipped(),
        }
    }
}

fn credentials(settings: &OpenRouterIntegrationConfig) -> Option<(String, String)> {
    let present = |value: &Option<String>| value.as_deref().map(str::trim).filter(|value| !value.is_empty()).map(str::to_owned);
    Some((present(&settings.provider_api_key)?, present(&settings.provider_slug)?))
}

fn skipped() {
    debug!("OpenRouter listing refresh skipped: no Provider API key configured");
    counter!(REFRESHES, "outcome" => "skipped").increment(1);
}

async fn refresh(client: &reqwest::Client, settings: &OpenRouterIntegrationConfig) {
    let Some((key, slug)) = credentials(settings) else {
        skipped();
        return;
    };
    let url = match refresh_url(&settings.api_base_url, &slug) {
        Ok(url) => url,
        Err(error) => {
            crate::background_error!(component::OPENROUTER_REFRESH, "bad_url", Error, error = %error, "OpenRouter listing refresh: cannot build the URL");
            counter!(REFRESHES, "outcome" => "failed").increment(1);
            return;
        }
    };
    match call(client, url, &key, settings.refresh_timeout).await {
        Ok(body) => {
            let job = body.as_ref().and_then(job_id);
            info!(job = job.as_deref(), "OpenRouter listing refresh requested");
            counter!(REFRESHES, "outcome" => "ok").increment(1);
        }
        Err(error) => {
            crate::background_error!(
                component::OPENROUTER_REFRESH,
                "refresh_failed",
                Warning,
                error = %error,
                "OpenRouter listing refresh failed; OpenRouter applies the price on its next poll"
            );
            counter!(REFRESHES, "outcome" => "failed").increment(1);
        }
    }
}

fn refresh_url(base: &url::Url, slug: &str) -> Result<url::Url, url::ParseError> {
    let mut url = base.clone();
    url.path_segments_mut()
        .map_err(|()| url::ParseError::RelativeUrlWithCannotBeABaseBase)?
        .pop_if_empty()
        .extend(["api", "v1", "providers", slug, "listings", "refresh"]);
    Ok(url)
}

async fn call(client: &reqwest::Client, url: url::Url, key: &str, timeout: Duration) -> anyhow::Result<Option<Value>> {
    let response = client.post(url).bearer_auth(key).timeout(timeout).send().await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        let excerpt: String = body.chars().take(300).collect();
        anyhow::bail!("HTTP {status}: {excerpt}");
    }
    Ok(serde_json::from_str(&body).ok())
}

fn job_id(body: &Value) -> Option<String> {
    let data = body.get("data").unwrap_or(body);
    ["job_id", "id"]
        .iter()
        .find_map(|field| data.get(*field))
        .map(|id| id.as_str().map_or_else(|| id.to_string(), str::to_owned))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn settings(server: &MockServer) -> OpenRouterIntegrationConfig {
        OpenRouterIntegrationConfig {
            account: Some("example-marketplace".to_string()),
            provider_slug: Some("example-provider".to_string()),
            provider_api_key: Some("test-provider-key".to_string()),
            api_base_url: server.uri().parse().unwrap(),
            refresh_coalesce: Duration::from_millis(50),
            ..Default::default()
        }
    }

    #[test]
    fn the_url_keeps_a_base_path() {
        let url = refresh_url(&"https://example.test/proxy/".parse().unwrap(), "example-provider").unwrap();
        assert_eq!(
            url.as_str(),
            "https://example.test/proxy/api/v1/providers/example-provider/listings/refresh"
        );
    }

    #[test]
    fn a_job_id_is_found_at_the_top_or_under_data() {
        assert_eq!(job_id(&json!({"data": {"id": "job-1"}})).as_deref(), Some("job-1"));
        assert_eq!(job_id(&json!({"job_id": 7})).as_deref(), Some("7"));
        assert_eq!(job_id(&json!({"ok": true})), None);
    }

    #[tokio::test]
    async fn a_burst_of_requests_makes_one_call() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/providers/example-provider/listings/refresh"))
            .and(header("authorization", "Bearer test-provider-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": {"id": "job-1"}})))
            .expect(1)
            .mount(&server)
            .await;
        let mut config = crate::test::utils::create_test_config();
        config.integrations.openrouter = settings(&server);
        let shutdown = CancellationToken::new();
        let (refresher, task) = ListingRefresher::spawn(SharedConfig::new(config), shutdown.clone());
        let task = tokio::spawn(task);

        for _ in 0..5 {
            refresher.request();
        }
        wait_for_calls(&server, 1).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(server.received_requests().await.unwrap().len(), 1);

        shutdown.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn schedules_for_one_start_make_one_call_then_stop_at_shutdown() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let mut config = crate::test::utils::create_test_config();
        config.integrations.openrouter = settings(&server);
        let shutdown = CancellationToken::new();
        let (refresher, task) = ListingRefresher::spawn(SharedConfig::new(config), shutdown.clone());
        let task = tokio::spawn(task);

        let starts = chrono::Utc::now() + chrono::Duration::milliseconds(200);
        for _ in 0..3 {
            refresher.request_at(starts);
        }
        refresher.request_at(chrono::Utc::now() + chrono::Duration::days(30));
        assert!(server.received_requests().await.unwrap().is_empty(), "nothing before the start");
        wait_for_calls(&server, 1).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(server.received_requests().await.unwrap().len(), 1, "one call for one start");

        shutdown.cancel();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_failed_call_is_reported_not_raised() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
            .mount(&server)
            .await;
        let error = call(
            &reqwest::Client::new(),
            refresh_url(&server.uri().parse().unwrap(), "example-provider").unwrap(),
            "test-provider-key",
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("503"), "{error}");
        refresh(&reqwest::Client::new(), &settings(&server)).await;
    }

    #[tokio::test]
    async fn without_a_key_nothing_is_sent() {
        let server = MockServer::start().await;
        let mut settings = settings(&server);
        settings.provider_api_key = None;
        refresh(&reqwest::Client::new(), &settings).await;
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    pub(crate) async fn wait_for_calls(server: &MockServer, calls: usize) {
        for _ in 0..500 {
            if server.received_requests().await.unwrap().len() >= calls {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("expected {calls} refresh calls");
    }
}
