
use axum::http::{HeaderMap, StatusCode};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::{
    AppState, auth::KeyHash, client::HttpClient, errors::OnwardsErrorResponse, serving,
    target::Targets,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestIdentity {
    pub account_id: Option<String>,
    pub account_name: Option<String>,
    pub api_key_id: Option<Uuid>,
    pub key_purpose: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RejectionContext {
    identity: RequestIdentity,
    authenticated: bool,
    traffic: &'static str,
    metric_labels: Option<(String, String)>,
}

impl RejectionContext {
    pub(crate) fn from_request<T: HttpClient>(
        state: &AppState<T>,
        headers: &HeaderMap,
        key: Option<KeyHash>,
    ) -> Self {
        let dispatched = state
            .first_token_timeout_exempt_header
            .as_deref()
            .is_some_and(|header| headers.contains_key(header));
        Self::for_key(&state.targets, key.as_ref(), dispatched)
    }

    pub fn for_key(targets: &Targets, key: Option<&KeyHash>, dispatched: bool) -> Self {
        let labels = key.and_then(|key| targets.key_labels.get(key));
        let label = |name: &str| labels.as_ref().and_then(|labels| labels.get(name).cloned());
        Self {
            authenticated: labels.is_some(),
            identity: RequestIdentity {
                model: None,
                account_id: label(serving::ACCOUNT_LABEL),
                account_name: label(serving::ACCOUNT_DISPLAY_NAME_LABEL)
                    .or_else(|| label(serving::ACCOUNT_NAME_LABEL)),
                api_key_id: label(crate::auth::API_KEY_ID_LABEL).and_then(|id| id.parse().ok()),
                key_purpose: label("purpose"),
            },
            traffic: if dispatched { "dispatched" } else { "realtime" },
            metric_labels: None,
        }
    }

    pub(crate) fn set_model(&mut self, model: &str) {
        self.identity.model = Some(model.to_string());
    }

    pub(crate) fn set_metric_labels(&mut self, model: &str, serving_class: &str) {
        self.metric_labels = Some((model.to_string(), serving_class.to_string()));
    }

    pub fn set_model_if_configured(&mut self, targets: &Targets, model: &str) {
        if let Some(alias) = targets.configured_alias(model) {
            self.set_model(alias);
            let (model, serving_class) = targets.metric_labels(alias);
            self.set_metric_labels(&model, &serving_class);
        }
    }

    pub(crate) fn record_error(&self, error: &OnwardsErrorResponse) {
        if !error.is_gateway_rejection() {
            return;
        }
        let (code, param) = error
            .body
            .as_ref()
            .map(|body| (body.code.as_str(), body.param.as_deref()))
            .unwrap_or(("", None));
        self.record(error.status, code, param);
    }

    pub fn record(&self, status: StatusCode, code: &str, param: Option<&str>) {
        self.record_with_reason(status, code, param, None);
    }

    pub fn record_with_reason(
        &self,
        status: StatusCode,
        code: &str,
        param: Option<&str>,
        reason: Option<&'static str>,
    ) {
        let (model, serving_class) = match &self.metric_labels {
            Some((model, serving_class)) => (model.clone(), serving_class.clone()),
            None => (
                self.identity.model.clone().unwrap_or_default(),
                crate::target::STANDARD_SERVING_CLASS.to_owned(),
            ),
        };
        metrics::counter!(
            "onwards_rejections_total",
            "model" => model,
            "serving_class" => serving_class,
            "status" => status.as_str().to_string(),
            "code" => code.to_string(),
            "traffic" => self.traffic,
        )
        .increment(1);

        let span = tracing::Span::current();
        span.record("http.response.status_code", status.as_u16());
        span.set_attribute("error.type", code.to_string());
        if let Some(param) = param {
            span.set_attribute("onwards.rejection.param", param.to_string());
        }
        if let Some(reason) = reason {
            span.set_attribute("onwards.rejection.reason", reason);
        }
        self.record_identity();
    }

    pub fn identity(&self) -> Option<RequestIdentity> {
        self.authenticated.then(|| self.identity.clone())
    }

    pub(crate) fn record_identity(&self) {
        if !self.authenticated {
            return;
        }
        let span = tracing::Span::current();
        let identity = &self.identity;
        if let Some(account) = &identity.account_id {
            span.set_attribute("onwards.account", account.clone());
        }
        if let Some(name) = &identity.account_name {
            span.set_attribute("onwards.account_name", name.clone());
        }
        if let Some(api_key_id) = identity.api_key_id {
            span.set_attribute("onwards.api_key_id", api_key_id.to_string());
        }
        if let Some(purpose) = &identity.key_purpose {
            span.set_attribute("onwards.key_purpose", purpose.clone());
        }
    }
}
