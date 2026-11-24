
use metrics::counter;

pub mod component {
    pub const SUPERVISOR: &str = "supervisor";
    pub const NOTIFICATIONS: &str = "notifications";
    pub const AUTO_TOPUP: &str = "auto_topup";
    pub const WEBHOOK_DISPATCH: &str = "webhook_dispatch";
    pub const LEADER_ELECTION: &str = "leader_election";
    pub const CONFIG_WATCHER: &str = "config_watcher";
    pub const PROBE_SCHEDULER: &str = "probe_scheduler";
    pub const TASK_WORKER: &str = "task_worker";
    pub const ONWARDS_SYNC: &str = "onwards_sync";
    pub const ZDR_KEY_SYNC: &str = "zdr_key_sync";
    pub const ZDR_DISPATCH: &str = "zdr_dispatch";
    pub const DISPATCH_AUTH: &str = "dispatch_auth";
    pub const ONWARDS_HEARTBEAT: &str = "onwards_heartbeat";
    pub const ANALYTICS: &str = "analytics";
    pub const ANALYTICS_BATCHER: &str = "analytics_batcher";
    pub const CLICKHOUSE_SINK: &str = "clickhouse_sink";
    pub const PREFIX_CHAIN: &str = "prefix_chain";
    pub const RESPONSES_WRITER: &str = "responses_writer";
    pub const BATCH_POPULATE: &str = "batch_populate";
    pub const BATCH_SLA_METRICS: &str = "batch_sla_metrics";
    pub const PAYMENTS: &str = "payments";
    pub const USAGE_REFRESH: &str = "usage_refresh";
    pub const TASK_RETENTION: &str = "task_retention";
    pub const PROMPT_CACHE_RETENTION: &str = "prompt_cache_retention";
    pub const REFERENCE_CACHE: &str = "reference_cache";
    pub const CONTINUATION: &str = "continuation";
    pub const ALLOCATOR_METRICS: &str = "allocator_metrics";
    pub const HEAP_PROFILING: &str = "heap_profiling";
    pub const REALTIME_INFLIGHT: &str = "realtime_inflight";
    pub const API_KEY_USAGE: &str = "api_key_usage";
    pub const PENDING_SECRETS: &str = "pending_secrets";
    pub const SERVICE_ACCOUNTS: &str = "service_accounts";
    pub const OPENROUTER_REFRESH: &str = "openrouter_refresh";
}

pub fn record(component: &'static str, reason: &'static str, severity: &'static str) {
    counter!(
        "dwctl_background_errors_total",
        "component" => component,
        "reason" => reason,
        "severity" => severity
    )
    .increment(1);
}

#[macro_export]
macro_rules! background_error {
    ($component:expr, $reason:expr, Critical, $($arg:tt)+) => {{
        $crate::metrics::errors::record($component, $reason, "critical");
        ::tracing::error!(component = $component, reason = $reason, $($arg)+);
    }};
    ($component:expr, $reason:expr, Error, $($arg:tt)+) => {{
        $crate::metrics::errors::record($component, $reason, "error");
        ::tracing::error!(component = $component, reason = $reason, $($arg)+);
    }};
    ($component:expr, $reason:expr, Warning, $($arg:tt)+) => {{
        $crate::metrics::errors::record($component, $reason, "warning");
        ::tracing::warn!(component = $component, reason = $reason, $($arg)+);
    }};
}
