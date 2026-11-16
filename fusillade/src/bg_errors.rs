
use metrics::counter;

pub fn record(reason: &'static str, severity: &'static str) {
    counter!(
        "fusillade_background_errors_total",
        "component" => "daemon",
        "reason" => reason,
        "severity" => severity
    )
    .increment(1);
}

#[macro_export]
macro_rules! background_error {
    ($reason:expr, Critical, $($arg:tt)+) => {{
        $crate::bg_errors::record($reason, "critical");
        ::tracing::error!(component = "daemon", reason = $reason, $($arg)+);
    }};
    ($reason:expr, Error, $($arg:tt)+) => {{
        $crate::bg_errors::record($reason, "error");
        ::tracing::error!(component = "daemon", reason = $reason, $($arg)+);
    }};
    ($reason:expr, Warning, $($arg:tt)+) => {{
        $crate::bg_errors::record($reason, "warning");
        ::tracing::warn!(component = "daemon", reason = $reason, $($arg)+);
    }};
}
