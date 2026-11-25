
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::clickhouse::{ClickhouseConfig, SinkOptions};

const TABLE: &str = "request_routing";
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const MAX_BATCH_ROWS: usize = 5_000;
const QUEUE_CAPACITY: usize = 50_000;
const RETRY_DELAY: Duration = Duration::from_millis(500);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RequestRoutingConfig {
    pub enabled: bool,
}

impl RequestRoutingConfig {
    pub fn validate(&self, clickhouse: Option<&ClickhouseConfig>, enable_analytics: bool) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        if clickhouse.is_none() {
            return Err("request_routing.enabled is true but there is no clickhouse section".to_string());
        }
        if !enable_analytics {
            return Err("request_routing.enabled is true but enable_analytics is false".to_string());
        }
        Ok(())
    }
}

pub fn sink_options() -> SinkOptions {
    SinkOptions {
        table: TABLE.to_string(),
        flush_interval: FLUSH_INTERVAL,
        max_batch_rows: MAX_BATCH_ROWS,
        queue_capacity: QUEUE_CAPACITY,
        retry_delay: RETRY_DELAY,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RequestRoutingRow {
    pub analytics_id: i64,
    pub x_request_id: Option<Uuid>,
    pub timestamp: DateTime<Utc>,
    pub model: String,
    pub served_model: Option<String>,
    pub resolved_serving_class: Option<String>,
    pub served_by: Option<String>,
    pub compute_pool: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clickhouse() -> ClickhouseConfig {
        ClickhouseConfig {
            endpoint_url: "https://example.clickhouse.cloud:8443".to_string(),
            database: "clay".to_string(),
            user: "dwctl".to_string(),
            password: "pw".to_string(),
        }
    }

    #[test]
    fn enabling_requires_clickhouse_and_analytics() {
        let off = RequestRoutingConfig::default();
        assert!(off.validate(None, false).is_ok(), "off ignores everything else");
        let on = RequestRoutingConfig { enabled: true };
        assert!(on.validate(Some(&clickhouse()), true).is_ok());
        assert!(on.validate(None, true).unwrap_err().contains("clickhouse"));
        assert!(on.validate(Some(&clickhouse()), false).unwrap_err().contains("enable_analytics"));
    }

    #[test]
    fn sink_options_are_valid() {
        let options = sink_options();
        assert_eq!(options.table, "request_routing");
        assert!(options.validate().is_ok());
    }

    #[test]
    fn row_serializes_to_the_table_columns() {
        let row = RequestRoutingRow {
            analytics_id: 42,
            x_request_id: None,
            timestamp: DateTime::parse_from_rfc3339("2026-11-23T12:00:00.250Z")
                .unwrap()
                .with_timezone(&Utc),
            model: "example-alias".to_string(),
            served_model: Some("example-org/example-model".to_string()),
            resolved_serving_class: None,
            served_by: None,
            compute_pool: Some("pool-a".to_string()),
        };
        assert_eq!(
            serde_json::to_value(&row).unwrap(),
            serde_json::json!({
                "analytics_id": 42,
                "x_request_id": null,
                "timestamp": "2026-11-23T12:00:00.250Z",
                "model": "example-alias",
                "served_model": "example-org/example-model",
                "resolved_serving_class": null,
                "served_by": null,
                "compute_pool": "pool-a",
            })
        );
    }
}
