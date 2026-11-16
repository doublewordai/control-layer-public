
use crate::request_logging::serializers::HttpAnalyticsRow;

use async_trait::async_trait;

#[async_trait]
pub trait MetricsRecorder: Send + Sync {
    async fn record_from_analytics(&self, row: &HttpAnalyticsRow);
}
