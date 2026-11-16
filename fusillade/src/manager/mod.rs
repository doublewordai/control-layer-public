
use std::sync::Arc;

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::error::Result;
use crate::http::HttpClient;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::PostgresDaemon;

pub use fusillade_core::manager::{
    ArchiveOutcome, BackgroundClaimKind, DaemonStorage, ModelFilter, ModelFilterState,
    RetainedResponseArchiveCutoffs, RetainedResponseArchiveOutcome,
    RetainedResponseMaintenanceError, RetainedResponsePartitionRunway,
    RetainedResponseRetirementOutcome, RetainedResponseWriteError, RetentionPolicy, Storage,
    TrailingDemandCount,
};

#[async_trait]
pub trait DaemonExecutor<H: HttpClient>: Send + Sync {
    fn http_client(&self) -> &Arc<H>;

    fn config(&self) -> &crate::daemon::DaemonConfig;

    fn run(
        self: Arc<Self>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let mode = self.config().mode;
        self.run_with_mode(shutdown_token, mode)
    }

    fn run_with_mode(
        self: Arc<Self>,
        shutdown_token: tokio_util::sync::CancellationToken,
        mode: crate::daemon::DaemonMode,
    ) -> Result<JoinHandle<Result<()>>>;
}
