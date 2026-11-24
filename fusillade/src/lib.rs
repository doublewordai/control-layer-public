
pub mod bg_errors;
pub mod daemon;
pub mod http;
pub mod manager;
pub mod processor;
pub mod transform;

pub use fusillade_core::{batch, error, request};

pub use daemon::{
    Daemon, DaemonConfig, DaemonMode, ModelEscalationConfig, RetentionMaintenanceConfig,
};
pub use fusillade_core::batch::*;
pub use fusillade_core::error::{FusilladeError, Result};
pub use fusillade_core::request::*;
pub use http::{HttpClient, HttpResponse, MockHttpClient, ReqwestHttpClient};
#[cfg(feature = "postgres")]
pub use manager::PostgresDaemon;
pub use manager::{
    DaemonStorage, ModelFilter, ModelFilterState, RetainedResponseArchiveCutoffs,
    RetainedResponseArchiveOutcome, RetainedResponseMaintenanceError,
    RetainedResponsePartitionRunway, RetainedResponseRetirementOutcome, RetainedResponseWriteError,
    RetentionPolicy, Storage, TrailingDemandCount,
};
pub use processor::{
    CancellationFuture, DefaultRequestProcessor, ProcessContext, RequestProcessor, ShouldRetry,
};
pub use transform::ResponseTransformer;
