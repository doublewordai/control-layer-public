
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
