
use crate::metrics::errors::component::TASK_WORKER;
use std::sync::{Arc, OnceLock, Weak};

use anyhow::Result;
use fusillade_arsenal::PostgresRequestManager;
use sqlx::PgPool;
use sqlx_pool_router::PoolProvider;
use tokio_util::sync::CancellationToken;
use underway::Job;

use crate::api::handlers::batches::{CascadeBatchStateInput, CreateBatchInput, build_cascade_batch_state_job, build_create_batch_job};
use crate::api::handlers::users::{PurgeUserDataInput, build_purge_user_data_job};
use crate::connections::sync::{
    ActivateBatchInput, IngestFileInput, SyncConnectionInput, build_activate_batch_job, build_ingest_file_job, build_sync_connection_job,
};

type WeakJobRef<I, P> = Arc<OnceLock<Weak<Job<I, TaskState<P>>>>>;

#[derive(Clone)]
pub struct TaskState<P: PoolProvider + Clone = sqlx_pool_router::DbPools> {
    pub request_manager: Arc<PostgresRequestManager<P>>,
    pub dwctl_pool: sqlx_pool_router::DynPools,
    pub config: crate::SharedConfig,
    pub encryption_key: Option<Vec<u8>>,
    pub ingest_file_job: WeakJobRef<IngestFileInput, P>,
    pub activate_batch_job: WeakJobRef<ActivateBatchInput, P>,
    pub create_batch_job: WeakJobRef<CreateBatchInput, P>,
    pub cascade_batch_state_job: WeakJobRef<CascadeBatchStateInput, P>,
}

impl<P: PoolProvider + Clone> TaskState<P> {
    pub fn get_ingest_file_job(&self) -> anyhow::Result<Arc<Job<IngestFileInput, TaskState<P>>>> {
        self.ingest_file_job
            .get()
            .ok_or_else(|| anyhow::anyhow!("ingest_file_job not initialized"))?
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("ingest_file_job dropped (TaskRunner gone)"))
    }

    pub fn get_activate_batch_job(&self) -> anyhow::Result<Arc<Job<ActivateBatchInput, TaskState<P>>>> {
        self.activate_batch_job
            .get()
            .ok_or_else(|| anyhow::anyhow!("activate_batch_job not initialized"))?
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("activate_batch_job dropped (TaskRunner gone)"))
    }

    pub fn get_create_batch_job(&self) -> anyhow::Result<Arc<Job<CreateBatchInput, TaskState<P>>>> {
        self.create_batch_job
            .get()
            .ok_or_else(|| anyhow::anyhow!("create_batch_job not initialized"))?
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("create_batch_job dropped (TaskRunner gone)"))
    }

    pub fn get_cascade_batch_state_job(&self) -> anyhow::Result<Arc<Job<CascadeBatchStateInput, TaskState<P>>>> {
        self.cascade_batch_state_job
            .get()
            .ok_or_else(|| anyhow::anyhow!("cascade_batch_state_job not initialized"))?
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("cascade_batch_state_job dropped (TaskRunner gone)"))
    }
}

pub struct TaskRunner<P: PoolProvider + Clone + 'static = sqlx_pool_router::DbPools> {
    pub create_batch_job: Arc<Job<CreateBatchInput, TaskState<P>>>,
    pub cascade_batch_state_job: Option<Arc<Job<CascadeBatchStateInput, TaskState<P>>>>,
    pub purge_user_data_job: Arc<Job<PurgeUserDataInput, TaskState<P>>>,
    pub sync_connection_job: Arc<Job<SyncConnectionInput, TaskState<P>>>,
    pub ingest_file_job: Arc<Job<IngestFileInput, TaskState<P>>>,
    pub activate_batch_job: Arc<Job<ActivateBatchInput, TaskState<P>>>,
}

impl<P: PoolProvider + Clone + Send + Sync + 'static> TaskRunner<P> {
    pub async fn new(pool: PgPool, state: TaskState<P>, task_config: &crate::config::TaskWorkersConfig) -> Result<Self> {
        let create_batch_job = Arc::new(build_create_batch_job(pool.clone(), state.clone()).await?);
        let cascade_batch_state_job = if task_config.cascade_batch_state_workers > 0 {
            let job = Arc::new(build_cascade_batch_state_job(pool.clone(), state.clone()).await?);
            state
                .cascade_batch_state_job
                .set(Arc::downgrade(&job))
                .map_err(|_| anyhow::anyhow!("cascade_batch_state_job OnceLock already set"))?;
            Some(job)
        } else {
            None
        };
        let ingest_file_job = Arc::new(build_ingest_file_job(pool.clone(), state.clone()).await?);
        let activate_batch_job = Arc::new(build_activate_batch_job(pool.clone(), state.clone()).await?);
        let purge_user_data_job = Arc::new(build_purge_user_data_job(pool.clone(), state.clone()).await?);
        let sync_connection_job = Arc::new(build_sync_connection_job(pool, state.clone()).await?);

        state
            .ingest_file_job
            .set(Arc::downgrade(&ingest_file_job))
            .map_err(|_| anyhow::anyhow!("ingest_file_job OnceLock already set — double initialization"))?;
        state
            .activate_batch_job
            .set(Arc::downgrade(&activate_batch_job))
            .map_err(|_| anyhow::anyhow!("activate_batch_job OnceLock already set — double initialization"))?;
        state
            .create_batch_job
            .set(Arc::downgrade(&create_batch_job))
            .map_err(|_| anyhow::anyhow!("create_batch_job OnceLock already set — double initialization"))?;

        Ok(Self {
            create_batch_job,
            cascade_batch_state_job,
            purge_user_data_job,
            sync_connection_job,
            ingest_file_job,
            activate_batch_job,
        })
    }

    pub fn start(
        &self,
        shutdown_token: CancellationToken,
        task_config: &crate::config::TaskWorkersConfig,
        sync_config: &crate::config::SyncWorkersConfig,
    ) -> Vec<(&'static str, tokio::task::JoinHandle<()>)> {
        let mut handles: Vec<(&'static str, tokio::task::JoinHandle<()>)> = Vec::new();

        let create_batch_workers = task_config.create_batch_workers.max(1);
        for i in 0..create_batch_workers {
            let mut worker = self.create_batch_job.worker();
            worker.set_shutdown_token(shutdown_token.clone());
            handles.push((
                "create-batch-worker",
                tokio::spawn(async move {
                    if let Err(e) = worker.run().await {
                        crate::background_error!(TASK_WORKER, "create_batch", Error, error = %e, worker = i, "Create-batch worker error");
                    }
                }),
            ));
        }

        if let Some(ref job) = self.cascade_batch_state_job {
            for i in 0..task_config.cascade_batch_state_workers {
                let mut worker = job.worker();
                worker.set_shutdown_token(shutdown_token.clone());
                handles.push((
                    "cascade-batch-state-worker",
                    tokio::spawn(async move {
                        if let Err(e) = worker.run().await {
                            crate::background_error!(TASK_WORKER, "cascade_batch_state", Error, error = %e, worker = i, "Cascade-batch-state worker error");
                        }
                    }),
                ));
            }
        }

        let purge_user_data_workers = task_config.purge_user_data_workers.max(1);
        for i in 0..purge_user_data_workers {
            let mut worker = self.purge_user_data_job.worker();
            worker.set_shutdown_token(shutdown_token.clone());
            handles.push((
                "purge-user-data-worker",
                tokio::spawn(async move {
                    if let Err(e) = worker.run().await {
                        crate::background_error!(TASK_WORKER, "purge_user_data", Error, error = %e, worker = i, "Purge-user-data worker error");
                    }
                }),
            ));
        }


        if !sync_config.enabled {
            tracing::info!("Sync workers disabled on this instance");
            return handles;
        }

        for i in 0..sync_config.discovery_workers {
            let mut worker = self.sync_connection_job.worker();
            worker.set_shutdown_token(shutdown_token.clone());
            handles.push((
                "sync-discovery-worker",
                tokio::spawn(async move {
                    if let Err(e) = worker.run().await {
                        crate::background_error!(TASK_WORKER, "sync_connection", Error, error = %e, worker = i, "Sync-connection worker error");
                    }
                }),
            ));
        }

        for i in 0..sync_config.ingest_workers {
            let mut worker = self.ingest_file_job.worker();
            worker.set_shutdown_token(shutdown_token.clone());
            handles.push((
                "ingest-file-worker",
                tokio::spawn(async move {
                    if let Err(e) = worker.run().await {
                        crate::background_error!(TASK_WORKER, "ingest_file", Error, error = %e, worker = i, "Ingest-file worker error");
                    }
                }),
            ));
        }

        for i in 0..sync_config.activate_workers {
            let mut worker = self.activate_batch_job.worker();
            worker.set_shutdown_token(shutdown_token.clone());
            handles.push((
                "activate-batch-worker",
                tokio::spawn(async move {
                    if let Err(e) = worker.run().await {
                        crate::background_error!(TASK_WORKER, "activate_batch", Error, error = %e, worker = i, "Activate-batch worker error");
                    }
                }),
            ));
        }

        tracing::info!(
            discovery_workers = sync_config.discovery_workers,
            ingest_workers = sync_config.ingest_workers,
            activate_workers = sync_config.activate_workers,
            "Sync workers started"
        );

        handles
    }
}
