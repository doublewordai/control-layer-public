
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
enum ActivateError {
    #[error("{0}")]
    Fatal(String),
    #[error("{0}")]
    Retryable(String),
}


#[derive(Debug, Serialize, Deserialize)]
pub struct SyncConnectionInput {
    pub sync_id: Uuid,
    pub connection_id: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IngestFileInput {
    pub sync_id: Uuid,
    pub sync_entry_id: Uuid,
    pub connection_id: Uuid,
    pub external_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ActivateBatchInput {
    pub sync_id: Uuid,
    pub sync_entry_id: Uuid,
    pub connection_id: Uuid,
    pub file_id: Uuid,
    pub template_count: i32,
}


use crate::tasks::TaskState;
use sqlx_pool_router::PoolProvider;

pub async fn build_sync_connection_job<P: PoolProvider + Clone + Send + Sync + 'static>(
    pool: sqlx::PgPool,
    state: TaskState<P>,
) -> anyhow::Result<underway::Job<SyncConnectionInput, TaskState<P>>> {
    use underway::Job;
    use underway::job::To;
    use underway::task::Error as TaskError;

    Job::<SyncConnectionInput, _>::builder()
        .state(state)
        .step(|cx, input: SyncConnectionInput| async move {
            if let Err(e) = run_sync_connection(&cx.state, &input).await {
                tracing::error!(
                    sync_id = %input.sync_id,
                    error = %e,
                    "SyncConnectionJob failed"
                );
                if let Ok(mut conn) = cx.state.dwctl_pool.write().acquire().await {
                    let _ = crate::db::handlers::connections::SyncOperations::new(&mut conn)
                        .update_status(input.sync_id, "failed")
                        .await;
                }
                return Err(TaskError::Fatal(e.to_string()));
            }
            To::done()
        })
        .name("sync-connection")
        .pool(pool)
        .build()
        .await
        .map_err(Into::into)
}

pub async fn build_ingest_file_job<P: PoolProvider + Clone + Send + Sync + 'static>(
    pool: sqlx::PgPool,
    state: TaskState<P>,
) -> anyhow::Result<underway::Job<IngestFileInput, TaskState<P>>> {
    use underway::Job;
    use underway::job::To;
    use underway::task::Error as TaskError;

    Job::<IngestFileInput, _>::builder()
        .state(state)
        .step(|cx, input: IngestFileInput| async move {
            match run_ingest_file(&cx.state, &input).await {
                Ok(()) => To::done(),
                Err(e) => {
                    tracing::error!(
                        sync_entry_id = %input.sync_entry_id,
                        external_key = %input.external_key,
                        error = %e,
                        "IngestFileJob failed"
                    );
                    if let Ok(mut conn) = cx.state.dwctl_pool.write().acquire().await {
                        let _ = crate::db::handlers::connections::SyncEntries::new(&mut conn)
                            .update_status(input.sync_entry_id, "failed", Some(&e.to_string()))
                            .await;
                        let _ = crate::db::handlers::connections::SyncOperations::new(&mut conn)
                            .increment_counter(input.sync_id, "files_failed")
                            .await;
                        let _ = crate::db::handlers::connections::SyncOperations::new(&mut conn)
                            .try_complete(input.sync_id)
                            .await;
                    }
                    Err(TaskError::Fatal(e.to_string()))
                }
            }
        })
        .name("ingest-file")
        .pool(pool)
        .build()
        .await
        .map_err(Into::into)
}

pub async fn build_activate_batch_job<P: PoolProvider + Clone + Send + Sync + 'static>(
    pool: sqlx::PgPool,
    state: TaskState<P>,
) -> anyhow::Result<underway::Job<ActivateBatchInput, TaskState<P>>> {
    use underway::Job;
    use underway::job::To;
    use underway::task::Error as TaskError;

    let retry_policy = underway::task::RetryPolicy::builder()
        .max_attempts(10_000)
        .initial_interval_ms(5_000)
        .max_interval_ms(300_000)
        .backoff_coefficient(2.0)
        .build();

    Job::<ActivateBatchInput, _>::builder()
        .state(state)
        .step(|cx, input: ActivateBatchInput| async move {
            match run_activate_batch(&cx.state, &input).await {
                Ok(()) => {
                    if let Ok(mut conn) = cx.state.dwctl_pool.write().acquire().await {
                        let _ = crate::db::handlers::connections::SyncOperations::new(&mut conn)
                            .try_complete(input.sync_id)
                            .await;
                    }
                    To::done()
                }
                Err(e) => {
                    let is_fatal = e
                        .downcast_ref::<ActivateError>()
                        .is_some_and(|ae| matches!(ae, ActivateError::Fatal(_)));

                    tracing::error!(
                        sync_entry_id = %input.sync_entry_id,
                        retryable = !is_fatal,
                        error = %e,
                        "ActivateBatchJob failed"
                    );

                    if !is_fatal {
                        return Err(TaskError::Retryable(e.to_string()));
                    }

                    if let Ok(mut conn) = cx.state.dwctl_pool.write().acquire().await {
                        let _ = crate::db::handlers::connections::SyncEntries::new(&mut conn)
                            .update_status(input.sync_entry_id, "failed", Some(&e.to_string()))
                            .await;
                        let _ = crate::db::handlers::connections::SyncOperations::new(&mut conn)
                            .increment_counter(input.sync_id, "files_failed")
                            .await;
                        let _ = crate::db::handlers::connections::SyncOperations::new(&mut conn)
                            .try_complete(input.sync_id)
                            .await;
                    }
                    Err(TaskError::Fatal(e.to_string()))
                }
            }
        })
        .retry_policy(retry_policy)
        .name("activate-batch")
        .pool(pool)
        .build()
        .await
        .map_err(Into::into)
}


async fn run_sync_connection<P: PoolProvider + Clone + Send + Sync + 'static>(
    state: &TaskState<P>,
    input: &SyncConnectionInput,
) -> anyhow::Result<()> {
    use crate::connections::provider;
    use crate::db::handlers::connections::{Connections, SyncEntries, SyncOperations};

    let dwctl = &state.dwctl_pool;

    let (connection, config_json) = {
        let mut conn = dwctl.write().acquire().await?;
        let connection = Connections::new(&mut conn)
            .get_by_id(input.connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("connection not found: {}", input.connection_id))?;

        let encryption_key = state
            .encryption_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("encryption key not configured"))?;
        let config_json = crate::encryption::decrypt_json(encryption_key, &connection.config_encrypted)?;
        (connection, config_json)
    };

    {
        let mut conn = dwctl.write().acquire().await?;
        SyncOperations::new(&mut conn).update_status(input.sync_id, "listing").await?;
    }

    let sync_op = {
        let mut conn = dwctl.write().acquire().await?;
        SyncOperations::new(&mut conn)
            .get_by_id(input.sync_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("sync operation not found"))?
    };

    let prov = provider::create_provider(&connection.provider, config_json)?;
    let files = match sync_op.strategy.as_str() {
        "snapshot" => prov.list_files().await.map_err(|e| anyhow::anyhow!("{e:#}"))?,
        "select" => {
            let keys: Vec<String> = sync_op
                .strategy_config
                .as_ref()
                .and_then(|c| c.get("file_keys"))
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();

            let key_set: std::collections::HashSet<String> = keys.into_iter().collect();

            let all_files = prov.list_files().await.map_err(|e| anyhow::anyhow!("{e:#}"))?;
            all_files.into_iter().filter(|f| key_set.contains(&f.key)).collect()
        }
        other => anyhow::bail!("unsupported strategy: {other}"),
    };

    let files_found = files.len() as i32;

    let force = sync_op
        .strategy_config
        .as_ref()
        .and_then(|c| c.get("force"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let (new_files, files_skipped) = if force {
        let all: Vec<&provider::ExternalFile> = files.iter().collect();
        (all, 0i32)
    } else {
        let keys_and_dates: Vec<(String, Option<chrono::DateTime<chrono::Utc>>)> =
            files.iter().map(|f| (f.key.clone(), f.last_modified)).collect();

        let already_synced = {
            let mut conn = dwctl.write().acquire().await?;
            SyncEntries::new(&mut conn)
                .find_existing(input.connection_id, &keys_and_dates)
                .await?
        };

        let already_synced_set: std::collections::HashSet<(String, Option<chrono::DateTime<chrono::Utc>>)> =
            already_synced.into_iter().collect();

        let new: Vec<&provider::ExternalFile> = files
            .iter()
            .filter(|f| !already_synced_set.contains(&(f.key.clone(), f.last_modified)))
            .collect();

        let skipped = files_found - new.len() as i32;
        (new, skipped)
    };

    let entry_data: Vec<(String, Option<chrono::DateTime<chrono::Utc>>, Option<i64>)> =
        new_files.iter().map(|f| (f.key.clone(), f.last_modified, f.size_bytes)).collect();

    let entries = {
        let mut conn = dwctl.write().acquire().await?;
        SyncEntries::new(&mut conn)
            .bulk_create(input.sync_id, input.connection_id, &entry_data)
            .await?
    };

    {
        let mut conn = dwctl.write().acquire().await?;
        SyncOperations::new(&mut conn)
            .update_counters(input.sync_id, Some(files_found), Some(files_skipped), None, None, None)
            .await?;
        SyncOperations::new(&mut conn).update_status(input.sync_id, "ingesting").await?;
    }


    for entry in &entries {
        state
            .get_ingest_file_job()?
            .enqueue(&IngestFileInput {
                sync_id: input.sync_id,
                sync_entry_id: entry.id,
                connection_id: input.connection_id,
                external_key: entry.external_key.clone(),
            })
            .await?;
    }

    if entries.is_empty() {
        let mut conn = dwctl.write().acquire().await?;
        SyncOperations::new(&mut conn).update_status(input.sync_id, "completed").await?;
    }

    tracing::info!(
        sync_id = %input.sync_id,
        files_found,
        files_skipped,
        files_new = entries.len(),
        "Sync discovery complete"
    );

    Ok(())
}

pub(crate) async fn run_ingest_file<P: PoolProvider + Clone + Send + Sync + 'static>(
    state: &TaskState<P>,
    input: &IngestFileInput,
) -> anyhow::Result<()> {
    use crate::connections::provider;
    use crate::db::handlers::connections::{Connections, SyncEntries, SyncOperations};
    use fusillade::{FileStreamItem, Storage};

    let dwctl = &state.dwctl_pool;

    {
        let mut conn = dwctl.write().acquire().await?;
        let updated = SyncEntries::new(&mut conn)
            .update_status(input.sync_entry_id, "ingesting", None)
            .await?;
        if !updated {
            tracing::info!(sync_entry_id = %input.sync_entry_id, "Sync entry deleted, skipping ingestion");
            return Ok(());
        }
    }

    let (connection, config_json) = {
        let mut conn = dwctl.write().acquire().await?;
        let connection = Connections::new(&mut conn)
            .get_by_id(input.connection_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("connection not found"))?;
        let encryption_key = state
            .encryption_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("encryption key not configured"))?;
        let config_json = crate::encryption::decrypt_json(encryption_key, &connection.config_encrypted)?;
        (connection, config_json)
    };

    let prov = provider::create_provider(&connection.provider, config_json)?;

    let sync_op = {
        let mut conn = dwctl.write().acquire().await?;
        SyncOperations::new(&mut conn)
            .get_by_id(input.sync_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("sync operation not found"))?
    };

    let api_path = sync_op
        .sync_config
        .get("endpoint")
        .and_then(|v| v.as_str())
        .unwrap_or("/v1/chat/completions")
        .to_string();

    let ai_base_url = sync_op
        .sync_config
        .get("ai_base_url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://127.0.0.1:3001/ai")
        .to_string();

    let (file_owner, file_api_key_id) = {
        use crate::db::handlers::api_keys::ApiKeys;
        use crate::db::models::api_keys::ApiKeyPurpose;

        let owner_id = connection.user_id;
        let triggered_by = sync_op.triggered_by;

        let mut conn = dwctl.write().acquire().await?;
        let key_id = ApiKeys::new(&mut conn)
            .get_or_create_hidden_key(owner_id, ApiKeyPurpose::Batch, triggered_by)
            .await
            .map_err(|e| anyhow::anyhow!("resolve file API key: {e}"))?;

        (owner_id, key_id)
    };

    let byte_stream = prov
        .stream_file(&input.external_key)
        .await
        .map_err(|e| anyhow::anyhow!("stream file: {e}"))?;

    let (tx, rx) = tokio::sync::mpsc::channel::<FileStreamItem>(64);

    let external_key = input.external_key.clone();
    let connection_id = input.connection_id;
    let producer = tokio::spawn(async move {
        use futures::StreamExt;

        let display_name = external_key.rsplit('/').next().unwrap_or(&external_key).to_string();

        let metadata = fusillade::FileMetadata {
            filename: Some(display_name),
            description: Some(format!("Synced from external source: {external_key}")),
            purpose: Some("batch".to_string()),
            uploaded_by: Some(file_owner.to_string()),
            api_key_id: Some(file_api_key_id),
            source_connection_id: Some(connection_id),
            source_external_key: Some(external_key.clone()),
            ..Default::default()
        };

        if tx.send(FileStreamItem::Metadata(metadata)).await.is_err() {
            return (0i32, 0i32, Vec::new());
        }

        let mut line_buf = String::new();
        let mut template_count: i32 = 0;
        let mut skipped_lines: i32 = 0;
        const MAX_VALIDATION_ERRORS: usize = 1000;
        const MAX_LINE_WARNINGS: i32 = 10;
        let mut validation_errors: Vec<(i32, u64, String)> = Vec::new();
        let mut line_number: u64 = 0;
        let mut stream = byte_stream;
        let mut utf8_buf: Vec<u8> = Vec::new();

        while let Some(chunk_result) = stream.next().await {
            let chunk = match chunk_result {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(error = %e, "Error reading from provider stream");
                    let _ = tx.send(FileStreamItem::Abort).await;
                    return (template_count, skipped_lines, validation_errors);
                }
            };

            utf8_buf.extend_from_slice(&chunk);

            let drain_up_to = match std::str::from_utf8(&utf8_buf) {
                Ok(s) => {
                    line_buf.push_str(s);
                    utf8_buf.len()
                }
                Err(e) => {
                    let valid = e.valid_up_to();
                    if valid > 0 {
                        let s = unsafe { std::str::from_utf8_unchecked(&utf8_buf[..valid]) };
                        line_buf.push_str(s);
                    }
                    if let Some(error_len) = e.error_len() {
                        line_buf.push(char::REPLACEMENT_CHARACTER);
                        valid + error_len
                    } else {
                        valid
                    }
                }
            };
            if drain_up_to > 0 {
                utf8_buf.drain(..drain_up_to);
            }

            let mut cursor = 0;
            while let Some(rel_pos) = line_buf[cursor..].find('\n') {
                let newline_pos = cursor + rel_pos;
                let line = line_buf[cursor..newline_pos].trim();
                cursor = newline_pos + 1;

                if line.is_empty() {
                    continue;
                }
                line_number += 1;

                match serde_json::from_str::<serde_json::Value>(line) {
                    Ok(parsed) => {
                        let custom_id = parsed.get("custom_id").and_then(|v| v.as_str()).map(|s| s.to_string());
                        let method = parsed.get("method").and_then(|v| v.as_str()).unwrap_or("POST").to_string();
                        let url = api_path.clone();
                        let body = parsed.get("body").map(|v| v.to_string()).unwrap_or_else(|| "{}".to_string());
                        let model = parsed
                            .get("body")
                            .and_then(|b| b.get("model"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();

                        let mut line_error: Option<String> = None;

                        if !matches!(method.as_str(), "POST" | "GET" | "PUT" | "PATCH" | "DELETE") {
                            line_error = Some(format!("invalid HTTP method: {method}"));
                        } else if model.is_empty() {
                            line_error = Some("missing model field in body".to_string());
                        } else {
                            const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;
                            if body.len() > MAX_BODY_SIZE {
                                line_error = Some(format!("oversized body: {} bytes", body.len()));
                            }
                        }
                        if line_error.is_none()
                            && let Some(ref cid) = custom_id
                            && cid.chars().any(|c| c.is_control())
                        {
                            line_error = Some("control characters in custom_id".to_string());
                        }

                        if let Some(ref err) = line_error {
                            if validation_errors.len() < MAX_LINE_WARNINGS as usize {
                                tracing::warn!(line_num = line_number, error = %err, "Validation error (tier 2), ingesting template with error");
                            } else {
                                tracing::debug!(line_num = line_number, error = %err, "Validation error (tier 2), ingesting template with error");
                            }
                            if validation_errors.len() < MAX_VALIDATION_ERRORS {
                                validation_errors.push((template_count, line_number, err.clone()));
                            }
                        }

                        let body = if line_error.is_some() {
                            "{}".to_string()
                        } else {
                            if let Ok(mut body_val) = serde_json::from_str::<serde_json::Value>(&body) {
                                if body_val.as_object_mut().is_some_and(|o| o.shift_remove("priority").is_some()) {
                                    serde_json::to_string(&body_val).unwrap_or(body)
                                } else {
                                    body
                                }
                            } else {
                                body
                            }
                        };

                        let template = fusillade::RequestTemplateInput {
                            custom_id,
                            endpoint: ai_base_url.clone(),
                            method,
                            path: url,
                            body,
                            model,
                        };

                        if tx.send(FileStreamItem::Template(template)).await.is_err() {
                            return (template_count, skipped_lines, validation_errors);
                        }
                        template_count += 1;
                    }
                    Err(e) => {
                        if skipped_lines < MAX_LINE_WARNINGS {
                            tracing::warn!(line_num = line_number, error = %e, "Skipping non-JSON line (tier 1)");
                        } else {
                            tracing::debug!(line_num = line_number, error = %e, "Skipping non-JSON line (tier 1)");
                        }
                        skipped_lines += 1;
                    }
                }
            }
            if cursor > 0 {
                line_buf.drain(..cursor);
            }
        }

        if !utf8_buf.is_empty() {
            if let Ok(s) = std::str::from_utf8(&utf8_buf) {
                line_buf.push_str(s);
            } else {
                tracing::warn!("Discarding {} trailing bytes (invalid UTF-8)", utf8_buf.len());
            }
        }

        let remaining = line_buf.trim();
        if !remaining.is_empty() {
            line_number += 1;
            match serde_json::from_str::<serde_json::Value>(remaining) {
                Ok(parsed) => {
                    let custom_id = parsed.get("custom_id").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let method = parsed.get("method").and_then(|v| v.as_str()).unwrap_or("POST").to_string();
                    let url = api_path.clone();
                    let body = parsed.get("body").map(|v| v.to_string()).unwrap_or_else(|| "{}".to_string());
                    let model = parsed
                        .get("body")
                        .and_then(|b| b.get("model"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    let mut line_error: Option<String> = None;

                    if !matches!(method.as_str(), "POST" | "GET" | "PUT" | "PATCH" | "DELETE") {
                        line_error = Some(format!("invalid HTTP method: {method}"));
                    } else if model.is_empty() {
                        line_error = Some("missing model field in body".to_string());
                    } else {
                        const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;
                        if body.len() > MAX_BODY_SIZE {
                            line_error = Some(format!("oversized body: {} bytes", body.len()));
                        }
                    }
                    if line_error.is_none()
                        && let Some(ref cid) = custom_id
                        && cid.chars().any(|c| c.is_control())
                    {
                        line_error = Some("control characters in custom_id".to_string());
                    }

                    if let Some(ref err) = line_error {
                        if validation_errors.len() < MAX_LINE_WARNINGS as usize {
                            tracing::warn!(line_num = line_number, error = %err, "Validation error (tier 2), ingesting template with error");
                        }
                        if validation_errors.len() < MAX_VALIDATION_ERRORS {
                            validation_errors.push((template_count, line_number, err.clone()));
                        }
                    }

                    let body = if line_error.is_some() {
                        "{}".to_string()
                    } else {
                        if let Ok(mut body_val) = serde_json::from_str::<serde_json::Value>(&body) {
                            if body_val.as_object_mut().is_some_and(|o| o.shift_remove("priority").is_some()) {
                                serde_json::to_string(&body_val).unwrap_or(body)
                            } else {
                                body
                            }
                        } else {
                            body
                        }
                    };

                    let template = fusillade::RequestTemplateInput {
                        custom_id,
                        endpoint: ai_base_url.clone(),
                        method,
                        path: url,
                        body,
                        model,
                    };
                    if tx.send(FileStreamItem::Template(template)).await.is_err() {
                        return (template_count, skipped_lines, validation_errors);
                    }
                    template_count += 1;
                }
                Err(err) => {
                    if skipped_lines < MAX_LINE_WARNINGS {
                        tracing::warn!(line_num = line_number, error = %err, "Skipping non-JSON line (tier 1)");
                    }
                    skipped_lines += 1;
                }
            }
        }

        if skipped_lines > MAX_LINE_WARNINGS {
            tracing::warn!(
                skipped_lines,
                "File had non-JSON lines (tier 1) — first {MAX_LINE_WARNINGS} logged individually"
            );
        }
        let total_validation_errors = validation_errors.len() as i32;
        if total_validation_errors > MAX_LINE_WARNINGS {
            tracing::warn!(
                total_validation_errors,
                "File had validation errors (tier 2) — first {MAX_LINE_WARNINGS} logged individually"
            );
        }

        (template_count, skipped_lines, validation_errors)
    });

    let rx_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let result = state.request_manager.create_file_stream(rx_stream).await;

    let (template_count, skipped_lines, validation_errors) = producer.await.map_err(|e| anyhow::anyhow!("producer task panicked: {e}"))?;

    match result {
        Ok(fusillade::FileStreamResult::Success(file_id)) => {
            let validation_errors_json = if validation_errors.is_empty() {
                None
            } else {
                let errors: Vec<serde_json::Value> = validation_errors
                    .iter()
                    .map(|(idx, line, err)| serde_json::json!({"template_index": idx, "line": line, "error": err}))
                    .collect();
                Some(serde_json::json!(errors))
            };

            let mut conn = dwctl.write().acquire().await?;
            let updated = SyncEntries::new(&mut conn)
                .set_ingested(
                    input.sync_entry_id,
                    file_id.0,
                    template_count,
                    skipped_lines,
                    validation_errors_json.as_ref(),
                )
                .await?;
            if !updated {
                tracing::info!(sync_entry_id = %input.sync_entry_id, "Sync entry deleted during ingestion, skipping activation");
                return Ok(());
            }
            if template_count == 0 {
                SyncEntries::new(&mut conn)
                    .update_status(
                        input.sync_entry_id,
                        "failed",
                        Some("No valid requests found in file — all lines were invalid or unparseable"),
                    )
                    .await?;
                SyncOperations::new(&mut conn)
                    .increment_counter(input.sync_id, "files_failed")
                    .await?;
                let _ = SyncOperations::new(&mut conn).try_complete(input.sync_id).await;
                tracing::info!(
                    sync_entry_id = %input.sync_entry_id,
                    skipped_lines,
                    validation_errors = validation_errors.len(),
                    "File has no valid requests, marked as failed"
                );
                return Ok(());
            }

            SyncOperations::new(&mut conn)
                .increment_counter(input.sync_id, "files_ingested")
                .await?;

            state
                .get_activate_batch_job()?
                .enqueue(&ActivateBatchInput {
                    sync_id: input.sync_id,
                    sync_entry_id: input.sync_entry_id,
                    connection_id: input.connection_id,
                    file_id: file_id.0,
                    template_count,
                })
                .await?;

            tracing::info!(
                sync_entry_id = %input.sync_entry_id,
                file_id = %file_id,
                template_count,
                skipped_lines,
                validation_error_count = validation_errors.len(),
                "File ingested"
            );

            Ok(())
        }
        Ok(fusillade::FileStreamResult::Aborted) => {
            anyhow::bail!("file ingestion aborted during streaming")
        }
        Err(e) => {
            anyhow::bail!("file ingestion failed: {e}")
        }
    }
}

pub(crate) async fn run_activate_batch<P: PoolProvider + Clone + Send + Sync + 'static>(
    state: &TaskState<P>,
    input: &ActivateBatchInput,
) -> anyhow::Result<()> {
    use crate::db::handlers::connections::{SyncEntries, SyncOperations};
    use fusillade::Storage;

    let dwctl = &state.dwctl_pool;

    {
        let mut conn = dwctl.write().acquire().await?;
        let updated = SyncEntries::new(&mut conn)
            .update_status(input.sync_entry_id, "activating", None)
            .await?;
        if !updated {
            tracing::info!(sync_entry_id = %input.sync_entry_id, "Sync entry deleted, skipping batch activation");
            return Ok(());
        }
    }

    let sync_op = {
        let mut conn = dwctl.write().acquire().await?;
        SyncOperations::new(&mut conn)
            .get_by_id(input.sync_id)
            .await?
            .ok_or_else(|| ActivateError::Fatal("sync operation not found".into()))?
    };

    let endpoint = sync_op
        .sync_config
        .get("endpoint")
        .and_then(|v| v.as_str())
        .unwrap_or("/v1/chat/completions")
        .to_string();
    let completion_window = sync_op
        .sync_config
        .get("completion_window")
        .and_then(|v| v.as_str())
        .unwrap_or("24h")
        .to_string();

    let sync_entry = {
        let mut conn = dwctl.write().acquire().await?;
        SyncEntries::new(&mut conn)
            .get_by_id(input.sync_entry_id)
            .await?
            .ok_or_else(|| ActivateError::Fatal(format!("sync entry not found: {}", input.sync_entry_id)))?
    };
    let external_key = sync_entry.external_key.clone();

    if sync_entry.batch_id.is_none() {
        use crate::db::handlers::users::Users;

        let (owner_id, owner_verified) = {
            let mut conn = dwctl.write().acquire().await?;
            let owner_id = crate::db::handlers::connections::Connections::new(&mut conn)
                .get_by_id(input.connection_id)
                .await?
                .ok_or_else(|| ActivateError::Fatal("connection not found".into()))?
                .user_id;
            let verified = Users::new(&mut conn).is_verified(owner_id).await?;
            (owner_id, verified)
        };

        match crate::api::handlers::unverified_volume::enforce_unverified_volume_limit(
            &*state.request_manager,
            state.config.snapshot().batches.unverified_requests_per_completion_hour,
            owner_id,
            owner_verified,
            &completion_window,
            input.template_count as i64,
            crate::api::handlers::unverified_volume::SubmissionKind::Batch,
        )
        .await
        {
            Ok(()) => {}
            Err(crate::errors::Error::TooManyRequests { message }) => {
                let entry_message = format!(
                    "Skipped — this file wasn't batched because it would exceed your unverified \
                     upload limit. It will be retried automatically on your next sync. {message}"
                );
                let mut conn = dwctl.write().acquire().await?;
                SyncEntries::new(&mut conn)
                    .update_status(input.sync_entry_id, "skipped", Some(&entry_message))
                    .await?;
                tracing::info!(
                    sync_entry_id = %input.sync_entry_id,
                    owner = %owner_id,
                    requested = input.template_count,
                    "Sync entry skipped: unverified upload-volume cap exceeded"
                );
                return Ok(());
            }
            Err(e) => return Err(anyhow::anyhow!("unverified upload-volume check: {e}")),
        }
    }

    let batch_already_populated = if let Some(existing_batch_id) = sync_entry.batch_id {
        let batch = state.request_manager.get_batch(fusillade::BatchId(existing_batch_id)).await;
        match batch {
            Ok(b) => b.pending_requests + b.in_progress_requests + b.completed_requests + b.failed_requests > 0,
            Err(_) => false,
        }
    } else {
        false
    };

    let file_stats = if batch_already_populated {
        Vec::new()
    } else {
        state
            .request_manager
            .get_file_template_stats(fusillade::FileId(input.file_id))
            .await
            .map_err(|e| anyhow::anyhow!("get file template stats: {e}"))?
    };

    let reservation_ids = if batch_already_populated {
        Vec::new()
    } else {
        use crate::api::handlers::sla_capacity::{CapacityError, CapacityReservationInput, reserve_capacity};
        use crate::db::handlers::deployments::Deployments;

        let file_model_counts: std::collections::HashMap<String, i64> = file_stats
            .iter()
            .filter(|s| !s.model.is_empty())
            .map(|s| (s.model.clone(), s.request_count))
            .collect();

        if file_model_counts.is_empty() {
            Vec::new()
        } else {
            let model_aliases: Vec<String> = file_model_counts.keys().cloned().collect();

            let batch_model_info = {
                let mut conn = dwctl.write().acquire().await?;
                Deployments::new(&mut conn).get_batch_model_info(&model_aliases).await?
            };

            let model_ids_by_alias = {
                let mut conn = dwctl.write().acquire().await?;
                Deployments::new(&mut conn).get_model_ids_by_aliases(&model_aliases).await?
            };

            let missing: Vec<&str> = model_aliases
                .iter()
                .filter(|a| !model_ids_by_alias.contains_key(*a))
                .map(|a| a.as_str())
                .collect();
            if !missing.is_empty() {
                return Err(ActivateError::Fatal(format!("model(s) no longer available: {}", missing.join(", "))).into());
            }

            let config = state.config.snapshot();
            let cap_input = CapacityReservationInput {
                completion_window: &completion_window,
                file_model_counts: &file_model_counts,
                model_throughputs: &batch_model_info.throughputs,
                model_ids_by_alias: &model_ids_by_alias,
                default_throughput: config.batches.default_throughput,
                relaxation_factor: config.batches.relaxation_factor(&completion_window),
                reservation_ttl_secs: config.batches.reservation_ttl_secs,
                include_pending_counts: config.batches.pending_capacity_counts_enabled,
            };

            match reserve_capacity(&dwctl.write(), &*state.request_manager, &cap_input).await {
                Ok(ids) => ids,
                Err(CapacityError::InsufficientCapacity { completion_window, models }) => {
                    return Err(ActivateError::Retryable(format!(
                        "insufficient capacity for {completion_window} window (models: {models})"
                    ))
                    .into());
                }
                Err(CapacityError::Internal(msg)) => {
                    return Err(anyhow::anyhow!("capacity reservation: {msg}"));
                }
            }
        }
    };

    let release_guard = {
        let pool = state.dwctl_pool.clone();
        let ids = reservation_ids.clone();
        scopeguard::guard((), move |()| {
            if !ids.is_empty() {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    handle.spawn(async move {
                        if let Err(e) = crate::api::handlers::sla_capacity::release_reservations(&pool.write(), &ids).await {
                            tracing::warn!(error = %e, "Failed to release capacity reservations — will expire via TTL");
                        }
                    });
                } else {
                    tracing::debug!("No Tokio runtime available for reservation release — TTL will handle cleanup");
                }
            }
        })
    };

    let (batch_owner, api_key_id, connection_name) = {
        use crate::db::handlers::api_keys::ApiKeys;
        use crate::db::models::api_keys::ApiKeyPurpose;

        let mut conn = dwctl.write().acquire().await?;
        let connection = crate::db::handlers::connections::Connections::new(&mut conn)
            .get_by_id(input.connection_id)
            .await?
            .ok_or_else(|| ActivateError::Fatal("connection not found".into()))?;

        let owner_id = connection.user_id;
        let conn_name = connection.name.clone();
        let triggered_by = sync_op.triggered_by;

        let mut keys = ApiKeys::new(&mut conn);
        let key_id = keys
            .get_or_create_hidden_key(owner_id, ApiKeyPurpose::Batch, triggered_by)
            .await
            .map_err(|e| anyhow::anyhow!("resolve batch API key: {e}"))?;
        (owner_id, key_id, conn_name)
    };

    let batch_id = if let Some(existing) = sync_entry.batch_id {
        tracing::info!(batch_id = %existing, "Reusing batch from previous attempt");
        existing
    } else {
        let models: Vec<String> = file_stats.iter().map(|s| s.model.clone()).filter(|m| !m.is_empty()).collect();
        let model_label = crate::api::handlers::batches::batch_model_label(&models);

        let mut metadata = serde_json::json!({
            "request_source": "sync",
            "dw_source_id": input.connection_id.to_string(),
            "dw_source_name": connection_name,
            "dw_sync_id": input.sync_id.to_string(),
            "dw_external_key": external_key,
        });
        if let Some(label) = model_label {
            metadata["dw_model"] = serde_json::Value::String(label);
        }

        let batch_input = fusillade::BatchInput {
            file_id: fusillade::FileId(input.file_id),
            endpoint,
            completion_window,
            metadata: Some(metadata),
            created_by: Some(batch_owner.to_string()),
            api_key_id: Some(api_key_id),
            total_requests: Some(input.template_count as i64),
        };

        let batch = state
            .request_manager
            .create_batch_record(batch_input)
            .await
            .map_err(|e| anyhow::anyhow!("create batch record: {e}"))?;

        let bid = *batch.id;

        let mut persist_err = None;
        for attempt in 0..3 {
            match dwctl.write().acquire().await {
                Ok(mut conn) => {
                    match sqlx::query!(
                        "UPDATE sync_entries SET batch_id = $2 WHERE id = $1 AND status != 'deleted'",
                        input.sync_entry_id,
                        bid,
                    )
                    .execute(&mut *conn)
                    .await
                    {
                        Ok(result) => {
                            if result.rows_affected() == 0 {
                                tracing::info!(sync_entry_id = %input.sync_entry_id, batch_id = %bid, "Sync entry deleted during activation, failing orphaned batch");
                                let _ = state
                                    .request_manager
                                    .mark_batch_failed(fusillade::BatchId(bid), "sync entry deleted during activation")
                                    .await;
                                return Ok(());
                            }
                            persist_err = None;
                            break;
                        }
                        Err(e) => {
                            tracing::warn!(attempt, error = %e, "Failed to persist batch_id on sync entry, retrying");
                            persist_err = Some(e);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(attempt, error = %e, "Failed to acquire conn for batch_id persist, retrying");
                    persist_err = Some(e);
                }
            }
        }
        if let Some(e) = persist_err {
            let _ = state
                .request_manager
                .mark_batch_failed(fusillade::BatchId(bid), "failed to persist batch_id on sync entry")
                .await;
            anyhow::bail!("persist batch_id on sync entry after 3 attempts: {e}");
        }

        bid
    };

    if let Err(e) = state
        .request_manager
        .populate_batch(fusillade::BatchId(batch_id), fusillade::FileId(input.file_id))
        .await
    {
        return Err(match &e {
            fusillade::FusilladeError::ValidationError(_) => {
                if let Err(mark_err) = state
                    .request_manager
                    .mark_batch_failed(fusillade::BatchId(batch_id), &e.to_string())
                    .await
                {
                    tracing::error!(batch_id = %batch_id, error = %mark_err, "Failed to mark batch as failed after validation error");
                    ActivateError::Retryable(format!("mark_batch_failed: {mark_err}")).into()
                } else {
                    ActivateError::Fatal(format!("populate batch: {e}")).into()
                }
            }
            _ => {
                ActivateError::Retryable(format!("populate batch: {e}")).into()
            }
        });
    }

    match crate::api::handlers::sla_capacity::release_reservations(&state.dwctl_pool.write(), &reservation_ids).await {
        Ok(()) => {
            scopeguard::ScopeGuard::into_inner(release_guard);
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to release capacity reservations after populate — guard will retry on drop");
        }
    }

    if let Some(errors) = &sync_entry.validation_errors
        && let Some(error_list) = errors.as_array()
    {
        let error_indices: Vec<i32> = error_list
            .iter()
            .filter_map(|e| e.get("template_index").and_then(|l| l.as_i64()).map(|l| l as i32))
            .collect();

        if !error_indices.is_empty() {
            let mut transaction = state
                .request_manager
                .begin_write()
                .await
                .map_err(|e| ActivateError::Retryable(format!("begin invalid-request update: {e}")))?;

            let template_ids: Vec<Uuid> =
                sqlx::query_scalar("SELECT id FROM fusillade.request_templates_all WHERE file_id = $1 AND line_number = ANY($2)")
                    .bind(input.file_id)
                    .bind(&error_indices)
                    .fetch_all(&mut *transaction)
                    .await
                    .map_err(|e| ActivateError::Retryable(format!("query templates: {e}")))?;

            if !template_ids.is_empty() {
                let rows = sqlx::query(
                    "UPDATE fusillade.requests SET state = 'failed', error = $1, failed_at = NOW() WHERE batch_id = $2 AND template_id = ANY($3) AND state = 'pending'",
                )
                .bind("Request failed validation during ingestion — check sync entry for details")
                .bind(batch_id)
                .bind(&template_ids)
                .execute(&mut *transaction)
                .await
                .map_err(|e| ActivateError::Retryable(format!("fail invalid requests: {e}")))?;

                tracing::info!(
                    batch_id = %batch_id,
                    failed_count = rows.rows_affected(),
                    "Failed invalid requests from tier 2 validation errors"
                );
            }
            transaction
                .commit()
                .await
                .map_err(|e| ActivateError::Retryable(format!("commit invalid-request update: {e}")))?;
        }
    }

    {
        let mut conn = dwctl.write().acquire().await?;
        SyncEntries::new(&mut conn).set_activated(input.sync_entry_id, batch_id).await?;
        SyncOperations::new(&mut conn)
            .increment_counter(input.sync_id, "batches_created")
            .await?;
    }

    tracing::info!(
        sync_entry_id = %input.sync_entry_id,
        batch_id = %batch_id,
        "Batch activated"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::utils::{create_test_config, create_test_user};
    use fusillade::{FileMetadata, FileStreamItem, RequestTemplateInput, Storage as _};
    use sqlx::PgPool;

    async fn setup_task_state(pool: PgPool) -> crate::tasks::TaskState<sqlx_pool_router::TestDbPools> {
        setup_task_state_with_config(pool, create_test_config()).await
    }

    async fn setup_task_state_with_config(
        pool: PgPool,
        config: crate::config::Config,
    ) -> crate::tasks::TaskState<sqlx_pool_router::TestDbPools> {
        use sqlx::Executor;
        use sqlx::postgres::PgConnectOptions;

        pool.execute("CREATE SCHEMA IF NOT EXISTS fusillade")
            .await
            .expect("create fusillade schema");

        let base_opts: PgConnectOptions = pool.connect_options().as_ref().clone();
        let fusillade_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .min_connections(0)
            .connect_with(base_opts.options([("search_path", "fusillade")]))
            .await
            .expect("fusillade pool");

        fusillade_arsenal::migrator()
            .run(&fusillade_pool)
            .await
            .expect("fusillade migrations");

        let fusillade_test_pools = sqlx_pool_router::TestDbPools::new(fusillade_pool)
            .await
            .expect("fusillade test pools");
        let request_manager = std::sync::Arc::new(fusillade_arsenal::PostgresRequestManager::new(
            fusillade_test_pools,
            Default::default(),
        ));

        crate::tasks::TaskState {
            request_manager,
            dwctl_pool: sqlx_pool_router::DynPools::new(pool),
            config: crate::SharedConfig::new(config),
            encryption_key: None,
            ingest_file_job: std::sync::Arc::new(std::sync::OnceLock::new()),
            activate_batch_job: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    async fn insert_test_connection(pool: &PgPool, user_id: Uuid) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query!(
            r#"INSERT INTO connections (id, user_id, kind, provider, name, config_encrypted)
               VALUES ($1, $2, 'source', 'test', 'test-conn', '\x00')"#,
            id,
            user_id,
        )
        .execute(pool)
        .await
        .expect("insert connection");
        id
    }

    async fn insert_test_sync_op(pool: &PgPool, connection_id: Uuid, triggered_by: Uuid) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query!(
            r#"INSERT INTO sync_operations (id, connection_id, status, strategy, triggered_by, sync_config)
               VALUES ($1, $2, 'running', 'select', $3, $4)"#,
            id,
            connection_id,
            triggered_by,
            serde_json::json!({"endpoint": "/v1/chat/completions", "completion_window": "24h"}),
        )
        .execute(pool)
        .await
        .expect("insert sync_op");
        id
    }

    async fn insert_test_sync_entry(pool: &PgPool, sync_id: Uuid, connection_id: Uuid, external_key: &str) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query!(
            r#"INSERT INTO sync_entries (id, sync_id, connection_id, external_key, status)
               VALUES ($1, $2, $3, $4, 'ingested')"#,
            id,
            sync_id,
            connection_id,
            external_key,
        )
        .execute(pool)
        .await
        .expect("insert sync_entry");
        id
    }

    async fn create_test_file<P: sqlx_pool_router::PoolProvider + Clone + Send + Sync + 'static>(
        state: &crate::tasks::TaskState<P>,
        owner_id: Uuid,
        templates: Vec<RequestTemplateInput>,
    ) -> Uuid {
        let mut items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some("test.jsonl".to_string()),
            purpose: Some("batch".to_string()),
            uploaded_by: Some(owner_id.to_string()),
            ..Default::default()
        })];
        for t in templates {
            items.push(FileStreamItem::Template(t));
        }
        match state
            .request_manager
            .create_file_stream(futures::stream::iter(items))
            .await
            .expect("create_file_stream")
        {
            fusillade::FileStreamResult::Success(file_id) => file_id.0,
            other => panic!("unexpected file stream result: {other:?}"),
        }
    }

    fn valid_template(model: &str) -> RequestTemplateInput {
        RequestTemplateInput {
            custom_id: None,
            endpoint: "http://127.0.0.1:3001/ai".to_string(),
            method: "POST".to_string(),
            path: "/v1/chat/completions".to_string(),
            body: serde_json::json!({"model": model, "messages": [{"role": "user", "content": "hi"}]}).to_string(),
            model: model.to_string(),
        }
    }

    fn invalid_template_missing_model() -> RequestTemplateInput {
        RequestTemplateInput {
            custom_id: None,
            endpoint: "http://127.0.0.1:3001/ai".to_string(),
            method: "POST".to_string(),
            path: "/v1/chat/completions".to_string(),
            body: "{}".to_string(),
            model: String::new(),
        }
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_three_tier_ingestion_and_activation(pool: PgPool) {
        use crate::test::utils::{create_test_endpoint, create_test_model};

        let state = setup_task_state(pool.clone()).await;
        let config = create_test_config();
        let _app_state = crate::test::utils::create_test_app_state_with_config(pool.clone(), config).await;

        let user = create_test_user(&pool, crate::api::models::users::Role::PlatformManager).await;
        let user_id = user.id;

        let endpoint_id = create_test_endpoint(&pool, "test-ep", user_id).await;
        create_test_model(&pool, "gpt-4-internal", "gpt-4", endpoint_id, user_id).await;

        let connection_id = insert_test_connection(&pool, user_id).await;
        let sync_id = insert_test_sync_op(&pool, connection_id, user_id).await;
        let entry_id = insert_test_sync_entry(&pool, sync_id, connection_id, "data/test.jsonl").await;

        let templates = vec![
            valid_template("gpt-4"),
            invalid_template_missing_model(),
            valid_template("gpt-4"),
            invalid_template_missing_model(),
        ];

        let file_id = create_test_file(&state, user_id, templates).await;

        let skipped_lines: i32 = 1;
        let validation_errors_json = serde_json::json!([
            {"template_index": 1, "line": 3, "error": "missing model field in body"},
            {"template_index": 3, "line": 5, "error": "missing model field in body"},
        ]);
        sqlx::query!(
            r#"UPDATE sync_entries SET file_id = $2, template_count = 4,
               skipped_lines = $3, validation_errors = $4
               WHERE id = $1"#,
            entry_id,
            file_id,
            skipped_lines,
            validation_errors_json,
        )
        .execute(&pool)
        .await
        .expect("update sync_entry");

        let input = ActivateBatchInput {
            sync_id,
            sync_entry_id: entry_id,
            connection_id,
            file_id,
            template_count: 4,
        };

        run_activate_batch(&state, &input).await.expect("run_activate_batch");

        let sync_entry = sqlx::query_as::<_, (Uuid, String)>("SELECT batch_id, status FROM sync_entries WHERE id = $1")
            .bind(entry_id)
            .fetch_one(&pool)
            .await
            .expect("fetch sync_entry");
        assert_eq!(sync_entry.1, "activated", "sync entry should be activated");
        let batch_id = fusillade::BatchId(sync_entry.0);

        let batch = state.request_manager.get_batch(batch_id).await.expect("get_batch");
        assert_eq!(
            batch.metadata.as_ref().and_then(|m| m.get("dw_model")).and_then(|v| v.as_str()),
            Some("gpt-4"),
            "sync-created batch should cache its model label as dw_model"
        );

        let requests = state
            .request_manager
            .get_batch_requests(batch_id)
            .await
            .expect("get_batch_requests");

        assert_eq!(requests.len(), 4, "should have 4 requests total");

        let mut pending_count = 0;
        let mut failed_count = 0;
        for req in &requests {
            match req {
                fusillade::AnyRequest::Pending(_) => pending_count += 1,
                fusillade::AnyRequest::Failed(_) => failed_count += 1,
                other => panic!("unexpected request state: {}", other.variant()),
            }
        }

        assert_eq!(pending_count, 2, "2 valid requests should be pending");
        assert_eq!(failed_count, 2, "2 invalid requests should be failed");

        let active_reservations: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(reserved_requests), 0)::BIGINT FROM batch_capacity_reservations WHERE released_at IS NULL AND expires_at > now()",
        )
        .fetch_one(&pool)
        .await
        .expect("query active reservations");
        assert_eq!(
            active_reservations, 0,
            "all capacity reservations should be released after successful activation"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_validation_errors_stored_correctly(pool: PgPool) {
        let state = setup_task_state(pool.clone()).await;

        let user = create_test_user(&pool, crate::api::models::users::Role::PlatformManager).await;
        let user_id = user.id;

        let connection_id = insert_test_connection(&pool, user_id).await;
        let sync_id = insert_test_sync_op(&pool, connection_id, user_id).await;
        let entry_id = insert_test_sync_entry(&pool, sync_id, connection_id, "data/test.jsonl").await;

        let templates = vec![valid_template("gpt-4"), invalid_template_missing_model(), valid_template("gpt-4")];
        let file_id = create_test_file(&state, user_id, templates).await;

        let skipped_lines: i32 = 2;
        let validation_errors_json = serde_json::json!([
            {"template_index": 1, "line": 4, "error": "missing model field in body"},
        ]);

        {
            use crate::db::handlers::connections::SyncEntries;

            let mut conn = pool.acquire().await.expect("acquire conn");
            let updated = SyncEntries::new(&mut conn)
                .set_ingested(
                    entry_id,
                    file_id,
                    3,
                    skipped_lines,
                    Some(&validation_errors_json),
                )
                .await
                .expect("set_ingested");
            assert!(updated, "set_ingested should return true");
        }

        let row = sqlx::query!(
            "SELECT status, skipped_lines, validation_errors, template_count, file_id FROM sync_entries WHERE id = $1",
            entry_id,
        )
        .fetch_one(&pool)
        .await
        .expect("read sync_entry");

        assert_eq!(row.status, "ingested");
        assert_eq!(row.skipped_lines, 2);
        assert_eq!(row.template_count.unwrap(), 3);
        assert_eq!(row.file_id.unwrap(), file_id);

        let errors: Vec<serde_json::Value> = serde_json::from_value(row.validation_errors.unwrap()).expect("parse validation_errors");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0]["template_index"], 1);
        assert_eq!(errors[0]["line"], 4);
        assert_eq!(errors[0]["error"], "missing model field in body");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_activate_batch_stamps_mixed_model_label(pool: PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::{create_test_endpoint, create_test_model};

        let state = setup_task_state(pool.clone()).await;

        let user = create_test_user(&pool, Role::PlatformManager).await;
        let user_id = user.id;

        let endpoint_id = create_test_endpoint(&pool, "test-endpoint", user_id).await;
        create_test_model(&pool, "gpt-4-internal", "gpt-4", endpoint_id, user_id).await;
        create_test_model(&pool, "gpt-3.5-internal", "gpt-3.5", endpoint_id, user_id).await;

        let connection_id = insert_test_connection(&pool, user_id).await;
        let sync_id = insert_test_sync_op(&pool, connection_id, user_id).await;
        let entry_id = insert_test_sync_entry(&pool, sync_id, connection_id, "data/mixed.jsonl").await;

        let templates = vec![valid_template("gpt-4"), valid_template("gpt-3.5")];
        let file_id = create_test_file(&state, user_id, templates).await;

        sqlx::query!(
            "UPDATE sync_entries SET file_id = $2, template_count = 2 WHERE id = $1",
            entry_id,
            file_id,
        )
        .execute(&pool)
        .await
        .expect("update sync_entry");

        let input = ActivateBatchInput {
            sync_id,
            sync_entry_id: entry_id,
            connection_id,
            file_id,
            template_count: 2,
        };

        run_activate_batch(&state, &input).await.expect("run_activate_batch");

        let batch_id: Uuid = sqlx::query_scalar("SELECT batch_id FROM sync_entries WHERE id = $1")
            .bind(entry_id)
            .fetch_one(&pool)
            .await
            .expect("fetch batch_id");
        let batch = state
            .request_manager
            .get_batch(fusillade::BatchId(batch_id))
            .await
            .expect("get_batch");

        assert_eq!(
            batch.metadata.as_ref().and_then(|m| m.get("dw_model")).and_then(|v| v.as_str()),
            Some("mixed"),
            "a multi-model file should cache the collapsed \"mixed\" label"
        );
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_activate_batch_rejects_on_insufficient_capacity(pool: PgPool) {
        use crate::api::models::users::Role;
        use crate::test::utils::{create_test_endpoint, create_test_model};

        let mut config = create_test_config();
        config.batches.default_throughput = 0.001;

        let state = setup_task_state_with_config(pool.clone(), config).await;

        let user = create_test_user(&pool, Role::PlatformManager).await;
        let user_id = user.id;

        let endpoint_id = create_test_endpoint(&pool, "test-endpoint", user_id).await;
        let model_alias = "test-model";
        create_test_model(&pool, "test-model-internal", model_alias, endpoint_id, user_id).await;

        let connection_id = insert_test_connection(&pool, user_id).await;
        let sync_id = insert_test_sync_op(&pool, connection_id, user_id).await;
        let entry_id = insert_test_sync_entry(&pool, sync_id, connection_id, "data/big.jsonl").await;

        let templates: Vec<_> = (0..200).map(|_| valid_template(model_alias)).collect();
        let file_id = create_test_file(&state, user_id, templates).await;

        sqlx::query!(
            "UPDATE sync_entries SET file_id = $2, template_count = 200 WHERE id = $1",
            entry_id,
            file_id,
        )
        .execute(&pool)
        .await
        .expect("update sync_entry");

        let input = ActivateBatchInput {
            sync_id,
            sync_entry_id: entry_id,
            connection_id,
            file_id,
            template_count: 200,
        };

        let err = run_activate_batch(&state, &input).await.unwrap_err();

        let is_retryable = err
            .downcast_ref::<ActivateError>()
            .is_some_and(|ae| matches!(ae, ActivateError::Retryable(_)));
        assert!(is_retryable, "capacity rejection should be retryable, got: {err}");

        let status: String = sqlx::query_scalar("SELECT status FROM sync_entries WHERE id = $1")
            .bind(entry_id)
            .fetch_one(&pool)
            .await
            .expect("fetch status");
        assert_eq!(status, "activating", "sync entry should not be marked failed on capacity rejection");

        let batch_id: Option<Uuid> = sqlx::query_scalar("SELECT batch_id FROM sync_entries WHERE id = $1")
            .bind(entry_id)
            .fetch_one(&pool)
            .await
            .expect("fetch batch_id");
        assert!(batch_id.is_none(), "no batch should be created when capacity is insufficient");
    }

    #[dwctl_test_macros::test]
    #[test_log::test]
    async fn test_activate_batch_skips_over_unverified_cap(pool: PgPool) {
        let mut config = create_test_config();
        config.batches.unverified_requests_per_completion_hour = 1;
        let state = setup_task_state_with_config(pool.clone(), config).await;

        let user = create_test_user(&pool, crate::api::models::users::Role::StandardUser).await;
        let user_id = user.id;

        let connection_id = insert_test_connection(&pool, user_id).await;
        let sync_id = insert_test_sync_op(&pool, connection_id, user_id).await;
        sqlx::query!(
            "UPDATE sync_operations SET sync_config = $2 WHERE id = $1",
            sync_id,
            serde_json::json!({"endpoint": "/v1/chat/completions", "completion_window": "1h"}),
        )
        .execute(&pool)
        .await
        .expect("set 1h window");
        let entry_id = insert_test_sync_entry(&pool, sync_id, connection_id, "data/big.jsonl").await;

        let input = ActivateBatchInput {
            sync_id,
            sync_entry_id: entry_id,
            connection_id,
            file_id: Uuid::new_v4(),
            template_count: 2,
        };

        run_activate_batch(&state, &input)
            .await
            .expect("run_activate_batch should return Ok when skipping");

        let (batch_id, status, error): (Option<Uuid>, String, Option<String>) =
            sqlx::query_as("SELECT batch_id, status, error FROM sync_entries WHERE id = $1")
                .bind(entry_id)
                .fetch_one(&pool)
                .await
                .expect("fetch sync_entry");

        assert_eq!(status, "skipped", "over-cap entry should be skipped, not failed/activated");
        assert!(batch_id.is_none(), "no batch row should be created for a skipped entry");
        let error = error.expect("an actionable error message should be stored");
        assert!(error.contains("unverified upload limit"), "message should explain the cap: {error}");
        assert!(error.contains("next sync"), "message should explain the auto-retry: {error}");
    }
}
