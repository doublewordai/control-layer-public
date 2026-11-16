
pub mod generators;

use crate::db::models::deployments::DeploymentDBResponse;
use crate::errors::Result;
use fusillade::{FileId, FileStreamResult, Storage};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use generators::{SampleGenerator, get_generators};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct SampleFilesConfig {
    pub enabled: bool,
    pub requests_per_file: usize,
}

impl Default for SampleFilesConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            requests_per_file: 100,
        }
    }
}

pub fn find_matching_model(deployments: &[DeploymentDBResponse], generator: &dyn SampleGenerator) -> Option<String> {
    deployments
        .iter()
        .find(|d| {
            let type_matches = d.model_type.as_ref() == Some(&generator.required_model_type());

            let required_caps = generator.required_capabilities();
            let caps_match = required_caps.iter().all(|req_cap| {
                d.capabilities
                    .as_ref()
                    .map(|caps| caps.iter().any(|c| c == *req_cap))
                    .unwrap_or(false)
            });

            let caps_ok = required_caps.is_empty() || caps_match;

            type_matches && caps_ok
        })
        .map(|d| d.alias.clone())
}

pub async fn create_sample_files_for_user<S: Storage>(
    storage: &S,
    user_id: Uuid,
    endpoint: &str,
    accessible_deployments: &[DeploymentDBResponse],
    config: &SampleFilesConfig,
) -> Result<Vec<FileId>> {
    use fusillade::{FileMetadata, FileStreamItem};
    use futures::stream;

    let mut created_files = Vec::new();

    for generator in get_generators() {
        let Some(model_alias) = find_matching_model(accessible_deployments, generator.as_ref()) else {
            tracing::debug!(
                generator = generator.name(),
                user_id = %user_id,
                "Skipping sample file - no matching model"
            );
            continue;
        };

        let templates = generator.generate(&model_alias, endpoint, config.requests_per_file);

        let size_bytes: i64 = templates
            .iter()
            .map(|t| {
                serde_json::to_string(t)
                    .map(|s| s.len() as i64 + 1)
                    .unwrap_or(0)
            })
            .sum();

        let mut items = vec![FileStreamItem::Metadata(FileMetadata {
            filename: Some(generator.name().to_string()),
            description: Some(generator.description().to_string()),
            purpose: Some("batch".to_string()),
            uploaded_by: Some(user_id.to_string()),
            size_bytes: Some(size_bytes),
            ..Default::default()
        })];

        for template in templates {
            items.push(FileStreamItem::Template(template));
        }

        let file_id = expect_sample_file_id(
            generator.name(),
            storage
                .create_file_stream(stream::iter(items))
                .await
                .map_err(|e| crate::errors::Error::Internal {
                    operation: format!("create sample file '{}': {}", generator.name(), e),
                })?,
        )?;

        tracing::info!(
            generator = generator.name(),
            file_id = %file_id,
            user_id = %user_id,
            request_count = config.requests_per_file,
            "Created sample file for user"
        );

        created_files.push(file_id);
    }

    Ok(created_files)
}

fn expect_sample_file_id(generator_name: &str, result: FileStreamResult) -> Result<FileId> {
    match result {
        FileStreamResult::Success(file_id) => Ok(file_id),
        FileStreamResult::Aborted => Err(crate::errors::Error::Internal {
            operation: format!(
                "create sample file '{}': fusillade aborted an internally generated stream",
                generator_name
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::models::deployments::ModelType;

    fn create_test_deployment(alias: &str, model_type: ModelType, capabilities: Option<Vec<String>>) -> DeploymentDBResponse {
        DeploymentDBResponse {
            id: uuid::Uuid::new_v4(),
            model_name: alias.to_string(),
            alias: alias.to_string(),
            display_name: None,
            description: None,
            model_type: Some(model_type),
            capabilities,
            hosted_on: Some(uuid::Uuid::new_v4()),
            realtime_inflight_limit: 14,
            batch_capacity: None,
            throughput: None,
            status: crate::db::models::deployments::ModelStatus::Active,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            created_by: uuid::Uuid::new_v4(),
            deleted: false,
            last_sync: None,
            provider_pricing: None,
            is_composite: false,
            lb_strategy: crate::db::models::deployments::LoadBalancingStrategy::default(),
            fallback_enabled: true,
            fallback_on_rate_limit: true,
            fallback_on_status: vec![429, 499, 500, 502, 503, 504],
            fallback_realtime_on_status: Vec::new(),
            fallback_with_replacement: false,
            fallback_max_attempts: None,
            backoff_enabled: false,
            backoff_initial_ms: 100,
            backoff_max_ms: 5_000,
            backoff_factor: 2.0,
            backoff_jitter: "full".to_string(),
            backoff_max_total_ms: None,
            first_token_timeout_ms: None,
            aimd: None,
            affinity: None,
            sanitize_responses: true,
            trusted: false,
            reasoning_translation_overrides: None,
            allowed_batch_completion_windows: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
            provisioning_source: None,
        }
    }

    #[test]
    fn test_find_matching_model_chat() {
        let deployments = vec![
            create_test_deployment("gpt-4", ModelType::Chat, None),
            create_test_deployment("text-embedding-ada", ModelType::Embeddings, None),
        ];

        let chat_generator = generators::ChatGenerator;
        let result = find_matching_model(&deployments, &chat_generator);
        assert_eq!(result, Some("gpt-4".to_string()));
    }

    #[test]
    fn test_find_matching_model_embeddings() {
        let deployments = vec![
            create_test_deployment("gpt-4", ModelType::Chat, None),
            create_test_deployment("text-embedding-ada", ModelType::Embeddings, None),
        ];

        let embeddings_generator = generators::EmbeddingsGenerator;
        let result = find_matching_model(&deployments, &embeddings_generator);
        assert_eq!(result, Some("text-embedding-ada".to_string()));
    }

    #[test]
    fn test_find_matching_model_vision_requires_capability() {
        let deployments = vec![
            create_test_deployment("gpt-4", ModelType::Chat, None),
            create_test_deployment("gpt-4-vision", ModelType::Chat, Some(vec!["vision".to_string()])),
        ];

        let vision_generator = generators::VisionGenerator;
        let result = find_matching_model(&deployments, &vision_generator);
        assert_eq!(result, Some("gpt-4-vision".to_string()));
    }

    #[test]
    fn test_expect_sample_file_id_returns_internal_error_on_aborted() {
        let err = super::expect_sample_file_id("Sample: Chat", FileStreamResult::Aborted)
            .expect_err("aborted result should map to internal error");

        match err {
            crate::errors::Error::Internal { operation } => {
                assert!(operation.contains("Sample: Chat"));
                assert!(operation.contains("aborted an internally generated stream"));
            }
            other => panic!("Expected Internal error, got {other:?}"),
        }
    }

    #[test]
    fn test_find_matching_model_no_match() {
        let deployments = vec![create_test_deployment("gpt-4", ModelType::Chat, None)];

        let embeddings_generator = generators::EmbeddingsGenerator;
        let result = find_matching_model(&deployments, &embeddings_generator);
        assert_eq!(result, None);
    }

    #[test]
    fn test_find_matching_model_vision_without_capability_no_match() {
        let deployments = vec![create_test_deployment("gpt-4", ModelType::Chat, None)];

        let vision_generator = generators::VisionGenerator;
        let result = find_matching_model(&deployments, &vision_generator);
        assert_eq!(result, None);
    }
}
