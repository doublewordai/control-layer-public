
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const DEFAULT_PRESET_SENTINEL: &str = "__default__";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct PinnedTolerations(pub Vec<SchedulingToleration>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulingToleration {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<SchedulingOperator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<SchedulingEffect>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum SchedulingOperator {
    Equal,
    Exists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum SchedulingEffect {
    #[serde(rename = "NoSchedule")]
    #[schema(rename = "NoSchedule")]
    NoSchedule,
    #[serde(rename = "PreferNoSchedule")]
    #[schema(rename = "PreferNoSchedule")]
    PreferNoSchedule,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TolerationError {
    #[error("toleration {index}: key must not be empty")]
    EmptyKey { index: usize },
    #[error("toleration {index}: operator Equal needs a value (or set operator: Exists)")]
    EqualNeedsValue { index: usize },
    #[error("toleration {index}: operator Exists must not carry a value")]
    ExistsNeedsNoValue { index: usize },
}

impl PinnedTolerations {
    pub fn validate(&self) -> Result<(), TolerationError> {
        for (index, toleration) in self.0.iter().enumerate() {
            if toleration.key.is_empty() {
                return Err(TolerationError::EmptyKey { index });
            }
            match toleration.operator.unwrap_or(SchedulingOperator::Equal) {
                SchedulingOperator::Equal => {
                    if toleration.value.is_none() {
                        return Err(TolerationError::EqualNeedsValue { index });
                    }
                }
                SchedulingOperator::Exists => {
                    if toleration.value.is_some() {
                        return Err(TolerationError::ExistsNeedsNoValue { index });
                    }
                }
            }
        }
        Ok(())
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("PinnedTolerations always serialises")
    }
}

#[derive(Debug, Clone)]
pub struct TolerationPresets {
    by_id: Arc<arc_swap::ArcSwap<HashMap<String, Arc<serde_json::Value>>>>,
    empty: Arc<serde_json::Value>,
    reported_unknown: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl Default for TolerationPresets {
    fn default() -> Self {
        Self {
            by_id: Arc::new(arc_swap::ArcSwap::from_pointee(HashMap::new())),
            empty: Arc::new(serde_json::json!([])),
            reported_unknown: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }
}

impl TolerationPresets {
    pub fn new(config: &crate::config::SchedulingConfig) -> Self {
        let by_id: HashMap<String, Arc<serde_json::Value>> = config
            .toleration_presets
            .iter()
            .map(|preset| (preset.id.clone(), Arc::new(PinnedTolerations(preset.tolerations.clone()).to_json())))
            .collect();
        Self {
            by_id: Arc::new(arc_swap::ArcSwap::from_pointee(by_id)),
            empty: Arc::new(serde_json::json!([])),
            reported_unknown: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }

    pub fn store(&self, presets: Self) {
        let by_id = presets.by_id.load_full();
        self.by_id.store(by_id);
    }

    pub fn resolve(&self, preset_id: Option<&str>) -> Option<Arc<serde_json::Value>> {
        let id = preset_id?;
        match self.by_id.load().get(id) {
            Some(tolerations) => Some(tolerations.clone()),
            None => {
                if self.mark_unknown_reported(id) {
                    tracing::error!(
                        preset_id = id,
                        "account references a scheduling toleration preset that is not configured; \
                         failing closed to the empty (most restrictive) toleration list"
                    );
                }
                Some(self.empty.clone())
            }
        }
    }

    fn mark_unknown_reported(&self, id: &str) -> bool {
        self.reported_unknown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SchedulingConfig, TolerationPreset};

    fn parse(value: serde_json::Value) -> (PinnedTolerations, serde_json::Value) {
        let pinned: PinnedTolerations = serde_json::from_value(value.clone()).unwrap();
        (pinned, value)
    }

    #[test]
    fn an_empty_list_round_trips() {
        let (pinned, _) = parse(serde_json::json!([]));
        assert_eq!(pinned.to_json(), serde_json::json!([]));
        pinned.validate().unwrap();
    }

    #[test]
    fn entries_keep_only_the_fields_the_client_sent() {
        let (pinned, original) = parse(serde_json::json!([
            {"key": "dedicated", "value": "only", "effect": "NoSchedule"},
            {"key": "gpu", "operator": "Exists"},
        ]));
        assert_eq!(pinned.to_json(), original);
        pinned.validate().unwrap();
    }

    #[test]
    fn equal_without_a_value_and_exists_with_one_are_refused() {
        let (equal, _) = parse(serde_json::json!([{"key": "dedicated"}]));
        assert_eq!(equal.validate(), Err(TolerationError::EqualNeedsValue { index: 0 }));

        let (exists, _) = parse(serde_json::json!([{"key": "dedicated", "operator": "Exists", "value": "nope"}]));
        assert_eq!(exists.validate(), Err(TolerationError::ExistsNeedsNoValue { index: 0 }));

        let (empty_key, _) = parse(serde_json::json!([{"key": ""}]));
        assert_eq!(empty_key.validate(), Err(TolerationError::EmptyKey { index: 0 }));
    }

    #[test]
    fn unknown_fields_and_spellings_are_refused_at_parse() {
        assert!(serde_json::from_value::<PinnedTolerations>(serde_json::json!([{"key": "k", "value": "v", "priority": 1}])).is_err());
        assert!(serde_json::from_value::<PinnedTolerations>(serde_json::json!([{"key": "k", "value": "v", "effect": "Bogus"}])).is_err());
        assert!(
            serde_json::from_value::<PinnedTolerations>(serde_json::json!([{"key": "k", "value": "v", "operator": "Sometimes"}])).is_err()
        );
        assert!(serde_json::from_value::<PinnedTolerations>(serde_json::json!({"key": "k"})).is_err());
    }

    #[test]
    fn presets_resolve_by_id_and_default_is_unpinned() {
        let config = SchedulingConfig {
            toleration_presets: vec![
                SchedulingConfig::default_preset(),
                TolerationPreset {
                    id: "gpu-only".to_string(),
                    label: "GPU only".to_string(),
                    description: "Pin to GPU capacity.".to_string(),
                    tolerations: vec![SchedulingToleration {
                        key: "gpu".to_string(),
                        operator: Some(SchedulingOperator::Exists),
                        value: None,
                        effect: None,
                    }],
                },
            ],
        };
        let presets = TolerationPresets::new(&config);
        assert!(presets.resolve(None).is_none());
        assert_eq!(presets.resolve(Some("no-overflow")).as_deref(), Some(&serde_json::json!([])));
        assert_eq!(
            presets.resolve(Some("gpu-only")).as_deref(),
            Some(&serde_json::json!([{"key": "gpu", "operator": "Exists"}]))
        );
    }

    #[test]
    fn an_unconfigured_id_fails_closed_to_the_empty_list() {
        let presets = TolerationPresets::new(&SchedulingConfig::default());
        assert_eq!(presets.resolve(Some("retired")).as_deref(), Some(&serde_json::json!([])));
    }

    #[test]
    fn a_repeated_unconfigured_id_logs_its_diagnostic_once() {
        let presets = TolerationPresets::new(&SchedulingConfig::default());
        assert!(presets.mark_unknown_reported("retired"));
        assert!(!presets.mark_unknown_reported("retired"));
        assert!(presets.mark_unknown_reported("also-retired"));
        assert_eq!(presets.resolve(Some("retired")).as_deref(), Some(&serde_json::json!([])));
    }

    #[test]
    fn preset_config_validation_rejects_duplicates_and_bad_shape() {
        let duplicate = SchedulingConfig {
            toleration_presets: vec![SchedulingConfig::default_preset(), SchedulingConfig::default_preset()],
        };
        assert!(duplicate.validate().unwrap_err().contains("duplicate id"));

        let empty_id = SchedulingConfig {
            toleration_presets: vec![TolerationPreset {
                id: "  ".to_string(),
                label: "x".to_string(),
                description: "y".to_string(),
                tolerations: vec![],
            }],
        };
        assert!(empty_id.validate().unwrap_err().contains("empty id"));

        let sentinel = SchedulingConfig {
            toleration_presets: vec![TolerationPreset {
                id: DEFAULT_PRESET_SENTINEL.to_string(),
                label: "Collides".to_string(),
                description: "y".to_string(),
                tolerations: vec![],
            }],
        };
        assert!(sentinel.validate().unwrap_err().contains("reserved"));

        let bad_shape = SchedulingConfig {
            toleration_presets: vec![TolerationPreset {
                id: "bad".to_string(),
                label: "Bad".to_string(),
                description: "Bad shape.".to_string(),
                tolerations: vec![SchedulingToleration {
                    key: "dedicated".to_string(),
                    operator: None,
                    value: None,
                    effect: None,
                }],
            }],
        };
        assert!(bad_shape.validate().unwrap_err().contains("is invalid"));

        SchedulingConfig::default().validate().unwrap();
    }
}
