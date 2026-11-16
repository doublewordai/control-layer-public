
use axum::http::Extensions;

use super::rejections::Recorder;

pub const STANDARD_CLASS: &str = "standard";

pub const CLASS_SEPARATOR: char = ':';

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServingName {
    pub requested: String,
    pub model: String,
    pub class: Option<String>,
}

impl ServingName {
    pub fn resolve(requested: &str, recorder: Option<&Recorder>) -> Self {
        if let Some(model) = recorder.and_then(|recorder| recorder.class_route_of(requested))
            && let Some(class) = requested
                .strip_prefix(model.as_str())
                .and_then(|rest| rest.strip_prefix(CLASS_SEPARATOR))
        {
            return Self {
                requested: requested.to_owned(),
                class: Some(class.to_owned()),
                model,
            };
        }
        Self {
            requested: requested.to_owned(),
            model: requested.to_owned(),
            class: None,
        }
    }

    pub fn of(extensions: &Extensions, requested: &str) -> Self {
        match extensions.get::<ServingName>() {
            Some(name) if name.requested == requested => name.clone(),
            _ => Self::resolve(requested, extensions.get::<Recorder>()),
        }
    }

    pub fn class_label(&self) -> &str {
        self.class.as_deref().unwrap_or(STANDARD_CLASS)
    }
}

pub fn split_served(requested: &str) -> (&str, Option<&str>) {
    match requested.split_once(CLASS_SEPARATOR) {
        Some((model, class)) => (model, Some(class)),
        None => (requested, None),
    }
}

pub fn has_class_suffix(model: &str) -> bool {
    model.contains(CLASS_SEPARATOR)
}

#[cfg(test)]
mod tests {
    use onwards::target::Targets;

    use super::*;

    fn recorder() -> Recorder {
        Recorder::new(
            Targets::from_config(
                serde_json::from_value(serde_json::json!({
                    "targets": {
                        "org/model": {"providers": [{"url": "https://upstream.example.com/v1/"}]},
                        "org/model:fast": {"providers": [{"url": "https://upstream.example.com/v1/"}], "class_route_of": "org/model"}
                    },
                    "auth": {"global_keys": [], "key_definitions": {}}
                }))
                .unwrap(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn only_a_configured_class_route_names_a_class() {
        let recorder = recorder();
        let fast = ServingName::resolve("org/model:fast", Some(&recorder));
        assert_eq!((fast.model.as_str(), fast.class_label()), ("org/model", "fast"));
        let standard = ServingName::resolve("org/model", Some(&recorder));
        assert_eq!(
            (standard.model.as_str(), standard.class.as_deref(), standard.class_label()),
            ("org/model", None, "standard")
        );
        let unknown = ServingName::resolve("org/model:gibberish", Some(&recorder));
        assert_eq!((unknown.model.as_str(), unknown.class), ("org/model:gibberish", None));
    }

    #[test]
    fn served_names_split_at_the_colon() {
        assert_eq!(split_served("org/model:fast"), ("org/model", Some("fast")));
        assert_eq!(split_served("org/model"), ("org/model", None));
    }

    #[test]
    fn suffixes_are_detected() {
        assert!(has_class_suffix("org/model:fast"));
        assert!(!has_class_suffix("org/model"));
    }
}
