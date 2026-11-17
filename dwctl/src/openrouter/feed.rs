
use std::collections::{BTreeMap, HashSet};

use rust_decimal::Decimal;

use crate::db::models::deployments::ModelType;

use super::{
    document::{
        Datacenter, DescriptorUnit, EnumValues, InputModality, InputPrice, InputPriceKind, Limit, MediaInputs, ModelDocument,
        OpenRouterSlug, OutputModality, OutputPrice, OutputPriceKind, ParameterDescriptor, PriceUnit, Quantization, SCHEMA_VERSION,
        TextInputs,
    },
    listing::OpenRouterListing,
};

#[derive(Debug, Clone)]
pub struct ListedModel<'a> {
    pub alias: &'a str,
    pub name: &'a str,
    pub created: i64,
    pub model_type: ModelType,
    pub reasoning: bool,
    pub context_window: Option<i64>,
    pub quantization: Option<&'a str>,
    pub listing: &'a OpenRouterListing,
    pub prices: Option<Prices>,
}

#[derive(Debug, Clone, Copy)]
pub struct Prices {
    pub input: Decimal,
    pub output: Decimal,
    pub cache: Option<CacheMultipliers>,
}

#[derive(Debug, Clone, Copy)]
pub struct CacheMultipliers {
    pub read: Decimal,
    pub write_5m: Decimal,
    pub write_1h: Decimal,
    pub write_24h: Decimal,
}

impl CacheMultipliers {
    fn write_tiers(&self) -> [(u64, Decimal); 3] {
        [(300, self.write_5m), (3_600, self.write_1h), (86_400, self.write_24h)]
    }
}

pub fn model_document(model: &ListedModel<'_>, refused: &HashSet<&str>, datacenters: &[Datacenter]) -> Option<ModelDocument> {
    let listing = model.listing;
    let output = match model.model_type {
        ModelType::Chat => OutputModality::Text {
            max_length: listing.max_output_tokens.map(Limit::tokens),
            streaming: true,
            supported_parameters: chat_parameters(model, refused),
            pricing: model
                .prices
                .and_then(|prices| billed(prices.output))
                .map(|cost_usd| OutputPrice {
                    kind: OutputPriceKind::Completion,
                    unit: PriceUnit::Token,
                    cost_usd,
                })
                .into_iter()
                .collect(),
        },
        ModelType::Embeddings => OutputModality::Embeddings {
            supported_parameters: BTreeMap::new(),
        },
        ModelType::Reranker => return None,
    };

    let mut input_modalities = vec![InputModality::Text {
        supported_inputs: model
            .context_window
            .and_then(|tokens| u64::try_from(tokens).ok())
            .filter(|tokens| *tokens > 0)
            .map(|tokens| TextInputs {
                max_context_length: Limit::tokens(tokens),
            }),
        pricing: model.prices.map(input_prices).unwrap_or_default(),
    }];
    input_modalities.extend(listing.media_inputs.iter().map(|(kind, sources)| InputModality::Media {
        kind: *kind,
        supported_inputs: MediaInputs {
            sources: EnumValues::new(sources.clone()),
        },
    }));

    Some(ModelDocument {
        schema_version: SCHEMA_VERSION,
        id: model.alias.to_string(),
        hugging_face_id: listing.hugging_face_id.clone(),
        name: model.name.to_string(),
        created: model.created,
        quantization: model.quantization.and_then(Quantization::parse),
        input_modalities,
        output_modalities: vec![output],
        deprecation_date: listing.deprecation_date.map(|date| date.format("%Y-%m-%d").to_string()),
        is_ready: listing.is_ready,
        openrouter: listing.slug.clone().map(|slug| OpenRouterSlug { slug }),
        datacenters: datacenters.to_vec(),
    })
}

fn input_prices(prices: Prices) -> Vec<InputPrice> {
    let Some(prompt) = billed(prices.input) else {
        return Vec::new();
    };
    let entry = |kind, cost_usd, ttl_seconds| InputPrice {
        kind,
        unit: PriceUnit::Token,
        cost_usd,
        ttl_seconds,
    };
    let mut entries = vec![entry(InputPriceKind::Prompt, prompt, None)];
    if let Some(cache) = prices.cache {
        entries.extend(usd(prices.input * cache.read).map(|cost| entry(InputPriceKind::CachedPrompt, cost, None)));
        entries.extend(
            cache
                .write_tiers()
                .into_iter()
                .filter_map(|(ttl, multiplier)| Some(entry(InputPriceKind::CacheWrite, usd(prices.input * multiplier)?, Some(ttl)))),
        );
    }
    entries
}

fn billed(price: Decimal) -> Option<String> {
    (price > Decimal::ZERO).then(|| usd(price)).flatten()
}

fn usd(price: Decimal) -> Option<String> {
    (!price.is_sign_negative()).then(|| price.normalize().to_string())
}

fn chat_parameters(model: &ListedModel<'_>, refused: &HashSet<&str>) -> BTreeMap<String, ParameterDescriptor> {
    let range = |min: f64, max: f64| ParameterDescriptor::Range {
        min: Some(min),
        max: Some(max),
        default: None,
        unit: None,
    };
    let integer = |min: Option<i64>, max: Option<i64>, unit: Option<DescriptorUnit>| ParameterDescriptor::Integer {
        min,
        max,
        default: None,
        unit,
    };
    let max_output = model.listing.max_output_tokens.and_then(|tokens| i64::try_from(tokens).ok());

    let mut parameters: BTreeMap<String, ParameterDescriptor> = [
        ("temperature", range(0.0, 2.0)),
        ("top_p", range(0.0, 1.0)),
        ("top_k", integer(Some(0), None, None)),
        ("min_p", range(0.0, 1.0)),
        ("frequency_penalty", range(-2.0, 2.0)),
        ("presence_penalty", range(-2.0, 2.0)),
        ("repetition_penalty", range(0.0, 2.0)),
        ("stop", ParameterDescriptor::Array { max_items: None }),
        ("seed", integer(None, None, None)),
        ("logit_bias", ParameterDescriptor::Unknown {}),
        ("max_tokens", integer(Some(1), max_output, Some(DescriptorUnit::Token))),
        ("logprobs", ParameterDescriptor::Boolean {}),
        ("top_logprobs", integer(Some(0), Some(20), None)),
        ("tools", ParameterDescriptor::Array { max_items: None }),
        ("tool_choice", ParameterDescriptor::Unknown {}),
        ("response_format", ParameterDescriptor::Unknown {}),
        ("structured_outputs", ParameterDescriptor::Boolean {}),
    ]
    .into_iter()
    .map(|(name, descriptor)| (name.to_string(), descriptor))
    .collect();
    if model.reasoning {
        parameters.insert("reasoning".to_string(), ParameterDescriptor::Unknown {});
        parameters.insert("include_reasoning".to_string(), ParameterDescriptor::Boolean {});
    }
    for (name, descriptor) in &model.listing.supported_parameters {
        match descriptor {
            Some(descriptor) => parameters.insert(name.clone(), descriptor.clone()),
            None => parameters.remove(name),
        };
    }
    parameters.retain(|name, _| !refused.contains(name.as_str()));
    parameters
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::NaiveDate;
    use serde_json::{Value, json};

    use super::*;
    use crate::openrouter::{
        document::{EnumValue, MediaInput, MediaSource, ModelFeed},
        schema_check::assert_valid,
    };

    fn price(value: &str) -> Decimal {
        Decimal::from_str(value).unwrap()
    }

    fn chat<'a>(listing: &'a OpenRouterListing) -> ListedModel<'a> {
        ListedModel {
            alias: "example-org/Chat-1",
            name: "Chat 1",
            created: 1_760_000_000,
            model_type: ModelType::Chat,
            reasoning: true,
            context_window: Some(262_144),
            quantization: Some("FP8"),
            listing,
            prices: Some(Prices {
                input: price("0.0000012"),
                output: price("0.0000038"),
                cache: Some(CacheMultipliers {
                    read: price("0.2"),
                    write_5m: price("1.25"),
                    write_1h: price("2"),
                    write_24h: price("2.5"),
                }),
            }),
        }
    }

    fn full_listing() -> OpenRouterListing {
        OpenRouterListing {
            hugging_face_id: Some("example-org/Chat-1".to_string()),
            slug: Some("example-org/chat-1".to_string()),
            is_ready: true,
            max_output_tokens: Some(65_536),
            deprecation_date: NaiveDate::from_ymd_opt(2027, 2, 1),
            media_inputs: [
                (MediaInput::Image, vec![MediaSource::Url, MediaSource::Base64]),
                (MediaInput::Video, vec![]),
            ]
            .into_iter()
            .collect(),
            supported_parameters: [
                ("top_k".to_string(), None),
                (
                    "reasoning_effort".to_string(),
                    Some(ParameterDescriptor::Enum {
                        values: vec![EnumValue::String("low".to_string()), EnumValue::String("high".to_string())],
                    }),
                ),
            ]
            .into_iter()
            .collect(),
        }
    }

    fn refused() -> HashSet<&'static str> {
        ["n", "logprobs", "top_logprobs", "prompt_logprobs"].into_iter().collect()
    }

    fn document_json(model: &ListedModel<'_>) -> Value {
        let datacenters = [Datacenter {
            country_code: "US".to_string(),
            region: Some("example-1".to_string()),
        }];
        serde_json::to_value(model_document(model, &refused(), &datacenters).unwrap()).unwrap()
    }

    #[test]
    fn feed_documents_validate_against_openrouter_schema() {
        let full = full_listing();
        let minimal = OpenRouterListing::default();
        let mut unpriced = chat(&minimal);
        unpriced.prices = None;
        unpriced.context_window = None;
        unpriced.quantization = Some("modelopt_mixed");
        unpriced.reasoning = false;
        let mut embeddings = chat(&minimal);
        embeddings.model_type = ModelType::Embeddings;
        let mut free_cache_reads = chat(&full);
        free_cache_reads.prices.as_mut().unwrap().cache.as_mut().unwrap().read = Decimal::ZERO;

        let datacenters = [
            Datacenter {
                country_code: "US".to_string(),
                region: None,
            },
            Datacenter {
                country_code: "FR".to_string(),
                region: Some("example-2".to_string()),
            },
        ];
        let feed = ModelFeed {
            data: [chat(&full), unpriced, embeddings, free_cache_reads]
                .iter()
                .map(|model| model_document(model, &refused(), &datacenters).unwrap())
                .collect(),
        };
        assert_valid(&serde_json::to_value(feed).unwrap());
    }

    #[test]
    fn the_schema_check_catches_an_unknown_key() {
        let listing = full_listing();
        let mut document = document_json(&chat(&listing));
        document["output_modalities"][0]["tariffs"] = json!([]);
        let result = std::panic::catch_unwind(|| assert_valid(&json!({ "data": [document] })));
        assert!(result.is_err(), "a strict schema must reject unknown keys");
    }

    #[test]
    fn document_carries_listing_catalogue_and_prices() {
        let listing = full_listing();
        let document = document_json(&chat(&listing));

        assert_eq!(document["schema_version"], "2.4");
        assert_eq!(document["id"], "example-org/Chat-1");
        assert_eq!(document["hugging_face_id"], "example-org/Chat-1");
        assert_eq!(document["openrouter"], json!({"slug": "example-org/chat-1"}));
        assert_eq!(document["quantization"], "fp8");
        assert_eq!(document["is_ready"], true);
        assert_eq!(document["deprecation_date"], "2027-02-01");
        assert_eq!(document["datacenters"], json!([{"country_code": "US", "region": "example-1"}]));

        let text = &document["input_modalities"][0];
        assert_eq!(text["type"], "text");
        assert_eq!(
            text["supported_inputs"]["max_context_length"],
            json!({"value": 262144, "unit": "token"})
        );
        assert_eq!(
            text["pricing"],
            json!([
                {"type": "prompt", "unit": "token", "cost_usd": "0.0000012"},
                {"type": "cached_prompt", "unit": "token", "cost_usd": "0.00000024"},
                {"type": "cache_write", "unit": "token", "cost_usd": "0.0000015", "ttl_seconds": 300},
                {"type": "cache_write", "unit": "token", "cost_usd": "0.0000024", "ttl_seconds": 3600},
                {"type": "cache_write", "unit": "token", "cost_usd": "0.000003", "ttl_seconds": 86400},
            ])
        );
        assert_eq!(
            document["input_modalities"][1],
            json!({"type": "image", "supported_inputs": {"sources": {"type": "enum", "values": ["url", "base64"]}}})
        );
        assert_eq!(
            document["input_modalities"][2],
            json!({"type": "video", "supported_inputs": {"sources": {"type": "enum", "values": []}}})
        );

        let output = &document["output_modalities"][0];
        assert_eq!(output["type"], "text");
        assert_eq!(output["max_length"], json!({"value": 65536, "unit": "token"}));
        assert_eq!(
            output["pricing"],
            json!([{"type": "completion", "unit": "token", "cost_usd": "0.0000038"}])
        );
        assert_eq!(
            output["supported_parameters"]["max_tokens"],
            json!({"type": "integer", "min": 1, "max": 65536, "unit": "token"})
        );
        let parameters = &output["supported_parameters"];
        assert_eq!(parameters["reasoning"], json!({"type": "unknown"}));
        assert_eq!(parameters["include_reasoning"], json!({"type": "boolean"}));
        assert_eq!(parameters["tools"], json!({"type": "array"}), "tools is a list of definitions");
        assert_eq!(parameters["tool_choice"], json!({"type": "unknown"}));
        assert_eq!(parameters["response_format"], json!({"type": "unknown"}));
    }

    #[test]
    fn parameters_follow_listing_changes_and_never_include_refused_ones() {
        let mut listing = full_listing();
        listing
            .supported_parameters
            .insert("logprobs".to_string(), Some(ParameterDescriptor::Boolean {}));
        let document = document_json(&chat(&listing));
        let parameters = document["output_modalities"][0]["supported_parameters"].as_object().unwrap();

        for refused in ["n", "logprobs", "top_logprobs"] {
            assert!(!parameters.contains_key(refused), "{refused} is refused by the gateway");
        }
        assert!(!parameters.contains_key("top_k"), "null removes a default parameter");
        assert_eq!(parameters["reasoning_effort"], json!({"type": "enum", "values": ["low", "high"]}));
        for kept in [
            "temperature",
            "top_p",
            "min_p",
            "stop",
            "seed",
            "frequency_penalty",
            "presence_penalty",
            "tools",
        ] {
            assert!(parameters.contains_key(kept), "{kept} is a default chat parameter");
        }

        let allowed: HashSet<&str> = ["n"].into_iter().collect();
        let document = serde_json::to_value(model_document(&chat(&listing), &allowed, &[]).unwrap()).unwrap();
        let parameters = &document["output_modalities"][0]["supported_parameters"];
        assert_eq!(parameters["logprobs"], json!({"type": "boolean"}));
        assert_eq!(parameters["top_logprobs"], json!({"type": "integer", "min": 0, "max": 20}));
    }

    #[test]
    fn undeclared_values_are_left_out() {
        let listing = OpenRouterListing::default();
        let mut model = chat(&listing);
        model.prices = Some(Prices {
            input: Decimal::ZERO,
            output: Decimal::ZERO,
            cache: None,
        });
        model.context_window = None;
        model.quantization = Some("modelopt_mixed");
        model.reasoning = false;
        let document = serde_json::to_value(model_document(&model, &refused(), &[]).unwrap()).unwrap();
        let object = document.as_object().unwrap();

        for absent in ["hugging_face_id", "quantization", "deprecation_date", "openrouter", "datacenters"] {
            assert!(!object.contains_key(absent), "{absent} must be omitted, not null");
        }
        assert_eq!(document["is_ready"], false, "a listing defaults to not ready");
        assert_eq!(
            document["input_modalities"],
            json!([{"type": "text"}]),
            "zero prices are not declared"
        );
        let output = &document["output_modalities"][0];
        assert!(output.get("max_length").is_none() && output.get("pricing").is_none());
        assert_eq!(
            output["supported_parameters"]["max_tokens"],
            json!({"type": "integer", "min": 1, "unit": "token"})
        );
        assert!(output["supported_parameters"].get("reasoning").is_none());
    }

    #[test]
    fn embeddings_and_rerankers() {
        let listing = OpenRouterListing::default();
        let mut model = chat(&listing);
        model.model_type = ModelType::Embeddings;
        let document = serde_json::to_value(model_document(&model, &refused(), &[]).unwrap()).unwrap();
        assert_eq!(
            document["output_modalities"],
            json!([{"type": "embeddings", "supported_parameters": {}}])
        );
        assert_eq!(document["input_modalities"][0]["pricing"][0]["cost_usd"], "0.0000012");

        model.model_type = ModelType::Reranker;
        assert!(model_document(&model, &refused(), &[]).is_none());
    }

    #[test]
    fn listing_parses_strictly_and_validates() {
        let listing: OpenRouterListing = serde_json::from_value(json!({
            "hugging_face_id": "example-org/Chat-1",
            "is_ready": true,
            "max_output_tokens": 8192,
            "deprecation_date": "2027-02-01",
            "media_inputs": {"image": ["url", "base64"], "file": []},
            "supported_parameters": {"top_k": null, "temperature": {"type": "range", "min": 0, "max": 1}},
        }))
        .unwrap();
        assert_eq!(listing.validate(), Ok(()));

        for bad in [
            json!({"is_ready": true, "price": "1"}),
            json!({"supported_parameters": {"temperature": {"type": "range", "values": [1]}}}),
            json!({"supported_parameters": {"tools": {"type": "boolean", "default": true}}}),
            json!({"media_inputs": {"image": ["ftp"]}}),
            json!({"deprecation_date": "soon"}),
        ] {
            assert!(
                serde_json::from_value::<OpenRouterListing>(bad.clone()).is_err(),
                "{bad} must not parse"
            );
        }
        for (bad, reason) in [
            (json!({"slug": " "}), "slug must not be empty"),
            (json!({"max_output_tokens": 0}), "max_output_tokens must be positive"),
            (
                json!({"media_inputs": {"image": ["url", "url"]}}),
                "media_inputs.image lists a source twice",
            ),
            (
                json!({"supported_parameters": {"top_p": {"type": "range", "min": 1, "max": 0}}}),
                "supported_parameters.top_p: min is above max",
            ),
        ] {
            let listing: OpenRouterListing = serde_json::from_value(bad).unwrap();
            assert_eq!(listing.validate(), Err(reason.to_string()));
        }
    }
}
