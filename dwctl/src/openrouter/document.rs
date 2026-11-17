
use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{
    Deserialize, Serialize,
    de::{
        IntoDeserializer,
        value::{self, StringDeserializer},
    },
};

pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

pub const SCHEMA_VERSION: &str = "2.4";

#[derive(Debug, Clone, Serialize)]
pub struct ModelFeed {
    pub data: Vec<ModelDocument>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModelDocument {
    pub schema_version: &'static str,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hugging_face_id: Option<String>,
    pub name: String,
    pub created: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantization: Option<Quantization>,
    pub input_modalities: Vec<InputModality>,
    pub output_modalities: Vec<OutputModality>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deprecation_date: Option<String>,
    pub is_ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub openrouter: Option<OpenRouterSlug>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub datacenters: Vec<Datacenter>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenRouterSlug {
    pub slug: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Quantization {
    Int4,
    Int8,
    Fp4,
    Mxfp4,
    Nvfp4,
    Fp6,
    Fp8,
    Mxfp8,
    Fp16,
    Bf16,
    Fp32,
}

impl Quantization {
    pub fn parse(value: &str) -> Option<Self> {
        let value: StringDeserializer<value::Error> = value.trim().to_ascii_lowercase().into_deserializer();
        Self::deserialize(value).ok()
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputModality {
    Text {
        #[serde(skip_serializing_if = "Option::is_none")]
        supported_inputs: Option<TextInputs>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pricing: Vec<InputPrice>,
    },
    #[serde(untagged)]
    Media {
        #[serde(rename = "type")]
        kind: MediaInput,
        supported_inputs: MediaInputs,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum MediaInput {
    Image,
    Video,
    Audio,
    File,
}

#[derive(Debug, Clone, Serialize)]
pub struct TextInputs {
    pub max_context_length: Limit,
}

#[derive(Debug, Clone, Serialize)]
pub struct MediaInputs {
    pub sources: EnumValues<MediaSource>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EnumValues<T> {
    #[serde(rename = "type")]
    pub kind: EnumKind,
    pub values: Vec<T>,
}

impl<T> EnumValues<T> {
    pub fn new(values: Vec<T>) -> Self {
        Self {
            kind: EnumKind::Enum,
            values,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EnumKind {
    Enum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum MediaSource {
    Url,
    Base64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Limit {
    pub value: u64,
    pub unit: DescriptorUnit,
}

impl Limit {
    pub fn tokens(value: u64) -> Self {
        Self {
            value,
            unit: DescriptorUnit::Token,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct InputPrice {
    #[serde(rename = "type")]
    pub kind: InputPriceKind,
    pub unit: PriceUnit,
    pub cost_usd: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InputPriceKind {
    Prompt,
    CachedPrompt,
    CacheWrite,
}

#[derive(Debug, Clone, Serialize)]
pub struct OutputPrice {
    #[serde(rename = "type")]
    pub kind: OutputPriceKind,
    pub unit: PriceUnit,
    pub cost_usd: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputPriceKind {
    Completion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PriceUnit {
    Token,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputModality {
    Text {
        #[serde(skip_serializing_if = "Option::is_none")]
        max_length: Option<Limit>,
        streaming: bool,
        supported_parameters: BTreeMap<String, ParameterDescriptor>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        pricing: Vec<OutputPrice>,
    },
    Embeddings {
        supported_parameters: BTreeMap<String, ParameterDescriptor>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ParameterDescriptor {
    Range {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        unit: Option<DescriptorUnit>,
    },
    Integer {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        unit: Option<DescriptorUnit>,
    },
    Boolean {},
    Enum { values: Vec<EnumValue> },
    Array {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(range(min = 1, max = 9_007_199_254_740_991u64))]
        max_items: Option<u64>,
    },
    Unknown {},
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum EnumValue {
    Integer(i64),
    Number(f64),
    String(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DescriptorUnit {
    Second,
    Pixel,
    Byte,
    Token,
    Character,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Datacenter {
    pub country_code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

impl Datacenter {
    pub fn is_valid(&self) -> bool {
        self.country_code.len() == 2 && self.country_code.bytes().all(|byte| byte.is_ascii_uppercase())
    }
}
