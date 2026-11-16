
mod chat;
mod embeddings;
mod vision;

pub use chat::ChatGenerator;
pub use embeddings::EmbeddingsGenerator;
pub use vision::VisionGenerator;

use crate::db::models::deployments::ModelType;
use fusillade::RequestTemplateInput;

pub trait SampleGenerator: Send + Sync {
    fn name(&self) -> &'static str;

    fn description(&self) -> &'static str;

    fn required_model_type(&self) -> ModelType;

    fn required_capabilities(&self) -> &'static [&'static str];

    fn generate(&self, model_alias: &str, endpoint: &str, count: usize) -> Vec<RequestTemplateInput>;
}

pub fn get_generators() -> Vec<Box<dyn SampleGenerator>> {
    vec![Box::new(ChatGenerator), Box::new(VisionGenerator), Box::new(EmbeddingsGenerator)]
}
