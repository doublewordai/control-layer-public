use anyhow::anyhow;
use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Clone, Parser)]
#[command(version, about, long_about = None)]
pub struct Config {
    #[arg(short = 'p', long, default_value_t = 3000)]
    pub port: u16,

    #[arg(long, default_value_t = 9090)]
    pub metrics_port: u16,

    #[arg(short = 'm', long, default_value_t = true)]
    pub metrics: bool,

    #[arg(short = 'f', long)]
    pub targets: PathBuf,

    #[arg(short = 'w', long, default_value_t = true)]
    pub watch: bool,

    #[arg(long, default_value = "onwards")]
    pub metrics_prefix: String,

    #[arg(long, env = "ONWARDS_SSE_BUFFER_LIMIT", default_value_t = crate::sse::DEFAULT_SSE_BUFFER_LIMIT)]
    pub sse_buffer_limit: usize,

    #[arg(long, env = "ONWARDS_SHUTDOWN_DELAY_SECS", default_value_t = 5)]
    pub shutdown_delay_secs: u64,

    #[arg(long, env = "ONWARDS_SHUTDOWN_TIMEOUT_SECS", default_value_t = 300,
          value_parser = clap::value_parser!(u64).range(1..))]
    pub shutdown_timeout_secs: u64,
}

impl Config {
    pub fn validate(self) -> Result<Self, anyhow::Error> {
        if !self.targets.exists() {
            return Err(anyhow!(
                "Config file '{}' does not exist",
                self.targets.display()
            ));
        }
        Ok(self)
    }
}
