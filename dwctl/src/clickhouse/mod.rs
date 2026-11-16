
use std::fmt;
use std::io::Write as _;
use std::time::Duration;

use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use url::Url;

pub mod sink;
pub use sink::{ClickHouseSink, SinkHandle, SinkOptions};

const USER_HEADER: &str = "X-ClickHouse-User";
const KEY_HEADER: &str = "X-ClickHouse-Key";
const ERROR_BODY_LIMIT: usize = 512;

fn default_database() -> String {
    "clay".to_string()
}

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Serialize, Deserialize)]
pub struct ClickhouseConfig {
    pub endpoint_url: String,
    #[serde(default = "default_database")]
    pub database: String,
    pub user: String,
    pub password: String,
}

impl fmt::Debug for ClickhouseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickhouseConfig")
            .field("endpoint_url", &redact_url(&self.endpoint_url))
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl ClickhouseConfig {
    pub fn validate(&self) -> Result<(), String> {
        let url = Url::parse(self.endpoint_url.trim()).map_err(|e| format!("clickhouse.endpoint_url is not a valid URL: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!("clickhouse.endpoint_url must be http(s), got {:?}", url.scheme()));
        }
        if !is_identifier(&self.database) {
            return Err(format!("clickhouse.database {:?} is not a plain identifier", self.database));
        }
        if self.user.trim().is_empty() {
            return Err("clickhouse.user is empty".to_string());
        }
        if self.password.is_empty() {
            return Err("clickhouse.password is empty (set DWCTL_CLICKHOUSE__PASSWORD)".to_string());
        }
        if url.password().is_some() || !url.username().is_empty() {
            return Err("clickhouse.endpoint_url must not embed credentials; use clickhouse.user and clickhouse.password".to_string());
        }
        Ok(())
    }
}

fn redact_url(raw: &str) -> String {
    match Url::parse(raw.trim()) {
        Ok(mut url) => {
            if url.password().is_some() || !url.username().is_empty() {
                let _ = url.set_username("<redacted>");
                let _ = url.set_password(None);
            }
            url.to_string()
        }
        Err(_) => "<unparseable url>".to_string(),
    }
}

pub fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_') && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[derive(Debug, thiserror::Error)]
pub enum ClickHouseError {
    #[error("invalid clickhouse config: {0}")]
    Config(String),
    #[error("clickhouse request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("clickhouse returned {status}: {body}")]
    Status { status: u16, body: String },
    #[error("failed to compress insert body: {0}")]
    Compress(#[from] std::io::Error),
}

#[derive(Clone)]
pub struct ClickHouseClient {
    http: reqwest::Client,
    endpoint: Url,
    database: String,
    user: String,
    password: String,
}

impl fmt::Debug for ClickHouseClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClickHouseClient")
            .field("endpoint", &redact_url(self.endpoint.as_str()))
            .field("database", &self.database)
            .field("user", &self.user)
            .finish_non_exhaustive()
    }
}

impl ClickHouseClient {
    pub fn from_config(config: &ClickhouseConfig) -> Result<Self, ClickHouseError> {
        config.validate().map_err(ClickHouseError::Config)?;
        let endpoint = Url::parse(config.endpoint_url.trim()).map_err(|e| ClickHouseError::Config(e.to_string()))?;
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(ClickHouseError::Transport)?;
        Ok(Self {
            http,
            endpoint,
            database: config.database.clone(),
            user: config.user.clone(),
            password: config.password.clone(),
        })
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    fn insert_url(&self, table: &str) -> Url {
        let mut url = self.endpoint.clone();
        url.query_pairs_mut()
            .append_pair("query", &format!("INSERT INTO {}.{} FORMAT JSONEachRow", self.database, table))
            .append_pair("date_time_input_format", "best_effort");
        url
    }

    pub async fn insert_json_each_row(&self, table: &str, json_lines: &[u8]) -> Result<(), ClickHouseError> {
        if !is_identifier(table) {
            return Err(ClickHouseError::Config(format!("table {table:?} is not a plain identifier")));
        }
        let mut gz = flate2::write::GzEncoder::new(Vec::with_capacity(json_lines.len() / 4 + 64), flate2::Compression::fast());
        gz.write_all(json_lines)?;
        let body = gz.finish()?;

        let response = self
            .http
            .post(self.insert_url(table))
            .header(USER_HEADER, &self.user)
            .header(KEY_HEADER, &self.password)
            .header(CONTENT_ENCODING, "gzip")
            .header(CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let mut body = response.text().await.unwrap_or_default();
        if body.len() > ERROR_BODY_LIMIT {
            let mut cut = ERROR_BODY_LIMIT;
            while !body.is_char_boundary(cut) {
                cut -= 1;
            }
            body.truncate(cut);
            body.push('…');
        }
        Err(ClickHouseError::Status {
            status: status.as_u16(),
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ClickhouseConfig {
        ClickhouseConfig {
            endpoint_url: "https://example.clickhouse.cloud:8443".to_string(),
            database: default_database(),
            user: "dwctl".to_string(),
            password: "pw".to_string(),
        }
    }

    #[test]
    fn config_validates_endpoint_identifiers_and_password() {
        assert!(config().validate().is_ok());
        let mut c = config();
        c.endpoint_url = "not a url".into();
        assert!(c.validate().unwrap_err().contains("endpoint_url"));
        let mut c = config();
        c.endpoint_url = "ftp://x".into();
        assert!(c.validate().unwrap_err().contains("http(s)"));
        let mut c = config();
        c.database = "clay; DROP".into();
        assert!(c.validate().unwrap_err().contains("identifier"));
        let mut c = config();
        c.password = String::new();
        assert!(c.validate().unwrap_err().contains("password"));
    }

    #[test]
    fn debug_output_redacts_the_password() {
        let s = format!("{:?}", config());
        assert!(s.contains("<redacted>"));
        assert!(!s.contains("pw\""));
    }

    #[test]
    fn debug_output_redacts_credentials_embedded_in_the_url() {
        let mut c = config();
        c.endpoint_url = "https://alice:s3cret@example.clickhouse.cloud:8443".into();
        let s = format!("{:?}", c);
        assert!(!s.contains("s3cret") && !s.contains("alice"), "{s}");
        assert!(c.validate().unwrap_err().contains("embed credentials"));
        assert_eq!(redact_url("not a url"), "<unparseable url>");
    }

    #[test]
    fn identifiers_are_plain() {
        assert!(is_identifier("prompt_chains"));
        assert!(is_identifier("_t1"));
        assert!(!is_identifier("1abc"));
        assert!(!is_identifier("a-b"));
        assert!(!is_identifier(""));
        assert!(!is_identifier("t;x"));
    }

    #[test]
    fn insert_url_carries_the_statement_and_the_datetime_setting() {
        let c = ClickHouseClient::from_config(&config()).unwrap();
        let url = c.insert_url("prompt_chains");
        let pairs: Vec<(String, String)> = url.query_pairs().map(|(k, v)| (k.into_owned(), v.into_owned())).collect();
        assert!(pairs.contains(&("query".into(), "INSERT INTO clay.prompt_chains FORMAT JSONEachRow".into())));
        assert!(pairs.contains(&("date_time_input_format".into(), "best_effort".into())));
    }

    #[tokio::test]
    async fn insert_rejects_a_bad_table_name_before_any_request() {
        let c = ClickHouseClient::from_config(&config()).unwrap();
        let err = c.insert_json_each_row("bad name", b"{}\n").await.unwrap_err();
        assert!(matches!(err, ClickHouseError::Config(_)));
    }
}
