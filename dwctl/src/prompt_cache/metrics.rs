
use metrics::{counter, histogram};

use crate::inference::serving_name::{STANDARD_CLASS, split_served};


pub fn record_marker_request(marked: bool) {
    counter!(
        "dwctl_cache_marker_requests_total",
        "marked" => if marked { "true" } else { "false" }
    )
    .increment(1);
}


pub fn record_request_outcome(outcome: &'static str) {
    counter!("dwctl_cache_requests_total", "outcome" => outcome).increment(1);
}

pub fn record_token_volumes(requested: &str, read: u64, creation_5m: u64, creation_1h: u64, creation_24h: u64) {
    let (model, class) = split_served(requested);
    let class = class.unwrap_or(STANDARD_CLASS).to_owned();
    if read > 0 {
        counter!("dwctl_cache_read_input_tokens_total", "model" => model.to_owned(), "serving_class" => class.clone()).increment(read);
    }
    for (tier, n) in [("5m", creation_5m), ("1h", creation_1h), ("24h", creation_24h)] {
        if n > 0 {
            counter!(
                "dwctl_cache_creation_input_tokens_total",
                "model" => model.to_owned(),
                "serving_class" => class.clone(),
                "tier" => tier
            )
            .increment(n);
        }
    }
}


pub fn record_classify(outcome: &'static str) {
    counter!("dwctl_cache_classify_total", "outcome" => outcome).increment(1);
}

pub fn record_classify_duration(seconds: f64) {
    histogram!("dwctl_cache_classify_duration_seconds").record(seconds);
}

pub fn record_lookup_duration(seconds: f64) {
    histogram!("dwctl_cache_lookup_duration_seconds").record(seconds);
}

pub fn record_skip(reason: &'static str) {
    counter!("dwctl_cache_skip_total", "reason" => reason).increment(1);
}


pub fn record_tokenizer_request(outcome: &'static str) {
    counter!("dwctl_cache_tokenizer_requests_total", "outcome" => outcome).increment(1);
}

pub fn record_tokenizer_duration(model: &str, size: &'static str, seconds: f64) {
    histogram!("dwctl_cache_tokenizer_duration_seconds", "model" => model.to_owned(), "size" => size).record(seconds);
}

pub fn tokenize_size_bucket(total_bytes: usize) -> &'static str {
    match total_bytes {
        0..=16_383 => "lt16k",
        16_384..=65_535 => "16k_64k",
        65_536..=262_143 => "64k_256k",
        _ => "gte256k",
    }
}

pub fn record_tokenizer_version_cache(result: &'static str) {
    counter!("dwctl_cache_tokenizer_version_cache_total", "result" => result).increment(1);
}


pub fn record_index_conn_retry(op: &'static str) {
    counter!("dwctl_cache_index_conn_retries_total", "op" => op).increment(1);
}


pub fn record_principal_resolve(result: &'static str) {
    counter!("dwctl_cache_principal_resolve_total", "result" => result).increment(1);
}

pub fn record_model_config_resolve(result: &'static str) {
    counter!("dwctl_cache_model_config_resolve_total", "result" => result).increment(1);
}


pub fn record_commit(result: &'static str) {
    counter!("dwctl_cache_commit_total", "result" => result).increment(1);
}

pub fn record_commit_vetoed(reason: &'static str) {
    counter!("dwctl_cache_commit_vetoed_total", "reason" => reason).increment(1);
}

pub fn record_commit_duration(seconds: f64) {
    histogram!("dwctl_cache_commit_duration_seconds").record(seconds);
}


pub fn record_body_read_failed() {
    counter!("dwctl_cache_body_read_failed_total").increment(1);
}

pub fn record_markers_rejected(reason: &'static str) {
    counter!("dwctl_cache_markers_rejected_total", "reason" => reason).increment(1);
}

pub fn record_query_breakpoint(outcome: &'static str) {
    counter!("dwctl_cache_query_breakpoint_total", "outcome" => outcome).increment(1);
}
