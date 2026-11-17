
use crate::inference::translation::anthropic::model::MessagesResponse;
use crate::inference::translation::responses::types::{ResponsesResponse, ResponsesStreamingEvent};
use crate::request_logging::models::AiResponse;
use outlet_postgres::SerializationError;
use serde_json::Value;
use std::io::Read as _;
use tracing::instrument;

use super::models::{ChatCompletionChunk, CompletionChunk, SseParseError};

fn parse_sse_chunks(body_str: &str) -> Result<Vec<String>, SseParseError> {
    let mut chunks = Vec::new();
    let mut current_event_data = String::new();
    let mut found_sse_data = false;
    let mut has_pending_data = false;

    for line in body_str.lines() {
        let trimmed = line.trim();

        if let Some(data_part) = trimmed.strip_prefix("data:") {
            let data_part = data_part.strip_prefix(' ').unwrap_or(data_part);
            current_event_data = data_part.to_string();
            found_sse_data = true;
            has_pending_data = true;
        } else if trimmed.is_empty() && has_pending_data {
            chunks.push(current_event_data.clone());
            current_event_data.clear();
            has_pending_data = false;
        }
    }

    if has_pending_data {
        chunks.push(current_event_data);
    }

    if !found_sse_data {
        return Err(SseParseError::InvalidFormat);
    }

    Ok(chunks)
}

fn process_completion_sse_chunks(chunks: Vec<String>) -> AiResponse {
    let chunks = chunks
        .into_iter()
        .filter_map(|x| {
            if x.trim() == "[DONE]" {
                Some(CompletionChunk::Done)
            } else {
                serde_json::from_str::<CompletionChunk>(&x).ok()
            }
        })
        .collect::<Vec<_>>();

    AiResponse::CompletionsStream(chunks)
}

fn process_sse_chunks(chunks: Vec<String>) -> AiResponse {
    let chunks = chunks
        .into_iter()
        .filter_map(|x| {
            if x.trim() == "[DONE]" {
                Some(ChatCompletionChunk::Done)
            } else {
                serde_json::from_str::<ChatCompletionChunk>(&x).ok()
            }
        })
        .collect::<Vec<_>>();

    AiResponse::ChatCompletionsStream(chunks)
}

#[instrument(skip_all, name = "dwctl.parse_completions_streaming_response")]
pub(crate) fn parse_completions_streaming_response(body_str: &str) -> Result<AiResponse, Box<dyn std::error::Error>> {
    parse_sse_chunks(body_str)
        .map(process_completion_sse_chunks)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        .or_else(|_| serde_json::from_str(body_str).map_err(|e| Box::new(e) as Box<dyn std::error::Error>))
}

#[instrument(skip_all, name = "dwctl.parse_streaming_response")]
pub(crate) fn parse_streaming_response(body_str: &str) -> Result<AiResponse, Box<dyn std::error::Error>> {
    parse_sse_chunks(body_str)
        .map(process_sse_chunks)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
        .or_else(|_| serde_json::from_str(body_str).map_err(|e| Box::new(e) as Box<dyn std::error::Error>))
}

#[instrument(skip_all, name = "dwctl.parse_non_streaming_response")]
pub(crate) fn parse_non_streaming_response(body_str: &str) -> Result<AiResponse, Box<dyn std::error::Error>> {
    serde_json::from_str(body_str).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

#[instrument(skip_all, name = "dwctl.parse_responses_non_streaming")]
pub(crate) fn parse_responses_non_streaming_response(body_str: &str) -> Result<AiResponse, Box<dyn std::error::Error>> {
    serde_json::from_str::<ResponsesResponse>(body_str)
        .map(AiResponse::Responses)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

#[instrument(skip_all, name = "dwctl.parse_responses_streaming")]
pub(crate) fn parse_responses_streaming_response(body_str: &str) -> Result<AiResponse, Box<dyn std::error::Error>> {
    let chunks = parse_sse_chunks(body_str).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    let events: Vec<ResponsesStreamingEvent> = chunks
        .into_iter()
        .filter_map(|chunk| serde_json::from_str::<ResponsesStreamingEvent>(&chunk).ok())
        .collect();

    if events.is_empty() {
        return Err(Box::new(SseParseError::InvalidFormat));
    }

    Ok(AiResponse::ResponsesStream(events))
}

#[instrument(skip_all, name = "dwctl.parse_anthropic_non_streaming")]
pub(crate) fn parse_anthropic_non_streaming_response(body_str: &str) -> Result<AiResponse, Box<dyn std::error::Error>> {
    serde_json::from_str::<MessagesResponse>(body_str)
        .map(AiResponse::Anthropic)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

#[instrument(skip_all, name = "dwctl.parse_anthropic_streaming")]
pub(crate) fn parse_anthropic_streaming_response(body_str: &str) -> Result<AiResponse, Box<dyn std::error::Error>> {
    let chunks = parse_sse_chunks(body_str).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    let events: Vec<Value> = chunks
        .into_iter()
        .filter_map(|chunk| serde_json::from_str::<Value>(&chunk).ok())
        .collect();

    if events.is_empty() {
        return Err(Box::new(SseParseError::InvalidFormat));
    }

    Ok(AiResponse::AnthropicStream(events))
}

#[instrument(skip_all, name = "dwctl.decompress_response")]
pub(crate) fn decompress_response_if_needed(
    bytes: &[u8],
    headers: &std::collections::HashMap<String, Vec<bytes::Bytes>>,
) -> Result<Vec<u8>, SerializationError> {
    let content_encoding = headers
        .get("content-encoding")
        .or_else(|| headers.get("Content-Encoding"))
        .and_then(|values| values.first())
        .map(|bytes| String::from_utf8_lossy(bytes))
        .map(|s| s.trim().to_lowercase());

    match content_encoding.as_deref() {
        Some("gzip") => {
            let mut decompressed = Vec::new();
            flate2::read::GzDecoder::new(bytes)
                .read_to_end(&mut decompressed)
                .map_err(|e| SerializationError {
                    fallback_data: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes),
                    error: Box::new(e),
                })?;
            Ok(decompressed)
        }
        Some("br") | Some("brotli") => {
            let mut decompressed = Vec::new();
            brotli::Decompressor::new(bytes, 4096)
                .read_to_end(&mut decompressed)
                .map_err(|e| SerializationError {
                    fallback_data: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes),
                    error: Box::new(e),
                })?;
            Ok(decompressed)
        }
        _ => Ok(bytes.to_vec()),
    }
}

pub(crate) fn header_map(request_data: &outlet::RequestData) -> axum::http::HeaderMap {
    request_data
        .headers
        .iter()
        .flat_map(|(name, values)| {
            let name = axum::http::HeaderName::from_bytes(name.as_bytes()).ok();
            values
                .iter()
                .filter_map(move |value| Some((name.clone()?, axum::http::HeaderValue::from_bytes(value).ok()?)))
        })
        .collect()
}

pub(crate) fn extract_header_as_uuid(request_data: &outlet::RequestData, header_name: &str) -> Option<uuid::Uuid> {
    request_data
        .headers
        .get(header_name)
        .and_then(|values| values.first())
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
}

pub(crate) fn extract_header_as_string(request_data: &outlet::RequestData, header_name: &str) -> Option<String> {
    request_data
        .headers
        .get(header_name)
        .and_then(|values| values.first())
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}


#[cfg(test)]
mod tests {
    use super::{
        decompress_response_if_needed, extract_header_as_string, parse_non_streaming_response, parse_responses_non_streaming_response,
        parse_responses_streaming_response, parse_sse_chunks, parse_streaming_response, process_sse_chunks,
    };
    use crate::request_logging::models::{AiResponse, ChatCompletionChunk, SseParseError};
    use axum::http::{Method, Uri};
    use bytes::Bytes;
    use outlet::RequestData;
    use std::collections::HashMap;
    use std::time::SystemTime;

    #[test]
    fn test_parse_sse_chunks_valid() {
        let sse_data = "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\"}\n\ndata: {\"id\":\"chatcmpl-456\",\"object\":\"chat.completion.chunk\"}\n\n";

        let result = parse_sse_chunks(sse_data).unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "{\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\"}");
        assert_eq!(result[1], "{\"id\":\"chatcmpl-456\",\"object\":\"chat.completion.chunk\"}");
    }

    #[test]
    fn test_parse_sse_chunks_single_chunk() {
        let sse_data = "data: {\"test\":\"value\"}\n\n";

        let result = parse_sse_chunks(sse_data).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "{\"test\":\"value\"}");
    }

    #[test]
    fn test_parse_sse_chunks_no_trailing_newline() {
        let sse_data = "data: {\"test\":\"value\"}";

        let result = parse_sse_chunks(sse_data).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "{\"test\":\"value\"}");
    }

    #[test]
    fn test_parse_sse_chunks_invalid_format() {
        let invalid_data = "this is not sse format";

        let result = parse_sse_chunks(invalid_data);

        assert_eq!(result.unwrap_err(), SseParseError::InvalidFormat);
    }

    #[test]
    fn test_parse_sse_chunks_empty_data() {
        let sse_data = "data: \n\n";

        let result = parse_sse_chunks(sse_data).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "");
    }

    #[test]
    fn test_parse_sse_chunks_with_extra_whitespace() {
        let sse_data = "  data: {\"test\":\"value\"}  \n\n";

        let result = parse_sse_chunks(sse_data).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "{\"test\":\"value\"}");
    }

    #[test]
    fn test_process_sse_chunks_valid_json() {
        let chunks = vec![
            r#"{"id":"chatcmpl-123","object":"chat.completion.chunk","created":1677652288,"model":"gpt-3.5-turbo","choices":[]}"#
                .to_string(),
            "[DONE]".to_string(),
        ];

        let result = process_sse_chunks(chunks);

        match result {
            AiResponse::ChatCompletionsStream(parsed_chunks) => {
                assert_eq!(parsed_chunks.len(), 2);
            }
            _ => panic!("Expected ChatCompletionsStream variant"),
        }
    }

    #[test]
    fn test_process_sse_chunks_invalid_json() {
        let chunks = vec!["invalid json".to_string(), r#"{"valid":"json"}"#.to_string()];

        let result = process_sse_chunks(chunks);

        match result {
            AiResponse::ChatCompletionsStream(parsed_chunks) => {
                assert_eq!(parsed_chunks.len(), 0);
            }
            _ => panic!("Expected ChatCompletionsStream variant"),
        }
    }

    #[test]
    fn test_process_sse_chunks_empty() {
        let chunks = vec![];

        let result = process_sse_chunks(chunks);

        match result {
            AiResponse::ChatCompletionsStream(parsed_chunks) => {
                assert_eq!(parsed_chunks.len(), 0);
            }
            _ => panic!("Expected ChatCompletionsStream variant"),
        }
    }

    #[test]
    fn test_process_sse_chunks_error_frame() {
        let chunks = vec![
            r#"{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"hi"}}]}"#
                .to_string(),
            r#"{"error":{"message":"Engine was shut down during token generation","type":"internal_server_error","code":500}}"#.to_string(),
        ];

        let result = process_sse_chunks(chunks);

        match result {
            AiResponse::ChatCompletionsStream(parsed_chunks) => {
                assert_eq!(parsed_chunks.len(), 2);
                assert!(matches!(parsed_chunks[0], ChatCompletionChunk::Chunk(_)));
                match &parsed_chunks[1] {
                    ChatCompletionChunk::Error(e) => {
                        assert_eq!(e.error.get("code").and_then(|v| v.as_i64()), Some(500));
                    }
                    other => panic!("Expected Error chunk, got {:?}", other),
                }
            }
            _ => panic!("Expected ChatCompletionsStream variant"),
        }
    }

    #[test]
    fn test_process_sse_chunks_done_marker() {
        let chunks = vec!["[DONE]".to_string()];

        let result = process_sse_chunks(chunks);

        match result {
            AiResponse::ChatCompletionsStream(parsed_chunks) => {
                assert_eq!(parsed_chunks.len(), 1);
                match &parsed_chunks[0] {
                    ChatCompletionChunk::Done => {}
                    _ => panic!("Expected Done variant"),
                }
            }
            _ => panic!("Expected ChatCompletionsStream variant"),
        }
    }

    #[test]
    fn test_parse_streaming_response_sse_success() {
        let result =
            parse_streaming_response("data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\"}\n\ndata: [DONE]\n\n").unwrap();

        match result {
            AiResponse::ChatCompletionsStream(_) => {}
            _ => panic!("Expected ChatCompletionsStream variant"),
        }
    }

    #[test]
    fn test_parse_streaming_response_json_fallback() {
        let result = parse_streaming_response(r#"{"id":"chatcmpl-123","choices":[]}"#).unwrap();

        matches!(result, AiResponse::Other(_));
    }

    #[test]
    fn test_parse_streaming_response_both_fail() {
        let result = parse_streaming_response("not sse and not json");

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_non_streaming_response_json_success() {
        let result = parse_non_streaming_response(r#"{"id":"chatcmpl-123","choices":[]}"#).unwrap();

        matches!(result, AiResponse::Other(_));
    }

    #[test]
    fn test_parse_non_streaming_response_sse_fails() {
        let sse_data = "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\"}\n\ndata: [DONE]\n\n";

        let result = parse_non_streaming_response(sse_data);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_non_streaming_response_invalid_json() {
        let invalid_data = "not json";

        let result = parse_non_streaming_response(invalid_data);

        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_response_no_compression() {
        let data = b"hello world";
        let headers = HashMap::new();

        let result = decompress_response_if_needed(data, &headers).unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_decompress_response_unknown_encoding() {
        let data = b"hello world";
        let mut headers = HashMap::new();
        headers.insert("content-encoding".to_string(), vec![Bytes::from("deflate")]);

        let result = decompress_response_if_needed(data, &headers).unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_decompress_response_gzip() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write as _;

        let original = b"hello world";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut headers = HashMap::new();
        headers.insert("content-encoding".to_string(), vec![Bytes::from("gzip")]);

        let result = decompress_response_if_needed(&compressed, &headers).unwrap();

        assert_eq!(result, original);
    }

    #[test]
    fn test_decompress_response_gzip_invalid_data() {
        let data = b"not valid gzip";
        let mut headers = HashMap::new();
        headers.insert("content-encoding".to_string(), vec![Bytes::from("gzip")]);

        let result = decompress_response_if_needed(data, &headers);

        assert!(result.is_err());
    }


    #[test]
    fn test_extract_fusillade_request_id() {
        let test_uuid = uuid::Uuid::new_v4();
        let mut headers = HashMap::new();
        headers.insert("x-fusillade-request-id".to_string(), vec![Bytes::from(test_uuid.to_string())]);

        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers,
            body: None,
            trace_id: None,
            span_id: None,
        };

        let result = extract_header_as_string(&request_data, "x-fusillade-request-id").and_then(|s| uuid::Uuid::parse_str(&s).ok());

        assert!(result.is_some());
        assert_eq!(result.unwrap(), test_uuid);
    }

    #[test]
    fn test_extract_fusillade_request_id_missing_header() {
        let headers = HashMap::new();

        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers,
            body: None,
            trace_id: None,
            span_id: None,
        };

        let result = extract_header_as_string(&request_data, "x-fusillade-request-id");

        assert!(result.is_none());
    }

    #[test]
    fn test_extract_fusillade_request_id_empty_value() {
        let mut headers = HashMap::new();
        headers.insert("x-fusillade-request-id".to_string(), vec![]);

        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers,
            body: None,
            trace_id: None,
            span_id: None,
        };

        let result = extract_header_as_string(&request_data, "x-fusillade-request-id");

        assert!(result.is_none());
    }

    #[test]
    fn test_extract_header_as_string_returns_non_uuid_values() {
        let mut headers = HashMap::new();
        headers.insert("x-fusillade-request-id".to_string(), vec![Bytes::from("notvalid")]);

        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers,
            body: None,
            trace_id: None,
            span_id: None,
        };

        let header_str = extract_header_as_string(&request_data, "x-fusillade-request-id");
        assert_eq!(header_str, Some("notvalid".to_string()));

        assert!(header_str.is_some());
        assert_eq!(header_str.unwrap(), "notvalid");
    }

    #[test]
    fn test_extract_fusillade_request_id_invalid_utf8() {
        let mut headers = HashMap::new();
        headers.insert("x-fusillade-request-id".to_string(), vec![Bytes::from(vec![0xFF, 0xFE, 0xFD])]);

        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers,
            body: None,
            trace_id: None,
            span_id: None,
        };

        let result = extract_header_as_string(&request_data, "x-fusillade-request-id");

        assert!(result.is_none());
    }

    #[test]
    fn test_extract_fusillade_request_id_all_zeros() {
        let mut headers = HashMap::new();
        headers.insert(
            "x-fusillade-request-id".to_string(),
            vec![Bytes::from("00000000-0000-0000-0000-000000000000")],
        );

        let request_data = RequestData {
            correlation_id: 123,
            timestamp: SystemTime::now(),
            method: Method::POST,
            uri: "/test".parse::<Uri>().unwrap(),
            headers,
            body: None,
            trace_id: None,
            span_id: None,
        };

        let result = extract_header_as_string(&request_data, "x-fusillade-request-id").and_then(|s| uuid::Uuid::parse_str(&s).ok());

        assert!(result.is_some());
        let uuid = result.unwrap();
        assert_eq!(uuid.to_string(), "00000000-0000-0000-0000-000000000000");
    }

    fn minimal_response_json(with_usage: bool) -> String {
        let usage = if with_usage {
            r#","usage":{"input_tokens":15,"input_tokens_details":{"cached_tokens":0},"output_tokens":25,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":40}"#
        } else {
            ""
        };
        format!(
            r#"{{"id":"resp_1","object":"response","created_at":1000,"status":"completed","model":"gpt-4o","output":[],"tools":[],"tool_choice":"auto","truncation":"disabled","parallel_tool_calls":true,"text":{{"format":{{"type":"text"}}}},"top_p":1.0,"presence_penalty":0.0,"frequency_penalty":0.0,"top_logprobs":0,"temperature":1.0,"reasoning":null,"store":false,"background":false,"service_tier":"default"{usage}}}"#
        )
    }

    #[test]
    fn test_parse_responses_non_streaming_valid() {
        let body = minimal_response_json(true);
        let result = parse_responses_non_streaming_response(&body).unwrap();

        match result {
            AiResponse::Responses(resp) => {
                assert_eq!(resp.model, "gpt-4o");
                let usage = resp.usage.unwrap();
                assert_eq!(usage.input_tokens, 15);
                assert_eq!(usage.output_tokens, 25);
                assert_eq!(usage.total_tokens, 40);
            }
            _ => panic!("expected AiResponse::Responses"),
        }
    }

    #[test]
    fn test_parse_responses_non_streaming_not_a_response_object() {
        let result = parse_responses_non_streaming_response(r#"{"error":{"message":"bad request"}}"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_responses_streaming_valid() {
        let response_json = minimal_response_json(true);
        let sse = format!(
            "data: {{\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"item_id\":\"i\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hi\"}}\n\ndata: {{\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{response_json}}}\n\n"
        );

        let result = parse_responses_streaming_response(&sse).unwrap();

        match result {
            AiResponse::ResponsesStream(events) => {
                assert!(!events.is_empty());
                let completed = events.iter().find(|e| e.event_type == "response.completed");
                assert!(completed.is_some(), "should contain a response.completed event");

                let usage = completed.unwrap().response.as_ref().unwrap().usage.as_ref().unwrap();
                assert_eq!(usage.input_tokens, 15);
                assert_eq!(usage.output_tokens, 25);
                assert_eq!(usage.total_tokens, 40);
            }
            _ => panic!("expected AiResponse::ResponsesStream"),
        }
    }

    #[test]
    fn test_parse_responses_streaming_empty_events_is_error() {
        let sse = "data: {\"not_a_response_event\":true}\n\ndata: {\"also_not\":true}\n\n";
        let result = parse_responses_streaming_response(sse);
        assert!(result.is_err(), "empty parsed events should return an error");
    }

    #[test]
    fn test_parse_responses_streaming_no_sse_is_error() {
        let result = parse_responses_streaming_response("not sse format at all");
        assert!(result.is_err());
    }
}
