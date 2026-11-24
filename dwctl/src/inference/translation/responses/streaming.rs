
use onwards::strict::schemas::chat_completions::{ChatCompletionChunk, ChunkChoice};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::types::{
    ContentPart, FunctionCallItem, Include, IncompleteDetails, Item, ItemStatus, MessageContent, MessageItem, ReasoningContent,
    ReasoningItem, ResponseStatus, ResponseUsage, ResponsesRequest, ResponsesResponse, SummaryContent, TextConfig, TextFormat,
    TruncationStrategy,
};
use super::util::{cache_write_tokens, chat_usage_to_response_usage, incomplete_reason, merge_reasoning_text};

#[derive(Debug, Clone)]
pub struct StreamingState {
    response_id: String,
    model: String,
    created_at: u64,
    items: Vec<StreamingItem>,
    completed_items: Vec<StreamingItem>,
    item_index_offset: usize,
    sequence_number: u32,
    started: bool,
    usage: Option<ResponseUsage>,
    request: ResponsesRequest,
    msg_item_for_choice: HashMap<usize, usize>,
    fn_call_item_for: HashMap<(usize, usize), usize>,
    reasoning_item_for_choice: HashMap<usize, usize>,
    incomplete_reason: Option<&'static str>,
}

#[derive(Debug, Clone)]
enum StreamingItemKind {
    Message {
        role: String,
        content_text: String,
        logprobs: Vec<serde_json::Value>,
        content_part_started: bool,
        content_index: u32,
    },
    FunctionCall {
        call_id: String,
        name: String,
        arguments: String,
    },
    Reasoning {
        summary_text: String,
        summary_part_started: bool,
    },
}

#[derive(Debug, Clone)]
struct StreamingItem {
    id: String,
    status: ItemStatus,
    kind: StreamingItemKind,
}

fn generate_response_id() -> String {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();

    use rand::RngExt;
    let random: u64 = rand::rng().random();

    format!("resp_{:016x}_{:08x}", timestamp, random)
}

impl StreamingState {
    pub fn new(request: &ResponsesRequest, response_id: Option<&str>) -> Self {
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

        Self {
            response_id: match response_id {
                Some(id) => format!("resp_{id}"),
                None => generate_response_id(),
            },
            model: request.model.clone(),
            created_at: timestamp,
            items: Vec::new(),
            completed_items: Vec::new(),
            item_index_offset: 0,
            sequence_number: 0,
            started: false,
            usage: None,
            request: request.clone(),
            msg_item_for_choice: HashMap::new(),
            fn_call_item_for: HashMap::new(),
            reasoning_item_for_choice: HashMap::new(),
            incomplete_reason: None,
        }
    }

    pub fn set_response_id(&mut self, id: String) {
        self.response_id = id;
    }

    pub fn process_chunk(&mut self, chunk: &ChatCompletionChunk) -> Vec<StreamingEvent> {
        let mut events = Vec::new();

        if !self.started {
            self.started = true;
            events.push(self.create_response_created_event());
        }

        for choice in &chunk.choices {
            let choice_events = self.process_choice(choice);
            events.extend(choice_events);
        }

        if let Some(ref usage) = chunk.usage {
            self.usage = Some(chat_usage_to_response_usage(usage));
        }

        for choice in &chunk.choices {
            if choice.finish_reason.is_some() {
                let reason = incomplete_reason(choice.finish_reason.as_deref());
                self.incomplete_reason = self.incomplete_reason.or(reason);
                let status = if reason.is_some() {
                    ItemStatus::Incomplete
                } else {
                    ItemStatus::Completed
                };
                let done_events = self.finalize_for_choice(choice.index as usize, status);
                events.extend(done_events);
            }
        }

        events
    }

    pub(super) fn preserve_cache_write_tokens(&mut self, raw_usage: Option<&Value>) {
        if let Some(usage) = self.usage.as_mut() {
            usage.input_tokens_details.cache_write_tokens = cache_write_tokens(raw_usage);
        }
    }

    pub fn finalize(&mut self) -> Vec<StreamingEvent> {
        let mut events = Vec::new();

        for i in 0..self.items.len() {
            if self.items[i].status == ItemStatus::InProgress {
                let done_events = self.finalize_item(i, ItemStatus::Completed);
                events.extend(done_events);
            }
        }

        events.push(self.create_response_terminal_event());

        events
    }

    fn process_choice(&mut self, choice: &ChunkChoice) -> Vec<StreamingEvent> {
        let mut events = Vec::new();
        let choice_index = choice.index as usize;
        let delta = &choice.delta;

        let reasoning_merged = merge_reasoning_text(
            delta.reasoning.as_ref(),
            delta.reasoning_content.as_ref(),
            delta.reasoning_details.as_ref(),
        );
        let reasoning_delta: Option<&str> = if reasoning_merged.is_empty() {
            None
        } else {
            Some(reasoning_merged.as_str())
        };

        if let Some(reasoning_text) = reasoning_delta {
            let r_idx = if let Some(&idx) = self.reasoning_item_for_choice.get(&choice_index) {
                idx
            } else {
                let idx = self.items.len();
                let item = StreamingItem {
                    id: format!("item_{}", self.item_index_offset + idx),
                    status: ItemStatus::InProgress,
                    kind: StreamingItemKind::Reasoning {
                        summary_text: String::new(),
                        summary_part_started: false,
                    },
                };
                self.items.push(item);
                self.reasoning_item_for_choice.insert(choice_index, idx);
                events.push(self.create_item_added_event(idx));
                idx
            };

            let mut emit_summary_part_added = false;
            if let StreamingItemKind::Reasoning {
                summary_text,
                summary_part_started,
            } = &mut self.items[r_idx].kind
            {
                if !*summary_part_started {
                    *summary_part_started = true;
                    emit_summary_part_added = true;
                }
                summary_text.push_str(reasoning_text);
            }

            if emit_summary_part_added {
                events.push(self.create_reasoning_summary_part_added_event(r_idx));
            }
            events.push(self.create_reasoning_summary_text_delta_event(r_idx, reasoning_text));
        }

        let mut emit_content_part_added = false;
        let mut emit_text_delta: Option<String> = None;
        let delta_logprobs = if self.request.includes(Include::MessageOutputTextLogprobs) {
            choice
                .logprobs
                .as_ref()
                .and_then(|value| value.get("content"))
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        let has_content = delta.content.as_ref().map(|s| !s.is_empty()).unwrap_or(false);

        if has_content {
            let msg_idx = if let Some(&idx) = self.msg_item_for_choice.get(&choice_index) {
                idx
            } else {
                let idx = self.items.len();
                let item = StreamingItem {
                    id: format!("item_{}", self.item_index_offset + idx),
                    status: ItemStatus::InProgress,
                    kind: StreamingItemKind::Message {
                        role: String::new(),
                        content_text: String::new(),
                        logprobs: Vec::new(),
                        content_part_started: false,
                        content_index: 0,
                    },
                };
                self.items.push(item);
                self.msg_item_for_choice.insert(choice_index, idx);
                events.push(self.create_item_added_event(idx));
                idx
            };

            if let StreamingItemKind::Message {
                role,
                content_text,
                logprobs,
                content_part_started,
                ..
            } = &mut self.items[msg_idx].kind
            {
                if let Some(ref r) = delta.role {
                    *role = r.clone();
                }
                if let Some(ref content) = delta.content
                    && !content.is_empty()
                {
                    if !*content_part_started {
                        *content_part_started = true;
                        emit_content_part_added = true;
                    }
                    content_text.push_str(content);
                    logprobs.extend(delta_logprobs.clone());
                    emit_text_delta = Some(content.clone());
                }
            }

            if emit_content_part_added {
                events.push(self.create_content_part_added_event(msg_idx));
            }
            if let Some(ref text) = emit_text_delta {
                events.push(self.create_text_delta_event(msg_idx, text, delta_logprobs));
            }
        }

        if let Some(ref tool_calls) = delta.tool_calls {
            for tc in tool_calls {
                let tc_index = tc.index as usize;
                let key = (choice_index, tc_index);

                let fc_idx = if let Some(&idx) = self.fn_call_item_for.get(&key) {
                    idx
                } else {
                    let idx = self.items.len();
                    let item = StreamingItem {
                        id: format!("item_{}", self.item_index_offset + idx),
                        status: ItemStatus::InProgress,
                        kind: StreamingItemKind::FunctionCall {
                            call_id: String::new(),
                            name: String::new(),
                            arguments: String::new(),
                        },
                    };
                    self.items.push(item);
                    self.fn_call_item_for.insert(key, idx);
                    events.push(self.create_item_added_event(idx));
                    idx
                };

                let mut emit_args_delta: Option<String> = None;

                if let StreamingItemKind::FunctionCall { call_id, name, arguments } = &mut self.items[fc_idx].kind {
                    if let Some(ref id) = tc.id {
                        *call_id = id.clone();
                    }
                    if let Some(ref func) = tc.function {
                        if let Some(ref n) = func.name {
                            *name = n.clone();
                        }
                        if let Some(ref args) = func.arguments
                            && !args.is_empty()
                        {
                            arguments.push_str(args);
                            emit_args_delta = Some(args.clone());
                        }
                    }
                }

                if let Some(delta) = emit_args_delta {
                    events.push(self.create_fn_call_arguments_delta_event(fc_idx, &delta));
                }
            }
        }

        events
    }

    fn finalize_for_choice(&mut self, choice_index: usize, status: ItemStatus) -> Vec<StreamingEvent> {
        let mut indices: Vec<usize> = Vec::new();
        if let Some(&idx) = self.reasoning_item_for_choice.get(&choice_index) {
            indices.push(idx);
        }
        if let Some(&idx) = self.msg_item_for_choice.get(&choice_index) {
            indices.push(idx);
        }
        let fc_indices: Vec<usize> = self
            .fn_call_item_for
            .iter()
            .filter(|((ci, _), _)| *ci == choice_index)
            .map(|(_, &idx)| idx)
            .collect();
        indices.extend(fc_indices);
        indices.sort_unstable();

        let mut events = Vec::new();
        for idx in indices {
            events.extend(self.finalize_item(idx, status));
        }
        events
    }

    fn finalize_item(&mut self, index: usize, status: ItemStatus) -> Vec<StreamingEvent> {
        if index >= self.items.len() || self.items[index].status != ItemStatus::InProgress {
            return vec![];
        }
        self.items[index].status = status;

        match &self.items[index].kind {
            StreamingItemKind::Message { content_part_started, .. } => {
                let had_content = *content_part_started;
                let mut events = Vec::new();
                if had_content {
                    events.push(self.create_content_part_done_event(index));
                }
                events.push(self.create_item_done_event(index));
                events
            }
            StreamingItemKind::FunctionCall { .. } => {
                vec![self.create_fn_call_arguments_done_event(index), self.create_item_done_event(index)]
            }
            StreamingItemKind::Reasoning { summary_part_started, .. } => {
                let had_part = *summary_part_started;
                let mut events = Vec::new();
                if had_part {
                    events.push(self.create_reasoning_summary_text_done_event(index));
                    events.push(self.create_reasoning_summary_part_done_event(index));
                }
                events.push(self.create_item_done_event(index));
                events
            }
        }
    }

    fn next_sequence(&mut self) -> u32 {
        let seq = self.sequence_number;
        self.sequence_number += 1;
        seq
    }

    fn create_response_created_event(&mut self) -> StreamingEvent {
        StreamingEvent {
            event_type: "response.created".to_string(),
            data: StreamingEventData::ResponseCreated {
                response: self.build_response_snapshot(ResponseStatus::InProgress, vec![], None),
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_item_added_event(&mut self, index: usize) -> StreamingEvent {
        let item = &self.items[index];
        let output_index = (self.item_index_offset + index) as u32;
        let output_item = match &item.kind {
            StreamingItemKind::Message { role, .. } => Item::Message(MessageItem {
                id: Some(item.id.clone()),
                role: if role.is_empty() { "assistant".to_string() } else { role.clone() },
                content: MessageContent::Parts(vec![]),
                status: Some(ItemStatus::InProgress),
            }),
            StreamingItemKind::FunctionCall { call_id, name, .. } => Item::FunctionCall(FunctionCallItem {
                id: Some(item.id.clone()),
                call_id: call_id.clone(),
                name: name.clone(),
                arguments: String::new(),
                status: Some(ItemStatus::InProgress),
            }),
            StreamingItemKind::Reasoning { .. } => Item::Reasoning(ReasoningItem {
                id: Some(item.id.clone()),
                content: None,
                encrypted_content: None,
                summary: Some(vec![]),
                status: Some(ItemStatus::InProgress),
            }),
        };
        StreamingEvent {
            event_type: "response.output_item.added".to_string(),
            data: StreamingEventData::OutputItemAdded {
                output_index,
                item: output_item,
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_content_part_added_event(&mut self, index: usize) -> StreamingEvent {
        let content_index = match &self.items[index].kind {
            StreamingItemKind::Message { content_index, .. } => *content_index,
            StreamingItemKind::FunctionCall { .. } | StreamingItemKind::Reasoning { .. } => 0,
        };
        StreamingEvent {
            event_type: "response.content_part.added".to_string(),
            data: StreamingEventData::ContentPartAdded {
                item_id: self.items[index].id.clone(),
                output_index: (self.item_index_offset + index) as u32,
                content_index,
                part: ContentPart::OutputText {
                    text: String::new(),
                    annotations: vec![],
                    logprobs: vec![],
                },
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_text_delta_event(&mut self, index: usize, delta: &str, logprobs: Vec<serde_json::Value>) -> StreamingEvent {
        let content_index = match &self.items[index].kind {
            StreamingItemKind::Message { content_index, .. } => *content_index,
            StreamingItemKind::FunctionCall { .. } | StreamingItemKind::Reasoning { .. } => 0,
        };
        StreamingEvent {
            event_type: "response.output_text.delta".to_string(),
            data: StreamingEventData::OutputTextDelta {
                item_id: self.items[index].id.clone(),
                output_index: (self.item_index_offset + index) as u32,
                content_index,
                delta: delta.to_string(),
                logprobs,
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_content_part_done_event(&mut self, index: usize) -> StreamingEvent {
        let (content_text, content_index, logprobs) = match &self.items[index].kind {
            StreamingItemKind::Message {
                content_text,
                content_index,
                logprobs,
                ..
            } => (content_text.clone(), *content_index, logprobs.clone()),
            StreamingItemKind::FunctionCall { .. } | StreamingItemKind::Reasoning { .. } => (String::new(), 0, Vec::new()),
        };
        StreamingEvent {
            event_type: "response.content_part.done".to_string(),
            data: StreamingEventData::ContentPartDone {
                item_id: self.items[index].id.clone(),
                output_index: (self.item_index_offset + index) as u32,
                content_index,
                part: ContentPart::OutputText {
                    text: content_text,
                    annotations: vec![],
                    logprobs,
                },
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_item_done_event(&mut self, index: usize) -> StreamingEvent {
        let item = &self.items[index];
        let output_index = (self.item_index_offset + index) as u32;
        let output_item = match &item.kind {
            StreamingItemKind::Message {
                role,
                content_text,
                logprobs,
                ..
            } => Item::Message(MessageItem {
                id: Some(item.id.clone()),
                role: if role.is_empty() { "assistant".to_string() } else { role.clone() },
                content: MessageContent::Parts(vec![ContentPart::OutputText {
                    text: content_text.clone(),
                    annotations: vec![],
                    logprobs: logprobs.clone(),
                }]),
                status: Some(item.status),
            }),
            StreamingItemKind::FunctionCall { call_id, name, arguments } => Item::FunctionCall(FunctionCallItem {
                id: Some(item.id.clone()),
                call_id: call_id.clone(),
                name: name.clone(),
                arguments: arguments.clone(),
                status: Some(item.status),
            }),
            StreamingItemKind::Reasoning { summary_text, .. } => Item::Reasoning(ReasoningItem {
                id: Some(item.id.clone()),
                content: Some(vec![ReasoningContent::Text {
                    text: summary_text.clone(),
                }]),
                encrypted_content: None,
                summary: Some(vec![SummaryContent::Text {
                    text: summary_text.clone(),
                }]),
                status: Some(item.status),
            }),
        };
        StreamingEvent {
            event_type: "response.output_item.done".to_string(),
            data: StreamingEventData::OutputItemDone {
                output_index,
                item: output_item,
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_reasoning_summary_part_added_event(&mut self, index: usize) -> StreamingEvent {
        StreamingEvent {
            event_type: "response.reasoning_summary_part.added".to_string(),
            data: StreamingEventData::ReasoningSummaryPartAdded {
                item_id: self.items[index].id.clone(),
                output_index: (self.item_index_offset + index) as u32,
                summary_index: 0,
                part: SummaryContent::Text { text: String::new() },
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_reasoning_summary_text_delta_event(&mut self, index: usize, delta: &str) -> StreamingEvent {
        StreamingEvent {
            event_type: "response.reasoning_summary_text.delta".to_string(),
            data: StreamingEventData::ReasoningSummaryTextDelta {
                item_id: self.items[index].id.clone(),
                output_index: (self.item_index_offset + index) as u32,
                summary_index: 0,
                delta: delta.to_string(),
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_reasoning_summary_text_done_event(&mut self, index: usize) -> StreamingEvent {
        let text = match &self.items[index].kind {
            StreamingItemKind::Reasoning { summary_text, .. } => summary_text.clone(),
            _ => String::new(),
        };
        StreamingEvent {
            event_type: "response.reasoning_summary_text.done".to_string(),
            data: StreamingEventData::ReasoningSummaryTextDone {
                item_id: self.items[index].id.clone(),
                output_index: (self.item_index_offset + index) as u32,
                summary_index: 0,
                text,
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_reasoning_summary_part_done_event(&mut self, index: usize) -> StreamingEvent {
        let text = match &self.items[index].kind {
            StreamingItemKind::Reasoning { summary_text, .. } => summary_text.clone(),
            _ => String::new(),
        };
        StreamingEvent {
            event_type: "response.reasoning_summary_part.done".to_string(),
            data: StreamingEventData::ReasoningSummaryPartDone {
                item_id: self.items[index].id.clone(),
                output_index: (self.item_index_offset + index) as u32,
                summary_index: 0,
                part: SummaryContent::Text { text },
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_fn_call_arguments_delta_event(&mut self, index: usize, delta: &str) -> StreamingEvent {
        StreamingEvent {
            event_type: "response.function_call_arguments.delta".to_string(),
            data: StreamingEventData::FunctionCallArgumentsDelta {
                item_id: self.items[index].id.clone(),
                output_index: (self.item_index_offset + index) as u32,
                delta: delta.to_string(),
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_fn_call_arguments_done_event(&mut self, index: usize) -> StreamingEvent {
        let arguments = match &self.items[index].kind {
            StreamingItemKind::FunctionCall { arguments, .. } => arguments.clone(),
            StreamingItemKind::Message { .. } | StreamingItemKind::Reasoning { .. } => String::new(),
        };
        StreamingEvent {
            event_type: "response.function_call_arguments.done".to_string(),
            data: StreamingEventData::FunctionCallArgumentsDone {
                item_id: self.items[index].id.clone(),
                output_index: (self.item_index_offset + index) as u32,
                arguments,
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn create_response_terminal_event(&mut self) -> StreamingEvent {
        let event_type = if self.incomplete_reason.is_some() {
            "response.incomplete"
        } else {
            "response.completed"
        };
        StreamingEvent {
            event_type: event_type.to_string(),
            data: StreamingEventData::ResponseCompleted {
                response: self.build_final_response(),
            },
            sequence_number: self.next_sequence(),
        }
    }

    fn build_response_snapshot(&self, status: ResponseStatus, output: Vec<Item>, usage: Option<ResponseUsage>) -> ResponsesResponse {
        let req = &self.request;

        let tool_choice = req
            .tool_choice
            .as_ref()
            .and_then(|tc| serde_json::to_value(tc).ok())
            .unwrap_or(serde_json::Value::String("auto".to_string()));

        let completed_at = if status == ResponseStatus::Completed {
            Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs())
        } else {
            None
        };

        ResponsesResponse {
            id: self.response_id.clone(),
            object: "response".to_string(),
            created_at: self.created_at,
            completed_at,
            status,
            incomplete_details: None,
            model: self.model.clone(),
            previous_response_id: req.previous_response_id.clone(),
            instructions: req.instructions.clone(),
            output,
            error: None,
            tools: req.tools.clone().unwrap_or_default(),
            tool_choice,
            truncation: req.truncation.clone().unwrap_or(TruncationStrategy::Disabled),
            parallel_tool_calls: req.parallel_tool_calls.unwrap_or(true),
            text: req.text.clone().unwrap_or(TextConfig {
                format: Some(TextFormat::Text),
            }),
            top_p: req.top_p.unwrap_or(1.0),
            presence_penalty: 0.0,
            frequency_penalty: 0.0,
            top_logprobs: req.top_logprobs.unwrap_or(0),
            temperature: req.temperature.unwrap_or(1.0),
            reasoning: serde_json::to_value(&req.reasoning).unwrap_or(serde_json::Value::Null),
            usage,
            max_output_tokens: req.max_output_tokens,
            max_tool_calls: None,
            store: req.store.unwrap_or(false),
            background: false,
            service_tier: "default".to_string(),
            metadata: req.metadata.clone(),
            safety_identifier: None,
            prompt_cache_key: req.prompt_cache_key.clone(),
        }
    }

    fn build_final_response(&self) -> ResponsesResponse {
        let output: Vec<Item> = self
            .completed_items
            .iter()
            .chain(self.items.iter())
            .map(|item| match &item.kind {
                StreamingItemKind::Message {
                    role,
                    content_text,
                    logprobs,
                    ..
                } => Item::Message(MessageItem {
                    id: Some(item.id.clone()),
                    role: if role.is_empty() { "assistant".to_string() } else { role.clone() },
                    content: MessageContent::Parts(vec![ContentPart::OutputText {
                        text: content_text.clone(),
                        annotations: vec![],
                        logprobs: logprobs.clone(),
                    }]),
                    status: Some(item.status),
                }),
                StreamingItemKind::FunctionCall { call_id, name, arguments } => Item::FunctionCall(FunctionCallItem {
                    id: Some(item.id.clone()),
                    call_id: call_id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                    status: Some(item.status),
                }),
                StreamingItemKind::Reasoning { summary_text, .. } => Item::Reasoning(ReasoningItem {
                    id: Some(item.id.clone()),
                    content: Some(vec![ReasoningContent::Text {
                        text: summary_text.clone(),
                    }]),
                    encrypted_content: None,
                    summary: Some(vec![SummaryContent::Text {
                        text: summary_text.clone(),
                    }]),
                    status: Some(item.status),
                }),
            })
            .collect();

        let status = if self.incomplete_reason.is_some() {
            ResponseStatus::Incomplete
        } else {
            ResponseStatus::Completed
        };
        let mut response = self.build_response_snapshot(status, output, self.usage.clone());
        response.incomplete_details = self.incomplete_reason.map(|reason| IncompleteDetails {
            reason: reason.to_string(),
        });
        response
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(flatten)]
    pub data: StreamingEventData,
    pub sequence_number: u32,
}

impl StreamingEvent {
    pub fn to_sse(&self) -> String {
        let json = serde_json::to_string(&self).unwrap_or_default();
        format!("event: {}\ndata: {}\n\n", self.event_type, json)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StreamingEventData {
    ResponseCreated {
        response: ResponsesResponse,
    },
    OutputItemAdded {
        output_index: u32,
        item: Item,
    },
    ContentPartAdded {
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: ContentPart,
    },
    OutputTextDelta {
        item_id: String,
        output_index: u32,
        content_index: u32,
        delta: String,
        logprobs: Vec<serde_json::Value>,
    },
    ContentPartDone {
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: ContentPart,
    },
    OutputItemDone {
        output_index: u32,
        item: Item,
    },
    FunctionCallArgumentsDelta {
        item_id: String,
        output_index: u32,
        delta: String,
    },
    FunctionCallArgumentsDone {
        item_id: String,
        output_index: u32,
        arguments: String,
    },
    ReasoningSummaryPartAdded {
        item_id: String,
        output_index: u32,
        summary_index: u32,
        part: SummaryContent,
    },
    ReasoningSummaryTextDelta {
        item_id: String,
        output_index: u32,
        summary_index: u32,
        delta: String,
    },
    ReasoningSummaryTextDone {
        item_id: String,
        output_index: u32,
        summary_index: u32,
        text: String,
    },
    ReasoningSummaryPartDone {
        item_id: String,
        output_index: u32,
        summary_index: u32,
        part: SummaryContent,
    },
    ResponseCompleted {
        response: ResponsesResponse,
    },
}

#[cfg(test)]
mod tests {
    use super::super::types::Input;
    use super::*;
    use onwards::strict::schemas::chat_completions::{ChunkChoice, ChunkDelta, ChunkFunctionCall, ChunkToolCall};

    fn test_request(model: &str) -> ResponsesRequest {
        ResponsesRequest {
            model: model.to_string(),
            input: Input::Text(String::new()),
            instructions: None,
            previous_response_id: None,
            include: None,
            store: None,
            metadata: None,
            temperature: None,
            top_p: None,
            top_logprobs: None,
            max_output_tokens: None,
            stop: None,
            stream: None,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            truncation: None,
            user: None,
            prompt_cache_key: None,
            reasoning: None,
            text: None,
            background: None,
            service_tier: None,
            extra: None,
        }
    }

    fn create_test_chunk(id: &str, content: Option<&str>, role: Option<&str>, finish_reason: Option<&str>) -> ChatCompletionChunk {
        ChatCompletionChunk {
            id: id.to_string(),
            object: "chat.completion.chunk".to_string(),
            created: 1234567890,
            model: "gpt-4".to_string(),
            choices: vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta {
                    role: role.map(String::from),
                    content: content.map(String::from),
                    tool_calls: None,
                    reasoning: None,
                    reasoning_content: None,
                    reasoning_details: None,
                },
                finish_reason: finish_reason.map(String::from),
                logprobs: None,
            }],
            usage: None,
            system_fingerprint: None,
            service_tier: None,
        }
    }

    #[test]
    fn test_streaming_state_initial_events() {
        let mut state = StreamingState::new(&test_request("gpt-4"), None);

        let chunk = create_test_chunk("chunk_1", None, Some("assistant"), None);
        let events = state.process_chunk(&chunk);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "response.created");

        let chunk2 = create_test_chunk("chunk_2", Some("Hi"), None, None);
        let events2 = state.process_chunk(&chunk2);
        assert_eq!(events2[0].event_type, "response.output_item.added");
    }

    #[test]
    fn test_streaming_state_content_events() {
        let mut state = StreamingState::new(&test_request("gpt-4"), None);

        let chunk1 = create_test_chunk("chunk_1", None, Some("assistant"), None);
        state.process_chunk(&chunk1);

        let chunk2 = create_test_chunk("chunk_2", Some("Hello"), None, None);
        let events = state.process_chunk(&chunk2);

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].event_type, "response.output_item.added");
        assert_eq!(events[1].event_type, "response.content_part.added");
        assert_eq!(events[2].event_type, "response.output_text.delta");
    }

    #[test]
    fn test_streaming_state_completion_events() {
        let mut state = StreamingState::new(&test_request("gpt-4"), None);

        let chunk1 = create_test_chunk("chunk_1", Some("Hi"), Some("assistant"), None);
        state.process_chunk(&chunk1);

        let chunk2 = create_test_chunk("chunk_2", None, None, Some("stop"));
        let events = state.process_chunk(&chunk2);

        assert!(events.iter().any(|e| e.event_type == "response.content_part.done"));
        assert!(events.iter().any(|e| e.event_type == "response.output_item.done"));
    }

    #[test]
    fn test_streaming_state_preserves_usage_token_details() {
        use onwards::strict::schemas::chat_completions::Usage;

        let mut state = StreamingState::new(&test_request("gpt-4"), None);

        let chunk1 = create_test_chunk("chunk_1", Some("Hi"), Some("assistant"), None);
        state.process_chunk(&chunk1);

        let mut chunk2 = create_test_chunk("chunk_2", None, None, Some("stop"));
        chunk2.usage = Some(Usage {
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 150,
            prompt_tokens_details: Some(serde_json::json!({ "cached_tokens": 42 })),
            completion_tokens_details: Some(serde_json::json!({ "reasoning_tokens": 30 })),
        });
        state.process_chunk(&chunk2);

        let usage = state.usage.as_ref().expect("usage should be captured");
        assert_eq!(usage.input_tokens, 100);
        assert_eq!(usage.output_tokens, 50);
        assert_eq!(usage.total_tokens, 150);
        assert_eq!(
            usage.input_tokens_details.cached_tokens, 42,
            "cached_tokens should flow through to streaming state"
        );
        assert_eq!(
            usage.output_tokens_details.reasoning_tokens, 30,
            "reasoning_tokens should flow through to streaming state"
        );
    }

    #[test]
    fn test_streaming_state_finalize() {
        let mut state = StreamingState::new(&test_request("gpt-4"), None);

        let chunk = create_test_chunk("chunk_1", Some("Hello"), Some("assistant"), Some("stop"));
        state.process_chunk(&chunk);

        let events = state.finalize();

        assert!(events.iter().any(|e| e.event_type == "response.completed"));
    }

    #[test]
    fn streamed_snapshots_echo_prompt_cache_key() {
        let mut request = test_request("gpt-4");
        request.prompt_cache_key = Some("session-123".to_string());
        let mut state = StreamingState::new(&request, None);

        let mut events = state.process_chunk(&create_test_chunk("chunk_1", Some("Hello"), Some("assistant"), Some("stop")));
        events.extend(state.finalize());

        let snapshots: Vec<&ResponsesResponse> = events
            .iter()
            .filter_map(|event| match &event.data {
                StreamingEventData::ResponseCreated { response } | StreamingEventData::ResponseCompleted { response } => Some(response),
                _ => None,
            })
            .collect();
        assert_eq!(snapshots.len(), 2, "created and completed snapshots");
        for response in snapshots {
            assert_eq!(response.prompt_cache_key.as_deref(), Some("session-123"));
        }
    }

    #[test]
    fn test_event_sse_format() {
        let event = StreamingEvent {
            event_type: "response.output_text.delta".to_string(),
            data: StreamingEventData::OutputTextDelta {
                item_id: "item_0".to_string(),
                output_index: 0,
                content_index: 0,
                delta: "Hello".to_string(),
                logprobs: vec![],
            },
            sequence_number: 1,
        };

        let sse = event.to_sse();
        assert!(sse.starts_with("event: response.output_text.delta\n"));
        assert!(sse.contains("data:"));
    }

    #[test]
    fn test_sequence_numbers_increase() {
        let mut state = StreamingState::new(&test_request("gpt-4"), None);

        let chunk1 = create_test_chunk("chunk_1", Some("Hello"), Some("assistant"), None);
        let events1 = state.process_chunk(&chunk1);

        let chunk2 = create_test_chunk("chunk_2", Some(" world"), None, None);
        let events2 = state.process_chunk(&chunk2);

        let all_events: Vec<_> = events1.into_iter().chain(events2).collect();
        for i in 1..all_events.len() {
            assert!(all_events[i].sequence_number > all_events[i - 1].sequence_number);
        }
    }

    fn create_tool_call_chunk(
        id: &str,
        role: Option<&str>,
        tool_call_id: Option<&str>,
        tool_name: Option<&str>,
        tool_args: Option<&str>,
        finish_reason: Option<&str>,
    ) -> ChatCompletionChunk {
        let tool_calls = if tool_call_id.is_some() || tool_name.is_some() || tool_args.is_some() {
            Some(vec![ChunkToolCall {
                index: 0,
                id: tool_call_id.map(String::from),
                call_type: tool_call_id.map(|_| "function".to_string()),
                function: if tool_name.is_some() || tool_args.is_some() {
                    Some(ChunkFunctionCall {
                        name: tool_name.map(String::from),
                        arguments: tool_args.map(String::from),
                    })
                } else {
                    None
                },
            }])
        } else {
            None
        };

        ChatCompletionChunk {
            id: id.to_string(),
            object: "chat.completion.chunk".to_string(),
            created: 1234567890,
            model: "gpt-4".to_string(),
            choices: vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta {
                    role: role.map(String::from),
                    content: None,
                    tool_calls,
                    reasoning: None,
                    reasoning_content: None,
                    reasoning_details: None,
                },
                finish_reason: finish_reason.map(String::from),
                logprobs: None,
            }],
            usage: None,
            system_fingerprint: None,
            service_tier: None,
        }
    }

    fn create_reasoning_chunk(
        id: &str,
        reasoning: Option<&str>,
        reasoning_content: Option<&str>,
        content: Option<&str>,
        role: Option<&str>,
        finish_reason: Option<&str>,
    ) -> ChatCompletionChunk {
        ChatCompletionChunk {
            id: id.to_string(),
            object: "chat.completion.chunk".to_string(),
            created: 1234567890,
            model: "deepseek-r1".to_string(),
            choices: vec![ChunkChoice {
                index: 0,
                delta: ChunkDelta {
                    role: role.map(String::from),
                    content: content.map(String::from),
                    tool_calls: None,
                    reasoning: reasoning.map(String::from),
                    reasoning_content: reasoning_content.map(String::from),
                    reasoning_details: None,
                },
                finish_reason: finish_reason.map(String::from),
                logprobs: None,
            }],
            usage: None,
            system_fingerprint: None,
            service_tier: None,
        }
    }

    #[test]
    fn test_reasoning_only_stream_produces_reasoning_item() {
        let mut state = StreamingState::new(&test_request("deepseek-r1"), None);

        let events1 = state.process_chunk(&create_reasoning_chunk(
            "c1",
            Some("Let me think"),
            None,
            None,
            Some("assistant"),
            None,
        ));

        assert!(events1.iter().any(|e| e.event_type == "response.created"));
        assert!(events1.iter().any(|e| e.event_type == "response.output_item.added"));
        assert!(events1.iter().any(|e| e.event_type == "response.reasoning_summary_part.added"));
        assert!(events1.iter().any(|e| e.event_type == "response.reasoning_summary_text.delta"));

        let events2 = state.process_chunk(&create_reasoning_chunk("c2", Some(" step by step"), None, None, None, None));
        assert_eq!(events2.len(), 1);
        assert_eq!(events2[0].event_type, "response.reasoning_summary_text.delta");

        state.process_chunk(&create_reasoning_chunk("c3", None, None, None, None, Some("stop")));

        let final_events = state.finalize();
        let completed = final_events.iter().find(|e| e.event_type == "response.completed").unwrap();
        if let StreamingEventData::ResponseCompleted { response } = &completed.data {
            assert_eq!(response.output.len(), 1);
            assert!(matches!(response.output[0], Item::Reasoning(_)));
            if let Item::Reasoning(ref r) = response.output[0] {
                let summary = r.summary.as_ref().unwrap();
                assert_eq!(summary.len(), 1);
                let SummaryContent::Text { text } = &summary[0];
                assert_eq!(text, "Let me think step by step");
            }
        } else {
            panic!("Expected ResponseCompleted");
        }
    }

    #[test]
    fn length_cut_reasoning_only_stream_ends_incomplete() {
        let mut state = StreamingState::new(&test_request("deepseek-r1"), None);
        state.process_chunk(&create_reasoning_chunk(
            "c1",
            Some("Let me think"),
            None,
            None,
            Some("assistant"),
            None,
        ));

        let done_events = state.process_chunk(&create_reasoning_chunk("c2", None, None, None, None, Some("length")));
        let item_done = done_events
            .iter()
            .find(|e| e.event_type == "response.output_item.done")
            .expect("item done");
        let StreamingEventData::OutputItemDone {
            item: Item::Reasoning(reasoning),
            ..
        } = &item_done.data
        else {
            panic!("expected a reasoning item, got {:?}", item_done.data);
        };
        assert_eq!(reasoning.status, Some(ItemStatus::Incomplete));

        let final_events = state.finalize();
        assert!(final_events.iter().all(|e| e.event_type != "response.completed"));
        let terminal = final_events.last().expect("terminal event");
        assert_eq!(terminal.event_type, "response.incomplete");
        let StreamingEventData::ResponseCompleted { response } = &terminal.data else {
            panic!("expected a response snapshot, got {:?}", terminal.data);
        };
        assert_eq!(response.status, ResponseStatus::Incomplete);
        assert_eq!(
            response.incomplete_details.as_ref().map(|d| d.reason.as_str()),
            Some("max_output_tokens")
        );
        assert_eq!(response.completed_at, None);
    }

    #[test]
    fn tool_call_stream_ends_completed() {
        let mut state = StreamingState::new(&test_request("gpt-4"), None);
        state.process_chunk(&create_tool_call_chunk(
            "c1",
            Some("assistant"),
            Some("call_1"),
            Some("get_weather"),
            Some("{}"),
            None,
        ));
        state.process_chunk(&create_tool_call_chunk("c2", None, None, None, None, Some("tool_calls")));

        let terminal = state.finalize().pop().expect("terminal event");
        assert_eq!(terminal.event_type, "response.completed");
        let StreamingEventData::ResponseCompleted { response } = &terminal.data else {
            panic!("expected a response snapshot, got {:?}", terminal.data);
        };
        assert_eq!(response.status, ResponseStatus::Completed);
        assert!(response.incomplete_details.is_none());
    }

    #[test]
    fn test_reasoning_plus_content_stream() {
        let mut state = StreamingState::new(&test_request("deepseek-r1"), None);

        state.process_chunk(&create_reasoning_chunk(
            "c1",
            None,
            Some("thinking..."),
            None,
            Some("assistant"),
            None,
        ));

        state.process_chunk(&create_reasoning_chunk("c2", None, None, Some("The answer is 42."), None, None));

        state.process_chunk(&create_reasoning_chunk("c3", None, None, None, None, Some("stop")));

        let final_events = state.finalize();
        let completed = final_events.iter().find(|e| e.event_type == "response.completed").unwrap();
        if let StreamingEventData::ResponseCompleted { response } = &completed.data {
            assert_eq!(response.output.len(), 2);
            assert!(matches!(response.output[0], Item::Reasoning(_)), "First item should be Reasoning");
            assert!(matches!(response.output[1], Item::Message(_)), "Second item should be Message");
        } else {
            panic!("Expected ResponseCompleted");
        }
    }

    #[test]
    fn test_reasoning_stream_event_sequence() {
        let mut state = StreamingState::new(&test_request("deepseek-r1"), None);

        let events = state.process_chunk(&create_reasoning_chunk("c1", Some("step 1"), None, None, Some("assistant"), None));

        let event_types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
        assert_eq!(
            event_types,
            vec![
                "response.created",
                "response.output_item.added",
                "response.reasoning_summary_part.added",
                "response.reasoning_summary_text.delta",
            ]
        );

        let done_events = state.process_chunk(&create_reasoning_chunk("c2", None, None, None, None, Some("stop")));

        let done_types: Vec<&str> = done_events.iter().map(|e| e.event_type.as_str()).collect();
        assert_eq!(
            done_types,
            vec![
                "response.reasoning_summary_text.done",
                "response.reasoning_summary_part.done",
                "response.output_item.done",
            ]
        );
    }

    #[test]
    fn streaming_logprobs_include_projects_delta_and_completed_output() {
        let mut request = test_request("gpt-4");
        request.include = Some(vec![Include::MessageOutputTextLogprobs]);
        request.top_logprobs = Some(2);
        let mut state = StreamingState::new(&request, None);
        let token_logprob = serde_json::json!({
            "token": "Hi",
            "logprob": -0.2,
            "bytes": [72, 105],
            "top_logprobs": []
        });
        let mut chunk = create_test_chunk("c1", Some("Hi"), Some("assistant"), Some("stop"));
        chunk.choices[0].logprobs = Some(serde_json::json!({"content": [token_logprob.clone()]}));

        let events = state.process_chunk(&chunk);
        let delta = events
            .iter()
            .find(|event| event.event_type == "response.output_text.delta")
            .expect("text delta emitted");
        let StreamingEventData::OutputTextDelta { logprobs, .. } = &delta.data else {
            panic!("expected output text delta");
        };
        assert_eq!(logprobs, &vec![token_logprob.clone()]);

        let completed = state
            .finalize()
            .into_iter()
            .find(|event| event.event_type == "response.completed")
            .expect("response completed");
        let StreamingEventData::ResponseCompleted { response } = completed.data else {
            panic!("expected completed response");
        };
        assert_eq!(response.top_logprobs, 2);
        let Item::Message(message) = &response.output[0] else {
            panic!("expected message output");
        };
        let MessageContent::Parts(parts) = &message.content else {
            panic!("expected output parts");
        };
        let ContentPart::OutputText { logprobs, .. } = &parts[0] else {
            panic!("expected output text");
        };
        assert_eq!(logprobs, &vec![token_logprob]);
    }
}
