use crate::llm::provider::LlmProvider;
use crate::llm::types::{
    CompletionResponse, ContentBlock, Message, StopReason, ToolCall, ToolDefinition, ToolResult,
    Usage,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

const ANTHROPIC_API_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const MAX_RETRIES: usize = 3;

pub struct AnthropicClient {
    client: Client,
    api_key: String,
    model: String,
}

impl AnthropicClient {
    pub fn new(api_key: String, model: Option<String>, timeout: Duration) -> Self {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            client,
            api_key,
            model: model.unwrap_or_else(|| "claude-haiku-4-5-20251001".into()),
        }
    }
}

#[async_trait]
impl LlmProvider for AnthropicClient {
    async fn complete(
        &self,
        messages: Vec<Message>,
        tools: Vec<ToolDefinition>,
        max_tokens: usize,
        system: Option<String>,
    ) -> Result<CompletionResponse> {
        let request = AnthropicRequest {
            model: self.model.clone(),
            messages: messages.into_iter().map(Into::into).collect(),
            max_tokens,
            tools: if tools.is_empty() { None } else { Some(tools) },
            system,
        };

        let mut backoff_ms: u64 = 700;
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 1..=MAX_RETRIES {
            let send_res = self
                .client
                .post(ANTHROPIC_API_URL)
                .header("x-api-key", &self.api_key)
                .header("anthropic-version", ANTHROPIC_VERSION)
                .header("content-type", "application/json")
                .json(&request)
                .send()
                .await;

            match send_res {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        let api_response: AnthropicResponse = response
                            .json()
                            .await
                            .context("Failed to parse Anthropic API response")?;
                        return Ok(api_response.into());
                    }

                    let error_text = response.text().await.unwrap_or_default();
                    let retryable = status.is_server_error() || status.as_u16() == 429;
                    let err = anyhow!("Anthropic API error ({status}): {error_text}");
                    if retryable && attempt < MAX_RETRIES {
                        tracing::warn!(
                            attempt,
                            status = %status,
                            "Anthropic API transient failure, retrying"
                        );
                        sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(3_000);
                        last_err = Some(err);
                        continue;
                    }
                    return Err(err);
                }
                Err(err) => {
                    let err = anyhow!("Failed to send request to Anthropic API: {err}");
                    if attempt < MAX_RETRIES {
                        tracing::warn!(attempt, "Anthropic transport error, retrying");
                        sleep(Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(3_000);
                        last_err = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("Anthropic request failed after retries")))
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

#[derive(Serialize)]
struct AnthropicRequest {
    model: String,
    messages: Vec<AnthropicMessage>,
    max_tokens: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<ToolDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct AnthropicMessage {
    role: String,
    content: Vec<AnthropicContentBlock>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum AnthropicContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(default)]
        is_error: bool,
    },
}

impl From<Message> for AnthropicMessage {
    fn from(msg: Message) -> Self {
        let content = msg
            .content
            .into_iter()
            .map(|b| match b {
                ContentBlock::Text(text) => AnthropicContentBlock::Text { text },
                ContentBlock::ToolUse(tc) => AnthropicContentBlock::ToolUse {
                    id: tc.id,
                    name: tc.name,
                    input: tc.arguments,
                },
                ContentBlock::ToolResult(tr) => AnthropicContentBlock::ToolResult {
                    tool_use_id: tr.tool_use_id,
                    content: tr.content,
                    is_error: tr.is_error,
                },
            })
            .collect();

        let role = match msg.role {
            crate::llm::types::Role::User => "user",
            crate::llm::types::Role::Assistant => "assistant",
            crate::llm::types::Role::Tool => "user",
        }
        .to_string();

        Self { role, content }
    }
}

#[derive(Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContentBlock>,
    stop_reason: String,
    usage: AnthropicUsage,
}

#[derive(Deserialize)]
struct AnthropicUsage {
    input_tokens: usize,
    output_tokens: usize,
}

impl From<AnthropicResponse> for CompletionResponse {
    fn from(resp: AnthropicResponse) -> Self {
        let content = resp
            .content
            .into_iter()
            .map(|block| match block {
                AnthropicContentBlock::Text { text } => ContentBlock::Text(text),
                AnthropicContentBlock::ToolUse { id, name, input } => {
                    ContentBlock::ToolUse(ToolCall {
                        id,
                        name,
                        arguments: input,
                    })
                }
                AnthropicContentBlock::ToolResult {
                    tool_use_id,
                    content,
                    is_error,
                } => ContentBlock::ToolResult(ToolResult {
                    tool_use_id,
                    content,
                    is_error,
                }),
            })
            .collect();

        let stop_reason = match resp.stop_reason.as_str() {
            "end_turn" => StopReason::EndTurn,
            "max_tokens" => StopReason::MaxTokens,
            "tool_use" => StopReason::ToolUse,
            _ => StopReason::EndTurn,
        };

        CompletionResponse {
            content,
            stop_reason,
            usage: Usage {
                input_tokens: resp.usage.input_tokens,
                output_tokens: resp.usage.output_tokens,
            },
        }
    }
}
