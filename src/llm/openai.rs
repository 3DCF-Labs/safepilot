use crate::llm::provider::LlmProvider;
use crate::llm::types::{
    CompletionResponse, ContentBlock, Message, Role, StopReason, ToolCall, ToolDefinition, Usage,
};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const OPENAI_API_URL: &str = "https://api.openai.com/v1/chat/completions";

pub struct OpenAIClient {
    client: Client,
    api_key: String,
    model: String,
}

impl OpenAIClient {
    pub fn new(api_key: String, model: Option<String>, timeout: Duration) -> Self {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            client,
            api_key,
            model: model.unwrap_or_else(|| "gpt-5-nano".into()),
        }
    }
}

#[async_trait]
impl LlmProvider for OpenAIClient {
    async fn complete(
        &self,
        messages: Vec<Message>,
        tools: Vec<ToolDefinition>,
        max_tokens: usize,
        system: Option<String>,
    ) -> Result<CompletionResponse> {
        let mut all_messages = Vec::new();
        if let Some(sys) = system {
            all_messages.push(OpenAIMessage::System { content: sys });
        }

        for m in messages {
            match m.role {
                Role::User => {
                    let text = join_text(&m.content);
                    all_messages.push(OpenAIMessage::User { content: text });
                }
                Role::Assistant => {
                    let text = join_text(&m.content);
                    let tool_calls = m
                        .content
                        .iter()
                        .filter_map(|b| match b {
                            ContentBlock::ToolUse(tc) => Some(OpenAIToolCall {
                                id: tc.id.clone(),
                                type_: "function".into(),
                                function: OpenAIFunctionCall {
                                    name: tc.name.clone(),
                                    arguments: serde_json::to_string(&tc.arguments)
                                        .unwrap_or_else(|_| "{}".into()),
                                },
                            }),
                            _ => None,
                        })
                        .collect::<Vec<_>>();

                    all_messages.push(OpenAIMessage::Assistant {
                        content: text,
                        tool_calls: if tool_calls.is_empty() {
                            None
                        } else {
                            Some(tool_calls)
                        },
                    });
                }
                Role::Tool => {
                    let (tool_call_id, content) = tool_result_to_openai(&m)?;
                    all_messages.push(OpenAIMessage::Tool {
                        tool_call_id,
                        content,
                    });
                }
            }
        }

        let use_completion_tokens = self.model.to_lowercase().contains("gpt-5");
        let request = OpenAIRequest {
            model: self.model.clone(),
            messages: all_messages,
            max_tokens: if use_completion_tokens {
                None
            } else {
                Some(max_tokens)
            },
            max_completion_tokens: if use_completion_tokens {
                Some(max_tokens)
            } else {
                None
            },
            tools: if tools.is_empty() {
                None
            } else {
                Some(
                    tools
                        .into_iter()
                        .map(|t| OpenAITool {
                            type_: "function".into(),
                            function: OpenAIFunction {
                                name: t.name,
                                description: t.description,
                                parameters: t.parameters,
                            },
                        })
                        .collect(),
                )
            },
        };

        let response = self
            .client
            .post(OPENAI_API_URL)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await
            .context("Failed to send request to OpenAI API")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("OpenAI API error ({status}): {error_text}"));
        }

        let api_response: OpenAIResponse = response
            .json()
            .await
            .context("Failed to parse OpenAI API response")?;

        Ok(api_response.into())
    }

    fn model_name(&self) -> &str {
        &self.model
    }
}

fn join_text(blocks: &[ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text(t) => Some(t.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn tool_result_to_openai(msg: &Message) -> Result<(String, String)> {
    let mut tool_call_id = None;
    let mut content = String::new();
    for b in &msg.content {
        if let ContentBlock::ToolResult(tr) = b {
            tool_call_id = Some(tr.tool_use_id.clone());
            content = tr.content.clone();
            break;
        }
    }
    let tool_call_id =
        tool_call_id.ok_or_else(|| anyhow!("Tool message missing ToolResult block"))?;
    Ok((tool_call_id, content))
}

#[derive(Serialize)]
struct OpenAIRequest {
    model: String,
    messages: Vec<OpenAIMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<usize>,
    #[serde(
        rename = "max_completion_tokens",
        skip_serializing_if = "Option::is_none"
    )]
    max_completion_tokens: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<OpenAITool>>,
}

#[derive(Serialize)]
#[serde(tag = "role")]
enum OpenAIMessage {
    #[serde(rename = "system")]
    System { content: String },
    #[serde(rename = "user")]
    User { content: String },
    #[serde(rename = "assistant")]
    Assistant {
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_calls: Option<Vec<OpenAIToolCall>>,
    },
    #[serde(rename = "tool")]
    Tool {
        tool_call_id: String,
        content: String,
    },
}

#[derive(Serialize)]
struct OpenAITool {
    #[serde(rename = "type")]
    type_: String,
    function: OpenAIFunction,
}

#[derive(Serialize)]
struct OpenAIFunction {
    name: String,
    description: String,
    parameters: serde_json::Value,
}

#[derive(Serialize, Deserialize)]
struct OpenAIToolCall {
    id: String,
    #[serde(rename = "type")]
    type_: String,
    function: OpenAIFunctionCall,
}

#[derive(Serialize, Deserialize)]
struct OpenAIFunctionCall {
    name: String,
    arguments: String,
}

#[derive(Deserialize)]
struct OpenAIResponse {
    choices: Vec<OpenAIChoice>,
    usage: OpenAIUsage,
}

#[derive(Deserialize)]
struct OpenAIChoice {
    message: OpenAIResponseMessage,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct OpenAIResponseMessage {
    content: Option<String>,
    tool_calls: Option<Vec<OpenAIToolCall>>,
}

#[derive(Deserialize)]
struct OpenAIUsage {
    prompt_tokens: usize,
    completion_tokens: usize,
}

impl From<OpenAIResponse> for CompletionResponse {
    fn from(resp: OpenAIResponse) -> Self {
        let choice = resp
            .choices
            .into_iter()
            .next()
            .expect("OpenAI response should have at least one choice");

        let mut content = Vec::new();

        if let Some(text) = choice.message.content {
            if !text.trim().is_empty() {
                content.push(ContentBlock::Text(text));
            }
        }

        if let Some(tool_calls) = choice.message.tool_calls {
            for tc in tool_calls {
                let args: serde_json::Value = serde_json::from_str(&tc.function.arguments)
                    .unwrap_or_else(|_| serde_json::json!({}));
                content.push(ContentBlock::ToolUse(ToolCall {
                    id: tc.id,
                    name: tc.function.name,
                    arguments: args,
                }));
            }
        }

        let stop_reason = match choice.finish_reason.as_deref() {
            Some("stop") => StopReason::EndTurn,
            Some("length") => StopReason::MaxTokens,
            Some("tool_calls") => StopReason::ToolUse,
            _ => StopReason::EndTurn,
        };

        CompletionResponse {
            content,
            stop_reason,
            usage: Usage {
                input_tokens: resp.usage.prompt_tokens,
                output_tokens: resp.usage.completion_tokens,
            },
        }
    }
}
