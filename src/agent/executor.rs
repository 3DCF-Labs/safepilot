use crate::agent::context::{AgentContext, ToolCallRecord};
use crate::llm::provider::LlmProvider;
use crate::llm::types::{ContentBlock, Message, Role, StopReason, ToolResult};
use crate::tools::registry::ToolRegistry;
use anyhow::{anyhow, Result};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

pub struct Agent {
    provider: Arc<dyn LlmProvider>,
    tools: ToolRegistry,
    base_system_prompt: String,
    max_tokens: usize,
    request_timeout: std::time::Duration,
}

#[derive(Debug, Clone)]
pub struct AgentResponse {
    pub final_message: String,
    pub tool_calls: Vec<ToolCallRecord>,
    pub iterations: usize,
    pub total_tokens: usize,
    pub model: String,
}

impl Agent {
    pub fn new(
        provider: Arc<dyn LlmProvider>,
        tools: ToolRegistry,
        base_system_prompt: String,
        max_tokens: usize,
        request_timeout: std::time::Duration,
    ) -> Self {
        Self {
            provider,
            tools,
            base_system_prompt,
            max_tokens,
            request_timeout,
        }
    }

    pub async fn execute(
        &self,
        mut context: AgentContext,
        cancel: CancellationToken,
        system_context: Option<String>,
    ) -> Result<AgentResponse> {
        let mut total_tokens = 0usize;
        let mut final_message = String::new();

        let system_prompt = match system_context {
            Some(ctx) if !ctx.trim().is_empty() => {
                format!("{}\n\n{}", self.base_system_prompt, ctx)
            }
            _ => self.base_system_prompt.clone(),
        };

        while context.can_continue() {
            if cancel.is_cancelled() {
                return Err(anyhow!("Agent execution cancelled"));
            }

            let mut stop_after_tools = false;

            let resp = self
                .call_llm(context.messages.clone(), Some(system_prompt.clone()))
                .await?;

            total_tokens = total_tokens.saturating_add(resp.usage.input_tokens);
            total_tokens = total_tokens.saturating_add(resp.usage.output_tokens);

            let text = resp
                .content
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::Text(t) => Some(t.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");
            if !text.trim().is_empty() {
                final_message = text;
            }

            let tool_calls = resp
                .content
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::ToolUse(tc) => Some(tc.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();

            context.messages.push(Message {
                role: Role::Assistant,
                content: resp.content.clone(),
            });

            if tool_calls.is_empty() && resp.stop_reason == StopReason::EndTurn {
                break;
            }

            for tc in tool_calls {
                if cancel.is_cancelled() {
                    return Err(anyhow!("Agent execution cancelled"));
                }

                context.track_call_for_loop_detection(&tc.name, &tc.arguments);
                let repeat_count = context.repeated_call_count(&tc.name, &tc.arguments);
                if repeat_count >= 5 {
                    return Err(anyhow!(
                        "Loop detected: tool {} called 5+ times with the same arguments",
                        tc.name
                    ));
                }

                let start = Instant::now();
                let result = if repeat_count >= 3 {
                    Ok(format!(
                        "WARNING: You have called {} with the same arguments {} times. \
                         Stop repeating this call. If you are creating a new file, \
                         output your JSON response directly without further tool calls.",
                        tc.name, repeat_count
                    ))
                } else {
                    self.tools.execute(&tc.name, &tc.arguments, &cancel).await
                };
                let duration_ms = start.elapsed().as_millis() as u64;

                let (content, is_error, record_result) = match result {
                    Ok(text) => (text.clone(), false, Ok(text)),
                    Err(err) => {
                        let msg = err.to_string();
                        (msg.clone(), true, Err(msg))
                    }
                };
                let checkpointed = !is_error && content.trim_start().starts_with("CHECKPOINT:");

                context.record_tool_call(ToolCallRecord {
                    call_id: tc.id.clone(),
                    name: tc.name.clone(),
                    arguments: tc.arguments.clone(),
                    result: record_result,
                    duration_ms,
                });

                context.messages.push(Message {
                    role: Role::Tool,
                    content: vec![ContentBlock::ToolResult(ToolResult {
                        tool_use_id: tc.id,
                        content,
                        is_error,
                    })],
                });

                if checkpointed {
                    stop_after_tools = true;
                    break;
                }
            }

            if stop_after_tools {
                break;
            }

            context.increment_iteration();
        }

        Ok(AgentResponse {
            final_message,
            tool_calls: context.tool_calls,
            iterations: context.iteration,
            total_tokens,
            model: self.provider.model_name().to_string(),
        })
    }

    async fn call_llm(
        &self,
        messages: Vec<Message>,
        system: Option<String>,
    ) -> Result<crate::llm::types::CompletionResponse> {
        let fut =
            self.provider
                .complete(messages, self.tools.definitions(), self.max_tokens, system);
        match tokio::time::timeout(self.request_timeout, fut).await {
            Ok(r) => r,
            Err(_) => Err(anyhow!(
                "LLM request timed out after {:?}",
                self.request_timeout
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::types::{CompletionResponse, ToolCall, ToolDefinition, Usage};
    use crate::tools::registry::Tool;
    use async_trait::async_trait;
    use serde_json::Value;
    use std::sync::Mutex;

    struct MockProvider {
        responses: Mutex<Vec<CompletionResponse>>,
    }

    impl MockProvider {
        fn new(mut responses: Vec<CompletionResponse>) -> Self {
            responses.reverse();
            Self {
                responses: Mutex::new(responses),
            }
        }
    }

    #[async_trait]
    impl LlmProvider for MockProvider {
        async fn complete(
            &self,
            _messages: Vec<Message>,
            _tools: Vec<ToolDefinition>,
            _max_tokens: usize,
            _system: Option<String>,
        ) -> Result<CompletionResponse> {
            self.responses
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pop()
                .ok_or_else(|| anyhow!("No more mock responses"))
        }

        fn model_name(&self) -> &str {
            "mock"
        }
    }

    struct MockTool;

    #[async_trait]
    impl Tool for MockTool {
        fn definition(&self) -> ToolDefinition {
            ToolDefinition {
                name: "mock".into(),
                description: "Mock tool".into(),
                parameters: serde_json::json!({"type":"object"}),
            }
        }

        async fn execute(&self, _arguments: &Value, _cancel: &CancellationToken) -> Result<String> {
            Ok("ok".into())
        }
    }

    #[tokio::test]
    async fn agent_single_iteration_no_tools() {
        let provider = Arc::new(MockProvider::new(vec![CompletionResponse {
            content: vec![ContentBlock::Text("Done".into())],
            stop_reason: StopReason::EndTurn,
            usage: Usage {
                input_tokens: 1,
                output_tokens: 1,
            },
        }]));

        let tools = ToolRegistry::builder().build();
        let agent = Agent::new(
            provider,
            tools,
            "sys".into(),
            128,
            std::time::Duration::from_secs(5),
        );

        let ctx = AgentContext::new(
            vec![Message {
                role: Role::User,
                content: vec![ContentBlock::Text("hi".into())],
            }],
            5,
        );

        let resp = agent
            .execute(ctx, CancellationToken::new(), None)
            .await
            .expect("agent");
        assert_eq!(resp.final_message, "Done");
        assert_eq!(resp.iterations, 0);
        assert_eq!(resp.tool_calls.len(), 0);
    }

    #[tokio::test]
    async fn agent_tool_call_then_final() {
        let provider = Arc::new(MockProvider::new(vec![
            CompletionResponse {
                content: vec![
                    ContentBlock::Text("Working...".into()),
                    ContentBlock::ToolUse(ToolCall {
                        id: "call-1".into(),
                        name: "mock".into(),
                        arguments: serde_json::json!({}),
                    }),
                ],
                stop_reason: StopReason::ToolUse,
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                },
            },
            CompletionResponse {
                content: vec![ContentBlock::Text("Finished".into())],
                stop_reason: StopReason::EndTurn,
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                },
            },
        ]));

        let tools = ToolRegistry::builder().register(MockTool).build();
        let agent = Agent::new(
            provider,
            tools,
            "sys".into(),
            128,
            std::time::Duration::from_secs(5),
        );

        let ctx = AgentContext::new(
            vec![Message {
                role: Role::User,
                content: vec![ContentBlock::Text("do it".into())],
            }],
            5,
        );

        let resp = agent
            .execute(ctx, CancellationToken::new(), None)
            .await
            .expect("agent");
        assert_eq!(resp.final_message, "Finished");
        assert_eq!(resp.tool_calls.len(), 1);
        assert_eq!(resp.iterations, 1);
    }
}
