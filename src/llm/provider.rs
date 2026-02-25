use crate::llm::types::{CompletionResponse, Message, ToolDefinition};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn complete(
        &self,
        messages: Vec<Message>,
        tools: Vec<ToolDefinition>,
        max_tokens: usize,
        system: Option<String>,
    ) -> Result<CompletionResponse>;

    fn model_name(&self) -> &str;
}
