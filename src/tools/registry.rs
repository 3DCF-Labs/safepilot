use crate::llm::types::ToolDefinition;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait Tool: Send + Sync {
    fn definition(&self) -> ToolDefinition;

    async fn execute(&self, arguments: &Value, cancel: &CancellationToken) -> Result<String>;
}

#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: Arc<HashMap<String, Arc<dyn Tool>>>,
}

impl ToolRegistry {
    pub fn new(tools: HashMap<String, Arc<dyn Tool>>) -> Self {
        Self {
            tools: Arc::new(tools),
        }
    }

    pub fn builder() -> ToolRegistryBuilder {
        ToolRegistryBuilder::default()
    }

    pub fn definitions(&self) -> Vec<ToolDefinition> {
        self.tools.values().map(|t| t.definition()).collect()
    }

    pub async fn execute(
        &self,
        tool_name: &str,
        arguments: &Value,
        cancel: &CancellationToken,
    ) -> Result<String> {
        let tool = self
            .tools
            .get(tool_name)
            .ok_or_else(|| anyhow::anyhow!("Tool not found: {}", tool_name))?;
        tool.execute(arguments, cancel).await
    }
}

#[derive(Default)]
pub struct ToolRegistryBuilder {
    tools: HashMap<String, Arc<dyn Tool>>,
}

impl ToolRegistryBuilder {
    pub fn register<T: Tool + 'static>(&mut self, tool: T) -> &mut Self {
        let def = tool.definition();
        self.tools.insert(def.name.clone(), Arc::new(tool));
        self
    }

    pub fn build(&self) -> ToolRegistry {
        ToolRegistry::new(self.tools.clone())
    }
}
