use crate::secrets::SecretSpec;
use crate::tools::registry::Tool;
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct SearchTool {
    api_key: SecretSpec,
    crypto: Option<Arc<crate::crypto::Crypto>>,
}

#[derive(Debug, Deserialize)]
struct Args {
    query: String,
    #[serde(default)]
    count: Option<usize>,
}

impl SearchTool {
    pub fn new(api_key: SecretSpec, crypto: Option<Arc<crate::crypto::Crypto>>) -> Self {
        Self { api_key, crypto }
    }
}

#[async_trait]
impl Tool for SearchTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        crate::llm::types::ToolDefinition {
            name: "search".into(),
            description: "Search the web using Brave Search API.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Search query" },
                    "count": { "type": "integer", "minimum": 1, "maximum": 20, "description": "Number of results (1-20)" }
                },
                "required": ["query"]
            }),
        }
    }

    async fn execute(&self, arguments: &Value, cancel: &CancellationToken) -> Result<String> {
        if cancel.is_cancelled() {
            anyhow::bail!("Cancelled");
        }

        let args: Args = serde_json::from_value(arguments.clone())?;
        let key = self.api_key.load_with_crypto(self.crypto.as_deref())?;
        let results =
            crate::tools::search::web_search(&args.query, &key, args.count.unwrap_or(5)).await?;
        Ok(crate::tools::search::format_results(&results))
    }
}
