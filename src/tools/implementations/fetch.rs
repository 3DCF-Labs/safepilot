use crate::tools::registry::Tool;
use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

pub struct FetchTool {
    allow_private: bool,
}

#[derive(Debug, Deserialize)]
struct Args {
    url: String,
}

impl FetchTool {
    pub fn new(allow_private: bool) -> Self {
        Self { allow_private }
    }
}

#[async_trait]
impl Tool for FetchTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        crate::llm::types::ToolDefinition {
            name: "fetch".into(),
            description: "Fetch a URL and return its text content (HTTP/HTTPS only). HTML pages are converted to readable text automatically. For JS-rendered pages, use the browser tool instead.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "URL to fetch (http/https)" }
                },
                "required": ["url"]
            }),
        }
    }

    async fn execute(&self, arguments: &Value, cancel: &CancellationToken) -> Result<String> {
        if cancel.is_cancelled() {
            anyhow::bail!("Cancelled");
        }

        let args: Args = serde_json::from_value(arguments.clone())?;
        crate::tools::search::web_fetch(&args.url, self.allow_private).await
    }
}
