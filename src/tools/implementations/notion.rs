use crate::secrets::SecretSpec;
use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct NotionTool {
    read_token: SecretSpec,
    write_token: Option<SecretSpec>,
    read_token_override: Option<String>,
    write_token_override: Option<String>,
    write_enabled: bool,
    crypto: Option<Arc<crate::crypto::Crypto>>,
}

#[derive(Debug, Deserialize)]
struct Args {
    action: String,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    object: Option<String>,
    #[serde(default)]
    database_id: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    page_id: Option<String>,
}

impl NotionTool {
    pub fn new(
        read_token: SecretSpec,
        write_token: Option<SecretSpec>,
        write_enabled: bool,
        crypto: Option<Arc<crate::crypto::Crypto>>,
    ) -> Self {
        Self {
            read_token,
            write_token,
            read_token_override: None,
            write_token_override: None,
            write_enabled,
            crypto,
        }
    }

    pub fn with_token_overrides(
        mut self,
        read_token_override: Option<String>,
        write_token_override: Option<String>,
    ) -> Self {
        self.read_token_override = read_token_override;
        self.write_token_override = write_token_override;
        self
    }
}

#[async_trait]
impl Tool for NotionTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        let actions = if self.write_enabled && self.write_token.is_some() {
            vec!["search", "query_database", "create_page", "get_page"]
        } else {
            vec!["search", "query_database", "get_page"]
        };

        crate::llm::types::ToolDefinition {
            name: "notion".into(),
            description:
                "Notion operations: search pages/databases, query a database, create a simple page."
                    .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": actions, "description": "Operation" },
                    "query": { "type": "string", "description": "Search query (search)" },
                    "object": { "type": "string", "enum": ["page","database"], "description": "Optional filter for search" },
                    "database_id": { "type": "string", "description": "Database ID (query_database/create_page)" },
                    "title": { "type": "string", "description": "Title (create_page)" },
                    "page_id": { "type": "string", "description": "Page ID (get_page)" }
                },
                "required": ["action"]
            }),
        }
    }

    async fn execute(&self, arguments: &Value, cancel: &CancellationToken) -> Result<String> {
        if cancel.is_cancelled() {
            anyhow::bail!("Cancelled");
        }

        let args: Args = serde_json::from_value(arguments.clone())?;
        let is_write = args.action == "create_page";
        let token = if is_write {
            if !self.write_enabled {
                return Err(anyhow!("Notion write actions are disabled in agent mode"));
            }
            if let Some(v) = self.write_token_override.as_ref() {
                v.clone()
            } else {
                self.write_token
                    .as_ref()
                    .ok_or_else(|| anyhow!("NOTION_API_KEY (write) not configured"))?
                    .load_with_crypto(self.crypto.as_deref())?
            }
        } else if let Some(v) = self.read_token_override.as_ref() {
            v.clone()
        } else {
            self.read_token.load_with_crypto(self.crypto.as_deref())?
        };
        let client = crate::tools::notion::NotionClient::new(&token);

        match args.action.as_str() {
            "search" => {
                let query = args
                    .query
                    .as_deref()
                    .ok_or_else(|| anyhow!("query is required for notion.search"))?;
                let results = client.search(query, args.object.as_deref()).await?;
                Ok(serde_json::to_string_pretty(&results)?)
            }
            "query_database" => {
                let database_id = args
                    .database_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("database_id is required for notion.query_database"))?;
                let pages = client.query_database(database_id, None, 20).await?;
                Ok(format!("Found {} pages.", pages.len()))
            }
            "create_page" => {
                let database_id = args
                    .database_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("database_id is required for notion.create_page"))?;
                let title = args
                    .title
                    .as_deref()
                    .ok_or_else(|| anyhow!("title is required for notion.create_page"))?;
                let mut props = HashMap::new();
                props.insert(
                    "Name".to_string(),
                    crate::tools::notion::title_property(title),
                );
                let page = client.create_page(database_id, props).await?;
                Ok(format!("Created page: {}", page.url))
            }
            "get_page" => {
                let page_id = args
                    .page_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("page_id is required for notion.get_page"))?;
                let page = client.get_page(page_id).await?;
                Ok(format!("Page: {}", page.url))
            }
            other => Err(anyhow!("Unknown notion.action: {}", other)),
        }
    }
}
