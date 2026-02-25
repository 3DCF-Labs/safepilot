use crate::secrets::SecretSpec;
use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct TodoistTool {
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
    project_id: Option<String>,
    #[serde(default)]
    task: Option<String>,
    #[serde(default)]
    due: Option<String>,
    #[serde(default)]
    task_id: Option<String>,
}

impl TodoistTool {
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
impl Tool for TodoistTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        let actions = if self.write_enabled && self.write_token.is_some() {
            vec!["list", "add", "complete", "projects"]
        } else {
            vec!["list", "projects"]
        };

        crate::llm::types::ToolDefinition {
            name: "todoist".into(),
            description: "Todoist operations: list tasks, add task, complete task, list projects."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": actions, "description": "Operation" },
                    "project_id": { "type": "string", "description": "Project ID (optional for list)" },
                    "task": { "type": "string", "description": "Task content (add)" },
                    "due": { "type": "string", "description": "Due string (add), e.g. 'tomorrow 10am'" },
                    "task_id": { "type": "string", "description": "Task ID (complete)" }
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
        let is_write = matches!(args.action.as_str(), "add" | "complete");
        let token = if is_write {
            if !self.write_enabled {
                return Err(anyhow!("Todoist write actions are disabled in agent mode"));
            }
            if let Some(v) = self.write_token_override.as_ref() {
                v.clone()
            } else {
                self.write_token
                    .as_ref()
                    .ok_or_else(|| anyhow!("TODOIST_API_KEY (write) not configured"))?
                    .load_with_crypto(self.crypto.as_deref())?
            }
        } else if let Some(v) = self.read_token_override.as_ref() {
            v.clone()
        } else {
            self.read_token.load_with_crypto(self.crypto.as_deref())?
        };
        let client = crate::tools::todoist::TodoistClient::new(&token);

        match args.action.as_str() {
            "list" => {
                let tasks = client.list_tasks(args.project_id.as_deref()).await?;
                Ok(crate::tools::todoist::format_tasks(&tasks))
            }
            "projects" => {
                let projects = client.list_projects().await?;
                Ok(crate::tools::todoist::format_projects(&projects))
            }
            "add" => {
                let task = args
                    .task
                    .as_deref()
                    .ok_or_else(|| anyhow!("task is required for todoist.add"))?;
                let input = crate::tools::todoist::CreateTaskInput {
                    content: task.to_string(),
                    description: None,
                    project_id: args.project_id,
                    due_string: args.due,
                    priority: None,
                    labels: None,
                };
                let created = client.create_task(input).await?;
                Ok(format!(
                    "Created task: {} ({})",
                    created.content, created.url
                ))
            }
            "complete" => {
                let task_id = args
                    .task_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("task_id is required for todoist.complete"))?;
                client.complete_task(task_id).await?;
                Ok("Task completed.".into())
            }
            other => Err(anyhow!("Unknown todoist.action: {}", other)),
        }
    }
}
