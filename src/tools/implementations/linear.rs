use crate::secrets::SecretSpec;
use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct LinearTool {
    read_api_key: SecretSpec,
    write_api_key: Option<SecretSpec>,
    read_api_key_override: Option<String>,
    write_api_key_override: Option<String>,
    write_enabled: bool,
    crypto: Option<Arc<crate::crypto::Crypto>>,
}

#[derive(Debug, Deserialize)]
struct Args {
    action: String,
    #[serde(default)]
    team: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

impl LinearTool {
    pub fn new(
        read_api_key: SecretSpec,
        write_api_key: Option<SecretSpec>,
        write_enabled: bool,
        crypto: Option<Arc<crate::crypto::Crypto>>,
    ) -> Self {
        Self {
            read_api_key,
            write_api_key,
            read_api_key_override: None,
            write_api_key_override: None,
            write_enabled,
            crypto,
        }
    }

    pub fn with_token_overrides(
        mut self,
        read_api_key_override: Option<String>,
        write_api_key_override: Option<String>,
    ) -> Self {
        self.read_api_key_override = read_api_key_override;
        self.write_api_key_override = write_api_key_override;
        self
    }
}

#[async_trait]
impl Tool for LinearTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        let actions = if self.write_enabled && self.write_api_key.is_some() {
            vec!["teams", "issues", "search", "create_issue"]
        } else {
            vec!["teams", "issues", "search"]
        };

        crate::llm::types::ToolDefinition {
            name: "linear".into(),
            description: "Linear operations: list teams, list issues, search issues, create issue."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": actions, "description": "Operation" },
                    "team": { "type": "string", "description": "Team key or name (issues/create_issue)" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "description": "Limit (issues/search)" },
                    "query": { "type": "string", "description": "Search query (search)" },
                    "title": { "type": "string", "description": "Issue title (create_issue)" },
                    "description": { "type": "string", "description": "Issue description (create_issue)" }
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
        let is_write = args.action == "create_issue";
        let api_key = if is_write {
            if !self.write_enabled {
                return Err(anyhow!("Linear write actions are disabled in agent mode"));
            }
            if let Some(v) = self.write_api_key_override.as_ref() {
                v.clone()
            } else {
                self.write_api_key
                    .as_ref()
                    .ok_or_else(|| anyhow!("LINEAR_API_KEY (write) not configured"))?
                    .load_with_crypto(self.crypto.as_deref())?
            }
        } else if let Some(v) = self.read_api_key_override.as_ref() {
            v.clone()
        } else {
            self.read_api_key.load_with_crypto(self.crypto.as_deref())?
        };
        let client = crate::tools::linear::LinearClient::new(&api_key);

        match args.action.as_str() {
            "teams" => {
                let teams = client.list_teams().await?;
                Ok(teams
                    .into_iter()
                    .map(|t| format!("{} ({})", t.name, t.key))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "issues" => {
                let team_key = args
                    .team
                    .as_deref()
                    .ok_or_else(|| anyhow!("team is required for linear.issues"))?;
                let team = client.get_team_by_key(team_key).await?;
                let issues = client
                    .list_issues(&team.id, args.limit.unwrap_or(20))
                    .await?;
                Ok(issues
                    .into_iter()
                    .map(|i| format!("{} {} ({})", i.identifier, i.title, i.url))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "search" => {
                let query = args
                    .query
                    .as_deref()
                    .ok_or_else(|| anyhow!("query is required for linear.search"))?;
                let issues = client
                    .search_issues(query, args.limit.unwrap_or(20))
                    .await?;
                Ok(issues
                    .into_iter()
                    .map(|i| format!("{} {} ({})", i.identifier, i.title, i.url))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "create_issue" => {
                let team_key = args
                    .team
                    .as_deref()
                    .ok_or_else(|| anyhow!("team is required for linear.create_issue"))?;
                let title = args
                    .title
                    .as_deref()
                    .ok_or_else(|| anyhow!("title is required for linear.create_issue"))?;
                let team = client.get_team_by_key(team_key).await?;
                let created = client
                    .create_issue(crate::tools::linear::CreateIssueInput {
                        title: title.to_string(),
                        description: args.description,
                        team_id: team.id,
                        priority: None,
                        assignee_id: None,
                        project_id: None,
                    })
                    .await?;
                Ok(format!(
                    "Created issue {} ({})",
                    created.identifier, created.url
                ))
            }
            other => Err(anyhow!("Unknown linear.action: {}", other)),
        }
    }
}
