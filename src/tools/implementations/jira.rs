use crate::secrets::SecretSpec;
use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct JiraTool {
    domain: String,
    email: String,
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
    jql: Option<String>,
    #[serde(default)]
    issue_key: Option<String>,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    issue_type: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    max_results: Option<i32>,
}

impl JiraTool {
    pub fn new(
        domain: String,
        email: String,
        read_token: SecretSpec,
        write_token: Option<SecretSpec>,
        write_enabled: bool,
        crypto: Option<Arc<crate::crypto::Crypto>>,
    ) -> Self {
        Self {
            domain,
            email,
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
impl Tool for JiraTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        let actions = if self.write_enabled && self.write_token.is_some() {
            vec!["search", "get", "create", "comment", "projects", "boards"]
        } else {
            vec!["search", "get", "projects", "boards"]
        };

        crate::llm::types::ToolDefinition {
            name: "jira".into(),
            description:
                "Jira operations: search, get issue, create issue, comment, list projects/boards."
                    .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": actions, "description": "Operation" },
                    "jql": { "type": "string", "description": "JQL query (search)" },
                    "issue_key": { "type": "string", "description": "Issue key like ABC-123 (get/comment)" },
                    "project": { "type": "string", "description": "Project key (create)" },
                    "issue_type": { "type": "string", "description": "Issue type name (create), e.g. 'Task'" },
                    "summary": { "type": "string", "description": "Issue summary (create)" },
                    "description": { "type": "string", "description": "Issue description (create)" },
                    "comment": { "type": "string", "description": "Comment text (comment)" },
                    "max_results": { "type": "integer", "minimum": 1, "maximum": 100, "description": "Max results (search/boards)" }
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
        let is_write = matches!(args.action.as_str(), "create" | "comment");
        let token = if is_write {
            if !self.write_enabled {
                return Err(anyhow!("Jira write actions are disabled in agent mode"));
            }
            if let Some(v) = self.write_token_override.as_ref() {
                v.clone()
            } else {
                self.write_token
                    .as_ref()
                    .ok_or_else(|| anyhow!("JIRA_API_TOKEN (write) not configured"))?
                    .load_with_crypto(self.crypto.as_deref())?
            }
        } else if let Some(v) = self.read_token_override.as_ref() {
            v.clone()
        } else {
            self.read_token.load_with_crypto(self.crypto.as_deref())?
        };
        let client = crate::tools::jira::JiraClient::new(&self.domain, &self.email, &token);

        match args.action.as_str() {
            "projects" => {
                let projects = client.list_projects().await?;
                if projects.is_empty() {
                    return Ok("No projects found.".into());
                }
                Ok(projects
                    .into_iter()
                    .map(|p| format!("{}: {}", p.key, p.name))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "boards" => {
                let boards = client.list_boards(args.max_results.unwrap_or(20)).await?;
                Ok(crate::tools::jira::format_boards(&boards))
            }
            "search" => {
                let jql = args
                    .jql
                    .as_deref()
                    .ok_or_else(|| anyhow!("jql is required for jira.search"))?;
                let max_results = args.max_results.unwrap_or(20).clamp(1, 100);
                let results = client.search(jql, max_results).await?;
                if results.issues.is_empty() {
                    return Ok("No issues found.".into());
                }
                Ok(results
                    .issues
                    .into_iter()
                    .map(|i| format!("{}: {}", i.key, i.fields.summary))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "get" => {
                let key = args
                    .issue_key
                    .as_deref()
                    .ok_or_else(|| anyhow!("issue_key is required for jira.get"))?;
                let issue = client.get_issue(key).await?;
                Ok(format!(
                    "{}: {} [{}]",
                    issue.key, issue.fields.summary, issue.fields.status.name
                ))
            }
            "create" => {
                let project = args
                    .project
                    .as_deref()
                    .ok_or_else(|| anyhow!("project is required for jira.create"))?;
                let issue_type = args
                    .issue_type
                    .as_deref()
                    .ok_or_else(|| anyhow!("issue_type is required for jira.create"))?;
                let summary = args
                    .summary
                    .as_deref()
                    .ok_or_else(|| anyhow!("summary is required for jira.create"))?;

                let created = client
                    .create_issue(project, issue_type, summary, args.description.as_deref())
                    .await?;
                Ok(format!("Created issue: {}", created.key))
            }
            "comment" => {
                let key = args
                    .issue_key
                    .as_deref()
                    .ok_or_else(|| anyhow!("issue_key is required for jira.comment"))?;
                let text = args
                    .comment
                    .as_deref()
                    .ok_or_else(|| anyhow!("comment is required for jira.comment"))?;
                let _ = client.add_comment(key, text).await?;
                Ok("Comment added.".into())
            }
            other => Err(anyhow!("Unknown jira.action: {}", other)),
        }
    }
}
