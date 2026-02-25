use crate::secrets::SecretSpec;
use crate::tools::github::{GitHubClient, Issue, PullRequest, WorkflowRun};
use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct GitHubTool {
    read_token: Option<SecretSpec>,
    write_token: Option<SecretSpec>,
    read_token_override: Option<String>,
    write_token_override: Option<String>,
    default_repo: Option<String>,
    write_enabled: bool,
    crypto: Option<Arc<crate::crypto::Crypto>>,
}

#[derive(Debug, Deserialize)]
struct Args {
    action: String,
    #[serde(default)]
    repo: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    issue_number: Option<i64>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    labels: Option<Vec<String>>,
}

impl GitHubTool {
    pub fn new(
        read_token: Option<SecretSpec>,
        write_token: Option<SecretSpec>,
        default_repo: Option<String>,
        write_enabled: bool,
        crypto: Option<Arc<crate::crypto::Crypto>>,
    ) -> Self {
        Self {
            read_token,
            write_token,
            read_token_override: None,
            write_token_override: None,
            default_repo,
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

    fn repo_or_default<'a>(&'a self, maybe: &'a Option<String>) -> &'a str {
        if let Some(r) = maybe.as_deref() {
            return r;
        }
        if let Some(r) = self.default_repo.as_deref() {
            return r;
        }
        ""
    }

    fn parse_owner_repo(repo: &str) -> Result<(&str, &str)> {
        let (owner, name) = repo
            .split_once('/')
            .ok_or_else(|| anyhow!("repo must be in owner/repo format (got: {})", repo))?;
        if owner.trim().is_empty() || name.trim().is_empty() {
            return Err(anyhow!("repo must be in owner/repo format (got: {})", repo));
        }
        Ok((owner, name))
    }

    fn format_issues(items: &[Issue]) -> String {
        if items.is_empty() {
            return "No issues found.".into();
        }
        items
            .iter()
            .map(|i| format!("#{} [{}] {} ({})", i.number, i.state, i.title, i.html_url))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn format_prs(items: &[PullRequest]) -> String {
        if items.is_empty() {
            return "No pull requests found.".into();
        }
        items
            .iter()
            .map(|p| format!("#{} [{}] {} ({})", p.number, p.state, p.title, p.html_url))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn format_runs(items: &[WorkflowRun]) -> String {
        if items.is_empty() {
            return "No workflow runs found.".into();
        }
        items
            .iter()
            .map(|r| {
                let conc = r.conclusion.as_deref().unwrap_or("-");
                format!("{}: {} / {} ({})", r.name, r.status, conc, r.html_url)
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[async_trait]
impl Tool for GitHubTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        let actions = if self.write_enabled && self.write_token.is_some() {
            vec![
                "get_repo",
                "list_issues",
                "search_issues",
                "list_prs",
                "list_ci",
                "create_issue",
                "comment",
            ]
        } else {
            vec![
                "get_repo",
                "list_issues",
                "search_issues",
                "list_prs",
                "list_ci",
            ]
        };

        crate::llm::types::ToolDefinition {
            name: "github".into(),
            description:
                "GitHub operations: get repo info, list/search issues/PRs, create issues, comment, list CI runs."
                    .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": actions,
                        "description": "Operation to perform"
                    },
                    "repo": { "type": "string", "description": "owner/repo (optional; defaults to DEFAULT_REPO if configured)" },
                    "query": { "type": "string", "description": "Search query (for search_issues)"},
                    "state": { "type": "string", "description": "open|closed|all (list_issues/list_prs)", "default": "open" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "description": "Result limit (1-100)" },
                    "issue_number": { "type": "integer", "description": "Issue or PR number (for comment)" },
                    "title": { "type": "string", "description": "Issue title (create_issue)" },
                    "body": { "type": "string", "description": "Issue body or comment text (create_issue/comment)" },
                    "labels": { "type": "array", "items": { "type": "string" }, "description": "Issue labels (create_issue)" }
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
        let is_write = matches!(args.action.as_str(), "create_issue" | "comment");
        let token = if is_write {
            if !self.write_enabled {
                return Err(anyhow!("GitHub write actions are disabled in agent mode"));
            }
            if let Some(v) = self.write_token_override.as_ref() {
                Some(v.clone())
            } else {
                Some(
                    self.write_token
                        .as_ref()
                        .ok_or_else(|| anyhow!("GITHUB_TOKEN (write) not configured"))?
                        .load_with_crypto(self.crypto.as_deref())?,
                )
            }
        } else if let Some(v) = self.read_token_override.as_ref() {
            Some(v.clone())
        } else {
            self.read_token
                .as_ref()
                .map(|spec| spec.load_with_crypto(self.crypto.as_deref()))
                .transpose()?
        };
        let client = GitHubClient::new(token.as_deref());

        match args.action.as_str() {
            "get_repo" => {
                let repo = self.repo_or_default(&args.repo);
                let (owner, name) = Self::parse_owner_repo(repo)?;
                let info = client.get_repo(owner, name).await?;
                Ok(crate::tools::github::format_repo(&info))
            }
            "list_issues" => {
                let repo = self.repo_or_default(&args.repo);
                let (owner, name) = Self::parse_owner_repo(repo)?;
                let state = args.state.as_deref().unwrap_or("open");
                let limit = args.limit.unwrap_or(20);
                let issues = client.list_issues(owner, name, state, limit).await?;
                Ok(Self::format_issues(&issues))
            }
            "search_issues" => {
                let query = args
                    .query
                    .as_deref()
                    .ok_or_else(|| anyhow!("query is required for search_issues"))?;
                let limit = args.limit.unwrap_or(20);
                let items = client.search_issues(query, limit).await?;
                Ok(Self::format_issues(&items))
            }
            "list_prs" => {
                let repo = self.repo_or_default(&args.repo);
                let (owner, name) = Self::parse_owner_repo(repo)?;
                let state = args.state.as_deref().unwrap_or("open");
                let limit = args.limit.unwrap_or(20);
                let prs = client.list_prs(owner, name, state, limit).await?;
                Ok(Self::format_prs(&prs))
            }
            "list_ci" => {
                let repo = self.repo_or_default(&args.repo);
                let (owner, name) = Self::parse_owner_repo(repo)?;
                let limit = args.limit.unwrap_or(10);
                let runs = client.list_workflow_runs(owner, name, limit).await?;
                Ok(Self::format_runs(&runs))
            }
            "create_issue" => {
                let repo = self.repo_or_default(&args.repo);
                let (owner, name) = Self::parse_owner_repo(repo)?;
                let title = args
                    .title
                    .as_deref()
                    .ok_or_else(|| anyhow!("title is required for create_issue"))?;
                let issue = client
                    .create_issue(owner, name, title, args.body.as_deref(), args.labels)
                    .await?;
                Ok(format!(
                    "Created issue #{}: {}",
                    issue.number, issue.html_url
                ))
            }
            "comment" => {
                let repo = self.repo_or_default(&args.repo);
                let (owner, name) = Self::parse_owner_repo(repo)?;
                let issue_number = args
                    .issue_number
                    .ok_or_else(|| anyhow!("issue_number is required for comment"))?;
                let body = args
                    .body
                    .as_deref()
                    .ok_or_else(|| anyhow!("body is required for comment"))?;
                let _ = client.add_comment(owner, name, issue_number, body).await?;
                Ok(format!("Comment added to #{}.", issue_number))
            }
            other => Err(anyhow!("Unknown github.action: {}", other)),
        }
    }
}
