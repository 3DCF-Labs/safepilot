#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

const GITHUB_API_URL: &str = "https://api.github.com";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Issue {
    pub number: i64,
    pub title: String,
    pub state: String,
    pub html_url: String,
    #[serde(default)]
    pub body: Option<String>,
    pub user: GitHubUser,
    pub created_at: String,
    #[serde(default)]
    pub labels: Vec<Label>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequest {
    pub number: i64,
    pub title: String,
    pub state: String,
    pub html_url: String,
    #[serde(default)]
    pub body: Option<String>,
    pub user: GitHubUser,
    pub head: GitRef,
    pub base: GitRef,
    #[serde(default)]
    pub draft: bool,
    #[serde(default)]
    pub mergeable: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitHubUser {
    pub login: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitRef {
    #[serde(rename = "ref")]
    pub ref_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Label {
    pub name: String,
    #[serde(default)]
    pub color: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Repository {
    pub full_name: String,
    pub html_url: String,
    pub description: Option<String>,
    pub stargazers_count: i64,
    pub open_issues_count: i64,
    pub default_branch: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowRun {
    pub id: i64,
    pub name: String,
    pub status: String,
    pub conclusion: Option<String>,
    pub html_url: String,
    pub head_branch: String,
}

#[derive(Debug, Deserialize)]
struct WorkflowRunsResponse {
    workflow_runs: Vec<WorkflowRun>,
}

pub struct GitHubClient {
    token: Option<String>,
    client: reqwest::Client,
}

impl GitHubClient {
    pub fn new(token: Option<&str>) -> Self {
        Self {
            token: token.map(|t| t.to_string()),
            client: reqwest::Client::new(),
        }
    }

    pub async fn get_repo(&self, owner: &str, repo: &str) -> Result<Repository> {
        let url = format!("{}/repos/{}/{}", GITHUB_API_URL, owner, repo);
        self.get(&url).await
    }

    pub async fn list_issues(
        &self,
        owner: &str,
        repo: &str,
        state: &str,
        limit: usize,
    ) -> Result<Vec<Issue>> {
        let url = format!(
            "{}/repos/{}/{}/issues?state={}&per_page={}",
            GITHUB_API_URL,
            owner,
            repo,
            state,
            limit.min(100)
        );
        self.get(&url).await
    }

    pub async fn create_issue(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        body: Option<&str>,
        labels: Option<Vec<String>>,
    ) -> Result<Issue> {
        let url = format!("{}/repos/{}/{}/issues", GITHUB_API_URL, owner, repo);

        let mut payload = serde_json::json!({
            "title": title,
        });

        if let Some(body) = body {
            payload["body"] = serde_json::Value::String(body.to_string());
        }
        if let Some(labels) = labels {
            payload["labels"] = serde_json::to_value(labels)?;
        }

        self.post(&url, &payload).await
    }

    pub async fn close_issue(&self, owner: &str, repo: &str, issue_number: i64) -> Result<Issue> {
        let url = format!(
            "{}/repos/{}/{}/issues/{}",
            GITHUB_API_URL, owner, repo, issue_number
        );
        let payload = serde_json::json!({ "state": "closed" });
        self.patch(&url, &payload).await
    }

    pub async fn add_comment(
        &self,
        owner: &str,
        repo: &str,
        issue_number: i64,
        body: &str,
    ) -> Result<serde_json::Value> {
        let url = format!(
            "{}/repos/{}/{}/issues/{}/comments",
            GITHUB_API_URL, owner, repo, issue_number
        );
        let payload = serde_json::json!({ "body": body });
        self.post(&url, &payload).await
    }

    pub async fn list_prs(
        &self,
        owner: &str,
        repo: &str,
        state: &str,
        limit: usize,
    ) -> Result<Vec<PullRequest>> {
        let url = format!(
            "{}/repos/{}/{}/pulls?state={}&per_page={}",
            GITHUB_API_URL,
            owner,
            repo,
            state,
            limit.min(100)
        );
        self.get(&url).await
    }

    pub async fn get_pr(&self, owner: &str, repo: &str, pr_number: i64) -> Result<PullRequest> {
        let url = format!(
            "{}/repos/{}/{}/pulls/{}",
            GITHUB_API_URL, owner, repo, pr_number
        );
        self.get(&url).await
    }

    pub async fn list_workflow_runs(
        &self,
        owner: &str,
        repo: &str,
        limit: usize,
    ) -> Result<Vec<WorkflowRun>> {
        let url = format!(
            "{}/repos/{}/{}/actions/runs?per_page={}",
            GITHUB_API_URL,
            owner,
            repo,
            limit.min(100)
        );
        let response: WorkflowRunsResponse = self.get(&url).await?;
        Ok(response.workflow_runs)
    }

    pub async fn search_issues(&self, query: &str, limit: usize) -> Result<Vec<Issue>> {
        let url = format!(
            "{}/search/issues?q={}&per_page={}",
            GITHUB_API_URL,
            urlencoding::encode(query),
            limit.min(100)
        );

        #[derive(Deserialize)]
        struct SearchResponse {
            items: Vec<Issue>,
        }

        let response: SearchResponse = self.get(&url).await?;
        Ok(response.items)
    }

    pub async fn trigger_workflow(
        &self,
        owner: &str,
        repo: &str,
        workflow_id: &str,
        ref_name: &str,
    ) -> Result<()> {
        let url = format!(
            "{}/repos/{}/{}/actions/workflows/{}/dispatches",
            GITHUB_API_URL, owner, repo, workflow_id
        );
        let payload = serde_json::json!({ "ref": ref_name });

        let mut req = self
            .client
            .post(&url)
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "safepilot")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .json(&payload);
        if let Some(token) = &self.token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        let response = req.send().await.context("Failed to trigger workflow")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("GitHub API error: {} - {}", status, body));
        }

        Ok(())
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T> {
        let mut req = self
            .client
            .get(url)
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "safepilot")
            .header("X-GitHub-Api-Version", "2022-11-28");
        if let Some(token) = &self.token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        let response = req.send().await.context("Failed to send GitHub request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("GitHub API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse GitHub response")
    }

    async fn post<T: for<'de> Deserialize<'de>>(
        &self,
        url: &str,
        payload: &serde_json::Value,
    ) -> Result<T> {
        let mut req = self
            .client
            .post(url)
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "safepilot")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .json(payload);
        if let Some(token) = &self.token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        let response = req.send().await.context("Failed to send GitHub request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("GitHub API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse GitHub response")
    }

    async fn patch<T: for<'de> Deserialize<'de>>(
        &self,
        url: &str,
        payload: &serde_json::Value,
    ) -> Result<T> {
        let mut req = self
            .client
            .patch(url)
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "safepilot")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .json(payload);
        if let Some(token) = &self.token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        let response = req.send().await.context("Failed to send GitHub request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("GitHub API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse GitHub response")
    }
}

pub fn format_issues(issues: &[Issue]) -> String {
    if issues.is_empty() {
        return "No issues found.".to_string();
    }

    issues
        .iter()
        .map(|i| {
            let labels = if i.labels.is_empty() {
                String::new()
            } else {
                format!(
                    " [{}]",
                    i.labels
                        .iter()
                        .map(|l| &l.name)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            format!("#{} {} {}{}", i.number, i.state, i.title, labels)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_prs(prs: &[PullRequest]) -> String {
    if prs.is_empty() {
        return "No pull requests found.".to_string();
    }

    prs.iter()
        .map(|pr| {
            let draft = if pr.draft { " (draft)" } else { "" };
            format!(
                "#{} {} {}{} ({}→{})",
                pr.number, pr.state, pr.title, draft, pr.head.ref_name, pr.base.ref_name
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_runs(runs: &[WorkflowRun]) -> String {
    if runs.is_empty() {
        return "No workflow runs found.".to_string();
    }

    runs.iter()
        .map(|r| {
            let conclusion = r.conclusion.as_deref().unwrap_or("in_progress");
            format!("{} {} [{}] {}", r.name, r.head_branch, r.status, conclusion)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_repo(repo: &Repository) -> String {
    let desc = repo.description.as_deref().unwrap_or("No description");
    format!(
        "{}\n{}\n{}\nStars: {} | Open issues: {} | Default branch: {}",
        repo.full_name,
        repo.html_url,
        desc,
        repo.stargazers_count,
        repo.open_issues_count,
        repo.default_branch
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_issues() {
        let issues = vec![Issue {
            number: 42,
            title: "Test issue".into(),
            state: "open".into(),
            html_url: "https://github.com/test/test/issues/42".into(),
            body: None,
            user: GitHubUser {
                login: "user".into(),
            },
            created_at: "2024-01-01".into(),
            labels: vec![],
        }];
        let formatted = format_issues(&issues);
        assert!(formatted.contains("#42"));
        assert!(formatted.contains("Test issue"));
    }
}
