#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct Issue {
    pub id: String,
    pub key: String,
    #[serde(rename = "self")]
    pub self_url: String,
    pub fields: IssueFields,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IssueFields {
    pub summary: String,
    #[serde(default)]
    pub description: Option<serde_json::Value>,
    pub status: Status,
    pub issuetype: IssueType,
    #[serde(default)]
    pub priority: Option<Priority>,
    #[serde(default)]
    pub assignee: Option<User>,
    #[serde(default)]
    pub reporter: Option<User>,
    pub project: ProjectRef,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default)]
    pub updated: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Status {
    pub name: String,
    #[serde(rename = "statusCategory")]
    pub status_category: StatusCategory,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StatusCategory {
    pub key: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IssueType {
    pub name: String,
    #[serde(default)]
    pub subtask: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Priority {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct User {
    #[serde(rename = "accountId")]
    pub account_id: String,
    #[serde(rename = "displayName")]
    pub display_name: String,
    #[serde(rename = "emailAddress", default)]
    pub email: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectRef {
    pub id: String,
    pub key: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Project {
    pub id: String,
    pub key: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "projectTypeKey")]
    pub project_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchResults {
    pub issues: Vec<Issue>,
    pub total: i64,
    #[serde(rename = "maxResults")]
    pub max_results: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateIssueInput {
    pub fields: CreateIssueFields,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateIssueFields {
    pub project: ProjectKey,
    pub summary: String,
    pub issuetype: IssueTypeName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<DocContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<PriorityName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectKey {
    pub key: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct IssueTypeName {
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PriorityName {
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DocContent {
    #[serde(rename = "type")]
    pub doc_type: String,
    pub version: i32,
    pub content: Vec<DocParagraph>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DocParagraph {
    #[serde(rename = "type")]
    pub para_type: String,
    pub content: Vec<DocText>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DocText {
    #[serde(rename = "type")]
    pub text_type: String,
    pub text: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreatedIssue {
    pub id: String,
    pub key: String,
    #[serde(rename = "self")]
    pub self_url: String,
}

pub struct JiraClient {
    base_url: String,
    agile_base_url: String,
    auth_header: String,
    client: reqwest::Client,
}

impl JiraClient {
    pub fn new(domain: &str, email: &str, api_token: &str) -> Self {
        let auth = format!("{}:{}", email, api_token);
        let auth_header = format!("Basic {}", STANDARD.encode(auth));
        let host = normalize_jira_domain(domain);

        Self {
            base_url: format!("https://{}/rest/api/3", host),
            agile_base_url: format!("https://{}/rest/agile/1.0", host),
            auth_header,
            client: reqwest::Client::new(),
        }
    }

    pub async fn search(&self, jql: &str, max_results: i32) -> Result<SearchResults> {
        let url = format!("{}/search", self.base_url);
        let body = serde_json::json!({
            "jql": jql,
            "maxResults": max_results,
            "fields": ["summary", "status", "issuetype", "priority", "assignee", "reporter", "project", "labels", "created", "updated", "description"]
        });

        self.post(&url, &body).await
    }

    pub async fn get_issue(&self, issue_key: &str) -> Result<Issue> {
        let url = format!("{}/issue/{}", self.base_url, issue_key);
        self.get(&url).await
    }

    pub async fn create_issue(
        &self,
        project_key: &str,
        issue_type: &str,
        summary: &str,
        description: Option<&str>,
    ) -> Result<CreatedIssue> {
        let url = format!("{}/issue", self.base_url);

        let mut fields = CreateIssueFields {
            project: ProjectKey {
                key: project_key.to_string(),
            },
            summary: summary.to_string(),
            issuetype: IssueTypeName {
                name: issue_type.to_string(),
            },
            description: None,
            priority: None,
            labels: None,
        };

        if let Some(desc) = description {
            fields.description = Some(text_to_adf(desc));
        }

        let input = CreateIssueInput { fields };
        self.post(&url, &input).await
    }

    pub async fn add_comment(&self, issue_key: &str, body: &str) -> Result<serde_json::Value> {
        let url = format!("{}/issue/{}/comment", self.base_url, issue_key);
        let payload = serde_json::json!({
            "body": text_to_adf(body)
        });
        self.post(&url, &payload).await
    }

    pub async fn transition_issue(&self, issue_key: &str, transition_id: &str) -> Result<()> {
        let url = format!("{}/issue/{}/transitions", self.base_url, issue_key);
        let payload = serde_json::json!({
            "transition": { "id": transition_id }
        });

        let response = self
            .client
            .post(&url)
            .header("Authorization", &self.auth_header)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("Failed to transition issue")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Jira API error: {} - {}", status, body));
        }

        Ok(())
    }

    pub async fn get_transitions(&self, issue_key: &str) -> Result<Vec<Transition>> {
        let url = format!("{}/issue/{}/transitions", self.base_url, issue_key);

        #[derive(Deserialize)]
        struct Response {
            transitions: Vec<Transition>,
        }

        let response: Response = self.get(&url).await?;
        Ok(response.transitions)
    }

    pub async fn list_projects(&self) -> Result<Vec<Project>> {
        let url = format!("{}/project", self.base_url);
        self.get(&url).await
    }

    pub async fn list_boards(&self, max_results: i32) -> Result<Vec<Board>> {
        let max_results = max_results.clamp(1, 100);
        let url = format!("{}/board?maxResults={}", self.agile_base_url, max_results);
        let response: BoardListResponse = self.get(&url).await?;
        Ok(response.values)
    }

    pub async fn assign_issue(&self, issue_key: &str, account_id: &str) -> Result<()> {
        let url = format!("{}/issue/{}/assignee", self.base_url, issue_key);
        let payload = serde_json::json!({
            "accountId": account_id
        });

        let response = self
            .client
            .put(&url)
            .header("Authorization", &self.auth_header)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("Failed to assign issue")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Jira API error: {} - {}", status, body));
        }

        Ok(())
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T> {
        let response = self
            .client
            .get(url)
            .header("Authorization", &self.auth_header)
            .header("Accept", "application/json")
            .send()
            .await
            .context("Failed to send Jira request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Jira API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse Jira response")
    }

    async fn post<T: for<'de> Deserialize<'de>, B: Serialize>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T> {
        let response = self
            .client
            .post(url)
            .header("Authorization", &self.auth_header)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .context("Failed to send Jira request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Jira API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse Jira response")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Transition {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
struct BoardListResponse {
    #[serde(default)]
    values: Vec<Board>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Board {
    pub id: i64,
    pub name: String,
    #[serde(rename = "type")]
    pub board_type: String,
}

pub fn normalize_jira_domain(domain: &str) -> String {
    let trimmed = domain.trim().trim_end_matches('/');
    let no_scheme = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))
        .unwrap_or(trimmed);
    let host = no_scheme.split('/').next().unwrap_or(no_scheme).trim();
    if host.ends_with(".atlassian.net") {
        host.to_string()
    } else {
        format!("{}.atlassian.net", host)
    }
}

pub fn text_to_adf(text: &str) -> DocContent {
    DocContent {
        doc_type: "doc".to_string(),
        version: 1,
        content: vec![DocParagraph {
            para_type: "paragraph".to_string(),
            content: vec![DocText {
                text_type: "text".to_string(),
                text: text.to_string(),
            }],
        }],
    }
}

pub fn format_issues(issues: &[Issue]) -> String {
    if issues.is_empty() {
        return "No issues found.".to_string();
    }

    issues
        .iter()
        .map(|i| {
            let priority = i
                .fields
                .priority
                .as_ref()
                .map(|p| format!(" [{}]", p.name))
                .unwrap_or_default();
            let assignee = i
                .fields
                .assignee
                .as_ref()
                .map(|a| format!(" @{}", a.display_name))
                .unwrap_or_default();
            format!(
                "{} [{}]{}{} {}",
                i.key, i.fields.status.name, priority, assignee, i.fields.summary
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_projects(projects: &[Project]) -> String {
    projects
        .iter()
        .map(|p| format!("{} ({})", p.name, p.key))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn format_boards(boards: &[Board]) -> String {
    if boards.is_empty() {
        return "No boards found.".to_string();
    }
    boards
        .iter()
        .map(|b| format!("{}: {} ({})", b.id, b.name, b.board_type))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::normalize_jira_domain;

    #[test]
    fn normalize_jira_domain_accepts_subdomain() {
        assert_eq!(normalize_jira_domain("acme"), "acme.atlassian.net");
    }

    #[test]
    fn normalize_jira_domain_accepts_full_host() {
        assert_eq!(
            normalize_jira_domain("acme.atlassian.net"),
            "acme.atlassian.net"
        );
    }

    #[test]
    fn normalize_jira_domain_accepts_full_url() {
        assert_eq!(
            normalize_jira_domain("https://acme.atlassian.net"),
            "acme.atlassian.net"
        );
    }
}
