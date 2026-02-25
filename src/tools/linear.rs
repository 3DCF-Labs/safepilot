#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

const LINEAR_API_URL: &str = "https://api.linear.app/graphql";

#[derive(Debug, Clone, Deserialize)]
pub struct Issue {
    pub id: String,
    pub identifier: String,
    pub title: String,
    pub description: Option<String>,
    pub url: String,
    pub state: IssueState,
    pub priority: i32,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    pub assignee: Option<User>,
    pub team: Team,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IssueState {
    pub name: String,
    #[serde(rename = "type")]
    pub state_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct User {
    pub id: String,
    pub name: String,
    pub email: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Team {
    pub id: String,
    pub name: String,
    pub key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub url: String,
    pub state: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateIssueInput {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "teamId")]
    pub team_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "assigneeId")]
    pub assignee_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "projectId")]
    pub project_id: Option<String>,
}

pub struct LinearClient {
    token: String,
    client: reqwest::Client,
}

impl LinearClient {
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn list_teams(&self) -> Result<Vec<Team>> {
        let query = r#"
            query {
                teams {
                    nodes {
                        id
                        name
                        key
                    }
                }
            }
        "#;

        #[derive(Deserialize)]
        struct Response {
            teams: Nodes<Team>,
        }

        let response: Response = self.graphql(query, None).await?;
        Ok(response.teams.nodes)
    }

    pub async fn get_team_by_key(&self, key: &str) -> Result<Team> {
        let teams = self.list_teams().await?;
        teams
            .into_iter()
            .find(|t| t.key.eq_ignore_ascii_case(key) || t.name.eq_ignore_ascii_case(key))
            .ok_or_else(|| anyhow!("Team not found: {}", key))
    }

    pub async fn list_issues(&self, team_id: &str, limit: usize) -> Result<Vec<Issue>> {
        let query = r#"
            query($teamId: String!, $first: Int!) {
                team(id: $teamId) {
                    issues(first: $first, orderBy: updatedAt) {
                        nodes {
                            id
                            identifier
                            title
                            description
                            url
                            priority
                            createdAt
                            state {
                                name
                                type
                            }
                            assignee {
                                id
                                name
                                email
                            }
                            team {
                                id
                                name
                                key
                            }
                        }
                    }
                }
            }
        "#;

        let variables = serde_json::json!({
            "teamId": team_id,
            "first": limit.min(50)
        });

        #[derive(Deserialize)]
        struct Response {
            team: TeamIssues,
        }
        #[derive(Deserialize)]
        struct TeamIssues {
            issues: Nodes<Issue>,
        }

        let response: Response = self.graphql(query, Some(variables)).await?;
        Ok(response.team.issues.nodes)
    }

    pub async fn search_issues(&self, query_str: &str, limit: usize) -> Result<Vec<Issue>> {
        let query = r#"
            query($query: String!, $first: Int!) {
                issueSearch(query: $query, first: $first) {
                    nodes {
                        id
                        identifier
                        title
                        description
                        url
                        priority
                        createdAt
                        state {
                            name
                            type
                        }
                        assignee {
                            id
                            name
                            email
                        }
                        team {
                            id
                            name
                            key
                        }
                    }
                }
            }
        "#;

        let variables = serde_json::json!({
            "query": query_str,
            "first": limit.min(50)
        });

        #[derive(Deserialize)]
        struct Response {
            #[serde(rename = "issueSearch")]
            issue_search: Nodes<Issue>,
        }

        let response: Response = self.graphql(query, Some(variables)).await?;
        Ok(response.issue_search.nodes)
    }

    pub async fn create_issue(&self, input: CreateIssueInput) -> Result<Issue> {
        let query = r#"
            mutation($input: IssueCreateInput!) {
                issueCreate(input: $input) {
                    success
                    issue {
                        id
                        identifier
                        title
                        description
                        url
                        priority
                        createdAt
                        state {
                            name
                            type
                        }
                        assignee {
                            id
                            name
                            email
                        }
                        team {
                            id
                            name
                            key
                        }
                    }
                }
            }
        "#;

        let variables = serde_json::json!({
            "input": input
        });

        #[derive(Deserialize)]
        struct Response {
            #[serde(rename = "issueCreate")]
            issue_create: IssueCreateResponse,
        }
        #[derive(Deserialize)]
        struct IssueCreateResponse {
            success: bool,
            issue: Option<Issue>,
        }

        let response: Response = self.graphql(query, Some(variables)).await?;

        if !response.issue_create.success {
            return Err(anyhow!("Failed to create issue"));
        }

        response
            .issue_create
            .issue
            .ok_or_else(|| anyhow!("No issue returned"))
    }

    pub async fn update_issue_state(&self, issue_id: &str, state_id: &str) -> Result<Issue> {
        let query = r#"
            mutation($issueId: String!, $stateId: String!) {
                issueUpdate(id: $issueId, input: { stateId: $stateId }) {
                    success
                    issue {
                        id
                        identifier
                        title
                        description
                        url
                        priority
                        createdAt
                        state {
                            name
                            type
                        }
                        assignee {
                            id
                            name
                            email
                        }
                        team {
                            id
                            name
                            key
                        }
                    }
                }
            }
        "#;

        let variables = serde_json::json!({
            "issueId": issue_id,
            "stateId": state_id
        });

        #[derive(Deserialize)]
        struct Response {
            #[serde(rename = "issueUpdate")]
            issue_update: IssueUpdateResponse,
        }
        #[derive(Deserialize)]
        struct IssueUpdateResponse {
            success: bool,
            issue: Option<Issue>,
        }

        let response: Response = self.graphql(query, Some(variables)).await?;

        if !response.issue_update.success {
            return Err(anyhow!("Failed to update issue"));
        }

        response
            .issue_update
            .issue
            .ok_or_else(|| anyhow!("No issue returned"))
    }

    pub async fn add_comment(&self, issue_id: &str, body: &str) -> Result<()> {
        let query = r#"
            mutation($issueId: String!, $body: String!) {
                commentCreate(input: { issueId: $issueId, body: $body }) {
                    success
                }
            }
        "#;

        let variables = serde_json::json!({
            "issueId": issue_id,
            "body": body
        });

        #[derive(Deserialize)]
        struct Response {
            #[serde(rename = "commentCreate")]
            comment_create: SuccessResponse,
        }
        #[derive(Deserialize)]
        struct SuccessResponse {
            success: bool,
        }

        let response: Response = self.graphql(query, Some(variables)).await?;

        if !response.comment_create.success {
            return Err(anyhow!("Failed to add comment"));
        }

        Ok(())
    }

    pub async fn list_states(&self, team_id: &str) -> Result<Vec<IssueState>> {
        let query = r#"
            query($teamId: String!) {
                team(id: $teamId) {
                    states {
                        nodes {
                            id
                            name
                            type
                        }
                    }
                }
            }
        "#;

        let variables = serde_json::json!({
            "teamId": team_id
        });

        #[derive(Deserialize)]
        struct Response {
            team: TeamStates,
        }
        #[derive(Deserialize)]
        struct TeamStates {
            states: Nodes<StateWithId>,
        }
        #[derive(Deserialize)]
        struct StateWithId {
            id: String,
            name: String,
            #[serde(rename = "type")]
            state_type: String,
        }

        let response: Response = self.graphql(query, Some(variables)).await?;
        Ok(response
            .team
            .states
            .nodes
            .into_iter()
            .map(|s| IssueState {
                name: s.name,
                state_type: s.state_type,
            })
            .collect())
    }

    async fn graphql<T: for<'de> Deserialize<'de>>(
        &self,
        query: &str,
        variables: Option<serde_json::Value>,
    ) -> Result<T> {
        let body = serde_json::json!({
            "query": query,
            "variables": variables.unwrap_or(serde_json::json!({}))
        });

        let response = self
            .client
            .post(LINEAR_API_URL)
            .header("Authorization", &self.token)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .context("Failed to send Linear request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Linear API error: {} - {}", status, body));
        }

        #[derive(Deserialize)]
        struct GraphQLResponse<T> {
            data: Option<T>,
            errors: Option<Vec<GraphQLError>>,
        }

        #[derive(Deserialize)]
        struct GraphQLError {
            message: String,
        }

        let gql_response: GraphQLResponse<T> = response
            .json()
            .await
            .context("Failed to parse Linear response")?;

        if let Some(errors) = gql_response.errors {
            let messages: Vec<_> = errors.iter().map(|e| e.message.clone()).collect();
            return Err(anyhow!("Linear GraphQL errors: {}", messages.join(", ")));
        }

        gql_response
            .data
            .ok_or_else(|| anyhow!("No data in Linear response"))
    }
}

#[derive(Deserialize)]
struct Nodes<T> {
    nodes: Vec<T>,
}

pub fn format_issues(issues: &[Issue]) -> String {
    if issues.is_empty() {
        return "No issues found.".to_string();
    }

    issues
        .iter()
        .map(|i| {
            let assignee = i
                .assignee
                .as_ref()
                .map(|a| format!(" @{}", a.name))
                .unwrap_or_default();
            let priority = match i.priority {
                0 => "",
                1 => " 🔴",
                2 => " 🟠",
                3 => " 🟡",
                _ => " ⚪",
            };
            format!(
                "{} [{}]{}{} {}",
                i.identifier, i.state.name, priority, assignee, i.title
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_teams(teams: &[Team]) -> String {
    teams
        .iter()
        .map(|t| format!("{} ({})", t.name, t.key))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_issues() {
        let issues = vec![Issue {
            id: "123".into(),
            identifier: "ENG-42".into(),
            title: "Test issue".into(),
            description: None,
            url: "https://linear.app/test".into(),
            state: IssueState {
                name: "In Progress".into(),
                state_type: "started".into(),
            },
            priority: 2,
            created_at: "2024-01-01".into(),
            assignee: None,
            team: Team {
                id: "t1".into(),
                name: "Engineering".into(),
                key: "ENG".into(),
            },
        }];
        let formatted = format_issues(&issues);
        assert!(formatted.contains("ENG-42"));
        assert!(formatted.contains("In Progress"));
    }
}
