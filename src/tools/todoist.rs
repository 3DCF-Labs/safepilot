#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const TODOIST_API_URL: &str = "https://api.todoist.com/api/v1";

#[derive(Debug, Clone, Deserialize)]
pub struct Task {
    pub id: String,
    pub content: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub project_id: String,
    #[serde(default)]
    pub is_completed: bool,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub due: Option<Due>,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Due {
    #[serde(default)]
    pub date: String,
    #[serde(default)]
    pub datetime: Option<String>,
    #[serde(default)]
    pub string: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub color: Option<String>,
    #[serde(default)]
    pub is_favorite: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateTaskInput {
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_string: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
}

pub struct TodoistClient {
    token: String,
    client: reqwest::Client,
}

impl TodoistClient {
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn list_tasks(&self, project_id: Option<&str>) -> Result<Vec<Task>> {
        let url = format!("{}/tasks", TODOIST_API_URL);
        let mut params = Vec::new();
        if let Some(pid) = project_id {
            params.push(("project_id".to_string(), pid.to_string()));
        }
        self.get_paginated(&url, params).await
    }

    pub async fn get_task(&self, task_id: &str) -> Result<Task> {
        let url = format!("{}/tasks/{}", TODOIST_API_URL, task_id);
        self.get(&url).await
    }

    pub async fn create_task(&self, input: CreateTaskInput) -> Result<Task> {
        let url = format!("{}/tasks", TODOIST_API_URL);
        self.post(&url, &input).await
    }

    pub async fn complete_task(&self, task_id: &str) -> Result<()> {
        let url = format!("{}/tasks/{}/close", TODOIST_API_URL, task_id);

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await
            .context("Failed to complete task")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Todoist API error: {} - {}", status, body));
        }

        Ok(())
    }

    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        let url = format!("{}/tasks/{}", TODOIST_API_URL, task_id);

        let response = self
            .client
            .delete(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await
            .context("Failed to delete task")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Todoist API error: {} - {}", status, body));
        }

        Ok(())
    }

    pub async fn list_projects(&self) -> Result<Vec<Project>> {
        let url = format!("{}/projects", TODOIST_API_URL);
        self.get_paginated(&url, Vec::new()).await
    }

    pub async fn get_project(&self, project_id: &str) -> Result<Project> {
        let url = format!("{}/projects/{}", TODOIST_API_URL, project_id);
        self.get(&url).await
    }

    async fn get<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T> {
        let response = self
            .client
            .get(url)
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await
            .context("Failed to send Todoist request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Todoist API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse Todoist response")
    }

    async fn get_paginated<T: DeserializeOwned>(
        &self,
        url: &str,
        base_params: Vec<(String, String)>,
    ) -> Result<Vec<T>> {
        let mut all = Vec::new();
        let mut next_cursor: Option<String> = None;

        loop {
            let mut params = base_params.clone();
            params.push(("limit".to_string(), "200".to_string()));
            if let Some(cursor) = &next_cursor {
                params.push(("cursor".to_string(), cursor.clone()));
            }

            let response = self
                .client
                .get(url)
                .header("Authorization", format!("Bearer {}", self.token))
                .query(&params)
                .send()
                .await
                .context("Failed to send Todoist request")?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(anyhow!("Todoist API error: {} - {}", status, body));
            }

            let body = response
                .text()
                .await
                .context("Failed to read Todoist response body")?;

            let parsed: serde_json::Value = serde_json::from_str(&body)
                .with_context(|| format!("Failed to parse Todoist response body: {}", body))?;
            if let Some(results) = parsed.get("results") {
                let page_results: Vec<T> = serde_json::from_value(results.clone())
                    .context("Failed to parse Todoist paginated results")?;
                all.extend(page_results);
                next_cursor = parsed
                    .get("next_cursor")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .filter(|c| !c.is_empty());
            } else {
                let items: Vec<T> = serde_json::from_value(parsed)
                    .context("Failed to parse Todoist list response")?;
                all.extend(items);
                next_cursor = None;
            }
            if next_cursor.is_none() {
                break;
            }
        }

        Ok(all)
    }

    async fn post<T: for<'de> Deserialize<'de>, B: Serialize>(
        &self,
        url: &str,
        body: &B,
    ) -> Result<T> {
        let response = self
            .client
            .post(url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .context("Failed to send Todoist request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Todoist API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse Todoist response")
    }
}

pub fn format_tasks(tasks: &[Task]) -> String {
    if tasks.is_empty() {
        return "No tasks found.".to_string();
    }

    tasks
        .iter()
        .map(|t| {
            let priority = match t.priority {
                4 => "🔴",
                3 => "🟠",
                2 => "🟡",
                _ => "⚪",
            };
            let due = t
                .due
                .as_ref()
                .map(|d| format!(" 📅 {}", d.string))
                .unwrap_or_default();
            let labels = if t.labels.is_empty() {
                String::new()
            } else {
                format!(" [{}]", t.labels.join(", "))
            };
            format!("{} {}{}{}", priority, t.content, due, labels)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn format_projects(projects: &[Project]) -> String {
    projects
        .iter()
        .map(|p| {
            let fav = if p.is_favorite { "⭐ " } else { "" };
            format!("{}{}", fav, p.name)
        })
        .collect::<Vec<_>>()
        .join(", ")
}
