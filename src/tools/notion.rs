#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const NOTION_API_URL: &str = "https://api.notion.com/v1";
const NOTION_VERSION: &str = "2022-06-28";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RichText {
    #[serde(rename = "type")]
    pub rich_type: String,
    pub text: Option<TextContent>,
    pub plain_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextContent {
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<Link>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Link {
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PropertyValue {
    Title { title: Vec<RichText> },
    RichText { rich_text: Vec<RichText> },
    Number { number: Option<f64> },
    Select { select: Option<SelectOption> },
    MultiSelect { multi_select: Vec<SelectOption> },
    Date { date: Option<DateValue> },
    Checkbox { checkbox: bool },
    Url { url: Option<String> },
    Email { email: Option<String> },
    Status { status: Option<SelectOption> },
    Other(serde_json::Value),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectOption {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateValue {
    pub start: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotionPage {
    pub id: String,
    pub url: String,
    pub properties: HashMap<String, PropertyValue>,
    pub created_time: String,
    pub last_edited_time: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotionDatabase {
    pub id: String,
    pub title: Vec<RichText>,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotionBlock {
    #[serde(rename = "type")]
    pub block_type: String,
    #[serde(flatten)]
    pub content: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct ListResponse<T> {
    results: Vec<T>,
    has_more: bool,
    #[serde(default)]
    next_cursor: Option<String>,
}

pub struct NotionClient {
    token: String,
    client: reqwest::Client,
}

impl NotionClient {
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn query_database(
        &self,
        database_id: &str,
        filter: Option<serde_json::Value>,
        page_size: usize,
    ) -> Result<Vec<NotionPage>> {
        let url = format!("{}/databases/{}/query", NOTION_API_URL, database_id);

        let mut body = serde_json::json!({
            "page_size": page_size.min(100),
        });

        if let Some(filter) = filter {
            body["filter"] = filter;
        }

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Notion-Version", NOTION_VERSION)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .context("Failed to query Notion database")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Notion API error: {} - {}", status, body));
        }

        let list: ListResponse<NotionPage> = response.json().await?;
        Ok(list.results)
    }

    pub async fn create_page(
        &self,
        database_id: &str,
        properties: HashMap<String, PropertyValue>,
    ) -> Result<NotionPage> {
        let url = format!("{}/pages", NOTION_API_URL);

        let body = serde_json::json!({
            "parent": { "database_id": database_id },
            "properties": properties,
        });

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Notion-Version", NOTION_VERSION)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .context("Failed to create Notion page")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Notion API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse page response")
    }

    pub async fn update_page(
        &self,
        page_id: &str,
        properties: HashMap<String, PropertyValue>,
    ) -> Result<NotionPage> {
        let url = format!("{}/pages/{}", NOTION_API_URL, page_id);

        let body = serde_json::json!({
            "properties": properties,
        });

        let response = self
            .client
            .patch(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Notion-Version", NOTION_VERSION)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .context("Failed to update Notion page")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Notion API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse page response")
    }

    pub async fn get_page(&self, page_id: &str) -> Result<NotionPage> {
        let url = format!("{}/pages/{}", NOTION_API_URL, page_id);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Notion-Version", NOTION_VERSION)
            .send()
            .await
            .context("Failed to get Notion page")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Notion API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse page response")
    }

    pub async fn get_page_content(&self, page_id: &str) -> Result<Vec<NotionBlock>> {
        let url = format!("{}/blocks/{}/children", NOTION_API_URL, page_id);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Notion-Version", NOTION_VERSION)
            .send()
            .await
            .context("Failed to get page content")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Notion API error: {} - {}", status, body));
        }

        let list: ListResponse<NotionBlock> = response.json().await?;
        Ok(list.results)
    }

    pub async fn search(
        &self,
        query: &str,
        filter: Option<&str>,
    ) -> Result<Vec<serde_json::Value>> {
        let url = format!("{}/search", NOTION_API_URL);

        let mut body = serde_json::json!({
            "query": query,
            "page_size": 20,
        });

        if let Some(object_type) = filter {
            body["filter"] = serde_json::json!({
                "property": "object",
                "value": object_type,
            });
        }

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Notion-Version", NOTION_VERSION)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .context("Failed to search Notion")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Notion API error: {} - {}", status, body));
        }

        let list: ListResponse<serde_json::Value> = response.json().await?;
        Ok(list.results)
    }

    pub async fn append_blocks(&self, page_id: &str, blocks: Vec<serde_json::Value>) -> Result<()> {
        let url = format!("{}/blocks/{}/children", NOTION_API_URL, page_id);

        let body = serde_json::json!({
            "children": blocks,
        });

        let response = self
            .client
            .patch(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Notion-Version", NOTION_VERSION)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .context("Failed to append blocks")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Notion API error: {} - {}", status, body));
        }

        Ok(())
    }
}

pub fn title_property(text: &str) -> PropertyValue {
    PropertyValue::Title {
        title: vec![RichText {
            rich_type: "text".into(),
            text: Some(TextContent {
                content: text.into(),
                link: None,
            }),
            plain_text: Some(text.into()),
        }],
    }
}

pub fn text_property(text: &str) -> PropertyValue {
    PropertyValue::RichText {
        rich_text: vec![RichText {
            rich_type: "text".into(),
            text: Some(TextContent {
                content: text.into(),
                link: None,
            }),
            plain_text: Some(text.into()),
        }],
    }
}

pub fn select_property(name: &str) -> PropertyValue {
    PropertyValue::Select {
        select: Some(SelectOption {
            name: name.into(),
            color: None,
        }),
    }
}

pub fn checkbox_property(checked: bool) -> PropertyValue {
    PropertyValue::Checkbox { checkbox: checked }
}

pub fn number_property(value: f64) -> PropertyValue {
    PropertyValue::Number {
        number: Some(value),
    }
}

pub fn url_property(url: &str) -> PropertyValue {
    PropertyValue::Url {
        url: Some(url.into()),
    }
}

pub fn date_property(start: &str, end: Option<&str>) -> PropertyValue {
    PropertyValue::Date {
        date: Some(DateValue {
            start: start.into(),
            end: end.map(|s| s.into()),
        }),
    }
}

pub fn paragraph_block(text: &str) -> serde_json::Value {
    serde_json::json!({
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{
                "type": "text",
                "text": { "content": text }
            }]
        }
    })
}

pub fn heading_block(text: &str, level: u8) -> serde_json::Value {
    let heading_type = match level {
        1 => "heading_1",
        2 => "heading_2",
        _ => "heading_3",
    };
    serde_json::json!({
        "object": "block",
        "type": heading_type,
        heading_type: {
            "rich_text": [{
                "type": "text",
                "text": { "content": text }
            }]
        }
    })
}

pub fn bullet_block(text: &str) -> serde_json::Value {
    serde_json::json!({
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {
            "rich_text": [{
                "type": "text",
                "text": { "content": text }
            }]
        }
    })
}

pub fn get_title_text(page: &NotionPage, property_name: &str) -> Option<String> {
    match page.properties.get(property_name)? {
        PropertyValue::Title { title } => Some(
            title
                .iter()
                .filter_map(|rt| rt.plain_text.clone())
                .collect(),
        ),
        _ => None,
    }
}

pub fn get_text(page: &NotionPage, property_name: &str) -> Option<String> {
    match page.properties.get(property_name)? {
        PropertyValue::RichText { rich_text } => Some(
            rich_text
                .iter()
                .filter_map(|rt| rt.plain_text.clone())
                .collect(),
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_title_property() {
        let prop = title_property("Test Title");
        match prop {
            PropertyValue::Title { title } => {
                assert_eq!(title[0].plain_text, Some("Test Title".into()));
            }
            _ => panic!("Expected Title property"),
        }
    }

    #[test]
    fn test_select_property() {
        let prop = select_property("Option A");
        match prop {
            PropertyValue::Select { select } => {
                assert_eq!(select.unwrap().name, "Option A");
            }
            _ => panic!("Expected Select property"),
        }
    }
}
