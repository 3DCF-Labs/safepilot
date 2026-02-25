#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

const SLACK_API_URL: &str = "https://slack.com/api";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackAttachment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub footer: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackBlock {
    #[serde(rename = "type")]
    pub block_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<SlackTextObject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackTextObject {
    #[serde(rename = "type")]
    pub text_type: String, // "plain_text" or "mrkdwn"
    pub text: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SlackChannel {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub is_private: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SlackMessage {
    pub ts: String,
    pub text: String,
    #[serde(default)]
    pub user: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SlackResponse<T> {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
    #[serde(flatten)]
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
struct ChannelsListData {
    channels: Vec<SlackChannel>,
}

#[derive(Debug, Deserialize)]
struct MessagesData {
    messages: Vec<SlackMessage>,
}

#[derive(Debug, Deserialize)]
struct PostMessageData {
    ts: String,
    channel: String,
}

pub struct SlackClient {
    token: String,
    client: reqwest::Client,
}

impl SlackClient {
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn send_message(
        &self,
        channel: &str,
        text: &str,
        attachments: Option<Vec<SlackAttachment>>,
    ) -> Result<String> {
        let mut body = serde_json::json!({
            "channel": channel,
            "text": text,
        });

        if let Some(attachments) = attachments {
            body["attachments"] = serde_json::to_value(attachments)?;
        }

        let response: SlackResponse<PostMessageData> = self.post("chat.postMessage", &body).await?;

        if !response.ok {
            return Err(anyhow!(
                "Slack API error: {}",
                response.error.unwrap_or_else(|| "unknown".into())
            ));
        }

        response
            .data
            .map(|d| d.ts)
            .ok_or_else(|| anyhow!("Missing timestamp in response"))
    }

    pub async fn send_blocks(
        &self,
        channel: &str,
        text: &str,
        blocks: Vec<SlackBlock>,
    ) -> Result<String> {
        let body = serde_json::json!({
            "channel": channel,
            "text": text, // Fallback for notifications
            "blocks": blocks,
        });

        let response: SlackResponse<PostMessageData> = self.post("chat.postMessage", &body).await?;

        if !response.ok {
            return Err(anyhow!(
                "Slack API error: {}",
                response.error.unwrap_or_else(|| "unknown".into())
            ));
        }

        response
            .data
            .map(|d| d.ts)
            .ok_or_else(|| anyhow!("Missing timestamp in response"))
    }

    pub async fn list_channels(&self, limit: usize) -> Result<Vec<SlackChannel>> {
        let response: SlackResponse<ChannelsListData> = self
            .get(
                "conversations.list",
                &[
                    ("types", "public_channel,private_channel"),
                    ("limit", &limit.to_string()),
                    ("exclude_archived", "true"),
                ],
            )
            .await?;

        if !response.ok {
            return Err(anyhow!(
                "Slack API error: {}",
                response.error.unwrap_or_else(|| "unknown".into())
            ));
        }

        response
            .data
            .map(|d| d.channels)
            .ok_or_else(|| anyhow!("Missing channels in response"))
    }

    pub async fn get_messages(&self, channel: &str, limit: usize) -> Result<Vec<SlackMessage>> {
        let response: SlackResponse<MessagesData> = self
            .get(
                "conversations.history",
                &[("channel", channel), ("limit", &limit.to_string())],
            )
            .await?;

        if !response.ok {
            return Err(anyhow!(
                "Slack API error: {}",
                response.error.unwrap_or_else(|| "unknown".into())
            ));
        }

        response
            .data
            .map(|d| d.messages)
            .ok_or_else(|| anyhow!("Missing messages in response"))
    }

    pub async fn search_messages(&self, query: &str, count: usize) -> Result<Vec<SlackMessage>> {
        #[derive(Debug, Deserialize)]
        struct SearchData {
            messages: SearchMatches,
        }
        #[derive(Debug, Deserialize)]
        struct SearchMatches {
            matches: Vec<SlackMessage>,
        }

        let response: SlackResponse<SearchData> = self
            .get(
                "search.messages",
                &[("query", query), ("count", &count.to_string())],
            )
            .await?;

        if !response.ok {
            return Err(anyhow!(
                "Slack API error: {}",
                response.error.unwrap_or_else(|| "unknown".into())
            ));
        }

        response
            .data
            .map(|d| d.messages.matches)
            .ok_or_else(|| anyhow!("Missing search results"))
    }

    pub async fn add_reaction(&self, channel: &str, timestamp: &str, emoji: &str) -> Result<()> {
        let body = serde_json::json!({
            "channel": channel,
            "timestamp": timestamp,
            "name": emoji,
        });

        let response: SlackResponse<()> = self.post("reactions.add", &body).await?;

        if !response.ok {
            return Err(anyhow!(
                "Slack API error: {}",
                response.error.unwrap_or_else(|| "unknown".into())
            ));
        }

        Ok(())
    }

    async fn get<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: &[(&str, &str)],
    ) -> Result<T> {
        let url = format!("{}/{}", SLACK_API_URL, method);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .query(params)
            .send()
            .await
            .context("Failed to send request to Slack API")?;

        response
            .json()
            .await
            .context("Failed to parse Slack response")
    }

    async fn post<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let url = format!("{}/{}", SLACK_API_URL, method);

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .context("Failed to send request to Slack API")?;

        response
            .json()
            .await
            .context("Failed to parse Slack response")
    }
}

pub fn text_block(text: &str, markdown: bool) -> SlackBlock {
    SlackBlock {
        block_type: "section".into(),
        text: Some(SlackTextObject {
            text_type: if markdown { "mrkdwn" } else { "plain_text" }.into(),
            text: text.into(),
        }),
    }
}

pub fn divider_block() -> SlackBlock {
    SlackBlock {
        block_type: "divider".into(),
        text: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_block() {
        let block = text_block("Hello *world*", true);
        assert_eq!(block.block_type, "section");
        assert_eq!(block.text.unwrap().text_type, "mrkdwn");
    }
}
