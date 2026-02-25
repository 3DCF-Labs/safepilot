use crate::secrets::SecretSpec;
use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct SlackTool {
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
    channel: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    emoji: Option<String>,
}

impl SlackTool {
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
impl Tool for SlackTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        let actions = if self.write_enabled && self.write_token.is_some() {
            vec!["send", "channels", "history", "search", "react"]
        } else {
            vec!["channels", "history", "search"]
        };

        crate::llm::types::ToolDefinition {
            name: "slack".into(),
            description:
                "Slack operations: send message, list channels, get history, search, react.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": actions, "description": "Operation" },
                    "channel": { "type": "string", "description": "Channel ID or #name (send/history/react)" },
                    "text": { "type": "string", "description": "Message text (send)" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 200, "description": "Limit (channels/history/search)" },
                    "query": { "type": "string", "description": "Search query (search)" },
                    "timestamp": { "type": "string", "description": "Slack message ts (react)" },
                    "emoji": { "type": "string", "description": "Emoji name without colons, e.g. 'thumbsup' (react)" }
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
        let is_write = matches!(args.action.as_str(), "send" | "react");
        let token = if is_write {
            if !self.write_enabled {
                return Err(anyhow!("Slack write actions are disabled in agent mode"));
            }
            if let Some(v) = self.write_token_override.as_ref() {
                v.clone()
            } else {
                self.write_token
                    .as_ref()
                    .ok_or_else(|| anyhow!("SLACK_BOT_TOKEN (write) not configured"))?
                    .load_with_crypto(self.crypto.as_deref())?
            }
        } else if let Some(v) = self.read_token_override.as_ref() {
            v.clone()
        } else {
            self.read_token.load_with_crypto(self.crypto.as_deref())?
        };
        let client = crate::tools::slack::SlackClient::new(&token);

        match args.action.as_str() {
            "send" => {
                let channel = args
                    .channel
                    .as_deref()
                    .ok_or_else(|| anyhow!("channel is required for slack.send"))?;
                let text = args
                    .text
                    .as_deref()
                    .ok_or_else(|| anyhow!("text is required for slack.send"))?;
                let ts = client.send_message(channel, text, None).await?;
                Ok(format!("Sent message to {} (ts={}).", channel, ts))
            }
            "channels" => {
                let limit = args.limit.unwrap_or(50).min(200);
                let channels = client.list_channels(limit).await?;
                if channels.is_empty() {
                    return Ok("No channels found.".into());
                }
                Ok(channels
                    .into_iter()
                    .map(|c| format!("{} ({})", c.name, c.id))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "history" => {
                let channel = args
                    .channel
                    .as_deref()
                    .ok_or_else(|| anyhow!("channel is required for slack.history"))?;
                let limit = args.limit.unwrap_or(20).min(200);
                let messages = client.get_messages(channel, limit).await?;
                if messages.is_empty() {
                    return Ok("No messages found.".into());
                }
                Ok(messages
                    .into_iter()
                    .map(|m| format!("{}: {}", m.ts, m.text))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "search" => {
                let query = args
                    .query
                    .as_deref()
                    .ok_or_else(|| anyhow!("query is required for slack.search"))?;
                let count = args.limit.unwrap_or(10).min(20);
                let messages = client.search_messages(query, count).await?;
                if messages.is_empty() {
                    return Ok("No matches found.".into());
                }
                Ok(messages
                    .into_iter()
                    .map(|m| format!("{}: {}", m.ts, m.text))
                    .collect::<Vec<_>>()
                    .join("\n"))
            }
            "react" => {
                let channel = args
                    .channel
                    .as_deref()
                    .ok_or_else(|| anyhow!("channel is required for slack.react"))?;
                let timestamp = args
                    .timestamp
                    .as_deref()
                    .ok_or_else(|| anyhow!("timestamp is required for slack.react"))?;
                let emoji = args
                    .emoji
                    .as_deref()
                    .ok_or_else(|| anyhow!("emoji is required for slack.react"))?;
                client.add_reaction(channel, timestamp, emoji).await?;
                Ok("Reaction added.".into())
            }
            other => Err(anyhow!("Unknown slack.action: {}", other)),
        }
    }
}
