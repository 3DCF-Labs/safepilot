use crate::secrets::SecretSpec;
use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct DiscordTool {
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
    guild_id: Option<String>,
    #[serde(default)]
    channel_id: Option<String>,
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    emoji: Option<String>,
    #[serde(default)]
    timeout_minutes: Option<i64>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

impl DiscordTool {
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
impl Tool for DiscordTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        let actions = if self.write_enabled && self.write_token.is_some() {
            vec![
                "validate",
                "channels",
                "history",
                "send",
                "reply",
                "react",
                "delete",
                "timeout_user",
                "kick_user",
            ]
        } else {
            vec!["validate", "channels", "history"]
        };

        crate::llm::types::ToolDefinition {
            name: "discord".into(),
            description: "Discord operations: list channels, read history, send/reply.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": actions },
                    "guild_id": { "type": "string", "description": "Discord guild ID (channels)" },
                    "channel_id": { "type": "string", "description": "Discord channel ID (history/send/reply)" },
                    "message_id": { "type": "string", "description": "Message ID to reply to (reply)" },
                    "user_id": { "type": "string", "description": "Discord user ID (timeout_user/kick_user)" },
                    "emoji": { "type": "string", "description": "Emoji for reaction, e.g. 👍 or %F0%9F%91%8D (react)" },
                    "timeout_minutes": { "type": "integer", "minimum": 1, "maximum": 10080, "description": "Timeout duration in minutes (timeout_user)" },
                    "reason": { "type": "string", "description": "Moderation reason (timeout_user/kick_user/delete)" },
                    "content": { "type": "string", "description": "Message body (send/reply)" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "description": "History limit" }
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
        let is_write = matches!(
            args.action.as_str(),
            "send" | "reply" | "react" | "delete" | "timeout_user" | "kick_user"
        );
        let token = if is_write {
            if !self.write_enabled {
                return Err(anyhow!("Discord write actions are disabled in agent mode"));
            }
            if let Some(v) = self.write_token_override.as_ref() {
                v.clone()
            } else {
                self.write_token
                    .as_ref()
                    .ok_or_else(|| anyhow!("DISCORD_BOT_TOKEN (write) not configured"))?
                    .load_with_crypto(self.crypto.as_deref())?
            }
        } else if let Some(v) = self.read_token_override.as_ref() {
            v.clone()
        } else {
            self.read_token.load_with_crypto(self.crypto.as_deref())?
        };

        let client = reqwest::Client::new();
        let base = "https://discord.com/api/v10";
        let auth = format!("Bot {}", token);

        match args.action.as_str() {
            "validate" => {
                let resp = client
                    .get(format!("{base}/users/@me"))
                    .header("Authorization", &auth)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Discord validate failed ({}): {}", status, body);
                }
                let body: serde_json::Value = resp.json().await?;
                let id = body.get("id").and_then(|x| x.as_str()).unwrap_or("?");
                let username = body
                    .get("username")
                    .and_then(|x| x.as_str())
                    .unwrap_or("unknown");
                Ok(format!("Discord token valid for `{username}` (id={id})."))
            }
            "channels" => {
                let guild_id = args
                    .guild_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("guild_id is required for discord.channels"))?;
                let resp = client
                    .get(format!("{base}/guilds/{guild_id}/channels"))
                    .header("Authorization", &auth)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Discord channels failed ({}): {}", status, body);
                }
                let rows: Vec<serde_json::Value> = resp.json().await?;
                let out = rows
                    .into_iter()
                    .map(|v| {
                        let id = v.get("id").and_then(|x| x.as_str()).unwrap_or("?");
                        let name = v
                            .get("name")
                            .and_then(|x| x.as_str())
                            .unwrap_or("(unnamed)");
                        let kind = v.get("type").and_then(|x| x.as_i64()).unwrap_or(-1);
                        format!("{name} ({id}) type={kind}")
                    })
                    .collect::<Vec<_>>();
                Ok(if out.is_empty() {
                    "No channels found.".to_string()
                } else {
                    out.join("\n")
                })
            }
            "history" => {
                let channel_id = args
                    .channel_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("channel_id is required for discord.history"))?;
                let limit = args.limit.unwrap_or(20).clamp(1, 100);
                let resp = client
                    .get(format!("{base}/channels/{channel_id}/messages"))
                    .header("Authorization", &auth)
                    .query(&[("limit", limit.to_string())])
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Discord history failed ({}): {}", status, body);
                }
                let rows: Vec<serde_json::Value> = resp.json().await?;
                let out = rows
                    .into_iter()
                    .map(|v| {
                        let id = v.get("id").and_then(|x| x.as_str()).unwrap_or("?");
                        let content = v.get("content").and_then(|x| x.as_str()).unwrap_or("");
                        format!("{id}: {}", content.replace('\n', " "))
                    })
                    .collect::<Vec<_>>();
                Ok(if out.is_empty() {
                    "No messages found.".to_string()
                } else {
                    out.join("\n")
                })
            }
            "send" => {
                let channel_id = args
                    .channel_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("channel_id is required for discord.send"))?;
                let content = args
                    .content
                    .as_deref()
                    .ok_or_else(|| anyhow!("content is required for discord.send"))?;
                let resp = client
                    .post(format!("{base}/channels/{channel_id}/messages"))
                    .header("Authorization", &auth)
                    .json(&serde_json::json!({ "content": content }))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Discord send failed ({}): {}", status, body);
                }
                let body: serde_json::Value = resp.json().await?;
                let id = body.get("id").and_then(|x| x.as_str()).unwrap_or("?");
                Ok(format!("Sent Discord message (id={id})."))
            }
            "reply" => {
                let channel_id = args
                    .channel_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("channel_id is required for discord.reply"))?;
                let message_id = args
                    .message_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("message_id is required for discord.reply"))?;
                let content = args
                    .content
                    .as_deref()
                    .ok_or_else(|| anyhow!("content is required for discord.reply"))?;
                let payload = serde_json::json!({
                    "content": content,
                    "message_reference": { "message_id": message_id, "channel_id": channel_id }
                });
                let resp = client
                    .post(format!("{base}/channels/{channel_id}/messages"))
                    .header("Authorization", &auth)
                    .json(&payload)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Discord reply failed ({}): {}", status, body);
                }
                let body: serde_json::Value = resp.json().await?;
                let id = body.get("id").and_then(|x| x.as_str()).unwrap_or("?");
                Ok(format!("Replied in Discord (id={id})."))
            }
            "react" => {
                let channel_id = args
                    .channel_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("channel_id is required for discord.react"))?;
                let message_id = args
                    .message_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("message_id is required for discord.react"))?;
                let emoji = args
                    .emoji
                    .as_deref()
                    .ok_or_else(|| anyhow!("emoji is required for discord.react"))?;
                let emoji_enc = urlencoding::encode(emoji);
                let resp = client
                    .put(format!(
                        "{base}/channels/{channel_id}/messages/{message_id}/reactions/{emoji_enc}/@me"
                    ))
                    .header("Authorization", &auth)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Discord react failed ({}): {}", status, body);
                }
                Ok("Reaction added.".to_string())
            }
            "delete" => {
                let channel_id = args
                    .channel_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("channel_id is required for discord.delete"))?;
                let message_id = args
                    .message_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("message_id is required for discord.delete"))?;
                let mut req = client
                    .delete(format!(
                        "{base}/channels/{channel_id}/messages/{message_id}"
                    ))
                    .header("Authorization", &auth);
                if let Some(reason) = args.reason.as_deref() {
                    req = req.header("X-Audit-Log-Reason", reason);
                }
                let resp = req.send().await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Discord delete failed ({}): {}", status, body);
                }
                Ok("Message deleted.".to_string())
            }
            "timeout_user" => {
                let guild_id = args
                    .guild_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("guild_id is required for discord.timeout_user"))?;
                let user_id = args
                    .user_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("user_id is required for discord.timeout_user"))?;
                let minutes = args.timeout_minutes.unwrap_or(60).clamp(1, 10080);
                let until = (chrono::Utc::now() + chrono::Duration::minutes(minutes)).to_rfc3339();
                let mut req = client
                    .patch(format!("{base}/guilds/{guild_id}/members/{user_id}"))
                    .header("Authorization", &auth)
                    .json(&serde_json::json!({ "communication_disabled_until": until }));
                if let Some(reason) = args.reason.as_deref() {
                    req = req.header("X-Audit-Log-Reason", reason);
                }
                let resp = req.send().await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Discord timeout failed ({}): {}", status, body);
                }
                Ok(format!("User timed out for {} minutes.", minutes))
            }
            "kick_user" => {
                let guild_id = args
                    .guild_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("guild_id is required for discord.kick_user"))?;
                let user_id = args
                    .user_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("user_id is required for discord.kick_user"))?;
                let mut req = client
                    .delete(format!("{base}/guilds/{guild_id}/members/{user_id}"))
                    .header("Authorization", &auth);
                if let Some(reason) = args.reason.as_deref() {
                    req = req.header("X-Audit-Log-Reason", reason);
                }
                let resp = req.send().await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Discord kick failed ({}): {}", status, body);
                }
                Ok("User kicked.".to_string())
            }
            other => Err(anyhow!("Unknown discord.action: {}", other)),
        }
    }
}
