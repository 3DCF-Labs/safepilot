use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

pub struct TelegramTool {
    token: String,
    write_enabled: bool,
    allowed_chat_id: i64,
    allow_external_targets: bool,
}

#[derive(Debug, Deserialize)]
struct Args {
    action: String,
    #[serde(default)]
    chat_id: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    from_chat_id: Option<String>,
    #[serde(default)]
    message_id: Option<i64>,
}

impl TelegramTool {
    pub fn new(
        token: String,
        write_enabled: bool,
        allowed_chat_id: i64,
        allow_external_targets: bool,
    ) -> Self {
        Self {
            token,
            write_enabled,
            allowed_chat_id,
            allow_external_targets,
        }
    }
}

#[async_trait]
impl Tool for TelegramTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        let actions = if self.write_enabled {
            vec!["send", "forward", "info"]
        } else {
            vec!["info"]
        };

        crate::llm::types::ToolDefinition {
            name: "telegram".into(),
            description: "Telegram Bot API tool for outbound messaging: send, forward, info."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": actions, "description": "Operation" },
                    "chat_id": { "type": "string", "description": "Target chat ID or @username (send/info/forward)" },
                    "message": { "type": "string", "description": "Message text (send)" },
                    "from_chat_id": { "type": "string", "description": "Source chat ID (forward)" },
                    "message_id": { "type": "integer", "description": "Message ID (forward)" }
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
        let client = crate::tools::telegram::TelegramClient::new(&self.token);

        match args.action.as_str() {
            "send" => {
                if !self.write_enabled {
                    return Err(anyhow!("Telegram write actions are disabled in agent mode"));
                }
                let chat_id = args
                    .chat_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("chat_id is required for telegram.send"))?;
                enforce_chat_target(chat_id, self.allowed_chat_id, self.allow_external_targets)?;
                let msg = args
                    .message
                    .as_deref()
                    .ok_or_else(|| anyhow!("message is required for telegram.send"))?;
                let sent = client.send_message(chat_id, msg, None).await?;
                Ok(format!("Sent message (id={}).", sent.message_id))
            }
            "forward" => {
                if !self.write_enabled {
                    return Err(anyhow!("Telegram write actions are disabled in agent mode"));
                }
                let chat_id = args
                    .chat_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("chat_id is required for telegram.forward"))?;
                enforce_chat_target(chat_id, self.allowed_chat_id, self.allow_external_targets)?;
                let from_chat_id = args
                    .from_chat_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("from_chat_id is required for telegram.forward"))?;
                enforce_chat_target(
                    from_chat_id,
                    self.allowed_chat_id,
                    self.allow_external_targets,
                )?;
                let message_id = args
                    .message_id
                    .ok_or_else(|| anyhow!("message_id is required for telegram.forward"))?;
                let msg = client
                    .forward_message(chat_id, from_chat_id, message_id)
                    .await?;
                Ok(format!("Forwarded message (id={}).", msg.message_id))
            }
            "info" => {
                let chat_id = args
                    .chat_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("chat_id is required for telegram.info"))?;
                enforce_chat_target(chat_id, self.allowed_chat_id, self.allow_external_targets)?;
                let chat = client.get_chat(chat_id).await?;
                let members = client.get_chat_member_count(chat_id).await.ok();
                Ok(format!(
                    "Chat {} ({}) members={}",
                    chat.title.unwrap_or_else(|| chat.id.to_string()),
                    chat.chat_type,
                    members.map(|m| m.to_string()).unwrap_or_else(|| "?".into())
                ))
            }
            other => Err(anyhow!("Unknown telegram.action: {}", other)),
        }
    }
}

fn enforce_chat_target(target: &str, allowed_chat_id: i64, allow_external: bool) -> Result<()> {
    if allow_external {
        return Ok(());
    }
    let t = target.trim();
    let ok = t
        .parse::<i64>()
        .ok()
        .is_some_and(|id| id == allowed_chat_id);
    if !ok {
        return Err(anyhow!(
            "Agent can only target the current chat ({}). Use /unsafe to allow external targets.",
            allowed_chat_id
        ));
    }
    Ok(())
}
