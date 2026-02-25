#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

const TELEGRAM_API_URL: &str = "https://api.telegram.org";

#[derive(Debug, Clone, Deserialize)]
pub struct Message {
    pub message_id: i64,
    pub chat: Chat,
    #[serde(default)]
    pub text: Option<String>,
    pub date: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Chat {
    pub id: i64,
    #[serde(rename = "type")]
    pub chat_type: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct User {
    pub id: i64,
    pub is_bot: bool,
    pub first_name: String,
    #[serde(default)]
    pub last_name: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChatMember {
    pub user: User,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct InlineKeyboardButton {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callback_data: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InlineKeyboardMarkup {
    pub inline_keyboard: Vec<Vec<InlineKeyboardButton>>,
}

#[derive(Debug, Deserialize)]
struct TelegramResponse<T> {
    ok: bool,
    #[serde(default)]
    description: Option<String>,
    result: Option<T>,
}

pub struct TelegramClient {
    token: String,
    client: reqwest::Client,
}

impl TelegramClient {
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn send_message(
        &self,
        chat_id: &str,
        text: &str,
        parse_mode: Option<&str>,
    ) -> Result<Message> {
        let mut params = serde_json::json!({
            "chat_id": chat_id,
            "text": text,
        });

        if let Some(mode) = parse_mode {
            params["parse_mode"] = serde_json::Value::String(mode.to_string());
        }

        self.call("sendMessage", &params).await
    }

    pub async fn send_message_with_keyboard(
        &self,
        chat_id: &str,
        text: &str,
        keyboard: InlineKeyboardMarkup,
    ) -> Result<Message> {
        let params = serde_json::json!({
            "chat_id": chat_id,
            "text": text,
            "reply_markup": keyboard,
        });

        self.call("sendMessage", &params).await
    }

    pub async fn forward_message(
        &self,
        chat_id: &str,
        from_chat_id: &str,
        message_id: i64,
    ) -> Result<Message> {
        let params = serde_json::json!({
            "chat_id": chat_id,
            "from_chat_id": from_chat_id,
            "message_id": message_id,
        });

        self.call("forwardMessage", &params).await
    }

    pub async fn edit_message_text(
        &self,
        chat_id: &str,
        message_id: i64,
        text: &str,
    ) -> Result<Message> {
        let params = serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
        });

        self.call("editMessageText", &params).await
    }

    pub async fn delete_message(&self, chat_id: &str, message_id: i64) -> Result<bool> {
        let params = serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
        });

        self.call("deleteMessage", &params).await
    }

    pub async fn send_document(
        &self,
        chat_id: &str,
        document_url: &str,
        caption: Option<&str>,
    ) -> Result<Message> {
        let mut params = serde_json::json!({
            "chat_id": chat_id,
            "document": document_url,
        });

        if let Some(caption) = caption {
            params["caption"] = serde_json::Value::String(caption.to_string());
        }

        self.call("sendDocument", &params).await
    }

    pub async fn send_photo(
        &self,
        chat_id: &str,
        photo_url: &str,
        caption: Option<&str>,
    ) -> Result<Message> {
        let mut params = serde_json::json!({
            "chat_id": chat_id,
            "photo": photo_url,
        });

        if let Some(caption) = caption {
            params["caption"] = serde_json::Value::String(caption.to_string());
        }

        self.call("sendPhoto", &params).await
    }

    pub async fn get_chat(&self, chat_id: &str) -> Result<Chat> {
        let params = serde_json::json!({
            "chat_id": chat_id,
        });

        self.call("getChat", &params).await
    }

    pub async fn get_chat_member_count(&self, chat_id: &str) -> Result<i64> {
        let params = serde_json::json!({
            "chat_id": chat_id,
        });

        self.call("getChatMemberCount", &params).await
    }

    pub async fn get_chat_administrators(&self, chat_id: &str) -> Result<Vec<ChatMember>> {
        let params = serde_json::json!({
            "chat_id": chat_id,
        });

        self.call("getChatAdministrators", &params).await
    }

    pub async fn get_chat_member(&self, chat_id: &str, user_id: i64) -> Result<ChatMember> {
        let params = serde_json::json!({
            "chat_id": chat_id,
            "user_id": user_id,
        });

        self.call("getChatMember", &params).await
    }

    pub async fn pin_message(&self, chat_id: &str, message_id: i64) -> Result<bool> {
        let params = serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
        });

        self.call("pinChatMessage", &params).await
    }

    pub async fn unpin_message(&self, chat_id: &str, message_id: i64) -> Result<bool> {
        let params = serde_json::json!({
            "chat_id": chat_id,
            "message_id": message_id,
        });

        self.call("unpinChatMessage", &params).await
    }

    pub async fn send_chat_action(&self, chat_id: &str, action: &str) -> Result<bool> {
        let params = serde_json::json!({
            "chat_id": chat_id,
            "action": action,
        });

        self.call("sendChatAction", &params).await
    }

    pub async fn get_me(&self) -> Result<User> {
        self.call("getMe", &serde_json::json!({})).await
    }

    pub async fn set_webhook(&self, url: &str) -> Result<bool> {
        let params = serde_json::json!({
            "url": url,
        });

        self.call("setWebhook", &params).await
    }

    pub async fn delete_webhook(&self) -> Result<bool> {
        self.call("deleteWebhook", &serde_json::json!({})).await
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<T> {
        let url = format!("{}/bot{}/{}", TELEGRAM_API_URL, self.token, method);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(params)
            .send()
            .await
            .context("Failed to send Telegram request")?;

        let status = response.status();
        let body: TelegramResponse<T> = response
            .json()
            .await
            .context("Failed to parse Telegram response")?;

        if !body.ok {
            return Err(anyhow!(
                "Telegram API error: {}",
                body.description
                    .unwrap_or_else(|| format!("HTTP {}", status))
            ));
        }

        body.result.ok_or_else(|| anyhow!("No result in response"))
    }
}

pub fn inline_keyboard(buttons: Vec<(&str, &str)>) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup {
        inline_keyboard: vec![buttons
            .into_iter()
            .map(|(text, data)| InlineKeyboardButton {
                text: text.to_string(),
                callback_data: Some(data.to_string()),
                url: None,
            })
            .collect()],
    }
}

pub fn url_keyboard(buttons: Vec<(&str, &str)>) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup {
        inline_keyboard: vec![buttons
            .into_iter()
            .map(|(text, url)| InlineKeyboardButton {
                text: text.to_string(),
                url: Some(url.to_string()),
                callback_data: None,
            })
            .collect()],
    }
}

pub fn parse_chat_id(input: &str) -> String {
    let trimmed = input.trim();
    if !trimmed.starts_with('@') && !trimmed.starts_with('-') && trimmed.parse::<i64>().is_err() {
        format!("@{}", trimmed)
    } else {
        trimmed.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_chat_id() {
        assert_eq!(parse_chat_id("123456"), "123456");
        assert_eq!(parse_chat_id("-100123456"), "-100123456");
        assert_eq!(parse_chat_id("@channel"), "@channel");
        assert_eq!(parse_chat_id("channel"), "@channel");
    }

    #[test]
    fn test_inline_keyboard() {
        let kb = inline_keyboard(vec![("Yes", "yes"), ("No", "no")]);
        assert_eq!(kb.inline_keyboard.len(), 1);
        assert_eq!(kb.inline_keyboard[0].len(), 2);
    }
}
