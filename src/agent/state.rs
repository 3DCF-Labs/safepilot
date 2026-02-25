use crate::llm::types::{ContentBlock, Message, Role};
use crate::utils::truncate_str;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAgentMessage {
    pub role: String,
    pub text: String,
}

pub fn decode_state(json: &str) -> Vec<StoredAgentMessage> {
    serde_json::from_str::<Vec<StoredAgentMessage>>(json).unwrap_or_default()
}

pub fn encode_state(messages: &[StoredAgentMessage]) -> String {
    serde_json::to_string(messages).unwrap_or_else(|_| "[]".into())
}

pub fn to_llm_messages(stored: &[StoredAgentMessage]) -> Vec<Message> {
    stored
        .iter()
        .filter_map(|m| {
            let role = match m.role.as_str() {
                "user" => Role::User,
                "assistant" => Role::Assistant,
                _ => return None,
            };
            Some(Message {
                role,
                content: vec![ContentBlock::Text(m.text.clone())],
            })
        })
        .collect()
}

pub fn append_turn(
    mut stored: Vec<StoredAgentMessage>,
    user_text: &str,
    assistant_text: &str,
    max_messages: usize,
) -> Vec<StoredAgentMessage> {
    stored.push(StoredAgentMessage {
        role: "user".into(),
        text: truncate_str(user_text, 10_000),
    });
    stored.push(StoredAgentMessage {
        role: "assistant".into(),
        text: truncate_str(assistant_text, 10_000),
    });

    if stored.len() > max_messages {
        stored.drain(0..(stored.len() - max_messages));
    }
    stored
}
