use crate::secrets::SecretSpec;
use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct XTool {
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
    text: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    in_reply_to_tweet_id: Option<String>,
    #[serde(default)]
    tweet_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    max_results: Option<usize>,
}

impl XTool {
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
impl Tool for XTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        let actions = if self.write_enabled && self.write_token.is_some() {
            vec![
                "validate", "search", "user", "post", "reply", "retweet", "like", "unlike",
                "delete",
            ]
        } else {
            vec!["validate", "search", "user"]
        };
        crate::llm::types::ToolDefinition {
            name: "x".into(),
            description: "X/Twitter operations: search posts, lookup user, post, reply.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": actions },
                    "text": { "type": "string", "description": "Post body (post/reply)" },
                    "query": { "type": "string", "description": "Search query (search)" },
                    "username": { "type": "string", "description": "Username without @ (user)" },
                    "in_reply_to_tweet_id": { "type": "string", "description": "Target tweet ID (reply)" },
                    "tweet_id": { "type": "string", "description": "Tweet ID for retweet/like/unlike/delete" },
                    "user_id": { "type": "string", "description": "Acting user ID for retweet/like/unlike" },
                    "max_results": { "type": "integer", "minimum": 10, "maximum": 100, "description": "Result limit for search" }
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
            "post" | "reply" | "retweet" | "like" | "unlike" | "delete"
        );
        let token = if is_write {
            if !self.write_enabled {
                return Err(anyhow!("X write actions are disabled in agent mode"));
            }
            if let Some(v) = self.write_token_override.as_ref() {
                v.clone()
            } else {
                self.write_token
                    .as_ref()
                    .ok_or_else(|| anyhow!("X_API_BEARER_TOKEN (write) not configured"))?
                    .load_with_crypto(self.crypto.as_deref())?
            }
        } else if let Some(v) = self.read_token_override.as_ref() {
            v.clone()
        } else {
            self.read_token.load_with_crypto(self.crypto.as_deref())?
        };

        let client = reqwest::Client::new();
        let auth = format!("Bearer {}", token);
        let base = "https://api.x.com/2";

        match args.action.as_str() {
            "validate" => {
                let resp = client
                    .get(format!("{base}/users/me"))
                    .header("Authorization", &auth)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("X validate failed ({}): {}", status, body);
                }
                let body: serde_json::Value = resp.json().await?;
                let user = body.get("data").cloned().unwrap_or_default();
                let id = user.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                let name = user
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let username = user
                    .get("username")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                Ok(format!(
                    "X token valid for `{name}` (@{username}, id={id})."
                ))
            }
            "search" => {
                let query = args
                    .query
                    .as_deref()
                    .ok_or_else(|| anyhow!("query is required for x.search"))?;
                let max_results = args.max_results.unwrap_or(10).clamp(10, 100);
                let resp = client
                    .get(format!("{base}/tweets/search/recent"))
                    .header("Authorization", &auth)
                    .query(&[
                        ("query", query.to_string()),
                        ("max_results", max_results.to_string()),
                        ("tweet.fields", "created_at,author_id".to_string()),
                    ])
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("X search failed ({}): {}", status, body);
                }
                let body: serde_json::Value = resp.json().await?;
                let rows = body
                    .get("data")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let out = rows
                    .into_iter()
                    .map(|v| {
                        let id = v.get("id").and_then(|x| x.as_str()).unwrap_or("?");
                        let text = v.get("text").and_then(|x| x.as_str()).unwrap_or("");
                        format!("{id}: {}", text.replace('\n', " "))
                    })
                    .collect::<Vec<_>>();
                Ok(if out.is_empty() {
                    "No results found.".to_string()
                } else {
                    out.join("\n")
                })
            }
            "user" => {
                let username = args
                    .username
                    .as_deref()
                    .ok_or_else(|| anyhow!("username is required for x.user"))?;
                let resp = client
                    .get(format!("{base}/users/by/username/{username}"))
                    .header("Authorization", &auth)
                    .query(&[("user.fields", "description,public_metrics".to_string())])
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("X user lookup failed ({}): {}", status, body);
                }
                let body: serde_json::Value = resp.json().await?;
                let data = body
                    .get("data")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({}));
                let id = data.get("id").and_then(|x| x.as_str()).unwrap_or("?");
                let name = data.get("name").and_then(|x| x.as_str()).unwrap_or("?");
                let uname = data.get("username").and_then(|x| x.as_str()).unwrap_or("?");
                Ok(format!("{} (@{}) id={}", name, uname, id))
            }
            "post" => {
                let text = args
                    .text
                    .as_deref()
                    .ok_or_else(|| anyhow!("text is required for x.post"))?;
                let resp = client
                    .post(format!("{base}/tweets"))
                    .header("Authorization", &auth)
                    .json(&serde_json::json!({ "text": text }))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("X post failed ({}): {}", status, body);
                }
                let body: serde_json::Value = resp.json().await?;
                let id = body
                    .get("data")
                    .and_then(|v| v.get("id"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                Ok(format!("Posted to X (id={id})."))
            }
            "reply" => {
                let text = args
                    .text
                    .as_deref()
                    .ok_or_else(|| anyhow!("text is required for x.reply"))?;
                let reply_to = args
                    .in_reply_to_tweet_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("in_reply_to_tweet_id is required for x.reply"))?;
                let payload = serde_json::json!({
                    "text": text,
                    "reply": { "in_reply_to_tweet_id": reply_to }
                });
                let resp = client
                    .post(format!("{base}/tweets"))
                    .header("Authorization", &auth)
                    .json(&payload)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("X reply failed ({}): {}", status, body);
                }
                let body: serde_json::Value = resp.json().await?;
                let id = body
                    .get("data")
                    .and_then(|v| v.get("id"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("?");
                Ok(format!("Replied on X (id={id})."))
            }
            "retweet" => {
                let user_id = args
                    .user_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("user_id is required for x.retweet"))?;
                let tweet_id = args
                    .tweet_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("tweet_id is required for x.retweet"))?;
                let resp = client
                    .post(format!("{base}/users/{user_id}/retweets"))
                    .header("Authorization", &auth)
                    .json(&serde_json::json!({ "tweet_id": tweet_id }))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("X retweet failed ({}): {}", status, body);
                }
                Ok("Retweet action submitted.".to_string())
            }
            "like" => {
                let user_id = args
                    .user_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("user_id is required for x.like"))?;
                let tweet_id = args
                    .tweet_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("tweet_id is required for x.like"))?;
                let resp = client
                    .post(format!("{base}/users/{user_id}/likes"))
                    .header("Authorization", &auth)
                    .json(&serde_json::json!({ "tweet_id": tweet_id }))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("X like failed ({}): {}", status, body);
                }
                Ok("Like action submitted.".to_string())
            }
            "unlike" => {
                let user_id = args
                    .user_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("user_id is required for x.unlike"))?;
                let tweet_id = args
                    .tweet_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("tweet_id is required for x.unlike"))?;
                let resp = client
                    .delete(format!("{base}/users/{user_id}/likes/{tweet_id}"))
                    .header("Authorization", &auth)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("X unlike failed ({}): {}", status, body);
                }
                Ok("Unlike action submitted.".to_string())
            }
            "delete" => {
                let tweet_id = args
                    .tweet_id
                    .as_deref()
                    .ok_or_else(|| anyhow!("tweet_id is required for x.delete"))?;
                let resp = client
                    .delete(format!("{base}/tweets/{tweet_id}"))
                    .header("Authorization", &auth)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("X delete failed ({}): {}", status, body);
                }
                Ok("Delete action submitted.".to_string())
            }
            other => Err(anyhow!("Unknown x.action: {}", other)),
        }
    }
}
