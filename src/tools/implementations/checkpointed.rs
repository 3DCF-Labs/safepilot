use crate::db::{Database, RiskTier, TaskRecord, TaskStatus};
use crate::policy;
use crate::tools::registry::Tool;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::Value;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub struct CheckpointedTool {
    inner: Arc<dyn Tool>,
    db: Arc<Database>,
    run_id: String,
    parent_task_id: String,
    agent_profile: String,
    default_owner_repo: Option<String>,
}

impl CheckpointedTool {
    pub fn new(
        inner: Arc<dyn Tool>,
        db: Arc<Database>,
        run_id: String,
        parent_task_id: String,
        agent_profile: String,
        default_owner_repo: Option<String>,
    ) -> Self {
        Self {
            inner,
            db,
            run_id,
            parent_task_id,
            agent_profile,
            default_owner_repo,
        }
    }
}

#[async_trait]
impl Tool for CheckpointedTool {
    fn definition(&self) -> crate::llm::types::ToolDefinition {
        self.inner.definition()
    }

    async fn execute(&self, arguments: &Value, cancel: &CancellationToken) -> Result<String> {
        if cancel.is_cancelled() {
            anyhow::bail!("Cancelled");
        }

        let tool_name = self.definition().name.clone();
        let (action_type, goal) = tool_call_to_job_action_and_goal(
            &tool_name,
            arguments,
            self.default_owner_repo.as_deref(),
        )?;

        let risk = policy::classify_job_action(&action_type, &goal);
        if risk == RiskTier::Safe {
            return self.inner.execute(arguments, cancel).await;
        }

        let tool_task_id = format!("task-{}", Uuid::new_v4().simple());
        let tool_task = TaskRecord {
            task_id: tool_task_id.clone(),
            run_id: self.run_id.clone(),
            agent: self.agent_profile.clone(),
            action_type: action_type.clone(),
            goal: goal.clone(),
            risk_tier: risk,
            status: TaskStatus::Queued,
            job_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        self.db.insert_task(&tool_task).await?;
        self.db
            .insert_task_dep(&tool_task_id, &self.parent_task_id)
            .await?;

        let resume_id = format!("task-{}", Uuid::new_v4().simple());
        let resume = TaskRecord {
            task_id: resume_id.clone(),
            run_id: self.run_id.clone(),
            agent: self.agent_profile.clone(),
            action_type: "agent".into(),
            goal: format!(
                "Resume after tool task {} ({}). Use dependency results to continue.",
                tool_task_id, action_type
            ),
            risk_tier: RiskTier::Safe,
            status: TaskStatus::Queued,
            job_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        self.db.insert_task(&resume).await?;
        self.db.insert_task_dep(&resume_id, &tool_task_id).await?;

        Ok(format!(
            "CHECKPOINT: created task `{}` (type={} risk={}) and resume task `{}`. Use `/plan {}` and approve if needed.",
            tool_task_id,
            action_type,
            risk.as_str(),
            resume_id,
            self.run_id
        ))
    }
}

fn tool_call_to_job_action_and_goal(
    tool_name: &str,
    arguments: &Value,
    default_owner_repo: Option<&str>,
) -> Result<(String, String)> {
    let action = arguments
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();

    match tool_name {
        "github" => {
            let repo = arguments
                .get("repo")
                .and_then(|v| v.as_str())
                .or(default_owner_repo)
                .ok_or_else(|| anyhow!("github repo missing; provide args.repo as owner/repo"))?;
            let goal = match action.as_str() {
                "list_issues" => serde_json::json!({"op":"issues","repo":repo}).to_string(),
                "search_issues" => {
                    let q = arguments
                        .get("query")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("github.search_issues missing query"))?;
                    serde_json::json!({"op":"search","query":q}).to_string()
                }
                "list_prs" => serde_json::json!({"op":"prs","repo":repo}).to_string(),
                "list_ci" => serde_json::json!({"op":"ci","repo":repo}).to_string(),
                "create_issue" => {
                    let title = arguments
                        .get("title")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("github.create_issue missing title"))?;
                    let body = arguments.get("body").and_then(|v| v.as_str()).unwrap_or("");
                    serde_json::json!({"op":"create-issue","repo":repo,"title":title,"body":body})
                        .to_string()
                }
                "comment" => {
                    let num = arguments
                        .get("issue_number")
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| anyhow!("github.comment missing issue_number"))?;
                    let body = arguments
                        .get("body")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("github.comment missing body"))?;
                    serde_json::json!({"op":"comment","repo":repo,"issue_number":num,"body":body})
                        .to_string()
                }
                "" => return Err(anyhow!("github tool call missing 'action' field")),
                other => return Err(anyhow!("Unknown github.action: {}", other)),
            };
            Ok(("github".into(), goal))
        }
        "slack" => {
            let goal = match action.as_str() {
                "channels" => {
                    let limit = arguments
                        .get("limit")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(50);
                    serde_json::json!({"op":"channels","limit":limit}).to_string()
                }
                "history" => {
                    let ch = arguments
                        .get("channel")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("slack.history missing channel"))?;
                    let limit = arguments
                        .get("limit")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(20);
                    serde_json::json!({"op":"history","channel":ch,"limit":limit}).to_string()
                }
                "search" => {
                    let q = arguments
                        .get("query")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("slack.search missing query"))?;
                    serde_json::json!({"op":"search","query":q}).to_string()
                }
                "send" => {
                    let ch = arguments
                        .get("channel")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("slack.send missing channel"))?;
                    let text = arguments
                        .get("text")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("slack.send missing text"))?;
                    serde_json::json!({"op":"send","channel":ch,"text":text}).to_string()
                }
                "react" => {
                    let ch = arguments
                        .get("channel")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("slack.react missing channel"))?;
                    let ts = arguments
                        .get("timestamp")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("slack.react missing timestamp"))?;
                    let emoji = arguments
                        .get("emoji")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("slack.react missing emoji"))?;
                    serde_json::json!({"op":"react","channel":ch,"timestamp":ts,"emoji":emoji})
                        .to_string()
                }
                "" => return Err(anyhow!("slack tool call missing 'action' field")),
                other => return Err(anyhow!("Unknown slack.action: {}", other)),
            };
            Ok(("slack".into(), goal))
        }
        "notion" => {
            let goal = match action.as_str() {
                "search" => {
                    let q = arguments
                        .get("query")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    serde_json::json!({"op":"search","query":q}).to_string()
                }
                "query_database" => {
                    let id = arguments
                        .get("database_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("notion.query_database missing database_id"))?;
                    serde_json::json!({"op":"query","database_id":id}).to_string()
                }
                "create_page" => {
                    let id = arguments
                        .get("database_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("notion.create_page missing database_id"))?;
                    let title = arguments
                        .get("title")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("notion.create_page missing title"))?;
                    serde_json::json!({"op":"create","database_id":id,"title":title}).to_string()
                }
                "get_page" => {
                    let id = arguments
                        .get("page_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("notion.get_page missing page_id"))?;
                    serde_json::json!({"op":"get","page_id":id}).to_string()
                }
                "" => return Err(anyhow!("notion tool call missing 'action' field")),
                other => return Err(anyhow!("Unknown notion.action: {}", other)),
            };
            Ok(("notion".into(), goal))
        }
        "linear" => {
            let goal = match action.as_str() {
                "teams" => serde_json::json!({"op":"teams"}).to_string(),
                "issues" => {
                    let team = arguments
                        .get("team")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("linear.issues missing team"))?;
                    serde_json::json!({"op":"issues","team":team}).to_string()
                }
                "search" => {
                    let q = arguments
                        .get("query")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("linear.search missing query"))?;
                    serde_json::json!({"op":"search","query":q}).to_string()
                }
                "create_issue" => {
                    let team = arguments
                        .get("team")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("linear.create_issue missing team"))?;
                    let title = arguments
                        .get("title")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("linear.create_issue missing title"))?;
                    let desc = arguments
                        .get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    serde_json::json!({"op":"create","team":team,"title":title,"description":desc})
                        .to_string()
                }
                "" => return Err(anyhow!("linear tool call missing 'action' field")),
                other => return Err(anyhow!("Unknown linear.action: {}", other)),
            };
            Ok(("linear".into(), goal))
        }
        "jira" => {
            let goal = match action.as_str() {
                "projects" => serde_json::json!({"op":"projects"}).to_string(),
                "boards" => serde_json::json!({"op":"boards"}).to_string(),
                "search" => {
                    let jql = arguments
                        .get("jql")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("jira.search missing jql"))?;
                    serde_json::json!({"op":"search","jql":jql}).to_string()
                }
                "get" => {
                    let key = arguments
                        .get("issue_key")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("jira.get missing issue_key"))?;
                    serde_json::json!({"op":"get","issue_key":key}).to_string()
                }
                "create" => {
                    let project = arguments
                        .get("project")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("jira.create missing project"))?;
                    let typ = arguments
                        .get("issue_type")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("jira.create missing issue_type"))?;
                    let summary = arguments
                        .get("summary")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("jira.create missing summary"))?;
                    let desc = arguments
                        .get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    serde_json::json!({"op":"create","project":project,"issue_type":typ,"summary":summary,"description":desc}).to_string()
                }
                "comment" => {
                    let key = arguments
                        .get("issue_key")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("jira.comment missing issue_key"))?;
                    let text = arguments
                        .get("comment")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("jira.comment missing comment"))?;
                    serde_json::json!({"op":"comment","issue_key":key,"comment":text}).to_string()
                }
                "" => return Err(anyhow!("jira tool call missing 'action' field")),
                other => return Err(anyhow!("Unknown jira.action: {}", other)),
            };
            Ok(("jira".into(), goal))
        }
        "todoist" => {
            let goal = match action.as_str() {
                "list" => serde_json::json!({"op":"list"}).to_string(),
                "projects" => serde_json::json!({"op":"projects"}).to_string(),
                "add" => {
                    let text = arguments
                        .get("task")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("todoist.add missing task"))?;
                    let due = arguments.get("due").and_then(|v| v.as_str()).unwrap_or("");
                    if due.is_empty() {
                        serde_json::json!({"op":"add","task":text}).to_string()
                    } else {
                        serde_json::json!({"op":"add","task":text,"due":due}).to_string()
                    }
                }
                "complete" => {
                    let id = arguments
                        .get("task_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("todoist.complete missing task_id"))?;
                    serde_json::json!({"op":"complete","task_id":id}).to_string()
                }
                "" => return Err(anyhow!("todoist tool call missing 'action' field")),
                other => return Err(anyhow!("Unknown todoist.action: {}", other)),
            };
            Ok(("todoist".into(), goal))
        }
        "telegram" => {
            let goal = match action.as_str() {
                "info" => {
                    let chat = arguments
                        .get("chat_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("telegram.info missing chat_id"))?;
                    serde_json::json!({"op":"info","chat_id":chat}).to_string()
                }
                "send" => {
                    let chat = arguments
                        .get("chat_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("telegram.send missing chat_id"))?;
                    let msg = arguments
                        .get("message")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("telegram.send missing message"))?;
                    serde_json::json!({"op":"send","chat_id":chat,"message":msg}).to_string()
                }
                "forward" => {
                    let to_chat = arguments
                        .get("chat_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("telegram.forward missing chat_id"))?;
                    let from_chat = arguments
                        .get("from_chat_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("telegram.forward missing from_chat_id"))?;
                    let msg_id = arguments
                        .get("message_id")
                        .and_then(|v| v.as_i64())
                        .ok_or_else(|| anyhow!("telegram.forward missing message_id"))?;
                    serde_json::json!({"op":"forward","chat_id":to_chat,"from_chat_id":from_chat,"message_id":msg_id}).to_string()
                }
                "" => return Err(anyhow!("telegram tool call missing 'action' field")),
                other => return Err(anyhow!("Unknown telegram.action: {}", other)),
            };
            Ok(("telegram".into(), goal))
        }
        "discord" => {
            let goal = match action.as_str() {
                "validate" => serde_json::json!({"op":"validate"}).to_string(),
                "channels" => {
                    let guild_id = arguments
                        .get("guild_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.channels missing guild_id"))?;
                    serde_json::json!({"op":"channels","guild_id":guild_id}).to_string()
                }
                "history" => {
                    let channel_id = arguments
                        .get("channel_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.history missing channel_id"))?;
                    let limit = arguments
                        .get("limit")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(20);
                    serde_json::json!({"op":"history","channel_id":channel_id,"limit":limit})
                        .to_string()
                }
                "send" => {
                    let channel_id = arguments
                        .get("channel_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.send missing channel_id"))?;
                    let content = arguments
                        .get("content")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.send missing content"))?;
                    serde_json::json!({"op":"send","channel_id":channel_id,"content":content})
                        .to_string()
                }
                "reply" => {
                    let channel_id = arguments
                        .get("channel_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.reply missing channel_id"))?;
                    let message_id = arguments
                        .get("message_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.reply missing message_id"))?;
                    let content = arguments
                        .get("content")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.reply missing content"))?;
                    serde_json::json!({"op":"reply","channel_id":channel_id,"message_id":message_id,"content":content}).to_string()
                }
                "react" => {
                    let channel_id = arguments
                        .get("channel_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.react missing channel_id"))?;
                    let message_id = arguments
                        .get("message_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.react missing message_id"))?;
                    let emoji = arguments
                        .get("emoji")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.react missing emoji"))?;
                    serde_json::json!({"op":"react","channel_id":channel_id,"message_id":message_id,"emoji":emoji}).to_string()
                }
                "delete" => {
                    let channel_id = arguments
                        .get("channel_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.delete missing channel_id"))?;
                    let message_id = arguments
                        .get("message_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.delete missing message_id"))?;
                    serde_json::json!({"op":"delete","channel_id":channel_id,"message_id":message_id}).to_string()
                }
                "timeout_user" => {
                    let guild_id = arguments
                        .get("guild_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.timeout_user missing guild_id"))?;
                    let user_id = arguments
                        .get("user_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.timeout_user missing user_id"))?;
                    let timeout_minutes = arguments
                        .get("timeout_minutes")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(10);
                    let reason = arguments
                        .get("reason")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    serde_json::json!({"op":"timeout_user","guild_id":guild_id,"user_id":user_id,"timeout_minutes":timeout_minutes,"reason":reason}).to_string()
                }
                "kick_user" => {
                    let guild_id = arguments
                        .get("guild_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.kick_user missing guild_id"))?;
                    let user_id = arguments
                        .get("user_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("discord.kick_user missing user_id"))?;
                    let reason = arguments
                        .get("reason")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    serde_json::json!({"op":"kick_user","guild_id":guild_id,"user_id":user_id,"reason":reason}).to_string()
                }
                "" => return Err(anyhow!("discord tool call missing 'action' field")),
                other => return Err(anyhow!("Unknown discord.action: {}", other)),
            };
            Ok(("discord".into(), goal))
        }
        "x" => {
            let goal = match action.as_str() {
                "validate" => serde_json::json!({"op":"validate"}).to_string(),
                "search" => {
                    let query = arguments
                        .get("query")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("x.search missing query"))?;
                    let max_results = arguments
                        .get("max_results")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(10);
                    serde_json::json!({"op":"search","query":query,"max_results":max_results})
                        .to_string()
                }
                "user" => {
                    let username = arguments
                        .get("username")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("x.user missing username"))?;
                    serde_json::json!({"op":"user","username":username}).to_string()
                }
                "post" => {
                    let text = arguments
                        .get("text")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("x.post missing text"))?;
                    serde_json::json!({"op":"post","text":text}).to_string()
                }
                "reply" => {
                    let tweet_id = arguments
                        .get("in_reply_to_tweet_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("x.reply missing in_reply_to_tweet_id"))?;
                    let text = arguments
                        .get("text")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("x.reply missing text"))?;
                    serde_json::json!({"op":"reply","tweet_id":tweet_id,"text":text}).to_string()
                }
                "retweet" | "like" | "unlike" | "delete" => {
                    let tweet_id = arguments
                        .get("tweet_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow!("x action missing tweet_id"))?;
                    let mut base = serde_json::json!({"op":action,"tweet_id":tweet_id});
                    if let Some(user_id) = arguments.get("user_id").and_then(|v| v.as_str()) {
                        base["user_id"] = serde_json::Value::String(user_id.to_string());
                    }
                    base.to_string()
                }
                "" => return Err(anyhow!("x tool call missing 'action' field")),
                other => return Err(anyhow!("Unknown x.action: {}", other)),
            };
            Ok(("x".into(), goal))
        }
        other => {
            tracing::warn!(tool = %other, "Unregistered tool in checkpoint wrapper - blocking");
            Err(anyhow!(
                "Tool '{}' is not mapped in the checkpoint wrapper. Cannot execute.",
                other
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_wrapper_never_returns_fallback() {
        let known_tools = [
            "github", "slack", "notion", "linear", "jira", "todoist", "telegram", "discord", "x",
        ];
        for tool in &known_tools {
            let args = serde_json::json!({"action": "test_nonexistent"});
            let result = tool_call_to_job_action_and_goal(tool, &args, None);
            assert!(
                result.is_err(),
                "Tool '{}' unexpectedly accepted unknown action",
                tool
            );
        }

        let args = serde_json::json!({"action": "anything"});
        let result = tool_call_to_job_action_and_goal("brand_new_tool", &args, None);
        assert!(
            result.is_err(),
            "Unknown tool did not error - checkpoint bypass risk"
        );
    }
}
