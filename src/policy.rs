use crate::db::RiskTier;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceAccess {
    ReadOnly,
    Write,
}

pub fn classify_job_action(action_type: &str, goal: &str) -> RiskTier {
    let action = action_type.trim().to_ascii_lowercase();
    let goal = goal.trim();

    match action.as_str() {
        "agent" | "codex" | "claude" => RiskTier::NeedsApproval,

        "shell" | "validate" | "merge" => RiskTier::Dangerous,

        "git" => RiskTier::Safe,

        "search" | "fetch" | "weather" => RiskTier::Safe,

        "github" => classify_goal_prefix(
            goal,
            &["issues", "prs", "ci", "search"],
            &["create-issue", "comment"],
        ),
        "slack" => classify_goal_prefix(
            goal,
            &["channels", "history", "search"],
            &["send", "blocks", "react"],
        ),
        "jira" => classify_goal_prefix(
            goal,
            &["search", "get", "projects", "boards"],
            &["create", "comment", "transition", "assign"],
        ),
        "linear" => classify_goal_prefix(goal, &["teams", "issues", "search"], &["create"]),
        "notion" => classify_goal_prefix(
            goal,
            &["search", "query", "get"],
            &["create", "update", "append"],
        ),
        "todoist" => {
            classify_goal_prefix(goal, &["list", "projects"], &["add", "complete", "delete"])
        }
        "telegram" => classify_goal_prefix(
            goal,
            &["info"],
            &["send", "forward", "edit", "delete", "pin", "unpin"],
        ),
        "discord" => classify_goal_prefix(
            goal,
            &["channels", "history"],
            &[
                "send",
                "reply",
                "react",
                "delete",
                "timeout_user",
                "kick_user",
            ],
        ),
        "x" => classify_goal_prefix(
            goal,
            &["search", "user", "lookup"],
            &["post", "reply", "retweet", "like", "delete"],
        ),

        _ => RiskTier::NeedsApproval,
    }
}

pub fn workspace_access(action_type: &str, _goal: &str) -> WorkspaceAccess {
    let action = action_type.trim().to_ascii_lowercase();
    match action.as_str() {
        "git" | "codex" | "claude" | "shell" | "validate" | "merge" => WorkspaceAccess::Write,
        _ => WorkspaceAccess::ReadOnly,
    }
}

fn classify_goal_prefix(goal: &str, safe: &[&str], needs_approval: &[&str]) -> RiskTier {
    let prefix = goal_prefix(goal);

    if safe.iter().any(|s| s.eq_ignore_ascii_case(&prefix)) {
        return RiskTier::Safe;
    }
    if needs_approval
        .iter()
        .any(|s| s.eq_ignore_ascii_case(&prefix))
    {
        return RiskTier::NeedsApproval;
    }

    RiskTier::NeedsApproval
}

fn goal_prefix(goal: &str) -> String {
    let g = goal.trim();
    if g.starts_with('{') {
        if let Ok(v) = serde_json::from_str::<Value>(g) {
            if let Some(op) = v.get("op").and_then(|v| v.as_str()) {
                let op = op.trim();
                if !op.is_empty() {
                    return op.to_ascii_lowercase();
                }
            }
        }
    }
    g.split('|')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}
