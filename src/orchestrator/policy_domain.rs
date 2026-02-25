use super::*;

impl Orchestrator {
    pub(super) fn workspace_role_presets() -> [(&'static str, &'static [&'static str]); 6] {
        [
            (
                "general",
                &[
                    "agent",
                    "git",
                    "fetch",
                    "search",
                    "list_files",
                    "read_file",
                    "shell",
                    "validate",
                    "codex",
                    "claude",
                    "merge",
                    "slack",
                    "telegram",
                    "notion",
                    "github",
                    "linear",
                    "todoist",
                    "jira",
                    "discord",
                    "x",
                    "weather",
                ],
            ),
            (
                "development",
                &[
                    "agent",
                    "git",
                    "list_files",
                    "read_file",
                    "shell",
                    "validate",
                    "codex",
                    "claude",
                    "merge",
                    "fetch",
                    "search",
                    "github",
                ],
            ),
            (
                "research",
                &[
                    "agent",
                    "search",
                    "fetch",
                    "read_file",
                    "list_files",
                    "notion",
                    "weather",
                ],
            ),
            (
                "social",
                &[
                    "agent", "search", "fetch", "telegram", "slack", "notion", "todoist", "linear",
                    "discord", "x",
                ],
            ),
            (
                "support",
                &[
                    "agent", "search", "fetch", "telegram", "slack", "notion", "discord", "x",
                ],
            ),
            (
                "sales",
                &[
                    "agent", "search", "fetch", "telegram", "slack", "notion", "discord", "x",
                    "github", "linear", "todoist",
                ],
            ),
        ]
    }

    pub(super) fn profile_for_role(role: &str) -> WorkspaceProfileRecord {
        let clean = role.trim().to_ascii_lowercase();
        let (selected_role, selected_tools) = Self::workspace_role_presets()
            .iter()
            .find(|(name, _)| *name == clean)
            .copied()
            .unwrap_or(Self::workspace_role_presets()[0]);
        let mut allowed_tools = selected_tools
            .iter()
            .map(|t| (*t).to_string())
            .collect::<Vec<_>>();
        allowed_tools.sort();
        allowed_tools.dedup();
        WorkspaceProfileRecord {
            workspace_id: String::new(),
            role_name: selected_role.to_string(),
            skill_prompt: String::new(),
            allowed_tools,
            updated_at: Utc::now(),
        }
    }

    pub(super) fn normalize_action_type(action_type: &str) -> String {
        action_type.trim().to_ascii_lowercase()
    }

    pub(super) fn is_action_allowed(action_type: &str, profile: &WorkspaceProfileRecord) -> bool {
        if profile.allowed_tools.is_empty() {
            return true;
        }
        let normalized = Self::normalize_action_type(action_type);
        profile.allowed_tools.iter().any(|t| t == &normalized)
    }

    pub(super) fn role_allows_local_workspace_access(role_name: &str) -> bool {
        matches!(role_name, "general" | "development")
    }

    pub(super) fn goal_targets_local_workspace(goal: &str) -> bool {
        let g = goal.trim().to_ascii_lowercase();
        g.contains("file://")
            || g.contains("/users/")
            || g.contains("/home/")
            || g.contains("/var/")
            || g.contains("\\\\")
            || g.contains("workspace")
            || g.contains("local file")
            || g.contains("locally")
            || g.contains("list files")
            || g.contains("read file")
            || g.contains("run script")
    }

    pub(super) fn user_request_targets_local_workspace(text: &str) -> bool {
        Self::goal_targets_local_workspace(text)
    }

    pub(super) fn integration_from_action(action_type: &str) -> Option<&'static str> {
        match action_type {
            "slack" => Some("slack"),
            "notion" => Some("notion"),
            "github" => Some("github"),
            "linear" => Some("linear"),
            "telegram" => Some("telegram"),
            "discord" => Some("discord"),
            "x" => Some("x"),
            "todoist" => Some("todoist"),
            "jira" => Some("jira"),
            _ => None,
        }
    }

    pub(super) fn goal_requests_integration_write(goal: &str) -> bool {
        let l = goal.to_ascii_lowercase();
        let markers = [
            "create ", "post ", "send ", "publish ", "update ", "edit ", "delete ", "remove ",
            "close ", "assign ", "comment ", "reply ", "write ", "retweet", "like ", "react ",
            "timeout ", "kick ", "ban ",
        ];
        markers.iter().any(|m| l.contains(m))
    }

    pub(super) fn action_requests_write(action_type: &str, goal: &str) -> bool {
        let normalized = Self::normalize_action_type(action_type);
        if matches!(
            normalized.as_str(),
            "git" | "codex" | "claude" | "shell" | "validate" | "merge"
        ) {
            return true;
        }
        if Self::integration_from_action(&normalized).is_some() {
            return Self::goal_requests_integration_write(goal);
        }
        false
    }

    pub(super) async fn integration_policy_rejection_reason(
        &self,
        workspace_id: &str,
        action_type: &str,
        goal: &str,
        task_id: Option<&str>,
    ) -> Option<String> {
        let integration = Self::integration_from_action(action_type)?;
        let cap = self
            .db
            .get_workspace_integration_cap(workspace_id, integration)
            .await
            .ok()
            .flatten()?;
        if !cap.enabled {
            return Some(format!(
                "integration `{}` is disabled in workspace policy",
                integration
            ));
        }
        if !cap.allow_read {
            return Some(format!(
                "integration `{}` read access is disabled in workspace policy",
                integration
            ));
        }
        let wants_write = Self::goal_requests_integration_write(goal);
        if wants_write && !cap.allow_write {
            return Some(format!(
                "integration `{}` write access is disabled in workspace policy",
                integration
            ));
        }
        if wants_write && cap.require_human_approval_for_write {
            let approved = if let Some(task_id) = task_id {
                self.db
                    .get_approval_for_task(task_id)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|a| a.status == ApprovalStatus::Approved)
            } else {
                false
            };
            if !approved {
                return Some(format!(
                    "integration `{}` write requires explicit operator approval",
                    integration
                ));
            }
        }
        None
    }

    pub(super) async fn channel_binding_policy_rejection_reason(
        &self,
        chat_id: i64,
        action_type: &str,
        goal: &str,
        task_id: Option<&str>,
    ) -> Option<String> {
        let binding = self
            .db
            .get_channel_binding("telegram", &chat_id.to_string())
            .await
            .ok()
            .flatten()?;
        if binding.mode != "public_skill" {
            return None;
        }
        let normalized_action = Self::normalize_action_type(action_type);
        if !binding.allowed_actions.is_empty()
            && !binding
                .allowed_actions
                .iter()
                .any(|a| a == &normalized_action)
        {
            return Some(format!(
                "binding policy blocks action `{}` (allowed: {})",
                normalized_action,
                binding.allowed_actions.join(", ")
            ));
        }

        let write_intent = Self::action_requests_write(&normalized_action, goal);
        match binding.write_policy.as_str() {
            "read_only" if write_intent => {
                return Some("binding policy is read-only for this channel".to_string());
            }
            "approval_required" if write_intent => {
                let approved = if let Some(task_id) = task_id {
                    self.db
                        .get_approval_for_task(task_id)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|a| a.status == ApprovalStatus::Approved)
                } else {
                    false
                };
                if !approved {
                    return Some(
                        "binding policy requires explicit approval for write actions".to_string(),
                    );
                }
            }
            _ => {}
        }
        None
    }

    pub(super) fn extract_hosts_from_text(text: &str) -> Vec<String> {
        let mut hosts = Vec::new();
        for raw in text.split_whitespace() {
            let token = raw.trim_matches(|c: char| {
                c.is_whitespace()
                    || matches!(
                        c,
                        '"' | '\'' | ',' | ';' | ')' | '(' | ']' | '[' | '>' | '<'
                    )
            });
            if !(token.starts_with("http://") || token.starts_with("https://")) {
                continue;
            }
            if let Ok(url) = url::Url::parse(token) {
                if let Some(host) = url.host_str() {
                    hosts.push(host.to_ascii_lowercase());
                }
            }
        }
        hosts.sort();
        hosts.dedup();
        hosts
    }

    fn request_likely_needs_network(text: &str) -> bool {
        let lower = text.to_ascii_lowercase();
        let keywords = [
            "http://",
            "https://",
            "www.",
            "search",
            "google",
            "bing",
            "duckduckgo",
            "web",
            "website",
            "site",
            "online",
            "browse",
            "find on",
            "look up",
            "fetch",
            "open url",
            "check this",
        ];
        keywords.iter().any(|k| lower.contains(k))
    }

    pub(super) fn host_matches_trusted(host: &str, trusted_domains: &[String]) -> bool {
        trusted_domains.iter().any(|d| {
            let domain = d.trim().to_ascii_lowercase();
            if domain.is_empty() {
                return false;
            }
            host == domain || host.ends_with(&format!(".{domain}"))
        })
    }

    pub(super) fn profile_policy_rejection_reason(
        action_type: &str,
        goal: &str,
        profile: &WorkspaceProfileRecord,
    ) -> Option<String> {
        let normalized = Self::normalize_action_type(action_type);
        if !Self::is_action_allowed(&normalized, profile) {
            return Some(format!(
                "blocked by workspace role `{}` (action `{}` is not allowed)",
                profile.role_name, normalized
            ));
        }
        if normalized == "agent"
            && !Self::role_allows_local_workspace_access(&profile.role_name)
            && Self::goal_targets_local_workspace(goal)
        {
            return Some(format!(
                "blocked by workspace role `{}` (local workspace access is disabled)",
                profile.role_name
            ));
        }
        None
    }

    pub(super) async fn precheck_user_request_policy(
        &self,
        chat_id: i64,
        text: &str,
    ) -> Option<String> {
        let (_, profile) = self.active_workspace_profile(chat_id).await.ok()?;
        let (_, cfg) = self.active_workspace_settings(chat_id).await.ok()?;
        if Self::user_request_targets_local_workspace(text)
            && !Self::role_allows_local_workspace_access(&profile.role_name)
        {
            let mut msg = format!(
                "🛡 Workspace role `{}` blocks local workspace/file actions.",
                profile.role_name
            );
            msg.push_str("\nI will not access local files/scripts in this workspace.");
            if !cfg.trusted_domains.is_empty() {
                msg.push_str(&format!(
                    "\nTrusted domains configured: {}",
                    cfg.trusted_domains.join(", ")
                ));
                msg.push_str(
                    "\nAsk me to search/fetch from those sources, or switch role/workspace in `/ws`.",
                );
            } else {
                msg.push_str("\nAsk me to search/fetch online, or switch role/workspace in `/ws`.");
            }
            return Some(msg);
        }
        if cfg.fetch_mode == WorkspaceFetchMode::TrustedOnly {
            let hosts = Self::extract_hosts_from_text(text);
            let network_intent = Self::request_likely_needs_network(text);
            let trusted = if cfg.trusted_domains.is_empty() {
                "none".to_string()
            } else {
                cfg.trusted_domains.join(", ")
            };

            if network_intent && cfg.trusted_domains.is_empty() {
                let mut msg = "🌐 Network policy is `trusted_only` for this workspace.".to_string();
                msg.push_str("\nTrusted domains: none");
                msg.push_str("\nNo trusted sources are configured yet, so web access is blocked.");
                msg.push_str(
                    "\nUse `/wsconfig` to add a trusted domain, then request that source explicitly.",
                );
                return Some(msg);
            }

            if network_intent && hosts.is_empty() && !cfg.trusted_domains.is_empty() {
                let mut msg = "🌐 Network policy is `trusted_only` for this workspace.".to_string();
                msg.push_str(&format!("\nTrusted domains: {}", trusted));
                msg.push_str(
                    "\nPlease include a URL from a trusted domain (or ask to fetch that specific source).",
                );
                return Some(msg);
            }

            if !hosts.is_empty() {
                let blocked_hosts: Vec<String> = hosts
                    .into_iter()
                    .filter(|h| !Self::host_matches_trusted(h, &cfg.trusted_domains))
                    .collect();
                if !blocked_hosts.is_empty() {
                    let mut msg =
                        "🌐 Network policy is `trusted_only` for this workspace.".to_string();
                    msg.push_str(&format!(
                        "\nBlocked URL host(s): {}",
                        blocked_hosts.join(", ")
                    ));
                    msg.push_str(&format!("\nTrusted domains: {}", trusted));
                    msg.push_str(
                        "\nUse `/wsconfig` to add the domain, change network policy, or request a trusted source.",
                    );
                    return Some(msg);
                }
            }
        }
        None
    }

    pub(super) async fn active_workspace_profile(
        &self,
        chat_id: i64,
    ) -> Result<(WorkspaceRecord, WorkspaceProfileRecord)> {
        let ws = self.active_workspace(chat_id).await?;
        self.db.ensure_workspace_profile(&ws.workspace_id).await?;
        let mut profile = self
            .db
            .get_workspace_profile(&ws.workspace_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Workspace profile missing"))?;
        if profile.allowed_tools.is_empty() {
            let preset = Self::profile_for_role(&profile.role_name);
            self.db
                .update_workspace_profile_role_and_tools(
                    &ws.workspace_id,
                    &preset.role_name,
                    &preset.allowed_tools,
                )
                .await?;
            profile = self
                .db
                .get_workspace_profile(&ws.workspace_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("Workspace profile missing after init"))?;
        }
        Ok((ws, profile))
    }

    pub fn classify_user_error(raw: &str) -> UserErrorClass {
        let l = raw.to_ascii_lowercase();
        if l.contains("network policy is `trusted_only`")
            || l.contains("blocked url host")
            || l.contains("trusted domains:")
            || l.contains("trusted_only")
        {
            return UserErrorClass::PolicyBlockDomain;
        }
        if l.contains("local workspace/file actions")
            || l.contains("local workspace access is disabled")
            || l.contains("i will not access local files")
        {
            return UserErrorClass::PolicyBlockLocal;
        }
        if l.contains("blocked by workspace role")
            || l.contains("blocked by workspace policy")
            || l.contains("is not allowed")
        {
            return UserErrorClass::PolicyBlockAction;
        }
        if l.contains("not available in public mode") || l.contains("permission denied") {
            return UserErrorClass::AuthzBlock;
        }
        if l.contains("operation failed")
            || l.contains("internal error")
            || l.contains("failed to ")
            || l.contains("exception")
            || l.contains("timed out")
            || l.contains("timeout")
        {
            return UserErrorClass::InternalError;
        }
        UserErrorClass::Unknown
    }

    pub fn map_message_for_audience(
        &self,
        raw: &str,
        audience: Audience,
        workspace_scope_hint: Option<&str>,
    ) -> String {
        if audience == Audience::Operator {
            return raw.to_string();
        }
        let class = Self::classify_user_error(raw);
        let scope = workspace_scope_hint
            .filter(|s| !s.trim().is_empty())
            .unwrap_or("the configured scope");
        match class {
            UserErrorClass::Unknown => raw.to_string(),
            UserErrorClass::PolicyBlockDomain => {
                "I can only use approved sources for this assistant.".to_string()
            }
            UserErrorClass::PolicyBlockLocal | UserErrorClass::PolicyBlockAction => {
                format!("I can only help with {scope}.")
            }
            UserErrorClass::AuthzBlock => {
                "This action is not available in this channel.".to_string()
            }
            UserErrorClass::InternalError => {
                "I can’t complete that request right now. Please try again.".to_string()
            }
        }
    }
}
