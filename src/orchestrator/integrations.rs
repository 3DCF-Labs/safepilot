use super::*;

impl Orchestrator {
    pub(super) fn parse_channel_binding_target(raw: &str) -> Option<(String, String)> {
        let input = raw.trim();
        let (integration, channel_id) = input.split_once(':')?;
        let integration = integration.trim().to_ascii_lowercase();
        let channel_id = channel_id.trim().to_string();
        if integration.is_empty() || channel_id.is_empty() {
            return None;
        }
        Some((integration, channel_id))
    }

    pub(super) async fn workspace_by_name_or_id(
        &self,
        chat_id: i64,
        ident: &str,
    ) -> Result<Option<WorkspaceRecord>> {
        if let Some(clean) = Self::sanitize_workspace_name(ident) {
            if let Some(ws) = self.db.get_workspace_by_name(chat_id, &clean).await? {
                return Ok(Some(ws));
            }
        }
        if let Some(ws) = self.db.get_workspace_by_id(ident).await? {
            if ws.chat_id == chat_id {
                return Ok(Some(ws));
            }
        }
        Ok(None)
    }

    pub async fn bind_channel_to_workspace(
        &self,
        chat_id: i64,
        binding: &str,
        workspace: &str,
    ) -> String {
        let (integration, channel_id) = match Self::parse_channel_binding_target(binding) {
            Some(v) => v,
            None => return "Invalid binding. Use `<integration>:<channel_id>`.".into(),
        };
        let ws = match self.workspace_by_name_or_id(chat_id, workspace).await {
            Ok(Some(ws)) => ws,
            Ok(None) => return format!("Workspace not found: `{workspace}`"),
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        if let Err(err) = self
            .db
            .upsert_channel_binding(&integration, &channel_id, &ws.workspace_id, "public_skill")
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.audit_event(
            chat_id,
            Some(&ws.workspace_id),
            None,
            Some("operator"),
            Audience::Operator,
            "channel_binding_upserted",
            &format!(
                "integration={} channel_id={} mode=public_skill workspace={}",
                integration, channel_id, ws.name
            ),
        )
        .await;
        format!(
            "✅ Bound `{integration}:{channel_id}` to workspace `{}` (mode: public_skill).",
            ws.name
        )
    }

    async fn workspace_secret_token(
        &self,
        workspace_id: Option<&str>,
        names: &[&str],
    ) -> Option<String> {
        let ws = workspace_id?.trim();
        if ws.is_empty() {
            return None;
        }
        for name in names {
            match self.db.get_workspace_secret_value(ws, name).await {
                Ok(Some(raw)) => match crate::secrets::resolve_secret_reference_or_literal(
                    &raw,
                    self.config.crypto.as_deref(),
                ) {
                    Ok(v) => return Some(v),
                    Err(err) => {
                        tracing::warn!(
                            workspace_id = ws,
                            secret_name = %name,
                            error = %err,
                            "Failed to resolve workspace secret for integration token"
                        );
                    }
                },
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(
                        workspace_id = ws,
                        secret_name = %name,
                        error = %err,
                        "Failed to read workspace secret for integration token"
                    );
                }
            }
        }
        None
    }

    pub(super) async fn validate_integration_token(
        &self,
        integration: &str,
        workspace_id: Option<&str>,
    ) -> Result<String> {
        match integration {
            "telegram" => {
                let token = self
                    .workspace_secret_token(workspace_id, &["TELEGRAM_BOT_TOKEN", "BOT_TOKEN"])
                    .await
                    .unwrap_or_else(|| self.config.telegram_token.clone());
                let client = crate::tools::telegram::TelegramClient::new(&token);
                let me = client.get_me().await?;
                let uname = me.username.unwrap_or_else(|| "unknown".to_string());
                Ok(format!(
                    "Telegram token OK for bot `{}` (id={}).",
                    uname, me.id
                ))
            }
            "slack" => {
                let token = if let Some(v) = self
                    .workspace_secret_token(
                        workspace_id,
                        &["SLACK_BOT_TOKEN_READ", "SLACK_BOT_TOKEN"],
                    )
                    .await
                {
                    v
                } else {
                    let spec =
                        self.config.slack_token_read.as_ref().ok_or_else(|| {
                            anyhow::anyhow!("SLACK_BOT_TOKEN_READ not configured")
                        })?;
                    spec.load_with_crypto(self.config.crypto.as_deref())?
                };
                let client = crate::tools::slack::SlackClient::new(&token);
                let _ = client.list_channels(1).await?;
                Ok("Slack token OK.".to_string())
            }
            "notion" => {
                let token = if let Some(v) = self
                    .workspace_secret_token(
                        workspace_id,
                        &["NOTION_API_KEY_READ", "NOTION_API_KEY"],
                    )
                    .await
                {
                    v
                } else {
                    let spec = self
                        .config
                        .notion_token_read
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("NOTION_API_KEY_READ not configured"))?;
                    spec.load_with_crypto(self.config.crypto.as_deref())?
                };
                let client = crate::tools::notion::NotionClient::new(&token);
                let _ = client.search("healthcheck", None).await?;
                Ok("Notion token OK.".to_string())
            }
            "github" => {
                let token = if let Some(v) = self
                    .workspace_secret_token(workspace_id, &["GITHUB_TOKEN_READ", "GITHUB_TOKEN"])
                    .await
                {
                    v
                } else {
                    let spec = self
                        .config
                        .github_token_read
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("GITHUB_TOKEN_READ not configured"))?;
                    spec.load_with_crypto(self.config.crypto.as_deref())?
                };
                let client = reqwest::Client::new();
                let resp = client
                    .get("https://api.github.com/user")
                    .header("Authorization", format!("Bearer {}", token))
                    .header("User-Agent", "safepilot")
                    .header("Accept", "application/vnd.github+json")
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("GitHub token validation failed ({status}): {body}");
                }
                let body: serde_json::Value = resp.json().await?;
                let login = body
                    .get("login")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                Ok(format!("GitHub token OK for `{}`.", login))
            }
            "linear" => {
                let token = if let Some(v) = self
                    .workspace_secret_token(
                        workspace_id,
                        &["LINEAR_API_KEY_READ", "LINEAR_API_KEY"],
                    )
                    .await
                {
                    v
                } else {
                    let spec = self
                        .config
                        .linear_api_read
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("LINEAR_API_KEY_READ not configured"))?;
                    spec.load_with_crypto(self.config.crypto.as_deref())?
                };
                let client = crate::tools::linear::LinearClient::new(&token);
                let _ = client.list_teams().await?;
                Ok("Linear token OK.".to_string())
            }
            "todoist" => {
                let token = if let Some(v) = self
                    .workspace_secret_token(
                        workspace_id,
                        &["TODOIST_API_KEY_READ", "TODOIST_API_KEY"],
                    )
                    .await
                {
                    v
                } else {
                    let spec =
                        self.config.todoist_token_read.as_ref().ok_or_else(|| {
                            anyhow::anyhow!("TODOIST_API_KEY_READ not configured")
                        })?;
                    spec.load_with_crypto(self.config.crypto.as_deref())?
                };
                let client = crate::tools::todoist::TodoistClient::new(&token);
                let _ = client.list_projects().await?;
                Ok("Todoist token OK.".to_string())
            }
            "jira" => {
                let domain = self
                    .config
                    .jira_domain
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("JIRA_DOMAIN not configured"))?;
                let email = self
                    .config
                    .jira_email
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("JIRA_EMAIL not configured"))?;
                let token = if let Some(v) = self
                    .workspace_secret_token(
                        workspace_id,
                        &["JIRA_API_TOKEN_READ", "JIRA_API_TOKEN"],
                    )
                    .await
                {
                    v
                } else {
                    let spec = self
                        .config
                        .jira_token_read
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("JIRA_TOKEN_READ not configured"))?;
                    spec.load_with_crypto(self.config.crypto.as_deref())?
                };
                let client = crate::tools::jira::JiraClient::new(domain, email, &token);
                let _ = client.list_projects().await?;
                Ok("Jira token OK.".to_string())
            }
            "discord" => {
                let token = if let Some(v) = self
                    .workspace_secret_token(
                        workspace_id,
                        &["DISCORD_BOT_TOKEN_READ", "DISCORD_BOT_TOKEN"],
                    )
                    .await
                {
                    v
                } else {
                    let spec =
                        self.config.discord_token_read.as_ref().ok_or_else(|| {
                            anyhow::anyhow!("DISCORD_BOT_TOKEN_READ not configured")
                        })?;
                    spec.load_with_crypto(self.config.crypto.as_deref())?
                };
                let client = reqwest::Client::new();
                let resp = client
                    .get("https://discord.com/api/v10/users/@me")
                    .header("Authorization", format!("Bot {}", token))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("Discord token validation failed ({status}): {body}");
                }
                let body: serde_json::Value = resp.json().await?;
                let id = body.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                let username = body
                    .get("username")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                Ok(format!("Discord token OK for bot `{username}` (id={id})."))
            }
            "x" => {
                let token = if let Some(v) = self
                    .workspace_secret_token(
                        workspace_id,
                        &["X_API_BEARER_TOKEN_READ", "X_API_BEARER_TOKEN"],
                    )
                    .await
                {
                    v
                } else {
                    let spec =
                        self.config.x_api_token_read.as_ref().ok_or_else(|| {
                            anyhow::anyhow!("X_API_BEARER_TOKEN_READ not configured")
                        })?;
                    spec.load_with_crypto(self.config.crypto.as_deref())?
                };
                let client = reqwest::Client::new();
                let resp = client
                    .get("https://api.x.com/2/users/me")
                    .header("Authorization", format!("Bearer {}", token))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("X token validation failed ({status}): {body}");
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
                Ok(format!("X token OK for `{name}` (@{username}, id={id})."))
            }
            other => anyhow::bail!("Unsupported integration for validation: {other}"),
        }
    }

    pub(super) fn known_integrations() -> [&'static str; 9] {
        [
            "telegram", "discord", "x", "slack", "github", "jira", "linear", "notion", "todoist",
        ]
    }

    pub(super) fn yes_no(v: bool) -> &'static str {
        if v {
            "yes"
        } else {
            "no"
        }
    }

    pub(super) async fn verify_integration_target_access(
        &self,
        integration: &str,
        target_id: &str,
        workspace_id: Option<&str>,
    ) -> Result<String> {
        let target = target_id.trim();
        if target.is_empty() {
            anyhow::bail!("empty target id");
        }
        let resolve_token = |opt: Option<String>,
                             spec: Option<&crate::secrets::SecretSpec>,
                             err_name: &str|
         -> Result<String> {
            if let Some(v) = opt {
                return Ok(v);
            }
            let spec = spec.ok_or_else(|| anyhow::anyhow!("{err_name} not configured"))?;
            spec.load_with_crypto(self.config.crypto.as_deref())
        };
        match integration {
            "telegram" => {
                let token = self
                    .workspace_secret_token(workspace_id, &["TELEGRAM_BOT_TOKEN", "BOT_TOKEN"])
                    .await
                    .unwrap_or_else(|| self.config.telegram_token.clone());
                let line = self.verify_telegram_bot_access(target, &token).await;
                if line.starts_with('✅') {
                    Ok(line)
                } else {
                    anyhow::bail!("{line}")
                }
            }
            "discord" => {
                let token = resolve_token(
                    self.workspace_secret_token(
                        workspace_id,
                        &["DISCORD_BOT_TOKEN_READ", "DISCORD_BOT_TOKEN"],
                    )
                    .await,
                    self.config.discord_token_read.as_ref(),
                    "DISCORD_BOT_TOKEN_READ",
                )?;
                let resp = reqwest::Client::new()
                    .get(format!("https://discord.com/api/v10/channels/{target}"))
                    .header("Authorization", format!("Bot {}", token))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("channel access failed ({status}): {body}");
                }
                Ok("target channel reachable".to_string())
            }
            "x" => {
                let token = resolve_token(
                    self.workspace_secret_token(
                        workspace_id,
                        &["X_API_BEARER_TOKEN_READ", "X_API_BEARER_TOKEN"],
                    )
                    .await,
                    self.config.x_api_token_read.as_ref(),
                    "X_API_BEARER_TOKEN_READ",
                )?;
                let client = reqwest::Client::new();
                let mut endpoint = format!("https://api.x.com/2/users/{target}");
                if target.starts_with('@') || !target.chars().all(|c| c.is_ascii_digit()) {
                    endpoint = format!(
                        "https://api.x.com/2/users/by/username/{}",
                        target.trim_start_matches('@')
                    );
                }
                let resp = client
                    .get(endpoint)
                    .header("Authorization", format!("Bearer {}", token))
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("account access failed ({status}): {body}");
                }
                Ok("target account reachable".to_string())
            }
            "slack" => {
                let token = resolve_token(
                    self.workspace_secret_token(
                        workspace_id,
                        &["SLACK_BOT_TOKEN_READ", "SLACK_BOT_TOKEN"],
                    )
                    .await,
                    self.config.slack_token_read.as_ref(),
                    "SLACK_BOT_TOKEN_READ",
                )?;
                let resp = reqwest::Client::new()
                    .get("https://slack.com/api/conversations.info")
                    .bearer_auth(token)
                    .query(&[("channel", target)])
                    .send()
                    .await?;
                let body: serde_json::Value = resp.json().await?;
                let ok = body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                if !ok {
                    let reason = body
                        .get("error")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    anyhow::bail!("channel access failed: {reason}");
                }
                Ok("target channel reachable".to_string())
            }
            "github" => {
                let token = resolve_token(
                    self.workspace_secret_token(
                        workspace_id,
                        &["GITHUB_TOKEN_READ", "GITHUB_TOKEN"],
                    )
                    .await,
                    self.config.github_token_read.as_ref(),
                    "GITHUB_TOKEN_READ",
                )?;
                let client = reqwest::Client::new();
                if target.contains('/') {
                    let resp = client
                        .get(format!("https://api.github.com/repos/{target}"))
                        .header("Authorization", format!("Bearer {}", token))
                        .header("User-Agent", "safepilot")
                        .header("Accept", "application/vnd.github+json")
                        .send()
                        .await?;
                    if !resp.status().is_success() {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        anyhow::bail!("repo access failed ({status}): {body}");
                    }
                    return Ok("target repo reachable".to_string());
                }
                let mut ok = false;
                for url in [
                    format!("https://api.github.com/users/{target}"),
                    format!("https://api.github.com/orgs/{target}"),
                ] {
                    let resp = client
                        .get(url)
                        .header("Authorization", format!("Bearer {}", token))
                        .header("User-Agent", "safepilot")
                        .header("Accept", "application/vnd.github+json")
                        .send()
                        .await?;
                    if resp.status().is_success() {
                        ok = true;
                        break;
                    }
                }
                if !ok {
                    anyhow::bail!("target owner/org not reachable");
                }
                Ok("target owner/org reachable".to_string())
            }
            "jira" => {
                let domain = self
                    .config
                    .jira_domain
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("JIRA_DOMAIN not configured"))?;
                let email = self
                    .config
                    .jira_email
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("JIRA_EMAIL not configured"))?;
                let token = resolve_token(
                    self.workspace_secret_token(
                        workspace_id,
                        &["JIRA_API_TOKEN_READ", "JIRA_API_TOKEN"],
                    )
                    .await,
                    self.config.jira_token_read.as_ref(),
                    "JIRA_TOKEN_READ",
                )?;
                let resp = reqwest::Client::new()
                    .get(format!(
                        "https://{}/rest/api/3/project/{}",
                        domain.trim_end_matches('/'),
                        target
                    ))
                    .basic_auth(email, Some(token))
                    .header("Accept", "application/json")
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("project access failed ({status}): {body}");
                }
                Ok("target project reachable".to_string())
            }
            "linear" => {
                let token = resolve_token(
                    self.workspace_secret_token(
                        workspace_id,
                        &["LINEAR_API_KEY_READ", "LINEAR_API_KEY"],
                    )
                    .await,
                    self.config.linear_api_read.as_ref(),
                    "LINEAR_API_KEY_READ",
                )?;
                let teams = crate::tools::linear::LinearClient::new(&token)
                    .list_teams()
                    .await?;
                let found = teams.iter().any(|t| {
                    t.id.eq_ignore_ascii_case(target)
                        || t.key.eq_ignore_ascii_case(target)
                        || t.name.eq_ignore_ascii_case(target)
                });
                if !found {
                    anyhow::bail!("team/workspace id not found in accessible teams");
                }
                Ok("target team/workspace reachable".to_string())
            }
            "notion" => {
                let token = resolve_token(
                    self.workspace_secret_token(
                        workspace_id,
                        &["NOTION_API_KEY_READ", "NOTION_API_KEY"],
                    )
                    .await,
                    self.config.notion_token_read.as_ref(),
                    "NOTION_API_KEY_READ",
                )?;
                let client = reqwest::Client::new();
                let headers = |req: reqwest::RequestBuilder| {
                    req.bearer_auth(&token)
                        .header("Notion-Version", "2022-06-28")
                        .header("Content-Type", "application/json")
                };
                let page = headers(client.get(format!("https://api.notion.com/v1/pages/{target}")))
                    .send()
                    .await?;
                if page.status().is_success() {
                    return Ok("target page reachable".to_string());
                }
                let db =
                    headers(client.get(format!("https://api.notion.com/v1/databases/{target}")))
                        .send()
                        .await?;
                if db.status().is_success() {
                    return Ok("target database reachable".to_string());
                }
                anyhow::bail!("page/database not reachable with current token")
            }
            "todoist" => {
                let token = resolve_token(
                    self.workspace_secret_token(
                        workspace_id,
                        &["TODOIST_API_KEY_READ", "TODOIST_API_KEY"],
                    )
                    .await,
                    self.config.todoist_token_read.as_ref(),
                    "TODOIST_API_KEY_READ",
                )?;
                let _ = crate::tools::todoist::TodoistClient::new(&token)
                    .get_project(target)
                    .await?;
                Ok("target project reachable".to_string())
            }
            other => anyhow::bail!("no readiness checker for integration `{other}`"),
        }
    }

    pub async fn integration_readiness_report(&self, chat_id: i64, scope: &str) -> String {
        let ws = match self.active_workspace(chat_id).await {
            Ok(v) => v,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let known = Self::known_integrations();
        let requested = scope.trim().to_ascii_lowercase();
        let integrations: Vec<&str> = if requested.is_empty() || requested == "all" {
            known.to_vec()
        } else if known.iter().any(|i| *i == requested) {
            vec![known
                .iter()
                .find(|i| **i == requested)
                .copied()
                .unwrap_or("telegram")]
        } else {
            return format!(
                "Unknown integration `{}`. Use one of: {}",
                requested,
                known.join(", ")
            );
        };
        let caps = self
            .db
            .list_workspace_integration_caps(&ws.workspace_id)
            .await
            .unwrap_or_default();
        let mut caps_map: HashMap<String, WorkspaceIntegrationCapabilityRecord> = HashMap::new();
        for c in caps {
            caps_map.insert(c.integration.clone(), c);
        }
        let bindings = self
            .db
            .list_channel_bindings_for_chat(chat_id)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|b| b.workspace_id == ws.workspace_id)
            .collect::<Vec<_>>();

        let mut lines = vec![
            "🧪 Integration Readiness".to_string(),
            format!("Workspace: {}", ws.name),
            format!(
                "Scope: {}",
                if requested.is_empty() {
                    "all".to_string()
                } else {
                    requested.clone()
                }
            ),
            "".to_string(),
        ];

        for integration in integrations {
            lines.push(format!("[{integration}]"));
            if let Some(cap) = caps_map.get(integration) {
                lines.push(format!(
                    "capability: enabled={} read={} write={} moderation={} write_approval={}",
                    Self::yes_no(cap.enabled),
                    Self::yes_no(cap.allow_read),
                    Self::yes_no(cap.allow_write),
                    Self::yes_no(cap.allow_moderation),
                    Self::yes_no(cap.require_human_approval_for_write),
                ));
            } else {
                lines.push("capability: missing row in workspace_integration_caps".to_string());
            }

            match self
                .validate_integration_token(integration, Some(&ws.workspace_id))
                .await
            {
                Ok(msg) => lines.push(format!("token: ok ({msg})")),
                Err(err) => lines.push(format!(
                    "token: fail ({})",
                    crate::safe_error::user_facing(&err)
                )),
            }

            let rows = bindings
                .iter()
                .filter(|b| b.integration == integration)
                .collect::<Vec<_>>();
            if rows.is_empty() {
                lines.push("bindings: none".to_string());
                lines.push(String::new());
                continue;
            }
            lines.push(format!("bindings: {}", rows.len()));
            for row in rows {
                match self
                    .verify_integration_target_access(
                        integration,
                        &row.channel_id,
                        Some(&row.workspace_id),
                    )
                    .await
                {
                    Ok(msg) => lines.push(format!("  - {}: ok ({msg})", row.channel_id)),
                    Err(err) => lines.push(format!(
                        "  - {}: fail ({})",
                        row.channel_id,
                        crate::safe_error::user_facing(&err)
                    )),
                }
            }
            lines.push(String::new());
        }

        lines.join("\n")
    }

    pub(super) async fn resolve_telegram_target(
        &self,
        raw_target: &str,
    ) -> Result<ResolvedTelegramTarget> {
        let target = raw_target.trim();
        if target.is_empty() {
            anyhow::bail!("Telegram target is empty");
        }
        if let Ok(id) = target.parse::<i64>() {
            return Ok(ResolvedTelegramTarget {
                chat_id: id.to_string(),
                display_name: None,
            });
        }
        if target.contains(' ') {
            anyhow::bail!("Invalid Telegram target. Use numeric chat_id or @channelusername");
        }
        let chat_ref = if target.starts_with('@') {
            target.to_string()
        } else {
            format!("@{target}")
        };
        let url = format!(
            "https://api.telegram.org/bot{}/getChat",
            self.config.telegram_token
        );
        let client = reqwest::Client::new();
        let resp = client
            .get(url)
            .query(&[("chat_id", chat_ref)])
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Telegram getChat failed ({status}): {body}");
        }
        let body: serde_json::Value = resp.json().await?;
        let ok = body.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
        if !ok {
            anyhow::bail!(
                "Telegram target lookup failed. For private groups/chats use numeric chat_id."
            );
        }
        let id = body
            .get("result")
            .and_then(|v| v.get("id"))
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow::anyhow!("Telegram response missing chat id"))?;
        let display_name = body
            .get("result")
            .and_then(|v| v.get("username"))
            .and_then(|v| v.as_str())
            .map(|u| format!("@{u}"))
            .or_else(|| {
                body.get("result")
                    .and_then(|v| v.get("title"))
                    .and_then(|v| v.as_str())
                    .map(|t| t.to_string())
            });
        Ok(ResolvedTelegramTarget {
            chat_id: id.to_string(),
            display_name,
        })
    }

    pub(super) async fn verify_telegram_bot_access(&self, chat_id: &str, token: &str) -> String {
        let client = crate::tools::telegram::TelegramClient::new(token);
        let me = match client.get_me().await {
            Ok(v) => v,
            Err(err) => {
                return format!(
                    "⚠️ Could not verify bot membership in target chat: {}",
                    crate::safe_error::user_facing(&err)
                );
            }
        };
        let chat_kind = client
            .get_chat(chat_id)
            .await
            .ok()
            .map(|c| c.chat_type.to_ascii_lowercase());
        match client.get_chat_member(chat_id, me.id).await {
            Ok(member) => {
                let status = member.status.to_ascii_lowercase();
                if matches!(status.as_str(), "left" | "kicked") {
                    "⚠️ Bot is not a member of this chat/channel yet. Add it first, then test."
                        .to_string()
                } else {
                    let mut line =
                        format!("✅ Bot membership verified in target (status: {}).", member.status);
                    if matches!(chat_kind.as_deref(), Some("group") | Some("supergroup")) {
                        line.push_str(
                            " Tip: if bot does not reply to normal group messages, disable Privacy Mode in BotFather (/setprivacy -> Disable) or mention the bot.",
                        );
                    }
                    line
                }
            }
            Err(err) => format!(
                "⚠️ Binding saved, but membership check failed: {}. Ensure bot is added to target and has permissions.",
                crate::safe_error::user_facing(&err)
            ),
        }
    }

    pub async fn connect_integration_help(&self, chat_id: i64, integration: &str) -> String {
        let ws = match self.active_workspace(chat_id).await {
            Ok(v) => v,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let token_status = match self
            .validate_integration_token(integration, Some(&ws.workspace_id))
            .await
        {
            Ok(msg) => format!("✅ {}", msg),
            Err(err) => format!("❌ {}", crate::safe_error::user_facing(&err)),
        };
        match integration {
            "telegram" => format!(
                "🔌 Connect Telegram\n\n1) Ensure TELEGRAM_BOT_TOKEN is configured.\n2) Add bot to target channel/group as member/subscriber.\n3) Promote bot to Admin in that target.\n4) Bind target with chat_id or @username:\n`/connecttelegram <chat_id_or_@username> {}`\n\n{}",
                ws.name, token_status
            ),
            "slack" => format!(
                "🔌 Connect Slack\n\n1) Add env token: SLACK_BOT_TOKEN_READ (+ optional WRITE).\n2) Invite bot to target channel.\n3) Bind target with:\n`/connect slack <channel_id> {}`\n\n{}",
                ws.name, token_status
            ),
            "notion" => format!(
                "🔌 Connect Notion\n\n1) Add env token: NOTION_API_KEY_READ (+ optional WRITE).\n2) Share target DB/page with integration.\n3) Bind target with:\n`/connect notion <database_or_page_id> {}`\n\n{}",
                ws.name, token_status
            ),
            "github" => format!(
                "🔌 Connect GitHub\n\n1) Add env token: GITHUB_TOKEN_READ (+ optional WRITE).\n2) Ensure repo/org permissions.\n3) Bind target with:\n`/connect github <owner_repo_or_scope> {}`\n\n{}",
                ws.name, token_status
            ),
            "linear" => format!(
                "🔌 Connect Linear\n\n1) Add env token: LINEAR_API_KEY_READ (+ optional WRITE).\n2) Ensure workspace/team access.\n3) Bind target with:\n`/connect linear <team_or_workspace_id> {}`\n\n{}",
                ws.name, token_status
            ),
            "todoist" => format!(
                "🔌 Connect Todoist\n\n1) Add env token: TODOIST_API_KEY_READ (+ optional WRITE).\n2) Choose target project/account.\n3) Bind target with:\n`/connect todoist <project_or_scope> {}`\n\n{}",
                ws.name, token_status
            ),
            "jira" => format!(
                "🔌 Connect Jira\n\n1) Add JIRA_DOMAIN, JIRA_EMAIL, JIRA_TOKEN_READ (+ optional WRITE).\n2) Ensure project permissions.\n3) Bind target with:\n`/connect jira <project_or_scope> {}`\n\n{}",
                ws.name, token_status
            ),
            "discord" => format!(
                "🔌 Connect Discord\n\n1) Add env token: DISCORD_BOT_TOKEN_READ (+ optional DISCORD_BOT_TOKEN_WRITE)\n2) Invite bot to your server/channels.\n3) Bind target with:\n`/connectdiscord <channel_id> {}`\n\n{}",
                ws.name, token_status
            ),
            "x" => format!(
                "🔌 Connect X\n\n1) Add env token: X_API_BEARER_TOKEN_READ (+ optional X_API_BEARER_TOKEN_WRITE)\n2) Ensure token has required scopes.\n3) Bind account target with:\n`/connectx <account_id> {}`\n\n{}",
                ws.name, token_status
            ),
            _ => "Unsupported integration.".to_string(),
        }
    }

    pub async fn connect_integration_binding(
        &self,
        chat_id: i64,
        integration: &str,
        target_id: &str,
        workspace: &str,
    ) -> String {
        let integration = integration.trim().to_ascii_lowercase();
        let ws = match self.workspace_by_name_or_id(chat_id, workspace).await {
            Ok(Some(ws)) => ws,
            Ok(None) => return format!("Workspace not found: `{workspace}`"),
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let telegram_runtime_token = if integration == "telegram" {
            self.workspace_secret_token(
                Some(&ws.workspace_id),
                &["TELEGRAM_BOT_TOKEN", "BOT_TOKEN"],
            )
            .await
            .unwrap_or_else(|| self.config.telegram_token.clone())
        } else {
            String::new()
        };
        let token_msg = match self
            .validate_integration_token(&integration, Some(&ws.workspace_id))
            .await
        {
            Ok(v) => format!("✅ {}", v),
            Err(err) => {
                return format!(
                    "❌ Token validation failed for `{}`: {}",
                    integration,
                    crate::safe_error::user_facing(&err)
                )
            }
        };
        let (resolved_target_id, telegram_target_name) = if integration == "telegram" {
            match self.resolve_telegram_target(target_id).await {
                Ok(v) => (v.chat_id, v.display_name),
                Err(err) => {
                    return format!(
                        "❌ Telegram target resolution failed: {}",
                        crate::safe_error::user_facing(&err)
                    );
                }
            }
        } else {
            (target_id.trim().to_string(), None)
        };
        let bind_msg = self
            .bind_channel_to_workspace(
                chat_id,
                &format!("{}:{}", integration, resolved_target_id),
                &ws.name,
            )
            .await;
        if bind_msg.starts_with('✅') {
            let write_policy = match integration.as_str() {
                "discord" | "x" => "approval_required",
                _ => "workspace_default",
            };
            let _ = self
                .db
                .update_channel_binding_policy(
                    &integration,
                    &resolved_target_id,
                    write_policy,
                    &[],
                    None,
                    Some(r#"{"preset":"default"}"#),
                )
                .await;
        }
        if integration == "telegram" {
            let verify = self
                .verify_telegram_bot_access(&resolved_target_id, &telegram_runtime_token)
                .await;
            let target_name = telegram_target_name
                .as_ref()
                .map(|n| format!(" ({})", n))
                .unwrap_or_default();
            format!(
                "{bind_msg}\n{token_msg}\nResolved chat_id: `{}`{}\n{}",
                resolved_target_id, target_name, verify
            )
        } else {
            format!("{bind_msg}\n{token_msg}")
        }
    }

    pub async fn unbind_channel(&self, chat_id: i64, binding: &str) -> String {
        let (integration, channel_id) = match Self::parse_channel_binding_target(binding) {
            Some(v) => v,
            None => return "Invalid binding. Use `<integration>:<channel_id>`.".into(),
        };
        let bound_ws = self
            .db
            .get_channel_binding(&integration, &channel_id)
            .await
            .ok()
            .flatten()
            .map(|b| b.workspace_id);
        if let Err(err) = self
            .db
            .delete_channel_binding(&integration, &channel_id)
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.audit_event(
            chat_id,
            bound_ws.as_deref(),
            None,
            Some("operator"),
            Audience::Operator,
            "channel_binding_deleted",
            &format!("integration={} channel_id={}", integration, channel_id),
        )
        .await;
        format!("✅ Removed binding `{integration}:{channel_id}`.")
    }

    pub async fn list_channel_bindings(&self, chat_id: i64) -> String {
        match self.db.list_channel_bindings_for_chat(chat_id).await {
            Ok(rows) if rows.is_empty() => {
                "🔗 <b>Connected Targets</b>\nNo channels/accounts are connected yet.".into()
            }
            Ok(rows) => {
                let mut lines = vec![
                    "🔗 <b>Connected Targets</b>".to_string(),
                    "Each row is: integration + target -> workspace".to_string(),
                ];
                for row in rows {
                    let ws_name = self
                        .db
                        .get_workspace_by_id(&row.workspace_id)
                        .await
                        .ok()
                        .flatten()
                        .map(|w| w.name)
                        .unwrap_or_else(|| "unknown".to_string());
                    lines.push(format!(
                        "• <code>{}:{}</code> → <code>{}</code> ({})\n  write policy: <code>{}</code>\n  allowed actions: <code>{}</code>{}",
                        row.integration,
                        row.channel_id,
                        ws_name,
                        row.mode,
                        row.write_policy,
                        if row.allowed_actions.is_empty() {
                            "any".to_string()
                        } else {
                            row.allowed_actions.join(",")
                        },
                        row.fallback_workspace_id
                            .as_ref()
                            .map(|f| format!("\n  fallback workspace: <code>{}</code>", Self::public_workspace_id(f)))
                            .unwrap_or_default()
                    ));
                }
                lines.join("\n")
            }
            Err(err) => crate::safe_error::user_facing(&err),
        }
    }

    pub async fn chat_audience(&self, chat_id: i64) -> Audience {
        let binding = self
            .db
            .get_channel_binding("telegram", &chat_id.to_string())
            .await
            .ok()
            .flatten();
        if binding.as_ref().is_some_and(|b| b.mode == "public_skill") {
            Audience::Public
        } else {
            Audience::Operator
        }
    }

    pub async fn binding_policy_summary(&self, chat_id: i64, binding: &str) -> String {
        let (integration, channel_id) = match Self::parse_channel_binding_target(binding) {
            Some(v) => v,
            None => return "Invalid binding. Use `<integration>:<channel_id>`.".into(),
        };
        let Some(row) = self
            .db
            .get_channel_binding(&integration, &channel_id)
            .await
            .ok()
            .flatten()
        else {
            return format!("Binding not found: `{integration}:{channel_id}`");
        };
        let ws_name = self
            .db
            .get_workspace_by_id(&row.workspace_id)
            .await
            .ok()
            .flatten()
            .filter(|w| w.chat_id == chat_id)
            .map(|w| w.name)
            .unwrap_or_else(|| "unknown".to_string());
        let fallback = row
            .fallback_workspace_id
            .as_ref()
            .map(|f| Self::public_workspace_id(f))
            .unwrap_or_else(|| "none".to_string());
        let write_human = match row.write_policy.as_str() {
            "workspace_default" => "Use workspace default",
            "read_only" => "Read-only (block writes)",
            "approval_required" => "Write needs approval",
            _ => row.write_policy.as_str(),
        };
        let actions_human = if row.allowed_actions.is_empty() {
            "Any action allowed".to_string()
        } else {
            row.allowed_actions.join(", ")
        };
        format!(
            "🔐 <b>Channel Rules</b>\nTarget: <code>{}:{}</code>\nWorkspace: <code>{}</code>\n\n<b>Current rules</b>\n• Write mode: <b>{}</b>\n• Allowed actions: <code>{}</code>\n• Fallback workspace: <code>{}</code>\n\n<b>How this works</b>\n• Channel Rules are per target (this specific channel/account)\n• Integration Permissions are workspace-wide defaults\n• Per-target rules can only restrict further, never bypass workspace safety",
            row.integration, row.channel_id, ws_name, write_human, actions_human, fallback
        )
    }

    pub async fn update_binding_policy(
        &self,
        chat_id: i64,
        binding: &str,
        write_policy: &str,
        allowed_actions_raw: &str,
        fallback_workspace: Option<&str>,
    ) -> String {
        let (integration, channel_id) = match Self::parse_channel_binding_target(binding) {
            Some(v) => v,
            None => return "Invalid binding. Use `<integration>:<channel_id>`.".into(),
        };
        let Some(existing) = self
            .db
            .get_channel_binding(&integration, &channel_id)
            .await
            .ok()
            .flatten()
        else {
            return format!("Binding not found: `{integration}:{channel_id}`");
        };
        let policy = write_policy.trim().to_ascii_lowercase();
        if !matches!(
            policy.as_str(),
            "workspace_default" | "read_only" | "approval_required"
        ) {
            return "Invalid write policy. Use: workspace_default | read_only | approval_required."
                .to_string();
        }
        let allowed_actions = if allowed_actions_raw.trim() == "*" {
            Vec::new()
        } else {
            let mut vals = allowed_actions_raw
                .split(',')
                .map(Self::normalize_action_type)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>();
            vals.sort();
            vals.dedup();
            vals
        };
        let fallback_workspace_id = if let Some(ident) = fallback_workspace {
            if ident.trim().is_empty() || ident.trim() == "-" {
                None
            } else {
                match self.workspace_by_name_or_id(chat_id, ident.trim()).await {
                    Ok(Some(ws)) => Some(ws.workspace_id),
                    Ok(None) => return format!("Fallback workspace not found: `{}`", ident.trim()),
                    Err(err) => return crate::safe_error::user_facing(&err),
                }
            }
        } else {
            existing.fallback_workspace_id.clone()
        };
        let metadata = format!(
            r#"{{"updated_via":"bindpolicy","updated_at":"{}"}}"#,
            Utc::now().to_rfc3339()
        );
        if let Err(err) = self
            .db
            .update_channel_binding_policy(
                &integration,
                &channel_id,
                &policy,
                &allowed_actions,
                fallback_workspace_id.as_deref(),
                Some(&metadata),
            )
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.audit_event(
            chat_id,
            Some(&existing.workspace_id),
            None,
            Some("operator"),
            Audience::Operator,
            "channel_binding_policy_updated",
            &format!(
                "binding={}:{} write_policy={} allowed_actions={} fallback={}",
                integration,
                channel_id,
                policy,
                if allowed_actions.is_empty() {
                    "*".to_string()
                } else {
                    allowed_actions.join(",")
                },
                fallback_workspace_id.unwrap_or_default()
            ),
        )
        .await;
        self.binding_policy_summary(chat_id, binding).await
    }

    pub async fn apply_binding_policy_preset(
        &self,
        chat_id: i64,
        binding: &str,
        preset: &str,
    ) -> String {
        let (write_policy, actions): (&str, Vec<String>) =
            match preset.trim().to_ascii_lowercase().as_str() {
                "search_only" => (
                    "read_only",
                    vec![
                        "search".to_string(),
                        "fetch".to_string(),
                        "agent".to_string(),
                    ],
                ),
                "social_posting" => (
                    "approval_required",
                    vec![
                        "search".to_string(),
                        "fetch".to_string(),
                        "agent".to_string(),
                        "telegram".to_string(),
                        "discord".to_string(),
                        "x".to_string(),
                        "slack".to_string(),
                    ],
                ),
                "moderation" => ("approval_required", vec!["discord".to_string()]),
                _ => {
                    return "Unknown preset. Use: search_only | social_posting | moderation.".into()
                }
            };
        self.update_binding_policy(chat_id, binding, write_policy, &actions.join(","), None)
            .await
    }

    pub async fn route_public_chat_workspace(
        &self,
        chat_id: i64,
        integration: &str,
        channel_id: &str,
    ) {
        let Ok(Some(binding)) = self.db.get_channel_binding(integration, channel_id).await else {
            return;
        };
        let mut target_workspace_id = binding.workspace_id.clone();
        let target_exists = self
            .db
            .get_workspace_by_id(&target_workspace_id)
            .await
            .ok()
            .flatten()
            .is_some();
        if !target_exists {
            if let Some(fallback) = binding.fallback_workspace_id.as_ref() {
                let fallback_exists = self
                    .db
                    .get_workspace_by_id(fallback)
                    .await
                    .ok()
                    .flatten()
                    .is_some();
                if fallback_exists {
                    target_workspace_id = fallback.clone();
                }
            }
        }
        let current = self
            .db
            .get_active_workspace_id(chat_id)
            .await
            .ok()
            .flatten();
        if current.as_deref() != Some(target_workspace_id.as_str()) {
            let _ = self
                .db
                .set_active_workspace(chat_id, Some(&target_workspace_id))
                .await;
            let _ = self.db.set_active_run(chat_id, None).await;
        }
    }
}
