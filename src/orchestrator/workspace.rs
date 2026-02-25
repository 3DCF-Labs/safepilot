use super::*;

impl Orchestrator {
    fn public_key_ref(path: &std::path::Path) -> String {
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("key");
        format!("key://{}", name)
    }

    pub async fn workspace_current(&self, chat_id: i64) -> String {
        let ws = match self.active_workspace(chat_id).await {
            Ok(ws) => ws,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let cfg = self
            .db
            .get_workspace_settings(&ws.workspace_id)
            .await
            .ok()
            .flatten();
        let profile = self
            .db
            .get_workspace_profile(&ws.workspace_id)
            .await
            .ok()
            .flatten();
        let now = Utc::now();
        let mode = cfg
            .as_ref()
            .map(|c| match c.security_mode {
                WorkspaceSecurityMode::Strict => "Safe".to_string(),
                WorkspaceSecurityMode::Trusted => {
                    if let Some(until) = c.mode_expires_at {
                        if until > now {
                            format!("Trusted until {}", until.to_rfc3339())
                        } else {
                            "Safe (trusted expired)".to_string()
                        }
                    } else {
                        "Trusted (persistent)".to_string()
                    }
                }
                WorkspaceSecurityMode::Unsafe => {
                    if let Some(until) = c.mode_expires_at {
                        if until > now {
                            format!("Unsafe until {}", until.to_rfc3339())
                        } else {
                            "Safe (unsafe expired)".to_string()
                        }
                    } else {
                        "Unsafe (persistent)".to_string()
                    }
                }
            })
            .unwrap_or_else(|| "Safe".to_string());
        let fetch_policy = cfg
            .as_ref()
            .map(|c| match c.fetch_mode {
                WorkspaceFetchMode::Open => "Open",
                WorkspaceFetchMode::TrustedOnly => "Trusted-only",
                WorkspaceFetchMode::TrustedPreferred => "Trusted-preferred",
            })
            .unwrap_or("Open");
        let shell_profile = cfg
            .as_ref()
            .map(|c| c.shell_pack.as_str())
            .unwrap_or("standard");
        let trusted_domains_count = cfg.as_ref().map(|c| c.trusted_domains.len()).unwrap_or(0);
        let trusted_preview = cfg
            .as_ref()
            .map(|c| {
                if c.trusted_domains.is_empty() {
                    "none".to_string()
                } else {
                    c.trusted_domains
                        .iter()
                        .take(3)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            })
            .unwrap_or_else(|| "none".to_string());
        let role = profile
            .as_ref()
            .map(|p| p.role_name.as_str())
            .unwrap_or("general");
        let skill = if profile
            .as_ref()
            .is_some_and(|p| !p.skill_prompt.trim().is_empty())
        {
            "set"
        } else {
            "not set"
        };
        format!(
            "📁 WORKSPACE STATUS\n\
━━━━━━━━━━━━━━━━━━\n\
Name: `{}`\n\
ID: `{}`\n\
Path: `{}`\n\
\n\
🎯 Role: {}\n\
📝 Skill prompt: {}\n\
🛡 Safety: {}\n\
🧰 Shell profile: {}\n\
🌐 Network: {}\n\
🔗 Trusted domains: {} ({})\n\
🔐 Encryption: {}\n\
\n\
Tip: use `/ws` to open the workspace panel and configure settings.",
            ws.name,
            Self::public_workspace_id(&ws.workspace_id),
            Self::public_workspace_path(&ws.name),
            role,
            skill,
            mode,
            shell_profile,
            fetch_policy,
            trusted_domains_count,
            trusted_preview,
            self.encryption_status_label()
        )
    }

    pub async fn workspace_list(&self, chat_id: i64) -> String {
        let _ = self.active_workspace(chat_id).await;
        let active_id = self
            .db
            .get_active_workspace_id(chat_id)
            .await
            .ok()
            .flatten();
        match self.db.list_workspaces(chat_id).await {
            Ok(list) if list.is_empty() => "No workspaces yet.".into(),
            Ok(list) => {
                let mut lines = vec!["📁 Workspaces:".to_string()];
                for ws in list {
                    let mark = if active_id.as_deref() == Some(ws.workspace_id.as_str()) {
                        " (active)"
                    } else {
                        ""
                    };
                    lines.push(format!("- `{}`{}", ws.name, mark));
                }
                lines.join("\n")
            }
            Err(err) => crate::safe_error::user_facing(&err),
        }
    }

    pub async fn workspace_create(&self, chat_id: i64, name: &str) -> String {
        match self.ensure_workspace(chat_id, name, true).await {
            Ok(ws) => format!(
                "✅ Workspace created and activated: `{}`\nPath: `{}`",
                ws.name,
                Self::public_workspace_path(&ws.name)
            ),
            Err(err) => crate::safe_error::user_facing(&err),
        }
    }

    pub async fn workspace_use(&self, chat_id: i64, name: &str) -> String {
        let clean = match Self::sanitize_workspace_name(name) {
            Some(v) => v,
            None => return "Invalid workspace name. Use [a-z0-9-_], max 32 chars.".into(),
        };
        let ws = match self.db.get_workspace_by_name(chat_id, &clean).await {
            Ok(Some(v)) => v,
            Ok(None) => return format!("Workspace not found: `{clean}`"),
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        if let Err(err) = self
            .db
            .set_active_workspace(chat_id, Some(&ws.workspace_id))
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        let _ = self.db.touch_workspace(&ws.workspace_id).await;
        let _ = self.db.set_active_run(chat_id, None).await;
        format!("✅ Switched to workspace `{}`", ws.name)
    }

    pub async fn workspace_delete(&self, chat_id: i64, name: &str) -> String {
        let clean = match Self::sanitize_workspace_name(name) {
            Some(v) => v,
            None => return "Invalid workspace name. Use [a-z0-9-_], max 32 chars.".into(),
        };
        let ws = match self.db.get_workspace_by_name(chat_id, &clean).await {
            Ok(Some(v)) => v,
            Ok(None) => return format!("Workspace not found: `{clean}`"),
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let active = self
            .db
            .get_active_workspace_id(chat_id)
            .await
            .ok()
            .flatten();
        if active.as_deref() == Some(ws.workspace_id.as_str()) {
            let list = match self.db.list_workspaces(chat_id).await {
                Ok(v) => v,
                Err(err) => return crate::safe_error::user_facing(&err),
            };
            let fallback = list
                .iter()
                .find(|w| w.workspace_id != ws.workspace_id && w.name == "default")
                .or_else(|| list.iter().find(|w| w.workspace_id != ws.workspace_id))
                .cloned();
            let Some(fallback) = fallback else {
                return "Cannot delete the only workspace. Create another workspace first.".into();
            };
            if let Err(err) = self
                .db
                .set_active_workspace(chat_id, Some(&fallback.workspace_id))
                .await
            {
                return crate::safe_error::user_facing(&err);
            }
            let _ = self.db.touch_workspace(&fallback.workspace_id).await;
            let _ = self.db.set_active_run(chat_id, None).await;
        }
        let active_jobs = match self
            .db
            .get_active_jobs_for_workspace(chat_id, &ws.workspace_id)
            .await
        {
            Ok(v) => v,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        if !active_jobs.is_empty() {
            return "Workspace has active jobs; wait or cancel them first.".into();
        }
        if ws
            .workspace_path
            .starts_with(&self.config.workspace_base_dir)
        {
            let _ = clear_directory_contents(&ws.workspace_path).await;
            let _ = tokio::fs::remove_dir(&ws.workspace_path).await;
        }
        if let Err(err) = self.db.delete_workspace(chat_id, &ws.workspace_id).await {
            return crate::safe_error::user_facing(&err);
        }
        let _ = self.db.set_active_run(chat_id, None).await;
        format!("✅ Deleted workspace `{}`", ws.name)
    }

    pub async fn workspace_config_summary(&self, chat_id: i64) -> String {
        let Ok((ws, cfg)) = self.active_workspace_settings(chat_id).await else {
            return "Could not load workspace config.".into();
        };
        let profile = self
            .active_workspace_profile(chat_id)
            .await
            .ok()
            .map(|(_, p)| p);
        let now = Utc::now();
        let mode_line = match cfg.security_mode {
            WorkspaceSecurityMode::Strict => "Safe".to_string(),
            WorkspaceSecurityMode::Trusted => {
                if let Some(until) = cfg.mode_expires_at {
                    if until > now {
                        format!("Trusted until {}", until.to_rfc3339())
                    } else {
                        "Safe (trusted expired)".to_string()
                    }
                } else {
                    "Trusted (persistent)".to_string()
                }
            }
            WorkspaceSecurityMode::Unsafe => {
                if let Some(until) = cfg.mode_expires_at {
                    if until > now {
                        format!("Unsafe until {}", until.to_rfc3339())
                    } else {
                        "Safe (unsafe expired)".to_string()
                    }
                } else {
                    "Unsafe (persistent)".to_string()
                }
            }
        };
        let fetch_mode = match cfg.fetch_mode {
            WorkspaceFetchMode::Open => "Open",
            WorkspaceFetchMode::TrustedOnly => "Trusted only",
            WorkspaceFetchMode::TrustedPreferred => "Trusted preferred",
        };
        let encryption = self.encryption_status_label();
        let domains_preview = if cfg.trusted_domains.is_empty() {
            "none".to_string()
        } else {
            cfg.trusted_domains
                .iter()
                .take(3)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        };
        let role_name = profile
            .as_ref()
            .map(|p| p.role_name.clone())
            .unwrap_or_else(|| "general".to_string());
        let custom_skill = profile
            .as_ref()
            .is_some_and(|p| !p.skill_prompt.trim().is_empty());
        let allowed_tools_count = profile.as_ref().map(|p| p.allowed_tools.len()).unwrap_or(0);
        format!(
            "⚙️ Workspace config\nWorkspace: <code>{}</code>\nRole preset: {}\nCustom skill: {}\nAllowed tools: {}\nMode: {}\nShell profile: {}\nFetch policy: {}\nTrusted domains: {} ({})\nEncryption: {}",
            ws.name,
            role_name,
            if custom_skill { "enabled" } else { "not set" },
            allowed_tools_count,
            mode_line,
            cfg.shell_pack.as_str(),
            fetch_mode,
            cfg.trusted_domains.len(),
            domains_preview,
            encryption
        )
    }

    pub async fn workspace_public_summary(&self, chat_id: i64) -> String {
        let ws = match self.active_workspace(chat_id).await {
            Ok(ws) => ws,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let _ = self
            .db
            .ensure_workspace_public_profile(&ws.workspace_id)
            .await;
        let profile = self
            .db
            .get_workspace_public_profile(&ws.workspace_id)
            .await
            .ok()
            .flatten();
        let caps = self
            .db
            .list_workspace_integration_caps(&ws.workspace_id)
            .await
            .unwrap_or_default();
        let caps_enabled = caps.iter().filter(|c| c.enabled).count();
        let writable = caps.iter().filter(|c| c.enabled && c.allow_write).count();
        let moderation = caps
            .iter()
            .filter(|c| c.enabled && c.allow_moderation)
            .count();
        let bindings = self
            .db
            .list_channel_bindings_for_chat(chat_id)
            .await
            .unwrap_or_default();
        let active_bindings = bindings
            .iter()
            .filter(|b| b.workspace_id == ws.workspace_id)
            .count();
        let scope = profile
            .as_ref()
            .and_then(|p| p.public_scope_text.as_ref())
            .map(|s| s.as_str())
            .unwrap_or("workspace-defined scope");
        let show_sources = if profile.as_ref().is_some_and(|p| p.show_sources) {
            "ON"
        } else {
            "OFF"
        };
        let sources_hint = if profile.as_ref().is_some_and(|p| p.show_sources) {
            "Replies may include short source references/links when available."
        } else {
            "Replies hide source references/links even when available."
        };
        format!(
            "🌍 <b>Public Workspace</b>\n<b>Name:</b> <code>{}</code>\n<b>Scope:</b> {}\n\n<b>Status</b>\n• Sources in replies: <b>{}</b>\n  <i>{}</i>\n• Connected targets (bindings): <b>{}</b>\n• Integration capabilities enabled: <b>{}</b>\n• Can write: <b>{}</b>\n• Can moderate: <b>{}</b>\n\n<b>What to configure</b>\n• <b>Bindings / Channel Rules</b>: which channel/account goes to this workspace and per-target rules\n• <b>Capabilities / Integration Permissions</b>: what each integration is allowed to do globally in this workspace\n\n<b>Quick path</b>\n• Connect integration\n• Configure channel rules\n• Review integration permissions",
            ws.name, scope, show_sources, sources_hint, active_bindings, caps_enabled, writable, moderation
        )
    }

    pub async fn public_scope_hint(&self, chat_id: i64) -> Option<String> {
        let ws = self.active_workspace(chat_id).await.ok()?;
        let profile = self
            .db
            .get_workspace_public_profile(&ws.workspace_id)
            .await
            .ok()
            .flatten()?;
        profile.public_scope_text
    }

    pub async fn workspace_integration_caps_summary(&self, chat_id: i64) -> String {
        let ws = match self.active_workspace(chat_id).await {
            Ok(ws) => ws,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let caps = match self
            .db
            .list_workspace_integration_caps(&ws.workspace_id)
            .await
        {
            Ok(v) => v,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let mut caps = caps;
        caps.sort_by(|a, b| a.integration.cmp(&b.integration));
        let mut lines = vec![
            format!(
                "🧰 <b>Capabilities</b>\nWorkspace: <code>{}</code>",
                ws.name
            ),
            "<pre>integration  E  R  W  M  A".to_string(),
        ];
        for c in caps {
            lines.push(format!(
                "{}  {}  {}  {}  {}  {}",
                c.integration,
                if c.enabled { "✅" } else { "❌" },
                if c.allow_read { "✅" } else { "❌" },
                if c.allow_write { "✅" } else { "❌" },
                if c.allow_moderation { "✅" } else { "❌" },
                if c.require_human_approval_for_write {
                    "✅"
                } else {
                    "❌"
                }
            ));
        }
        lines.push("</pre>".to_string());
        lines.push(
            "<b>Legend</b>: E=enabled, R=read, W=write, M=moderation, A=write-approval".to_string(),
        );
        lines.push("Capabilities are workspace-wide integration permissions.".to_string());
        lines.push("For per-channel restrictions, use Channel Rules (Bindings).".to_string());
        lines.join("\n")
    }

    pub async fn workspace_integration_cap_detail(
        &self,
        chat_id: i64,
        integration: &str,
    ) -> String {
        let ws = match self.active_workspace(chat_id).await {
            Ok(ws) => ws,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let cap = match self
            .db
            .get_workspace_integration_cap(&ws.workspace_id, integration)
            .await
        {
            Ok(Some(c)) => c,
            Ok(None) => return format!("Integration capability not found: `{}`", integration),
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        format!(
            "🛠 <b>Edit Integration</b>\nWorkspace: <code>{}</code>\nIntegration: <code>{}</code>\n\nCurrent values\n- Enabled: {}\n- Read: {}\n- Write: {}\n- Moderation: {}\n- Write approval required: {}",
            ws.name,
            cap.integration,
            if cap.enabled { "ON" } else { "OFF" },
            if cap.allow_read { "ON" } else { "OFF" },
            if cap.allow_write { "ON" } else { "OFF" },
            if cap.allow_moderation { "ON" } else { "OFF" },
            if cap.require_human_approval_for_write { "ON" } else { "OFF" }
        )
    }

    pub async fn workspace_toggle_public_sources(&self, chat_id: i64) -> String {
        let ws = match self.active_workspace(chat_id).await {
            Ok(ws) => ws,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let _ = self
            .db
            .ensure_workspace_public_profile(&ws.workspace_id)
            .await;
        let current = self
            .db
            .get_workspace_public_profile(&ws.workspace_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|p| p.show_sources);
        let next = !current;
        if let Err(err) = self
            .db
            .update_workspace_public_show_sources(&ws.workspace_id, next)
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
            "workspace_public_show_sources_toggled",
            &format!("show_sources={}", next),
        )
        .await;
        format!(
            "✅ Public profile updated: show sources is now {}.",
            if next { "enabled" } else { "disabled" }
        )
    }

    pub async fn workspace_toggle_integration_cap(
        &self,
        chat_id: i64,
        integration: &str,
        field: &str,
    ) -> String {
        let ws = match self.active_workspace(chat_id).await {
            Ok(ws) => ws,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let cap = match self
            .db
            .toggle_workspace_integration_cap_field(&ws.workspace_id, integration, field)
            .await
        {
            Ok(Some(v)) => v,
            Ok(None) => return "Unknown integration capability field.".into(),
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        self.audit_event(
            chat_id,
            Some(&ws.workspace_id),
            None,
            Some("operator"),
            Audience::Operator,
            "workspace_integration_cap_toggled",
            &format!(
                "integration={} field={} enabled={} read={} write={} moderation={} write_approval={}",
                cap.integration,
                field,
                cap.enabled,
                cap.allow_read,
                cap.allow_write,
                cap.allow_moderation,
                cap.require_human_approval_for_write
            ),
        )
        .await;
        format!(
            "✅ Updated `{}` capability.\n{}",
            cap.integration,
            self.workspace_integration_caps_summary(chat_id).await
        )
    }

    pub async fn workspace_apply_caps_template(&self, chat_id: i64, template: &str) -> String {
        let ws = match self.active_workspace(chat_id).await {
            Ok(ws) => ws,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let caps = match self
            .db
            .list_workspace_integration_caps(&ws.workspace_id)
            .await
        {
            Ok(v) => v,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let template = template.trim().to_ascii_lowercase();
        let updated = caps
            .into_iter()
            .map(|mut c| {
                match template.as_str() {
                    "support" => {
                        c.enabled = matches!(
                            c.integration.as_str(),
                            "slack" | "telegram" | "notion" | "discord" | "x"
                        );
                        c.allow_read = c.enabled;
                        c.allow_write = matches!(
                            c.integration.as_str(),
                            "slack" | "telegram" | "discord" | "x"
                        );
                        c.allow_moderation = false;
                        c.require_human_approval_for_write = true;
                    }
                    "social" => {
                        c.enabled = matches!(
                            c.integration.as_str(),
                            "discord" | "x" | "telegram" | "slack" | "notion"
                        );
                        c.allow_read = c.enabled;
                        c.allow_write = matches!(
                            c.integration.as_str(),
                            "discord" | "x" | "telegram" | "slack"
                        );
                        c.allow_moderation = false;
                        c.require_human_approval_for_write = true;
                    }
                    "moderation" => {
                        c.enabled = matches!(c.integration.as_str(), "discord" | "telegram");
                        c.allow_read = c.enabled;
                        c.allow_write = c.enabled;
                        c.allow_moderation = c.integration == "discord";
                        c.require_human_approval_for_write = true;
                    }
                    "strict_readonly" => {
                        c.enabled = true;
                        c.allow_read = true;
                        c.allow_write = false;
                        c.allow_moderation = false;
                        c.require_human_approval_for_write = true;
                    }
                    _ => {}
                }
                c
            })
            .collect::<Vec<_>>();
        if !matches!(
            template.as_str(),
            "support" | "social" | "moderation" | "strict_readonly"
        ) {
            return "Unknown template. Use: support | social | moderation | strict_readonly."
                .to_string();
        }
        for c in updated {
            if let Err(err) = self
                .db
                .upsert_workspace_integration_cap(
                    &c.workspace_id,
                    &c.integration,
                    c.enabled,
                    c.allow_read,
                    c.allow_write,
                    c.allow_moderation,
                    c.require_human_approval_for_write,
                )
                .await
            {
                return crate::safe_error::user_facing(&err);
            }
        }
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        self.audit_event(
            chat_id,
            Some(&ws.workspace_id),
            None,
            Some("operator"),
            Audience::Operator,
            "workspace_caps_template_applied",
            &format!("template={}", template),
        )
        .await;
        format!(
            "✅ Applied capability template `{}`.\n{}",
            template,
            self.workspace_integration_caps_summary(chat_id).await
        )
    }

    pub(super) fn recommended_caps_template_for_role(role_name: &str) -> &'static str {
        match role_name.trim().to_ascii_lowercase().as_str() {
            "support" => "support",
            "social" | "sales" => "social",
            "general" | "development" | "research" => "strict_readonly",
            _ => "strict_readonly",
        }
    }

    pub async fn workspace_apply_recommended_caps(&self, chat_id: i64) -> String {
        let role_name = match self.active_workspace_profile(chat_id).await {
            Ok((_, p)) => p.role_name,
            Err(_) => "general".to_string(),
        };
        let template = Self::recommended_caps_template_for_role(&role_name);
        self.workspace_apply_caps_template(chat_id, template).await
    }

    #[allow(dead_code)]
    pub async fn workspace_integration_cap_for(
        &self,
        workspace_id: &str,
        integration: &str,
    ) -> Option<WorkspaceIntegrationCapabilityRecord> {
        self.db
            .get_workspace_integration_cap(workspace_id, integration)
            .await
            .ok()
            .flatten()
    }

    pub(super) fn secret_source_status(&self, spec: &Option<crate::secrets::SecretSpec>) -> String {
        let Some(spec) = spec.as_ref() else {
            return "not configured".to_string();
        };
        let mut source = "configured".to_string();
        let from_env = spec
            .envs
            .iter()
            .any(|k| std::env::var(k).ok().is_some_and(|v| !v.trim().is_empty()));
        let from_file = spec
            .file_envs
            .iter()
            .any(|k| std::env::var(k).ok().is_some_and(|v| !v.trim().is_empty()));
        if from_file {
            source = "configured via file".to_string();
        } else if from_env {
            source = "configured via env".to_string();
        }
        let encrypted = match spec.load() {
            Ok(raw) => crate::crypto::Crypto::is_encrypted(&raw),
            Err(_) => false,
        };
        if encrypted {
            format!("{source} (encrypted literal)")
        } else {
            source
        }
    }

    pub(super) fn encryption_status_label(&self) -> String {
        if self.db.encryption_enabled() {
            "enabled".to_string()
        } else {
            "disabled".to_string()
        }
    }

    fn normalize_workspace_secret_name(name: &str) -> Option<String> {
        let normalized = name
            .trim()
            .to_ascii_uppercase()
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect::<String>();
        if normalized.is_empty() || normalized.len() > 64 {
            return None;
        }
        Some(normalized)
    }

    pub async fn workspace_tools_and_secrets_summary(&self, chat_id: i64) -> String {
        let ws = self.active_workspace(chat_id).await.ok();
        let ws_name = ws
            .as_ref()
            .map(|w| w.name.clone())
            .unwrap_or_else(|| "default".to_string());
        let secret_count = if let Some(ws) = ws.as_ref() {
            self.db
                .list_workspace_secrets(&ws.workspace_id)
                .await
                .map(|v| v.len())
                .unwrap_or(0)
        } else {
            0
        };
        let mut lines = vec![
            "🧩 Tools & secrets".to_string(),
            format!("Workspace: <code>{}</code>", ws_name),
            format!("Encryption at rest: {}", self.encryption_status_label()),
            format!("Workspace secrets: {}", secret_count),
            "".to_string(),
            format!(
                "LLM Anthropic: {}",
                self.secret_source_status(&self.config.anthropic_api)
            ),
            format!(
                "LLM OpenAI: {}",
                self.secret_source_status(&self.config.openai_api)
            ),
            format!(
                "GitHub: read={}, write={}",
                self.secret_source_status(&self.config.github_token_read),
                self.secret_source_status(&self.config.github_token_write)
            ),
            format!(
                "Slack: read={}, write={}",
                self.secret_source_status(&self.config.slack_token_read),
                self.secret_source_status(&self.config.slack_token_write)
            ),
            format!(
                "Notion: read={}, write={}",
                self.secret_source_status(&self.config.notion_token_read),
                self.secret_source_status(&self.config.notion_token_write)
            ),
            format!(
                "Linear: read={}, write={}",
                self.secret_source_status(&self.config.linear_api_read),
                self.secret_source_status(&self.config.linear_api_write)
            ),
            format!(
                "Todoist: read={}, write={}",
                self.secret_source_status(&self.config.todoist_token_read),
                self.secret_source_status(&self.config.todoist_token_write)
            ),
            format!(
                "Jira: read={}, write={}",
                self.secret_source_status(&self.config.jira_token_read),
                self.secret_source_status(&self.config.jira_token_write)
            ),
            format!(
                "Brave Search: {}",
                self.secret_source_status(&self.config.brave_api)
            ),
            format!(
                "OpenWeather: {}",
                self.secret_source_status(&self.config.openweather_api)
            ),
            format!(
                "Telegram tool token: {}",
                if self.config.telegram_token.trim().is_empty() {
                    "not configured".to_string()
                } else {
                    "configured".to_string()
                }
            ),
        ];
        if self.config.crypto.is_none() {
            lines.push("".to_string());
            lines.push(
                "Tip: use Enable Encryption to generate a master key file and restart.".to_string(),
            );
        }
        lines.join("\n")
    }

    pub async fn workspace_list_secret_names(&self, chat_id: i64) -> String {
        let Ok(ws) = self.active_workspace(chat_id).await else {
            return "Could not load workspace.".to_string();
        };
        let list = match self.db.list_workspace_secrets(&ws.workspace_id).await {
            Ok(v) => v,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        if list.is_empty() {
            return format!(
                "🔐 <b>Workspace Secrets</b>\nWorkspace: <code>{}</code>\nNo workspace-scoped secrets yet.",
                ws.name
            );
        }
        let mut lines = vec![
            "🔐 <b>Workspace Secrets</b>".to_string(),
            format!("Workspace: <code>{}</code>", ws.name),
            "".to_string(),
        ];
        for item in list {
            lines.push(format!("• <code>{}</code>", item.secret_name));
        }
        lines.join("\n")
    }

    pub async fn workspace_set_secret(&self, chat_id: i64, raw: &str) -> String {
        let Ok(ws) = self.active_workspace(chat_id).await else {
            return "Could not load workspace.".to_string();
        };
        let Some((name, value)) = raw.split_once('=') else {
            return "Invalid format. Use <code>SECRET_NAME=secret-value</code>.".to_string();
        };
        let Some(name) = Self::normalize_workspace_secret_name(name) else {
            return "Invalid secret name. Use letters/numbers/_ only, max 64 chars.".to_string();
        };
        let value = value.trim();
        if value.is_empty() {
            return "Secret value cannot be empty.".to_string();
        }
        let is_reference = value.starts_with("env:") || value.starts_with("file:");
        if !is_reference {
            return "Raw secret input in chat is disabled. Use <code>env:VAR_NAME</code> or <code>file:/absolute/path</code>.".to_string();
        }
        if let Err(err) = self
            .db
            .upsert_workspace_secret(&ws.workspace_id, &name, value)
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
            "workspace_secret_upserted",
            &format!("name={}", name),
        )
        .await;
        format!(
            "✅ Workspace secret saved: <code>{}</code>\nValue is stored encrypted at rest when encryption is enabled.",
            name
        )
    }

    pub async fn workspace_delete_secret(&self, chat_id: i64, name: &str) -> String {
        let Ok(ws) = self.active_workspace(chat_id).await else {
            return "Could not load workspace.".to_string();
        };
        let Some(name) = Self::normalize_workspace_secret_name(name) else {
            return "Invalid secret name. Use letters/numbers/_ only, max 64 chars.".to_string();
        };
        let deleted = match self
            .db
            .delete_workspace_secret(&ws.workspace_id, &name)
            .await
        {
            Ok(v) => v,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        if !deleted {
            return format!("Secret not found: <code>{}</code>", name);
        }
        self.audit_event(
            chat_id,
            Some(&ws.workspace_id),
            None,
            Some("operator"),
            Audience::Operator,
            "workspace_secret_deleted",
            &format!("name={}", name),
        )
        .await;
        format!("✅ Workspace secret deleted: <code>{}</code>", name)
    }

    pub async fn enable_encryption_with_generated_key(&self) -> String {
        if self.db.encryption_enabled() {
            return "Encryption is already enabled.".to_string();
        }
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let key_dir = std::path::PathBuf::from(home).join(".tg-orch").join("keys");
        let key_path = key_dir.join("master.key");
        let key_ref = Self::public_key_ref(&key_path);

        if key_path.exists() {
            return format!(
                "Key file already exists: <code>{}</code>.\nRestart safepilot to apply encryption key loading.",
                key_ref
            );
        }

        if let Err(err) = std::fs::create_dir_all(&key_dir) {
            return crate::safe_error::user_facing(&anyhow::anyhow!(err));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&key_dir, std::fs::Permissions::from_mode(0o700));
        }

        let mut key = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut key);
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);

        if let Err(err) = std::fs::write(&key_path, format!("{key_b64}\n")) {
            return crate::safe_error::user_facing(&anyhow::anyhow!(err));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
        }

        let new_crypto = match crate::crypto::Crypto::from_key_str(&key_b64) {
            Ok(v) => std::sync::Arc::new(v),
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        self.db.set_crypto(Some(new_crypto));
        format!(
            "✅ Generated encryption key: <code>{}</code> (mode 600).\nEncryption at rest is now active for new sensitive records.",
            key_ref
        )
    }

    pub async fn rotate_encryption_master_key(&self, chat_id: i64) -> String {
        if !self.db.encryption_enabled() {
            return "Encryption is disabled. Use Enable Encryption first.".to_string();
        }

        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let key_dir = std::path::PathBuf::from(home).join(".tg-orch").join("keys");
        if let Err(err) = std::fs::create_dir_all(&key_dir) {
            return crate::safe_error::user_facing(&anyhow::anyhow!(err));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&key_dir, std::fs::Permissions::from_mode(0o700));
        }

        let ts = Utc::now().format("%Y%m%d-%H%M%S");
        let key_path = key_dir.join(format!("master-{}.key", ts));
        let key_ref = Self::public_key_ref(&key_path);
        let mut key = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut key);
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);

        if let Err(err) = std::fs::write(&key_path, format!("{key_b64}\n")) {
            return crate::safe_error::user_facing(&anyhow::anyhow!(err));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
        }

        let new_crypto = match crate::crypto::Crypto::from_key_str(&key_b64) {
            Ok(v) => std::sync::Arc::new(v),
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let stats = match self.db.rotate_encrypted_data(new_crypto).await {
            Ok(v) => v,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let canonical_key_path = key_dir.join("master.key");
        let canonical_ref = Self::public_key_ref(&canonical_key_path);
        if let Err(err) = std::fs::write(&canonical_key_path, format!("{key_b64}\n")) {
            return crate::safe_error::user_facing(&anyhow::anyhow!(err));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(
                &canonical_key_path,
                std::fs::Permissions::from_mode(0o600),
            );
        }
        self.audit_event(
            chat_id,
            None,
            None,
            Some("operator"),
            Audience::Operator,
            "encryption_key_rotated",
            &format!(
                "updated_rows={} skipped_rows={} key_ref={}",
                stats.updated_rows, stats.skipped_rows, key_ref
            ),
        )
        .await;
        format!(
            "✅ Encryption key rotated.\nNew key file: <code>{}</code>\nCanonical key updated: <code>{}</code>\nUpdated rows: <code>{}</code>\nSkipped rows: <code>{}</code>",
            key_ref,
            canonical_ref,
            stats.updated_rows,
            stats.skipped_rows
        )
    }

    pub async fn workspace_set_security_mode(
        &self,
        chat_id: i64,
        mode: WorkspaceSecurityMode,
        minutes: Option<u64>,
    ) -> String {
        let Ok((ws, _)) = self.active_workspace_settings(chat_id).await else {
            return "Could not load workspace.".into();
        };
        let expires_at = minutes.map(|m| Utc::now() + chrono::Duration::minutes(m as i64));
        if let Err(err) = self
            .db
            .update_workspace_security_mode(&ws.workspace_id, mode, expires_at)
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        let label = match mode {
            WorkspaceSecurityMode::Strict => "Safe".to_string(),
            WorkspaceSecurityMode::Trusted => {
                if let Some(mins) = minutes {
                    format!("Trusted for {}m", mins)
                } else {
                    "Trusted (persistent)".to_string()
                }
            }
            WorkspaceSecurityMode::Unsafe => {
                if let Some(mins) = minutes {
                    format!("Unsafe for {}m", mins)
                } else {
                    "Unsafe (persistent)".to_string()
                }
            }
        };
        format!(
            "✅ Workspace `{}` mode updated: {}.\nWorkspace context/run state was reset to apply policy cleanly.",
            ws.name, label
        )
    }

    pub async fn workspace_set_shell_pack(&self, chat_id: i64, pack: WorkspaceShellPack) -> String {
        let Ok((ws, _)) = self.active_workspace_settings(chat_id).await else {
            return "Could not load workspace.".into();
        };
        if let Err(err) = self
            .db
            .update_workspace_shell_pack(&ws.workspace_id, pack)
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        format!(
            "✅ Workspace `{}` shell profile set to `{}`.\nWorkspace context/run state was reset.",
            ws.name,
            pack.as_str()
        )
    }

    pub async fn workspace_set_fetch_mode(&self, chat_id: i64, mode: WorkspaceFetchMode) -> String {
        let Ok((ws, _)) = self.active_workspace_settings(chat_id).await else {
            return "Could not load workspace.".into();
        };
        if let Err(err) = self
            .db
            .update_workspace_fetch_mode(&ws.workspace_id, mode)
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        format!(
            "✅ Workspace `{}` fetch policy set to `{}`.\nWorkspace context/run state was reset.",
            ws.name,
            mode.as_str()
        )
    }

    pub async fn workspace_add_trusted_domain(&self, chat_id: i64, domain: &str) -> String {
        let Ok((ws, mut cfg)) = self.active_workspace_settings(chat_id).await else {
            return "Could not load workspace.".into();
        };
        let raw = domain.trim();
        let mut clean = raw
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_ascii_lowercase();
        if let Some((head, _)) = clean.split_once('/') {
            clean = head.to_string();
        }
        if let Some((head, _)) = clean.split_once('?') {
            clean = head.to_string();
        }
        if let Some((head, _)) = clean.split_once('#') {
            clean = head.to_string();
        }
        if let Some((head, _)) = clean.split_once(':') {
            clean = head.to_string();
        }
        if clean.is_empty() || clean.contains('/') || clean.contains(' ') {
            return "Invalid domain. Send host only, for example: `example.com`".into();
        }
        if !cfg.trusted_domains.iter().any(|d| d == &clean) {
            cfg.trusted_domains.push(clean.clone());
            cfg.trusted_domains.sort();
        }
        if let Err(err) = self
            .db
            .update_workspace_trusted_domains(&ws.workspace_id, &cfg.trusted_domains)
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        format!(
            "✅ Added trusted domain `{}` in workspace `{}`.\nWorkspace context/run state was reset.",
            clean, ws.name
        )
    }

    pub async fn workspace_clear_trusted_domains(&self, chat_id: i64) -> String {
        let Ok((ws, _)) = self.active_workspace_settings(chat_id).await else {
            return "Could not load workspace.".into();
        };
        let empty: Vec<String> = Vec::new();
        if let Err(err) = self
            .db
            .update_workspace_trusted_domains(&ws.workspace_id, &empty)
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        format!(
            "✅ Cleared trusted domains for workspace `{}`.\nWorkspace context/run state was reset.",
            ws.name
        )
    }

    pub async fn workspace_list_trusted_domains(&self, chat_id: i64) -> String {
        let Ok((ws, cfg)) = self.active_workspace_settings(chat_id).await else {
            return "Could not load workspace.".into();
        };
        if cfg.trusted_domains.is_empty() {
            return format!("No trusted domains configured in workspace `{}`.", ws.name);
        }
        let mut lines = vec![format!("Trusted domains in `{}`:", ws.name)];
        for d in cfg.trusted_domains {
            lines.push(format!("- {}", d));
        }
        lines.join("\n")
    }

    pub async fn workspace_remove_trusted_domain(&self, chat_id: i64, domain: &str) -> String {
        let Ok((ws, mut cfg)) = self.active_workspace_settings(chat_id).await else {
            return "Could not load workspace.".into();
        };
        let clean = domain
            .trim()
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_ascii_lowercase();
        if clean.is_empty() {
            return "Invalid domain.".into();
        }
        let before = cfg.trusted_domains.len();
        cfg.trusted_domains.retain(|d| d != &clean);
        if cfg.trusted_domains.len() == before {
            return format!("Domain `{}` was not in trusted list.", clean);
        }
        if let Err(err) = self
            .db
            .update_workspace_trusted_domains(&ws.workspace_id, &cfg.trusted_domains)
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        format!(
            "✅ Removed trusted domain `{}` from `{}`.\nWorkspace context/run state was reset.",
            clean, ws.name
        )
    }

    pub async fn workspace_profile_summary(&self, chat_id: i64) -> String {
        let Ok((ws, profile)) = self.active_workspace_profile(chat_id).await else {
            return "Could not load workspace profile.".into();
        };
        let mut lines = vec![
            "🎯 Workspace role & skill".to_string(),
            format!("Workspace: <code>{}</code>", ws.name),
            format!("Role preset: {}", profile.role_name),
            format!(
                "Custom skill: {}",
                if profile.skill_prompt.trim().is_empty() {
                    "not set"
                } else {
                    "enabled"
                }
            ),
        ];
        if profile.allowed_tools.is_empty() {
            lines.push("Allowed tools: inherited defaults".to_string());
        } else {
            lines.push(format!(
                "Allowed tools: {}",
                profile.allowed_tools.join(", ")
            ));
        }
        lines.push(
            "Note: allowed tools define what the planner may use. Integration access is configured separately in Public Runtime -> Capabilities.".to_string(),
        );
        if !profile.skill_prompt.trim().is_empty() {
            lines.push("".to_string());
            lines.push(format!(
                "Skill prompt:\n{}",
                truncate_str(profile.skill_prompt.trim(), 1400)
            ));
        }
        lines.join("\n")
    }

    pub async fn workspace_set_role_preset(&self, chat_id: i64, role: &str) -> String {
        let Ok((ws, existing)) = self.active_workspace_profile(chat_id).await else {
            return "Could not load workspace profile.".into();
        };
        let preset = Self::profile_for_role(role);
        if let Err(err) = self
            .db
            .update_workspace_profile_role_and_tools(
                &ws.workspace_id,
                &preset.role_name,
                &preset.allowed_tools,
            )
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        if !existing.skill_prompt.trim().is_empty() {
            let _ = self
                .db
                .update_workspace_profile_skill_prompt(&ws.workspace_id, &existing.skill_prompt)
                .await;
        }
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        format!(
            "✅ Workspace `{}` role set to `{}`.\nAllowed tools updated to match this preset.\nWorkspace context/run state was reset.",
            ws.name, preset.role_name
        )
    }

    pub async fn workspace_set_skill_prompt(&self, chat_id: i64, skill_prompt: &str) -> String {
        let Ok((ws, _)) = self.active_workspace_profile(chat_id).await else {
            return "Could not load workspace profile.".into();
        };
        let trimmed = skill_prompt.trim();
        if trimmed.is_empty() {
            return "Skill prompt cannot be empty.".to_string();
        }
        if let Err(err) = self
            .db
            .update_workspace_profile_skill_prompt(&ws.workspace_id, trimmed)
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        format!(
            "✅ Saved custom skill prompt for workspace `{}` ({} chars).\nWorkspace context/run state was reset.",
            ws.name,
            trimmed.len()
        )
    }

    pub async fn workspace_clear_skill_prompt(&self, chat_id: i64) -> String {
        let Ok((ws, _)) = self.active_workspace_profile(chat_id).await else {
            return "Could not load workspace profile.".into();
        };
        if let Err(err) = self
            .db
            .update_workspace_profile_skill_prompt(&ws.workspace_id, "")
            .await
        {
            return crate::safe_error::user_facing(&err);
        }
        self.reset_active_workspace_state_after_config_change(chat_id)
            .await;
        format!(
            "✅ Cleared custom skill prompt in workspace `{}`.\nWorkspace context/run state was reset.",
            ws.name
        )
    }
}
