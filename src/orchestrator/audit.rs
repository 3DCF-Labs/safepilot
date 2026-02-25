use super::*;

impl Orchestrator {
    #[allow(clippy::too_many_arguments)]
    pub async fn audit_event(
        &self,
        chat_id: i64,
        workspace_id: Option<&str>,
        principal_id: Option<&str>,
        role: Option<&str>,
        audience: Audience,
        event_type: &str,
        details: &str,
    ) {
        if let Err(err) = self
            .db
            .insert_audit_event(
                chat_id,
                workspace_id,
                principal_id,
                role,
                audience.as_str(),
                event_type,
                details,
            )
            .await
        {
            tracing::warn!(error = %err, "Failed to persist audit event");
        }
    }

    pub async fn audit_recent(&self, chat_id: i64, limit: usize) -> String {
        let active_ws = self
            .db
            .get_active_workspace_id(chat_id)
            .await
            .ok()
            .flatten();
        let rows = match self
            .db
            .list_audit_events(chat_id, active_ws.as_deref(), limit)
            .await
        {
            Ok(v) => v,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        if rows.is_empty() {
            return "No audit events yet.".into();
        }
        let mut lines = vec!["📜 Audit events (latest first)".to_string()];
        for row in rows {
            lines.push(format!(
                "- [{}] {} [{}:{}] {}",
                row.created_at.to_rfc3339(),
                row.event_type,
                row.audience,
                row.role.unwrap_or_else(|| "-".to_string()),
                truncate_str(&row.details, 180)
            ));
        }
        lines.join("\n")
    }

    pub async fn audit_filtered(&self, chat_id: i64, query: &str) -> String {
        let mut limit: usize = 30;
        let mut audience_filter: Option<String> = None;
        let mut event_filter: Option<String> = None;
        let mut principal_filter: Option<String> = None;
        let mut contains_filter: Option<String> = None;
        let mut workspace_filter: Option<String> = None;
        for token in query.split_whitespace() {
            let Some((k, v)) = token.split_once('=') else {
                continue;
            };
            match k.to_ascii_lowercase().as_str() {
                "limit" => {
                    if let Ok(n) = v.parse::<usize>() {
                        limit = n.clamp(1, 200);
                    }
                }
                "audience" => audience_filter = Some(v.to_ascii_lowercase()),
                "event" => event_filter = Some(v.to_ascii_lowercase()),
                "principal" => principal_filter = Some(v.to_ascii_lowercase()),
                "contains" => contains_filter = Some(v.to_ascii_lowercase()),
                "workspace" => workspace_filter = Some(v.to_string()),
                _ => {}
            }
        }
        let ws_id_filter = if let Some(wf) = workspace_filter {
            if wf.eq_ignore_ascii_case("active") {
                self.db
                    .get_active_workspace_id(chat_id)
                    .await
                    .ok()
                    .flatten()
            } else if let Ok(Some(ws)) = self.workspace_by_name_or_id(chat_id, &wf).await {
                Some(ws.workspace_id)
            } else {
                Some(wf)
            }
        } else {
            None
        };
        let rows = match self
            .db
            .list_audit_events(chat_id, ws_id_filter.as_deref(), 200)
            .await
        {
            Ok(v) => v,
            Err(err) => return crate::safe_error::user_facing(&err),
        };
        let mut filtered = rows
            .into_iter()
            .filter(|r| {
                audience_filter
                    .as_ref()
                    .is_none_or(|f| r.audience.eq_ignore_ascii_case(f))
            })
            .filter(|r| {
                event_filter
                    .as_ref()
                    .is_none_or(|f| r.event_type.to_ascii_lowercase().contains(f))
            })
            .filter(|r| {
                principal_filter.as_ref().is_none_or(|f| {
                    r.principal_id
                        .as_ref()
                        .is_some_and(|p| p.to_ascii_lowercase().contains(f))
                })
            })
            .filter(|r| {
                contains_filter.as_ref().is_none_or(|f| {
                    r.details.to_ascii_lowercase().contains(f)
                        || r.event_type.to_ascii_lowercase().contains(f)
                })
            })
            .take(limit)
            .collect::<Vec<_>>();
        if filtered.is_empty() {
            return "📜 <b>Audit</b>\nNo events matched this filter.\nTip: try /auditf audience=public limit=50".into();
        }
        filtered.sort_by_key(|r| std::cmp::Reverse(r.id));
        let mut lines = vec![format!(
            "📜 Audit events filtered (showing {})",
            filtered.len()
        )];
        for row in filtered {
            lines.push(format!(
                "- [{}] {} [{}:{}] {}",
                row.created_at.to_rfc3339(),
                row.event_type,
                row.audience,
                row.role.unwrap_or_else(|| "-".to_string()),
                truncate_str(&row.details, 180)
            ));
        }
        lines.join("\n")
    }

    pub async fn audit_export(&self, chat_id: i64, query: &str) -> String {
        let body = self.audit_filtered(chat_id, query).await;
        if body.starts_with("No audit events") || body.starts_with("Could not") {
            return body;
        }
        format!(
            "📦 Audit export (text)\nUse filter args like `audience=public event=policy limit=100`.\n\n{}",
            body
        )
    }
}
