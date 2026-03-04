use super::*;

pub(super) async fn should_offer_quick_setup(orchestrator: &Orchestrator, chat_id: i64) -> bool {
    let messages = orchestrator
        .db
        .count_active_messages(chat_id)
        .await
        .unwrap_or(0);
    if messages > 0 {
        return false;
    }
    let workspaces = orchestrator
        .db
        .list_workspaces(chat_id)
        .await
        .unwrap_or_default();
    workspaces.len() <= 1
}

pub(super) async fn handle_message(
    bot: Bot,
    msg: Message,
    orchestrator: Arc<Orchestrator>,
) -> ResponseResult<()> {
    let user_id_opt = msg.from().map(|u| u.id.0 as i64);
    let user_id = user_id_opt.unwrap_or(0);
    let text_raw = msg.text().unwrap_or("").trim();
    let first_token = text_raw.split_whitespace().next().unwrap_or("");
    let first_token_lc = first_token.to_ascii_lowercase();
    let command_name = first_token_lc
        .strip_prefix('/')
        .and_then(|cmd| cmd.split('@').next())
        .unwrap_or("");
    let role = if let Some(uid) = user_id_opt {
        orchestrator.resolve_telegram_role(uid).await
    } else if msg.chat.is_private() && msg.chat.id.0 == orchestrator.config.allowed_user_id {
        AccessRole::Owner
    } else {
        AccessRole::Public
    };

    let mut text = match msg.text() {
        Some(text) if !text.trim().is_empty() => text.trim().to_string(),
        _ => return Ok(()),
    };

    let chat_id = msg.chat.id;
    let is_operator = Orchestrator::is_operator_role(role);
    tracing::info!(user_id, len = text.len(), "Incoming message");
    tracing::debug!(user_id, text = %text, "Incoming message (full)");

    // Allow /whereami in senderless channel/group posts as a read-only bootstrap helper.
    if !is_operator && command_name == "whereami" && user_id_opt.is_none() && !msg.chat.is_private()
    {
        let chat_kind = if msg.chat.is_group() {
            "group"
        } else if msg.chat.is_supergroup() {
            "supergroup"
        } else if msg.chat.is_channel() {
            "channel"
        } else {
            "unknown"
        };
        let title = msg.chat.title().unwrap_or("n/a");
        let username = msg
            .chat
            .username()
            .map(|u| format!("@{}", u))
            .unwrap_or_else(|| "n/a".to_string());
        let response = format!(
            "📍 <b>Current Chat</b>\nType: <code>{}</code>\nTitle: <code>{}</code>\nUsername: <code>{}</code>\nchat_id: <code>{}</code>\n\nUse this in connect:\n<code>/connecttelegram {} WORKSPACE_NAME</code>",
            chat_kind, title, username, msg.chat.id.0, msg.chat.id.0
        );
        send_message(&bot, chat_id, &response).await?;
        return Ok(());
    }

    if !is_operator && text.starts_with('/') {
        if let Some(rewritten) = rewrite_public_command_as_text(&text) {
            orchestrator
                .audit_event(
                    chat_id.0,
                    None,
                    Some(&format!("telegram-user-{}", user_id)),
                    Some(role.as_str()),
                    crate::orchestrator::Audience::Public,
                    "acl_message_command_rewritten",
                    &format!(
                        "original={} rewritten={}",
                        truncate_str(&text, 120),
                        truncate_str(&rewritten, 120)
                    ),
                )
                .await;
            text = rewritten;
        } else {
            return Ok(());
        }
    }
    if !msg.chat.is_private() || !is_operator {
        orchestrator
            .route_public_chat_workspace(chat_id.0, "telegram", &chat_id.0.to_string())
            .await;
    }

    let awaiting_name = WS_AWAITING_NAME
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&chat_id.0);
    let awaiting_domain = WS_AWAITING_DOMAIN
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&chat_id.0);
    let awaiting_domain_remove = WS_AWAITING_DOMAIN_REMOVE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&chat_id.0);
    let awaiting_skill = WS_AWAITING_SKILL
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&chat_id.0);
    let awaiting_skill_wizard = WS_AWAITING_SKILL_WIZARD
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&chat_id.0);
    let awaiting_connect_target = WS_AWAITING_CONNECT_TARGET
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&chat_id.0);
    let awaiting_secret_set = WS_AWAITING_SECRET_SET
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&chat_id.0);
    let awaiting_secret_remove = WS_AWAITING_SECRET_REMOVE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&chat_id.0);
    let awaiting_any = awaiting_name
        || awaiting_domain
        || awaiting_domain_remove
        || awaiting_skill
        || awaiting_skill_wizard
        || awaiting_connect_target
        || awaiting_secret_set
        || awaiting_secret_remove;

    if !awaiting_any {
        clear_workspace_flow_owner_if_idle(chat_id.0);
    }
    if awaiting_any
        && user_id_opt.is_some()
        && workspace_flow_owner(chat_id.0).is_some_and(|owner| owner != user_id)
    {
        send_message(
            &bot,
            chat_id,
            "Setup is in progress by another operator in this chat. Use /cancel in the owner session or continue in private chat.",
        )
        .await?;
        return Ok(());
    }

    if (awaiting_skill || awaiting_skill_wizard) && awaiting_connect_target {
        WS_AWAITING_CONNECT_TARGET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id.0);
        WS_CONNECT_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id.0);
    }
    if awaiting_connect_target {
        if text.eq_ignore_ascii_case("/cancel") {
            WS_AWAITING_CONNECT_TARGET
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            WS_CONNECT_WIZARD
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            let summary = orchestrator.workspace_public_summary(chat_id.0).await;
            let _ = bot
                .send_message(chat_id, truncate_str(&summary, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(workspace_public_keyboard())
                .await?;
            return Ok(());
        }
        let state = WS_CONNECT_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&chat_id.0)
            .cloned();
        let Some(state) = state else {
            WS_AWAITING_CONNECT_TARGET
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            let _ = bot
                .send_message(
                    chat_id,
                    "Connect flow expired. Open /ws → Public Runtime → Connect Integration.",
                )
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(workspace_public_keyboard())
                .await?;
            return Ok(());
        };
        let Some(integration) = state.integration.as_deref() else {
            let _ = bot
                .send_message(
                    chat_id,
                    "Pick integration first in the connect wizard, then send target id.",
                )
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(workspace_connect_integration_keyboard())
                .await?;
            return Ok(());
        };
        let Some(workspace_name) = state.workspace_name.as_deref() else {
            let _ = bot
                .send_message(
                    chat_id,
                    "Pick workspace first in the connect wizard, then send target id.",
                )
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(workspace_connect_workspace_keyboard(
                    &state.workspace_options,
                ))
                .await?;
            return Ok(());
        };

        let msg = orchestrator
            .connect_integration_binding(chat_id.0, integration, &text, workspace_name)
            .await;
        if msg.starts_with("✅") {
            WS_AWAITING_CONNECT_TARGET
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            WS_CONNECT_WIZARD
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            let summary = orchestrator.workspace_public_summary(chat_id.0).await;
            let target_display = if integration == "telegram" {
                extract_resolved_chat_id(&msg).unwrap_or_else(|| text.clone())
            } else {
                text.clone()
            };
            let diagnostics = extract_connect_diagnostics(&msg);
            let (status_title, status_line) = if diagnostics.is_empty() {
                (
                    "✅ <b>Connection Complete</b>",
                    "This integration is now bound and active for this workspace.".to_string(),
                )
            } else {
                (
                    "⚠️ <b>Connection Saved With Issues</b>",
                    "Binding was saved, but runtime checks reported problems. Fix diagnostics below, then re-test.".to_string(),
                )
            };
            let diagnostics_block = if diagnostics.is_empty() {
                String::new()
            } else {
                format!(
                    "\n\n<b>Diagnostics</b>\n<pre>{}</pre>",
                    escape_html(&diagnostics.join("\n"))
                )
            };
            let full = format!(
                "{}\n<b>Integration:</b> {}\n<b>Workspace:</b> <code>{}</code>\n<b>Target:</b> <code>{}</code>\n\n{}{}\n\n{}",
                status_title,
                integration_label(integration),
                escape_html(workspace_name),
                escape_html(&target_display),
                status_line,
                diagnostics_block,
                summary
            );
            let _ = bot
                .send_message(chat_id, truncate_str(&full, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(workspace_public_keyboard())
                .await?;
        } else {
            let hint = format!(
                "{}\n\nSend another {} or /cancel.",
                msg,
                integration_target_label(integration)
            );
            let _ = bot
                .send_message(chat_id, truncate_str(&hint, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(workspace_connect_target_keyboard())
                .await?;
        }
        return Ok(());
    }
    if awaiting_skill || awaiting_skill_wizard {
        if text.eq_ignore_ascii_case("/cancel") {
            WS_AWAITING_SKILL
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            if awaiting_skill_wizard {
                WS_AWAITING_SKILL_WIZARD
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&chat_id.0);
                let _ = bot
                    .send_message(
                        chat_id,
                        "Step 5/5: Skill prompt skipped.\n\n✅ Setup complete.\nUse /ws to continue configuring integrations/capabilities.",
                    )
                    .parse_mode(ParseMode::Html)
                    .disable_web_page_preview(true)
                    .reply_markup(workspace_wizard_done_keyboard())
                    .await?;
                return Ok(());
            }
            let (panel, kb) = workspace_config_panel(&orchestrator, chat_id.0).await;
            let _ = bot
                .send_message(chat_id, truncate_str(&panel, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(kb)
                .await?;
            return Ok(());
        }
        let msg = orchestrator
            .workspace_set_skill_prompt(chat_id.0, &text)
            .await;
        if msg.starts_with("✅") {
            WS_AWAITING_SKILL
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            WS_AWAITING_SKILL_WIZARD
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
        }
        if awaiting_skill_wizard {
            let full = if msg.starts_with("✅") {
                format!(
                    "{}\n\n✅ Setup complete.\nNext: connect channels/accounts in Public Runtime if needed.",
                    msg
                )
            } else {
                format!(
                    "{}\n\nSend your skill prompt again, or /cancel to skip.",
                    msg
                )
            };
            let _ = bot
                .send_message(chat_id, truncate_str(&full, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(workspace_wizard_done_keyboard())
                .await?;
            return Ok(());
        }
        let (panel, kb) = workspace_config_panel(&orchestrator, chat_id.0).await;
        let full = format!("{msg}\n\n{panel}");
        let _ = bot
            .send_message(chat_id, truncate_str(&full, 4000))
            .parse_mode(ParseMode::Html)
            .disable_web_page_preview(true)
            .reply_markup(kb)
            .await?;
        return Ok(());
    }
    if awaiting_secret_set {
        if !is_operator {
            WS_AWAITING_SECRET_SET
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            send_message(&bot, chat_id, public_command_denied_message()).await?;
            return Ok(());
        }
        if text.eq_ignore_ascii_case("/cancel") {
            WS_AWAITING_SECRET_SET
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            let msg = orchestrator
                .workspace_tools_and_secrets_summary(chat_id.0)
                .await;
            let _ = bot
                .send_message(chat_id, truncate_str(&msg, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(workspace_secrets_keyboard())
                .await?;
            return Ok(());
        }
        let msg = orchestrator.workspace_set_secret(chat_id.0, &text).await;
        if msg.starts_with("✅") {
            WS_AWAITING_SECRET_SET
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
        }
        let summary = orchestrator
            .workspace_tools_and_secrets_summary(chat_id.0)
            .await;
        let _ = bot
            .send_message(chat_id, truncate_str(&format!("{msg}\n\n{summary}"), 4000))
            .parse_mode(ParseMode::Html)
            .disable_web_page_preview(true)
            .reply_markup(workspace_secrets_keyboard())
            .await?;
        return Ok(());
    }
    if awaiting_secret_remove {
        if !is_operator {
            WS_AWAITING_SECRET_REMOVE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            send_message(&bot, chat_id, public_command_denied_message()).await?;
            return Ok(());
        }
        if text.eq_ignore_ascii_case("/cancel") {
            WS_AWAITING_SECRET_REMOVE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            let msg = orchestrator
                .workspace_tools_and_secrets_summary(chat_id.0)
                .await;
            let _ = bot
                .send_message(chat_id, truncate_str(&msg, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(workspace_secrets_keyboard())
                .await?;
            return Ok(());
        }
        let msg = orchestrator.workspace_delete_secret(chat_id.0, &text).await;
        if msg.starts_with("✅") || msg.starts_with("Secret not found") {
            WS_AWAITING_SECRET_REMOVE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
        }
        let summary = orchestrator
            .workspace_tools_and_secrets_summary(chat_id.0)
            .await;
        let _ = bot
            .send_message(chat_id, truncate_str(&format!("{msg}\n\n{summary}"), 4000))
            .parse_mode(ParseMode::Html)
            .disable_web_page_preview(true)
            .reply_markup(workspace_secrets_keyboard())
            .await?;
        return Ok(());
    }
    if awaiting_domain_remove {
        if text.eq_ignore_ascii_case("/cancel") {
            WS_AWAITING_DOMAIN_REMOVE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            let (panel, kb) = workspace_config_panel(&orchestrator, chat_id.0).await;
            let _ = bot
                .send_message(chat_id, truncate_str(&panel, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(kb)
                .await?;
            return Ok(());
        }
        let msg = orchestrator
            .workspace_remove_trusted_domain(chat_id.0, &text)
            .await;
        if msg.starts_with("✅") {
            WS_AWAITING_DOMAIN_REMOVE
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
        }
        let (panel, kb) = workspace_config_panel(&orchestrator, chat_id.0).await;
        let full = format!("{msg}\n\n{panel}");
        let _ = bot
            .send_message(chat_id, truncate_str(&full, 4000))
            .parse_mode(ParseMode::Html)
            .disable_web_page_preview(true)
            .reply_markup(kb)
            .await?;
        return Ok(());
    }
    if awaiting_domain {
        if text.eq_ignore_ascii_case("/cancel") {
            WS_AWAITING_DOMAIN
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            let (panel, kb) = workspace_config_panel(&orchestrator, chat_id.0).await;
            let _ = bot
                .send_message(chat_id, truncate_str(&panel, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(kb)
                .await?;
            return Ok(());
        }
        let msg = orchestrator
            .workspace_add_trusted_domain(chat_id.0, &text)
            .await;
        if msg.starts_with("✅") {
            WS_AWAITING_DOMAIN
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            let (panel, kb) = workspace_config_panel(&orchestrator, chat_id.0).await;
            let full = format!("{msg}\n\n{panel}");
            let _ = bot
                .send_message(chat_id, truncate_str(&full, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(kb)
                .await?;
        } else {
            let kb = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                "⬅️ Cancel",
                "ws:cfg:menu",
            )]]);
            let full = format!(
                "{}\n\nSend trusted domain again (example.com), or /cancel.",
                msg
            );
            let _ = bot
                .send_message(chat_id, truncate_str(&full, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(kb)
                .await?;
        }
        return Ok(());
    }
    if awaiting_name {
        if text.eq_ignore_ascii_case("/cancel") {
            WS_AWAITING_NAME
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            let (panel, kb) = workspace_panel(&orchestrator, chat_id.0).await;
            let _ = bot
                .send_message(chat_id, truncate_str(&panel, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(kb)
                .await?;
            return Ok(());
        }

        let create_msg = orchestrator.workspace_create(chat_id.0, &text).await;
        if create_msg.starts_with("✅") {
            WS_AWAITING_NAME
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&chat_id.0);
            let full = format!(
                "{}\n\n🚀 Quick Setup\nStep 1/5: Choose workspace role (recommended: General).",
                create_msg
            );
            let _ = bot
                .send_message(chat_id, truncate_str(&full, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(workspace_wizard_role_keyboard())
                .await?;
        } else {
            let kb = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
                "⬅️ Cancel naming",
                "ws:new:cancel",
            )]]);
            let full = format!("{}\n\nSend another name, or /cancel.", create_msg);
            let _ = bot
                .send_message(chat_id, truncate_str(&full, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(kb)
                .await?;
        }
        return Ok(());
    }

    let lower = text.trim().to_ascii_lowercase();
    if lower == "/unsafe" {
        let (reply, job_ids) = orchestrator.unsafe_active_run(chat_id.0, 10).await;
        let sent = send_message_maybe_approval(&bot, chat_id, &reply).await?;
        if !job_ids.is_empty() {
            maybe_spawn_inline_progress(
                bot.clone(),
                orchestrator.clone(),
                chat_id,
                sent.id,
                &job_ids,
            )
            .await;
        }
        for job_id in job_ids {
            spawn_job_watcher(bot.clone(), orchestrator.clone(), job_id);
        }
        return Ok(());
    }
    if lower == "/trusted" {
        let (reply, job_ids) = orchestrator.trusted_active_run(chat_id.0, 10).await;
        let sent = send_message_maybe_approval(&bot, chat_id, &reply).await?;
        if !job_ids.is_empty() {
            maybe_spawn_inline_progress(
                bot.clone(),
                orchestrator.clone(),
                chat_id,
                sent.id,
                &job_ids,
            )
            .await;
        }
        for job_id in job_ids {
            spawn_job_watcher(bot.clone(), orchestrator.clone(), job_id);
        }
        return Ok(());
    }
    let normalized =
        lower.trim_matches(|c: char| c.is_whitespace() || matches!(c, '.' | '!' | '?'));
    let approval_phrases = [
        "yes",
        "approve",
        "approved",
        "go ahead",
        "do it",
        "yes do it",
    ];
    if approval_phrases.contains(&normalized) {
        if let Some(blocked_task_id) = orchestrator.get_single_blocked_task(chat_id.0).await {
            let (reply, job_ids) = orchestrator.approve_task(&blocked_task_id).await;
            let sent = send_message_maybe_approval(&bot, chat_id, &reply).await?;
            if !job_ids.is_empty() {
                maybe_spawn_inline_progress(
                    bot.clone(),
                    orchestrator.clone(),
                    chat_id,
                    sent.id,
                    &job_ids,
                )
                .await;
            }
            for job_id in job_ids {
                spawn_job_watcher(bot.clone(), orchestrator.clone(), job_id);
            }
            return Ok(());
        }
    }

    {
        let info_patterns: &[&str] = &[
            "what", "why", "how", "explain", "describe", "tell", "show", "details", "info",
            "about", "which",
        ];
        let is_info_seeking =
            lower.contains('?') || info_patterns.iter().any(|p| lower.starts_with(p));
        if is_info_seeking {
            if let Some(description) = orchestrator.describe_blocked_task(chat_id.0).await {
                let out = if is_operator {
                    description
                } else {
                    let scope_hint = orchestrator.public_scope_hint(chat_id.0).await;
                    orchestrator.map_message_for_audience(
                        &description,
                        crate::orchestrator::Audience::Public,
                        scope_hint.as_deref(),
                    )
                };
                send_message(&bot, chat_id, &out).await?;
                return Ok(());
            }
        }
    }

    if let Some((action_type, goal)) = parse_explicit_code_intent(&text) {
        let (reply, job_ids) = orchestrator
            .create_explicit_code_task(chat_id.0, &action_type, &goal)
            .await;
        let mut response = reply.trim().to_string();
        if !is_operator {
            let scope_hint = orchestrator.public_scope_hint(chat_id.0).await;
            response = orchestrator.map_message_for_audience(
                &response,
                crate::orchestrator::Audience::Public,
                scope_hint.as_deref(),
            );
        }
        if response.is_empty() {
            response = "⚠️ Could not create code task.".into();
        }
        let sent = send_message_maybe_approval(&bot, chat_id, &response).await?;
        if !job_ids.is_empty() {
            maybe_spawn_inline_progress(
                bot.clone(),
                orchestrator.clone(),
                chat_id,
                sent.id,
                &job_ids,
            )
            .await;
        }
        for job_id in job_ids {
            spawn_job_watcher(bot.clone(), orchestrator.clone(), job_id);
        }
        return Ok(());
    }

    let (reply, job_ids) = orchestrator.process_message(chat_id.0, &text).await;
    let mut response = reply.trim().to_string();
    if !is_operator {
        let scope_hint = orchestrator.public_scope_hint(chat_id.0).await;
        let mapped = orchestrator.map_message_for_audience(
            &response,
            crate::orchestrator::Audience::Public,
            scope_hint.as_deref(),
        );
        if mapped != response {
            orchestrator
                .audit_event(
                    chat_id.0,
                    None,
                    Some(&format!("telegram-user-{}", user_id)),
                    Some(role.as_str()),
                    crate::orchestrator::Audience::Public,
                    "public_response_masked",
                    &format!(
                        "original_class={:?}",
                        Orchestrator::classify_user_error(&response)
                    ),
                )
                .await;
        }
        response = mapped;
    }
    if response.is_empty() {
        response = "⚠️ I received an empty response. Please retry.".into();
    }
    if !job_ids.is_empty() && !response.contains("⏳") {
        response.push_str("\n\n⏳ Working on your request...");
    }

    let sent = send_message_maybe_approval(&bot, chat_id, &response).await?;

    if !job_ids.is_empty() {
        maybe_spawn_inline_progress(
            bot.clone(),
            orchestrator.clone(),
            chat_id,
            sent.id,
            &job_ids,
        )
        .await;
    }

    for job_id in job_ids {
        spawn_job_watcher(bot.clone(), orchestrator.clone(), job_id);
    }

    Ok(())
}
