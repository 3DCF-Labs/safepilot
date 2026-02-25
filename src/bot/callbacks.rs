use super::*;

pub(super) async fn handle_callback(
    bot: Bot,
    q: CallbackQuery,
    orchestrator: Arc<Orchestrator>,
) -> ResponseResult<()> {
    let data = q.data.clone().unwrap_or_default();
    let user_id = q.from.id.0 as i64;
    let role = orchestrator.resolve_telegram_role(user_id).await;
    if !Orchestrator::is_operator_role(role) {
        if let Some(chat_id) = q.message.as_ref().map(|m| m.chat.id) {
            orchestrator
                .audit_event(
                    chat_id.0,
                    None,
                    Some(&format!("telegram-user-{}", user_id)),
                    Some(role.as_str()),
                    crate::orchestrator::Audience::Public,
                    "acl_callback_denied",
                    &format!("callback={}", truncate_str(&data, 120)),
                )
                .await;
        }
        bot.answer_callback_query(q.id).await?;
        return Ok(());
    }
    let Some(chat_id) = q.message.as_ref().map(|m| m.chat.id) else {
        bot.answer_callback_query(q.id).await?;
        return Ok(());
    };
    let callback_message_id = q.message.as_ref().map(|m| m.id);

    if data.starts_with("ws:") {
        if q.message.as_ref().is_some_and(|m| !m.chat.is_private()) {
            bot.answer_callback_query(q.id)
                .text("Workspace setup is private-only. Open a DM with the bot.")
                .await?;
            return Ok(());
        }
        let integration_owner_only = data.starts_with("ws:cfg:caps:")
            || data == "ws:cfg:public:bindings"
            || data.starts_with("ws:cfg:public:connect")
            || data.starts_with("ws:cfg:connect:")
            || data.starts_with("ws:cfg:binding:");
        if integration_owner_only && !Orchestrator::is_owner_role(role) {
            orchestrator
                .audit_event(
                    chat_id.0,
                    None,
                    Some(&format!("telegram-user-{}", user_id)),
                    Some(role.as_str()),
                    crate::orchestrator::Audience::Public,
                    "acl_integration_management_denied",
                    &format!("callback={}", truncate_str(&data, 120)),
                )
                .await;
            bot.answer_callback_query(q.id)
                .text("Only owner can manage workspace integrations.")
                .await?;
            return Ok(());
        }
        claim_workspace_flow_owner(chat_id.0, user_id);
        handle_workspace_callback(
            &bot,
            &orchestrator,
            chat_id,
            callback_message_id,
            chat_id.0,
            &data,
        )
        .await?;
        bot.answer_callback_query(q.id).await?;
        return Ok(());
    }

    let (response, job_ids) = if let Some(task_id) = data.strip_prefix("approve:") {
        orchestrator.approve_task(task_id).await
    } else if let Some(task_id) = data.strip_prefix("deny:") {
        (orchestrator.deny_task(task_id).await, vec![])
    } else if let Some(task_id) = data.strip_prefix("approve_run:") {
        orchestrator
            .approve_task_with_grant(task_id, ApprovalGrantScope::Run, 10, false)
            .await
    } else if let Some(task_id) = data.strip_prefix("approve_broad_run:") {
        orchestrator
            .approve_task_with_grant(task_id, ApprovalGrantScope::Run, 10, true)
            .await
    } else if let Some(task_id) = data.strip_prefix("approve_workspace:") {
        orchestrator
            .approve_task_with_grant(task_id, ApprovalGrantScope::Workspace, 10, false)
            .await
    } else if let Some(mins) = data.strip_prefix("trusted:") {
        let m: u64 = mins.parse().unwrap_or(10);
        orchestrator.trusted_active_run(chat_id.0, m).await
    } else if let Some(mins) = data.strip_prefix("unsafe:") {
        let m: u64 = mins.parse().unwrap_or(10);
        orchestrator.unsafe_active_run(chat_id.0, m).await
    } else if let Some(job_id) = data.strip_prefix("log:") {
        let clean_id = job_id.split_whitespace().next().unwrap_or(job_id);
        let log_text = orchestrator.get_log(clean_id).await;
        let _ = send_message(&bot, chat_id, &log_text).await;
        bot.answer_callback_query(q.id).await?;
        return Ok(());
    } else {
        ("Unknown action".into(), vec![])
    };

    let sent = send_message_maybe_approval(&bot, chat_id, &response).await?;
    let is_approval_action =
        data.starts_with("approve") || data.starts_with("trusted:") || data.starts_with("unsafe:");
    if is_approval_action {
        takeover_inline_progress(bot.clone(), orchestrator.clone(), chat_id, sent.id).await;
    } else if !job_ids.is_empty() {
        maybe_spawn_inline_progress(
            bot.clone(),
            orchestrator.clone(),
            chat_id,
            sent.id,
            &job_ids,
        )
        .await;
    }
    for jid in job_ids {
        spawn_job_watcher(bot.clone(), orchestrator.clone(), jid);
    }
    bot.answer_callback_query(q.id).await?;
    Ok(())
}

async fn send_or_edit_workspace(
    bot: &Bot,
    chat_id: ChatId,
    message_id: Option<MessageId>,
    text: &str,
    kb: InlineKeyboardMarkup,
) -> ResponseResult<()> {
    let raw = truncate_str(text, 3600);
    let rendered = truncate_str(&format_for_telegram_html(&raw), 4000);
    if let Some(mid) = message_id {
        let edited = bot
            .edit_message_text(chat_id, mid, rendered.clone())
            .parse_mode(ParseMode::Html)
            .disable_web_page_preview(true)
            .reply_markup(kb.clone())
            .await;
        if edited.is_ok() {
            return Ok(());
        }
    }
    let _ = bot
        .send_message(chat_id, rendered)
        .parse_mode(ParseMode::Html)
        .disable_web_page_preview(true)
        .reply_markup(kb)
        .await?;
    Ok(())
}

async fn handle_workspace_callback(
    bot: &Bot,
    orchestrator: &Arc<Orchestrator>,
    chat_id: ChatId,
    message_id: Option<MessageId>,
    chat_id_i64: i64,
    data: &str,
) -> ResponseResult<()> {
    let _ = orchestrator.workspace_current(chat_id_i64).await;

    if data == "ws:menu" {
        let (text, kb) = workspace_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &text, kb).await;
    }
    if data == "ws:cfg:menu" {
        WS_AWAITING_SKILL
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_AWAITING_SKILL_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        let (text, kb) = workspace_config_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &text, kb).await;
    }
    if data == "ws:cfg:wizard:start" {
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "🚀 <b>Quick Setup</b>\nStep 1/5: Choose workspace role (recommended: General).",
            workspace_wizard_role_keyboard(),
        )
        .await;
    }
    if let Some(role) = data.strip_prefix("ws:cfg:wizard:role:") {
        let msg = orchestrator
            .workspace_set_role_preset(chat_id_i64, role)
            .await;
        let full = format!("{msg}\n\nStep 2/5: Choose safety mode (recommended: Safe).");
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &full,
            workspace_wizard_safety_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:wizard:safety" {
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Step 2/5: Choose safety mode (recommended: Safe).",
            workspace_wizard_safety_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:wizard:network" {
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Step 3/5: Choose network policy (recommended: Open).",
            workspace_wizard_network_keyboard(),
        )
        .await;
    }
    if let Some(mode) = data.strip_prefix("ws:cfg:wizard:safety:") {
        let msg = match mode {
            "trusted30" => {
                orchestrator
                    .workspace_set_security_mode(
                        chat_id_i64,
                        WorkspaceSecurityMode::Trusted,
                        Some(30),
                    )
                    .await
            }
            "unsafe10" => {
                orchestrator
                    .workspace_set_security_mode(
                        chat_id_i64,
                        WorkspaceSecurityMode::Unsafe,
                        Some(10),
                    )
                    .await
            }
            _ => {
                orchestrator
                    .workspace_set_security_mode(chat_id_i64, WorkspaceSecurityMode::Strict, None)
                    .await
            }
        };
        let full = format!("{msg}\n\nStep 3/5: Choose network policy (recommended: Open).");
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &full,
            workspace_wizard_network_keyboard(),
        )
        .await;
    }
    if let Some(fetch_mode) = data.strip_prefix("ws:cfg:wizard:fetch:") {
        let mode = match fetch_mode {
            "trusted_only" => WorkspaceFetchMode::TrustedOnly,
            "trusted_preferred" => WorkspaceFetchMode::TrustedPreferred,
            _ => WorkspaceFetchMode::Open,
        };
        let msg = orchestrator
            .workspace_set_fetch_mode(chat_id_i64, mode)
            .await;
        let full = format!(
            "{msg}\n\nStep 4/5: Choose integration scope.\n• Recommended (least privilege): enable only role-matched integrations.\n• All integrations (read-only): enable reads everywhere, no writes.\n• Customize now: manually set each integration capability.",
        );
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &full,
            workspace_wizard_integrations_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:wizard:integrations" {
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Step 4/5: Choose integration scope.",
            workspace_wizard_integrations_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:wizard:caps:recommended" {
        let msg = orchestrator
            .workspace_apply_recommended_caps(chat_id_i64)
            .await;
        let full = format!(
            "{msg}\n\nStep 5/5: Add custom skill prompt? (recommended for focused assistants)"
        );
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &full,
            workspace_wizard_skill_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:wizard:caps:all_readonly" {
        let msg = orchestrator
            .workspace_apply_caps_template(chat_id_i64, "strict_readonly")
            .await;
        let full = format!(
            "{msg}\n\nStep 5/5: Add custom skill prompt? (recommended for focused assistants)"
        );
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &full,
            workspace_wizard_skill_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:wizard:caps:custom" {
        let full = "Step 5/5: You can customize integrations in Public Runtime -> Capabilities.\nAdd custom skill prompt now?";
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            full,
            workspace_wizard_skill_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:wizard:skill:set" {
        WS_AWAITING_CONNECT_TARGET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_CONNECT_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_AWAITING_SKILL
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64);
        WS_AWAITING_SKILL_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64);
        let kb = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
            "⏭ Skip",
            "ws:cfg:wizard:skill:skip",
        )]]);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Send the custom skill prompt as your next message. /cancel to skip.",
            kb,
        )
        .await;
    }
    if data == "ws:cfg:wizard:skill:skip" {
        WS_AWAITING_SKILL
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_AWAITING_SKILL_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        let summary = orchestrator.workspace_config_summary(chat_id_i64).await;
        let full = format!(
            "✅ Setup complete.\n\n{}",
            summary.replace("⚙️ Workspace config", "⚙️ Final Workspace Setup")
        );
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &full,
            workspace_wizard_done_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:profile:menu" {
        WS_AWAITING_SKILL
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_AWAITING_SKILL_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "🎯 <b>Role & Skill</b>\nChoose a role preset and optional custom instructions for this workspace.",
            workspace_profile_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:profile:view" {
        let msg = orchestrator.workspace_profile_summary(chat_id_i64).await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_profile_keyboard(),
        )
        .await;
    }
    if let Some(role) = data.strip_prefix("ws:cfg:profile:") {
        if role != "menu" && role != "view" {
            let msg = orchestrator
                .workspace_set_role_preset(chat_id_i64, role)
                .await;
            let summary = orchestrator.workspace_profile_summary(chat_id_i64).await;
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                &format!("{msg}\n\n{summary}"),
                workspace_profile_keyboard(),
            )
            .await;
        }
    }
    if data == "ws:cfg:skill:set" {
        WS_AWAITING_CONNECT_TARGET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_CONNECT_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_AWAITING_SKILL
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64);
        let kb = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
            "⬅️ Back to Role & Skill",
            "ws:cfg:profile:menu",
        )]]);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Send the custom skill prompt as your next message. /cancel to abort.",
            kb,
        )
        .await;
    }
    if data == "ws:cfg:skill:clear" {
        let msg = orchestrator.workspace_clear_skill_prompt(chat_id_i64).await;
        let summary = orchestrator.workspace_profile_summary(chat_id_i64).await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &format!("{msg}\n\n{summary}"),
            workspace_profile_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:safety:menu" {
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "🛡 <b>Safety Mode</b>\nChoose how strict this workspace should be.",
            workspace_safety_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:tools:menu" {
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "🧰 <b>Tools Profile</b>\nSelect shell command profile for this workspace.",
            workspace_tools_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:network:menu" {
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "🌐 <b>Network Policy</b>\nControl how fetch works and which domains are trusted.",
            workspace_network_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:secrets:menu" {
        let msg = orchestrator
            .workspace_tools_and_secrets_summary(chat_id_i64)
            .await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_secrets_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:public:menu" {
        let summary = orchestrator.workspace_public_summary(chat_id_i64).await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &summary,
            workspace_public_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:public:sources:toggle" {
        let msg = orchestrator
            .workspace_toggle_public_sources(chat_id_i64)
            .await;
        let summary = orchestrator.workspace_public_summary(chat_id_i64).await;
        let combined = format!("{}\n\n{}", msg, summary);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &combined,
            workspace_public_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:public:bindings" {
        let msg = orchestrator.list_channel_bindings(chat_id_i64).await;
        let full = format!(
            "{}\n\nTip: use Connect Integration to add more targets.",
            msg
        );
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &full,
            workspace_public_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:binding:menu" {
        let rows = orchestrator
            .db
            .list_channel_bindings_for_chat(chat_id_i64)
            .await
            .unwrap_or_default();
        if rows.is_empty() {
            let msg = "🔐 <b>Channel Rules</b>\nNo connected targets yet.\nConnect one integration first, then define rules per target.";
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                msg,
                workspace_binding_policy_empty_keyboard(),
            )
            .await;
        }
        let msg = "🔐 <b>Channel Rules</b>\nPick one connected target.\nThen set:\n• write mode for that target\n• allowed actions for that target";
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            msg,
            workspace_binding_policy_keyboard(&rows),
        )
        .await;
    }
    if let Some(binding) = data.strip_prefix("ws:cfg:binding:edit:") {
        if let Some((integration, channel_id)) = parse_binding_target(binding) {
            if let Ok(Some(row)) = orchestrator
                .db
                .get_channel_binding(&integration, &channel_id)
                .await
            {
                WS_BINDING_EDITOR
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(
                        chat_id_i64,
                        BindingEditorState {
                            binding: binding.to_string(),
                            selected: row.allowed_actions.into_iter().collect(),
                        },
                    );
            }
        }
        let msg = orchestrator
            .binding_policy_summary(chat_id_i64, binding)
            .await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_binding_edit_keyboard(binding),
        )
        .await;
    }
    if let Some(binding) = data.strip_prefix("ws:cfg:binding:actions:start:") {
        let Some((integration, channel_id)) = parse_binding_target(binding) else {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Invalid binding id.",
                workspace_public_keyboard(),
            )
            .await;
        };
        let Some(row) = orchestrator
            .db
            .get_channel_binding(&integration, &channel_id)
            .await
            .ok()
            .flatten()
        else {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Binding not found.",
                workspace_public_keyboard(),
            )
            .await;
        };
        let state = BindingEditorState {
            binding: binding.to_string(),
            selected: row.allowed_actions.into_iter().collect(),
        };
        WS_BINDING_EDITOR
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64, state.clone());
        let text = binding_actions_editor_text(&state);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &text,
            workspace_binding_actions_keyboard(binding, &state),
        )
        .await;
    }
    if let Some(action) = data.strip_prefix("ws:cfg:binding:actions:toggle:") {
        let updated = {
            let mut guard = WS_BINDING_EDITOR.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(state) = guard.get_mut(&chat_id_i64) {
                if state.selected.contains(action) {
                    state.selected.remove(action);
                } else {
                    state.selected.insert(action.to_string());
                }
                Some(state.clone())
            } else {
                None
            }
        };
        let Some(state) = updated else {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Open a binding first: Public Runtime -> Binding Policies.",
                workspace_public_keyboard(),
            )
            .await;
        };
        let text = binding_actions_editor_text(&state);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &text,
            workspace_binding_actions_keyboard(&state.binding, &state),
        )
        .await;
    }
    if data == "ws:cfg:binding:actions:any" {
        let updated = {
            let mut guard = WS_BINDING_EDITOR.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(state) = guard.get_mut(&chat_id_i64) {
                state.selected.clear();
                Some(state.clone())
            } else {
                None
            }
        };
        let Some(state) = updated else {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Open a binding first: Public Runtime -> Binding Policies.",
                workspace_public_keyboard(),
            )
            .await;
        };
        let text = binding_actions_editor_text(&state);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &text,
            workspace_binding_actions_keyboard(&state.binding, &state),
        )
        .await;
    }
    if data == "ws:cfg:binding:actions:reset" {
        let binding = WS_BINDING_EDITOR
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&chat_id_i64)
            .map(|s| s.binding.clone());
        let Some(binding) = binding else {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Open a binding first: Public Runtime -> Binding Policies.",
                workspace_public_keyboard(),
            )
            .await;
        };
        let Some((integration, channel_id)) = parse_binding_target(&binding) else {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Invalid binding id.",
                workspace_public_keyboard(),
            )
            .await;
        };
        if let Ok(Some(row)) = orchestrator
            .db
            .get_channel_binding(&integration, &channel_id)
            .await
        {
            let state = BindingEditorState {
                binding: binding.clone(),
                selected: row.allowed_actions.into_iter().collect(),
            };
            WS_BINDING_EDITOR
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(chat_id_i64, state.clone());
            let text = binding_actions_editor_text(&state);
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                &text,
                workspace_binding_actions_keyboard(&binding, &state),
            )
            .await;
        }
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Could not reload binding policy.",
            workspace_public_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:binding:actions:save" {
        let state = WS_BINDING_EDITOR
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&chat_id_i64)
            .cloned();
        let Some(state) = state else {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Open a binding first: Public Runtime -> Binding Policies.",
                workspace_public_keyboard(),
            )
            .await;
        };
        let Some((integration, channel_id)) = parse_binding_target(&state.binding) else {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Invalid binding id.",
                workspace_public_keyboard(),
            )
            .await;
        };
        let Some(row) = orchestrator
            .db
            .get_channel_binding(&integration, &channel_id)
            .await
            .ok()
            .flatten()
        else {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Binding not found.",
                workspace_public_keyboard(),
            )
            .await;
        };
        let mut selected = state.selected.iter().cloned().collect::<Vec<_>>();
        selected.sort();
        let allowed_raw = if selected.is_empty() {
            "*".to_string()
        } else {
            selected.join(",")
        };
        let msg = orchestrator
            .update_binding_policy(
                chat_id_i64,
                &state.binding,
                &row.write_policy,
                &allowed_raw,
                None,
            )
            .await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_binding_edit_keyboard(&state.binding),
        )
        .await;
    }
    if let Some(rest) = data.strip_prefix("ws:cfg:binding:wp:") {
        if let Some((binding, wp)) = rest.rsplit_once(':') {
            let msg = orchestrator
                .update_binding_policy(chat_id_i64, binding, wp, "*", None)
                .await;
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                &msg,
                workspace_binding_edit_keyboard(binding),
            )
            .await;
        }
    }
    if let Some(rest) = data.strip_prefix("ws:cfg:binding:preset:") {
        if let Some((binding, preset)) = rest.rsplit_once(':') {
            let msg = orchestrator
                .apply_binding_policy_preset(chat_id_i64, binding, preset)
                .await;
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                &msg,
                workspace_binding_edit_keyboard(binding),
            )
            .await;
        }
    }
    if data == "ws:cfg:connect:menu" {
        WS_AWAITING_SKILL
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_AWAITING_SKILL_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_AWAITING_CONNECT_TARGET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_CONNECT_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64, ConnectWizardState::default());
        let msg = "🔌 <b>Connect Integration</b>\nStep 1/3: Pick integration.";
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            msg,
            workspace_connect_integration_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:connect:cancel" {
        WS_AWAITING_CONNECT_TARGET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_CONNECT_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        let summary = orchestrator.workspace_public_summary(chat_id_i64).await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &summary,
            workspace_public_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:connect:back:ws" {
        WS_AWAITING_CONNECT_TARGET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        let state = WS_CONNECT_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&chat_id_i64)
            .cloned();
        if let Some(state) = state {
            if state.workspace_options.is_empty() {
                return send_or_edit_workspace(
                    bot,
                    chat_id,
                    message_id,
                    "No workspaces found. Create one with /wsnew first.",
                    workspace_public_keyboard(),
                )
                .await;
            }
            let msg = format!(
                "🔌 <b>Connect {}</b>\nStep 2/3: Pick workspace.",
                integration_label(state.integration.as_deref().unwrap_or_default())
            );
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                &msg,
                workspace_connect_workspace_keyboard(&state.workspace_options),
            )
            .await;
        }
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Connect flow expired. Start again from Public Runtime.",
            workspace_public_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:connect:validate" {
        let state = WS_CONNECT_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&chat_id_i64)
            .cloned();
        if let Some(state) = state {
            if let Some(integration) = state.integration.as_deref() {
                let msg = orchestrator
                    .connect_integration_help(chat_id_i64, integration)
                    .await;
                return send_or_edit_workspace(
                    bot,
                    chat_id,
                    message_id,
                    &msg,
                    workspace_connect_target_keyboard(),
                )
                .await;
            }
        }
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Pick integration first.",
            workspace_connect_integration_keyboard(),
        )
        .await;
    }
    if let Some(integration) = data.strip_prefix("ws:cfg:connect:int:") {
        let workspaces = orchestrator
            .db
            .list_workspaces(chat_id_i64)
            .await
            .unwrap_or_default();
        let workspace_options = workspaces.into_iter().map(|w| w.name).collect::<Vec<_>>();
        if workspace_options.is_empty() {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "No workspaces found. Create one with /wsnew first.",
                workspace_public_keyboard(),
            )
            .await;
        }
        WS_AWAITING_CONNECT_TARGET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        WS_CONNECT_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                chat_id_i64,
                ConnectWizardState {
                    integration: Some(integration.to_string()),
                    workspace_name: None,
                    workspace_options: workspace_options.clone(),
                },
            );
        let msg = format!(
            "🔌 <b>Connect {}</b>\nStep 2/3: Pick workspace.",
            integration_label(integration)
        );
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_connect_workspace_keyboard(&workspace_options),
        )
        .await;
    }
    if let Some(idx_raw) = data.strip_prefix("ws:cfg:connect:ws:") {
        let idx = idx_raw.parse::<usize>().ok();
        let Some(idx) = idx else {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Invalid workspace selection.",
                workspace_connect_integration_keyboard(),
            )
            .await;
        };
        let mut expired = false;
        let mut invalid_options: Option<Vec<String>> = None;
        let mut integration = "integration".to_string();
        let mut workspace_name = String::new();
        let mut workspace_options = Vec::new();
        {
            let mut guard = WS_CONNECT_WIZARD.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(state) = guard.get_mut(&chat_id_i64) {
                if let Some(selected_workspace) = state.workspace_options.get(idx).cloned() {
                    integration = state
                        .integration
                        .clone()
                        .unwrap_or_else(|| "integration".to_string());
                    state.workspace_name = Some(selected_workspace.clone());
                    workspace_name = selected_workspace;
                    workspace_options = state.workspace_options.clone();
                } else {
                    invalid_options = Some(state.workspace_options.clone());
                }
            } else {
                expired = true;
            }
        }
        if expired {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Connect flow expired. Start again from Public Runtime.",
                workspace_public_keyboard(),
            )
            .await;
        }
        if let Some(options) = invalid_options {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Invalid workspace selection.",
                workspace_connect_workspace_keyboard(&options),
            )
            .await;
        }

        if workspace_options.is_empty() {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "No workspaces found. Create one with /wsnew first.",
                workspace_public_keyboard(),
            )
            .await;
        }
        WS_AWAITING_CONNECT_TARGET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64);
        let step_msg = if integration == "telegram" {
            format!(
                "🔌 <b>Connect {}</b>\nStep 3/3: Send target now.\n\nBefore sending target:\n1) Add bot to this channel/group as member/subscriber\n2) Promote bot to Admin\n\nAccepted:\n• <code>-1001234567890</code> (chat_id)\n• <code>@mychannel</code> (public username)\n\nTip: if you don't know chat_id, open target chat and send <code>/whereami</code>.\n\nWorkspace: <code>{}</code>\n\nI will validate and bind after your next message.\nType /cancel to exit.",
                integration_label(&integration),
                escape_html(&workspace_name),
            )
        } else {
            format!(
                "🔌 <b>Connect {}</b>\nStep 3/3: Send {} as your next message.\nWorkspace: <code>{}</code>\n\nI will validate and bind after your next message.\nType /cancel to exit.",
                integration_label(&integration),
                integration_target_label(&integration),
                escape_html(&workspace_name)
            )
        };
        let msg = step_msg.to_string();
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_connect_target_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:public:connect:discord" {
        let kb = workspace_connect_integration_keyboard();
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "🔌 <b>Connect Integration</b>\nStep 1/3: Pick integration.\nHint: choose Discord.",
            kb,
        )
        .await;
    }
    if data == "ws:cfg:public:connect:x" {
        let kb = workspace_connect_integration_keyboard();
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "🔌 <b>Connect Integration</b>\nStep 1/3: Pick integration.\nHint: choose X.",
            kb,
        )
        .await;
    }
    if data == "ws:cfg:public:connect:telegram" {
        let kb = workspace_connect_integration_keyboard();
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "🔌 <b>Connect Integration</b>\nStep 1/3: Pick integration.\nHint: choose Telegram.",
            kb,
        )
        .await;
    }
    if data == "ws:cfg:caps:menu" {
        let summary = orchestrator
            .workspace_integration_caps_summary(chat_id_i64)
            .await;
        let full = format!(
            "🧭 <b>How this works</b>\nStep 1: apply one preset (optional)\nStep 2: open one integration and adjust switches\n\n{}",
            summary
        );
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &full,
            workspace_caps_menu_keyboard(),
        )
        .await;
    }
    if let Some(template) = data.strip_prefix("ws:cfg:caps:preset:") {
        let msg = orchestrator
            .workspace_apply_caps_template(chat_id_i64, template)
            .await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_caps_menu_keyboard(),
        )
        .await;
    }
    if let Some(integration) = data.strip_prefix("ws:cfg:caps:edit:") {
        let detail = orchestrator
            .workspace_integration_cap_detail(chat_id_i64, integration)
            .await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &detail,
            workspace_cap_edit_keyboard(integration),
        )
        .await;
    }
    if let Some(rest) = data.strip_prefix("ws:cfg:caps:toggle:") {
        let mut parts = rest.split(':');
        let integration = parts.next().unwrap_or_default();
        let field = parts.next().unwrap_or_default();
        if integration.is_empty() || field.is_empty() {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                "Invalid capability toggle payload.",
                workspace_caps_menu_keyboard(),
            )
            .await;
        }
        let msg = orchestrator
            .workspace_toggle_integration_cap(chat_id_i64, integration, field)
            .await;
        let detail = orchestrator
            .workspace_integration_cap_detail(chat_id_i64, integration)
            .await;
        let table = orchestrator
            .workspace_integration_caps_summary(chat_id_i64)
            .await;
        let full = if msg.starts_with("✅") {
            format!("{}\n\n{}\n\n{}", msg, detail, table)
        } else {
            msg
        };
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &full,
            workspace_cap_edit_keyboard(integration),
        )
        .await;
    }
    if data == "ws:cfg:mode:safe" {
        let msg = orchestrator
            .workspace_set_security_mode(chat_id_i64, WorkspaceSecurityMode::Strict, None)
            .await;
        let (panel, kb) = workspace_config_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &format!("{msg}\n\n{panel}"), kb)
            .await;
    }
    if data == "ws:cfg:mode:trusted30" {
        let msg = orchestrator
            .workspace_set_security_mode(chat_id_i64, WorkspaceSecurityMode::Trusted, Some(30))
            .await;
        let (panel, kb) = workspace_config_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &format!("{msg}\n\n{panel}"), kb)
            .await;
    }
    if data == "ws:cfg:mode:unsafe10" {
        let msg = orchestrator
            .workspace_set_security_mode(chat_id_i64, WorkspaceSecurityMode::Unsafe, Some(10))
            .await;
        let (panel, kb) = workspace_config_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &format!("{msg}\n\n{panel}"), kb)
            .await;
    }
    if data == "ws:cfg:mode:trusted_forever" {
        let msg = orchestrator
            .workspace_set_security_mode(chat_id_i64, WorkspaceSecurityMode::Trusted, None)
            .await;
        let (panel, kb) = workspace_config_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &format!("{msg}\n\n{panel}"), kb)
            .await;
    }
    if data == "ws:cfg:mode:unsafe_forever" {
        let msg = orchestrator
            .workspace_set_security_mode(chat_id_i64, WorkspaceSecurityMode::Unsafe, None)
            .await;
        let (panel, kb) = workspace_config_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &format!("{msg}\n\n{panel}"), kb)
            .await;
    }
    if let Some(pack) = data.strip_prefix("ws:cfg:shell:") {
        let pack = match pack {
            "strict" => WorkspaceShellPack::Strict,
            "extended" => WorkspaceShellPack::Extended,
            _ => WorkspaceShellPack::Standard,
        };
        let msg = orchestrator
            .workspace_set_shell_pack(chat_id_i64, pack)
            .await;
        let (panel, kb) = workspace_config_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &format!("{msg}\n\n{panel}"), kb)
            .await;
    }
    if let Some(mode) = data.strip_prefix("ws:cfg:fetch:") {
        let mode = match mode {
            "trusted_only" => WorkspaceFetchMode::TrustedOnly,
            "trusted_preferred" => WorkspaceFetchMode::TrustedPreferred,
            _ => WorkspaceFetchMode::Open,
        };
        let msg = orchestrator
            .workspace_set_fetch_mode(chat_id_i64, mode)
            .await;
        let (panel, kb) = workspace_config_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &format!("{msg}\n\n{panel}"), kb)
            .await;
    }
    if data == "ws:cfg:domain:add" {
        WS_AWAITING_DOMAIN
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64);
        let kb = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
            "⬅️ Cancel",
            "ws:cfg:menu",
        )]]);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Send trusted domain as next message (example.com). /cancel to abort.",
            kb,
        )
        .await;
    }
    if data == "ws:cfg:domain:list" {
        let msg = orchestrator
            .workspace_list_trusted_domains(chat_id_i64)
            .await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_network_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:domain:remove" {
        WS_AWAITING_DOMAIN_REMOVE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64);
        let kb = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
            "⬅️ Cancel",
            "ws:cfg:network:menu",
        )]]);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Send trusted domain to remove (example.com). /cancel to abort.",
            kb,
        )
        .await;
    }
    if data == "ws:cfg:domain:clear" {
        let msg = orchestrator
            .workspace_clear_trusted_domains(chat_id_i64)
            .await;
        let (panel, kb) = workspace_config_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &format!("{msg}\n\n{panel}"), kb)
            .await;
    }
    if data == "ws:cfg:tools" {
        let msg = orchestrator
            .workspace_tools_and_secrets_summary(chat_id_i64)
            .await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_secrets_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:secret:list" {
        let msg = orchestrator.workspace_list_secret_names(chat_id_i64).await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_secrets_keyboard(),
        )
        .await;
    }
    if data == "ws:cfg:secret:set" {
        WS_AWAITING_SECRET_SET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64);
        WS_AWAITING_SECRET_REMOVE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        let kb = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
            "⬅️ Cancel",
            "ws:cfg:secrets:menu",
        )]]);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Send secret reference as <code>NAME=env:VAR_NAME</code> or <code>NAME=file:/absolute/path</code>.\nExample: <code>SLACK_BOT_TOKEN_WRITE=env:SLACK_TOKEN_WRITE</code>\n\nRaw secret values in chat are blocked.",
            kb,
        )
        .await;
    }
    if data == "ws:cfg:secret:remove" {
        WS_AWAITING_SECRET_REMOVE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64);
        WS_AWAITING_SECRET_SET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        let kb = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
            "⬅️ Cancel",
            "ws:cfg:secrets:menu",
        )]]);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Send secret name to remove.\nExample: <code>SLACK_BOT_TOKEN_WRITE</code>",
            kb,
        )
        .await;
    }
    if data == "ws:cfg:audit:menu" {
        let msg = "📜 <b>Audit Views</b>\nPick a quick filter below.\nFor advanced filtering use:\n<code>/auditf audience=public event=policy limit=50</code>";
        return send_or_edit_workspace(bot, chat_id, message_id, msg, workspace_audit_keyboard())
            .await;
    }
    if let Some(filter) = data.strip_prefix("ws:cfg:audit:view:") {
        let msg = orchestrator.audit_filtered(chat_id_i64, filter).await;
        if msg.contains("<b>") || msg.contains("<code>") {
            return send_or_edit_workspace(
                bot,
                chat_id,
                message_id,
                &msg,
                workspace_audit_keyboard(),
            )
            .await;
        }
        let pre = format!("<pre>{}</pre>", escape_html(&truncate_str(&msg, 3600)));
        return send_or_edit_workspace(bot, chat_id, message_id, &pre, workspace_audit_keyboard())
            .await;
    }
    if data == "ws:cfg:enc:enable" {
        let msg = orchestrator.enable_encryption_with_generated_key().await;
        let (panel, kb) = workspace_config_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &format!("{msg}\n\n{panel}"), kb)
            .await;
    }
    if data == "ws:cfg:enc:rotate" {
        let msg = orchestrator.rotate_encryption_master_key(chat_id_i64).await;
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            &msg,
            workspace_secrets_keyboard(),
        )
        .await;
    }
    if data == "ws:start" {
        let (text, kb) = workspace_panel(orchestrator, chat_id_i64).await;
        let full = format!(
            "{}\n\n✅ Ready. Send your next task message in this workspace.",
            text
        );
        return send_or_edit_workspace(bot, chat_id, message_id, &full, kb).await;
    }
    if data == "ws:switch" {
        let active_id = orchestrator
            .db
            .get_active_workspace_id(chat_id_i64)
            .await
            .ok()
            .flatten();
        let workspaces = orchestrator
            .db
            .list_workspaces(chat_id_i64)
            .await
            .unwrap_or_default();
        let text = "Select a workspace:";
        let kb = workspace_switch_keyboard(&workspaces, active_id.as_deref());
        return send_or_edit_workspace(bot, chat_id, message_id, text, kb).await;
    }
    if let Some(ws_id) = data.strip_prefix("ws:use:") {
        if let Ok(Some(ws)) = orchestrator.db.get_workspace_by_id(ws_id).await {
            let _ = orchestrator.workspace_use(chat_id_i64, &ws.name).await;
        }
        let (text, kb) = workspace_panel(orchestrator, chat_id_i64).await;
        let full = format!(
            "✅ Switched workspace.\n\n{}\n\n▶️ Send your task now.",
            text
        );
        return send_or_edit_workspace(bot, chat_id, message_id, &full, kb).await;
    }
    if data == "ws:new" {
        WS_AWAITING_NAME
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(chat_id_i64);
        let kb = InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
            "⬅️ Cancel naming",
            "ws:new:cancel",
        )]]);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Send the new workspace name as your next message.\nAllowed: a-z, 0-9, -, _ (max 32)\nUse /cancel to abort.",
            kb,
        )
        .await;
    }
    if data == "ws:new:cancel" {
        WS_AWAITING_NAME
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id_i64);
        let (text, kb) = workspace_panel(orchestrator, chat_id_i64).await;
        return send_or_edit_workspace(bot, chat_id, message_id, &text, kb).await;
    }
    if data == "ws:clear:ask" {
        let kb = InlineKeyboardMarkup::new(vec![vec![
            InlineKeyboardButton::callback("✅ Confirm clear", "ws:clear:ok"),
            InlineKeyboardButton::callback("⬅️ Cancel", "ws:menu"),
        ]]);
        return send_or_edit_workspace(
            bot,
            chat_id,
            message_id,
            "Clear current workspace files and context?",
            kb,
        )
        .await;
    }
    if data == "ws:clear:ok" {
        let msg = orchestrator.new_workspace(chat_id_i64).await;
        let (panel_text, kb) = workspace_panel(orchestrator, chat_id_i64).await;
        let full = format!("{msg}\n\n{panel_text}\n\n▶️ Send your task now.");
        return send_or_edit_workspace(bot, chat_id, message_id, &full, kb).await;
    }
    if data == "ws:delete" {
        let active_id = orchestrator
            .db
            .get_active_workspace_id(chat_id_i64)
            .await
            .ok()
            .flatten();
        let workspaces = orchestrator
            .db
            .list_workspaces(chat_id_i64)
            .await
            .unwrap_or_default();
        let text = "Select a workspace to delete.\nIf you delete the active workspace, bot will switch to another workspace automatically.";
        let kb = workspace_delete_keyboard(&workspaces, active_id.as_deref());
        return send_or_edit_workspace(bot, chat_id, message_id, text, kb).await;
    }
    if let Some(ws_id) = data.strip_prefix("ws:delask:") {
        let name = orchestrator
            .db
            .get_workspace_by_id(ws_id)
            .await
            .ok()
            .flatten()
            .map(|w| w.name)
            .unwrap_or_else(|| "workspace".to_string());
        let kb = InlineKeyboardMarkup::new(vec![vec![
            InlineKeyboardButton::callback("🗑 Confirm delete", format!("ws:delok:{ws_id}")),
            InlineKeyboardButton::callback("⬅️ Cancel", "ws:delete"),
        ]]);
        let text = format!("Delete workspace <code>{}</code>?", escape_html(&name));
        return send_or_edit_workspace(bot, chat_id, message_id, &text, kb).await;
    }
    if let Some(ws_id) = data.strip_prefix("ws:delok:") {
        let msg = if let Ok(Some(ws)) = orchestrator.db.get_workspace_by_id(ws_id).await {
            orchestrator.workspace_delete(chat_id_i64, &ws.name).await
        } else {
            "Workspace not found.".to_string()
        };
        let (panel_text, kb) = workspace_panel(orchestrator, chat_id_i64).await;
        let ws_list = orchestrator.workspace_list(chat_id_i64).await;
        let full = format!("{msg}\n\n{panel_text}\n\n{ws_list}");
        return send_or_edit_workspace(bot, chat_id, message_id, &full, kb).await;
    }

    let (text, kb) = workspace_panel(orchestrator, chat_id_i64).await;
    send_or_edit_workspace(bot, chat_id, message_id, &text, kb).await
}
