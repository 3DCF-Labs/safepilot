use super::*;

pub(super) fn spawn_inline_progress_loop(
    bot: Bot,
    orchestrator: Arc<Orchestrator>,
    chat_id: ChatId,
    message_id: MessageId,
    run_id: String,
) {
    {
        let mut set = INLINE_PROGRESS_RUNS
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if set.contains(&run_id) {
            return;
        }
        set.insert(run_id.clone());
    }

    tokio::spawn(async move {
        let mut delay = tokio::time::Duration::from_millis(900);
        let mut last_blocked_task_id: Option<String> = None;
        loop {
            let (text, truncated_jid) = render_run_progress(&orchestrator, &run_id).await;
            let truncated_text = truncate_str(&text, 4000);
            if let Some(ref jid) = truncated_jid {
                let _ = bot
                    .edit_message_text(chat_id, message_id, truncated_text)
                    .parse_mode(ParseMode::Html)
                    .disable_web_page_preview(true)
                    .reply_markup(show_full_keyboard(jid))
                    .await;
            } else {
                let _ = bot
                    .edit_message_text(chat_id, message_id, truncated_text)
                    .parse_mode(ParseMode::Html)
                    .disable_web_page_preview(true)
                    .await;
            }

            let run_snapshot = orchestrator.db.get_run(&run_id).await.ok().flatten();
            let terminal = run_snapshot
                .as_ref()
                .map(|r| {
                    matches!(
                        r.status,
                        RunStatus::Done | RunStatus::Failed | RunStatus::Cancelled
                    )
                })
                .unwrap_or(true);
            if terminal {
                break;
            }

            let is_blocked = run_snapshot
                .as_ref()
                .map(|r| r.status == RunStatus::Blocked)
                .unwrap_or(false);

            if is_blocked {
                if let Ok(tasks) = orchestrator.db.list_tasks(&run_id).await {
                    if let Some(blocked_task) =
                        tasks.iter().find(|t| t.status == TaskStatus::Blocked)
                    {
                        let current_id = &blocked_task.task_id;
                        let already_notified = last_blocked_task_id
                            .as_ref()
                            .map(|id| id == current_id)
                            .unwrap_or(false);
                        if !already_notified {
                            let msg = orchestrator.approval_required_message(current_id).await;
                            let _ = send_message_maybe_approval(&bot, chat_id, &msg).await;
                            last_blocked_task_id = Some(current_id.clone());
                        }
                    }
                }
            } else {
                last_blocked_task_id = None;
            }

            if is_blocked {
                delay = tokio::time::Duration::from_millis(500);
            }

            tokio::time::sleep(delay).await;
            delay = (delay * 5 / 4).min(tokio::time::Duration::from_secs(4));
        }

        INLINE_PROGRESS_RUNS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&run_id);
    });
}

pub(super) async fn render_run_progress(
    orchestrator: &Orchestrator,
    run_id: &str,
) -> (String, Option<String>) {
    let Some(run) = orchestrator.db.get_run(run_id).await.ok().flatten() else {
        return ("⏳ Working on your request...".to_string(), None);
    };
    let mut tasks = orchestrator.db.list_tasks(run_id).await.unwrap_or_default();
    tasks.sort_by_key(|t| t.created_at);
    if tasks.is_empty() {
        return ("⏳ Working on your request...".to_string(), None);
    }
    let mut truncated_job_id: Option<String> = None;
    let audience = orchestrator.chat_audience(run.chat_id).await;
    let public_scope = orchestrator.public_scope_hint(run.chat_id).await;
    let is_public = audience == crate::orchestrator::Audience::Public;

    let total = tasks.len();
    let done = tasks
        .iter()
        .filter(|t| t.status == TaskStatus::Done)
        .count();

    let now = chrono::Utc::now();
    let mut modes = Vec::new();
    if run.trusted_until.as_ref().is_some_and(|d| *d > now) {
        let mins = (*run.trusted_until.as_ref().unwrap() - now).num_minutes();
        modes.push(format!("trusted ({}m left)", mins.max(1)));
    }
    if run.unsafe_until.as_ref().is_some_and(|d| *d > now) {
        let mins = (*run.unsafe_until.as_ref().unwrap() - now).num_minutes();
        modes.push(format!("unsafe ({}m left)", mins.max(1)));
    }
    if modes.is_empty() {
        modes.push("strict".to_string());
    }
    let mode_str = modes.join(", ");
    let trusted = run.trusted_until.as_ref().is_some_and(|d| *d > now);
    let unsafe_mode = run.unsafe_until.as_ref().is_some_and(|d| *d > now);

    let status_text = match run.status {
        RunStatus::Done => format!("✅ Done · {done}/{total} · {mode_str}"),
        RunStatus::Failed => format!("❌ Failed · {done}/{total} · {mode_str}"),
        RunStatus::Cancelled => format!("🚫 Cancelled · {done}/{total} · {mode_str}"),
        RunStatus::Blocked => format!("🛑 Waiting for approval · {done}/{total} · {mode_str}"),
        _ => format!("⏳ Working · {done}/{total} · {mode_str}"),
    };
    let header = format!("———— {} ————", status_text);

    let mut lines = vec![header, String::new()];
    let goal_preview = truncate_str(run.user_goal.trim(), 140);
    if !goal_preview.is_empty() {
        lines.push(format!("📝 <b>Request</b>: {}", escape_html(&goal_preview)));
    }
    lines.push("━━━━".to_string());

    let shown = 6usize.min(tasks.len());
    let hidden = tasks.len().saturating_sub(shown);
    for task in tasks.iter().skip(hidden) {
        let detail = task_goal_preview(task);
        let lifecycle = task_lifecycle_label(
            task.status,
            &task.action_type,
            task.risk_tier,
            trusted,
            unsafe_mode,
        );
        let label = task_label(&task.action_type);
        let suffix = if detail.is_empty() {
            String::new()
        } else {
            format!(": {}", detail)
        };
        lines.push(format!("{} {}{}", lifecycle, label, suffix));
    }
    if hidden > 0 {
        lines.push(format!("… and {} earlier steps", hidden));
    }

    let blocked: Vec<_> = tasks
        .iter()
        .filter(|t| t.status == TaskStatus::Blocked)
        .collect();
    if !blocked.is_empty() {
        lines.push(String::new());
        lines.push("<b>Waiting approvals:</b>".to_string());
        for task in blocked.iter().take(2) {
            let short_id = short_task_id(&task.task_id);
            lines.push(format!(
                "▸ <code>{}</code> [{}] {}",
                short_id,
                task.action_type,
                task_goal_preview(task)
            ));
        }
        if blocked.len() > 2 {
            lines.push(format!("… and {} more blocked tasks", blocked.len() - 2));
        }
    }

    let pending_approval: Vec<_> = tasks
        .iter()
        .filter(|t| {
            t.status == TaskStatus::Queued && {
                let eff = effective_risk(&t.action_type, t.risk_tier);
                !is_bypassed(eff, trusted, unsafe_mode)
            }
        })
        .collect();
    if !pending_approval.is_empty() {
        lines.push(String::new());
        lines.push("<b>Upcoming approvals:</b>".to_string());
        for task in pending_approval.iter().take(2) {
            lines.push(format!(
                "▸ [{}] {}",
                task.action_type,
                task_goal_preview(task)
            ));
        }
        if pending_approval.len() > 2 {
            lines.push(format!(
                "… and {} more tasks that may need approval",
                pending_approval.len() - 2
            ));
        }
        if is_public {
            lines.push("Awaiting operator approval before continuing.".to_string());
        } else if unsafe_mode {
            lines.push(
                "Unsafe mode active \u{2014} these will auto-approve when scheduled.".to_string(),
            );
        } else if trusted {
            lines.push("Tip: /unsafe &lt;minutes&gt; also covers dangerous tasks.".to_string());
        } else {
            lines.push(
                "Tip: /unsafe &lt;minutes&gt; approves all dangerous tasks at once.".to_string(),
            );
        }
    }

    if !matches!(
        run.status,
        RunStatus::Done | RunStatus::Failed | RunStatus::Cancelled
    ) {
        let running_shell: Vec<_> = tasks
            .iter()
            .filter(|t| {
                t.status == TaskStatus::Running
                    && matches!(t.action_type.as_str(), "shell" | "validate")
            })
            .collect();
        for task in running_shell.iter().take(1) {
            if let Some(ref job_id) = task.job_id {
                if let Some(tail) = orchestrator.get_log_tail_raw(job_id, 600).await {
                    let tail = tail.trim();
                    if !tail.is_empty() {
                        let tail_lines: Vec<&str> = tail.lines().collect();
                        let skip = tail_lines.len().saturating_sub(8);
                        let display: String = tail_lines[skip..].join("\n");
                        lines.push(String::new());
                        lines.push("📋 <b>Live output</b>".to_string());
                        lines.push(format!("<pre>{}</pre>", escape_html(&display)));
                    }
                }
            }
        }
    }

    if matches!(
        run.status,
        RunStatus::Done | RunStatus::Failed | RunStatus::Cancelled
    ) {
        if run.status == RunStatus::Failed {
            lines.push("▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔".to_string());
            if let Some((text, job_id)) = latest_failure_text(orchestrator, &tasks).await {
                let text = if is_public {
                    orchestrator.map_message_for_audience(
                        &text,
                        crate::orchestrator::Audience::Public,
                        public_scope.as_deref(),
                    )
                } else {
                    text
                };
                lines.push("❌ <b>Error</b>".to_string());
                let (block, tjid) = format_output_block(&text, job_id.as_deref(), 15, 1800);
                lines.push(block);
                if truncated_job_id.is_none() {
                    truncated_job_id = tjid;
                }
            } else {
                lines.push(if is_public {
                    "Run failed for this request scope.".to_string()
                } else {
                    "Run failed. Use <code>/jobs</code> and <code>/log &lt;job_id&gt;</code> for details, then retry."
                        .to_string()
                });
            }
        } else if let Some((text, job_id)) = latest_result_text(orchestrator, &tasks).await {
            let action_label = latest_action_label(&tasks);
            lines.push("▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔▔".to_string());
            if let Some(label) = action_label {
                lines.push(format!(
                    "⚡ <b>Action</b>: <code>{}</code>",
                    escape_html(&label)
                ));
            }
            lines.push("📤 <b>Output</b>".to_string());
            let (block, tjid) = format_output_block(&text, job_id.as_deref(), 15, 2000);
            lines.push(block);
            if truncated_job_id.is_none() {
                truncated_job_id = tjid;
            }
        }
    }

    (lines.join("\n"), truncated_job_id)
}

pub(super) fn format_output_block(
    text: &str,
    job_id: Option<&str>,
    max_lines: usize,
    max_chars: usize,
) -> (String, Option<String>) {
    let escaped = escape_html(text);
    let mut output_lines: Vec<&str> = escaped.lines().collect();
    let mut truncated = false;
    if output_lines.len() > max_lines {
        output_lines.truncate(max_lines);
        truncated = true;
    }
    let mut block = output_lines.join("\n");
    if block.len() > max_chars {
        let mut end = max_chars;
        while end > 0 && !block.is_char_boundary(end) {
            end -= 1;
        }
        block = format!("{}…", &block[..end]);
        truncated = true;
    }
    let mut result = format!("<pre>{}</pre>", block);
    let truncated_job_id = if truncated {
        if let Some(jid) = job_id {
            result.push_str("\n<i>Output truncated.</i>");
            Some(jid.to_string())
        } else {
            result.push_str("\n<i>Output truncated.</i>");
            None
        }
    } else {
        None
    };
    (result, truncated_job_id)
}

pub(super) fn show_full_keyboard(job_id: &str) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![vec![InlineKeyboardButton::callback(
        "📄 Show full output",
        format!("log:{job_id}"),
    )]])
}

pub(super) fn latest_action_label(tasks: &[crate::db::TaskRecord]) -> Option<String> {
    for task in tasks.iter().rev() {
        if task.status != TaskStatus::Done {
            continue;
        }
        if matches!(task.action_type.as_str(), "shell" | "validate") {
            let goal = task.goal.trim();
            if !goal.is_empty() {
                return Some(truncate_str(goal, 120));
            }
        }
    }
    None
}

pub(super) async fn latest_result_text(
    orchestrator: &Orchestrator,
    tasks: &[crate::db::TaskRecord],
) -> Option<(String, Option<String>)> {
    for task in tasks.iter().rev() {
        if task.status != TaskStatus::Done {
            continue;
        }
        if !matches!(task.action_type.as_str(), "shell" | "validate") {
            continue;
        }
        if let Some(result) = task_result_text(orchestrator, task).await {
            return Some(result);
        }
    }
    for task in tasks.iter().rev() {
        if task.status != TaskStatus::Done {
            continue;
        }
        if !matches!(task.action_type.as_str(), "codex" | "claude" | "merge") {
            continue;
        }
        if let Some(result) = task_result_text(orchestrator, task).await {
            return Some(result);
        }
    }
    for task in tasks.iter().rev() {
        if task.status != TaskStatus::Done {
            continue;
        }
        if task.agent == "review" {
            continue;
        }
        if let Some(result) = task_result_text(orchestrator, task).await {
            return Some(result);
        }
    }
    for task in tasks.iter().rev() {
        if task.status != TaskStatus::Done {
            continue;
        }
        if let Some(result) = task_result_text(orchestrator, task).await {
            return Some(result);
        }
    }
    None
}

pub(super) async fn task_result_text(
    orchestrator: &Orchestrator,
    task: &crate::db::TaskRecord,
) -> Option<(String, Option<String>)> {
    let job_id = task.job_id.as_ref()?;
    let job = orchestrator.db.get_job(job_id).await.ok().flatten()?;
    let result = job.result.as_deref()?;
    let trimmed = result.trim();
    if !trimmed.is_empty() {
        Some((trimmed.to_string(), Some(job_id.clone())))
    } else {
        None
    }
}

pub(super) async fn latest_failure_text(
    orchestrator: &Orchestrator,
    tasks: &[crate::db::TaskRecord],
) -> Option<(String, Option<String>)> {
    for task in tasks.iter().rev() {
        if task.status != TaskStatus::Failed {
            continue;
        }
        let Some(job_id) = task.job_id.as_ref() else {
            continue;
        };
        let Some(job) = orchestrator.db.get_job(job_id).await.ok().flatten() else {
            continue;
        };
        let result = job.result.unwrap_or_default();
        let reason = result.trim();
        if reason.is_empty() {
            return Some((
                format!(
                    "Task {} failed. Use /log {} for full logs.",
                    task_label(&task.action_type),
                    job_id
                ),
                Some(job_id.clone()),
            ));
        }
        return Some((
            format!(
                "Task {} failed:\n{}",
                task_label(&task.action_type),
                truncate_str(reason, 1400),
            ),
            Some(job_id.clone()),
        ));
    }
    None
}

pub(super) fn effective_risk(
    action_type: &str,
    stored: crate::db::RiskTier,
) -> crate::db::RiskTier {
    match action_type {
        "shell" | "validate" | "merge" => crate::db::RiskTier::Dangerous,
        _ => stored,
    }
}

pub(super) fn is_bypassed(risk: crate::db::RiskTier, trusted: bool, unsafe_mode: bool) -> bool {
    match risk {
        crate::db::RiskTier::Safe => true,
        crate::db::RiskTier::NeedsApproval => trusted || unsafe_mode,
        crate::db::RiskTier::Dangerous => unsafe_mode,
    }
}

pub(super) fn task_lifecycle_label(
    status: TaskStatus,
    action_type: &str,
    risk_tier: crate::db::RiskTier,
    trusted: bool,
    unsafe_mode: bool,
) -> &'static str {
    let eff = effective_risk(action_type, risk_tier);
    match status {
        TaskStatus::Done => "✓",
        TaskStatus::Failed => "✗",
        TaskStatus::Cancelled => "—",
        TaskStatus::Running => "⟳ Running -",
        TaskStatus::Blocked => {
            if is_bypassed(eff, trusted, unsafe_mode) {
                "⟳ Will auto-approve -"
            } else {
                "⏸ Awaiting approval -"
            }
        }
        TaskStatus::Queued => {
            if matches!(
                eff,
                crate::db::RiskTier::NeedsApproval | crate::db::RiskTier::Dangerous
            ) {
                if is_bypassed(eff, trusted, unsafe_mode) {
                    "○ Queued (auto) -"
                } else {
                    "○ Will need approval -"
                }
            } else {
                "○ Queued -"
            }
        }
    }
}

pub(super) fn task_label(action_type: &str) -> &'static str {
    match action_type {
        "search" => "Search web",
        "fetch" => "Fetch page",
        "git" => "Clone repository",
        "agent" => "Analyze and summarize",
        "codex" => "Implement changes",
        "claude" => "Implement changes",
        "validate" => "Validate changes",
        "shell" => "Run command",
        "merge" => "Merge changes",
        "github" => "Check GitHub",
        "weather" => "Get weather",
        _ => "Process task",
    }
}

pub(super) fn task_goal_preview(task: &crate::db::TaskRecord) -> String {
    let raw = truncate_str(task.goal.trim(), 90);
    if raw.is_empty() {
        return String::new();
    }
    escape_html(&raw)
}

pub(super) fn short_task_id(task_id: &str) -> String {
    if task_id.len() <= 8 {
        return task_id.to_string();
    }
    format!("…{}", &task_id[task_id.len() - 8..])
}
