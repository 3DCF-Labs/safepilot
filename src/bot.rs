mod callbacks;
mod keyboards;
mod messages;
mod progress;
mod ui_text;

use crate::db::{
    AccessRole, JobState, RunStatus, TaskStatus, WorkspaceFetchMode, WorkspaceSecurityMode,
    WorkspaceShellPack,
};
use crate::orchestrator::{ApprovalGrantScope, Orchestrator};
use crate::utils::truncate_str;
use keyboards::*;
use messages::*;
use once_cell::sync::Lazy;
use progress::*;
use regex::Regex;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use teloxide::prelude::*;
use teloxide::types::MessageId;
use teloxide::types::{CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, ParseMode};
use teloxide::utils::command::BotCommands;
use tokio_util::sync::CancellationToken;
use ui_text::*;

static FOLLOWERS: Lazy<Mutex<HashMap<i64, CancellationToken>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static WATCHING_JOBS: Lazy<Mutex<HashSet<String>>> = Lazy::new(|| Mutex::new(HashSet::new()));
static INLINE_PROGRESS_RUNS: Lazy<Mutex<HashSet<String>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));
static WS_AWAITING_NAME: Lazy<Mutex<HashSet<i64>>> = Lazy::new(|| Mutex::new(HashSet::new()));
static WS_AWAITING_DOMAIN: Lazy<Mutex<HashSet<i64>>> = Lazy::new(|| Mutex::new(HashSet::new()));
static WS_AWAITING_DOMAIN_REMOVE: Lazy<Mutex<HashSet<i64>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));
static WS_AWAITING_SKILL: Lazy<Mutex<HashSet<i64>>> = Lazy::new(|| Mutex::new(HashSet::new()));
static WS_AWAITING_SKILL_WIZARD: Lazy<Mutex<HashSet<i64>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));
static WS_AWAITING_CONNECT_TARGET: Lazy<Mutex<HashSet<i64>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));
static WS_AWAITING_SECRET_SET: Lazy<Mutex<HashSet<i64>>> = Lazy::new(|| Mutex::new(HashSet::new()));
static WS_AWAITING_SECRET_REMOVE: Lazy<Mutex<HashSet<i64>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));
static WS_BINDING_EDITOR: Lazy<Mutex<HashMap<i64, BindingEditorState>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static WS_CONNECT_WIZARD: Lazy<Mutex<HashMap<i64, ConnectWizardState>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static WS_FLOW_OWNER: Lazy<Mutex<HashMap<i64, i64>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static RE_MD_HEADER: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?m)^#{1,6}\s*(.+)$").expect("regex"));
static RE_MD_BOLD: Lazy<Regex> = Lazy::new(|| Regex::new(r"\*\*(.+?)\*\*").expect("regex"));
static RE_MD_CODE: Lazy<Regex> = Lazy::new(|| Regex::new(r"`([^`\n]+)`").expect("regex"));

#[derive(Clone, Debug, Default)]
struct BindingEditorState {
    binding: String,
    selected: HashSet<String>,
}

#[derive(Clone, Debug, Default)]
struct ConnectWizardState {
    integration: Option<String>,
    workspace_name: Option<String>,
    workspace_options: Vec<String>,
}

fn private_only_management_message() -> &'static str {
    "This setup action is private-only. Open a private chat with the bot to manage workspace settings."
}

fn is_private_management_command(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Ws
            | Command::Wslist
            | Command::Wsnew { .. }
            | Command::Wsuse { .. }
            | Command::Wsdelete { .. }
            | Command::Wsconfig
            | Command::Wsprofile
            | Command::Wsskill { .. }
            | Command::Bind { .. }
            | Command::Unbind { .. }
            | Command::Bindings
            | Command::Bindpolicy { .. }
            | Command::Connectdiscord { .. }
            | Command::Connectx { .. }
            | Command::Connecttelegram { .. }
            | Command::Connect { .. }
            | Command::Intcheck { .. }
            | Command::Wspublic
            | Command::Wscaps
            | Command::Capspreset { .. }
            | Command::Audit
            | Command::Auditf { .. }
            | Command::Auditexport { .. }
    )
}

fn has_active_workspace_flow(chat_id: i64) -> bool {
    WS_AWAITING_NAME
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains(&chat_id)
        || WS_AWAITING_DOMAIN
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&chat_id)
        || WS_AWAITING_DOMAIN_REMOVE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&chat_id)
        || WS_AWAITING_SKILL
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&chat_id)
        || WS_AWAITING_SKILL_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&chat_id)
        || WS_AWAITING_CONNECT_TARGET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&chat_id)
        || WS_AWAITING_SECRET_SET
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&chat_id)
        || WS_AWAITING_SECRET_REMOVE
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&chat_id)
        || WS_CONNECT_WIZARD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&chat_id)
}

fn claim_workspace_flow_owner(chat_id: i64, user_id: i64) {
    WS_FLOW_OWNER
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(chat_id, user_id);
}

fn workspace_flow_owner(chat_id: i64) -> Option<i64> {
    WS_FLOW_OWNER
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&chat_id)
        .copied()
}

fn clear_workspace_flow_owner_if_idle(chat_id: i64) {
    if !has_active_workspace_flow(chat_id) {
        WS_FLOW_OWNER
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&chat_id);
    }
}

#[derive(BotCommands, Clone, Debug)]
#[command(rename_rule = "lowercase", description = "Available commands:")]
enum Command {
    #[command(description = "Start the bot")]
    Start,
    #[command(description = "Show bot status")]
    Status,
    #[command(description = "List recent jobs")]
    Jobs,
    #[command(description = "Show job log: /log <job_id>")]
    Log { job_id: String },
    #[command(description = "Cancel a queued job: /cancel <job_id>")]
    Cancel { job_id: String },
    #[command(description = "Show the active run summary")]
    Run,
    #[command(description = "Show the active run plan")]
    PlanActive,
    #[command(description = "Show a run plan: /plan <run_id>")]
    Plan { run_id: String },
    #[command(description = "Set the active run: /use <run_id>")]
    Use { run_id: String },
    #[command(description = "Force the next message to start a new run")]
    Newrun,
    #[command(description = "Clear workspace and start fresh")]
    Newworkspace,
    #[command(description = "Approve a blocked task: /approve <task_id>")]
    Approve { task_id: String },
    #[command(description = "Deny a blocked task: /deny <task_id>")]
    Deny { task_id: String },
    #[command(
        description = "Bypass approvals for the active run for N minutes: /trusted <minutes>"
    )]
    Trusted { minutes: u64 },
    #[command(description = "Bypass dangerous checkpoints for N minutes: /unsafe <minutes>")]
    Unsafe { minutes: u64 },
    #[command(
        description = "Enable agent write tools for N minutes (requires AGENT_ENABLE_WRITE_TOOLS=1): /write_tools <minutes>"
    )]
    WriteTools { minutes: u64 },
    #[command(description = "Disable trusted mode for the active run")]
    Strict,
    #[command(description = "Follow the active run progress (edits one message)")]
    Follow,
    #[command(description = "Follow a run progress (edits one message): /follow_run <run_id>")]
    FollowRun { run_id: String },
    #[command(description = "Stop following run progress")]
    Unfollow,
    #[command(description = "Clear conversation context")]
    Reset,
    #[command(description = "Show concise help")]
    Help,
    #[command(description = "Project info and legal notice")]
    About,
    #[command(description = "Rotate encryption key and re-encrypt sensitive DB data")]
    Rotatekey,
    #[command(description = "Show current Telegram chat id and target info")]
    Whereami,
    #[command(description = "Show all commands")]
    HelpAll,
    #[command(description = "Show current workspace")]
    Wscurrent,
    #[command(description = "Open workspace panel")]
    Ws,
    #[command(description = "List workspaces")]
    Wslist,
    #[command(description = "Create and switch workspace: /wsnew <name>")]
    Wsnew { name: String },
    #[command(description = "Switch workspace: /wsuse <name>")]
    Wsuse { name: String },
    #[command(description = "Delete workspace: /wsdelete <name>")]
    Wsdelete { name: String },
    #[command(description = "Workspace configuration panel")]
    Wsconfig,
    #[command(description = "Show workspace role/skill profile")]
    Wsprofile,
    #[command(description = "Set custom workspace skill prompt: /wsskill <text>")]
    Wsskill { text: String },
    #[command(description = "Bind channel to workspace: /bind <integration:channel> <workspace>")]
    Bind { args: String },
    #[command(description = "Remove channel binding: /unbind <integration:channel>")]
    Unbind { binding: String },
    #[command(description = "List channel bindings")]
    Bindings,
    #[command(
        description = "Show/update channel binding policy: /bindpolicy <integration:channel> [write_policy] [allowed_actions|*] [fallback_workspace]"
    )]
    Bindpolicy { args: String },
    #[command(
        description = "Connect Discord to workspace: /connectdiscord <channel_id> <workspace>"
    )]
    Connectdiscord { args: String },
    #[command(description = "Connect X to workspace: /connectx <account_id> <workspace>")]
    Connectx { args: String },
    #[command(
        description = "Connect Telegram to workspace: /connecttelegram <chat_or_channel_id> <workspace>"
    )]
    Connecttelegram { args: String },
    #[command(
        description = "Connect any integration: /connect <integration> <target_id> <workspace>"
    )]
    Connect { args: String },
    #[command(description = "Integration readiness check: /intcheck [integration|all]")]
    Intcheck { args: String },
    #[command(description = "Public profile summary for active workspace")]
    Wspublic,
    #[command(description = "Integration capability matrix for active workspace")]
    Wscaps,
    #[command(
        description = "Apply capability preset: /capspreset <support|social|moderation|strict_readonly>"
    )]
    Capspreset { name: String },
    #[command(description = "Show recent audit trail")]
    Audit,
    #[command(description = "Show filtered audit trail: /auditf key=value ...")]
    Auditf { args: String },
    #[command(description = "Export audit trail: /auditexport key=value ...")]
    Auditexport { args: String },
}

fn command_allowed_for_role(cmd: &Command, role: AccessRole) -> bool {
    if matches!(
        cmd,
        Command::Bind { .. }
            | Command::Unbind { .. }
            | Command::Bindpolicy { .. }
            | Command::Connectdiscord { .. }
            | Command::Connectx { .. }
            | Command::Connecttelegram { .. }
            | Command::Connect { .. }
    ) {
        return Orchestrator::is_owner_role(role);
    }
    if Orchestrator::is_operator_role(role) {
        return true;
    }
    let _ = cmd;
    false
}

pub async fn run(orchestrator: Arc<Orchestrator>) {
    let bot = Bot::new(&orchestrator.config.bot_token);
    tracing::info!("Starting Telegram bot loop");
    orchestrator.bootstrap_access_control().await;

    spawn_background_reconciler(bot.clone(), orchestrator.clone());

    let handler = dptree::entry()
        .branch(
            Update::filter_message()
                .branch(
                    dptree::entry()
                        .filter_command::<Command>()
                        .endpoint(handle_command),
                )
                .branch(
                    dptree::filter(|msg: Message| msg.text().is_some()).endpoint(handle_message),
                ),
        )
        .branch(
            Update::filter_channel_post().branch(
                dptree::filter(|msg: Message| msg.text().is_some()).endpoint(handle_message),
            ),
        )
        .branch(Update::filter_callback_query().endpoint(callbacks::handle_callback));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![orchestrator])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

async fn handle_command(
    bot: Bot,
    msg: Message,
    cmd: Command,
    orchestrator: Arc<Orchestrator>,
) -> ResponseResult<()> {
    let user_id = msg.from().map(|u| u.id.0 as i64).unwrap_or(0);
    let role = orchestrator.resolve_telegram_role(user_id).await;
    if !command_allowed_for_role(&cmd, role) {
        orchestrator
            .audit_event(
                msg.chat.id.0,
                None,
                Some(&format!("telegram-user-{}", user_id)),
                Some(role.as_str()),
                crate::orchestrator::Audience::Public,
                "acl_command_denied",
                &format!("cmd={:?}", cmd),
            )
            .await;
        let _ = send_message(&bot, msg.chat.id, public_command_denied_message()).await?;
        return Ok(());
    }
    let chat_id = msg.chat.id;
    if !msg.chat.is_private()
        && Orchestrator::is_operator_role(role)
        && is_private_management_command(&cmd)
    {
        let _ = send_message(&bot, chat_id, private_only_management_message()).await?;
        return Ok(());
    }

    let mut use_approval_keyboard = false;
    let mut force_preformatted = false;
    let (response, job_ids) = match cmd {
        Command::Start => {
            if should_offer_quick_setup(&orchestrator, chat_id.0).await {
                let text = "👋 Welcome to SafePilot.\n\n🚀 <b>Quick Setup</b>\nStep 1/5: Choose workspace role (recommended: General).\n\nYou can always open this again from /wsconfig.";
                let _ = bot
                    .send_message(chat_id, text)
                    .parse_mode(ParseMode::Html)
                    .disable_web_page_preview(true)
                    .reply_markup(workspace_wizard_role_keyboard())
                    .await?;
                return Ok(());
            }
            if Orchestrator::is_operator_role(role) {
                (concise_help_text(), vec![])
            } else {
                (public_help_text(), vec![])
            }
        }
        Command::Status => {
            let mut text = orchestrator.status(chat_id.0).await;
            if let Some(task_id) = orchestrator.first_pending_approval_task_id(chat_id.0).await {
                text.push_str(&format!(
                    "\n\n🛑 Task awaiting approval.\n/approve {}\n/deny {}",
                    task_id, task_id
                ));
                use_approval_keyboard = true;
            }
            (text, vec![])
        }
        Command::Jobs => (orchestrator.list_jobs(chat_id.0).await, vec![]),
        Command::Log { job_id } => {
            let clean_id = job_id.split_whitespace().next().unwrap_or(&job_id);
            (orchestrator.get_log(clean_id).await, vec![])
        }
        Command::Cancel { job_id } => (orchestrator.cancel_job(&job_id).await, vec![]),
        Command::Run => match orchestrator.db.get_active_run(chat_id.0).await {
            Ok(Some(run_id)) => {
                let mut text = orchestrator.run_summary(&run_id).await;
                if let Some(task_id) = orchestrator.first_pending_approval_task_id(chat_id.0).await
                {
                    text.push_str(&format!(
                        "\n\n🛑 Task awaiting approval.\n/approve {}\n/deny {}",
                        task_id, task_id
                    ));
                    use_approval_keyboard = true;
                }
                (text, vec![])
            }
            Ok(None) => (
                "No active run. Send a message to create one.".into(),
                vec![],
            ),
            Err(err) => (crate::safe_error::user_facing(&err), vec![]),
        },
        Command::PlanActive => (orchestrator.plan_active_run(chat_id.0).await, vec![]),
        Command::Plan { run_id } => (orchestrator.plan_run(&run_id).await, vec![]),
        Command::Use { run_id } => (orchestrator.use_run(chat_id.0, &run_id).await, vec![]),
        Command::Newrun => (orchestrator.new_run(chat_id.0).await, vec![]),
        Command::Newworkspace => (orchestrator.new_workspace(chat_id.0).await, vec![]),
        Command::Approve { task_id } => orchestrator.approve_task(&task_id).await,
        Command::Deny { task_id } => (orchestrator.deny_task(&task_id).await, vec![]),
        Command::Trusted { minutes } => orchestrator.trusted_active_run(chat_id.0, minutes).await,
        Command::Unsafe { minutes } => orchestrator.unsafe_active_run(chat_id.0, minutes).await,
        Command::WriteTools { minutes } => (
            orchestrator
                .write_tools_active_run(chat_id.0, minutes)
                .await,
            vec![],
        ),
        Command::Strict => (orchestrator.strict_active_run(chat_id.0).await, vec![]),
        Command::Follow => {
            let run_id = orchestrator
                .db
                .get_active_run(chat_id.0)
                .await
                .ok()
                .flatten();
            if let Some(run_id) = run_id {
                stop_follow(chat_id.0);
                let token = CancellationToken::new();
                FOLLOWERS
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(chat_id.0, token.clone());

                let initial = format!("🔎 Following `{}`…", run_id);
                let sent = send_message(&bot, chat_id, &initial).await?;
                spawn_follow_loop(
                    bot.clone(),
                    orchestrator.clone(),
                    chat_id,
                    sent.id,
                    run_id,
                    token,
                );

                ("✅ Follow started (editing this message).".into(), vec![])
            } else {
                ("No active run to follow.".into(), vec![])
            }
        }
        Command::FollowRun { run_id } => {
            stop_follow(chat_id.0);
            let token = CancellationToken::new();
            FOLLOWERS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(chat_id.0, token.clone());

            let initial = format!("🔎 Following `{}`…", run_id);
            let sent = send_message(&bot, chat_id, &initial).await?;
            spawn_follow_loop(
                bot.clone(),
                orchestrator.clone(),
                chat_id,
                sent.id,
                run_id,
                token,
            );

            ("✅ Follow started (editing this message).".into(), vec![])
        }
        Command::Unfollow => {
            stop_follow(chat_id.0);
            ("✅ Follow stopped.".into(), vec![])
        }
        Command::Reset => (orchestrator.reset(chat_id.0).await, vec![]),
        Command::Help => {
            if Orchestrator::is_operator_role(role) {
                (concise_help_text(), vec![])
            } else {
                (public_help_text(), vec![])
            }
        }
        Command::About => (about_text(), vec![]),
        Command::Rotatekey => (
            orchestrator.rotate_encryption_master_key(chat_id.0).await,
            vec![],
        ),
        Command::Whereami => {
            let chat_kind = if msg.chat.is_private() {
                "private"
            } else if msg.chat.is_group() {
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
            (
                format!(
                    "📍 <b>Current Chat</b>\nType: <code>{}</code>\nTitle: <code>{}</code>\nUsername: <code>{}</code>\nchat_id: <code>{}</code>\n\nUse this in connect:\n<code>/connecttelegram {} WORKSPACE_NAME</code>",
                    chat_kind,
                    title,
                    username,
                    msg.chat.id.0,
                    msg.chat.id.0
                ),
                vec![],
            )
        }
        Command::HelpAll => (Command::descriptions().to_string(), vec![]),
        Command::Wscurrent => (orchestrator.workspace_current(chat_id.0).await, vec![]),
        Command::Ws => {
            let _ = orchestrator.workspace_current(chat_id.0).await;
            let (text, kb) = workspace_panel(&orchestrator, chat_id.0).await;
            let rendered = truncate_str(&text, 4000);
            let _ = bot
                .send_message(chat_id, rendered)
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(kb)
                .await?;
            return Ok(());
        }
        Command::Wslist => (orchestrator.workspace_list(chat_id.0).await, vec![]),
        Command::Wsnew { name } => {
            let msg = orchestrator.workspace_create(chat_id.0, &name).await;
            if msg.starts_with("✅") {
                let full = format!(
                    "{}\n\n🚀 Quick Setup\nStep 1/5: Choose workspace role (recommended: General).",
                    msg
                );
                let _ = bot
                    .send_message(chat_id, truncate_str(&full, 4000))
                    .parse_mode(ParseMode::Html)
                    .disable_web_page_preview(true)
                    .reply_markup(workspace_wizard_role_keyboard())
                    .await?;
                return Ok(());
            }
            (msg, vec![])
        }
        Command::Wsuse { name } => (orchestrator.workspace_use(chat_id.0, &name).await, vec![]),
        Command::Wsdelete { name } => (
            orchestrator.workspace_delete(chat_id.0, &name).await,
            vec![],
        ),
        Command::Wsconfig => {
            let (text, kb) = workspace_config_panel(&orchestrator, chat_id.0).await;
            let _ = bot
                .send_message(chat_id, truncate_str(&text, 4000))
                .parse_mode(ParseMode::Html)
                .disable_web_page_preview(true)
                .reply_markup(kb)
                .await?;
            return Ok(());
        }
        Command::Wsprofile => (
            orchestrator.workspace_profile_summary(chat_id.0).await,
            vec![],
        ),
        Command::Wsskill { text } => (
            orchestrator
                .workspace_set_skill_prompt(chat_id.0, &text)
                .await,
            vec![],
        ),
        Command::Bind { args } => {
            let mut parts = args.split_whitespace();
            let binding = parts.next().unwrap_or_default();
            let workspace = parts.next().unwrap_or_default();
            if binding.is_empty() || workspace.is_empty() {
                (
                    "Usage: /bind <integration:channel> <workspace>".to_string(),
                    vec![],
                )
            } else {
                (
                    orchestrator
                        .bind_channel_to_workspace(chat_id.0, binding, workspace)
                        .await,
                    vec![],
                )
            }
        }
        Command::Unbind { binding } => (
            orchestrator.unbind_channel(chat_id.0, &binding).await,
            vec![],
        ),
        Command::Bindings => {
            force_preformatted = true;
            (orchestrator.list_channel_bindings(chat_id.0).await, vec![])
        }
        Command::Bindpolicy { args } => {
            let mut parts = args.split_whitespace();
            let binding = parts.next().unwrap_or_default();
            if binding.is_empty() {
                (
                    "Usage:\n/bindpolicy <integration:channel>\n/bindpolicy <integration:channel> <write_policy> <allowed_actions|*> [fallback_workspace]".to_string(),
                    vec![],
                )
            } else {
                let write_policy = parts.next();
                let allowed = parts.next();
                let fallback = parts.next();
                if write_policy.is_none() || allowed.is_none() {
                    (
                        orchestrator
                            .binding_policy_summary(chat_id.0, binding)
                            .await,
                        vec![],
                    )
                } else {
                    (
                        orchestrator
                            .update_binding_policy(
                                chat_id.0,
                                binding,
                                write_policy.unwrap_or_default(),
                                allowed.unwrap_or_default(),
                                fallback,
                            )
                            .await,
                        vec![],
                    )
                }
            }
        }
        Command::Connectdiscord { args } => {
            if args.trim().is_empty() {
                (
                    orchestrator
                        .connect_integration_help(chat_id.0, "discord")
                        .await,
                    vec![],
                )
            } else {
                let mut parts = args.split_whitespace();
                let target_id = parts.next().unwrap_or_default();
                let workspace = parts.next().unwrap_or_default();
                if target_id.is_empty() || workspace.is_empty() {
                    (
                        "Usage: /connectdiscord <channel_id> <workspace>".to_string(),
                        vec![],
                    )
                } else {
                    (
                        orchestrator
                            .connect_integration_binding(chat_id.0, "discord", target_id, workspace)
                            .await,
                        vec![],
                    )
                }
            }
        }
        Command::Connectx { args } => {
            if args.trim().is_empty() {
                (
                    orchestrator.connect_integration_help(chat_id.0, "x").await,
                    vec![],
                )
            } else {
                let mut parts = args.split_whitespace();
                let target_id = parts.next().unwrap_or_default();
                let workspace = parts.next().unwrap_or_default();
                if target_id.is_empty() || workspace.is_empty() {
                    (
                        "Usage: /connectx <account_id> <workspace>".to_string(),
                        vec![],
                    )
                } else {
                    (
                        orchestrator
                            .connect_integration_binding(chat_id.0, "x", target_id, workspace)
                            .await,
                        vec![],
                    )
                }
            }
        }
        Command::Connecttelegram { args } => {
            if args.trim().is_empty() {
                (
                    orchestrator
                        .connect_integration_help(chat_id.0, "telegram")
                        .await,
                    vec![],
                )
            } else {
                let mut parts = args.split_whitespace();
                let target_id = parts.next().unwrap_or_default();
                let workspace = parts.next().unwrap_or_default();
                if target_id.is_empty() || workspace.is_empty() {
                    (
                        "Usage: /connecttelegram <chat_or_channel_id> <workspace>".to_string(),
                        vec![],
                    )
                } else {
                    (
                        orchestrator
                            .connect_integration_binding(
                                chat_id.0, "telegram", target_id, workspace,
                            )
                            .await,
                        vec![],
                    )
                }
            }
        }
        Command::Connect { args } => {
            let mut parts = args.split_whitespace();
            let integration = parts.next().unwrap_or_default().to_ascii_lowercase();
            if integration.is_empty() {
                (
                    "Usage: /connect <integration> <target_id> <workspace>\nExample: /connect slack C123 workspace-2".to_string(),
                    vec![],
                )
            } else {
                let target_id = parts.next().unwrap_or_default();
                let workspace = parts.next().unwrap_or_default();
                if target_id.is_empty() || workspace.is_empty() {
                    (
                        orchestrator
                            .connect_integration_help(chat_id.0, &integration)
                            .await,
                        vec![],
                    )
                } else {
                    (
                        orchestrator
                            .connect_integration_binding(
                                chat_id.0,
                                &integration,
                                target_id,
                                workspace,
                            )
                            .await,
                        vec![],
                    )
                }
            }
        }
        Command::Intcheck { args } => {
            force_preformatted = true;
            (
                orchestrator
                    .integration_readiness_report(chat_id.0, args.trim())
                    .await,
                vec![],
            )
        }
        Command::Wspublic => (
            orchestrator.workspace_public_summary(chat_id.0).await,
            vec![],
        ),
        Command::Wscaps => {
            force_preformatted = true;
            (
                orchestrator
                    .workspace_integration_caps_summary(chat_id.0)
                    .await,
                vec![],
            )
        }
        Command::Capspreset { name } => {
            force_preformatted = true;
            (
                orchestrator
                    .workspace_apply_caps_template(chat_id.0, &name)
                    .await,
                vec![],
            )
        }
        Command::Audit => {
            force_preformatted = true;
            (orchestrator.audit_recent(chat_id.0, 30).await, vec![])
        }
        Command::Auditf { args } => {
            force_preformatted = true;
            (orchestrator.audit_filtered(chat_id.0, &args).await, vec![])
        }
        Command::Auditexport { args } => {
            force_preformatted = true;
            (orchestrator.audit_export(chat_id.0, &args).await, vec![])
        }
    };

    let sent = if use_approval_keyboard {
        send_message_maybe_approval(&bot, chat_id, &response).await?
    } else if force_preformatted && !(response.contains("<b>") || response.contains("<code>")) {
        send_message_preformatted(&bot, chat_id, &response).await?
    } else {
        send_message(&bot, chat_id, &response).await?
    };

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

async fn send_message(bot: &Bot, chat_id: ChatId, text: &str) -> ResponseResult<Message> {
    let clean = if text.trim().is_empty() {
        "⚠️ Empty response from model; please retry.".to_string()
    } else {
        text.to_string()
    };
    let raw = truncate_str(&clean, 3600);
    let rendered = truncate_str(&format_for_telegram_html(&raw), 4000);
    bot.send_message(chat_id, rendered)
        .parse_mode(ParseMode::Html)
        .disable_web_page_preview(true)
        .await
}

async fn send_message_preformatted(
    bot: &Bot,
    chat_id: ChatId,
    text: &str,
) -> ResponseResult<Message> {
    let clean = if text.trim().is_empty() {
        "⚠️ Empty response from model; please retry.".to_string()
    } else {
        text.to_string()
    };
    let wrapped = format!("<pre>{}</pre>", escape_html(&truncate_str(&clean, 3600)));
    bot.send_message(chat_id, truncate_str(&wrapped, 4000))
        .parse_mode(ParseMode::Html)
        .disable_web_page_preview(true)
        .await
}

async fn send_message_maybe_approval(
    bot: &Bot,
    chat_id: ChatId,
    text: &str,
) -> ResponseResult<Message> {
    let safe_text = if text.trim().is_empty() {
        "⚠️ Empty response from model; please retry."
    } else {
        text
    };
    if let Some(task_id) = extract_blocked_task_id(safe_text) {
        let raw = truncate_str(safe_text, 3600);
        let rendered = truncate_str(&format_for_telegram_html(&raw), 4000);
        bot.send_message(chat_id, rendered)
            .parse_mode(ParseMode::Html)
            .disable_web_page_preview(true)
            .reply_markup(approval_keyboard(&task_id))
            .await
    } else {
        send_message(bot, chat_id, safe_text).await
    }
}

async fn workspace_panel(
    orchestrator: &Orchestrator,
    chat_id: i64,
) -> (String, InlineKeyboardMarkup) {
    let active_id = orchestrator
        .db
        .get_active_workspace_id(chat_id)
        .await
        .ok()
        .flatten();
    let workspaces = orchestrator
        .db
        .list_workspaces(chat_id)
        .await
        .unwrap_or_default();
    let active_name = workspaces
        .iter()
        .find(|w| active_id.as_deref() == Some(w.workspace_id.as_str()))
        .map(|w| w.name.clone())
        .unwrap_or_else(|| "default".to_string());
    let text = format!(
        "📁 Workspace panel\nCurrent workspace: <code>{}</code>\nYou can switch, create, clear, or delete workspaces.",
        escape_html(&active_name)
    );
    let kb = workspace_panel_keyboard();
    (text, kb)
}

async fn workspace_config_panel(
    orchestrator: &Orchestrator,
    chat_id: i64,
) -> (String, InlineKeyboardMarkup) {
    let raw = orchestrator.workspace_config_summary(chat_id).await;
    let summary = raw
        .replace("⚙️ Workspace config", "⚙️ Workspace Settings")
        .replace("Workspace:", "Current workspace:")
        .replace("Role preset:", "Workspace role:")
        .replace("Custom skill:", "Role instructions:")
        .replace("Allowed tools:", "Role tool scope:")
        .replace("Mode:", "Safety mode:")
        .replace("Shell profile:", "Tools profile:")
        .replace("Fetch policy:", "Network policy:");
    let text = format!(
        "{}\n\nStart here: 1) Role & Skill, 2) Safety, 3) Network, 4) Integrations.\nAdvanced: Tools, Secrets.",
        summary
    );
    let kb = workspace_config_keyboard();
    (text, kb)
}

fn binding_action_catalog() -> &'static [&'static str] {
    &[
        "agent",
        "search",
        "fetch",
        "telegram",
        "discord",
        "x",
        "slack",
        "notion",
        "github",
        "linear",
        "todoist",
        "jira",
        "shell",
        "list_files",
        "read_file",
    ]
}

fn binding_action_label(action: &str) -> String {
    let title = match action {
        "agent" => "Agent",
        "search" => "Search",
        "fetch" => "Fetch",
        "telegram" => "Telegram",
        "discord" => "Discord",
        "x" => "X",
        "slack" => "Slack",
        "notion" => "Notion",
        "github" => "GitHub",
        "linear" => "Linear",
        "todoist" => "Todoist",
        "jira" => "Jira",
        "shell" => "Shell",
        "list_files" => "List Files",
        "read_file" => "Read File",
        _ => action,
    };
    title.to_string()
}

fn workspace_binding_actions_keyboard(
    binding: &str,
    state: &BindingEditorState,
) -> InlineKeyboardMarkup {
    let mut rows: Vec<Vec<InlineKeyboardButton>> = Vec::new();
    let mut current: Vec<InlineKeyboardButton> = Vec::new();
    for action in binding_action_catalog() {
        let selected = state.selected.contains(*action);
        let mark = if selected { "✅" } else { "⬜" };
        current.push(InlineKeyboardButton::callback(
            format!("{} {}", mark, binding_action_label(action)),
            format!("ws:cfg:binding:actions:toggle:{}", action),
        ));
        if current.len() == 2 {
            rows.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        rows.push(current);
    }
    rows.push(vec![
        InlineKeyboardButton::callback("Allow Any (*)", "ws:cfg:binding:actions:any"),
        InlineKeyboardButton::callback("Reset", "ws:cfg:binding:actions:reset"),
    ]);
    rows.push(vec![InlineKeyboardButton::callback(
        "💾 Save",
        "ws:cfg:binding:actions:save",
    )]);
    rows.push(vec![InlineKeyboardButton::callback(
        "⬅️ Back to Binding",
        format!("ws:cfg:binding:edit:{}", binding),
    )]);
    InlineKeyboardMarkup::new(rows)
}

fn binding_actions_editor_text(state: &BindingEditorState) -> String {
    let actions = if state.selected.is_empty() {
        "* (any action allowed)".to_string()
    } else {
        let mut vals = state.selected.iter().cloned().collect::<Vec<_>>();
        vals.sort();
        vals.join(", ")
    };
    format!(
        "✏️ Custom allowed actions\nBinding: `{}`\nCurrent selection: {}\n\nToggle actions, then click `Save Allowed Actions`.",
        state.binding, actions
    )
}

fn parse_binding_target(raw: &str) -> Option<(String, String)> {
    let (integration, channel_id) = raw.split_once(':')?;
    let integration = integration.trim().to_ascii_lowercase();
    let channel_id = channel_id.trim().to_string();
    if integration.is_empty() || channel_id.is_empty() {
        return None;
    }
    Some((integration, channel_id))
}

fn extract_resolved_chat_id(message: &str) -> Option<String> {
    let marker = "Resolved chat_id:";
    let idx = message.find(marker)?;
    let rest = message[idx + marker.len()..].trim();
    let first_token = rest.split_whitespace().next().unwrap_or_default();
    let value = first_token.trim_matches('`').trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn extract_connect_diagnostics(message: &str) -> Vec<String> {
    message
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with('⚠') || l.starts_with('❌'))
        .map(|s| s.to_string())
        .collect()
}

fn parse_explicit_code_intent(original: &str) -> Option<(String, String)> {
    let lower = original.to_ascii_lowercase();
    let prefixes = [
        ("run codex to ", "codex"),
        ("run codex ", "codex"),
        ("use codex to ", "codex"),
        ("use codex ", "codex"),
        ("codex: ", "codex"),
        ("run claude to ", "claude"),
        ("run claude ", "claude"),
        ("use claude to ", "claude"),
        ("use claude ", "claude"),
        ("claude: ", "claude"),
    ];
    for (prefix, action) in &prefixes {
        if lower.starts_with(prefix) {
            let goal = original[prefix.len()..].trim();
            if !goal.is_empty() {
                return Some((action.to_string(), goal.to_string()));
            }
        }
    }
    None
}

fn extract_blocked_task_id(text: &str) -> Option<String> {
    if let Some(pos) = text.find("/deny ") {
        let rest = &text[pos + 6..];
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '`' || c == '\n')
            .unwrap_or(rest.len());
        let task_id = rest[..end].trim();
        if !task_id.is_empty() {
            return Some(task_id.to_string());
        }
    }
    if let Some(start) = text.find("Approval required for `") {
        let rest = &text[start + 23..];
        if let Some(end) = rest.find('`') {
            let task_id = rest[..end].trim();
            if !task_id.is_empty() {
                return Some(task_id.to_string());
            }
        }
    }
    None
}

fn spawn_job_watcher(bot: Bot, orchestrator: Arc<Orchestrator>, job_id: String) {
    {
        let mut set = WATCHING_JOBS.lock().unwrap_or_else(|e| e.into_inner());
        if set.contains(&job_id) {
            return;
        }
        set.insert(job_id.clone());
    }

    tokio::spawn(async move {
        let start = tokio::time::Instant::now();
        let max_wait =
            tokio::time::Duration::from_secs((orchestrator.config.job_timeout_secs + 120).max(600));
        let mut delay = tokio::time::Duration::from_secs(2);

        loop {
            tokio::time::sleep(delay).await;
            match orchestrator.db.get_job(&job_id).await {
                Ok(Some(job)) => match job.state {
                    JobState::Done | JobState::Failed | JobState::Cancelled => {
                        let (new_jobs, blocked, notices) =
                            orchestrator.on_job_terminal_state(&job_id).await;
                        let audience = orchestrator.chat_audience(job.chat_id).await;
                        let scope_hint = orchestrator.public_scope_hint(job.chat_id).await;
                        for notice in notices {
                            let out = if audience == crate::orchestrator::Audience::Public {
                                orchestrator.map_message_for_audience(
                                    &notice,
                                    crate::orchestrator::Audience::Public,
                                    scope_hint.as_deref(),
                                )
                            } else {
                                notice
                            };
                            let _ = send_message(&bot, ChatId(job.chat_id), &out).await;
                        }
                        if let Some(ref task_id) = blocked {
                            let run_tracked =
                                if let Ok(Some(task)) = orchestrator.db.get_task(task_id).await {
                                    INLINE_PROGRESS_RUNS
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner())
                                        .contains(&task.run_id)
                                } else {
                                    false
                                };
                            if !run_tracked && audience != crate::orchestrator::Audience::Public {
                                let msg = orchestrator.approval_required_message(task_id).await;
                                let _ =
                                    send_message_maybe_approval(&bot, ChatId(job.chat_id), &msg)
                                        .await;
                            }
                        }
                        for j in &new_jobs {
                            spawn_job_watcher(bot.clone(), orchestrator.clone(), j.clone());
                        }
                        break;
                    }
                    _ => {
                        if start.elapsed() > max_wait {
                            break;
                        }
                    }
                },
                Ok(None) => break,
                Err(_) => break,
            }

            delay = (delay * 3 / 2).min(tokio::time::Duration::from_secs(20));
        }

        WATCHING_JOBS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&job_id);
    });
}

async fn maybe_spawn_inline_progress(
    bot: Bot,
    orchestrator: Arc<Orchestrator>,
    chat_id: ChatId,
    message_id: MessageId,
    job_ids: &[String],
) {
    if job_ids.is_empty() {
        return;
    }
    if FOLLOWERS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .contains_key(&chat_id.0)
    {
        return;
    }
    let run_id = orchestrator
        .db
        .get_active_run(chat_id.0)
        .await
        .ok()
        .flatten();
    if let Some(run_id) = run_id {
        spawn_inline_progress_loop(bot, orchestrator, chat_id, message_id, run_id);
    }
}

async fn takeover_inline_progress(
    bot: Bot,
    orchestrator: Arc<Orchestrator>,
    chat_id: ChatId,
    message_id: MessageId,
) {
    let run_id = orchestrator
        .db
        .get_active_run(chat_id.0)
        .await
        .ok()
        .flatten();
    let Some(run_id) = run_id else { return };
    let run = orchestrator.db.get_run(&run_id).await.ok().flatten();
    let is_terminal = run
        .as_ref()
        .map(|r| {
            matches!(
                r.status,
                RunStatus::Done | RunStatus::Failed | RunStatus::Cancelled
            )
        })
        .unwrap_or(true);
    if is_terminal {
        return;
    }
    {
        INLINE_PROGRESS_RUNS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&run_id);
    }
    spawn_inline_progress_loop(bot, orchestrator, chat_id, message_id, run_id);
}

fn stop_follow(chat_id: i64) {
    if let Some(token) = FOLLOWERS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&chat_id)
    {
        token.cancel();
    }
}

fn spawn_follow_loop(
    bot: Bot,
    orchestrator: Arc<Orchestrator>,
    chat_id: ChatId,
    message_id: MessageId,
    run_id: String,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        let mut delay = tokio::time::Duration::from_secs(2);
        loop {
            if cancel.is_cancelled() {
                break;
            }

            let (rendered, truncated_jid) = render_run_progress(&orchestrator, &run_id).await;
            let truncated_text = truncate_str(&rendered, 4000);
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

            match orchestrator.db.get_run(&run_id).await {
                Ok(Some(run)) => {
                    if matches!(
                        run.status,
                        crate::db::RunStatus::Done
                            | crate::db::RunStatus::Failed
                            | crate::db::RunStatus::Cancelled
                    ) {
                        break;
                    }
                }
                _ => break,
            }

            tokio::select! {
                _ = tokio::time::sleep(delay) => {},
                _ = cancel.cancelled() => break,
            }
            delay = (delay * 3 / 2).min(tokio::time::Duration::from_secs(10));
        }

        stop_follow(chat_id.0);
    });
}

fn spawn_background_reconciler(bot: Bot, orchestrator: Arc<Orchestrator>) {
    tokio::spawn(async move {
        let mut delay = tokio::time::Duration::from_secs(3);
        let mut notified_expired: HashSet<String> = HashSet::new();
        loop {
            let runs = orchestrator
                .db
                .list_incomplete_runs(50)
                .await
                .unwrap_or_default();

            for run in &runs {
                let (new_job_ids, _blocked) = orchestrator.reconcile_run(&run.run_id).await;
                for job_id in new_job_ids {
                    spawn_job_watcher(bot.clone(), orchestrator.clone(), job_id);
                }

                if let Ok(tasks) = orchestrator.db.list_tasks(&run.run_id).await {
                    for t in tasks {
                        let Some(job_id) = t.job_id else { continue };
                        if let Ok(Some(job)) = orchestrator.db.get_job(&job_id).await {
                            if matches!(job.state, JobState::Queued | JobState::Running) {
                                spawn_job_watcher(bot.clone(), orchestrator.clone(), job_id);
                            }
                        }
                    }
                }

                let now = chrono::Utc::now();
                let trusted_expired = run
                    .trusted_until
                    .as_ref()
                    .is_some_and(|d| *d < now && (*d + chrono::Duration::seconds(20)) > now);
                let unsafe_expired = run
                    .unsafe_until
                    .as_ref()
                    .is_some_and(|d| *d < now && (*d + chrono::Duration::seconds(20)) > now);
                if (trusted_expired || unsafe_expired) && !notified_expired.contains(&run.run_id) {
                    notified_expired.insert(run.run_id.clone());
                    let mode = if trusted_expired && unsafe_expired {
                        "Trusted and unsafe modes expired"
                    } else if trusted_expired {
                        "Trusted mode expired"
                    } else {
                        "Unsafe mode expired"
                    };
                    let msg = format!(
                        "{} for run `{}`. Approval checks are active again.",
                        mode, run.run_id
                    );
                    let _ = send_message(&bot, ChatId(run.chat_id), &msg).await;
                }
            }

            tokio::time::sleep(delay).await;
            if runs.is_empty() {
                delay = tokio::time::Duration::from_secs(3);
            } else {
                delay = (delay * 3 / 2).min(tokio::time::Duration::from_secs(15));
            }
        }
    });
}

fn format_for_telegram_html(input: &str) -> String {
    let preserved = preserve_allowed_html_tags(input);
    let escaped = escape_html(&preserved);
    let escaped = restore_preserved_html_tags(&escaped);
    let mut lines = Vec::new();
    for line in escaped.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            lines.push(String::new());
            continue;
        }
        if let Some(h) = canonical_heading(trimmed) {
            lines.push(h.to_string());
            continue;
        }
        if let Some(item) = strip_list_prefix(trimmed) {
            lines.push(format!("• {}", item));
            continue;
        }
        lines.push(line.to_string());
    }

    let joined = lines.join("\n");
    let with_headers = RE_MD_HEADER.replace_all(&joined, "<b>$1</b>").to_string();
    let with_bold = RE_MD_BOLD
        .replace_all(&with_headers, "<b>$1</b>")
        .to_string();
    RE_MD_CODE
        .replace_all(&with_bold, "<code>$1</code>")
        .to_string()
}

fn preserve_allowed_html_tags(input: &str) -> String {
    input
        .replace("<b>", "__TG_B_OPEN__")
        .replace("</b>", "__TG_B_CLOSE__")
        .replace("<code>", "__TG_CODE_OPEN__")
        .replace("</code>", "__TG_CODE_CLOSE__")
        .replace("<pre>", "__TG_PRE_OPEN__")
        .replace("</pre>", "__TG_PRE_CLOSE__")
        .replace("<i>", "__TG_I_OPEN__")
        .replace("</i>", "__TG_I_CLOSE__")
        .replace("<u>", "__TG_U_OPEN__")
        .replace("</u>", "__TG_U_CLOSE__")
        .replace("<s>", "__TG_S_OPEN__")
        .replace("</s>", "__TG_S_CLOSE__")
}

fn restore_preserved_html_tags(input: &str) -> String {
    input
        .replace("__TG_B_OPEN__", "<b>")
        .replace("__TG_B_CLOSE__", "</b>")
        .replace("__TG_CODE_OPEN__", "<code>")
        .replace("__TG_CODE_CLOSE__", "</code>")
        .replace("__TG_PRE_OPEN__", "<pre>")
        .replace("__TG_PRE_CLOSE__", "</pre>")
        .replace("__TG_I_OPEN__", "<i>")
        .replace("__TG_I_CLOSE__", "</i>")
        .replace("__TG_U_OPEN__", "<u>")
        .replace("__TG_U_CLOSE__", "</u>")
        .replace("__TG_S_OPEN__", "<s>")
        .replace("__TG_S_CLOSE__", "</s>")
}

fn escape_html(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 32);
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    out
}

fn canonical_heading(line: &str) -> Option<&'static str> {
    let cleaned = line
        .trim()
        .trim_matches(|c: char| c == '*' || c == '#' || c == '_' || c.is_whitespace())
        .trim_end_matches(':')
        .to_ascii_lowercase();
    match cleaned.as_str() {
        "summary" | "overview" => Some("📌 <b>Summary</b>"),
        "key points" | "highlights" | "findings" => Some("🔍 <b>Key Points</b>"),
        "sources" | "references" | "links" => Some("🔗 <b>Sources</b>"),
        "next" | "next steps" | "recommendation" | "recommendations" | "verdict" => {
            Some("➡️ <b>Next</b>")
        }
        _ => None,
    }
}

fn strip_list_prefix(line: &str) -> Option<&str> {
    if let Some(rest) = line.strip_prefix("- ") {
        return Some(rest.trim());
    }
    if let Some(rest) = line.strip_prefix("* ") {
        return Some(rest.trim());
    }
    if let Some(rest) = line.strip_prefix("• ") {
        return Some(rest.trim());
    }

    let bytes = line.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i > 0 && i + 1 < bytes.len() && bytes[i] == b'.' && bytes[i + 1] == b' ' {
        return Some(line[i + 2..].trim());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_mode_shows_approval_needed() {
        assert_eq!(
            task_lifecycle_label(
                TaskStatus::Queued,
                "shell",
                crate::db::RiskTier::Dangerous,
                false,
                false
            ),
            "○ Will need approval -"
        );
        assert_eq!(
            task_lifecycle_label(
                TaskStatus::Blocked,
                "shell",
                crate::db::RiskTier::Dangerous,
                false,
                false
            ),
            "⏸ Awaiting approval -"
        );
    }

    #[test]
    fn unsafe_mode_shows_auto() {
        assert_eq!(
            task_lifecycle_label(
                TaskStatus::Queued,
                "shell",
                crate::db::RiskTier::Dangerous,
                false,
                true
            ),
            "○ Queued (auto) -"
        );
        assert_eq!(
            task_lifecycle_label(
                TaskStatus::Blocked,
                "shell",
                crate::db::RiskTier::Dangerous,
                false,
                true
            ),
            "⟳ Will auto-approve -"
        );
    }

    #[test]
    fn trusted_mode_bypasses_needs_approval_not_dangerous() {
        assert_eq!(
            task_lifecycle_label(
                TaskStatus::Queued,
                "codex",
                crate::db::RiskTier::NeedsApproval,
                true,
                false
            ),
            "○ Queued (auto) -"
        );
        assert_eq!(
            task_lifecycle_label(
                TaskStatus::Queued,
                "shell",
                crate::db::RiskTier::NeedsApproval,
                true,
                false
            ),
            "○ Will need approval -"
        );
    }

    #[test]
    fn effective_risk_forces_dangerous_for_shell() {
        assert_eq!(
            effective_risk("shell", crate::db::RiskTier::Safe),
            crate::db::RiskTier::Dangerous
        );
        assert_eq!(
            effective_risk("validate", crate::db::RiskTier::NeedsApproval),
            crate::db::RiskTier::Dangerous
        );
        assert_eq!(
            effective_risk("merge", crate::db::RiskTier::Safe),
            crate::db::RiskTier::Dangerous
        );
        assert_eq!(
            effective_risk("codex", crate::db::RiskTier::NeedsApproval),
            crate::db::RiskTier::NeedsApproval
        );
    }

    #[test]
    fn formatter_renders_markdown_and_headings() {
        let input = "2. **Relationship News**: The rest\nSources:\n- People\n- Billboard";
        let out = format_for_telegram_html(input);
        assert!(out.contains("• <b>Relationship News</b>: The rest"));
        assert!(out.contains("🔗 <b>Sources</b>"));
        assert!(out.contains("• People"));
    }

    #[test]
    fn formatter_escapes_html() {
        let input = "Summary:\n<script>alert(1)</script>";
        let out = format_for_telegram_html(input);
        assert!(out.contains("📌 <b>Summary</b>"));
        assert!(out.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
    }
}
