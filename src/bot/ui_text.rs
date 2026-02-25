use crate::utils::truncate_str;

pub fn concise_help_text() -> String {
    [
        "Quick start:",
        "/ws - workspace panel (create/switch/configure)",
        "/wscurrent - current workspace status",
        "/run - active run summary",
        "/follow - live progress updates",
        "/status - bot health and queue",
        "/approve <task_id> - approve blocked task",
        "/deny <task_id> - deny blocked task",
        "/rotatekey - rotate DB encryption key",
        "",
        "Integrations:",
        "/whereami - show this chat id (for Telegram connect)",
        "/ws -> Public Runtime -> Connect Integration (recommended)",
        "/connect <integration> <target_id> <workspace> - manual connect (advanced)",
        "/intcheck [integration|all] - verify token/bindings/access",
        "",
        "About:",
        "/about - project links, version, and usage notice",
        "",
        "Need everything?",
        "/helpall - full command list",
    ]
    .join("\n")
}

pub fn about_text() -> String {
    let version = env!("CARGO_PKG_VERSION");
    let hash = option_env!("GIT_COMMIT_HASH")
        .or(option_env!("VERGEN_GIT_SHA"))
        .or(option_env!("SOURCE_VERSION"))
        .map(|s| truncate_str(s, 12))
        .unwrap_or_else(|| "local".to_string());
    format!(
        "ℹ️ <b>SafePilot</b>\n\
------------------------------------------------------------------------------\n\
<b>GitHub:</b> https://github.com/3DCF-Labs/safepilot\n\
<b>Version:</b> v{} ({})\n\
Open source (Apache-2.0), self-hosted AI assistant managed by this instance operator. The operator is responsible for configuration, integrations, and actions executed through this bot. Use at your own risk and in compliance with applicable laws and platform policies.",
        version, hash
    )
}

pub fn public_help_text() -> String {
    [
        "Available in this channel:",
        "- Send normal requests in the configured assistant scope",
        "- Management/configuration commands are operator-only",
    ]
    .join("\n")
}

pub fn public_command_denied_message() -> &'static str {
    "This command is not available in public mode."
}
