use crate::utils::truncate_str;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};

pub fn approval_keyboard(task_id: &str) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![
            InlineKeyboardButton::callback("✅ Approve", format!("approve:{task_id}")),
            InlineKeyboardButton::callback("❌ Deny", format!("deny:{task_id}")),
        ],
        vec![InlineKeyboardButton::callback(
            "✅ Approve Shell/Validate (10m)",
            format!("approve_broad_run:{task_id}"),
        )],
        vec![InlineKeyboardButton::callback("🔓 Unsafe 10m", "unsafe:10")],
    ])
}

pub fn workspace_panel_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "▶️ Start Chatting",
            "ws:start",
        )],
        vec![
            InlineKeyboardButton::callback("🔄 Switch", "ws:switch"),
            InlineKeyboardButton::callback("➕ New", "ws:new"),
        ],
        vec![
            InlineKeyboardButton::callback("🧹 Clear Current", "ws:clear:ask"),
            InlineKeyboardButton::callback("🗑 Delete", "ws:delete"),
        ],
        vec![InlineKeyboardButton::callback("⚙️ Config", "ws:cfg:menu")],
        vec![InlineKeyboardButton::callback("♻️ Refresh", "ws:menu")],
    ])
}

pub fn workspace_config_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "🔁 Quick Setup",
            "ws:cfg:wizard:start",
        )],
        vec![InlineKeyboardButton::callback(
            "🎯 Role & Skill",
            "ws:cfg:profile:menu",
        )],
        vec![
            InlineKeyboardButton::callback("🛡 Safety", "ws:cfg:safety:menu"),
            InlineKeyboardButton::callback("🧰 Tools", "ws:cfg:tools:menu"),
        ],
        vec![
            InlineKeyboardButton::callback("🌐 Network", "ws:cfg:network:menu"),
            InlineKeyboardButton::callback("🔐 Secrets", "ws:cfg:secrets:menu"),
        ],
        vec![InlineKeyboardButton::callback(
            "🌍 Public Runtime",
            "ws:cfg:public:menu",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Workspace",
            "ws:menu",
        )],
    ])
}

pub fn workspace_wizard_role_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "🧭 General",
            "ws:cfg:wizard:role:general",
        )],
        vec![InlineKeyboardButton::callback(
            "🧪 Development",
            "ws:cfg:wizard:role:development",
        )],
        vec![InlineKeyboardButton::callback(
            "🔎 Research",
            "ws:cfg:wizard:role:research",
        )],
        vec![InlineKeyboardButton::callback(
            "📣 Social",
            "ws:cfg:wizard:role:social",
        )],
        vec![
            InlineKeyboardButton::callback("🆘 Support", "ws:cfg:wizard:role:support"),
            InlineKeyboardButton::callback("💼 Sales", "ws:cfg:wizard:role:sales"),
        ],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Settings",
            "ws:cfg:menu",
        )],
    ])
}

pub fn workspace_wizard_safety_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "🛡 Safe",
            "ws:cfg:wizard:safety:safe",
        )],
        vec![InlineKeyboardButton::callback(
            "⚡ Trusted 30m",
            "ws:cfg:wizard:safety:trusted30",
        )],
        vec![InlineKeyboardButton::callback(
            "⚠️ Unsafe 10m",
            "ws:cfg:wizard:safety:unsafe10",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Role Step",
            "ws:cfg:wizard:start",
        )],
    ])
}

pub fn workspace_wizard_network_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "🌐 Open",
            "ws:cfg:wizard:fetch:open",
        )],
        vec![InlineKeyboardButton::callback(
            "🌐 Trusted-only",
            "ws:cfg:wizard:fetch:trusted_only",
        )],
        vec![InlineKeyboardButton::callback(
            "🌐 Trusted-preferred",
            "ws:cfg:wizard:fetch:trusted_preferred",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Safety Step",
            "ws:cfg:wizard:safety",
        )],
    ])
}

pub fn workspace_wizard_integrations_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "✅ Recommended (least privilege)",
            "ws:cfg:wizard:caps:recommended",
        )],
        vec![InlineKeyboardButton::callback(
            "🌐 All integrations (read-only)",
            "ws:cfg:wizard:caps:all_readonly",
        )],
        vec![InlineKeyboardButton::callback(
            "✏️ Customize now (advanced)",
            "ws:cfg:wizard:caps:custom",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Network Step",
            "ws:cfg:wizard:network",
        )],
    ])
}

pub fn workspace_wizard_skill_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "✍️ Add Skill Prompt Now",
            "ws:cfg:wizard:skill:set",
        )],
        vec![InlineKeyboardButton::callback(
            "⏭ Skip For Now",
            "ws:cfg:wizard:skill:skip",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Integrations Step",
            "ws:cfg:wizard:integrations",
        )],
    ])
}

pub fn workspace_wizard_done_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "➕ Add trusted domain",
            "ws:cfg:domain:add",
        )],
        vec![InlineKeyboardButton::callback(
            "🌍 Public Runtime",
            "ws:cfg:public:menu",
        )],
        vec![InlineKeyboardButton::callback(
            "🏁 Finish Setup",
            "ws:cfg:menu",
        )],
    ])
}

pub fn workspace_profile_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "📋 View profile",
            "ws:cfg:profile:view",
        )],
        vec![
            InlineKeyboardButton::callback("🧭 General", "ws:cfg:profile:general"),
            InlineKeyboardButton::callback("🧪 Development", "ws:cfg:profile:development"),
        ],
        vec![
            InlineKeyboardButton::callback("🔎 Research", "ws:cfg:profile:research"),
            InlineKeyboardButton::callback("📣 Social", "ws:cfg:profile:social"),
        ],
        vec![
            InlineKeyboardButton::callback("🆘 Support", "ws:cfg:profile:support"),
            InlineKeyboardButton::callback("💼 Sales", "ws:cfg:profile:sales"),
        ],
        vec![
            InlineKeyboardButton::callback("✍️ Set skill prompt", "ws:cfg:skill:set"),
            InlineKeyboardButton::callback("🧹 Clear skill prompt", "ws:cfg:skill:clear"),
        ],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Settings",
            "ws:cfg:menu",
        )],
    ])
}

pub fn workspace_safety_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "🛡 Safe (Recommended)",
            "ws:cfg:mode:safe",
        )],
        vec![
            InlineKeyboardButton::callback("⚡ Trusted 30m", "ws:cfg:mode:trusted30"),
            InlineKeyboardButton::callback("⚡ Trusted Forever", "ws:cfg:mode:trusted_forever"),
        ],
        vec![
            InlineKeyboardButton::callback("⚠️ Unsafe 10m", "ws:cfg:mode:unsafe10"),
            InlineKeyboardButton::callback("⚠️ Unsafe Forever", "ws:cfg:mode:unsafe_forever"),
        ],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Settings",
            "ws:cfg:menu",
        )],
    ])
}

pub fn workspace_tools_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "🧰 Shell Strict",
            "ws:cfg:shell:strict",
        )],
        vec![InlineKeyboardButton::callback(
            "🧰 Shell Standard (Recommended)",
            "ws:cfg:shell:standard",
        )],
        vec![InlineKeyboardButton::callback(
            "🧰 Shell Extended",
            "ws:cfg:shell:extended",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Settings",
            "ws:cfg:menu",
        )],
    ])
}

pub fn workspace_network_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "🌐 Open",
            "ws:cfg:fetch:open",
        )],
        vec![InlineKeyboardButton::callback(
            "🌐 Trusted-only",
            "ws:cfg:fetch:trusted_only",
        )],
        vec![InlineKeyboardButton::callback(
            "🌐 Trusted-preferred",
            "ws:cfg:fetch:trusted_preferred",
        )],
        vec![
            InlineKeyboardButton::callback("➕ Add trusted domain", "ws:cfg:domain:add"),
            InlineKeyboardButton::callback("📋 List domains", "ws:cfg:domain:list"),
        ],
        vec![
            InlineKeyboardButton::callback("➖ Remove domain", "ws:cfg:domain:remove"),
            InlineKeyboardButton::callback("🧹 Clear trusted domains", "ws:cfg:domain:clear"),
        ],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Settings",
            "ws:cfg:menu",
        )],
    ])
}

pub fn workspace_secrets_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback("🔄 Refresh", "ws:cfg:tools")],
        vec![
            InlineKeyboardButton::callback("🔑 Set Secret", "ws:cfg:secret:set"),
            InlineKeyboardButton::callback("📋 List Secrets", "ws:cfg:secret:list"),
        ],
        vec![InlineKeyboardButton::callback(
            "🗑 Remove Secret",
            "ws:cfg:secret:remove",
        )],
        vec![InlineKeyboardButton::callback(
            "🔐 Enable Encryption",
            "ws:cfg:enc:enable",
        )],
        vec![InlineKeyboardButton::callback(
            "♻️ Rotate Key",
            "ws:cfg:enc:rotate",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Settings",
            "ws:cfg:menu",
        )],
    ])
}

pub fn workspace_public_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "🔄 Refresh",
            "ws:cfg:public:menu",
        )],
        vec![InlineKeyboardButton::callback(
            "📰 Toggle Sources In Replies",
            "ws:cfg:public:sources:toggle",
        )],
        vec![InlineKeyboardButton::callback(
            "🔌 Connect Integration",
            "ws:cfg:connect:menu",
        )],
        vec![InlineKeyboardButton::callback(
            "🔗 Connected Targets",
            "ws:cfg:public:bindings",
        )],
        vec![InlineKeyboardButton::callback(
            "🔐 Channel Rules (Bindings)",
            "ws:cfg:binding:menu",
        )],
        vec![InlineKeyboardButton::callback(
            "🧰 Integration Permissions",
            "ws:cfg:caps:menu",
        )],
        vec![InlineKeyboardButton::callback(
            "📜 Audit",
            "ws:cfg:audit:menu",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Settings",
            "ws:cfg:menu",
        )],
    ])
}

pub fn workspace_caps_menu_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "🎛 Apply Support Preset",
            "ws:cfg:caps:preset:support",
        )],
        vec![InlineKeyboardButton::callback(
            "📣 Apply Social Preset",
            "ws:cfg:caps:preset:social",
        )],
        vec![InlineKeyboardButton::callback(
            "🛡 Apply Moderation Preset",
            "ws:cfg:caps:preset:moderation",
        )],
        vec![InlineKeyboardButton::callback(
            "🔒 Apply Read-only Preset",
            "ws:cfg:caps:preset:strict_readonly",
        )],
        vec![InlineKeyboardButton::callback(
            "✏️ Edit Slack",
            "ws:cfg:caps:edit:slack",
        )],
        vec![InlineKeyboardButton::callback(
            "✏️ Edit Notion",
            "ws:cfg:caps:edit:notion",
        )],
        vec![InlineKeyboardButton::callback(
            "✏️ Edit GitHub",
            "ws:cfg:caps:edit:github",
        )],
        vec![InlineKeyboardButton::callback(
            "✏️ Edit Linear",
            "ws:cfg:caps:edit:linear",
        )],
        vec![InlineKeyboardButton::callback(
            "✏️ Edit Telegram",
            "ws:cfg:caps:edit:telegram",
        )],
        vec![InlineKeyboardButton::callback(
            "✏️ Edit Todoist",
            "ws:cfg:caps:edit:todoist",
        )],
        vec![InlineKeyboardButton::callback(
            "✏️ Edit Jira",
            "ws:cfg:caps:edit:jira",
        )],
        vec![InlineKeyboardButton::callback(
            "✏️ Edit Discord",
            "ws:cfg:caps:edit:discord",
        )],
        vec![InlineKeyboardButton::callback(
            "✏️ Edit X",
            "ws:cfg:caps:edit:x",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Public Runtime",
            "ws:cfg:public:menu",
        )],
    ])
}

pub fn workspace_binding_policy_empty_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "🔌 Connect Integration",
            "ws:cfg:connect:menu",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Public Runtime",
            "ws:cfg:public:menu",
        )],
    ])
}

pub fn integration_label(integration: &str) -> &'static str {
    match integration {
        "telegram" => "Telegram",
        "discord" => "Discord",
        "x" => "X",
        "slack" => "Slack",
        "notion" => "Notion",
        "github" => "GitHub",
        "linear" => "Linear",
        "todoist" => "Todoist",
        "jira" => "Jira",
        _ => "Integration",
    }
}

pub fn integration_target_label(integration: &str) -> &'static str {
    match integration {
        "telegram" => "chat_id or @channelusername (example: -1001234567890 or @mychannel)",
        "discord" => "channel id",
        "x" => "account id",
        "slack" => "channel id",
        "notion" => "database/page id",
        "github" => "owner/repo or scope",
        "linear" => "team/workspace id",
        "todoist" => "project id or scope",
        "jira" => "project key or scope",
        _ => "target id",
    }
}

pub fn workspace_connect_integration_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![
            InlineKeyboardButton::callback("Telegram", "ws:cfg:connect:int:telegram"),
            InlineKeyboardButton::callback("Discord", "ws:cfg:connect:int:discord"),
        ],
        vec![
            InlineKeyboardButton::callback("X", "ws:cfg:connect:int:x"),
            InlineKeyboardButton::callback("Slack", "ws:cfg:connect:int:slack"),
        ],
        vec![
            InlineKeyboardButton::callback("Notion", "ws:cfg:connect:int:notion"),
            InlineKeyboardButton::callback("GitHub", "ws:cfg:connect:int:github"),
        ],
        vec![
            InlineKeyboardButton::callback("Linear", "ws:cfg:connect:int:linear"),
            InlineKeyboardButton::callback("Todoist", "ws:cfg:connect:int:todoist"),
        ],
        vec![InlineKeyboardButton::callback(
            "Jira",
            "ws:cfg:connect:int:jira",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Public Runtime",
            "ws:cfg:public:menu",
        )],
    ])
}

pub fn workspace_connect_workspace_keyboard(workspaces: &[String]) -> InlineKeyboardMarkup {
    let mut rows = Vec::new();
    for (idx, name) in workspaces.iter().take(10).enumerate() {
        rows.push(vec![InlineKeyboardButton::callback(
            format!("📁 {}", truncate_str(name, 36)),
            format!("ws:cfg:connect:ws:{}", idx),
        )]);
    }
    rows.push(vec![
        InlineKeyboardButton::callback("⬅️ Change Integration", "ws:cfg:connect:menu"),
        InlineKeyboardButton::callback("❌ Cancel", "ws:cfg:connect:cancel"),
    ]);
    InlineKeyboardMarkup::new(rows)
}

pub fn workspace_connect_target_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "⬅️ Change Workspace",
            "ws:cfg:connect:back:ws",
        )],
        vec![InlineKeyboardButton::callback(
            "🔁 Change Integration",
            "ws:cfg:connect:menu",
        )],
        vec![InlineKeyboardButton::callback(
            "❌ Cancel",
            "ws:cfg:connect:cancel",
        )],
    ])
}

pub fn workspace_audit_keyboard() -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "👥 Public",
            "ws:cfg:audit:view:audience=public",
        )],
        vec![InlineKeyboardButton::callback(
            "🛠 Operator",
            "ws:cfg:audit:view:audience=operator",
        )],
        vec![InlineKeyboardButton::callback(
            "🛡 Policy",
            "ws:cfg:audit:view:event=policy",
        )],
        vec![InlineKeyboardButton::callback(
            "🚫 ACL",
            "ws:cfg:audit:view:event=acl",
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Public Runtime",
            "ws:cfg:public:menu",
        )],
    ])
}

pub fn workspace_cap_edit_keyboard(integration: &str) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "Toggle Enabled",
            format!("ws:cfg:caps:toggle:{}:enabled", integration),
        )],
        vec![InlineKeyboardButton::callback(
            "Toggle Read",
            format!("ws:cfg:caps:toggle:{}:allow_read", integration),
        )],
        vec![InlineKeyboardButton::callback(
            "Toggle Write",
            format!("ws:cfg:caps:toggle:{}:allow_write", integration),
        )],
        vec![InlineKeyboardButton::callback(
            "Toggle Moderation",
            format!("ws:cfg:caps:toggle:{}:allow_moderation", integration),
        )],
        vec![InlineKeyboardButton::callback(
            "Toggle Write Approval",
            format!(
                "ws:cfg:caps:toggle:{}:require_human_approval_for_write",
                integration
            ),
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back to Integrations",
            "ws:cfg:caps:menu",
        )],
    ])
}

pub fn workspace_switch_keyboard(
    workspaces: &[crate::db::WorkspaceRecord],
    active_id: Option<&str>,
) -> InlineKeyboardMarkup {
    let mut rows: Vec<Vec<InlineKeyboardButton>> = Vec::new();
    for ws in workspaces.iter().take(20) {
        let label = if active_id == Some(ws.workspace_id.as_str()) {
            format!("✅ {}", ws.name)
        } else {
            ws.name.clone()
        };
        rows.push(vec![InlineKeyboardButton::callback(
            label,
            format!("ws:use:{}", ws.workspace_id),
        )]);
    }
    rows.push(vec![InlineKeyboardButton::callback("⬅️ Back", "ws:menu")]);
    InlineKeyboardMarkup::new(rows)
}

pub fn workspace_delete_keyboard(
    workspaces: &[crate::db::WorkspaceRecord],
    active_id: Option<&str>,
) -> InlineKeyboardMarkup {
    let mut rows: Vec<Vec<InlineKeyboardButton>> = Vec::new();
    for ws in workspaces.iter().take(20) {
        let label = if active_id == Some(ws.workspace_id.as_str()) {
            format!("🗑 {} (active)", ws.name)
        } else {
            format!("🗑 {}", ws.name)
        };
        rows.push(vec![InlineKeyboardButton::callback(
            label,
            format!("ws:delask:{}", ws.workspace_id),
        )]);
    }
    rows.push(vec![InlineKeyboardButton::callback("⬅️ Back", "ws:menu")]);
    InlineKeyboardMarkup::new(rows)
}

pub fn workspace_binding_policy_keyboard(
    bindings: &[crate::db::ChannelBindingRecord],
) -> InlineKeyboardMarkup {
    let mut rows: Vec<Vec<InlineKeyboardButton>> = Vec::new();
    for row in bindings.iter().take(12) {
        let key = format!("{}:{}", row.integration, row.channel_id);
        rows.push(vec![InlineKeyboardButton::callback(
            format!("{}:{}", row.integration, truncate_str(&row.channel_id, 24)),
            format!("ws:cfg:binding:edit:{}", key),
        )]);
    }
    rows.push(vec![InlineKeyboardButton::callback(
        "⬅️ Back To Public Workspace",
        "ws:cfg:public:menu",
    )]);
    InlineKeyboardMarkup::new(rows)
}

pub fn workspace_binding_edit_keyboard(binding: &str) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::new(vec![
        vec![InlineKeyboardButton::callback(
            "✏️ Edit Allowed Actions List",
            format!("ws:cfg:binding:actions:start:{}", binding),
        )],
        vec![InlineKeyboardButton::callback(
            "Write Mode: Workspace Default",
            format!("ws:cfg:binding:wp:{}:workspace_default", binding),
        )],
        vec![InlineKeyboardButton::callback(
            "Write Mode: Read-Only",
            format!("ws:cfg:binding:wp:{}:read_only", binding),
        )],
        vec![InlineKeyboardButton::callback(
            "Write Mode: Approval Required",
            format!("ws:cfg:binding:wp:{}:approval_required", binding),
        )],
        vec![InlineKeyboardButton::callback(
            "Preset: Search Only",
            format!("ws:cfg:binding:preset:{}:search_only", binding),
        )],
        vec![InlineKeyboardButton::callback(
            "Preset: Social Posting",
            format!("ws:cfg:binding:preset:{}:social_posting", binding),
        )],
        vec![InlineKeyboardButton::callback(
            "Preset: Moderation",
            format!("ws:cfg:binding:preset:{}:moderation", binding),
        )],
        vec![InlineKeyboardButton::callback(
            "⬅️ Back To Channel Rules",
            "ws:cfg:binding:menu",
        )],
    ])
}
