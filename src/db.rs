use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::task;

const DB_SCHEMA_VERSION: i32 = 1;

#[derive(Debug, Clone)]
pub struct Database {
    pool: Pool<SqliteConnectionManager>,
    crypto: std::sync::Arc<std::sync::RwLock<Option<std::sync::Arc<crate::crypto::Crypto>>>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Message {
    pub id: i64,
    pub chat_id: i64,
    pub role: String,
    pub content: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Queued,
    Running,
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Planning,
    Running,
    Blocked,
    Done,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Planning => "planning",
            RunStatus::Running => "running",
            RunStatus::Blocked => "blocked",
            RunStatus::Done => "done",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
        }
    }
}

impl From<&str> for RunStatus {
    fn from(value: &str) -> Self {
        match value {
            "planning" => RunStatus::Planning,
            "running" => RunStatus::Running,
            "blocked" => RunStatus::Blocked,
            "done" => RunStatus::Done,
            "failed" => RunStatus::Failed,
            "cancelled" => RunStatus::Cancelled,
            _ => RunStatus::Planning,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Queued,
    Running,
    Blocked,
    Done,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Queued => "queued",
            TaskStatus::Running => "running",
            TaskStatus::Blocked => "blocked",
            TaskStatus::Done => "done",
            TaskStatus::Failed => "failed",
            TaskStatus::Cancelled => "cancelled",
        }
    }
}

impl From<&str> for TaskStatus {
    fn from(value: &str) -> Self {
        match value {
            "queued" => TaskStatus::Queued,
            "running" => TaskStatus::Running,
            "blocked" => TaskStatus::Blocked,
            "done" => TaskStatus::Done,
            "failed" => TaskStatus::Failed,
            "cancelled" => TaskStatus::Cancelled,
            _ => TaskStatus::Queued,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Denied,
}

impl ApprovalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ApprovalStatus::Pending => "pending",
            ApprovalStatus::Approved => "approved",
            ApprovalStatus::Denied => "denied",
        }
    }
}

impl From<&str> for ApprovalStatus {
    fn from(value: &str) -> Self {
        match value {
            "pending" => ApprovalStatus::Pending,
            "approved" => ApprovalStatus::Approved,
            "denied" => ApprovalStatus::Denied,
            _ => ApprovalStatus::Pending,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskTier {
    Safe,
    NeedsApproval,
    Dangerous,
}

impl RiskTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            RiskTier::Safe => "safe",
            RiskTier::NeedsApproval => "needs_approval",
            RiskTier::Dangerous => "dangerous",
        }
    }
}

impl From<&str> for RiskTier {
    fn from(value: &str) -> Self {
        match value {
            "safe" => RiskTier::Safe,
            "dangerous" => RiskTier::Dangerous,
            "needs_approval" => RiskTier::NeedsApproval,
            _ => RiskTier::NeedsApproval,
        }
    }
}

impl JobState {
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Running => "running",
            JobState::Done => "done",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
        }
    }

    pub fn emoji(&self) -> &'static str {
        match self {
            JobState::Queued => "⏳",
            JobState::Running => "⚙️",
            JobState::Done => "✅",
            JobState::Failed => "❌",
            JobState::Cancelled => "🚫",
        }
    }
}

impl From<&str> for JobState {
    fn from(value: &str) -> Self {
        match value {
            "running" => JobState::Running,
            "done" => JobState::Done,
            "failed" => JobState::Failed,
            "cancelled" => JobState::Cancelled,
            _ => JobState::Queued,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RunRecord {
    pub run_id: String,
    pub chat_id: i64,
    pub workspace_id: String,
    pub user_goal: String,
    pub status: RunStatus,
    pub mode: String,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub workspace_path: PathBuf,
    pub trusted_until: Option<DateTime<Utc>>,
    pub unsafe_until: Option<DateTime<Utc>>,
    pub write_tools_until: Option<DateTime<Utc>>,
    pub workspace_repo: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WorkspaceRecord {
    pub workspace_id: String,
    pub chat_id: i64,
    pub name: String,
    pub workspace_path: PathBuf,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessRole {
    Owner,
    Admin,
    Public,
}

impl AccessRole {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            AccessRole::Owner => "owner",
            AccessRole::Admin => "admin",
            AccessRole::Public => "public",
        }
    }
}

impl From<&str> for AccessRole {
    fn from(value: &str) -> Self {
        match value {
            "owner" => AccessRole::Owner,
            "admin" => AccessRole::Admin,
            "public" => AccessRole::Public,
            _ => AccessRole::Public,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PrincipalKind {
    TelegramUser,
}

impl PrincipalKind {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            PrincipalKind::TelegramUser => "telegram_user",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ChannelBindingRecord {
    pub integration: String,
    pub channel_id: String,
    pub workspace_id: String,
    pub mode: String,
    pub metadata_json: String,
    pub allowed_actions: Vec<String>,
    pub write_policy: String,
    pub fallback_workspace_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WorkspaceIntegrationCapabilityRecord {
    pub workspace_id: String,
    pub integration: String,
    pub enabled: bool,
    pub allow_read: bool,
    pub allow_write: bool,
    pub allow_moderation: bool,
    pub require_human_approval_for_write: bool,
    pub updated_at: DateTime<Utc>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WorkspacePublicProfileRecord {
    pub workspace_id: String,
    pub brand_name: Option<String>,
    pub public_refusal_text: Option<String>,
    pub public_scope_text: Option<String>,
    pub show_sources: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WorkspaceSecretRecord {
    pub secret_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct ReencryptStats {
    pub updated_rows: usize,
    pub skipped_rows: usize,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AuditEventRecord {
    pub id: i64,
    pub chat_id: i64,
    pub workspace_id: Option<String>,
    pub principal_id: Option<String>,
    pub role: Option<String>,
    pub audience: String,
    pub event_type: String,
    pub details: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceSecurityMode {
    Strict,
    Trusted,
    Unsafe,
}

impl WorkspaceSecurityMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkspaceSecurityMode::Strict => "strict",
            WorkspaceSecurityMode::Trusted => "trusted",
            WorkspaceSecurityMode::Unsafe => "unsafe",
        }
    }
}

impl From<&str> for WorkspaceSecurityMode {
    fn from(value: &str) -> Self {
        match value {
            "trusted" => WorkspaceSecurityMode::Trusted,
            "unsafe" => WorkspaceSecurityMode::Unsafe,
            _ => WorkspaceSecurityMode::Strict,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceShellPack {
    Strict,
    Standard,
    Extended,
}

impl WorkspaceShellPack {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkspaceShellPack::Strict => "strict",
            WorkspaceShellPack::Standard => "standard",
            WorkspaceShellPack::Extended => "extended",
        }
    }
}

impl From<&str> for WorkspaceShellPack {
    fn from(value: &str) -> Self {
        match value {
            "strict" => WorkspaceShellPack::Strict,
            "extended" => WorkspaceShellPack::Extended,
            _ => WorkspaceShellPack::Standard,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceFetchMode {
    Open,
    TrustedOnly,
    TrustedPreferred,
}

impl WorkspaceFetchMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkspaceFetchMode::Open => "open",
            WorkspaceFetchMode::TrustedOnly => "trusted_only",
            WorkspaceFetchMode::TrustedPreferred => "trusted_preferred",
        }
    }
}

impl From<&str> for WorkspaceFetchMode {
    fn from(value: &str) -> Self {
        match value {
            "trusted_only" => WorkspaceFetchMode::TrustedOnly,
            "trusted_preferred" => WorkspaceFetchMode::TrustedPreferred,
            _ => WorkspaceFetchMode::Open,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WorkspaceSettingsRecord {
    pub workspace_id: String,
    pub security_mode: WorkspaceSecurityMode,
    pub mode_expires_at: Option<DateTime<Utc>>,
    pub write_tools_enabled: bool,
    pub write_tools_expires_at: Option<DateTime<Utc>>,
    pub shell_pack: WorkspaceShellPack,
    pub fetch_mode: WorkspaceFetchMode,
    pub trusted_domains: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WorkspaceProfileRecord {
    pub workspace_id: String,
    pub role_name: String,
    pub skill_prompt: String,
    pub allowed_tools: Vec<String>,
    #[allow(dead_code)]
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct RunMemoryRecord {
    pub id: i64,
    pub run_id: String,
    pub kind: String,
    pub format: String,
    pub budget: Option<i64>,
    pub content: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct TaskRecord {
    pub task_id: String,
    pub run_id: String,
    pub agent: String,
    pub action_type: String,
    pub goal: String,
    pub risk_tier: RiskTier,
    pub status: TaskStatus,
    pub job_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ApprovalRecord {
    pub approval_id: String,
    pub task_id: String,
    pub status: ApprovalStatus,
    pub reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub decided_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ApprovalGrantRecord {
    pub grant_id: String,
    pub scope_type: String, // "run" | "workspace"
    pub scope_id: String,   // run_id or workspace_path
    pub action_type: String,
    pub command_prefix: Option<String>,
    pub risk_tier: RiskTier,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JobRecord {
    pub id: String,
    pub chat_id: i64,
    pub action_type: String,
    pub goal: String,
    pub state: JobState,
    pub result: Option<String>,
    pub log_path: PathBuf,
    pub work_dir: PathBuf,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub depends_on: Option<String>,
}

impl Database {
    fn known_integrations() -> [&'static str; 9] {
        [
            "slack", "notion", "github", "linear", "telegram", "todoist", "jira", "discord", "x",
        ]
    }

    pub async fn new(
        path: &Path,
        crypto: Option<std::sync::Arc<crate::crypto::Crypto>>,
    ) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("Failed to create database parent dir {}", parent.display())
            })?;
        }

        let manager = SqliteConnectionManager::file(path).with_flags(
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        );
        let pool = Pool::builder()
            .max_size(8)
            .build(manager)
            .with_context(|| {
                format!(
                    "Failed to create sqlite connection pool at {}",
                    path.display()
                )
            })?;

        let db = Self {
            pool,
            crypto: std::sync::Arc::new(std::sync::RwLock::new(crypto)),
        };

        db.with_conn(|conn| {
            conn.busy_timeout(std::time::Duration::from_secs(5)).ok();
            let current_schema_version = current_schema_version(conn).unwrap_or(0);
            conn.execute_batch(
                r#"
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    workspace_id TEXT,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    summarized INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id);

                CREATE TABLE IF NOT EXISTS summaries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    workspace_id TEXT,
                    content TEXT NOT NULL,
                    up_to_message_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_summaries_chat ON summaries(chat_id);

                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    action_type TEXT NOT NULL,
                    goal TEXT NOT NULL,
                    state TEXT NOT NULL,
                    result TEXT,
                    log_path TEXT NOT NULL,
                    work_dir TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    depends_on TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_chat ON jobs(chat_id);

                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    workspace_id TEXT,
                    user_goal TEXT NOT NULL,
                    status TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    provider TEXT,
                    model TEXT,
                    workspace_path TEXT NOT NULL,
                    trusted_until TEXT,
                    unsafe_until TEXT,
                    write_tools_until TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_runs_chat ON runs(chat_id);
                CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);

                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    goal TEXT NOT NULL,
                    risk_tier TEXT NOT NULL,
                    status TEXT NOT NULL,
                    job_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_run ON tasks(run_id);
                CREATE INDEX IF NOT EXISTS idx_tasks_job ON tasks(job_id);
                CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);

                CREATE TABLE IF NOT EXISTS task_deps (
                    task_id TEXT NOT NULL,
                    depends_on_task_id TEXT NOT NULL,
                    PRIMARY KEY (task_id, depends_on_task_id)
                );
                CREATE INDEX IF NOT EXISTS idx_task_deps_task ON task_deps(task_id);
                CREATE INDEX IF NOT EXISTS idx_task_deps_dep ON task_deps(depends_on_task_id);

                CREATE TABLE IF NOT EXISTS approvals (
                    approval_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reason TEXT,
                    created_at TEXT NOT NULL,
                    decided_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_approvals_task ON approvals(task_id);

                CREATE TABLE IF NOT EXISTS approval_grants (
                    grant_id TEXT PRIMARY KEY,
                    scope_type TEXT NOT NULL,
                    scope_id TEXT NOT NULL,
                    action_type TEXT NOT NULL,
                    command_prefix TEXT,
                    risk_tier TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_approval_grants_scope ON approval_grants(scope_type, scope_id);
                CREATE INDEX IF NOT EXISTS idx_approval_grants_expiry ON approval_grants(expires_at);

                CREATE TABLE IF NOT EXISTS chat_state (
                    chat_id INTEGER PRIMARY KEY,
                    active_run_id TEXT,
                    active_workspace_id TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS workspaces (
                    workspace_id TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    workspace_path TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_workspaces_chat_name ON workspaces(chat_id, name);
                CREATE INDEX IF NOT EXISTS idx_workspaces_chat_updated ON workspaces(chat_id, updated_at DESC);

                CREATE TABLE IF NOT EXISTS workspace_settings (
                    workspace_id TEXT PRIMARY KEY,
                    security_mode TEXT NOT NULL DEFAULT 'strict',
                    mode_expires_at TEXT,
                    write_tools_enabled INTEGER NOT NULL DEFAULT 0,
                    write_tools_expires_at TEXT,
                    shell_pack TEXT NOT NULL DEFAULT 'standard',
                    fetch_mode TEXT NOT NULL DEFAULT 'open',
                    trusted_domains TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS workspace_profiles (
                    workspace_id TEXT PRIMARY KEY,
                    role_name TEXT NOT NULL DEFAULT 'general',
                    skill_prompt TEXT NOT NULL DEFAULT '',
                    allowed_tools TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS principals (
                    principal_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    external_ref TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_principals_kind_external ON principals(kind, external_ref);

                CREATE TABLE IF NOT EXISTS principal_roles (
                    principal_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    scope_type TEXT NOT NULL DEFAULT 'global',
                    scope_id TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (principal_id, role, scope_type, scope_id)
                );
                CREATE INDEX IF NOT EXISTS idx_principal_roles_principal ON principal_roles(principal_id);
                CREATE INDEX IF NOT EXISTS idx_principal_roles_scope ON principal_roles(scope_type, scope_id);

                CREATE TABLE IF NOT EXISTS channel_bindings (
                    integration TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    workspace_id TEXT NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'public_skill',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    allowed_actions TEXT NOT NULL DEFAULT '[]',
                    write_policy TEXT NOT NULL DEFAULT 'workspace_default',
                    fallback_workspace_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (integration, channel_id)
                );
                CREATE INDEX IF NOT EXISTS idx_channel_bindings_workspace ON channel_bindings(workspace_id);

                CREATE TABLE IF NOT EXISTS public_profiles (
                    workspace_id TEXT PRIMARY KEY,
                    brand_name TEXT,
                    public_refusal_text TEXT,
                    public_scope_text TEXT,
                    show_sources INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS workspace_integration_caps (
                    workspace_id TEXT NOT NULL,
                    integration TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    allow_read INTEGER NOT NULL DEFAULT 1,
                    allow_write INTEGER NOT NULL DEFAULT 0,
                    allow_moderation INTEGER NOT NULL DEFAULT 0,
                    require_human_approval_for_write INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (workspace_id, integration)
                );
                CREATE INDEX IF NOT EXISTS idx_workspace_integration_caps_workspace ON workspace_integration_caps(workspace_id);

                CREATE TABLE IF NOT EXISTS workspace_secrets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace_id TEXT NOT NULL,
                    secret_name TEXT NOT NULL,
                    secret_value TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(workspace_id, secret_name)
                );
                CREATE INDEX IF NOT EXISTS idx_workspace_secrets_workspace ON workspace_secrets(workspace_id);

                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    workspace_id TEXT,
                    principal_id TEXT,
                    role TEXT,
                    audience TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_events_chat ON audit_events(chat_id, id DESC);
                CREATE INDEX IF NOT EXISTS idx_audit_events_workspace ON audit_events(workspace_id, id DESC);

                CREATE TABLE IF NOT EXISTS run_memories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    format TEXT NOT NULL,
                    budget INTEGER,
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_run_memories_run ON run_memories(run_id);

                CREATE TABLE IF NOT EXISTS agent_states (
                    run_id TEXT NOT NULL,
                    agent TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, agent)
                );
                CREATE INDEX IF NOT EXISTS idx_agent_states_run ON agent_states(run_id);
                "#,
            )?;
            ensure_table_column(
                conn,
                "jobs",
                "depends_on",
                "ALTER TABLE jobs ADD COLUMN depends_on TEXT",
            )?;
            ensure_table_column(
                conn,
                "runs",
                "workspace_id",
                "ALTER TABLE runs ADD COLUMN workspace_id TEXT",
            )?;
            ensure_table_column(
                conn,
                "messages",
                "workspace_id",
                "ALTER TABLE messages ADD COLUMN workspace_id TEXT",
            )?;
            ensure_table_column(
                conn,
                "summaries",
                "workspace_id",
                "ALTER TABLE summaries ADD COLUMN workspace_id TEXT",
            )?;
            ensure_table_column(
                conn,
                "chat_state",
                "active_workspace_id",
                "ALTER TABLE chat_state ADD COLUMN active_workspace_id TEXT",
            )?;
            ensure_table_column(
                conn,
                "runs",
                "trusted_until",
                "ALTER TABLE runs ADD COLUMN trusted_until TEXT",
            )?;
            ensure_table_column(
                conn,
                "runs",
                "unsafe_until",
                "ALTER TABLE runs ADD COLUMN unsafe_until TEXT",
            )?;
            ensure_table_column(
                conn,
                "runs",
                "write_tools_until",
                "ALTER TABLE runs ADD COLUMN write_tools_until TEXT",
            )?;
            ensure_table_column(
                conn,
                "runs",
                "workspace_repo",
                "ALTER TABLE runs ADD COLUMN workspace_repo TEXT",
            )?;
            ensure_table_column(
                conn,
                "tasks",
                "agent",
                "ALTER TABLE tasks ADD COLUMN agent TEXT NOT NULL DEFAULT 'default'",
            )?;
            ensure_table_column(
                conn,
                "channel_bindings",
                "metadata_json",
                "ALTER TABLE channel_bindings ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'",
            )?;
            ensure_table_column(
                conn,
                "channel_bindings",
                "allowed_actions",
                "ALTER TABLE channel_bindings ADD COLUMN allowed_actions TEXT NOT NULL DEFAULT '[]'",
            )?;
            ensure_table_column(
                conn,
                "channel_bindings",
                "write_policy",
                "ALTER TABLE channel_bindings ADD COLUMN write_policy TEXT NOT NULL DEFAULT 'workspace_default'",
            )?;
            ensure_table_column(
                conn,
                "channel_bindings",
                "fallback_workspace_id",
                "ALTER TABLE channel_bindings ADD COLUMN fallback_workspace_id TEXT",
            )?;
            conn.execute_batch(
                r#"
                CREATE INDEX IF NOT EXISTS idx_messages_workspace ON messages(chat_id, workspace_id, summarized, id);
                CREATE INDEX IF NOT EXISTS idx_summaries_workspace ON summaries(chat_id, workspace_id, id);
                CREATE INDEX IF NOT EXISTS idx_runs_workspace ON runs(chat_id, workspace_id, updated_at);
                DROP TABLE IF EXISTS chat_workspaces;
                "#,
            )?;
            if current_schema_version < DB_SCHEMA_VERSION {
                set_schema_version(conn, DB_SCHEMA_VERSION)?;
            }
            Ok(())
        })
        .await?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            if let Err(e) = std::fs::set_permissions(path, perms) {
                tracing::warn!("Could not set DB file permissions: {e}");
            }
        }

        Ok(db)
    }

    async fn with_conn<F, T>(&self, task_fn: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let pool = self.pool.clone();
        task::spawn_blocking(move || {
            let conn = pool
                .get()
                .with_context(|| "Failed to get sqlite connection from pool".to_string())?;
            task_fn(&conn)
        })
        .await?
    }

    fn crypto_snapshot(&self) -> Option<std::sync::Arc<crate::crypto::Crypto>> {
        self.crypto
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn set_crypto(&self, crypto: Option<std::sync::Arc<crate::crypto::Crypto>>) {
        *self.crypto.write().unwrap_or_else(|e| e.into_inner()) = crypto;
    }

    pub fn encryption_enabled(&self) -> bool {
        self.crypto
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    pub async fn add_message(&self, chat_id: i64, role: &str, content: &str) -> Result<()> {
        let role = role.to_string();
        let content = self.protect_value(content);
        self.with_conn(move |conn| {
            let workspace_id = active_workspace_id_for_chat(conn, chat_id)?;
            let ts = Utc::now().to_rfc3339();
            conn.execute(
                "INSERT INTO messages (chat_id, workspace_id, role, content, created_at) VALUES (?, ?, ?, ?, ?)",
                params![chat_id, workspace_id, role, content, ts],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_active_messages(&self, chat_id: i64, limit: usize) -> Result<Vec<Message>> {
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let workspace_id = active_workspace_id_for_chat(conn, chat_id)?;
            let mut stmt = conn.prepare(
                "SELECT id, chat_id, role, content, created_at FROM messages
                 WHERE chat_id = ?
                   AND summarized = 0
                   AND (
                       (workspace_id = ?)
                       OR (workspace_id IS NULL AND ? IS NULL)
                   )
                 ORDER BY id DESC
                 LIMIT ?",
            )?;
            let rows = stmt
                .query_map(
                    params![chat_id, workspace_id, workspace_id, limit as i64],
                    |row| {
                        let ts: String = row.get(4)?;
                        let raw: String = row.get(3)?;
                        let content = unprotect_value(&crypto, &raw);
                        Ok(Message {
                            id: row.get(0)?,
                            chat_id: row.get(1)?,
                            role: row.get(2)?,
                            content,
                            created_at: DateTime::parse_from_rfc3339(&ts)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                        })
                    },
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let mut messages = rows;
            messages.reverse();
            Ok(messages)
        })
        .await
    }

    pub async fn count_active_messages(&self, chat_id: i64) -> Result<usize> {
        self.with_conn(move |conn| {
            let workspace_id = active_workspace_id_for_chat(conn, chat_id)?;
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM messages
                 WHERE chat_id = ?
                   AND summarized = 0
                   AND ((workspace_id = ?) OR (workspace_id IS NULL AND ? IS NULL))",
                params![chat_id, workspace_id, workspace_id],
                |row| row.get(0),
            )?;
            Ok(count as usize)
        })
        .await
    }

    pub async fn mark_summarized(&self, chat_id: i64, up_to_id: i64) -> Result<()> {
        self.with_conn(move |conn| {
            let workspace_id = active_workspace_id_for_chat(conn, chat_id)?;
            conn.execute(
                "UPDATE messages
                 SET summarized = 1
                 WHERE chat_id = ?
                   AND id <= ?
                   AND ((workspace_id = ?) OR (workspace_id IS NULL AND ? IS NULL))",
                params![chat_id, up_to_id, workspace_id, workspace_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn insert_run(&self, run: &RunRecord) -> Result<()> {
        let run = run.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO runs (run_id, chat_id, workspace_id, user_goal, status, mode, provider, model, workspace_path, trusted_until, unsafe_until, write_tools_until, workspace_repo, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    run.run_id,
                    run.chat_id,
                    run.workspace_id,
                    run.user_goal,
                    run.status.as_str(),
                    run.mode,
                    run.provider,
                    run.model,
                    run.workspace_path.to_string_lossy().to_string(),
                    run.trusted_until.as_ref().map(|d| d.to_rfc3339()),
                    run.unsafe_until.as_ref().map(|d| d.to_rfc3339()),
                    run.write_tools_until.as_ref().map(|d| d.to_rfc3339()),
                    run.workspace_repo,
                    run.created_at.to_rfc3339(),
                    run.updated_at.to_rfc3339(),
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>> {
        let run_id = run_id.to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                "SELECT run_id, chat_id, workspace_id, user_goal, status, mode, provider, model, workspace_path, trusted_until, unsafe_until, write_tools_until, workspace_repo, created_at, updated_at
                 FROM runs WHERE run_id = ?",
                params![run_id],
                |row| {
                    let trusted_until: Option<String> = row.get(9)?;
                    let unsafe_until: Option<String> = row.get(10)?;
                    let write_tools_until: Option<String> = row.get(11)?;
                    let workspace_repo: Option<String> = row.get(12)?;
                    let created_at: String = row.get(13)?;
                    let updated_at: String = row.get(14)?;
                    Ok(RunRecord {
                        run_id: row.get(0)?,
                        chat_id: row.get(1)?,
                        workspace_id: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                        user_goal: row.get(3)?,
                        status: RunStatus::from(row.get::<_, String>(4)?.as_str()),
                        mode: row.get(5)?,
                        provider: row.get(6)?,
                        model: row.get(7)?,
                        workspace_path: PathBuf::from(row.get::<_, String>(8)?),
                        trusted_until: trusted_until.and_then(|ts| {
                            DateTime::parse_from_rfc3339(&ts)
                                .ok()
                                .map(|d| d.with_timezone(&Utc))
                        }),
                        unsafe_until: unsafe_until.and_then(|ts| {
                            DateTime::parse_from_rfc3339(&ts)
                                .ok()
                                .map(|d| d.with_timezone(&Utc))
                        }),
                        write_tools_until: write_tools_until.and_then(|ts| {
                            DateTime::parse_from_rfc3339(&ts)
                                .ok()
                                .map(|d| d.with_timezone(&Utc))
                        }),
                        workspace_repo,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        updated_at: DateTime::parse_from_rfc3339(&updated_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn list_recent_runs(&self, chat_id: i64, limit: usize) -> Result<Vec<RunRecord>> {
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT run_id, chat_id, workspace_id, user_goal, status, mode, provider, model, workspace_path, trusted_until, unsafe_until, write_tools_until, workspace_repo, created_at, updated_at
                 FROM runs
                 WHERE chat_id = ?
                   AND (
                        workspace_id = (SELECT active_workspace_id FROM chat_state WHERE chat_id = ?)
                        OR ((SELECT active_workspace_id FROM chat_state WHERE chat_id = ?) IS NULL AND workspace_id IS NULL)
                   )
                 ORDER BY updated_at DESC
                 LIMIT ?",
            )?;
            let rows = stmt
                .query_map(params![chat_id, chat_id, chat_id, limit as i64], |row| {
                    let trusted_until: Option<String> = row.get(9)?;
                    let unsafe_until: Option<String> = row.get(10)?;
                    let write_tools_until: Option<String> = row.get(11)?;
                    let workspace_repo: Option<String> = row.get(12)?;
                    let created_at: String = row.get(13)?;
                    let updated_at: String = row.get(14)?;
                    Ok(RunRecord {
                        run_id: row.get(0)?,
                        chat_id: row.get(1)?,
                        workspace_id: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                        user_goal: row.get(3)?,
                        status: RunStatus::from(row.get::<_, String>(4)?.as_str()),
                        mode: row.get(5)?,
                        provider: row.get(6)?,
                        model: row.get(7)?,
                        workspace_path: PathBuf::from(row.get::<_, String>(8)?),
                        trusted_until: trusted_until.and_then(|ts| {
                            DateTime::parse_from_rfc3339(&ts)
                                .ok()
                                .map(|d| d.with_timezone(&Utc))
                        }),
                        unsafe_until: unsafe_until.and_then(|ts| {
                            DateTime::parse_from_rfc3339(&ts)
                                .ok()
                                .map(|d| d.with_timezone(&Utc))
                        }),
                        write_tools_until: write_tools_until.and_then(|ts| {
                            DateTime::parse_from_rfc3339(&ts)
                                .ok()
                                .map(|d| d.with_timezone(&Utc))
                        }),
                        workspace_repo,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        updated_at: DateTime::parse_from_rfc3339(&updated_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn update_run_status(&self, run_id: &str, status: RunStatus) -> Result<()> {
        let run_id = run_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE runs SET status = ?, updated_at = ? WHERE run_id = ?",
                params![status.as_str(), Utc::now().to_rfc3339(), run_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn update_run_trusted_until(
        &self,
        run_id: &str,
        trusted_until: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let run_id = run_id.to_string();
        let trusted_until = trusted_until.map(|d| d.to_rfc3339());
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE runs SET trusted_until = ?, updated_at = ? WHERE run_id = ?",
                params![trusted_until, Utc::now().to_rfc3339(), run_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn list_incomplete_runs(&self, limit: usize) -> Result<Vec<RunRecord>> {
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT run_id, chat_id, workspace_id, user_goal, status, mode, provider, model, workspace_path, trusted_until, unsafe_until, write_tools_until, workspace_repo, created_at, updated_at
                 FROM runs
                 WHERE status IN ('planning','running','blocked')
                 ORDER BY updated_at ASC
                 LIMIT ?",
            )?;
            let rows = stmt
                .query_map(params![limit as i64], |row| {
                    let trusted_until: Option<String> = row.get(9)?;
                    let unsafe_until: Option<String> = row.get(10)?;
                    let write_tools_until: Option<String> = row.get(11)?;
                    let workspace_repo: Option<String> = row.get(12)?;
                    let created_at: String = row.get(13)?;
                    let updated_at: String = row.get(14)?;
                    Ok(RunRecord {
                        run_id: row.get(0)?,
                        chat_id: row.get(1)?,
                        workspace_id: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                        user_goal: row.get(3)?,
                        status: RunStatus::from(row.get::<_, String>(4)?.as_str()),
                        mode: row.get(5)?,
                        provider: row.get(6)?,
                        model: row.get(7)?,
                        workspace_path: PathBuf::from(row.get::<_, String>(8)?),
                        trusted_until: trusted_until.and_then(|ts| {
                            DateTime::parse_from_rfc3339(&ts)
                                .ok()
                                .map(|d| d.with_timezone(&Utc))
                        }),
                        unsafe_until: unsafe_until.and_then(|ts| {
                            DateTime::parse_from_rfc3339(&ts)
                                .ok()
                                .map(|d| d.with_timezone(&Utc))
                        }),
                        write_tools_until: write_tools_until.and_then(|ts| {
                            DateTime::parse_from_rfc3339(&ts)
                                .ok()
                                .map(|d| d.with_timezone(&Utc))
                        }),
                        workspace_repo,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        updated_at: DateTime::parse_from_rfc3339(&updated_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn update_run_unsafe_until(
        &self,
        run_id: &str,
        unsafe_until: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let run_id = run_id.to_string();
        let unsafe_until = unsafe_until.map(|d| d.to_rfc3339());
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE runs SET unsafe_until = ?, updated_at = ? WHERE run_id = ?",
                params![unsafe_until, Utc::now().to_rfc3339(), run_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn update_run_write_tools_until(
        &self,
        run_id: &str,
        write_tools_until: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let run_id = run_id.to_string();
        let write_tools_until = write_tools_until.map(|d| d.to_rfc3339());
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE runs SET write_tools_until = ?, updated_at = ? WHERE run_id = ?",
                params![write_tools_until, Utc::now().to_rfc3339(), run_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn update_run_workspace_repo(
        &self,
        run_id: &str,
        workspace_repo: Option<&str>,
    ) -> Result<()> {
        let run_id = run_id.to_string();
        let workspace_repo = workspace_repo.map(|s| s.to_string());
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE runs SET workspace_repo = ?, updated_at = ? WHERE run_id = ?",
                params![workspace_repo, Utc::now().to_rfc3339(), run_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn insert_run_memory(
        &self,
        run_id: &str,
        kind: &str,
        format: &str,
        budget: Option<i64>,
        content: &str,
    ) -> Result<()> {
        let run_id = run_id.to_string();
        let kind = kind.to_string();
        let format = format.to_string();
        let content = self.protect_value(content);
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO run_memories (run_id, kind, format, budget, content, created_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    run_id,
                    kind,
                    format,
                    budget,
                    content,
                    Utc::now().to_rfc3339()
                ],
            )?;

            let _ = conn.execute(
                "DELETE FROM run_memories
                 WHERE run_id = ?
                   AND id NOT IN (
                        SELECT id FROM run_memories
                        WHERE run_id = ?
                        ORDER BY id DESC
                        LIMIT 40
                   )",
                params![run_id, run_id],
            );
            Ok(())
        })
        .await
    }

    pub async fn get_latest_run_memory(&self, run_id: &str) -> Result<Option<RunMemoryRecord>> {
        let run_id = run_id.to_string();
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT id, run_id, kind, format, budget, content, created_at
                     FROM run_memories
                     WHERE run_id = ?
                     ORDER BY id DESC
                     LIMIT 1",
                    params![run_id],
                    |row| {
                        let created_at: String = row.get(6)?;
                        Ok(RunMemoryRecord {
                            id: row.get(0)?,
                            run_id: row.get(1)?,
                            kind: row.get(2)?,
                            format: row.get(3)?,
                            budget: row.get(4)?,
                            content: unprotect_value(&crypto, &row.get::<_, String>(5)?),
                            created_at: DateTime::parse_from_rfc3339(&created_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                        })
                    },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn get_agent_state(&self, run_id: &str, agent: &str) -> Result<Option<String>> {
        let run_id = run_id.to_string();
        let agent = agent.to_string();
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let row: Option<String> = conn
                .query_row(
                    "SELECT state_json FROM agent_states WHERE run_id = ? AND agent = ?",
                    params![run_id, agent],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(row.map(|s| unprotect_value(&crypto, &s)))
        })
        .await
    }

    pub async fn set_agent_state(&self, run_id: &str, agent: &str, state_json: &str) -> Result<()> {
        let run_id = run_id.to_string();
        let agent = agent.to_string();
        let state_json = self.protect_value(state_json);
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO agent_states (run_id, agent, state_json, updated_at)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(run_id, agent) DO UPDATE SET state_json = excluded.state_json, updated_at = excluded.updated_at",
                params![run_id, agent, state_json, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_recent_agent_state_for_chat(
        &self,
        chat_id: i64,
        agent: &str,
        run_limit: usize,
    ) -> Result<Option<String>> {
        let agent = agent.to_string();
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT a.state_json
                 FROM agent_states a
                 JOIN runs r ON r.run_id = a.run_id
                 WHERE r.chat_id = ?
                   AND a.agent = ?
                   AND (
                        r.workspace_id = (SELECT active_workspace_id FROM chat_state WHERE chat_id = ?)
                        OR ((SELECT active_workspace_id FROM chat_state WHERE chat_id = ?) IS NULL AND r.workspace_id IS NULL)
                   )
                 ORDER BY r.updated_at DESC
                 LIMIT ?",
            )?;
            let rows = stmt
                .query_map(params![chat_id, agent, chat_id, chat_id, run_limit as i64], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            for raw in rows {
                let state = unprotect_value(&crypto, &raw);
                if !state.trim().is_empty() && state.trim() != "[]" {
                    return Ok(Some(state));
                }
            }
            Ok(None)
        })
        .await
    }

    pub async fn set_active_run(&self, chat_id: i64, run_id: Option<&str>) -> Result<()> {
        let run_id = run_id.map(|s| s.to_string());
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO chat_state (chat_id, active_run_id, updated_at)
                 VALUES (?, ?, ?)
                 ON CONFLICT(chat_id) DO UPDATE SET active_run_id = excluded.active_run_id, updated_at = excluded.updated_at",
                params![chat_id, run_id, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_active_run(&self, chat_id: i64) -> Result<Option<String>> {
        self.with_conn(move |conn| {
            let row: Option<Option<String>> = conn
                .query_row(
                    "SELECT active_run_id FROM chat_state WHERE chat_id = ?",
                    params![chat_id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(row.flatten())
        })
        .await
    }

    pub async fn create_workspace(
        &self,
        chat_id: i64,
        workspace_id: &str,
        name: &str,
        path: &Path,
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        let name = name.to_string();
        let workspace_path = path.to_string_lossy().to_string();
        self.with_conn(move |conn| {
            let ts = Utc::now().to_rfc3339();
            conn.execute(
                "INSERT INTO workspaces (workspace_id, chat_id, name, workspace_path, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![workspace_id, chat_id, name, workspace_path, ts, ts],
            )?;
            conn.execute(
                "INSERT OR IGNORE INTO workspace_settings (workspace_id, updated_at)
                 VALUES (?, ?)",
                params![workspace_id, ts],
            )?;
            conn.execute(
                "INSERT OR IGNORE INTO workspace_profiles (workspace_id, updated_at)
                 VALUES (?, ?)",
                params![workspace_id, ts],
            )?;
            for integration in Self::known_integrations() {
                conn.execute(
                    "INSERT OR IGNORE INTO workspace_integration_caps
                     (workspace_id, integration, enabled, allow_read, allow_write, allow_moderation, require_human_approval_for_write, updated_at)
                     VALUES (?, ?, 1, 1, 0, 0, 1, ?)",
                    params![workspace_id, integration, Utc::now().to_rfc3339()],
                )?;
            }
            conn.execute(
                "INSERT OR IGNORE INTO public_profiles (workspace_id, updated_at)
                 VALUES (?, ?)",
                params![workspace_id, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn list_workspaces(&self, chat_id: i64) -> Result<Vec<WorkspaceRecord>> {
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT workspace_id, chat_id, name, workspace_path, created_at, updated_at
                 FROM workspaces
                 WHERE chat_id = ?
                 ORDER BY updated_at DESC, name ASC",
            )?;
            let rows = stmt
                .query_map(params![chat_id], |row| {
                    let created_at: String = row.get(4)?;
                    let updated_at: String = row.get(5)?;
                    Ok(WorkspaceRecord {
                        workspace_id: row.get(0)?,
                        chat_id: row.get(1)?,
                        name: row.get(2)?,
                        workspace_path: PathBuf::from(row.get::<_, String>(3)?),
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        updated_at: DateTime::parse_from_rfc3339(&updated_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn get_workspace_by_name(
        &self,
        chat_id: i64,
        name: &str,
    ) -> Result<Option<WorkspaceRecord>> {
        let name = name.to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT workspace_id, chat_id, name, workspace_path, created_at, updated_at
                     FROM workspaces
                     WHERE chat_id = ? AND name = ?
                     LIMIT 1",
                    params![chat_id, name],
                    |row| {
                        let created_at: String = row.get(4)?;
                        let updated_at: String = row.get(5)?;
                        Ok(WorkspaceRecord {
                            workspace_id: row.get(0)?,
                            chat_id: row.get(1)?,
                            name: row.get(2)?,
                            workspace_path: PathBuf::from(row.get::<_, String>(3)?),
                            created_at: DateTime::parse_from_rfc3339(&created_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                            updated_at: DateTime::parse_from_rfc3339(&updated_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                        })
                    },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn get_workspace_by_id(&self, workspace_id: &str) -> Result<Option<WorkspaceRecord>> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT workspace_id, chat_id, name, workspace_path, created_at, updated_at
                     FROM workspaces
                     WHERE workspace_id = ?
                     LIMIT 1",
                    params![workspace_id],
                    |row| {
                        let created_at: String = row.get(4)?;
                        let updated_at: String = row.get(5)?;
                        Ok(WorkspaceRecord {
                            workspace_id: row.get(0)?,
                            chat_id: row.get(1)?,
                            name: row.get(2)?,
                            workspace_path: PathBuf::from(row.get::<_, String>(3)?),
                            created_at: DateTime::parse_from_rfc3339(&created_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                            updated_at: DateTime::parse_from_rfc3339(&updated_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                        })
                    },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn ensure_owner_principal(&self, telegram_user_id: i64) -> Result<()> {
        let external_ref = telegram_user_id.to_string();
        self.with_conn(move |conn| {
            let now = Utc::now().to_rfc3339();
            let principal_id = format!("telegram-user-{external_ref}");
            conn.execute(
                "INSERT OR IGNORE INTO principals (principal_id, kind, external_ref, created_at)
                 VALUES (?, 'telegram_user', ?, ?)",
                params![principal_id, external_ref, now],
            )?;
            conn.execute(
                "INSERT OR IGNORE INTO principal_roles (principal_id, role, scope_type, scope_id, created_at)
                 VALUES (?, 'owner', 'global', '', ?)",
                params![principal_id, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    #[allow(dead_code)]
    pub async fn set_telegram_user_role(
        &self,
        telegram_user_id: i64,
        role: AccessRole,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<()> {
        let external_ref = telegram_user_id.to_string();
        let role = role.as_str().to_string();
        let scope_type = scope_type.to_string();
        let scope_id = scope_id.to_string();
        self.with_conn(move |conn| {
            let now = Utc::now().to_rfc3339();
            let principal_id = format!("telegram-user-{external_ref}");
            conn.execute(
                "INSERT OR IGNORE INTO principals (principal_id, kind, external_ref, created_at)
                 VALUES (?, 'telegram_user', ?, ?)",
                params![principal_id, external_ref, now],
            )?;
            conn.execute(
                "INSERT OR REPLACE INTO principal_roles (principal_id, role, scope_type, scope_id, created_at)
                 VALUES (?, ?, ?, ?, ?)",
                params![principal_id, role, scope_type, scope_id, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn list_telegram_user_roles(&self, telegram_user_id: i64) -> Result<Vec<AccessRole>> {
        let external_ref = telegram_user_id.to_string();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT pr.role
                 FROM principal_roles pr
                 JOIN principals p ON p.principal_id = pr.principal_id
                 WHERE p.kind = 'telegram_user' AND p.external_ref = ?",
            )?;
            let roles = stmt
                .query_map(params![external_ref], |row| {
                    let role: String = row.get(0)?;
                    Ok(AccessRole::from(role.as_str()))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(roles)
        })
        .await
    }

    pub async fn get_telegram_user_effective_role(
        &self,
        telegram_user_id: i64,
    ) -> Result<AccessRole> {
        let roles = self.list_telegram_user_roles(telegram_user_id).await?;
        if roles.contains(&AccessRole::Owner) {
            return Ok(AccessRole::Owner);
        }
        if roles.contains(&AccessRole::Admin) {
            return Ok(AccessRole::Admin);
        }
        Ok(AccessRole::Public)
    }

    #[allow(dead_code)]
    pub async fn upsert_channel_binding(
        &self,
        integration: &str,
        channel_id: &str,
        workspace_id: &str,
        mode: &str,
    ) -> Result<()> {
        let integration = integration.trim().to_ascii_lowercase();
        let channel_id = channel_id.trim().to_string();
        let workspace_id = workspace_id.trim().to_string();
        let mode = mode.trim().to_ascii_lowercase();
        self.with_conn(move |conn| {
            let now = Utc::now().to_rfc3339();
            let metadata_json = "{}".to_string();
            let allowed_actions = "[]".to_string();
            let write_policy = "workspace_default".to_string();
            conn.execute(
                "INSERT INTO channel_bindings
                 (integration, channel_id, workspace_id, mode, metadata_json, allowed_actions, write_policy, fallback_workspace_id, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                 ON CONFLICT(integration, channel_id)
                 DO UPDATE SET
                   workspace_id = excluded.workspace_id,
                   mode = excluded.mode,
                   updated_at = excluded.updated_at",
                params![
                    integration,
                    channel_id,
                    workspace_id,
                    mode,
                    metadata_json,
                    allowed_actions,
                    write_policy,
                    now,
                    Utc::now().to_rfc3339()
                ],
            )?;
            Ok(())
        })
        .await
    }

    #[allow(dead_code)]
    pub async fn get_channel_binding(
        &self,
        integration: &str,
        channel_id: &str,
    ) -> Result<Option<ChannelBindingRecord>> {
        let integration = integration.trim().to_ascii_lowercase();
        let channel_id = channel_id.trim().to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT integration, channel_id, workspace_id, mode, metadata_json, allowed_actions, write_policy, fallback_workspace_id, created_at, updated_at
                     FROM channel_bindings
                     WHERE integration = ? AND channel_id = ?
                     LIMIT 1",
                    params![integration, channel_id],
                    |row| {
                        let allowed_actions_raw: String = row.get(5)?;
                        let created_at: String = row.get(8)?;
                        let updated_at: String = row.get(9)?;
                        let allowed_actions = serde_json::from_str::<Vec<String>>(
                            &allowed_actions_raw,
                        )
                        .unwrap_or_default();
                        Ok(ChannelBindingRecord {
                            integration: row.get(0)?,
                            channel_id: row.get(1)?,
                            workspace_id: row.get(2)?,
                            mode: row.get(3)?,
                            metadata_json: row.get(4)?,
                            allowed_actions,
                            write_policy: row.get(6)?,
                            fallback_workspace_id: row.get(7)?,
                            created_at: DateTime::parse_from_rfc3339(&created_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                            updated_at: DateTime::parse_from_rfc3339(&updated_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                        })
                    },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    #[allow(dead_code)]
    pub async fn delete_channel_binding(&self, integration: &str, channel_id: &str) -> Result<()> {
        let integration = integration.trim().to_ascii_lowercase();
        let channel_id = channel_id.trim().to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "DELETE FROM channel_bindings WHERE integration = ? AND channel_id = ?",
                params![integration, channel_id],
            )?;
            Ok(())
        })
        .await
    }

    #[allow(dead_code)]
    pub async fn list_channel_bindings_for_chat(
        &self,
        chat_id: i64,
    ) -> Result<Vec<ChannelBindingRecord>> {
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT cb.integration, cb.channel_id, cb.workspace_id, cb.mode, cb.metadata_json, cb.allowed_actions, cb.write_policy, cb.fallback_workspace_id, cb.created_at, cb.updated_at
                 FROM channel_bindings cb
                 JOIN workspaces w ON w.workspace_id = cb.workspace_id
                 WHERE w.chat_id = ?
                 ORDER BY cb.updated_at DESC",
            )?;
            let rows = stmt
                .query_map(params![chat_id], |row| {
                    let allowed_actions_raw: String = row.get(5)?;
                    let created_at: String = row.get(8)?;
                    let updated_at: String = row.get(9)?;
                    let allowed_actions =
                        serde_json::from_str::<Vec<String>>(&allowed_actions_raw)
                            .unwrap_or_default();
                    Ok(ChannelBindingRecord {
                        integration: row.get(0)?,
                        channel_id: row.get(1)?,
                        workspace_id: row.get(2)?,
                        mode: row.get(3)?,
                        metadata_json: row.get(4)?,
                        allowed_actions,
                        write_policy: row.get(6)?,
                        fallback_workspace_id: row.get(7)?,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        updated_at: DateTime::parse_from_rfc3339(&updated_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    #[allow(dead_code)]
    pub async fn update_channel_binding_policy(
        &self,
        integration: &str,
        channel_id: &str,
        write_policy: &str,
        allowed_actions: &[String],
        fallback_workspace_id: Option<&str>,
        metadata_json: Option<&str>,
    ) -> Result<()> {
        let integration = integration.trim().to_ascii_lowercase();
        let channel_id = channel_id.trim().to_string();
        let write_policy = write_policy.trim().to_ascii_lowercase();
        let allowed_actions_json = serde_json::to_string(allowed_actions)?;
        let fallback_workspace_id = fallback_workspace_id.map(|s| s.trim().to_string());
        let metadata_json = metadata_json
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "{}".to_string());
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE channel_bindings
                 SET write_policy = ?, allowed_actions = ?, fallback_workspace_id = ?, metadata_json = ?, updated_at = ?
                 WHERE integration = ? AND channel_id = ?",
                params![
                    write_policy,
                    allowed_actions_json,
                    fallback_workspace_id,
                    metadata_json,
                    Utc::now().to_rfc3339(),
                    integration,
                    channel_id
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn touch_workspace(&self, workspace_id: &str) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE workspaces SET updated_at = ? WHERE workspace_id = ?",
                params![Utc::now().to_rfc3339(), workspace_id],
            )?;
            conn.execute(
                "INSERT OR IGNORE INTO workspace_settings (workspace_id, updated_at)
                 VALUES (?, ?)",
                params![workspace_id, Utc::now().to_rfc3339()],
            )?;
            conn.execute(
                "INSERT OR IGNORE INTO workspace_profiles (workspace_id, updated_at)
                 VALUES (?, ?)",
                params![workspace_id, Utc::now().to_rfc3339()],
            )?;
            for integration in Self::known_integrations() {
                conn.execute(
                    "INSERT OR IGNORE INTO workspace_integration_caps
                     (workspace_id, integration, enabled, allow_read, allow_write, allow_moderation, require_human_approval_for_write, updated_at)
                     VALUES (?, ?, 1, 1, 0, 0, 1, ?)",
                    params![workspace_id, integration, Utc::now().to_rfc3339()],
                )?;
            }
            conn.execute(
                "INSERT OR IGNORE INTO public_profiles (workspace_id, updated_at)
                 VALUES (?, ?)",
                params![workspace_id, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn set_active_workspace(
        &self,
        chat_id: i64,
        workspace_id: Option<&str>,
    ) -> Result<()> {
        let workspace_id = workspace_id.map(|s| s.to_string());
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO chat_state (chat_id, active_workspace_id, updated_at)
                 VALUES (?, ?, ?)
                 ON CONFLICT(chat_id) DO UPDATE SET active_workspace_id = excluded.active_workspace_id, updated_at = excluded.updated_at",
                params![chat_id, workspace_id, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_active_workspace_id(&self, chat_id: i64) -> Result<Option<String>> {
        self.with_conn(move |conn| {
            let row: Option<Option<String>> = conn
                .query_row(
                    "SELECT active_workspace_id FROM chat_state WHERE chat_id = ?",
                    params![chat_id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(row.flatten())
        })
        .await
    }

    pub async fn delete_workspace(&self, chat_id: i64, workspace_id: &str) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE chat_state
                 SET active_workspace_id = NULL, updated_at = ?
                 WHERE chat_id = ? AND active_workspace_id = ?",
                params![Utc::now().to_rfc3339(), chat_id, workspace_id],
            )?;
            conn.execute(
                "DELETE FROM workspaces WHERE chat_id = ? AND workspace_id = ?",
                params![chat_id, workspace_id],
            )?;
            conn.execute(
                "DELETE FROM workspace_settings WHERE workspace_id = ?",
                params![workspace_id],
            )?;
            conn.execute(
                "DELETE FROM workspace_profiles WHERE workspace_id = ?",
                params![workspace_id],
            )?;
            conn.execute(
                "DELETE FROM workspace_integration_caps WHERE workspace_id = ?",
                params![workspace_id],
            )?;
            conn.execute(
                "DELETE FROM public_profiles WHERE workspace_id = ?",
                params![workspace_id],
            )?;
            conn.execute(
                "DELETE FROM channel_bindings WHERE workspace_id = ?",
                params![workspace_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_workspace_settings(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceSettingsRecord>> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT workspace_id, security_mode, mode_expires_at, write_tools_enabled, write_tools_expires_at, shell_pack, fetch_mode, trusted_domains, updated_at
                     FROM workspace_settings
                     WHERE workspace_id = ?",
                    params![workspace_id],
                    |row| {
                        let mode_expires_at: Option<String> = row.get(2)?;
                        let write_tools_expires_at: Option<String> = row.get(4)?;
                        let trusted_domains_raw: String = row.get(7)?;
                        let updated_at: String = row.get(8)?;
                        let trusted_domains: Vec<String> =
                            serde_json::from_str(&trusted_domains_raw).unwrap_or_default();
                        Ok(WorkspaceSettingsRecord {
                            workspace_id: row.get(0)?,
                            security_mode: WorkspaceSecurityMode::from(
                                row.get::<_, String>(1)?.as_str(),
                            ),
                            mode_expires_at: mode_expires_at.and_then(|ts| {
                                DateTime::parse_from_rfc3339(&ts)
                                    .ok()
                                    .map(|d| d.with_timezone(&Utc))
                            }),
                            write_tools_enabled: row.get::<_, i64>(3)? != 0,
                            write_tools_expires_at: write_tools_expires_at.and_then(|ts| {
                                DateTime::parse_from_rfc3339(&ts)
                                    .ok()
                                    .map(|d| d.with_timezone(&Utc))
                            }),
                            shell_pack: WorkspaceShellPack::from(
                                row.get::<_, String>(5)?.as_str(),
                            ),
                            fetch_mode: WorkspaceFetchMode::from(row.get::<_, String>(6)?.as_str()),
                            trusted_domains,
                            updated_at: DateTime::parse_from_rfc3339(&updated_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                        })
                    },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn ensure_workspace_settings(&self, workspace_id: &str) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO workspace_settings (workspace_id, updated_at)
                 VALUES (?, ?)",
                params![workspace_id, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn ensure_workspace_public_profile(&self, workspace_id: &str) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO public_profiles (workspace_id, updated_at)
                 VALUES (?, ?)",
                params![workspace_id, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_workspace_public_profile(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspacePublicProfileRecord>> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT workspace_id, brand_name, public_refusal_text, public_scope_text, show_sources, updated_at
                     FROM public_profiles
                     WHERE workspace_id = ?",
                    params![workspace_id],
                    |row| {
                        let updated_at: String = row.get(5)?;
                        Ok(WorkspacePublicProfileRecord {
                            workspace_id: row.get(0)?,
                            brand_name: row.get(1)?,
                            public_refusal_text: row.get(2)?,
                            public_scope_text: row.get(3)?,
                            show_sources: row.get::<_, i64>(4)? != 0,
                            updated_at: DateTime::parse_from_rfc3339(&updated_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                        })
                    },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn update_workspace_public_show_sources(
        &self,
        workspace_id: &str,
        show_sources: bool,
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        let show_sources_int: i64 = if show_sources { 1 } else { 0 };
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO public_profiles (workspace_id, show_sources, updated_at)
                 VALUES (?, ?, ?)
                 ON CONFLICT(workspace_id) DO UPDATE
                 SET show_sources = excluded.show_sources, updated_at = excluded.updated_at",
                params![workspace_id, show_sources_int, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn ensure_workspace_integration_caps(&self, workspace_id: &str) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            let now = Utc::now().to_rfc3339();
            for integration in Self::known_integrations() {
                conn.execute(
                    "INSERT OR IGNORE INTO workspace_integration_caps
                     (workspace_id, integration, enabled, allow_read, allow_write, allow_moderation, require_human_approval_for_write, updated_at)
                     VALUES (?, ?, 1, 1, 0, 0, 1, ?)",
                    params![workspace_id, integration, now],
                )?;
            }
            Ok(())
        })
        .await
    }

    pub async fn list_workspace_integration_caps(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceIntegrationCapabilityRecord>> {
        self.ensure_workspace_integration_caps(workspace_id).await?;
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT workspace_id, integration, enabled, allow_read, allow_write, allow_moderation, require_human_approval_for_write, updated_at
                 FROM workspace_integration_caps
                 WHERE workspace_id = ?
                 ORDER BY integration ASC",
            )?;
            let rows = stmt
                .query_map(params![workspace_id], |row| {
                    let updated_at: String = row.get(7)?;
                    Ok(WorkspaceIntegrationCapabilityRecord {
                        workspace_id: row.get(0)?,
                        integration: row.get(1)?,
                        enabled: row.get::<_, i64>(2)? != 0,
                        allow_read: row.get::<_, i64>(3)? != 0,
                        allow_write: row.get::<_, i64>(4)? != 0,
                        allow_moderation: row.get::<_, i64>(5)? != 0,
                        require_human_approval_for_write: row.get::<_, i64>(6)? != 0,
                        updated_at: DateTime::parse_from_rfc3339(&updated_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn get_workspace_integration_cap(
        &self,
        workspace_id: &str,
        integration: &str,
    ) -> Result<Option<WorkspaceIntegrationCapabilityRecord>> {
        self.ensure_workspace_integration_caps(workspace_id).await?;
        let workspace_id = workspace_id.to_string();
        let integration = integration.to_ascii_lowercase();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT workspace_id, integration, enabled, allow_read, allow_write, allow_moderation, require_human_approval_for_write, updated_at
                     FROM workspace_integration_caps
                     WHERE workspace_id = ? AND integration = ?",
                    params![workspace_id, integration],
                    |row| {
                        let updated_at: String = row.get(7)?;
                        Ok(WorkspaceIntegrationCapabilityRecord {
                            workspace_id: row.get(0)?,
                            integration: row.get(1)?,
                            enabled: row.get::<_, i64>(2)? != 0,
                            allow_read: row.get::<_, i64>(3)? != 0,
                            allow_write: row.get::<_, i64>(4)? != 0,
                            allow_moderation: row.get::<_, i64>(5)? != 0,
                            require_human_approval_for_write: row.get::<_, i64>(6)? != 0,
                            updated_at: DateTime::parse_from_rfc3339(&updated_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                        })
                    },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_workspace_integration_cap(
        &self,
        workspace_id: &str,
        integration: &str,
        enabled: bool,
        allow_read: bool,
        allow_write: bool,
        allow_moderation: bool,
        require_human_approval_for_write: bool,
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        let integration = integration.to_ascii_lowercase();
        let enabled_int: i64 = if enabled { 1 } else { 0 };
        let allow_read_int: i64 = if allow_read { 1 } else { 0 };
        let allow_write_int: i64 = if allow_write { 1 } else { 0 };
        let allow_mod_int: i64 = if allow_moderation { 1 } else { 0 };
        let approval_int: i64 = if require_human_approval_for_write {
            1
        } else {
            0
        };
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO workspace_integration_caps
                 (workspace_id, integration, enabled, allow_read, allow_write, allow_moderation, require_human_approval_for_write, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(workspace_id, integration) DO UPDATE SET
                    enabled = excluded.enabled,
                    allow_read = excluded.allow_read,
                    allow_write = excluded.allow_write,
                    allow_moderation = excluded.allow_moderation,
                    require_human_approval_for_write = excluded.require_human_approval_for_write,
                    updated_at = excluded.updated_at",
                params![
                    workspace_id,
                    integration,
                    enabled_int,
                    allow_read_int,
                    allow_write_int,
                    allow_mod_int,
                    approval_int,
                    Utc::now().to_rfc3339()
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn toggle_workspace_integration_cap_field(
        &self,
        workspace_id: &str,
        integration: &str,
        field: &str,
    ) -> Result<Option<WorkspaceIntegrationCapabilityRecord>> {
        let Some(mut cap) = self
            .get_workspace_integration_cap(workspace_id, integration)
            .await?
        else {
            return Ok(None);
        };
        match field {
            "enabled" => cap.enabled = !cap.enabled,
            "allow_read" => cap.allow_read = !cap.allow_read,
            "allow_write" => cap.allow_write = !cap.allow_write,
            "allow_moderation" => cap.allow_moderation = !cap.allow_moderation,
            "require_human_approval_for_write" => {
                cap.require_human_approval_for_write = !cap.require_human_approval_for_write
            }
            _ => return Ok(None),
        }
        self.upsert_workspace_integration_cap(
            &cap.workspace_id,
            &cap.integration,
            cap.enabled,
            cap.allow_read,
            cap.allow_write,
            cap.allow_moderation,
            cap.require_human_approval_for_write,
        )
        .await?;
        self.get_workspace_integration_cap(workspace_id, integration)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_audit_event(
        &self,
        chat_id: i64,
        workspace_id: Option<&str>,
        principal_id: Option<&str>,
        role: Option<&str>,
        audience: &str,
        event_type: &str,
        details: &str,
    ) -> Result<()> {
        let workspace_id = workspace_id.map(|s| s.to_string());
        let principal_id = principal_id.map(|s| s.to_string());
        let role = role.map(|s| s.to_string());
        let audience = audience.to_string();
        let event_type = event_type.to_string();
        let details = details.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO audit_events (chat_id, workspace_id, principal_id, role, audience, event_type, details, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    chat_id,
                    workspace_id,
                    principal_id,
                    role,
                    audience,
                    event_type,
                    details,
                    Utc::now().to_rfc3339()
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn list_audit_events(
        &self,
        chat_id: i64,
        workspace_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AuditEventRecord>> {
        let workspace_id = workspace_id.map(|s| s.to_string());
        let limit = limit.clamp(1, 200) as i64;
        self.with_conn(move |conn| {
            let (sql, params_any): (&str, Vec<rusqlite::types::Value>) = if let Some(ws) = workspace_id
            {
                (
                    "SELECT id, chat_id, workspace_id, principal_id, role, audience, event_type, details, created_at
                     FROM audit_events
                     WHERE chat_id = ? AND workspace_id = ?
                     ORDER BY id DESC
                     LIMIT ?",
                    vec![chat_id.into(), ws.into(), limit.into()],
                )
            } else {
                (
                    "SELECT id, chat_id, workspace_id, principal_id, role, audience, event_type, details, created_at
                     FROM audit_events
                     WHERE chat_id = ?
                     ORDER BY id DESC
                     LIMIT ?",
                    vec![chat_id.into(), limit.into()],
                )
            };
            let mut stmt = conn.prepare(sql)?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(params_any), |row| {
                    let created_at: String = row.get(8)?;
                    Ok(AuditEventRecord {
                        id: row.get(0)?,
                        chat_id: row.get(1)?,
                        workspace_id: row.get(2)?,
                        principal_id: row.get(3)?,
                        role: row.get(4)?,
                        audience: row.get(5)?,
                        event_type: row.get(6)?,
                        details: row.get(7)?,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn update_workspace_security_mode(
        &self,
        workspace_id: &str,
        mode: WorkspaceSecurityMode,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        let expires_at = expires_at.map(|d| d.to_rfc3339());
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO workspace_settings (workspace_id, security_mode, mode_expires_at, updated_at)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(workspace_id) DO UPDATE SET
                    security_mode = excluded.security_mode,
                    mode_expires_at = excluded.mode_expires_at,
                    updated_at = excluded.updated_at",
                params![workspace_id, mode.as_str(), expires_at, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn update_workspace_shell_pack(
        &self,
        workspace_id: &str,
        shell_pack: WorkspaceShellPack,
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO workspace_settings (workspace_id, shell_pack, updated_at)
                 VALUES (?, ?, ?)
                 ON CONFLICT(workspace_id) DO UPDATE SET
                    shell_pack = excluded.shell_pack,
                    updated_at = excluded.updated_at",
                params![workspace_id, shell_pack.as_str(), Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn update_workspace_fetch_mode(
        &self,
        workspace_id: &str,
        fetch_mode: WorkspaceFetchMode,
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO workspace_settings (workspace_id, fetch_mode, updated_at)
                 VALUES (?, ?, ?)
                 ON CONFLICT(workspace_id) DO UPDATE SET
                    fetch_mode = excluded.fetch_mode,
                    updated_at = excluded.updated_at",
                params![workspace_id, fetch_mode.as_str(), Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn update_workspace_trusted_domains(
        &self,
        workspace_id: &str,
        domains: &[String],
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        let domains_json = serde_json::to_string(domains).unwrap_or_else(|_| "[]".to_string());
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO workspace_settings (workspace_id, trusted_domains, updated_at)
                 VALUES (?, ?, ?)
                 ON CONFLICT(workspace_id) DO UPDATE SET
                    trusted_domains = excluded.trusted_domains,
                    updated_at = excluded.updated_at",
                params![workspace_id, domains_json, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_workspace_profile(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceProfileRecord>> {
        let workspace_id = workspace_id.to_string();
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT workspace_id, role_name, skill_prompt, allowed_tools, updated_at
                     FROM workspace_profiles
                     WHERE workspace_id = ?",
                    params![workspace_id],
                    |row| {
                        let allowed_tools_raw: String = row.get(3)?;
                        let updated_at: String = row.get(4)?;
                        let allowed_tools: Vec<String> =
                            serde_json::from_str(&allowed_tools_raw).unwrap_or_default();
                        let raw_skill: String = row.get(2)?;
                        Ok(WorkspaceProfileRecord {
                            workspace_id: row.get(0)?,
                            role_name: row.get(1)?,
                            skill_prompt: unprotect_value(&crypto, &raw_skill),
                            allowed_tools,
                            updated_at: DateTime::parse_from_rfc3339(&updated_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                        })
                    },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn ensure_workspace_profile(&self, workspace_id: &str) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO workspace_profiles (workspace_id, updated_at)
                 VALUES (?, ?)",
                params![workspace_id, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn update_workspace_profile_role_and_tools(
        &self,
        workspace_id: &str,
        role_name: &str,
        allowed_tools: &[String],
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        let role_name = role_name.to_string();
        let tools_json = serde_json::to_string(allowed_tools).unwrap_or_else(|_| "[]".to_string());
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO workspace_profiles (workspace_id, role_name, allowed_tools, updated_at)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(workspace_id) DO UPDATE SET
                    role_name = excluded.role_name,
                    allowed_tools = excluded.allowed_tools,
                    updated_at = excluded.updated_at",
                params![workspace_id, role_name, tools_json, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn update_workspace_profile_skill_prompt(
        &self,
        workspace_id: &str,
        skill_prompt: &str,
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        let skill_prompt = self.protect_value(skill_prompt);
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO workspace_profiles (workspace_id, skill_prompt, updated_at)
                 VALUES (?, ?, ?)
                 ON CONFLICT(workspace_id) DO UPDATE SET
                    skill_prompt = excluded.skill_prompt,
                    updated_at = excluded.updated_at",
                params![workspace_id, skill_prompt, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        })
        .await
    }

    fn protect_secret_value(&self, input: &str) -> String {
        if let Some(crypto) = self.crypto_snapshot() {
            crypto
                .encrypt_str(input)
                .unwrap_or_else(|_| input.to_string())
        } else {
            input.to_string()
        }
    }

    pub async fn upsert_workspace_secret(
        &self,
        workspace_id: &str,
        secret_name: &str,
        secret_value: &str,
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        let secret_name = secret_name.to_string();
        let secret_value = self.protect_secret_value(secret_value);
        self.with_conn(move |conn| {
            let now = Utc::now().to_rfc3339();
            conn.execute(
                "INSERT INTO workspace_secrets (workspace_id, secret_name, secret_value, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(workspace_id, secret_name) DO UPDATE SET
                    secret_value = excluded.secret_value,
                    updated_at = excluded.updated_at",
                params![workspace_id, secret_name, secret_value, now, now],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn list_workspace_secrets(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<WorkspaceSecretRecord>> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT secret_name
                 FROM workspace_secrets
                 WHERE workspace_id = ?
                 ORDER BY secret_name ASC",
            )?;
            let rows = stmt
                .query_map(params![workspace_id], |row| {
                    Ok(WorkspaceSecretRecord {
                        secret_name: row.get(0)?,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn delete_workspace_secret(
        &self,
        workspace_id: &str,
        secret_name: &str,
    ) -> Result<bool> {
        let workspace_id = workspace_id.to_string();
        let secret_name = secret_name.to_string();
        self.with_conn(move |conn| {
            let changed = conn.execute(
                "DELETE FROM workspace_secrets WHERE workspace_id = ? AND secret_name = ?",
                params![workspace_id, secret_name],
            )?;
            Ok(changed > 0)
        })
        .await
    }

    #[allow(dead_code)]
    pub async fn get_workspace_secret_value(
        &self,
        workspace_id: &str,
        secret_name: &str,
    ) -> Result<Option<String>> {
        let workspace_id = workspace_id.to_string();
        let secret_name = secret_name.to_string();
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let raw: Option<String> = conn
                .query_row(
                    "SELECT secret_value FROM workspace_secrets WHERE workspace_id = ? AND secret_name = ?",
                    params![workspace_id, secret_name],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(raw) = raw else { return Ok(None) };
            if let Some(c) = crypto.as_ref() {
                if let Some(pt) = c.decrypt_str(&raw)? {
                    return Ok(Some(pt));
                }
            }
            if crate::crypto::Crypto::is_encrypted(&raw) {
                anyhow::bail!("workspace secret is encrypted but current key cannot decrypt it");
            }
            Ok(Some(raw))
        })
        .await
    }

    pub async fn rotate_encrypted_data(
        &self,
        new_crypto: std::sync::Arc<crate::crypto::Crypto>,
    ) -> Result<ReencryptStats> {
        let old_crypto = self
            .crypto_snapshot()
            .ok_or_else(|| anyhow::anyhow!("Encryption is not enabled; no key to rotate"))?;

        let new_crypto_for_tx = new_crypto.clone();
        let stats = self
            .with_conn(move |conn| {
                conn.execute_batch("BEGIN IMMEDIATE TRANSACTION;")?;
                let inner = (|| -> Result<ReencryptStats> {
                    let mut stats = ReencryptStats::default();
                    rotate_table_column(
                        conn,
                        ReencryptColumnSpec {
                            table: "messages",
                            id_col: "id",
                            value_col: "content",
                            nullable: false,
                        },
                        &old_crypto,
                        &new_crypto_for_tx,
                        &mut stats,
                    )?;
                    rotate_table_column(
                        conn,
                        ReencryptColumnSpec {
                            table: "summaries",
                            id_col: "id",
                            value_col: "content",
                            nullable: false,
                        },
                        &old_crypto,
                        &new_crypto_for_tx,
                        &mut stats,
                    )?;
                    rotate_table_column(
                        conn,
                        ReencryptColumnSpec {
                            table: "agent_states",
                            id_col: "rowid",
                            value_col: "state_json",
                            nullable: false,
                        },
                        &old_crypto,
                        &new_crypto_for_tx,
                        &mut stats,
                    )?;
                    rotate_table_column(
                        conn,
                        ReencryptColumnSpec {
                            table: "workspace_profiles",
                            id_col: "workspace_id",
                            value_col: "skill_prompt",
                            nullable: false,
                        },
                        &old_crypto,
                        &new_crypto_for_tx,
                        &mut stats,
                    )?;
                    rotate_table_column(
                        conn,
                        ReencryptColumnSpec {
                            table: "jobs",
                            id_col: "id",
                            value_col: "result",
                            nullable: true,
                        },
                        &old_crypto,
                        &new_crypto_for_tx,
                        &mut stats,
                    )?;
                    rotate_table_column(
                        conn,
                        ReencryptColumnSpec {
                            table: "workspace_secrets",
                            id_col: "id",
                            value_col: "secret_value",
                            nullable: false,
                        },
                        &old_crypto,
                        &new_crypto_for_tx,
                        &mut stats,
                    )?;
                    Ok(stats)
                })();
                match inner {
                    Ok(stats) => {
                        conn.execute_batch("COMMIT;")?;
                        Ok(stats)
                    }
                    Err(err) => {
                        let _ = conn.execute_batch("ROLLBACK;");
                        Err(err)
                    }
                }
            })
            .await?;
        self.set_crypto(Some(new_crypto));
        Ok(stats)
    }

    pub async fn bind_legacy_context_to_workspace(
        &self,
        chat_id: i64,
        workspace_id: &str,
    ) -> Result<()> {
        let workspace_id = workspace_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE messages
                 SET workspace_id = ?
                 WHERE chat_id = ?
                   AND workspace_id IS NULL",
                params![workspace_id, chat_id],
            )?;
            conn.execute(
                "UPDATE summaries
                 SET workspace_id = ?
                 WHERE chat_id = ?
                   AND workspace_id IS NULL",
                params![workspace_id, chat_id],
            )?;
            conn.execute(
                "UPDATE runs
                 SET workspace_id = ?
                 WHERE chat_id = ?
                   AND workspace_id IS NULL",
                params![workspace_id, chat_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn insert_task(&self, task: &TaskRecord) -> Result<()> {
        let task = task.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO tasks (task_id, run_id, agent, action_type, goal, risk_tier, status, job_id, created_at, updated_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    task.task_id,
                    task.run_id,
                    task.agent,
                    task.action_type,
                    task.goal,
                    task.risk_tier.as_str(),
                    task.status.as_str(),
                    task.job_id,
                    task.created_at.to_rfc3339(),
                    task.updated_at.to_rfc3339()
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn list_tasks(&self, run_id: &str) -> Result<Vec<TaskRecord>> {
        let run_id = run_id.to_string();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT task_id, run_id, agent, action_type, goal, risk_tier, status, job_id, created_at, updated_at
                 FROM tasks WHERE run_id = ?
                 ORDER BY created_at ASC",
            )?;
            let rows = stmt
                .query_map(params![run_id], |row| {
                    let created_at: String = row.get(8)?;
                    let updated_at: String = row.get(9)?;
                    Ok(TaskRecord {
                        task_id: row.get(0)?,
                        run_id: row.get(1)?,
                        agent: row.get(2)?,
                        action_type: row.get(3)?,
                        goal: row.get(4)?,
                        risk_tier: RiskTier::from(row.get::<_, String>(5)?.as_str()),
                        status: TaskStatus::from(row.get::<_, String>(6)?.as_str()),
                        job_id: row.get(7)?,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        updated_at: DateTime::parse_from_rfc3339(&updated_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn get_task(&self, task_id: &str) -> Result<Option<TaskRecord>> {
        let task_id = task_id.to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                "SELECT task_id, run_id, agent, action_type, goal, risk_tier, status, job_id, created_at, updated_at
                 FROM tasks WHERE task_id = ?",
                params![task_id],
                |row| {
                    let created_at: String = row.get(8)?;
                    let updated_at: String = row.get(9)?;
                    Ok(TaskRecord {
                        task_id: row.get(0)?,
                        run_id: row.get(1)?,
                        agent: row.get(2)?,
                        action_type: row.get(3)?,
                        goal: row.get(4)?,
                        risk_tier: RiskTier::from(row.get::<_, String>(5)?.as_str()),
                        status: TaskStatus::from(row.get::<_, String>(6)?.as_str()),
                        job_id: row.get(7)?,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        updated_at: DateTime::parse_from_rfc3339(&updated_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn get_task_by_job_id(&self, job_id: &str) -> Result<Option<TaskRecord>> {
        let job_id = job_id.to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                "SELECT task_id, run_id, agent, action_type, goal, risk_tier, status, job_id, created_at, updated_at
                 FROM tasks WHERE job_id = ?",
                params![job_id],
                |row| {
                    let created_at: String = row.get(8)?;
                    let updated_at: String = row.get(9)?;
                    Ok(TaskRecord {
                        task_id: row.get(0)?,
                        run_id: row.get(1)?,
                        agent: row.get(2)?,
                        action_type: row.get(3)?,
                        goal: row.get(4)?,
                        risk_tier: RiskTier::from(row.get::<_, String>(5)?.as_str()),
                        status: TaskStatus::from(row.get::<_, String>(6)?.as_str()),
                        job_id: row.get(7)?,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        updated_at: DateTime::parse_from_rfc3339(&updated_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn update_task_status(&self, task_id: &str, status: TaskStatus) -> Result<()> {
        let task_id = task_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?",
                params![status.as_str(), Utc::now().to_rfc3339(), task_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn try_assign_task_job(&self, task_id: &str, job_id: &str) -> Result<bool> {
        let task_id = task_id.to_string();
        let job_id = job_id.to_string();
        self.with_conn(move |conn| {
            let changed = conn.execute(
                "UPDATE tasks
                 SET job_id = ?, updated_at = ?
                 WHERE task_id = ?
                   AND job_id IS NULL
                   AND status IN ('queued','blocked')",
                params![job_id, Utc::now().to_rfc3339(), task_id],
            )?;
            Ok(changed == 1)
        })
        .await
    }

    pub async fn clear_task_job(&self, task_id: &str, job_id: &str) -> Result<bool> {
        let task_id = task_id.to_string();
        let job_id = job_id.to_string();
        self.with_conn(move |conn| {
            let changed = conn.execute(
                "UPDATE tasks SET job_id = NULL, updated_at = ? WHERE task_id = ? AND job_id = ?",
                params![Utc::now().to_rfc3339(), task_id, job_id],
            )?;
            Ok(changed == 1)
        })
        .await
    }

    pub async fn insert_task_dep(&self, task_id: &str, depends_on_task_id: &str) -> Result<()> {
        let task_id = task_id.to_string();
        let depends_on_task_id = depends_on_task_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO task_deps (task_id, depends_on_task_id) VALUES (?, ?)",
                params![task_id, depends_on_task_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn list_task_deps(&self, run_id: &str) -> Result<Vec<(String, String)>> {
        let run_id = run_id.to_string();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT td.task_id, td.depends_on_task_id
                 FROM task_deps td
                 JOIN tasks t ON t.task_id = td.task_id
                 WHERE t.run_id = ?",
            )?;
            let rows = stmt
                .query_map(params![run_id], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn insert_approval(&self, approval: &ApprovalRecord) -> Result<()> {
        let approval = approval.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO approvals (approval_id, task_id, status, reason, created_at, decided_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    approval.approval_id,
                    approval.task_id,
                    approval.status.as_str(),
                    approval.reason,
                    approval.created_at.to_rfc3339(),
                    approval
                        .decided_at
                        .as_ref()
                        .map(|d| d.to_rfc3339())
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_approval_for_task(&self, task_id: &str) -> Result<Option<ApprovalRecord>> {
        let task_id = task_id.to_string();
        self.with_conn(move |conn| {
            let row = conn
                .query_row(
                    "SELECT approval_id, task_id, status, reason, created_at, decided_at
                 FROM approvals WHERE task_id = ? ORDER BY created_at DESC LIMIT 1",
                    params![task_id],
                    |row| {
                        let created_at: String = row.get(4)?;
                        let decided_at: Option<String> = row.get(5)?;
                        Ok(ApprovalRecord {
                            approval_id: row.get(0)?,
                            task_id: row.get(1)?,
                            status: ApprovalStatus::from(row.get::<_, String>(2)?.as_str()),
                            reason: row.get(3)?,
                            created_at: DateTime::parse_from_rfc3339(&created_at)
                                .map(|d| d.with_timezone(&Utc))
                                .unwrap_or_else(|_| Utc::now()),
                            decided_at: decided_at.and_then(|ts| {
                                DateTime::parse_from_rfc3339(&ts)
                                    .ok()
                                    .map(|d| d.with_timezone(&Utc))
                            }),
                        })
                    },
                )
                .optional()?;
            Ok(row)
        })
        .await
    }

    pub async fn update_approval_status(
        &self,
        task_id: &str,
        status: ApprovalStatus,
        reason: Option<&str>,
    ) -> Result<()> {
        let task_id = task_id.to_string();
        let reason = reason.map(|s| s.to_string());
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE approvals SET status = ?, reason = COALESCE(?, reason), decided_at = ? WHERE task_id = ?",
                params![
                    status.as_str(),
                    reason,
                    Utc::now().to_rfc3339(),
                    task_id
                ],
            )?;
            Ok(())
        })
        .await
    }

    #[allow(dead_code)]
    pub async fn list_pending_approvals_by_run(&self, run_id: &str) -> Result<Vec<ApprovalRecord>> {
        let run_id = run_id.to_string();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT a.approval_id, a.task_id, a.status, a.reason, a.created_at, a.decided_at
                 FROM approvals a
                 JOIN tasks t ON t.task_id = a.task_id
                 WHERE t.run_id = ? AND a.status = 'pending'
                 ORDER BY a.created_at ASC",
            )?;
            let rows = stmt
                .query_map(params![run_id], |row| {
                    let created_at: String = row.get(4)?;
                    let decided_at: Option<String> = row.get(5)?;
                    Ok(ApprovalRecord {
                        approval_id: row.get(0)?,
                        task_id: row.get(1)?,
                        status: ApprovalStatus::from(row.get::<_, String>(2)?.as_str()),
                        reason: row.get(3)?,
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        decided_at: decided_at.and_then(|ts| {
                            DateTime::parse_from_rfc3339(&ts)
                                .ok()
                                .map(|d| d.with_timezone(&Utc))
                        }),
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn insert_approval_grant(&self, grant: &ApprovalGrantRecord) -> Result<()> {
        let grant = grant.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO approval_grants (grant_id, scope_type, scope_id, action_type, command_prefix, risk_tier, expires_at, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    grant.grant_id,
                    grant.scope_type,
                    grant.scope_id,
                    grant.action_type,
                    grant.command_prefix,
                    grant.risk_tier.as_str(),
                    grant.expires_at.to_rfc3339(),
                    grant.created_at.to_rfc3339(),
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn list_active_approval_grants_for_scope(
        &self,
        run_id: &str,
        workspace_path: &Path,
        now: DateTime<Utc>,
    ) -> Result<Vec<ApprovalGrantRecord>> {
        let run_id = run_id.to_string();
        let workspace = workspace_path.to_string_lossy().to_string();
        let now_s = now.to_rfc3339();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT grant_id, scope_type, scope_id, action_type, command_prefix, risk_tier, expires_at, created_at
                 FROM approval_grants
                 WHERE expires_at > ?
                   AND ((scope_type = 'run' AND scope_id = ?) OR (scope_type = 'workspace' AND scope_id = ?))
                 ORDER BY created_at DESC",
            )?;
            let rows = stmt
                .query_map(params![now_s, run_id, workspace], |row| {
                    let expires_at: String = row.get(6)?;
                    let created_at: String = row.get(7)?;
                    Ok(ApprovalGrantRecord {
                        grant_id: row.get(0)?,
                        scope_type: row.get(1)?,
                        scope_id: row.get(2)?,
                        action_type: row.get(3)?,
                        command_prefix: row.get(4)?,
                        risk_tier: RiskTier::from(row.get::<_, String>(5)?.as_str()),
                        expires_at: DateTime::parse_from_rfc3339(&expires_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        created_at: DateTime::parse_from_rfc3339(&created_at)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
    }

    pub async fn save_summary(
        &self,
        chat_id: i64,
        summary: &str,
        last_message_id: i64,
    ) -> Result<()> {
        let summary = self.protect_value(summary);
        self.with_conn(move |conn| {
            let workspace_id = active_workspace_id_for_chat(conn, chat_id)?;
            let ts = Utc::now().to_rfc3339();
            conn.execute(
                "INSERT INTO summaries (chat_id, workspace_id, content, up_to_message_id, created_at)
                 VALUES (?, ?, ?, ?, ?)",
                params![chat_id, workspace_id, summary, last_message_id, ts],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn get_latest_summary(&self, chat_id: i64) -> Result<Option<String>> {
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let workspace_id = active_workspace_id_for_chat(conn, chat_id)?;
            let summary: Option<String> = conn
                .query_row(
                    "SELECT content FROM summaries
                     WHERE chat_id = ?
                       AND ((workspace_id = ?) OR (workspace_id IS NULL AND ? IS NULL))
                     ORDER BY id DESC LIMIT 1",
                    params![chat_id, workspace_id, workspace_id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(summary.map(|s| unprotect_value(&crypto, &s)))
        })
        .await
    }

    pub async fn clear_context(&self, chat_id: i64) -> Result<()> {
        self.with_conn(move |conn| {
            let workspace_id = active_workspace_id_for_chat(conn, chat_id)?;
            conn.execute(
                "DELETE FROM messages
                 WHERE chat_id = ?
                   AND ((workspace_id = ?) OR (workspace_id IS NULL AND ? IS NULL))",
                params![chat_id, workspace_id, workspace_id],
            )?;
            conn.execute(
                "DELETE FROM summaries
                 WHERE chat_id = ?
                   AND ((workspace_id = ?) OR (workspace_id IS NULL AND ? IS NULL))",
                params![chat_id, workspace_id, workspace_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn clear_workspace_runtime_state(&self, chat_id: i64) -> Result<()> {
        self.with_conn(move |conn| {
            let workspace_id = active_workspace_id_for_chat(conn, chat_id)?;
            let run_ids: Vec<String> = {
                let mut stmt = conn.prepare(
                    "SELECT run_id FROM runs
                     WHERE chat_id = ?
                       AND ((workspace_id = ?) OR (workspace_id IS NULL AND ? IS NULL))",
                )?;
                let rows = stmt
                    .query_map(params![chat_id, workspace_id, workspace_id], |row| {
                        row.get::<_, String>(0)
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                rows
            };

            for run_id in run_ids {
                conn.execute(
                    "DELETE FROM approvals
                     WHERE task_id IN (SELECT task_id FROM tasks WHERE run_id = ?)",
                    params![run_id],
                )?;
                conn.execute(
                    "DELETE FROM task_deps
                     WHERE task_id IN (SELECT task_id FROM tasks WHERE run_id = ?)
                        OR depends_on_task_id IN (SELECT task_id FROM tasks WHERE run_id = ?)",
                    params![run_id, run_id],
                )?;
                conn.execute(
                    "DELETE FROM jobs
                     WHERE id IN (
                        SELECT job_id FROM tasks
                        WHERE run_id = ? AND job_id IS NOT NULL
                     )",
                    params![run_id],
                )?;
                conn.execute("DELETE FROM tasks WHERE run_id = ?", params![run_id])?;
                conn.execute("DELETE FROM run_memories WHERE run_id = ?", params![run_id])?;
                conn.execute("DELETE FROM agent_states WHERE run_id = ?", params![run_id])?;
                conn.execute("DELETE FROM runs WHERE run_id = ?", params![run_id])?;
            }

            conn.execute(
                "UPDATE chat_state
                 SET active_run_id = NULL, updated_at = ?
                 WHERE chat_id = ?",
                params![Utc::now().to_rfc3339(), chat_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn insert_job(&self, job: &JobRecord) -> Result<()> {
        let job = job.clone();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO jobs (id, chat_id, action_type, goal, state, result, log_path, work_dir, created_at, updated_at, depends_on)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    job.id,
                    job.chat_id,
                    job.action_type,
                    job.goal,
                    job.state.as_str(),
                    job.result,
                    job.log_path.to_string_lossy(),
                    job.work_dir.to_string_lossy(),
                    job.created_at.to_rfc3339(),
                    job.updated_at.to_rfc3339(),
                    job.depends_on,
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn update_job_state(
        &self,
        job_id: &str,
        state: JobState,
        result: Option<&str>,
    ) -> Result<()> {
        let job_id = job_id.to_string();
        let result = result.map(|s| self.protect_value(s));
        self.with_conn(move |conn| {
            let ts = Utc::now().to_rfc3339();
            conn.execute(
                "UPDATE jobs SET state = ?, result = ?, updated_at = ? WHERE id = ?",
                params![state.as_str(), result, ts, job_id],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn fail_orphaned_running_jobs(
        &self,
        reason: &str,
        older_than: Option<DateTime<Utc>>,
    ) -> Result<usize> {
        let reason = reason.to_string();
        let older_than = older_than.map(|d| d.to_rfc3339());
        self.with_conn(move |conn| {
            let ts = Utc::now().to_rfc3339();
            let updated = if let Some(cutoff) = older_than {
                conn.execute(
                    "UPDATE jobs SET state = 'failed', result = ?, updated_at = ?
                     WHERE state = 'running' AND updated_at < ?",
                    params![reason, ts, cutoff],
                )?
            } else {
                conn.execute(
                    "UPDATE jobs SET state = 'failed', result = ?, updated_at = ?
                     WHERE state = 'running'",
                    params![reason, ts],
                )?
            };
            Ok(updated)
        })
        .await
    }

    pub async fn get_job(&self, job_id: &str) -> Result<Option<JobRecord>> {
        let job_id = job_id.to_string();
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let job = conn
                .query_row(
                    "SELECT id, chat_id, action_type, goal, state, result, log_path, work_dir, created_at, updated_at, depends_on
                     FROM jobs WHERE id = ?",
                    params![job_id],
                    |row| map_job(row, &crypto),
                )
                .optional()?;
            Ok(job)
        })
        .await
    }

    pub async fn get_active_jobs(&self, chat_id: i64) -> Result<Vec<JobRecord>> {
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT j.id, j.chat_id, j.action_type, j.goal, j.state, j.result, j.log_path, j.work_dir, j.created_at, j.updated_at, j.depends_on
                 FROM jobs j
                 LEFT JOIN tasks t ON t.job_id = j.id
                 LEFT JOIN runs r ON r.run_id = t.run_id
                 WHERE j.chat_id = ?
                   AND j.state IN ('queued','running')
                   AND (
                        r.workspace_id = (SELECT active_workspace_id FROM chat_state WHERE chat_id = ?)
                        OR ((SELECT active_workspace_id FROM chat_state WHERE chat_id = ?) IS NULL AND r.workspace_id IS NULL)
                        OR r.run_id IS NULL
                   )
                 ORDER BY j.created_at ASC",
            )?;
            let jobs = stmt
                .query_map(params![chat_id, chat_id, chat_id], |row| map_job(row, &crypto))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(jobs)
        })
        .await
    }

    pub async fn get_recent_jobs(&self, chat_id: i64, limit: usize) -> Result<Vec<JobRecord>> {
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT j.id, j.chat_id, j.action_type, j.goal, j.state, j.result, j.log_path, j.work_dir, j.created_at, j.updated_at, j.depends_on
                 FROM jobs j
                 LEFT JOIN tasks t ON t.job_id = j.id
                 LEFT JOIN runs r ON r.run_id = t.run_id
                 WHERE j.chat_id = ?
                   AND (
                        r.workspace_id = (SELECT active_workspace_id FROM chat_state WHERE chat_id = ?)
                        OR ((SELECT active_workspace_id FROM chat_state WHERE chat_id = ?) IS NULL AND r.workspace_id IS NULL)
                        OR r.run_id IS NULL
                   )
                 ORDER BY j.created_at DESC LIMIT ?",
            )?;
            let jobs = stmt
                .query_map(params![chat_id, chat_id, chat_id, limit as i64], |row| map_job(row, &crypto))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(jobs)
        })
        .await
    }

    pub async fn get_active_jobs_for_workspace(
        &self,
        chat_id: i64,
        workspace_id: &str,
    ) -> Result<Vec<JobRecord>> {
        let workspace_id = workspace_id.to_string();
        let crypto = self.crypto_snapshot();
        self.with_conn(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT j.id, j.chat_id, j.action_type, j.goal, j.state, j.result, j.log_path, j.work_dir, j.created_at, j.updated_at, j.depends_on
                 FROM jobs j
                 JOIN tasks t ON t.job_id = j.id
                 JOIN runs r ON r.run_id = t.run_id
                 WHERE j.chat_id = ?
                   AND j.state IN ('queued','running')
                   AND r.workspace_id = ?
                 ORDER BY j.created_at ASC",
            )?;
            let jobs = stmt
                .query_map(params![chat_id, workspace_id], |row| map_job(row, &crypto))?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(jobs)
        })
        .await
    }

    fn protect_value(&self, input: &str) -> String {
        let redacted = crate::redact::redact_text(input);
        if let Some(crypto) = self.crypto_snapshot() {
            crypto.encrypt_str(&redacted).unwrap_or(redacted)
        } else {
            redacted
        }
    }
}

fn current_schema_version(conn: &Connection) -> Result<i32> {
    let version: i32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    Ok(version)
}

struct ReencryptColumnSpec<'a> {
    table: &'a str,
    id_col: &'a str,
    value_col: &'a str,
    nullable: bool,
}

fn rotate_table_column(
    conn: &Connection,
    spec: ReencryptColumnSpec<'_>,
    old_crypto: &crate::crypto::Crypto,
    new_crypto: &crate::crypto::Crypto,
    stats: &mut ReencryptStats,
) -> Result<()> {
    let select_sql = if spec.nullable {
        format!(
            "SELECT {}, {} FROM {} WHERE {} IS NOT NULL AND {} LIKE 'enc:v1:%'",
            spec.id_col, spec.value_col, spec.table, spec.value_col, spec.value_col
        )
    } else {
        format!(
            "SELECT {}, {} FROM {} WHERE {} LIKE 'enc:v1:%'",
            spec.id_col, spec.value_col, spec.table, spec.value_col
        )
    };
    let update_sql = format!(
        "UPDATE {} SET {} = ? WHERE {} = ?",
        spec.table, spec.value_col, spec.id_col
    );

    let mut stmt = conn.prepare(&select_sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, rusqlite::types::Value>(0)?,
            row.get::<_, String>(1)?,
        ))
    })?;
    let rows = rows.collect::<std::result::Result<Vec<_>, _>>()?;

    let mut update_stmt = conn.prepare(&update_sql)?;
    for (id, raw) in rows {
        match old_crypto.decrypt_str(&raw) {
            Ok(Some(plaintext)) => {
                let next = new_crypto.encrypt_str(&plaintext)?;
                update_stmt.execute(params![next, id])?;
                stats.updated_rows += 1;
            }
            Ok(None) => {
                stats.skipped_rows += 1;
            }
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "Failed to decrypt encrypted value in {}.{}: {}",
                    spec.table,
                    spec.value_col,
                    err
                ));
            }
        }
    }
    Ok(())
}

fn set_schema_version(conn: &Connection, version: i32) -> Result<()> {
    conn.execute_batch(&format!("PRAGMA user_version = {version};"))?;
    Ok(())
}

fn ensure_table_column(conn: &Connection, table: &str, column: &str, ddl: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(());
        }
    }
    let _ = conn.execute(ddl, []);
    Ok(())
}

fn map_job(
    row: &rusqlite::Row<'_>,
    crypto: &Option<std::sync::Arc<crate::crypto::Crypto>>,
) -> rusqlite::Result<JobRecord> {
    let created: String = row.get(8)?;
    let updated: String = row.get(9)?;
    let raw_result: Option<String> = row.get(5)?;
    let result = raw_result.map(|s| unprotect_value(crypto, &s));
    Ok(JobRecord {
        id: row.get(0)?,
        chat_id: row.get(1)?,
        action_type: row.get(2)?,
        goal: row.get(3)?,
        state: JobState::from(row.get::<_, String>(4)?.as_str()),
        result,
        log_path: PathBuf::from(row.get::<_, String>(6)?),
        work_dir: PathBuf::from(row.get::<_, String>(7)?),
        created_at: DateTime::parse_from_rfc3339(&created)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        updated_at: DateTime::parse_from_rfc3339(&updated)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        depends_on: row.get(10).ok(),
    })
}

fn unprotect_value(crypto: &Option<std::sync::Arc<crate::crypto::Crypto>>, raw: &str) -> String {
    if let Some(c) = crypto.as_ref() {
        match c.decrypt_str(raw) {
            Ok(Some(pt)) => return pt,
            Ok(None) => return raw.to_string(),
            Err(_) => return "[encrypted]".to_string(),
        }
    }
    if crate::crypto::Crypto::is_encrypted(raw) {
        return "[encrypted]".to_string();
    }
    raw.to_string()
}

fn active_workspace_id_for_chat(conn: &Connection, chat_id: i64) -> Result<Option<String>> {
    let row: Option<Option<String>> = conn
        .query_row(
            "SELECT active_workspace_id FROM chat_state WHERE chat_id = ?",
            params![chat_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(row.flatten())
}
