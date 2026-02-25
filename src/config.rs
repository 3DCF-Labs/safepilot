use crate::secrets::SecretSpec;
use anyhow::{anyhow, Context, Result};
use base64::Engine;
use rand::RngCore;
use std::env;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LlmMode {
    Direct,
    Agent,
}

impl LlmMode {
    fn parse(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "direct" => Ok(LlmMode::Direct),
            "agent" => Ok(LlmMode::Agent),
            other => Err(anyhow!(
                "Invalid LLM_MODE={other}. Expected one of: direct, agent"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LlmProviderKind {
    Anthropic,
    OpenAI,
}

impl LlmProviderKind {
    fn from_env(value: Option<String>) -> Result<Option<Self>> {
        let Some(v) = value else { return Ok(None) };
        match v.to_ascii_lowercase().as_str() {
            "anthropic" => Ok(Some(LlmProviderKind::Anthropic)),
            "openai" => Ok(Some(LlmProviderKind::OpenAI)),
            other => Err(anyhow!(
                "Invalid LLM_PROVIDER={other}. Expected one of: anthropic, openai"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub bot_token: String,
    pub allowed_user_id: i64,
    pub default_repo: Option<String>,
    pub data_dir: PathBuf,
    pub workspace_base_dir: PathBuf,
    pub log_dir: PathBuf,
    pub max_messages: usize,
    pub compress_threshold: usize,
    pub token_budget: usize,
    pub max_concurrent_jobs: usize,
    pub job_timeout_secs: u64,
    pub dependency_wait_timeout_secs: u64,
    pub claude_timeout_secs: u64,
    pub shutdown_grace_secs: u64,
    pub min_message_interval_ms: u64,
    pub three_dcf_budget: usize,
    pub allowed_shell_commands: Vec<String>,
    pub unsafe_shell_commands: Vec<String>,
    pub openai_api: Option<SecretSpec>,
    pub anthropic_api: Option<SecretSpec>,
    pub llm_mode: LlmMode,
    pub llm_provider: Option<LlmProviderKind>,
    pub anthropic_model: String,
    pub openai_model: String,
    pub agent_model_default: Option<String>,
    pub agent_model_research: Option<String>,
    pub agent_model_review: Option<String>,
    pub llm_max_tokens: usize,
    pub max_llm_iterations: usize,
    pub llm_request_timeout_secs: u64,
    pub llm_http_timeout_secs: u64,
    pub allow_private_fetch: bool,
    pub agent_enable_write_tools: bool,
    pub agent_enable_browser_tool: bool,
    pub sensitive_path_prefixes: Vec<String>,
    pub brave_api: Option<SecretSpec>,
    pub slack_token_read: Option<SecretSpec>,
    pub slack_token_write: Option<SecretSpec>,
    pub notion_token_read: Option<SecretSpec>,
    pub notion_token_write: Option<SecretSpec>,
    pub github_token_read: Option<SecretSpec>,
    pub github_token_write: Option<SecretSpec>,
    pub linear_api_read: Option<SecretSpec>,
    pub linear_api_write: Option<SecretSpec>,
    pub discord_token_read: Option<SecretSpec>,
    pub discord_token_write: Option<SecretSpec>,
    pub x_api_token_read: Option<SecretSpec>,
    pub x_api_token_write: Option<SecretSpec>,
    pub telegram_token: String,
    pub openweather_api: Option<SecretSpec>,
    pub todoist_token_read: Option<SecretSpec>,
    pub todoist_token_write: Option<SecretSpec>,
    pub jira_domain: Option<String>,
    pub jira_email: Option<String>,
    pub jira_token_read: Option<SecretSpec>,
    pub jira_token_write: Option<SecretSpec>,
    pub crypto: Option<std::sync::Arc<crate::crypto::Crypto>>,
}

impl Config {
    fn user_local_root_dir() -> PathBuf {
        env::var("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| env::temp_dir())
            .join(".tg-orch")
    }

    fn ensure_default_master_key() -> Result<Option<String>> {
        let explicit = SecretSpec::new(
            "ORCH_MASTER_KEY",
            &["ORCH_MASTER_KEY"],
            &["ORCH_MASTER_KEY_FILE"],
        );
        if explicit.is_configured() {
            return Ok(None);
        }

        let key_dir = Self::user_local_root_dir().join("keys");
        let key_path = key_dir.join("master.key");
        if !key_path.exists() {
            std::fs::create_dir_all(&key_dir)
                .with_context(|| format!("Failed to create key dir {}", key_dir.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&key_dir, std::fs::Permissions::from_mode(0o700));
            }
            let mut key = [0u8; 32];
            rand::rngs::OsRng.fill_bytes(&mut key);
            let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);
            std::fs::write(&key_path, format!("{key_b64}\n"))
                .with_context(|| format!("Failed to write {}", key_path.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600));
            }
            tracing::info!(
                path = %key_path.display(),
                "Generated default ORCH master key"
            );
        }
        Ok(Some(key_path.to_string_lossy().to_string()))
    }

    pub fn from_env() -> Result<Self> {
        if let Some(path) = Self::ensure_default_master_key()? {
            // Keep explicit env precedence; only set fallback when user didn't provide one.
            if env::var("ORCH_MASTER_KEY_FILE").ok().is_none()
                && env::var("ORCH_MASTER_KEY").ok().is_none()
            {
                env::set_var("ORCH_MASTER_KEY_FILE", path);
            }
        }
        let orch_master_key_spec = SecretSpec::new(
            "ORCH_MASTER_KEY",
            &["ORCH_MASTER_KEY"],
            &["ORCH_MASTER_KEY_FILE"],
        );
        let orch_master_key = if orch_master_key_spec.is_configured() {
            Some(orch_master_key_spec.load()?)
        } else {
            None
        };
        let crypto = orch_master_key
            .as_deref()
            .and_then(|s| match crate::crypto::Crypto::from_key_str(s) {
                Ok(c) => Some(Arc::new(c)),
                Err(err) => {
                    tracing::warn!(error = %err, "Invalid ORCH_MASTER_KEY; encryption/decrypt disabled");
                    None
                }
            });

        let bot_token_spec = SecretSpec::new("BOT_TOKEN", &["BOT_TOKEN"], &["BOT_TOKEN_FILE"]);
        let bot_token = bot_token_spec.load_with_crypto(crypto.as_deref())?;
        let allowed_user_id: i64 = get_env("ALLOWED_USER_ID")?
            .parse()
            .context("ALLOWED_USER_ID must be an integer")?;
        let default_repo = env::var("DEFAULT_REPO")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let data_dir_from_env = env::var("DATA_DIR").ok().map(PathBuf::from);
        let log_dir_from_env = env::var("LOG_DIR").ok().map(PathBuf::from);

        let mut data_dir = data_dir_from_env
            .clone()
            .unwrap_or_else(|| PathBuf::from("/var/lib/tg-orch"));
        let mut log_dir = log_dir_from_env
            .clone()
            .unwrap_or_else(|| PathBuf::from("/var/log/tg-orch"));

        if data_dir_from_env.is_none() {
            if let Err(err) = std::fs::create_dir_all(&data_dir) {
                if err.kind() == ErrorKind::PermissionDenied {
                    let fallback = Self::user_local_root_dir().join("data");
                    tracing::warn!(
                        original = %data_dir.display(),
                        fallback = %fallback.display(),
                        "DATA_DIR default not writable, using fallback"
                    );
                    data_dir = fallback;
                } else {
                    return Err(err).with_context(|| {
                        format!("Failed to create data dir {}", data_dir.display())
                    });
                }
            }
        }
        std::fs::create_dir_all(&data_dir)
            .with_context(|| format!("Failed to create data dir {}", data_dir.display()))?;

        if log_dir_from_env.is_none() {
            if let Err(err) = std::fs::create_dir_all(&log_dir) {
                if err.kind() == ErrorKind::PermissionDenied {
                    let fallback = Self::user_local_root_dir().join("logs");
                    tracing::warn!(
                        original = %log_dir.display(),
                        fallback = %fallback.display(),
                        "LOG_DIR default not writable, using fallback"
                    );
                    log_dir = fallback;
                } else {
                    return Err(err).with_context(|| {
                        format!("Failed to create log dir {}", log_dir.display())
                    });
                }
            }
        }
        std::fs::create_dir_all(&log_dir)
            .with_context(|| format!("Failed to create log dir {}", log_dir.display()))?;

        let workspace_base_dir = data_dir.join("chats");
        std::fs::create_dir_all(data_dir.join("jobs"))?;
        std::fs::create_dir_all(data_dir.join("workspace"))?;
        std::fs::create_dir_all(&workspace_base_dir)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700));
            let _ = std::fs::set_permissions(&log_dir, std::fs::Permissions::from_mode(0o700));
        }

        let allowed_shell_commands = env::var("ALLOWED_SHELL_COMMANDS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .filter_map(|cmd| {
                        let trimmed = cmd.trim();
                        if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed.to_string())
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|list| !list.is_empty())
            .unwrap_or_else(|| {
                vec![
                    "pwd".into(),
                    "echo".into(),
                    "env".into(),
                    "printenv".into(),
                    "which".into(),
                    "whoami".into(),
                    "date".into(),
                    "uname".into(),
                    "ls".into(),
                    "cat".into(),
                    "head".into(),
                    "tail".into(),
                    "find".into(),
                    "tree".into(),
                    "wc".into(),
                    "file".into(),
                    "stat".into(),
                    "du".into(),
                    "df".into(),
                    "mkdir".into(),
                    "cp".into(),
                    "mv".into(),
                    "touch".into(),
                    "chmod".into(),
                    "ln".into(),
                    "grep".into(),
                    "rg".into(),
                    "sed".into(),
                    "awk".into(),
                    "sort".into(),
                    "uniq".into(),
                    "diff".into(),
                    "xargs".into(),
                    "tar".into(),
                    "zip".into(),
                    "unzip".into(),
                    "gzip".into(),
                    "curl".into(),
                    "wget".into(),
                    "git".into(),
                    "cargo".into(),
                    "rustc".into(),
                    "go".into(),
                    "python3".into(),
                    "python".into(),
                    "pip3".into(),
                    "pip".into(),
                    "uv".into(),
                    "poetry".into(),
                    "pytest".into(),
                    "ruff".into(),
                    "mypy".into(),
                    "npm".into(),
                    "pnpm".into(),
                    "yarn".into(),
                    "node".into(),
                    "npx".into(),
                    "bun".into(),
                    "deno".into(),
                    "java".into(),
                    "javac".into(),
                    "mvn".into(),
                    "gradle".into(),
                    "dotnet".into(),
                    "php".into(),
                    "composer".into(),
                    "ruby".into(),
                    "bundle".into(),
                    "rake".into(),
                    "make".into(),
                    "cmake".into(),
                    "ctest".into(),
                    "ninja".into(),
                    "just".into(),
                    "jest".into(),
                    "docker".into(),
                    "docker-compose".into(),
                ]
            });

        let unsafe_shell_commands = env::var("UNSAFE_SHELL_COMMANDS")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .filter_map(|cmd| {
                        let trimmed = cmd.trim();
                        if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed.to_string())
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|list| !list.is_empty())
            .unwrap_or_default();

        let mut sensitive_path_prefixes = env::var("SENSITIVE_PATH_PREFIXES")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .filter_map(|p| {
                        let trimmed = p.trim();
                        if trimmed.is_empty() {
                            None
                        } else {
                            Some(trimmed.to_string())
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| vec!["/run/secrets".into(), "/var/run/secrets".into()]);

        fn push_sensitive(prefixes: &mut Vec<String>, raw: &str) {
            let p = raw.trim();
            if p.is_empty() {
                return;
            }
            prefixes.push(p.to_string());
            let path = Path::new(p);
            if let Some(parent) = path.parent() {
                let s = parent.to_string_lossy().to_string();
                if !s.is_empty() {
                    prefixes.push(s);
                }
            }
        }
        const FILE_ENVS: &[&str] = &[
            "BOT_TOKEN_FILE",
            "ORCH_MASTER_KEY_FILE",
            "OPENAI_API_KEY_FILE",
            "ANTHROPIC_API_KEY_FILE",
            "BRAVE_API_KEY_FILE",
            "OPENWEATHER_API_KEY_FILE",
            "GITHUB_TOKEN_FILE",
            "GITHUB_TOKEN_READ_FILE",
            "GITHUB_TOKEN_WRITE_FILE",
            "SLACK_BOT_TOKEN_FILE",
            "SLACK_BOT_TOKEN_READ_FILE",
            "SLACK_BOT_TOKEN_WRITE_FILE",
            "SLACK_BOT_API_TOKEN_FILE",
            "SLACK_BOT_API_TOKEN_READ_FILE",
            "SLACK_BOT_API_TOKEN_WRITE_FILE",
            "NOTION_API_KEY_FILE",
            "NOTION_API_KEY_READ_FILE",
            "NOTION_API_KEY_WRITE_FILE",
            "NOTION_BOT_API_TOKEN_FILE",
            "NOTION_BOT_API_TOKEN_READ_FILE",
            "NOTION_BOT_API_TOKEN_WRITE_FILE",
            "LINEAR_API_KEY_FILE",
            "LINEAR_API_KEY_READ_FILE",
            "LINEAR_API_KEY_WRITE_FILE",
            "DISCORD_BOT_TOKEN_FILE",
            "DISCORD_BOT_TOKEN_READ_FILE",
            "DISCORD_BOT_TOKEN_WRITE_FILE",
            "X_API_BEARER_TOKEN_FILE",
            "X_API_BEARER_TOKEN_READ_FILE",
            "X_API_BEARER_TOKEN_WRITE_FILE",
            "TODOIST_API_KEY_FILE",
            "TODOIST_API_KEY_READ_FILE",
            "TODOIST_API_KEY_WRITE_FILE",
            "JIRA_API_TOKEN_FILE",
            "JIRA_API_TOKEN_READ_FILE",
            "JIRA_API_TOKEN_WRITE_FILE",
        ];
        for k in FILE_ENVS {
            if let Ok(v) = env::var(k) {
                push_sensitive(&mut sensitive_path_prefixes, &v);
            }
        }
        sensitive_path_prefixes.sort();
        sensitive_path_prefixes.dedup();

        let telegram_token = env::var("TELEGRAM_BOT_TOKEN").unwrap_or_else(|_| bot_token.clone());

        let openai_api = SecretSpec::new(
            "OPENAI_API_KEY",
            &["OPENAI_API_KEY"],
            &["OPENAI_API_KEY_FILE"],
        );
        let anthropic_api = SecretSpec::new(
            "ANTHROPIC_API_KEY",
            &["ANTHROPIC_API_KEY"],
            &["ANTHROPIC_API_KEY_FILE"],
        );
        let openai_api = openai_api.is_configured().then_some(openai_api);
        let anthropic_api = anthropic_api.is_configured().then_some(anthropic_api);

        if anthropic_api.is_none() && openai_api.is_none() {
            return Err(anyhow!(
                "Missing LLM API key. Set ANTHROPIC_API_KEY/OPENAI_API_KEY (or *_FILE variants)."
            ));
        }

        let llm_mode = match env::var("LLM_MODE").ok() {
            Some(v) => LlmMode::parse(&v)?,
            None => LlmMode::Direct,
        };
        let llm_provider_pref = LlmProviderKind::from_env(env::var("LLM_PROVIDER").ok())?;
        let anthropic_model =
            env::var("ANTHROPIC_MODEL").unwrap_or_else(|_| "claude-haiku-4-5-20251001".into());
        let openai_model = env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4o-mini".into());
        let agent_model_default = env::var("AGENT_MODEL_DEFAULT")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let agent_model_research = env::var("AGENT_MODEL_RESEARCH")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let agent_model_review = env::var("AGENT_MODEL_REVIEW")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let llm_provider = llm_provider_pref.or_else(|| {
            if anthropic_api.is_some() {
                Some(LlmProviderKind::Anthropic)
            } else if openai_api.is_some() {
                Some(LlmProviderKind::OpenAI)
            } else {
                None
            }
        });

        let provider = llm_provider.ok_or_else(|| {
            anyhow!("LLM_MODE requires an API key. Set ANTHROPIC_API_KEY or OPENAI_API_KEY.")
        })?;
        match provider {
            LlmProviderKind::Anthropic => {
                if anthropic_api.is_none() {
                    return Err(anyhow!(
                        "LLM_PROVIDER=anthropic requires ANTHROPIC_API_KEY to be set"
                    ));
                }
            }
            LlmProviderKind::OpenAI => {
                if openai_api.is_none() {
                    return Err(anyhow!(
                        "LLM_PROVIDER=openai requires OPENAI_API_KEY to be set"
                    ));
                }
            }
        }

        let brave_api =
            SecretSpec::new("BRAVE_API_KEY", &["BRAVE_API_KEY"], &["BRAVE_API_KEY_FILE"])
                .is_configured()
                .then_some(SecretSpec::new(
                    "BRAVE_API_KEY",
                    &["BRAVE_API_KEY"],
                    &["BRAVE_API_KEY_FILE"],
                ));

        let slack_token_read = SecretSpec::new(
            "SLACK_BOT_TOKEN (read)",
            &[
                "SLACK_BOT_TOKEN_READ",
                "SLACK_BOT_API_TOKEN_READ",
                "SLACK_BOT_TOKEN",
                "SLACK_BOT_API_TOKEN",
            ],
            &[
                "SLACK_BOT_TOKEN_READ_FILE",
                "SLACK_BOT_API_TOKEN_READ_FILE",
                "SLACK_BOT_TOKEN_FILE",
                "SLACK_BOT_API_TOKEN_FILE",
            ],
        );
        let slack_token_write = SecretSpec::new(
            "SLACK_BOT_TOKEN (write)",
            &[
                "SLACK_BOT_TOKEN_WRITE",
                "SLACK_BOT_API_TOKEN_WRITE",
                "SLACK_BOT_TOKEN",
                "SLACK_BOT_API_TOKEN",
            ],
            &[
                "SLACK_BOT_TOKEN_WRITE_FILE",
                "SLACK_BOT_API_TOKEN_WRITE_FILE",
                "SLACK_BOT_TOKEN_FILE",
                "SLACK_BOT_API_TOKEN_FILE",
            ],
        );
        let slack_token_read = slack_token_read.is_configured().then_some(slack_token_read);
        let slack_token_write = slack_token_write
            .is_configured()
            .then_some(slack_token_write);

        let notion_token_read = SecretSpec::new(
            "NOTION_API_KEY (read)",
            &[
                "NOTION_API_KEY_READ",
                "NOTION_BOT_API_TOKEN_READ",
                "NOTION_API_KEY",
                "NOTION_BOT_API_TOKEN",
            ],
            &[
                "NOTION_API_KEY_READ_FILE",
                "NOTION_BOT_API_TOKEN_READ_FILE",
                "NOTION_API_KEY_FILE",
                "NOTION_BOT_API_TOKEN_FILE",
            ],
        );
        let notion_token_write = SecretSpec::new(
            "NOTION_API_KEY (write)",
            &[
                "NOTION_API_KEY_WRITE",
                "NOTION_BOT_API_TOKEN_WRITE",
                "NOTION_API_KEY",
                "NOTION_BOT_API_TOKEN",
            ],
            &[
                "NOTION_API_KEY_WRITE_FILE",
                "NOTION_BOT_API_TOKEN_WRITE_FILE",
                "NOTION_API_KEY_FILE",
                "NOTION_BOT_API_TOKEN_FILE",
            ],
        );
        let notion_token_read = notion_token_read
            .is_configured()
            .then_some(notion_token_read);
        let notion_token_write = notion_token_write
            .is_configured()
            .then_some(notion_token_write);

        let github_token_read = SecretSpec::new(
            "GITHUB_TOKEN (read)",
            &["GITHUB_TOKEN_READ", "GITHUB_TOKEN"],
            &["GITHUB_TOKEN_READ_FILE", "GITHUB_TOKEN_FILE"],
        );
        let github_token_write = SecretSpec::new(
            "GITHUB_TOKEN (write)",
            &["GITHUB_TOKEN_WRITE", "GITHUB_TOKEN"],
            &["GITHUB_TOKEN_WRITE_FILE", "GITHUB_TOKEN_FILE"],
        );
        let github_token_read = github_token_read
            .is_configured()
            .then_some(github_token_read);
        let github_token_write = github_token_write
            .is_configured()
            .then_some(github_token_write);

        let linear_api_read = SecretSpec::new(
            "LINEAR_API_KEY (read)",
            &["LINEAR_API_KEY_READ", "LINEAR_API_KEY"],
            &["LINEAR_API_KEY_READ_FILE", "LINEAR_API_KEY_FILE"],
        );
        let linear_api_write = SecretSpec::new(
            "LINEAR_API_KEY (write)",
            &["LINEAR_API_KEY_WRITE", "LINEAR_API_KEY"],
            &["LINEAR_API_KEY_WRITE_FILE", "LINEAR_API_KEY_FILE"],
        );
        let linear_api_read = linear_api_read.is_configured().then_some(linear_api_read);
        let linear_api_write = linear_api_write.is_configured().then_some(linear_api_write);

        let discord_token_read = SecretSpec::new(
            "DISCORD_BOT_TOKEN (read)",
            &["DISCORD_BOT_TOKEN_READ", "DISCORD_BOT_TOKEN"],
            &["DISCORD_BOT_TOKEN_READ_FILE", "DISCORD_BOT_TOKEN_FILE"],
        );
        let discord_token_write = SecretSpec::new(
            "DISCORD_BOT_TOKEN (write)",
            &["DISCORD_BOT_TOKEN_WRITE", "DISCORD_BOT_TOKEN"],
            &["DISCORD_BOT_TOKEN_WRITE_FILE", "DISCORD_BOT_TOKEN_FILE"],
        );
        let discord_token_read = discord_token_read
            .is_configured()
            .then_some(discord_token_read);
        let discord_token_write = discord_token_write
            .is_configured()
            .then_some(discord_token_write);

        let x_api_token_read = SecretSpec::new(
            "X_API_BEARER_TOKEN (read)",
            &["X_API_BEARER_TOKEN_READ", "X_API_BEARER_TOKEN"],
            &["X_API_BEARER_TOKEN_READ_FILE", "X_API_BEARER_TOKEN_FILE"],
        );
        let x_api_token_write = SecretSpec::new(
            "X_API_BEARER_TOKEN (write)",
            &["X_API_BEARER_TOKEN_WRITE", "X_API_BEARER_TOKEN"],
            &["X_API_BEARER_TOKEN_WRITE_FILE", "X_API_BEARER_TOKEN_FILE"],
        );
        let x_api_token_read = x_api_token_read.is_configured().then_some(x_api_token_read);
        let x_api_token_write = x_api_token_write
            .is_configured()
            .then_some(x_api_token_write);

        let openweather_api = SecretSpec::new(
            "OPENWEATHER_API_KEY",
            &["OPENWEATHER_API_KEY"],
            &["OPENWEATHER_API_KEY_FILE"],
        );
        let openweather_api = openweather_api.is_configured().then_some(openweather_api);

        let todoist_token_read = SecretSpec::new(
            "TODOIST_API_KEY (read)",
            &["TODOIST_API_KEY_READ", "TODOIST_API_KEY"],
            &["TODOIST_API_KEY_READ_FILE", "TODOIST_API_KEY_FILE"],
        );
        let todoist_token_write = SecretSpec::new(
            "TODOIST_API_KEY (write)",
            &["TODOIST_API_KEY_WRITE", "TODOIST_API_KEY"],
            &["TODOIST_API_KEY_WRITE_FILE", "TODOIST_API_KEY_FILE"],
        );
        let todoist_token_read = todoist_token_read
            .is_configured()
            .then_some(todoist_token_read);
        let todoist_token_write = todoist_token_write
            .is_configured()
            .then_some(todoist_token_write);

        let jira_token_read = SecretSpec::new(
            "JIRA_API_TOKEN (read)",
            &["JIRA_API_TOKEN_READ", "JIRA_API_TOKEN"],
            &["JIRA_API_TOKEN_READ_FILE", "JIRA_API_TOKEN_FILE"],
        );
        let jira_token_write = SecretSpec::new(
            "JIRA_API_TOKEN (write)",
            &["JIRA_API_TOKEN_WRITE", "JIRA_API_TOKEN"],
            &["JIRA_API_TOKEN_WRITE_FILE", "JIRA_API_TOKEN_FILE"],
        );
        let jira_token_read = jira_token_read.is_configured().then_some(jira_token_read);
        let jira_token_write = jira_token_write.is_configured().then_some(jira_token_write);

        Ok(Self {
            bot_token,
            allowed_user_id,
            default_repo,
            data_dir,
            workspace_base_dir,
            log_dir,
            max_messages: env_num("MAX_MESSAGES", 30)?,
            compress_threshold: env_num("COMPRESS_THRESHOLD", 20)?,
            token_budget: env_num("TOKEN_BUDGET", 8192)?,
            max_concurrent_jobs: env_num("MAX_CONCURRENT_JOBS", 3)?,
            job_timeout_secs: env_num("JOB_TIMEOUT_SECS", 300)?,
            dependency_wait_timeout_secs: env_num("DEPENDENCY_WAIT_TIMEOUT_SECS", 900)?,
            claude_timeout_secs: env_num("CLAUDE_TIMEOUT_SECS", 90)?,
            shutdown_grace_secs: env_num("SHUTDOWN_GRACE_SECS", 10)?,
            min_message_interval_ms: env_num("MIN_MESSAGE_INTERVAL_MS", 750)?,
            three_dcf_budget: env_num("THREE_DCF_BUDGET", 256)?,
            allowed_shell_commands,
            unsafe_shell_commands,
            openai_api,
            anthropic_api,
            llm_mode,
            llm_provider,
            anthropic_model,
            openai_model,
            agent_model_default,
            agent_model_research,
            agent_model_review,
            llm_max_tokens: env_num("LLM_MAX_TOKENS", 2048)?,
            max_llm_iterations: env_num("MAX_LLM_ITERATIONS", 10)?,
            llm_request_timeout_secs: env_num("LLM_REQUEST_TIMEOUT_SECS", 90)?,
            llm_http_timeout_secs: env_num("LLM_HTTP_TIMEOUT_SECS", 60)?,
            allow_private_fetch: env_bool("ALLOW_PRIVATE_FETCH", false),
            agent_enable_write_tools: env_bool("AGENT_ENABLE_WRITE_TOOLS", false),
            agent_enable_browser_tool: env_bool("AGENT_ENABLE_BROWSER_TOOL", false),
            sensitive_path_prefixes,
            brave_api,
            slack_token_read,
            slack_token_write,
            notion_token_read,
            notion_token_write,
            github_token_read,
            github_token_write,
            linear_api_read,
            linear_api_write,
            discord_token_read,
            discord_token_write,
            x_api_token_read,
            x_api_token_write,
            telegram_token,
            openweather_api,
            todoist_token_read,
            todoist_token_write,
            jira_domain: env::var("JIRA_DOMAIN").ok(),
            jira_email: env::var("JIRA_EMAIL").ok(),
            jira_token_read,
            jira_token_write,
            crypto,
        })
    }

    pub fn sqlite_path(&self) -> PathBuf {
        self.data_dir.join("orchestrator.db")
    }
}

fn get_env(key: &str) -> Result<String> {
    env::var(key).map_err(|_| anyhow!("Missing required env var {}", key))
}

fn env_num<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr + Copy,
    T::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(value) => value
            .parse()
            .map_err(|e| anyhow!("{} must parse: {}", key, e)),
        Err(_) => Ok(default),
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(value) => {
            let v = value.trim().to_ascii_lowercase();
            matches!(v.as_str(), "1" | "true" | "yes" | "on")
        }
        Err(_) => default,
    }
}
