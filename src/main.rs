#![allow(clippy::uninlined_format_args)]

mod agent;
mod bot;
mod code;
mod config;
mod context;
mod crypto;
mod db;
mod jobs;
mod llm;
mod orchestrator;
mod planning;
mod policy;
mod redact;
mod safe_error;
mod secrets;
mod security_prompt;
mod tools;
mod utils;

use anyhow::Result;
use orchestrator::Orchestrator;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(|s| s.as_str()) == Some("encrypt") {
        use std::io::Read;

        let spec = secrets::SecretSpec::new(
            "ORCH_MASTER_KEY",
            &["ORCH_MASTER_KEY"],
            &["ORCH_MASTER_KEY_FILE"],
        );
        let key = spec.load()?;
        let crypto = crypto::Crypto::from_key_str(&key)?;
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        let plaintext = buf.trim_end_matches(&['\n', '\r'][..]);
        let enc = crypto.encrypt_str(plaintext)?;
        println!("{enc}");
        return Ok(());
    }

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info,teloxide=warn".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("Starting SafePilot...");

    let config = config::Config::from_env()?;
    let provider_name = if config.llm_provider.is_some() {
        match config.llm_provider {
            Some(config::LlmProviderKind::Anthropic) => "anthropic",
            Some(config::LlmProviderKind::OpenAI) => "openai",
            None => "none",
        }
    } else if config.anthropic_api.is_some() {
        "anthropic"
    } else if config.openai_api.is_some() {
        "openai"
    } else {
        "none"
    };
    tracing::info!(
        llm_mode = ?config.llm_mode,
        llm_provider = provider_name,
        openai_model = %config.openai_model,
        anthropic_model = %config.anthropic_model,
        "LLM configuration loaded"
    );
    tracing::info!("Configuration loaded");

    let db = db::Database::new(&config.sqlite_path(), config.crypto.clone()).await?;
    tracing::info!(path = %config.sqlite_path().display(), "Database ready");

    if let Ok(count) = db
        .fail_orphaned_running_jobs("Orphaned (previous process exited)", None)
        .await
    {
        if count > 0 {
            tracing::warn!(count, "Marked orphaned running jobs as failed");
        }
    }

    let orchestrator = Arc::new(Orchestrator::new(config, db));

    bot::run(orchestrator.clone()).await;
    orchestrator.shutdown().await;

    Ok(())
}
