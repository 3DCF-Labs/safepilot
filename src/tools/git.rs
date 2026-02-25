use crate::tools::shell;
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

pub async fn clone_repo(
    repo: &str,
    dest: &Path,
    log_path: &Path,
    cancel: &CancellationToken,
    github_token: Option<&crate::secrets::SecretSpec>,
    crypto: Option<&crate::crypto::Crypto>,
) -> Result<String> {
    if dest.join(".git").exists() {
        if let Some(origin) = git_origin_url(dest).await {
            if normalize_repo_url(&origin) == normalize_repo_url(repo) {
                shell::append_log(
                    log_path,
                    &format!(
                        "Repository already present at {} (origin: {})",
                        dest.display(),
                        origin
                    ),
                )
                .await
                .ok();
                return Ok("Repository already present (skipped clone)".into());
            }
        }
        tokio::fs::remove_dir_all(dest).await.ok();
    } else if dest.exists() {
        tokio::fs::remove_dir_all(dest).await.ok();
    }
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let args = vec![
        "clone".to_string(),
        repo.to_string(),
        dest.to_string_lossy().to_string(),
    ];
    let github_https = repo.starts_with("https://github.com/");
    if github_https {
        if let Some(spec) = github_token {
            let token = spec.load_with_crypto(crypto)?;
            let auth = format!(
                "AUTHORIZATION: basic {}",
                base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    format!("x-access-token:{token}")
                )
            );
            let mut env = HashMap::new();
            env.insert("GIT_CONFIG_COUNT".to_string(), "1".to_string());
            env.insert(
                "GIT_CONFIG_KEY_0".to_string(),
                "http.https://github.com/.extraheader".to_string(),
            );
            env.insert("GIT_CONFIG_VALUE_0".to_string(), auth);
            match shell::run_with_env("git", &args, dest.parent(), log_path, cancel, Some(&env))
                .await
            {
                Ok(()) => return Ok("Repository cloned".into()),
                Err(auth_err) => {
                    if dest.exists() {
                        tokio::fs::remove_dir_all(dest).await.ok();
                    }
                    if let Some(parent) = dest.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    match shell::run("git", &args, dest.parent(), log_path, cancel).await {
                        Ok(()) => return Ok("Repository cloned".into()),
                        Err(plain_err) => {
                            let combined = format!("{auth_err}; {plain_err}");
                            if combined.contains("could not read Username for 'https://github.com'")
                                || combined.contains("Authentication failed")
                                || combined.contains("Repository not found")
                            {
                                return Err(anyhow::anyhow!(
                                    "GitHub clone failed. The repository may be private, or the token is missing/invalid. \
Set GITHUB_TOKEN (or GITHUB_TOKEN_READ/GITHUB_TOKEN_WRITE) with access to this repo and retry."
                                ));
                            }
                            return Err(plain_err);
                        }
                    }
                }
            }
        }
    }
    if let Err(err) = shell::run("git", &args, dest.parent(), log_path, cancel).await {
        let msg = err.to_string();
        if github_https && msg.contains("could not read Username for 'https://github.com'") {
            return Err(anyhow::anyhow!(
                "GitHub access token is not configured for HTTPS clone. \
Set GITHUB_TOKEN (or GITHUB_TOKEN_READ/GITHUB_TOKEN_WRITE) and retry."
            ));
        }
        return Err(err);
    }
    Ok("Repository cloned".into())
}

async fn git_origin_url(dest: &Path) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dest)
        .arg("remote")
        .arg("get-url")
        .arg("origin")
        .output()
        .await
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

fn normalize_repo_url(url: &str) -> String {
    let mut s = url.trim().to_string();
    if let Some(rest) = s.strip_prefix("git@github.com:") {
        s = format!("https://github.com/{rest}");
    } else if let Some(rest) = s.strip_prefix("ssh://git@github.com/") {
        s = format!("https://github.com/{rest}");
    }
    if s.ends_with(".git") {
        s.truncate(s.len() - 4);
    }
    while s.ends_with('/') {
        s.pop();
    }
    s
}

fn git_commit_identity() -> (String, String) {
    let name = std::env::var("TG_ORCH_GIT_USER_NAME")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "tg-orch".to_string());
    let email = std::env::var("TG_ORCH_GIT_USER_EMAIL")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "tg-orch@local".to_string());
    (name, email)
}

pub async fn merge_main(
    work_dir: &Path,
    branch: &str,
    log_path: &Path,
    cancel: &CancellationToken,
    github_token: Option<&crate::secrets::SecretSpec>,
    crypto: Option<&crate::crypto::Crypto>,
) -> Result<String> {
    let (commit_name, commit_email) = git_commit_identity();
    let origin = git_origin_url(work_dir).await.unwrap_or_default();
    let github_https = origin.starts_with("https://github.com/");
    let mut github_auth_env = HashMap::new();
    if github_https {
        let spec = github_token.ok_or_else(|| {
            anyhow::anyhow!(
                "GitHub write token is not configured for HTTPS push. \
Set GITHUB_TOKEN_WRITE (or GITHUB_TOKEN) and retry."
            )
        })?;
        let token = spec.load_with_crypto(crypto)?;
        let auth = format!(
            "AUTHORIZATION: basic {}",
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("x-access-token:{token}")
            )
        );
        github_auth_env.insert("GIT_CONFIG_COUNT".to_string(), "1".to_string());
        github_auth_env.insert(
            "GIT_CONFIG_KEY_0".to_string(),
            "http.https://github.com/.extraheader".to_string(),
        );
        github_auth_env.insert("GIT_CONFIG_VALUE_0".to_string(), auth);
    }
    let commands = vec![
        (
            "git",
            vec!["checkout".to_string(), "-b".into(), branch.into()],
        ),
        ("git", vec!["add".into(), "-A".into()]),
        (
            "git",
            vec![
                "-c".into(),
                format!("user.name={commit_name}"),
                "-c".into(),
                format!("user.email={commit_email}"),
                "-c".into(),
                "commit.gpgsign=false".into(),
                "commit".into(),
                "-m".into(),
                format!("Automated task {}", branch),
            ],
        ),
        ("git", vec!["push".into(), "origin".into(), branch.into()]),
        ("git", vec!["checkout".into(), "main".into()]),
        ("git", vec!["merge".into(), "--no-ff".into(), branch.into()]),
        ("git", vec!["push".into(), "origin".into(), "main".into()]),
    ];

    for (program, args) in commands {
        let is_push = args.first().is_some_and(|s| s == "push");
        if github_https && is_push {
            shell::run_dangerous_with_env(
                program,
                &args,
                Some(work_dir),
                log_path,
                cancel,
                Some(&github_auth_env),
            )
            .await?;
        } else {
            shell::run_dangerous(program, &args, Some(work_dir), log_path, cancel).await?;
        }
    }

    Ok("Merge to main completed".into())
}
