use crate::llm::types::ToolDefinition;
use crate::tools::registry::Tool;
use crate::utils::truncate_str;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

pub struct RepoTool {
    work_dir: PathBuf,
}

impl RepoTool {
    pub fn new(work_dir: PathBuf) -> Self {
        Self { work_dir }
    }

    async fn run_git(&self, args: &[&str], cancel: &CancellationToken) -> Result<String> {
        if cancel.is_cancelled() {
            return Err(anyhow!("Cancelled"));
        }
        let mut cmd = Command::new("git");
        cmd.args(args);
        cmd.current_dir(&self.work_dir);
        cmd.env_clear();
        cmd.env("PATH", crate::tools::shell::safe_path());
        cmd.env("HOME", "/tmp");
        cmd.env("LANG", "en_US.UTF-8");
        cmd.env("GIT_TERMINAL_PROMPT", "0");
        cmd.env("CI", "1");
        let out = cmd.output().await?;
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        if !out.status.success() {
            return Err(anyhow!(
                "git {} failed: {}",
                args.join(" "),
                truncate_str(&stderr, 2000)
            ));
        }
        Ok(stdout)
    }

    fn safe_rel_path(&self, rel: &str) -> Result<PathBuf> {
        if rel.contains("..") {
            return Err(anyhow!("Refusing path traversal"));
        }
        let p = Path::new(rel);
        if p.is_absolute() {
            return Err(anyhow!("Path must be relative"));
        }
        let joined = self.work_dir.join(p);

        let canonical = joined
            .canonicalize()
            .map_err(|e| anyhow!("Cannot resolve path {}: {e}", joined.display()))?;
        let workspace_canonical = self.work_dir.canonicalize().map_err(|e| {
            anyhow!(
                "Cannot resolve workspace root {}: {e}",
                self.work_dir.display()
            )
        })?;

        if !canonical.starts_with(&workspace_canonical) {
            return Err(anyhow!(
                "Path escapes workspace (symlink?): {} -> {}",
                joined.display(),
                canonical.display()
            ));
        }

        Ok(canonical)
    }
}

#[async_trait]
impl Tool for RepoTool {
    fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            name: "repo".into(),
            description:
                "Read-only repository introspection: status/diff/list/read. Never writes files."
                    .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "op": {
                        "type": "string",
                        "enum": ["status", "diff_stat", "diff", "list", "read"]
                    },
                    "path": { "type": "string", "description": "Relative file path (for read)" },
                    "max_bytes": { "type": "integer", "description": "Optional limit for read output (default 20000)" }
                },
                "required": ["op"]
            }),
        }
    }

    async fn execute(&self, arguments: &Value, cancel: &CancellationToken) -> Result<String> {
        let op = arguments
            .get("op")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Missing op"))?;

        match op {
            "status" => {
                let branch = self
                    .run_git(&["rev-parse", "--abbrev-ref", "HEAD"], cancel)
                    .await
                    .unwrap_or_else(|_| "unknown".into());
                let status = self
                    .run_git(&["status", "--porcelain=v1"], cancel)
                    .await
                    .unwrap_or_else(|e| format!("error: {e}"));
                Ok(format!(
                    "branch: {}\nstatus:\n{}",
                    branch.trim(),
                    truncate_str(&status, 20_000)
                ))
            }
            "diff_stat" => {
                let out = self.run_git(&["diff", "--stat"], cancel).await?;
                Ok(truncate_str(&out, 20_000))
            }
            "diff" => {
                let out = self.run_git(&["diff"], cancel).await?;
                Ok(truncate_str(&out, 20_000))
            }
            "list" => {
                let out = self
                    .run_git(&["ls-files", "-z"], cancel)
                    .await
                    .unwrap_or_default();
                let files = out
                    .split('\0')
                    .filter(|s| !s.trim().is_empty())
                    .take(500)
                    .collect::<Vec<_>>()
                    .join("\n");
                Ok(files)
            }
            "read" => {
                let rel = arguments
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow!("Missing path"))?;
                let max_bytes = arguments
                    .get("max_bytes")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(20_000)
                    .min(200_000) as usize;
                let path = self.safe_rel_path(rel)?;
                let data = tokio::fs::read(&path).await?;
                let text = String::from_utf8_lossy(&data).to_string();
                Ok(truncate_str(&text, max_bytes))
            }
            _ => Err(anyhow!("Unknown op: {op}")),
        }
    }
}
