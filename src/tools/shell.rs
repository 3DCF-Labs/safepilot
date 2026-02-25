use anyhow::{anyhow, Context, Result};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

static SAFE_PATH: Lazy<String> = Lazy::new(|| {
    std::env::var("TG_ORCH_SAFE_PATH")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin".to_string())
});

pub fn safe_path() -> &'static str {
    SAFE_PATH.as_str()
}

static DANGEROUS_SANDBOX: Lazy<String> = Lazy::new(|| {
    std::env::var("TG_ORCH_DANGEROUS_SANDBOX")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "auto".to_string())
});

static DANGEROUS_SANDBOX_NET: Lazy<String> = Lazy::new(|| {
    std::env::var("TG_ORCH_DANGEROUS_SANDBOX_NET")
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "off".to_string())
});

fn sandbox_mode() -> &'static str {
    DANGEROUS_SANDBOX.as_str()
}

pub fn dangerous_sandbox_unshare_net() -> bool {
    matches!(DANGEROUS_SANDBOX_NET.as_str(), "1" | "true" | "yes" | "on")
}

fn bwrap_available() -> bool {
    let true_bin = ["/bin/true", "/usr/bin/true"]
        .iter()
        .find(|p| std::path::Path::new(*p).exists());
    let Some(true_bin) = true_bin else {
        return false;
    };
    std::process::Command::new("bwrap")
        .args(["--unshare-user", "--", true_bin])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn add_ro_bind_if_exists(cmd: &mut Command, src: &str, dst: &str) {
    if std::path::Path::new(src).exists() {
        cmd.args(["--ro-bind", src, dst]);
    }
}

fn sandboxed_command(
    program: &str,
    args: &[String],
    workspace: &Path,
    unshare_net: bool,
) -> Command {
    let mut cmd = Command::new("bwrap");
    cmd.args(["--ro-bind", "/usr", "/usr"]);
    cmd.args(["--ro-bind", "/bin", "/bin"]);
    add_ro_bind_if_exists(&mut cmd, "/lib", "/lib");
    add_ro_bind_if_exists(&mut cmd, "/lib64", "/lib64");
    add_ro_bind_if_exists(&mut cmd, "/etc/resolv.conf", "/etc/resolv.conf");
    add_ro_bind_if_exists(&mut cmd, "/etc/hosts", "/etc/hosts");
    add_ro_bind_if_exists(&mut cmd, "/etc/nsswitch.conf", "/etc/nsswitch.conf");
    add_ro_bind_if_exists(&mut cmd, "/etc/ssl", "/etc/ssl");
    add_ro_bind_if_exists(&mut cmd, "/etc/pki", "/etc/pki");

    let ws = workspace.to_string_lossy().to_string();
    cmd.args(["--bind", &ws, &ws]);
    cmd.args([
        "--tmpfs",
        "/tmp",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--unshare-user",
        "--unshare-pid",
        "--unshare-uts",
    ]);
    if unshare_net {
        cmd.args(["--unshare-net"]);
    }
    cmd.args(["--die-with-parent", "--", program]);
    cmd.args(args);
    cmd
}

pub async fn append_log(log_path: &Path, line: &str) -> Result<()> {
    if let Some(parent) = log_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let line = crate::redact::redact_text(line);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .await?;
    file.write_all(line.as_bytes()).await?;
    if !line.ends_with('\n') {
        file.write_all(b"\n").await?;
    }
    Ok(())
}

pub async fn run(
    program: &str,
    args: &[String],
    dir: Option<&Path>,
    log_path: &Path,
    cancel: &CancellationToken,
) -> Result<()> {
    run_with_env(program, args, dir, log_path, cancel, None).await
}

pub async fn run_with_env(
    program: &str,
    args: &[String],
    dir: Option<&Path>,
    log_path: &Path,
    cancel: &CancellationToken,
    env_vars: Option<&HashMap<String, String>>,
) -> Result<()> {
    let command_line = format!("$ {} {}", program, args.join(" "));
    append_log(log_path, &command_line).await?;

    let mut cmd = Command::new(program);
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null()); // Never block waiting for interactive input
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.args(args);
    cmd.env_clear();
    cmd.env("PATH", safe_path());
    cmd.env("HOME", dir.unwrap_or_else(|| Path::new("/tmp")));
    cmd.env("LANG", "en_US.UTF-8");
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    cmd.env("CI", "1");
    if let Ok(log_level) = std::env::var("RUST_LOG") {
        if !log_level.trim().is_empty() {
            cmd.env("RUST_LOG", log_level);
        }
    }
    if let Some(dir) = dir {
        cmd.current_dir(dir);
    }
    if let Some(vars) = env_vars {
        for (key, value) in vars {
            cmd.env(key, value);
        }
    }

    let mut child = cmd
        .spawn()
        .with_context(|| format!("Failed to run {}", program))?;

    let child_stdout = child.stdout.take();
    let child_stderr = child.stderr.take();
    let stdout_task = child_stdout.map(|out| {
        tokio::spawn(stream_reader_to_log(
            out,
            log_path.to_path_buf(),
            "stdout".to_string(),
        ))
    });
    let stderr_task = child_stderr.map(|err| {
        tokio::spawn(stream_reader_to_log(
            err,
            log_path.to_path_buf(),
            "stderr".to_string(),
        ))
    });

    let status = tokio::select! {
        result = child.wait() => result?,
        _ = cancel.cancelled() => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(anyhow!("{} cancelled", program));
        }
    };

    let stdout_str = match stdout_task {
        Some(t) => t.await.unwrap_or_default(),
        None => String::new(),
    };
    let stderr_str = match stderr_task {
        Some(t) => t.await.unwrap_or_default(),
        None => String::new(),
    };

    if !status.success() {
        let detail = if stderr_str.trim().is_empty() {
            stdout_str.trim()
        } else {
            stderr_str.trim()
        };
        let cwd = dir
            .map(|d| d.display().to_string())
            .unwrap_or_else(|| "/tmp".to_string());
        if detail.is_empty() {
            Err(anyhow!(
                "{} exited with status {} (no output) in {}",
                program,
                status,
                cwd
            ))
        } else {
            Err(anyhow!(
                "{} exited with status {}: {}",
                program,
                status,
                crate::utils::truncate_str(detail, 1_500)
            ))
        }
    } else {
        Ok(())
    }
}

pub async fn run_dangerous(
    program: &str,
    args: &[String],
    dir: Option<&Path>,
    log_path: &Path,
    cancel: &CancellationToken,
) -> Result<String> {
    run_dangerous_maybe_unshare_net_with_env(program, args, dir, log_path, cancel, false, None)
        .await
}

pub async fn run_dangerous_with_env(
    program: &str,
    args: &[String],
    dir: Option<&Path>,
    log_path: &Path,
    cancel: &CancellationToken,
    env_vars: Option<&HashMap<String, String>>,
) -> Result<String> {
    run_dangerous_maybe_unshare_net_with_env(program, args, dir, log_path, cancel, false, env_vars)
        .await
}

pub async fn run_dangerous_maybe_unshare_net(
    program: &str,
    args: &[String],
    dir: Option<&Path>,
    log_path: &Path,
    cancel: &CancellationToken,
    unshare_net: bool,
) -> Result<String> {
    run_dangerous_maybe_unshare_net_with_env(
        program,
        args,
        dir,
        log_path,
        cancel,
        unshare_net,
        None,
    )
    .await
}

pub async fn run_dangerous_maybe_unshare_net_with_env(
    program: &str,
    args: &[String],
    dir: Option<&Path>,
    log_path: &Path,
    cancel: &CancellationToken,
    unshare_net: bool,
    env_vars: Option<&HashMap<String, String>>,
) -> Result<String> {
    let command_line = format!("$ {} {}", program, args.join(" "));
    append_log(log_path, &command_line).await?;

    let use_bwrap = match sandbox_mode() {
        "off" | "none" | "0" | "false" => false,
        "bwrap" => true,
        _ => bwrap_available(),
    };

    let mut cmd = if use_bwrap {
        let workspace = dir.unwrap_or_else(|| Path::new("/tmp"));
        sandboxed_command(program, args, workspace, unshare_net)
    } else {
        let mut c = Command::new(program);
        c.args(args);
        c
    };
    cmd.kill_on_drop(true);
    cmd.stdin(Stdio::null()); // Never block waiting for interactive input
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    if let Some(dir) = dir {
        cmd.current_dir(dir);
    }
    cmd.env_clear();
    cmd.env("PATH", safe_path());
    cmd.env("HOME", dir.unwrap_or_else(|| Path::new("/tmp")));
    cmd.env("LANG", "en_US.UTF-8");
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    cmd.env("CI", "1");
    if let Ok(log_level) = std::env::var("RUST_LOG") {
        if !log_level.trim().is_empty() {
            cmd.env("RUST_LOG", log_level);
        }
    }
    if let Some(vars) = env_vars {
        for (key, value) in vars {
            cmd.env(key, value);
        }
    }

    let mut child = cmd
        .spawn()
        .with_context(|| format!("Failed to run {}", program))?;

    let child_stdout = child.stdout.take();
    let child_stderr = child.stderr.take();
    let stdout_task = child_stdout.map(|out| {
        tokio::spawn(stream_reader_to_log(
            out,
            log_path.to_path_buf(),
            "stdout".to_string(),
        ))
    });
    let stderr_task = child_stderr.map(|err| {
        tokio::spawn(stream_reader_to_log(
            err,
            log_path.to_path_buf(),
            "stderr".to_string(),
        ))
    });

    let shell_timeout = tokio::time::Duration::from_secs(60);

    let timed_out;
    let cancelled;
    let exit_status;
    tokio::select! {
        result = child.wait() => {
            exit_status = Some(result?);
            timed_out = false;
            cancelled = false;
        }
        _ = cancel.cancelled() => {
            let _ = child.kill().await;
            let _ = child.wait().await; // reap zombie
            exit_status = None;
            timed_out = false;
            cancelled = true;
        }
        _ = tokio::time::sleep(shell_timeout) => {
            let _ = child.kill().await;
            let _ = child.wait().await; // reap zombie
            exit_status = None;
            timed_out = true;
            cancelled = false;
        }
    };

    let stdout_str = if let Some(task) = stdout_task {
        match task.await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("stdout stream task join failed: {}", e);
                String::new()
            }
        }
    } else {
        String::new()
    };
    let stderr_str = if let Some(task) = stderr_task {
        match task.await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("stderr stream task join failed: {}", e);
                String::new()
            }
        }
    } else {
        String::new()
    };

    if cancelled {
        return Err(anyhow!("{} cancelled", program));
    }

    if timed_out {
        let combined = if stdout_str.trim().is_empty() {
            stderr_str.clone()
        } else if stderr_str.trim().is_empty() {
            stdout_str.clone()
        } else {
            format!("{}\n{}", stdout_str, stderr_str)
        };
        let display = crate::utils::truncate_str(combined.trim(), 1_500);
        if display.is_empty() {
            return Ok("Process ran for 60s (no output captured). The script may be working — it was stopped to avoid blocking.".into());
        }
        return Ok(format!("Process output (stopped after 60s):\n{}", display));
    }

    let output_status = exit_status.unwrap();

    if !output_status.success() {
        let detail = if stderr_str.trim().is_empty() {
            stdout_str.trim()
        } else {
            stderr_str.trim()
        };
        if detail.is_empty() {
            Err(anyhow!("{} exited with status {}", program, output_status))
        } else {
            Err(anyhow!(
                "{} exited with status {}: {}",
                program,
                output_status,
                crate::utils::truncate_str(detail, 1_500)
            ))
        }
    } else {
        Ok(stdout_str)
    }
}

async fn stream_reader_to_log<R>(mut reader: R, log_path: PathBuf, stream_name: String) -> String
where
    R: AsyncRead + Unpin,
{
    let mut captured = Vec::new();
    let mut buf = [0u8; 4096];
    let mut pending = String::new();

    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                captured.extend_from_slice(&buf[..n]);
                let chunk = String::from_utf8_lossy(&buf[..n]);
                pending.push_str(&chunk);

                while let Some(pos) = pending.find('\n') {
                    let line = pending[..pos].to_string();
                    if let Err(e) = append_log(&log_path, &line).await {
                        tracing::warn!("failed to append {} log line: {}", stream_name, e);
                    }
                    pending.drain(..=pos);
                }
            }
            Err(e) => {
                tracing::warn!("failed reading {} stream: {}", stream_name, e);
                break;
            }
        }
    }

    if !pending.is_empty() {
        let tail = pending.clone();
        if let Err(e) = append_log(&log_path, &tail).await {
            tracing::warn!("failed to append {} log tail: {}", stream_name, e);
        }
    }

    String::from_utf8_lossy(&captured).to_string()
}
