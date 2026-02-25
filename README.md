# SafePilot

[![Lint (PR)](https://img.shields.io/github/actions/workflow/status/3DCF-Labs/safepilot/lint.yml?event=pull_request&label=lint%20(PR))](https://github.com/3DCF-Labs/safepilot/actions/workflows/lint.yml)
[![Test (PR)](https://img.shields.io/github/actions/workflow/status/3DCF-Labs/safepilot/ci.yml?event=pull_request&label=test%20(PR))](https://github.com/3DCF-Labs/safepilot/actions/workflows/ci.yml)
[![Audit (PR)](https://img.shields.io/github/actions/workflow/status/3DCF-Labs/safepilot/audit.yml?event=pull_request&label=audit%20(PR))](https://github.com/3DCF-Labs/safepilot/actions/workflows/audit.yml)
[![Docker](https://github.com/3DCF-Labs/safepilot/actions/workflows/docker.yml/badge.svg)](https://github.com/3DCF-Labs/safepilot/actions/workflows/docker.yml)
[![Latest Release](https://img.shields.io/github/v/release/3DCF-Labs/safepilot?display_name=tag)](https://github.com/3DCF-Labs/safepilot/releases/latest)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)


**SafePilot** - a self-hosted AI assistant that executes real work, safely. It turns messages into executable automation runs with SQLite persistence, job execution, 3DCF context compression, and integrations (Slack/GitHub/Notion/Linear/Jira/Todoist/Weather/Brave Search/Telegram/etc).

## Features
- Role-aware Telegram interface with owner/admin/public access. `ALLOWED_USER_ID` bootstraps the owner user, while public channels can be bound to workspace-scoped runtimes.
- Workspace-first UX for create/switch/configure/connect flows (`/ws`, `/wscurrent`, `/wslist`, `/wsnew`, `/wsuse`, `/wsconfig`, `/wspublic`, `/wscaps`).
- Public runtime bindings per integration target (`/bind`, `/unbind`, `/bindings`, `/bindpolicy`) with capability and policy enforcement.
- Inline approval buttons for blocked tasks (callback queries), plus simple natural-language approval for the single blocked task (`"yes"`, `"approve"`, `"go ahead"`, etc).
- Persistent context in SQLite (messages + summaries) with optional 3DCF compression.
  - Recommended: enable [3DCF compression](https://github.com/3DCF-Labs/doc2dataset) in production to reduce prompt size and improve long-run behavior.
- Multiple LLM modes:
  - `LLM_MODE=direct`: uses Anthropic/OpenAI HTTP APIs to produce `{reply, actions}`.
  - `LLM_MODE=agent`: iterative tool-calling loop with per-run checkpoints. Safe tool calls execute inline; risky tool calls are converted into Run Tasks (and may require approval).
- Run scheduler with checkpoints:
  - Each message in `direct` creates (or continues) a **Run** (tasks + deps).
  - Safe tasks are queued as **Jobs** automatically.
  - Risky tasks are blocked until you approve them (or you enable a temporary bypass window).
- Workspace continuity:
  - Each run executes in its assigned workspace path (stored on the run, typically under `DATA_DIR/chats/<chat_id>/<workspace_name>`).
- Safer execution defaults:
  - `shell`/`validate` actions require an allowlisted bare binary name and refuse `bash`/`sh` (supports a separate unsafe allowlist).
  - `fetch` is SSRF-protected by default (blocks private/loopback/link-local/metadata IPs) and pins DNS resolution to validated IPs for the request.
  - Agent-mode write tools are disabled by default.
  - Subprocesses run with a cleared environment (no inherited API keys/tokens), and a minimal `PATH`.

## Architecture

At a high level:
- Telegram chat messages create or continue a durable **Run** (stored in SQLite).
- Each run contains a DAG of **Tasks** (planned actions) with explicit dependencies.
- Eligible tasks execute as **Jobs**. Jobs run in the workspace attached to the run.
- A policy layer classifies tasks into `safe`, `needs_approval`, or `dangerous` and enforces
  checkpoints before execution.

Details: [`docs/architecture.md`](docs/architecture.md).

## Security At A Glance

SafePilot uses checkpointed execution plus defense-in-depth:
- Explicit checkpoints: `/approve`, `/trusted`, `/unsafe`
- Network controls: SSRF protection for `fetch` by default
- Workspace network policy: trusted-domain allowlist mode (`trusted_only`) blocks web access outside configured domains
- Process controls: cleared subprocess env + minimal `PATH` (`TG_ORCH_SAFE_PATH`)
- Optional Linux sandboxing for dangerous jobs (`TG_ORCH_DANGEROUS_SANDBOX*`)

Security docs:
- Model and checkpoint behavior: [`docs/security-model.md`](docs/security-model.md)
- Host hardening: [`docs/hardening.md`](docs/hardening.md)
- Docker hardening/deploy: [`docs/docker.md`](docs/docker.md)
- Security docs index: [`docs/security/README.md`](docs/security/README.md)

## Recommended Security Configuration

Baseline:
- Run in Docker using [`docker-compose.yml`](docker-compose.yml) (non-root, read-only root filesystem, secrets via files, persistent volumes only for `DATA_DIR` and `LOG_DIR`).
- Keep `LLM_MODE=direct` unless you specifically need `LLM_MODE=agent`.
- Keep `AGENT_ENABLE_WRITE_TOOLS=0` and `AGENT_ENABLE_BROWSER_TOOL=0` unless you explicitly want those capabilities.
- Keep `STRICT_SECRET_FILE_PERMS=1` and store secrets in `*_FILE` paths with `chmod 600`.
- Set `ORCH_MASTER_KEY_FILE` to your managed secret path in production.
  - If unset, SafePilot auto-generates `~/.tg-orch/keys/master.key` and enables encryption by default.

If you deploy without Docker, use [`docs/hardening.md`](docs/hardening.md) as a starting point (systemd hardening and egress controls).

### Secret Handling

Use `*_FILE` variables for secrets and keep files at `chmod 600`.

SafePilot enables at-rest encryption for sensitive DB fields by default.
- Preferred production mode: set `ORCH_MASTER_KEY_FILE` from your secret platform.
- Dev fallback: auto-generated key at `~/.tg-orch/keys/master.key`.

Encrypted DB fields include:
- messages, summaries, run memories, agent state, job results
- workspace profile skill prompt
- workspace secret values

When encryption is active, SafePilot also:
- decrypts `enc:v1:...` values when reading secrets from env/files

Encryption limitations:
- this is selected-column encryption, not full SQLite file encryption
- losing the master key makes encrypted values unrecoverable

See [`docs/security-model.md`](docs/security-model.md) for full security and limitation details.

Encrypt a value for secure file-based storage:

```bash
# Reads plaintext from stdin, prints enc:v1:... to stdout.
# ORCH_MASTER_KEY must decode to 32 bytes (base64) or be 64 hex chars.
export ORCH_MASTER_KEY=...   # recommended: store this in a root-only readable file
echo -n "sk-your-openai-key" | safepilot encrypt
```

Generate a master key:

```bash
# base64 (32 bytes)
openssl rand -base64 32

# hex (32 bytes)
openssl rand -hex 32
```

## Requirements
- Rust toolchain.
- Set at least one LLM API key: `ANTHROPIC_API_KEY`/`ANTHROPIC_API_KEY_FILE` or `OPENAI_API_KEY`/`OPENAI_API_KEY_FILE` (required).
- `LLM_MODE` is `direct` by default; set `LLM_MODE=agent` to enable tool-calling agent mode.

## Build
```bash
cargo test
cargo build --release
```

## Quick Start (Local)
```bash
export BOT_TOKEN=123456:abc
export ALLOWED_USER_ID=94000918
# Optional: required only for repo-oriented tasks (git clone / repo reads / code changes)
export DEFAULT_REPO=git@github.com:3DCF-Labs/safepilot.git
export DATA_DIR=$(pwd)/data
export LOG_DIR=$(pwd)/logs

# Choose one
export LLM_MODE=direct
export LLM_PROVIDER=openai
export OPENAI_API_KEY=sk-your-openai-key

# Optional
export BRAVE_API_KEY=...
export GITHUB_TOKEN=... # legacy: used for both read+write; prefer *_READ/*_WRITE split below

cargo run
```

## Quick Start (Docker Compose)

See [`docs/docker.md`](docs/docker.md) and [`docker-compose.yml`](docker-compose.yml).

Notes:
- `DEFAULT_REPO` is optional, but required for repo-oriented tasks. In containers, prefer an HTTPS URL.
- The compose example runs with a read-only root filesystem, non-root user, secrets via files, and persistent volumes for `DATA_DIR` and `LOG_DIR`.
- By default, it disables bubblewrap inside the container (`TG_ORCH_DANGEROUS_SANDBOX=off`) and tightens `ALLOWED_SHELL_COMMANDS` to `git`.

## Telegram Commands
Core and run-control:
- `/help`: concise quick-start help
- `/helpall`: full command list generated from the bot command registry
- `/status`, `/jobs`, `/log <job_id>`, `/cancel <job_id>`
- `/run`, `/planactive`, `/plan <run_id>`, `/use <run_id>`
- `/approve <task_id>`, `/deny <task_id>`, `/trusted <minutes>`, `/unsafe <minutes>`, `/writetools <minutes>`, `/strict`
- `/follow`, `/followrun <run_id>`, `/unfollow`
- `/newrun`, `/newworkspace`, `/reset`
- `/rotatekey` (owner/admin): rotate encryption key and re-encrypt DB sensitive fields

Workspace/public runtime:
- `/ws`, `/wscurrent`, `/wslist`, `/wsnew <name>`, `/wsuse <name>`, `/wsdelete <name>`
- `/wsconfig`, `/wsprofile`, `/wsskill <text>`, `/wspublic`, `/wscaps`, `/capspreset <name>`
- `/bind <integration:channel> <workspace>`, `/unbind <integration:channel>`, `/bindings`, `/bindpolicy ...`
- `/connect <integration> <target_id> <workspace>`, `/connecttelegram ...`, `/connectdiscord ...`, `/connectx ...`
- `/intcheck [integration|all]`, `/audit`, `/auditf ...`, `/auditexport ...`
- `/whereami`, `/about`

## Configuration
Parsed in [`src/config.rs`](src/config.rs).

Core:
- `BOT_TOKEN` or `BOT_TOKEN_FILE` (required)
- `ALLOWED_USER_ID` (required)
- `DEFAULT_REPO` (optional; required for `git` jobs and as a default for GitHub repo tool calls)
- `DATA_DIR` (default `/var/lib/tg-orch`)
- `LOG_DIR` (default `/var/log/tg-orch`)
- `RUST_LOG` (default `info,teloxide=warn`)
- `TELEGRAM_BOT_TOKEN` (optional): token used by the agent `telegram` tool actions; defaults to `BOT_TOKEN`

LLM:
- `LLM_MODE` (default `direct`): `direct` | `agent`
- `LLM_PROVIDER` (optional): `anthropic` | `openai` (auto-detected from keys if unset)
- `ANTHROPIC_API_KEY` or `ANTHROPIC_API_KEY_FILE`
- `OPENAI_API_KEY` or `OPENAI_API_KEY_FILE`
- `ANTHROPIC_MODEL` (default `claude-haiku-4-5-20251001`)
- `OPENAI_MODEL` (default `gpt-4o-mini`)
- `AGENT_MODEL_DEFAULT` (optional): override model used for `agent` tasks (for the selected provider)
- `AGENT_MODEL_RESEARCH` (optional): override model used when `task.agent == "research"`
- `AGENT_MODEL_REVIEW` (optional): override model used when `task.agent == "review"`
- `LLM_MAX_TOKENS` (default `2048`)
- `MAX_LLM_ITERATIONS` (default `10`)
- `LLM_REQUEST_TIMEOUT_SECS` (default `90`)
- `LLM_HTTP_TIMEOUT_SECS` (default `60`)
- `CLAUDE_TIMEOUT_SECS` (default `90`): timeout for summarization calls

Security:
- `ALLOWED_SHELL_COMMANDS` (default: built-in allowlist in `src/config.rs`; override explicitly for your environment)
- `UNSAFE_SHELL_COMMANDS` (default empty): additional binaries permitted only while the run is in `/unsafe`
- `SENSITIVE_PATH_PREFIXES` (default `/run/secrets,/var/run/secrets`): if a `shell`/`validate` command mentions these paths, it is blocked unless the run is in `/unsafe` or the task was explicitly approved
- `TG_ORCH_SAFE_PATH` (optional): override the subprocess `PATH` used for jobs. Default is `/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin` (dev-friendly). Production recommendation: `/usr/bin:/bin` plus absolute paths where feasible.
- `TG_ORCH_DANGEROUS_SANDBOX` (optional, default `auto`): `auto` | `bwrap` | `off`. When enabled and `bwrap` is available (Linux), dangerous jobs run inside a bubblewrap sandbox.
- `TG_ORCH_DANGEROUS_SANDBOX_NET` (optional, default `off`): `on` enables `--unshare-net` for sandboxed `shell`/`validate` jobs.
- `STRICT_SECRET_FILE_PERMS` (default `true`): refuses to read `*_FILE` secrets unless the file mode is `600`. Set `STRICT_SECRET_FILE_PERMS=0` to allow broader perms (not recommended).
- `ALLOW_PRIVATE_FETCH` (default `false`): allow `fetch` to reach private/loopback/link-local/metadata networks
- `AGENT_ENABLE_WRITE_TOOLS` (default `false`): enable agent-mode write tools (still requires `/writetools` or `/unsafe` per run)
- `AGENT_ENABLE_BROWSER_TOOL` (default `false`): enable agent-mode `browser` tool (headless Chromium); only available while the run is in `/unsafe`
- `ORCH_MASTER_KEY` or `ORCH_MASTER_KEY_FILE` (optional override): master key for at-rest encryption. If unset, SafePilot auto-generates `~/.tg-orch/keys/master.key` and uses it.
  - key format: base64 (32 bytes) or hex (64 chars)
  - production recommendation: inject `ORCH_MASTER_KEY_FILE` from Docker/K8s/systemd/Vault secret mounts

Queue and context tuning:
- `MAX_MESSAGES` (default `30`)
- `COMPRESS_THRESHOLD` (default `20`)
- `TOKEN_BUDGET` (default `8192`)
- `THREE_DCF_BUDGET` (default `256`)
- `MAX_CONCURRENT_JOBS` (default `3`)
- `JOB_TIMEOUT_SECS` (default `300`)
- `DEPENDENCY_WAIT_TIMEOUT_SECS` (default `900`)
- `SHUTDOWN_GRACE_SECS` (default `10`)
- `MIN_MESSAGE_INTERVAL_MS` (default `750`)

Integrations:
- `BRAVE_API_KEY` or `BRAVE_API_KEY_FILE`
- Telegram (integration runtime): `TELEGRAM_BOT_TOKEN` or `TELEGRAM_BOT_TOKEN_FILE` (falls back to `BOT_TOKEN`/`BOT_TOKEN_FILE` if unset).
- GitHub: `GITHUB_TOKEN_READ`/`GITHUB_TOKEN_READ_FILE` (read), `GITHUB_TOKEN_WRITE`/`GITHUB_TOKEN_WRITE_FILE` (write). Legacy `GITHUB_TOKEN`/`GITHUB_TOKEN_FILE` applies to both.
- Slack: `SLACK_BOT_TOKEN_READ`/`SLACK_BOT_TOKEN_READ_FILE` (read), `SLACK_BOT_TOKEN_WRITE`/`SLACK_BOT_TOKEN_WRITE_FILE` (write). Legacy `SLACK_BOT_TOKEN`/`SLACK_BOT_TOKEN_FILE` applies to both. `SLACK_BOT_API_TOKEN*` aliases are also supported.
- Notion: `NOTION_API_KEY_READ`/`NOTION_API_KEY_READ_FILE` (read), `NOTION_API_KEY_WRITE`/`NOTION_API_KEY_WRITE_FILE` (write). Legacy `NOTION_API_KEY`/`NOTION_API_KEY_FILE` applies to both. `NOTION_BOT_API_TOKEN*` aliases are also supported.
- Linear: `LINEAR_API_KEY_READ`/`LINEAR_API_KEY_READ_FILE` (read), `LINEAR_API_KEY_WRITE`/`LINEAR_API_KEY_WRITE_FILE` (write). Legacy `LINEAR_API_KEY`/`LINEAR_API_KEY_FILE` applies to both.
- Discord: `DISCORD_BOT_TOKEN_READ`/`DISCORD_BOT_TOKEN_READ_FILE` (read), `DISCORD_BOT_TOKEN_WRITE`/`DISCORD_BOT_TOKEN_WRITE_FILE` (write). Legacy `DISCORD_BOT_TOKEN`/`DISCORD_BOT_TOKEN_FILE` applies to both.
- X: `X_API_BEARER_TOKEN_READ`/`X_API_BEARER_TOKEN_READ_FILE` (read), `X_API_BEARER_TOKEN_WRITE`/`X_API_BEARER_TOKEN_WRITE_FILE` (write). Legacy `X_API_BEARER_TOKEN`/`X_API_BEARER_TOKEN_FILE` applies to both.
- OpenWeather: `OPENWEATHER_API_KEY` or `OPENWEATHER_API_KEY_FILE`
- Todoist: `TODOIST_API_KEY_READ`/`TODOIST_API_KEY_READ_FILE` (read), `TODOIST_API_KEY_WRITE`/`TODOIST_API_KEY_WRITE_FILE` (write). Legacy `TODOIST_API_KEY`/`TODOIST_API_KEY_FILE` applies to both.
- Jira: `JIRA_DOMAIN`, `JIRA_EMAIL`, and `JIRA_API_TOKEN_READ`/`JIRA_API_TOKEN_READ_FILE` (read), `JIRA_API_TOKEN_WRITE`/`JIRA_API_TOKEN_WRITE_FILE` (write). Legacy `JIRA_API_TOKEN`/`JIRA_API_TOKEN_FILE` applies to both.
## Data Layout
- `DATA_DIR/orchestrator.db`: SQLite DB (messages/summaries/jobs plus runs/tasks/deps/approvals). Sensitive fields are encrypted at rest by default (auto key or configured key). On unix, permissions are set to `0600` best-effort.
- `DATA_DIR/chats/<chat_id>/<workspace_name>`: workspace directories used by runs
- `LOG_DIR/<job_id>.log`: per-job logs (best-effort token redaction is applied)

## Notes
- `fetch` is HTTP/HTTPS only and blocks private/loopback/link-local/metadata addresses by default. Override with `ALLOW_PRIVATE_FETCH=1` only if you understand the SSRF risk.
- Agent mode uses runs too: safe tool calls execute inline, but risky tool calls are checkpointed into Tasks so they can be approved and executed as Jobs.
- Write-capable tools are hidden and hard-blocked unless `AGENT_ENABLE_WRITE_TOOLS=1`, and they also require `/writetools` (or `/unsafe`) for the active run.
- The `browser` tool supports simple automation steps (click/type/press/wait) and outputs DOM/screenshot/pdf; it is only exposed during `/unsafe` runs.
- The planner can schedule `agent` tasks (tool-calling workers) by returning `tasks` with `type: \"agent\"` and an `agent` profile like `research` or `review`. Agent tasks run as jobs and can optionally emit follow-up `tasks` JSON which will be appended to the run.
- By default, the agent `telegram` tool can only target the current chat. External chat targets are only permitted while the run is in `/unsafe`.
- For the detailed security model and checkpoints, see [`docs/security-model.md`](docs/security-model.md). For deployment hardening (systemd + egress), see [`docs/hardening.md`](docs/hardening.md).

## Workspace Secrets

- Workspace secrets are stored per workspace and encrypted at rest.
- Telegram secret input accepts references only:
  - `NAME=env:VAR_NAME`
  - `NAME=file:/absolute/path`
- Raw secret values in chat are intentionally blocked.
- Resolution precedence for integrations:
  1. workspace secret override
  2. global env/file secret

## Planned Encryption Provider Mode

Planned optional provider-backed key mode (in progress):
- AWS KMS / Secrets Manager
- GCP KMS / Secret Manager
- HashiCorp Vault

Current recommended pattern is already supported via `ORCH_MASTER_KEY_FILE` mounted from your secret manager runtime.

## License and Security
- License: [`Apache-2.0`](LICENSE)
- Security docs index: [`docs/security/README.md`](docs/security/README.md)

## Contact
- yevhenii [at] 3dcf.dev
