# Security Model

This document describes the intended safety properties of SafePilot and the controls currently implemented.

For a system overview, see [`docs/architecture.md`](architecture.md). For deployment hardening, see
[`docs/hardening.md`](hardening.md).

Security docs index: [`docs/security/README.md`](security/README.md). Vulnerability reporting: [`SECURITY.md`](../SECURITY.md).

## Core Idea

We separate:
- **Planning** (LLM proposes a DAG of Tasks)
- **Execution** (the scheduler runs eligible Tasks as Jobs)

The scheduler enforces a **policy layer** with explicit checkpoints, so the LLM cannot directly execute high-risk actions just by proposing them.

## Risk Tiers

Each Task is classified into one of:
- `safe`: read-only or low-impact actions, auto-scheduled when deps are satisfied.
- `needs_approval`: blocked until `/approve <task_id>` or a temporary `/trusted <minutes>` window.
- `dangerous`: blocked until `/approve <task_id>` or a temporary `/unsafe <minutes>` window.

Current defaults:
- `git`, `search`, `fetch`, `weather` are `safe`.
- `codex`, `claude` are `needs_approval`.
- `agent` tasks are `needs_approval` by default.
- `shell`, `validate`, `merge` are `dangerous`.
- Integrations are tiered by goal prefix (read-only safe; write operations need approval).

The mapping lives in [`src/policy.rs`](../src/policy.rs).

## Checkpoints And Bypass Windows

Per run, we store optional time windows:
- `trusted_until`: bypass `needs_approval` checkpoints for the run.
- `unsafe_until`: bypass `dangerous` checkpoints for the run.
- `write_tools_until`: enables agent write-capable tools for the run (only if `AGENT_ENABLE_WRITE_TOOLS=1`).

Telegram commands:
- `/trusted <minutes>`
- `/unsafe <minutes>`
- `/writetools <minutes>`
- `/strict` (clears all windows)

## Workspace Safety

All Jobs in a run share the workspace attached to that run:
- typically `DATA_DIR/chats/<chat_id>/<workspace_name>`

The scheduler prevents concurrent workspace writers by serializing Tasks that have
`WorkspaceAccess::Write` (defined in [`src/policy.rs`](../src/policy.rs)).

For public runtime entrypoints, execution is additionally constrained by channel binding policy
(`write_policy`, `allowed_actions`, and optional fallback workspace) before tasks are scheduled.

## Shell Controls

`shell` and `validate` actions:
- Require a bare binary name (no `/bin/bash` style paths).
- Refuse `bash` and `sh`.
- Enforce an allowlist:
  - `ALLOWED_SHELL_COMMANDS` always allowed.
  - `UNSAFE_SHELL_COMMANDS` only allowed when `/unsafe` is active for the run.

## Subprocess Isolation

All subprocesses run with an environment cleared by default (no inherited API keys/tokens) and a
minimal `PATH` (`TG_ORCH_SAFE_PATH` can override).

For dangerous actions, the runner supports best-effort sandboxing:
- `TG_ORCH_DANGEROUS_SANDBOX=auto|bwrap|off` (default `auto`)
- `TG_ORCH_DANGEROUS_SANDBOX_NET=on|off` (default `off`): when `on`, bubblewrap passes `--unshare-net`
  for `shell`/`validate` to prevent network egress from those commands.

If you deploy via Docker, the container can be your primary isolation boundary; in that case it is
reasonable to set `TG_ORCH_DANGEROUS_SANDBOX=off` inside the container and rely on container-level
controls (read-only root FS, non-root user, restricted mounts, and network policy).

## SSRF Protections

`fetch` only allows `http/https`, and blocks:
- localhost / loopback
- RFC1918 private IPs
- link-local (including `169.254.169.254` metadata)

Override is possible only via:
- `ALLOW_PRIVATE_FETCH=1`

Workspace-level network policy adds a second layer:
- `open`: no domain allowlist restriction
- `trusted_preferred`: prefer configured trusted domains for sourcing
- `trusted_only`: block network requests that are not explicitly within trusted domains

In `trusted_only`, SafePilot enforces trusted-domain checks before planning/execution for public runtime requests.

Redirects:
- When `ALLOW_PRIVATE_FETCH=0`, `fetch` will refuse cross-origin redirects to avoid SSRF bypasses.

## Timeouts

All HTTP clients used for provider APIs and search/fetch are configured with timeouts so an agent loop cannot hang indefinitely.

## Prompt Injection Protection

We treat the following as untrusted data:
- fetched pages, search results
- repo files (including README instructions)
- logs and tool outputs

The system prompts explicitly instruct models to ignore instructions contained in untrusted content and only follow trusted system/operator instructions plus active workspace constraints.

This is defense-in-depth. The primary hard controls are:
- checkpoints (approvals)
- restricted tool surface (no inline shell tool)
- SSRF protections
- allowlists and output truncation

## Secret Handling Model

- Sensitive DB fields are encrypted at rest by default (selected columns, not full DB encryption).
- If no master key is provided, SafePilot auto-generates a local key (`~/.tg-orch/keys/master.key`).
- In production, operators should provide `ORCH_MASTER_KEY_FILE` from a managed secret runtime.
- Workspace secret values are encrypted at rest and can override global integration secrets per workspace.
- Telegram chat input for workspace secrets is reference-only (`env:VAR` or `file:/path`); raw secret values in chat are blocked.
- Encrypted selected columns include messages, summaries, run memories, agent state, job results, workspace skill prompt, and workspace secret values.

### Encryption Limitations

- This is selected-column application-level encryption, not full SQLite file encryption.
- If the master key is lost, encrypted values are not recoverable.
- Plaintext exists in process memory while values are decrypted for runtime use.
- Auto-generated local key is convenient for local use; production should use managed secret/key delivery (`ORCH_MASTER_KEY_FILE`).

## Agent Mode Checkpoints

In `LLM_MODE=agent`, the tool-calling loop runs with a per-run policy wrapper:
- Safe tool calls execute inline.
- Risky tool calls (writes to GitHub/Slack/Jira/etc, outbound Telegram sends, and other privileged operations) are converted into Run Tasks and may require approval.
- A follow-up agent Task is automatically queued to resume after the checkpointed Task completes.

## Known Gaps / Future Work

- A richer policy engine could classify shell subcommands and arguments (not just the binary).
- Browser automation is powerful and risky; even though a `browser` tool exists, it should remain gated behind `/unsafe` and SSRF protections.
- Better provenance tracking for Task origins (user vs tool output) can tighten approvals further.
- Optional provider-backed master-key mode (AWS/GCP/Vault direct integration) is planned.
