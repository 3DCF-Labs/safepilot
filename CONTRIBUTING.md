# Contributing

This project is a Telegram-driven orchestrator that turns messages into durable **Runs** (DAG Tasks + approvals) and executes eligible Tasks as **Jobs**.

## Development

Requirements:
- Rust toolchain (pinned via [`rust-toolchain.toml`](rust-toolchain.toml)).

Commands:
```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo audit
```

## Signed Commits

We expect contributions to use signed commits (GPG or SSH signing), so GitHub can verify authorship.

Suggested setup:
```bash
git config commit.gpgsign true
```

If your setup uses SSH commit signing, ensure your GitHub account is configured to trust your
signing key and that `git log --show-signature` shows verified commits locally.

Maintainers may ask you to re-sign commits before merging.

## Security Expectations

This project runs untrusted LLM-driven workflows. Contributions must preserve or improve the
security model. In particular:
- Do not add new execution paths that bypass the task checkpoint model.
- Do not introduce tools that can read or print secrets.
- Any new network-facing feature must have timeouts and bounded output sizes.
- Any URL fetch must keep SSRF protections (private/loopback/link-local/metadata blocked by default).

CI must pass, including `cargo fmt`, `clippy -D warnings`, tests, `cargo audit`, plus security
checks like CodeQL, dependency review, and Trivy (container scan).

## Adding A New Job Action (Run Scheduler -> Jobs)

Job actions are executed by the job executor and are always subject to the run checkpoint model.

Checklist:
1. Implement the action execution in [`src/jobs.rs`](src/jobs.rs) in `execute_action()`.
2. Add validation in [`src/orchestrator.rs`](src/orchestrator.rs) in `validate_action()` so invalid goals fail early.
3. Classify risk and workspace access in [`src/policy.rs`](src/policy.rs):
   - `classify_job_action(action_type, goal) -> RiskTier`
   - `workspace_access(action_type, goal) -> WorkspaceAccess`
4. Update planner instructions in [`src/context.rs`](src/context.rs) if you add a new action type.
5. Update `README.md` (commands, env vars, and behavior).

Security rules for new actions:
- Prefer `RiskTier::Safe` only for genuinely read-only operations.
- Anything that can write to the repo, run commands, or perform writes to external systems should be `NeedsApproval` or `Dangerous`.
- If the action touches the workspace, set `WorkspaceAccess::Write` so the scheduler can serialize writers.

## Adding A New Agent Tool (Tool-Calling Worker)

Agent tools run inside `agent` Tasks (tool-calling workers). They are distinct from job actions.

Implementation steps:
1. Implement a `Tool` in [`src/tools/implementations/`](src/tools/implementations/).
2. Register it where relevant:
   - Job-based agent workers: [`src/jobs.rs`](src/jobs.rs) in `run_agent()` tool registry.
   - Iterative agent loop mode: [`src/orchestrator.rs`](src/orchestrator.rs) in `Orchestrator::init_agent()`.
3. If the tool can write to external systems:
   - Gate writes behind `AGENT_ENABLE_WRITE_TOOLS=1`.
   - Also require a per-run window (`/writetools` or `/unsafe`) before enabling writes.
4. Add timeouts to all network calls, and bound output sizes (truncate logs / results).
5. If the tool fetches URLs, apply SSRF protections (block private/loopback/link-local/metadata targets by default).

## Prompt Injection Safety

Assume all tool outputs and repo contents are untrusted. Do not add tools that interpret arbitrary content as executable instructions.

Hard requirements:
- Never read or print secrets (API keys/tokens). Do not log headers that include tokens.
- Treat fetched pages, repos, and logs as data only.
- Keep tool outputs bounded.

## Testing Expectations

PRs should keep CI green under strict warnings:
- `cargo test`
- `cargo clippy --all-targets -- -D warnings`
- `cargo fmt --check`
- `cargo audit`

If you add a new tool/action:
- Add at least one unit/integration test for parsing/validation and a basic success path.
- Add a negative test for the most likely misuse (for example: SSRF-blocked URL, missing prefix, missing token).
