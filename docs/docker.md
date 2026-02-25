# Docker Deployment

This is the recommended way to run SafePilot for most users because it:
- makes dependencies predictable
- lets you run with a read-only root filesystem
- keeps secrets in files (Docker secrets or bind-mounted `600` files)

The CLI binary name is `safepilot` (used in commands below).

See also:
- Security docs index: [`docs/security/README.md`](security/README.md)
- Vulnerability reporting: [`SECURITY.md`](../SECURITY.md)

## Quick Start (Docker Compose)

1) Create secret files (permissions matter):

```bash
mkdir -p secrets
chmod 700 secrets
printf '%s' '123456:abc' > secrets/bot_token
printf '%s' 'sk-...' > secrets/openai_key
printf '%s' 'enc-or-raw-master-key' > secrets/master_key
chmod 600 secrets/*
```

Optional (recommended): set your own master key file.

SafePilot enables encryption at rest by default. If you set `ORCH_MASTER_KEY_FILE`, you control
the key source explicitly (recommended in production) and SafePilot can
decrypt `enc:v1:...` values when loading secrets from `*_FILE` paths. That allows you to store
encrypted secrets on disk.

Example:

```bash
# Generate a master key (32 bytes, base64) and store it with strict perms.
openssl rand -base64 32 > secrets/master_key
chmod 600 secrets/master_key

# Encrypt and store your OpenAI key.
printf '%s' 'sk-...' | docker run --rm -i \
  -e ORCH_MASTER_KEY_FILE=/run/secrets/master_key \
  -v "$PWD/secrets/master_key:/run/secrets/master_key:ro" \
  ghcr.io/3dcf-labs/safepilot:latest \
  safepilot encrypt > secrets/openai_key
chmod 600 secrets/openai_key
```

2) Edit [`docker-compose.yml`](../docker-compose.yml):
- set `ALLOWED_USER_ID`
- set `DEFAULT_REPO` if you want repo-oriented tasks (git clone / repo reads / code changes)
- choose your provider (`OPENAI_API_KEY_FILE` / `ANTHROPIC_API_KEY_FILE`)

3) Start it:

```bash
docker compose up -d
docker compose logs -f
```

## Sandbox Notes

- The container is the main isolation boundary in this setup.
- The compose example sets `TG_ORCH_DANGEROUS_SANDBOX=off` to disable bubblewrap inside the container.
- If you run on Linux and want bubblewrap anyway, set:
  - `TG_ORCH_DANGEROUS_SANDBOX=auto` (or `bwrap`)
  - optionally `TG_ORCH_DANGEROUS_SANDBOX_NET=on` to pass `--unshare-net` for `shell`/`validate`.

## Agent Mode

If you enable `LLM_MODE=agent`, keep these defaults unless you understand the risk tradeoffs:
- `AGENT_ENABLE_WRITE_TOOLS=0`
- `AGENT_ENABLE_BROWSER_TOOL=0`

The bot requires explicit `/approve`, `/trusted`, and `/unsafe` checkpoints before higher-risk work
will run.

See also:
- [`docs/architecture.md`](architecture.md)
- [`docs/security-model.md`](security-model.md)
- [`docs/hardening.md`](hardening.md)
