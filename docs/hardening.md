# Hardening Guide

This project runs untrusted LLM-driven workflows. You should assume that a compromised tool call
or dependency can attempt to read secrets or escape the workspace. Defense in depth is required.

The CLI binary name is `safepilot` (you may choose to name your systemd unit `safepilot.service`).

See also:
- [`docs/architecture.md`](architecture.md)
- [`docs/security-model.md`](security-model.md)
- [`docs/docker.md`](docker.md)
- Security docs index: [`docs/security/README.md`](security/README.md)
- Vulnerability reporting: [`SECURITY.md`](../SECURITY.md)

## Service Hardening (systemd)

Example `safepilot.service` settings (adjust paths/users):

```ini
[Service]
User=tg-orch
Group=tg-orch
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/tg-orch /var/log/tg-orch
PrivateDevices=true
RestrictSUIDSGID=true
# NOTE: If you use bubblewrap sandboxing (TG_ORCH_DANGEROUS_SANDBOX=bwrap/auto), you will likely need
# namespaces enabled for this service. If you set RestrictNamespaces=true, bubblewrap may fail.
# Options:
# - set TG_ORCH_DANGEROUS_SANDBOX=off (disables bwrap)
# - or set RestrictNamespaces=false (and rely on bwrap + other hardening instead)
RestrictNamespaces=false
LockPersonality=true
MemoryDenyWriteExecute=true
CapabilityBoundingSet=
SystemCallFilter=@system-service

# Secrets via systemd credentials (preferred)
LoadCredential=bot_token:/etc/tg-orch/secrets/bot_token
LoadCredential=master_key:/etc/tg-orch/secrets/master_key
Environment=BOT_TOKEN_FILE=%d/bot_token
Environment=ORCH_MASTER_KEY_FILE=%d/master_key
```

## Network Egress Controls

Block outbound traffic to private ranges from the bot user (example iptables rules):

```bash
iptables -A OUTPUT -m owner --uid-owner tg-orch -d 10.0.0.0/8 -j DROP
iptables -A OUTPUT -m owner --uid-owner tg-orch -d 172.16.0.0/12 -j DROP
iptables -A OUTPUT -m owner --uid-owner tg-orch -d 192.168.0.0/16 -j DROP
iptables -A OUTPUT -m owner --uid-owner tg-orch -d 169.254.0.0/16 -j DROP
iptables -A OUTPUT -m owner --uid-owner tg-orch -d 127.0.0.0/8 -j DROP
```

## Process Isolation For Dangerous Jobs

If you enable `shell`/`validate` tasks, isolate them (bubblewrap, docker/podman, or a dedicated VM).
This project now supports a best-effort bubblewrap sandbox for dangerous jobs when running on Linux
with `bwrap` installed:

- `TG_ORCH_DANGEROUS_SANDBOX=auto|bwrap|off` (default `auto`)
- `TG_ORCH_DANGEROUS_SANDBOX_NET=on|off` (default `off`): if `on`, pass `--unshare-net` for
  `shell`/`validate` jobs (blocks network egress from sandboxed commands).

Even with bubblewrap enabled, treat this as defense-in-depth; for production you should still apply
OS-level controls (separate user, restricted filesystem, and outbound network policies).

## At-Rest Encryption (Recommended)

SafePilot enables encryption of sensitive DB fields at rest by default.
- If no key is configured, it auto-generates `~/.tg-orch/keys/master.key`.
- In production, set `ORCH_MASTER_KEY_FILE` (or `ORCH_MASTER_KEY`) from your secret runtime.

This does not encrypt the entire SQLite file; it encrypts selected sensitive values stored in it
(messages/summaries/run memories/agent state/job results/workspace skill prompt/workspace secret values), and decrypts them on read.

With a master key configured, you can also store secrets encrypted on disk by writing `enc:v1:...`
values into secret files. SafePilot will decrypt them when loading `*_FILE` secrets.

Key requirements:
- 32 bytes, provided as base64 (recommended) or 64 hex chars
- store the key in a root-only readable file (e.g. systemd credential or Docker secret)

Planned provider mode (in progress):
- optional direct key-provider integration for AWS/GCP/Vault

Limitations reminder:
- this is selected-column encryption, not full SQLite file encryption
- keep key management operationally strong (rotation, backup, restricted access)
