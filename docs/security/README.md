# Security Docs

This folder is the index for SafePilot security-related documentation.

## Reporting Vulnerabilities
- [`SECURITY.md`](../../SECURITY.md)

## Security Model And Controls
- Security model and checkpoints: [`docs/security-model.md`](../security-model.md)
- Deployment hardening (systemd + egress): [`docs/hardening.md`](../hardening.md)
- Docker deployment (recommended): [`docs/docker.md`](../docker.md)

## Encryption Notes
- At-rest encryption is enabled by default (auto-generated local key if no key is configured).
- Production recommendation: provide `ORCH_MASTER_KEY_FILE` from your secret runtime.
- Planned provider mode: direct AWS/GCP/Vault key-provider integration.
- Limitations are documented in [`docs/security-model.md`](../security-model.md#encryption-limitations).

## Architecture Context
- High-level architecture overview: [`docs/architecture.md`](../architecture.md)
