# syntax=docker/dockerfile:1.7

FROM rust:1.88.0-bookworm AS builder

WORKDIR /app

# Cache deps first.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release

# Build the real binary.
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates git openssh-client && \
    rm -rf /var/lib/apt/lists/*

# Non-root runtime user.
RUN groupadd -g 10001 tg-orch && \
    useradd -m -u 10001 -g 10001 -s /usr/sbin/nologin tg-orch

ENV DATA_DIR=/var/lib/tg-orch \
    LOG_DIR=/var/log/tg-orch \
    RUST_LOG=info,teloxide=warn

RUN mkdir -p /var/lib/tg-orch /var/log/tg-orch && \
    chown -R tg-orch:tg-orch /var/lib/tg-orch /var/log/tg-orch

COPY --from=builder /app/target/release/safepilot /usr/local/bin/safepilot

USER 10001:10001

ENTRYPOINT ["/usr/local/bin/safepilot"]
