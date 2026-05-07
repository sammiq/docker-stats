FROM rust:1.95-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

FROM alpine:3.23

COPY --from=builder /app/target/release/docker-stats /usr/local/bin/docker-stats
COPY docker/entrypoint.sh /usr/local/bin/docker-stats-entrypoint
COPY docker/healthcheck.sh /usr/local/bin/docker-stats-healthcheck

RUN chmod +x \
  /usr/local/bin/docker-stats \
  /usr/local/bin/docker-stats-entrypoint \
  /usr/local/bin/docker-stats-healthcheck

ENV BIND_ADDR=0.0.0.0 \
  LISTEN_PORT=9100 \
  RENDER_SECONDS=5

EXPOSE 9100

ENTRYPOINT ["/usr/local/bin/docker-stats-entrypoint"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/usr/local/bin/docker-stats-healthcheck"]
