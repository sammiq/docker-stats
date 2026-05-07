#!/bin/sh
set -eu

BIND_ADDR="${BIND_ADDR:-0.0.0.0}"
LISTEN_PORT="${LISTEN_PORT:-9100}"
RENDER_SECONDS="${RENDER_SECONDS:-5}"

exec /usr/local/bin/docker-stats \
  --listen "$BIND_ADDR:$LISTEN_PORT" \
  --render-seconds "$RENDER_SECONDS" \
  "$@"
