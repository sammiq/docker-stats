#!/bin/sh
set -eu

LISTEN_PORT="${LISTEN_PORT:-9100}"
HEALTHCHECK_URL="${HEALTHCHECK_URL:-http://127.0.0.1:${LISTEN_PORT}/health}"

wget -q -O /dev/null "$HEALTHCHECK_URL"
