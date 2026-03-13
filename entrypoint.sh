#!/usr/bin/env bash
set -euo pipefail

# ── Clean up stale api file from previous crash ──────────────────────────
rm -f "$IPFS_PATH/api"

# ── Initialise IPFS repo if needed ──────────────────────────────────────
if [ ! -f "$IPFS_PATH/config" ]; then
    echo "Initialising IPFS repo at $IPFS_PATH ..." >&2
    ipfs init --profile=pebbleds
fi

# ── If arguments were passed, run ia-fil directly ───────────────────────
if [ $# -gt 0 ]; then
    exec uv run --project /app ia-fil "$@"
fi

# ── Interactive session: start daemons, then drop into shell ────────────
uv run --project /app ia-fil run-daemons >/dev/null 2>&1 &
DAEMON_PID=$!

cleanup() {
    trap - EXIT INT TERM
    kill -TERM "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo -n "Starting daemons..." >&2
for i in $(seq 1 30); do
    if ipfs --api /ip4/127.0.0.1/tcp/5009 id >/dev/null 2>&1; then
        break
    fi
    echo -n "." >&2
    sleep 1
done
echo " ready." >&2

exec uv run --project /app python /app/shell.py
