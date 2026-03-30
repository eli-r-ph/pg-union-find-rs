#!/usr/bin/env bash
#
# Spin up Docker Compose (Postgres), wait for it, migrate, start the server,
# and run benchmarks against it via HTTP.
#
# Usage:
#   ./bin/run-bench.sh                         # defaults
#   BENCH_WARM=1000000 ./bin/run-bench.sh      # override sizes
#   ./bin/run-bench.sh --down                   # tear down after

set -euo pipefail
cd "$(dirname "$0")/.."

export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:54320/union_find}"
PG_HOST=localhost
PG_PORT=54320
SERVER_PORT=3000
SERVER_PID=""

TEARDOWN=false
for arg in "$@"; do
  case "$arg" in
    --down) TEARDOWN=true ;;
  esac
done

cleanup() {
  if [ -n "$SERVER_PID" ]; then
    echo "==> Stopping server (pid $SERVER_PID)..."
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ "$TEARDOWN" = true ]; then
    echo "==> Tearing down Docker Compose..."
    docker compose down -v
  fi
}
trap cleanup EXIT

# ---- 1. Fresh start — tear down any previous run's volume -----------------

echo "==> Cleaning up previous containers and volumes..."
docker compose down -v 2>/dev/null || true

echo "==> Starting Docker Compose..."
docker compose up -d

# ---- 2. Wait for Postgres to accept connections ----------------------------

echo "==> Waiting for Postgres on ${PG_HOST}:${PG_PORT}..."
retries=0
max_retries=30
until pg_isready -h "$PG_HOST" -p "$PG_PORT" -U postgres -q 2>/dev/null; do
  retries=$((retries + 1))
  if [ "$retries" -ge "$max_retries" ]; then
    echo "ERROR: Postgres not ready after ${max_retries}s"
    exit 1
  fi
  sleep 1
done
echo "    Postgres is ready."

# ---- 3. Build release binaries ---------------------------------------------

echo "==> Building release binaries..."
cargo build --release --bin migrate --bin bench --bin pg-union-find-rs

# ---- 4. Run migrations -----------------------------------------------------

echo "==> Running migrations..."
./target/release/migrate

# ---- 5. Start the server ---------------------------------------------------

echo "==> Starting server..."
./target/release/pg-union-find-rs &
SERVER_PID=$!

echo "==> Waiting for server on port ${SERVER_PORT}..."
retries=0
max_retries=15
until curl -sf http://127.0.0.1:${SERVER_PORT}/health >/dev/null 2>&1; do
  retries=$((retries + 1))
  if [ "$retries" -ge "$max_retries" ]; then
    echo "ERROR: Server not ready after ${max_retries}s"
    exit 1
  fi
  sleep 1
done
echo "    Server is ready (pid $SERVER_PID)."

# ---- 6. Run benchmarks -----------------------------------------------------

echo "==> Running benchmarks..."
./target/release/bench

echo "==> Done."
