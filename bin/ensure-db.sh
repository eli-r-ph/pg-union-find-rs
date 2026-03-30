#!/usr/bin/env bash
#
# Ensure Docker Postgres is running, healthy, and fully migrated.
# Idempotent — safe to call multiple times or source from other scripts.
#
# Usage:
#   ./bin/ensure-db.sh           # ensure up + migrate
#   ./bin/ensure-db.sh --fresh   # destroy volume, recreate, migrate
#   source bin/ensure-db.sh      # use from other scripts

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:54320/union_find}"
PG_HOST=localhost
PG_PORT=54320

FRESH=false
for arg in "$@"; do
  case "$arg" in
    --fresh) FRESH=true ;;
  esac
done

if [ "$FRESH" = true ]; then
  echo "==> Tearing down Docker Compose and removing volume..."
  docker compose down -v 2>/dev/null || true
fi

if ! docker compose ps --status running 2>/dev/null | grep -q postgres; then
  echo "==> Starting Docker Compose..."
  docker compose up -d
fi

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

echo "==> Running migrations..."
cargo build --release --bin migrate 2>&1 | tail -1
./target/release/migrate
echo "==> Database is up to date."
