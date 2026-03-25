#!/usr/bin/env bash
#
# Reset the benchmark database to a clean state.
#
# By default, truncates the application tables while keeping the running
# Postgres container intact.  Pass --full to tear down the container and
# its data volume entirely (a subsequent run-bench.sh will recreate it).
#
# Usage:
#   ./bin/reset-db.sh          # fast: truncate tables
#   ./bin/reset-db.sh --full   # full: destroy container + volume

set -euo pipefail
cd "$(dirname "$0")/.."

export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:54320/union_find}"
PG_HOST=localhost
PG_PORT=54320

MODE=truncate
for arg in "$@"; do
  case "$arg" in
    --full) MODE=full ;;
  esac
done

if [ "$MODE" = full ]; then
  echo "==> Tearing down Docker Compose and removing volume..."
  docker compose down -v
  echo "==> Done. Run bin/run-bench.sh to start fresh."
else
  echo "==> Truncating application tables..."
  PGPASSWORD=postgres psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d union_find \
    -c "TRUNCATE persons, distinct_ids, person_overrides;"
  echo "==> Done. Database is ready for another benchmark run."
fi
