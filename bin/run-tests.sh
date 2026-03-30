#!/usr/bin/env bash
#
# Full-reset test runner: destroy volume, recreate DB, migrate, truncate, test.
#
# Usage:
#   ./bin/run-tests.sh                   # full reset + all tests
#   ./bin/run-tests.sh -- test_name      # full reset + single test

set -euo pipefail
cd "$(dirname "$0")/.."

export DATABASE_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost:54320/union_find}"
PG_HOST=localhost
PG_PORT=54320

# Collect extra args for cargo test (everything after --)
CARGO_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --) shift; CARGO_ARGS=("$@"); break ;;
    *) shift ;;
  esac
done

source bin/ensure-db.sh --fresh

echo "==> Truncating application tables..."
PGPASSWORD=postgres psql -h "$PG_HOST" -p "$PG_PORT" -U postgres -d union_find \
  -c "TRUNCATE person_mapping, distinct_id_mappings, union_find;"

echo "==> Running tests..."
if [ ${#CARGO_ARGS[@]} -gt 0 ]; then
  cargo test "${CARGO_ARGS[@]}"
else
  cargo test
fi
