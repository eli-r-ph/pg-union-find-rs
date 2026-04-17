#!/usr/bin/env bash
#
# Run the union-find benchmark against a native, tuned Postgres 17 instance
# on macOS (Homebrew). This script auto-bootstraps everything it needs:
#
#   * Installs postgresql@17 via Homebrew if missing
#   * Initializes a dedicated data directory at BENCH_PGDATA if missing
#   * Writes the tuned postgresql.conf and trust-auth pg_hba.conf
#   * Starts the PG cluster on PG_PORT (default 54321) if not running
#   * Creates the union_find database if missing
#   * Builds the release binaries, migrates, starts the server, runs bench
#
# The Docker setup at port 54320 is untouched; local PG lives on 54321 and
# the two can coexist.
#
# Usage:
#   ./bin/run-bench-local.sh                 # full bench
#   BENCH_WARM=1000000 ./bin/run-bench-local.sh
#   ./bin/run-bench-local.sh --fresh         # nuke BENCH_PGDATA, reinit, run
#   ./bin/run-bench-local.sh --down          # stop PG after the run
#   ./bin/run-bench-local.sh --check         # bootstrap/verify only; no bench
#   ./bin/run-bench-local.sh --stop          # stop PG and exit
#
# Env overrides:
#   BENCH_PGDATA   (default: $HOME/.local/share/pg-bench-17)
#   PG_PORT        (default: 54321)
#   PG_BIN         (default: /opt/homebrew/opt/postgresql@17/bin)
#   BENCH_USER     (default: $(whoami))
#   DATABASE_URL   (default: postgres://$BENCH_USER@localhost:$PG_PORT/union_find)

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Cursor's sandbox sets CARGO_TARGET_DIR to a temp cache, which causes cargo
# to write binaries there while this script runs them from ./target/release/.
# Unsetting it ensures cargo writes to the default ./target/ directory.
unset CARGO_TARGET_DIR

PG_BIN="${PG_BIN:-/opt/homebrew/opt/postgresql@17/bin}"
PG_HOST=localhost
PG_PORT="${PG_PORT:-54321}"
BENCH_PGDATA="${BENCH_PGDATA:-$HOME/.local/share/pg-bench-17}"
BENCH_USER="${BENCH_USER:-$(whoami)}"
export DATABASE_URL="${DATABASE_URL:-postgres://${BENCH_USER}@localhost:${PG_PORT}/union_find}"
SERVER_PORT=3000
SERVER_PID=""

TEARDOWN=false
FRESH=false
CHECK_ONLY=false
STOP_ONLY=false
for arg in "$@"; do
  case "$arg" in
    --down)  TEARDOWN=true ;;
    --fresh) FRESH=true ;;
    --check) CHECK_ONLY=true ;;
    --stop)  STOP_ONLY=true ;;
    *) echo "Unknown arg: $arg" >&2; exit 2 ;;
  esac
done

log() { printf '==> %s\n' "$*"; }
sub() { printf '    %s\n' "$*"; }

# ---- 0. Pick up the pinned PG binaries -------------------------------------

if [ ! -x "$PG_BIN/pg_ctl" ]; then
  log "postgresql@17 not found at $PG_BIN"
  if ! command -v brew >/dev/null 2>&1; then
    echo "ERROR: Homebrew is required to auto-install postgresql@17." >&2
    echo "       Install Homebrew from https://brew.sh or install PG 17 manually" >&2
    echo "       and set PG_BIN to its bin directory." >&2
    exit 1
  fi
  log "Installing postgresql@17 via Homebrew (this may take a minute)..."
  brew install postgresql@17
fi

export PATH="$PG_BIN:$PATH"
# Homebrew PG on macOS requires LC_ALL set before startup, otherwise the
# postmaster trips its "became multithreaded during startup" FATAL.
export LC_ALL="${LC_ALL:-en_US.UTF-8}"
sub "pg_ctl:  $(command -v pg_ctl)"
sub "version: $(pg_config --version)"

# ---- 1. Stop-only fast path ------------------------------------------------

stop_pg() {
  if [ -f "$BENCH_PGDATA/postmaster.pid" ] \
     && pg_isready -h "$PG_HOST" -p "$PG_PORT" -q 2>/dev/null; then
    log "Stopping local PG (data dir $BENCH_PGDATA)..."
    pg_ctl -D "$BENCH_PGDATA" -m fast stop || true
  else
    sub "Local PG is not running."
  fi
}

if [ "$STOP_ONLY" = true ]; then
  stop_pg
  exit 0
fi

# ---- 2. Optional fresh reset (nuke data directory) -------------------------

if [ "$FRESH" = true ] && [ -d "$BENCH_PGDATA" ]; then
  log "Fresh run requested -- stopping PG and wiping $BENCH_PGDATA..."
  pg_ctl -D "$BENCH_PGDATA" -m immediate stop 2>/dev/null || true
  rm -rf "$BENCH_PGDATA"
fi

# ---- 3. Initialize the cluster if missing ----------------------------------

if [ ! -f "$BENCH_PGDATA/PG_VERSION" ]; then
  log "Initializing PG cluster at $BENCH_PGDATA..."
  mkdir -p "$(dirname "$BENCH_PGDATA")"
  initdb --locale=en_US.UTF-8 -E UTF-8 -D "$BENCH_PGDATA" >/dev/null
else
  sub "PG cluster already present at $BENCH_PGDATA"
fi

# ---- 4. Write tuned config (idempotent, marked section) --------------------

CONF="$BENCH_PGDATA/postgresql.conf"
HBA="$BENCH_PGDATA/pg_hba.conf"
CONF_MARKER="# >>> pg-union-find-rs bench config >>>"
CONF_END="# <<< pg-union-find-rs bench config <<<"

write_config() {
  log "Writing tuned postgresql.conf + pg_hba.conf..."

  # Strip any prior managed section so we can cleanly rewrite.
  if grep -qF "$CONF_MARKER" "$CONF"; then
    awk -v start="$CONF_MARKER" -v end="$CONF_END" '
      $0==start {skip=1; next}
      $0==end   {skip=0; next}
      !skip {print}
    ' "$CONF" > "$CONF.tmp" && mv "$CONF.tmp" "$CONF"
  fi

  cat >> "$CONF" <<EOF
$CONF_MARKER
# Tuned for benchmarking on an Apple Silicon workstation (14 cores, 48 GB RAM).
# Durability is OFF -- NEVER run a production workload with this config.

# --- Connection ---
listen_addresses = 'localhost'
port = ${PG_PORT}
max_connections = 200
unix_socket_directories = '/tmp'

# --- Memory ---
shared_buffers = 4GB
effective_cache_size = 16GB
work_mem = 32MB
maintenance_work_mem = 512MB
huge_pages = try

# --- Planner ---
random_page_cost = 1.1
# macOS lacks posix_fadvise(); PG requires effective_io_concurrency = 0 here.
# (On Linux bench hosts, raise this to ~200.)
effective_io_concurrency = 0

# --- WAL (durability OFF) ---
wal_buffers = 64MB
wal_compression = lz4
wal_level = minimal
max_wal_senders = 0
max_wal_size = 2GB
checkpoint_timeout = 15min
checkpoint_completion_target = 0.9
full_page_writes = off
fsync = off
synchronous_commit = off
commit_delay = 200
commit_siblings = 5

# --- Autovacuum OFF for benchmarks ---
autovacuum = off

# --- Logging ---
log_min_messages = warning
log_min_duration_statement = -1
$CONF_END
EOF

  cat > "$HBA" <<'EOF'
# Trust auth for local bench only -- DO NOT use on a shared machine.
local   all   all                 trust
host    all   all   127.0.0.1/32  trust
host    all   all   ::1/128       trust
EOF
}

if ! grep -qF "$CONF_MARKER" "$CONF" 2>/dev/null; then
  write_config
elif [ "$FRESH" = true ]; then
  write_config
else
  sub "postgresql.conf already contains bench config (use --fresh to rewrite)"
fi

# ---- 5. Start PG if not running --------------------------------------------

start_pg() {
  log "Starting PG (log -> $BENCH_PGDATA/server.log)..."
  pg_ctl -D "$BENCH_PGDATA" -l "$BENCH_PGDATA/server.log" -w start >/dev/null
}

if pg_isready -h "$PG_HOST" -p "$PG_PORT" -q 2>/dev/null; then
  sub "PG already accepting connections on $PG_HOST:$PG_PORT"
else
  # Stale pid file recovery: if postmaster.pid exists but pg_isready fails,
  # clean up before starting.
  if [ -f "$BENCH_PGDATA/postmaster.pid" ]; then
    sub "Stale postmaster.pid detected; running pg_ctl stop -m immediate first..."
    pg_ctl -D "$BENCH_PGDATA" -m immediate stop 2>/dev/null || true
  fi
  start_pg
fi

# Wait for readiness (should be instant, but handle slow starts).
retries=0
until pg_isready -h "$PG_HOST" -p "$PG_PORT" -q 2>/dev/null; do
  retries=$((retries + 1))
  if [ "$retries" -ge 30 ]; then
    echo "ERROR: PG not ready after 30s on $PG_HOST:$PG_PORT" >&2
    echo "       Check $BENCH_PGDATA/server.log" >&2
    exit 1
  fi
  sleep 1
done
sub "PG ready on $PG_HOST:$PG_PORT"

# ---- 6. Create the union_find database if missing --------------------------

if psql -h "$PG_HOST" -p "$PG_PORT" -U "$BENCH_USER" -d postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname='union_find'" | grep -q 1; then
  sub "Database 'union_find' already exists"
else
  log "Creating database 'union_find'..."
  createdb -h "$PG_HOST" -p "$PG_PORT" -U "$BENCH_USER" union_find
fi

# ---- 7. Show version / key runtime params for the run log ------------------

log "Runtime parameters:"
psql -h "$PG_HOST" -p "$PG_PORT" -U "$BENCH_USER" -d union_find -c "
SELECT name, setting, unit
FROM pg_settings
WHERE name IN (
  'server_version','shared_buffers','effective_cache_size','work_mem',
  'maintenance_work_mem','wal_buffers','wal_level','fsync',
  'synchronous_commit','full_page_writes','autovacuum','huge_pages',
  'max_connections','commit_delay','commit_siblings','checkpoint_timeout'
)
ORDER BY name;"

if [ "$CHECK_ONLY" = true ]; then
  log "--check: bootstrap complete, skipping benchmark."
  exit 0
fi

# ---- 8. Cleanup trap (stops server; leaves PG up unless --down) ------------

cleanup() {
  if [ -n "$SERVER_PID" ]; then
    log "Stopping bench server (pid $SERVER_PID)..."
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ "$TEARDOWN" = true ]; then
    stop_pg
  fi
}
trap cleanup EXIT

# ---- 9. Build and migrate --------------------------------------------------

log "Building release binaries..."
cargo build --release --bin migrate --bin bench --bin server

log "Running migrations..."
./target/release/migrate

# ---- 10. Start the server --------------------------------------------------

if lsof -iTCP:${SERVER_PORT} -sTCP:LISTEN -n -P 2>/dev/null | tail -n +2 | grep -q .; then
  echo "ERROR: Port ${SERVER_PORT} is already in use. A stale server here would" >&2
  echo "       silently short-circuit the benchmark onto the wrong database." >&2
  echo "       Offending listeners:" >&2
  lsof -iTCP:${SERVER_PORT} -sTCP:LISTEN -n -P >&2
  exit 1
fi

log "Starting server..."
./target/release/server &
SERVER_PID=$!

log "Waiting for server on port ${SERVER_PORT}..."
retries=0
until curl -sf "http://127.0.0.1:${SERVER_PORT}/health" >/dev/null 2>&1; do
  retries=$((retries + 1))
  if [ "$retries" -ge 15 ]; then
    echo "ERROR: Server not ready after 15s" >&2
    exit 1
  fi
  sleep 1
done
sub "Server ready (pid $SERVER_PID)"

# ---- 11. Run the benchmark -------------------------------------------------

log "Running benchmarks..."
./target/release/bench

log "Done."
