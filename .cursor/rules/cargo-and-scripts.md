---
description: Building, running, and benchmarking the project
globs: ["src/**", "bin/**", "Cargo.toml"]
---

# Cargo builds and bin/ scripts

## CARGO_TARGET_DIR sandbox issue

Cursor's sandbox sets `CARGO_TARGET_DIR` to a temp cache directory. This causes
`cargo build` to write binaries there instead of the default `./target/` directory.
The `bin/` scripts run binaries from `./target/release/`, so a stale binary will be
executed if the env var is not cleared.

**When running cargo commands directly**, always `unset CARGO_TARGET_DIR` first, or
pass `required_permissions: ["all"]` AND explicitly unset the var:

```bash
unset CARGO_TARGET_DIR && cargo build --release --bin server --bin bench --bin migrate
```

**When running bin/ scripts**, the scripts already unset `CARGO_TARGET_DIR` internally.
Always prefer using the scripts over manual commands:

- `./bin/run-bench.sh` — full benchmark lifecycle (Docker reset, build, migrate, server, bench)
- `./bin/run-tests.sh` — full test lifecycle (DB reset, truncate, cargo test)
- `./bin/ensure-db.sh` — idempotent DB setup (Docker + migrate)
- `./bin/reset-db.sh` — truncate tables or full Docker teardown

## Building

All binaries (server, bench, migrate) should be built as `--release` for benchmarking.
The bench binary is an HTTP load-test harness (not a Rust `#[bench]` test) — `--release`
is correct for it since we want the client to be fast, and the server is what we measure.

## Running benchmarks

Use `./bin/run-bench.sh` with `required_permissions: ["all"]`. The script:
1. Tears down Docker and recreates a fresh Postgres volume
2. Builds all release binaries
3. Runs migrations
4. Starts the server in the background
5. Runs the full benchmark suite
6. Cleans up the server on exit

Override sizes with env vars: `BENCH_WARM=1000000 ./bin/run-bench.sh`

To monitor Postgres during a run, use `PGPASSWORD=postgres psql -h localhost -p 54320 -U postgres -d union_find` in a separate shell.
