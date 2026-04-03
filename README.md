# pg-union-find-rs

A Postgres-backed union-find service for person/distinct_id resolution, modeled after PostHog's identity merge system. Built with Rust (Axum + SQLx + Tokio).

## Architecture

- **Three tables:** `person_mapping`, `distinct_id_mappings`, `union_find` -- the union_find table forms a linked chain of distinct_id PKs traversed by a recursive CTE. Root rows carry the `person_id`.
- **Worker pool:** HTTP handlers partition operations by `team_id` into N bounded channels, serializing same-team writes while different teams run in parallel. Write enqueue uses a 100ms timeout, returning 503 if the queue is full.
- **Path compression:** After resolve operations that traverse chains deeper than `PATH_COMPRESS_THRESHOLD`, a background task compresses the chain to reduce future traversal cost.

### Schema

```
person_mapping:       person_id (PK bigserial), team_id, person_uuid
distinct_id_mappings: id (PK bigserial), team_id, distinct_id, is_identified, deleted_at
union_find:           (team_id, current) PK, next (nullable), person_id (nullable)
```

- `person_mapping` maps internal bigint PKs to external person UUIDs.
- `distinct_id_mappings` maps `(team_id, distinct_id)` strings to internal bigint PKs.
- `union_find` rows are chain links: `current -> next -> ... -> root`. A root row has `next = NULL` and `person_id` set.

### Endpoints

| Method | Path | Body | Description |
|--------|------|------|-------------|
| GET | `/health` | -- | Health check (verifies DB connection) |
| POST | `/create` | `{ team_id, distinct_id }` | Get-or-create a person for a single distinct_id |
| POST | `/identify` | `{ team_id, target, anonymous }` | Link anonymous distinct_id to target (`$identify`) |
| POST | `/alias` | `{ team_id, target, alias }` | Link alias distinct_id to target (`$create_alias`) |
| POST | `/merge` | `{ team_id, target, sources }` | Force-merge N sources into target (`$merge_dangerously`) |
| POST | `/batched_merge` | `{ team_id, target, sources }` | Batched variant of `/merge` — same semantics, fewer DB round-trips |
| POST | `/resolve` | `{ team_id, distinct_id }` | Resolve distinct_id to person_uuid via chain traversal |
| POST | `/delete_person` | `{ team_id, person_uuid }` | Soft-delete a person and all associated distinct_ids |
| POST | `/delete_distinct_id` | `{ team_id, distinct_id }` | Soft-delete a single distinct_id |

### Operation details

- **`/create`**: If the distinct_id exists, returns the resolved person. Otherwise creates `person_mapping` + `distinct_id_mappings` + `union_find` root row.
- **`/identify`**: Routes to the same 4-case merge logic as `/alias`. When `target == anonymous`, acts as get-or-create for that single distinct_id.
- **`/alias`**: Handles 4 cases:
  - One exists, the other doesn't: link the new one into the existing chain.
  - Both exist, same person: no-op.
  - Both exist, different persons: merge if the source person is unidentified; reject with 409 if already identified (caller must use `/merge` to force).
  - Neither exists: create a new person with both distinct_ids.
- **`/merge`**: Ignores `is_identified` -- always merges. For each source: if new, create link to target; if existing with a different person, link source's tree into target's tree.
- **`/batched_merge`**: Same request/response shape and semantics as `/merge`. Replaces per-source serial SQL with bulk lookups (`ANY()`), a single recursive CTE for all sources, and batch `INSERT`/`UPDATE`/`DELETE` statements. Reduces DB round-trips from O(N) to O(1) for typical batches.
- **`/resolve`**: Walk union_find chain via recursive CTE to root, join person_mapping to return `person_uuid`. Triggers background path compression when chain depth exceeds threshold.
- **`/delete_person`**: Sets `deleted_at` on all distinct_ids for the person and removes the person_mapping row.
- **`/delete_distinct_id`**: Sets `deleted_at` on the distinct_id. If it was the last live distinct_id for its person, also removes the person_mapping row.

## Running

```bash
docker compose up -d
cargo run --release --bin migrate
cargo run --release --bin server
```

Server configuration via env vars:

| Env Var | Default | Description |
|---------|---------|-------------|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:54320/union_find` | Postgres connection string |
| `WORKER_POOL_SIZE` | 100 | Number of per-team worker channels |
| `WORKER_CHANNEL_CAPACITY` | 1024 | Bounded channel capacity per worker |
| `PATH_COMPRESS_THRESHOLD` | 20 | Chain depth that triggers background compression |

## Scripts

| Script | Description |
|--------|-------------|
| `bin/run-bench.sh` | Full benchmark lifecycle: tears down Docker, rebuilds, migrates, starts server, runs benchmark client, cleans up. Pass `--down` to destroy containers on exit. |
| `bin/run-tests.sh` | Full-reset test runner: destroys volume, recreates DB, migrates, truncates, runs `cargo test`. Pass `-- test_name` to run a single test. |
| `bin/ensure-db.sh` | Idempotent helper: ensures Docker Postgres is running and migrated. Pass `--fresh` to destroy and recreate. Safe to `source` from other scripts. |
| `bin/reset-db.sh` | Reset benchmark data: truncates application tables (fast) or pass `--full` to destroy the container and volume entirely. |

## Benchmarks

The benchmark harness (`src/bin/bench.rs`) sends parallel HTTP requests to a running server and measures throughput and latency. Fixture data is pre-seeded via direct DB calls so timed sections measure only server performance under concurrent load.

### Phases

1. **Warm-up** -- DB-direct seeding of N persons across M teams (parallel, untimed)
2. **Create** -- `POST /create` (80% new, 20% existing, shuffled)
3. **Alias** -- `POST /alias` (90% new-source-to-existing-target, 5% self-alias, 5% both-new, shuffled)
4. **Merge** -- `POST /merge` in batches of 10 (sources DB-seeded before timed phase)
5. **Batched Merge** -- `POST /batched_merge` with same batch size and data shape as Phase 4 (separate seed, for direct comparison)
6. **Chain deepening** -- DB-direct merges to build realistic chain depths (untimed)
7. **Resolve** -- `POST /resolve` against DB-seeded IDs only (warm-up primaries + merge IDs)
8. **Delete distinct_id** -- `POST /delete_distinct_id` (DB-seeded before timed phase)
9. **Delete person** -- `POST /delete_person` (DB-seeded before timed phase)

### Running

```bash
# Full bootstrap (recommended):
bin/run-bench.sh

# Override sizes:
BENCH_WARM=1000000 BENCH_READS=5000000 bin/run-bench.sh

# Manual: reset DB, start server, run benchmark client:
bin/reset-db.sh
cargo run --release --bin server &
cargo run --release --bin bench
```

### Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `BENCH_TEAMS` | auto (N_WARM/1000) | Number of team_ids |
| `BENCH_WARM` | 100,000 | Phase 1 person count |
| `BENCH_CREATE` | 50,000 | Phase 2 create count |
| `BENCH_ALIAS` | 100,000 | Phase 3 alias count |
| `BENCH_MERGE` | 100,000 | Phase 4 merge distinct_id count |
| `BENCH_BATCHED_MERGE` | 100,000 | Phase 5 batched merge distinct_id count |
| `BENCH_BATCH` | 10 | Phase 4/5 sub-batch size |
| `BENCH_CHAIN_DEPTH` | 100 | Phase 6 max union_find chain depth |
| `BENCH_READS` | 1,000,000 | Phase 6 read count |
| `BENCH_DELETE_DID` | 10,000 | Phase 7 delete_distinct_id count |
| `BENCH_DELETE_PERSON` | 10,000 | Phase 8 delete_person count |
| `BENCH_DB_POOL` | 50 | Max DB connections (seeding) |
| `BENCH_CONCURRENCY` | 50 | Max parallel HTTP requests |
| `BENCH_BASE_URL` | http://127.0.0.1:3000 | Server base URL |

Results from the latest run are in [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md).
