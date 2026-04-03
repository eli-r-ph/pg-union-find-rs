# Benchmark Methodology

## Overview

Benchmarks send **parallel HTTP requests** to a running server instance using a
configurable concurrency level (`BENCH_CONCURRENCY`). State is pre-seeded via
direct DB calls so the timed sections only measure server throughput under
concurrent load, including the worker channel queue, serialized per-team write
processing, and Postgres round-trips.

All write endpoints use a 100 ms enqueue timeout (`ENQUEUE_TIMEOUT`). Requests
that fail (5xx or timeout) are counted as failures and excluded from latency
percentiles. `ops/s` is calculated from wall-clock time (total_ops / wall_time),
not the sum of individual latencies.

## Running

```bash
# Full bootstrap: tears down Docker, rebuilds, starts server, runs benchmarks:
bin/run-bench.sh

# Or manually: reset DB, start the server, run the benchmark client:
bin/reset-db.sh
cargo run --release &
cargo run --release --bin bench
```

## Phases

| Phase | Method | Description |
|-------|--------|-------------|
| 1 — Warm-up | DB-direct | Batch-seed N persons across N teams (untimed) |
| 1b — Create | POST /create | 80% new distinct_ids, 20% existing |
| 2 — Alias | POST /alias | Mixed: 85% Case 1a, 5% Case 2a, 5% src=dest, 5% Case 3 |
| 3 — Merge | POST /merge | Seed + merge in sub-batches |
| 3a — Batched Merge | POST /batched_merge | Same shape as Phase 3, separate seed, direct comparison |
| 3b — Deepen | DB-direct | Build realistic chain depths (untimed) |
| 4 — Read | POST /resolve | Random lookups of non-primary distinct_ids |
| 5 — Delete distinct_id | POST /delete_distinct_id | Delete pre-seeded distinct_ids |
| 6 — Delete person | POST /delete_person | Delete pre-seeded persons |

### Phase details

**Phase 1 (warm-up)** seeds the database via direct DB transactions in batches
of 500 ops/tx. This is untimed fixture creation, not a benchmark.

**Phase 1b (create)** sends parallel `POST /create` requests. 80% target new
distinct_ids (write path: 3 table inserts in one transaction), 20% target
existing distinct_ids (read path: simple SELECT).

**Phase 2 (alias)** sends parallel `POST /alias` requests with a mixed case
distribution: 85% Case 1a (target exists, source is new), 5% Case 2a (both
exist, same person), 5% target==source (get-or-create shortcut), 5% Case 3
(neither exists, create both).

**Phase 3 (merge)** first seeds N merge distinct_ids via direct DB (untimed),
then sends parallel `POST /merge` requests in sub-batches of configurable size.

**Phase 3a (batched merge)** seeds a separate pool of N distinct_ids via direct DB
(untimed), then sends parallel `POST /batched_merge` requests in sub-batches of
the same configurable size as Phase 3. This phase exists for direct latency
comparison against Phase 3 — identical data shape, different endpoint. The
batched path replaces per-source serial SQL with bulk lookups and batch
`INSERT`/`UPDATE`/`DELETE` statements.

**Phase 3b (chain deepen)** merges targets into each other via direct DB calls
to build realistic union_find chain depths. This is untimed fixture preparation
so Phase 4 reads traverse chains of varying depth. Teams are processed in
parallel via `JoinSet` (chains within a team are disjoint, so there is no
contention). Within each chain, links are sequential since each depends on the
prior merge.

**Phase 4 (read)** sends parallel `POST /resolve` requests for random
non-primary distinct_ids. Reads bypass the worker channel and go direct to the
DB pool.

**Phase 5 (delete_distinct_id)** seeds N distinct_ids via direct DB (untimed),
then sends parallel `POST /delete_distinct_id` requests.

**Phase 6 (delete_person)** seeds N persons via direct DB (untimed), capturing
their `person_uuid` values, then sends parallel `POST /delete_person` requests.

Delete phases run last so they don't interfere with read/write benchmarks.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `BENCH_TEAMS` | auto (N_WARM/1000) | Number of team_ids to distribute across |
| `BENCH_WARM` | 100,000 | Phase 1 person count |
| `BENCH_CREATE` | 50,000 | Phase 1b create count |
| `BENCH_ALIAS` | 100,000 | Phase 2 alias count |
| `BENCH_MERGE` | 100,000 | Phase 3 merge distinct_id count |
| `BENCH_BATCHED_MERGE` | 100,000 | Phase 3a batched merge distinct_id count |
| `BENCH_BATCH` | 10 | Phase 3/3a sub-batch size |
| `BENCH_CHAIN_DEPTH` | 100 | Phase 3b max chain depth |
| `BENCH_READS` | 1,000,000 | Phase 4 read count |
| `BENCH_DELETE_DID` | 10,000 | Phase 5 delete_distinct_id count |
| `BENCH_DELETE_PERSON` | 10,000 | Phase 6 delete_person count |
| `BENCH_DB_POOL` | 50 | Max DB connections (seeding only) |
| `BENCH_CONCURRENCY` | 50 | Max parallel HTTP requests |
| `BENCH_BASE_URL` | http://127.0.0.1:3000 | Server base URL |

## Output format

Each timed phase reports:

```
  [phase_name]
    ops:       50000
    failures:  0 (0.00%)
    wall time: 2.34s
    ops/s:     21367
    p50:       1.12ms
    p95:       3.45ms
    p99:       8.22ms
    max:       45.10ms
```

- **ops**: total requests sent (successes + failures)
- **failures**: count and percentage of non-2xx responses (queue full, worker
  unavailable, DB errors)
- **wall time**: elapsed clock time for the entire phase
- **ops/s**: total_ops / wall_time
- **p50/p95/p99/max**: latency percentiles computed from successful requests only

## Queue saturation tracking

All write endpoints use `tokio::time::timeout(100ms)` when enqueueing to the
per-team worker channel. If the channel is full and the timeout expires, the
server returns 503 `"queue full"`. The benchmark tracks these as failures.

With the default configuration (100 workers, 1024 channel capacity each), queue
saturation is expected to be negligible under normal concurrency (50 parallel
requests). High failure rates would indicate the worker pool or channel capacity
needs scaling.

## Docker Postgres tuning

The `docker-compose.yml` configures production-grade Postgres settings for
realistic benchmark results:

| Setting | Value | Purpose |
|---------|-------|---------|
| `shared_buffers` | 256 MB | Buffer cache |
| `effective_cache_size` | 512 MB | Planner cost estimates |
| `work_mem` | 8 MB | Per-sort/hash memory |
| `maintenance_work_mem` | 64 MB | VACUUM/CREATE INDEX |
| `wal_buffers` | 16 MB | WAL write buffering |
| `max_wal_size` | 1 GB | Checkpoint spacing |
| `wal_compression` | lz4 | WAL size reduction |
| `commit_delay` | 10 us | Group commit window |
| `commit_siblings` | 5 | Group commit threshold |
| `random_page_cost` | 1.1 | SSD-tuned planner |
| `effective_io_concurrency` | 200 | Parallel I/O hints |
| `checkpoint_completion_target` | 0.9 | Spread checkpoint I/O |
| `max_connections` | 200 | Connection limit |
| `shm_size` | 512 MB | Container shared memory |
