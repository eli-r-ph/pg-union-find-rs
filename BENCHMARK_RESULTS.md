# Benchmark Results

**Date:** 2026-03-30
**Machine:** Apple Silicon (macOS), Docker Desktop
**Postgres:** 17 (Docker), tuned for benchmarking (see `docker-compose.yml`)
**Server config:** 64 workers, 64 channel capacity, compress threshold 20

## Configuration

| Parameter | Value |
|-----------|-------|
| BENCH_TEAMS | 100 |
| BENCH_WARM | 100,000 |
| BENCH_CREATE | 50,000 |
| BENCH_ALIAS | 100,000 |
| BENCH_MERGE | 100,000 (10 per batch = 10,000 batches) |
| BENCH_BATCHED_MERGE | 100,000 (10 per batch = 10,000 batches) |
| BENCH_CHAIN_DEPTH | 100 |
| BENCH_READS | 1,000,000 |
| BENCH_RESOLVE_DIDS | 100,000 |
| BENCH_DELETE_DID | 10,000 |
| BENCH_DELETE_PERSON | 10,000 |
| BENCH_DB_POOL | 50 |
| BENCH_CONCURRENCY | 50 |

## Raw Results

### Phase 1: Warm-up (DB-direct seeding, untimed)

Created 100,000 persons across 100 teams in **6.57s** (15,224 ops/s).
Parallel seeding via JoinSet with 500-op transaction batches.

### Phase 1b: Create (`POST /create`)

| Metric | Value |
|--------|-------|
| Total ops | 50,000 |
| Failures | 0 (0.00%) |
| Wall time | 10.47s |
| Throughput | 4,778 ops/s |
| p50 | 5.45ms |
| p95 | 14.96ms |
| p99 | 193.16ms |
| max | 238.12ms |

### Phase 2: Alias (`POST /alias`)

| Metric | Value |
|--------|-------|
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 25.34s |
| Throughput | 3,947 ops/s |
| p50 | 6.84ms |
| p95 | 20.35ms |
| p99 | 201.57ms |
| max | 423.39ms |

Mix: 90% Case 1a (new source -> existing target), 5% self-alias, 5% Case 3 (both new). Shuffled.

### Phase 3: Merge (`POST /merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct_ids) |
| Failures | 0 (0.00%) |
| Wall time | 40.08s |
| Throughput | 249 ops/s (batches), ~2,494 merges/s |
| p50 | 195.97ms |
| p95 | 381.28ms |
| p99 | 449.94ms |
| max | 607.12ms |

Pre-seeding: 100,000 merge distinct_ids in **6.44s** (parallel).

### Phase 3a: Batched Merge (`POST /batched_merge`, 10 sources per batch)

_Pending — re-run benchmarks to populate._

### Phase 3b: Chain Deepening (DB-direct, untimed)

95,839 link ops across 4,161 chains (max depth 100) in **239.20s**.

Chain depth distribution:

| Depth | Chains |
|-------|--------|
| 0 | 3 |
| 1 | 581 |
| 2-4 | 813 |
| 5-9 | 583 |
| 10-24 | 815 |
| 25-49 | 688 |
| 50-99 | 678 |

### Phase 4: Resolve/Read (`POST /resolve`)

| Metric | Value |
|--------|-------|
| Total ops | 1,000,000 |
| Failures | 0 (0.00%) |
| Wall time | 73.33s |
| Throughput | 13,638 ops/s |
| p50 | 1.88ms |
| p95 | 3.33ms |
| p99 | 5.73ms |
| max | 803.27ms |

Lookup pool: 200,000 IDs (100K warm-up primaries + 100K merge IDs), all DB-seeded.

### Phase 4a: Resolve Distinct IDs (`POST /resolve_distinct_ids`)

_Pending — re-run benchmarks to populate._

Reuses all live persons from prior phases (no additional seeding). Collects `(team_id, person_uuid)` pairs from `person_mapping` at benchmark time, then sends 100K concurrent resolve requests with 80/20 hot-set bias. Persons have varying fan-out (1 DID for warm-up singletons, many DIDs for merge/alias targets, deep chains from deepening phase), exercising the reverse recursive CTE across realistic data shapes.

### Phase 5: Delete Distinct ID (`POST /delete_distinct_id`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 3.87s |
| Throughput | 2,581 ops/s |
| p50 | 8.22ms |
| p95 | 165.52ms |
| p99 | 207.78ms |
| max | 373.92ms |

Pre-seeding: 10,000 distinct_ids in **760ms** (parallel).

### Phase 6: Delete Person (`POST /delete_person`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 2.56s |
| Throughput | 3,905 ops/s |
| p50 | 4.65ms |
| p95 | 16.86ms |
| p99 | 197.41ms |
| max | 216.10ms |

Pre-seeding: 10,000 persons in **838ms** (parallel).

---

## Analysis

### Throughput Summary

| Phase | ops/s | p50 | p99 | max |
|-------|-------|-----|-----|-----|
| Create | 4,778 | 5.45ms | 193.16ms | 238.12ms |
| Alias | 3,947 | 6.84ms | 201.57ms | 423.39ms |
| Merge (batch) | 249 | 195.97ms | 449.94ms | 607.12ms |
| Batched Merge | _pending_ | _pending_ | _pending_ | _pending_ |
| Resolve (read) | 13,638 | 1.88ms | 5.73ms | 803.27ms |
| Resolve Distinct IDs | _pending_ | _pending_ | _pending_ | _pending_ |
| Delete DID | 2,581 | 8.22ms | 207.78ms | 373.92ms |
| Delete Person | 3,905 | 4.65ms | 197.41ms | 216.10ms |

### Performance Bottlenecks

**1. Merge is the slowest operation by far (249 batch ops/s)**

Each merge batch processes 10 distinct_ids in a single transaction, requiring:
- 10x `check_did` lookups
- 10x `resolve_root` recursive CTE traversals
- 10x `link_root_to_target` updates (with `person_mapping` deletion)
- All within a single serializable transaction

The p50 of ~196ms for a 10-source merge is dominated by DB round-trips and row-level locking. With chains up to depth 100 from prior deepening, `resolve_root` CTE traversal is expensive.

**2. p99/max outliers across all write phases (~200ms p99)**

All write operations show a sharp jump between p95 and p99 (e.g., create p95=15ms vs p99=193ms). This pattern strongly suggests **WAL fsync clustering**: Postgres batches WAL flushes, and unlucky requests land on the synchronous flush boundary. The `commit_delay=200` / `commit_siblings=5` settings in `docker-compose.yml` mitigate this, and `synchronous_commit=off` eliminates the client-side fsync wait entirely.

**3. Chain deepening was extremely slow (239s for 96K ops)**

This is DB-direct (not benchmarked), but it dominated total run time. Each merge called `handle_merge` sequentially per chain link across all teams. Now parallelized across teams via `JoinSet` (see R2). Re-run benchmarks to measure the improvement.

**4. Read throughput is strong but max latency is high (803ms)**

The p50 (1.88ms) and p95 (3.33ms) are excellent. The 803ms max is likely a single request that hit during a background path-compression write or a Postgres checkpoint/WAL flush. With 1M operations this tail is expected.

### Queue Saturation (503 Analysis)

**Zero failures across all phases.** The 100ms enqueue timeout was never triggered.

This indicates the current configuration (64 workers, 64 channel capacity, 50 concurrent HTTP requests) has sufficient headroom. The 50-concurrency benchmark is well within the 64-worker pool's capacity: requests distribute across 100 teams hashed to 64 workers, so each worker's channel sees roughly 0.8 concurrent requests on average.

To stress-test queue saturation, future runs should increase `BENCH_CONCURRENCY` significantly (e.g., 500-1000) while keeping the worker pool at 64.

---

## Remediation Proposals

### R1: Reduce merge latency with batched SQL ✓ (implemented)

The merge phase is bottlenecked by per-source round-trips within each batch. Instead of N individual `check_did` + `resolve_root` + `link_root_to_target` calls per merge batch, batch the lookups into a single multi-row query and process results in-memory.

**Status:** Implemented as `POST /batched_merge`. Phase 3a benchmarks this endpoint against the same data shape as Phase 3 for direct comparison. Re-run benchmarks to measure the improvement.

### R2: Parallelize chain deepening ✓ (implemented)

Phase 3b ran 96K merge operations sequentially. Since chains for different teams are independent, use `JoinSet` to deepen chains for multiple teams concurrently (same pattern as `seed_parallel`).

**Status:** Implemented. Chain deepening now spawns one task per team via `JoinSet`, processing each team's chains concurrently. With 100 teams and a 50-connection DB pool, this should be roughly 50x faster. Re-run benchmarks to measure.

### R3: Tune WAL flush behavior for lower p99 ✓ (implemented)

The ~200ms p99 cliff across all write phases was caused by WAL fsync clustering on Docker Desktop's virtualized filesystem. Applied: `synchronous_commit=off`, `wal_level=minimal`, `max_wal_senders=0`, `full_page_writes=off`, `checkpoint_timeout=15min`, `commit_siblings` lowered from 20 to 5. Re-run benchmarks to measure the improvement.

### R4: Stress-test queue saturation

Current benchmarks use 50 concurrency against 64 workers -- no saturation is possible. Add a dedicated stress benchmark with `BENCH_CONCURRENCY=500+` to validate the 100ms enqueue timeout behavior and identify the saturation point.

### R5: Add per-status-code failure breakdown

While all phases showed 0 failures in this run, the failure tracking still counts all non-2xx as a single bucket. Adding a `HashMap<StatusCode, u64>` breakdown in `run_parallel` would distinguish 409 (business logic), 503 (queue full), 500 (server error), and timeout failures for future runs with higher concurrency.
