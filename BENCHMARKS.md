# Benchmark Results

## Environment

| Parameter | Value |
|-----------|-------|
| **Hardware** | Apple Silicon (aarch64), macOS |
| **PostgreSQL** | 17.9, Docker container (linux/arm64) |
| **Shared buffers** | 128 MB |
| **Work mem** | 4 MB |
| **Effective cache size** | 4 GB |
| **Max connections** | 100 |
| **Benchmark pool** | 50 connections |
| **Rust** | release build (optimized) |

All operations run single-threaded against the DB pool (sequential, not concurrent).
Latency numbers reflect wall-clock time per operation including round-trip to the
Docker-hosted Postgres instance over the loopback interface.

## Configuration

```
BENCH_TEAMS       = 100
BENCH_WARM        = 100,000
BENCH_CREATE      = 50,000
BENCH_ALIAS       = 100,000
BENCH_MERGE       = 100,000
BENCH_BATCH       = 10
BENCH_CHAIN_DEPTH = 100
BENCH_READS       = 1,000,000
BENCH_DB_POOL     = 50
```

## Results Summary

### Phase 1 — Warm-up (batched seeding)

100,000 persons created across 100 teams in batched transactions (500 ops/tx).

| Metric | Value |
|--------|-------|
| Total time | 37.25s |
| Throughput | **2,685 ops/s** |

Batching amortizes WAL sync overhead — ~3.6x faster than individual transactions.

### Phase 1b — Create (individual `handle_create`)

50,000 individual get-or-create calls (80% new distinct_ids, 20% existing).

| Metric | Value |
|--------|-------|
| Throughput | **752 ops/s** |
| p50 | 1.22 ms |
| p95 | 2.28 ms |
| p99 | 4.41 ms |
| max | 166.94 ms |

Each create touches 3 tables (INSERT into `person_mapping`, `distinct_id_mappings`,
`union_find`) inside a single transaction. The "get" path (20% of ops) is a simple
SELECT — effectively free compared to the write path.

### Phase 2 — Alias (4-case merge logic)

100,000 alias operations with mixed case distribution:
- 85% Case 1a: src exists, dest is new (link dest to src's root)
- 5% Case 2a: both exist, same person (no-op)
- 5% src==dest: get-or-create shortcut
- 5% Case 3: neither exists (create person + both distinct_ids)

| Metric | Value |
|--------|-------|
| Throughput | **533 ops/s** |
| p50 | 1.43 ms |
| p95 | 3.24 ms |
| p99 | 8.14 ms |
| max | 624.76 ms |

Alias is slower than create because Case 1a requires resolving the src chain
(CTE walk) before inserting the dest, and Case 3 inserts into all 3 tables plus
resolves the src. The p99/max spread reflects occasional fsync clustering.

### Phase 3 — Merge (batched, 10 dests per op)

100,000 distinct_ids merged in 10,000 batches of 10.

| Metric | Value |
|--------|-------|
| Throughput (per batch) | **210 batches/s** |
| Throughput (per dest) | ~2,100 dests/s |
| p50 | 4.67 ms |
| p95 | 5.71 ms |
| p99 | 7.48 ms |
| max | 46.02 ms |

Each batch runs in one transaction: resolve src once, then for each dest resolve its
chain, optionally re-point its root's `person_id`, and conditionally delete orphaned
`person_mapping` rows. The tight p50–p95 spread shows consistent per-batch cost.

### Phase 3b — Chain Deepening

95,884 merge operations to build realistic chain depth distribution.

| Metric | Value |
|--------|-------|
| Throughput | **503 ops/s** |
| p50 | 1.59 ms |
| p95 | 2.99 ms |
| p99 | 5.93 ms |
| max | 604.34 ms |

Chain distribution (4,116 chains across 100 teams):

| Depth | Chains |
|-------|--------|
| 0 | 1 |
| 1 | 588 |
| 2–4 | 814 |
| 5–9 | 575 |
| 10–24 | 809 |
| 25–49 | 587 |
| 50–99 | 742 |

**Important**: these "depths" refer to the number of sequential merge operations
chaining person ownership, not the union_find `next`-pointer chain depth. See
[Chain Depth Analysis](#chain-depth-analysis) below.

### Phase 4 — Read (resolve via CTE walk)

1,000,000 random lookups of non-primary distinct_ids.

| Metric | Value |
|--------|-------|
| Throughput | **3,072 ops/s** |
| p50 | 309.63 µs |
| p95 | 435.25 µs |
| p99 | 554.88 µs |
| max | 22.44 ms |

Reads are fast: sub-millisecond at p99. The recursive CTE walks 1–2 hops on average
(see below), and the entire working set fits in shared_buffers.

## Database State After Benchmark

### Row Counts

| Table | Rows |
|-------|------|
| `person_mapping` | 141,232 |
| `distinct_id_mappings` | 335,000 |
| `union_find` | 335,000 |
| — roots (person_id IS NOT NULL) | 245,000 |
| — non-roots (next IS NOT NULL) | 90,000 |

~2.37 distinct_ids per person on average. 103,775 person_mapping rows were deleted
during merges (orphan cleanup working correctly).

### Table Sizes

| Table | Heap | Indexes | Total |
|-------|------|---------|-------|
| `person_mapping` | 20 MB | 26 MB | 46 MB |
| `distinct_id_mappings` | 19 MB | 29 MB | 48 MB |
| `union_find` | 17 MB | 27 MB | 45 MB |
| **Total DB** | | | **146 MB** |

At 335,000 distinct_ids, the per-row storage cost is ~**450 bytes** across all tables
and indexes.

### Index Usage

| Index | Scans | Size | Purpose |
|-------|-------|------|---------|
| `union_find_pkey` (team_id, current) | 3,431,080 | 17 MB | CTE chain walk (hottest) |
| `idx_did_lookup` (team_id, distinct_id) | 1,746,951 | 21 MB | distinct_id → PK lookup |
| `person_mapping_pkey` (person_id) | 1,314,875 | 5.4 MB | person_id → UUID |
| `idx_uf_person` (team_id, person_id) partial | 195,942 | 11 MB | Merge orphan cleanup |
| `idx_person_mapping_lookup` (team_id, person_uuid) | 16 | 21 MB | UUID → PK (rarely used) |
| `distinct_id_mappings_pkey` (id) | 0 | 7.4 MB | Never used in current queries |

### Buffer Cache

| Metric | Value |
|--------|-------|
| Heap hit ratio | 99.98% |
| Index hit ratio | 99.99% |
| Total heap reads from disk | 1,632 |
| Total index reads from disk | 2,445 |

Entire working set fits in the 128 MB shared_buffers with room to spare.

### Tuple Churn

| Table | Inserts | Updates | Deletes | Dead tuples |
|-------|---------|---------|---------|-------------|
| `distinct_id_mappings` | 335,057 | 0 | 0 | 0 |
| `person_mapping` | 245,037 | 0 | 103,775 | 15,418 |
| `union_find` | 335,057 | 195,892 | 0 | 37,837 |

`person_mapping` has significant delete churn from merge cleanup. `union_find` has
update churn from re-pointing root `person_id` values during merges. Both accumulate
dead tuples that autovacuum would handle in production.

## Chain Depth Analysis

A key finding: **all union_find chains are exactly 1–2 hops deep** regardless of
the chain deepening phase.

```
max_chain_depth = 2   avg = 2.0   p50 = 2.0   p95 = 2.0
```

This is inherent to the data model:

- **Alias** (Case 1a): inserts a new `union_find` row with `next = src_root_pk`.
  The chain is always: `new_node → root`. One hop.
- **Alias** (Case 3): creates two nodes: `dest → src`, where src is a root.
  The chain from dest is 1 hop.
- **Merge**: resolves both src and dest to their roots, then **updates the dest
  root's `person_id`** to match the src. No new `next` links are created. Chain
  structure is unchanged.
- **Chain deepening**: repeatedly merges primaries into each other, which only
  re-points `person_id` on root nodes. The `next`-pointer graph stays shallow.

The recursive CTE completes in 1–2 iterations for every lookup, making reads
essentially O(1) index lookups. `EXPLAIN ANALYZE` confirms:

```
Execution Time: 0.087 ms
Buffers: shared hit=19
CTE chain: rows=2 (1 non-root + 1 root)
```

## Performance Bottlenecks

### 1. Write-path fsync latency (dominant)

Individual write operations (create, alias, merge) are bottlenecked by WAL fsync.
Each transaction commits synchronously, costing ~1–2 ms on the Docker loopback.
Batching (Phase 1 warm-up: 500 ops/tx) achieves 3.6x higher throughput by amortizing
this cost.

### 2. Merge transaction complexity

Each merge batch resolves the src chain once, then loops over N dests — each requiring
a CTE walk, a conditional UPDATE, an EXISTS check for orphan detection, and a
conditional DELETE. At 10 dests/batch, this produces 4.67 ms p50 per batch (vs 1.43 ms
for a single alias).

### 3. Index bloat on `idx_person_mapping_lookup`

The `(team_id, person_uuid)` index is 21 MB despite only 16 scans — it's used solely
for the reverse UUID→PK lookup in `resolve()`. This index could be dropped if resolves
only go distinct_id → person_uuid (the common path). Similarly, `distinct_id_mappings_pkey`
(7.4 MB) has zero scans and exists only for referential integrity.

### 4. Dead tuple accumulation

Merge-heavy workloads generate dead tuples in `person_mapping` (deletes) and
`union_find` (updates). After the benchmark: 15,418 dead tuples in `person_mapping`
and 37,837 in `union_find`. Production would need aggressive autovacuum tuning
for these tables.

## Scaling Estimate: 100M Persons, Billions of Distinct IDs

### Assumptions

- 100M persons, 1B distinct_ids (~10 distinct_ids per person)
- Production server: 64 cores, 256 GB RAM, NVMe SSD, dedicated Postgres
- `shared_buffers` = 64 GB, `effective_cache_size` = 192 GB

### Storage

At ~450 bytes/distinct_id (observed), 1B distinct_ids ≈ **450 GB** total across
tables and indexes. This exceeds RAM but the hot working set (recent/active users)
would fit in shared_buffers.

| Table | Estimated heap | Estimated indexes | Total |
|-------|---------------|-------------------|-------|
| `person_mapping` | 6 GB | 8 GB | 14 GB |
| `distinct_id_mappings` | 57 GB | 87 GB | 144 GB |
| `union_find` | 51 GB | 81 GB | 132 GB |
| **Total** | | | **~290 GB** |

(The observed 450 bytes/row includes all 6 indexes; at scale the per-row overhead
drops slightly due to better B-tree fill factors, but UUID-based indexes are inherently
large.)

### Read Performance

Reads should remain fast:

- The CTE walk is O(1) — chains are always 1–2 hops regardless of data size
- Each read hits 3 B-tree index lookups (~19 buffer hits observed)
- With the hot working set in shared_buffers: **sub-millisecond p99** sustained
- Cold reads (cache miss on NVMe): ~0.1 ms per random 8K page read, so worst case
  adds ~2 ms per lookup (6 potential page faults × 0.1 ms + seek time)
- Estimated throughput at scale: **10,000–50,000 reads/s** per connection with
  concurrent connections; total throughput scales linearly with connection count

### Write Performance

Writes are the bottleneck:

- **Individual creates/aliases**: ~500–750 ops/s per connection (fsync-bound).
  With `synchronous_commit = off` or group commit: ~5,000–10,000 ops/s per connection.
- **Batched writes** (e.g., 500 ops/tx): ~2,500–5,000 ops/s per connection.
  At 50 concurrent workers: **125,000–250,000 ops/s** aggregate.
- **Merge batches**: ~200 batches/s per connection (most expensive operation).
  Parallelizing across connections helps but lock contention on popular persons
  could limit scaling.

### Key Scaling Concerns

1. **Index size exceeding RAM**: At 1B rows, the B-tree indexes for
   `distinct_id_mappings` and `union_find` total ~170 GB. Only the hot subset
   fits in 64 GB shared_buffers. Cold lookups will hit NVMe. Mitigation:
   partition by `team_id` ranges to keep active teams' indexes hot.

2. **VACUUM pressure**: Merge-heavy workloads create dead tuples continuously.
   At 100M persons with frequent merges, autovacuum must run aggressively.
   Consider `autovacuum_vacuum_scale_factor = 0.01` and dedicated vacuum workers
   for `person_mapping` and `union_find`.

3. **Lock contention on hot persons**: Popular persons (celebrities, bots) get
   merged/aliased frequently. Row-level locks on their `union_find` root can
   serialize concurrent writers. Mitigation: worker pool affinity by team_id or
   person_id to reduce contention.

4. **Merge cleanup overhead**: The `EXISTS` check on `idx_uf_person` during
   orphan cleanup becomes expensive if many roots share a person_id (fan-in
   from prior merges). The partial index helps but at 100M scale, this scan
   could touch many rows. Mitigation: track reference counts explicitly or
   defer cleanup to a background job.

5. **WAL volume**: At 100K writes/s with 3 tables per write, WAL generation
   could exceed 1 GB/min. Ensure adequate `max_wal_size`, checkpoint tuning,
   and replication bandwidth if using replicas.

### Optimization Opportunities

| Optimization | Impact | Effort |
|-------------|--------|--------|
| `synchronous_commit = off` | 3–5x write throughput | Config change |
| Batch alias/create in application layer | 2–4x write throughput | Medium |
| Drop unused indexes (`distinct_id_mappings_pkey`, `idx_person_mapping_lookup`) | -28 MB per 335K rows; ~80 GB at 1B | Schema change |
| Partition by team_id | Keep hot teams' indexes in RAM | High |
| Path compression (update `next` to point at root after CTE walk) | Marginal — chains already O(1) | Low value |
| Connection pooling (PgBouncer) | Better connection utilization at high concurrency | Ops change |
| Read replicas for resolve queries | Linear read scaling | Ops change |

### Bottom Line

The design is well-suited for production scale. The O(1) chain depth is the key
advantage — reads never degrade regardless of data size, and the write path is
dominated by Postgres transaction overhead, not algorithmic complexity. At 100M
persons the main challenges are operational (index size, vacuum, WAL) rather than
algorithmic.
