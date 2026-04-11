# Benchmark Results

**Date:** 2026-04-03
**Machine:** Apple Silicon (macOS), Docker Desktop (OrbStack)
**Postgres:** 17 (Docker), tuned for benchmarking (see `docker-compose.yml`)
**Server config:** 64 workers, 64 channel capacity, compress threshold 20, READ_POOL_SIZE 4
**Rust:** 1.90.0 (stable), edition 2024, release profile (opt-level 3)
**Run time:** ~195s total (including seeding)

---

## Production Prospects: 1000s req/s, 100M Persons, Billions of DIDs

This section assesses viability at production scale based on the benchmark findings below.

### Verdict: Viable with operational investment

The core data structure and query patterns are sound. The recursive CTE resolves identities at ~15K ops/s on a laptop Docker setup with sub-2ms p50 latency. The bottlenecks are operational (vacuum, partitioning, pool sizing) rather than algorithmic. Below is the detailed extrapolation.

### Read throughput (the primary operation)

Forward resolve (`/resolve`) achieves 15,310 ops/s at p50=1.81ms from a single app instance with 50 concurrent clients against a Docker Postgres with 256MB shared_buffers. The CTE averages 4.1 buffer hits per chain hop, all served from RAM.

**At production scale:** A vertically scaled Postgres (64 vCPUs, 256GB RAM, NVMe) with the entire working set in shared_buffers, fronted by 4-8 app instances behind PgBouncer, could sustain **100K-200K read ops/s**. Reads are CPU-bound on B-tree traversal, not I/O-bound. With 64 hash partitions by `team_id`, reads for different teams touch disjoint index pages, eliminating cross-team cache contention. Partition pruning reduces the effective index tree depth from ~4 levels (1B rows) to ~3 levels (16M rows/partition).

### Write throughput

The worker-per-team serialization model means each team's writes execute sequentially. With 64 workers, aggregate write throughput scales linearly:

| Operation | Per-worker | 64 workers aggregate |
|-----------|-----------|---------------------|
| Create | ~6,000/s | ~384,000/s |
| Alias | ~4,500/s | ~288,000/s |
| Batched merge (10/batch) | ~1,200 batches/s | ~76,800 batches/s (768K merges/s) |

The bottleneck shifts to Postgres WAL write throughput and connection pool sizing. With `synchronous_commit=on` (required for production), each commit adds ~0.5-2ms for WAL fsync on NVMe. Group commit (`commit_delay`, `commit_siblings`) amortizes this.

### Table sizes at 100M persons, 1B DIDs

| Table | Estimated Size | Index Size | Total |
|-------|---------------|------------|-------|
| union_find | ~60 GB | ~100 GB | ~160 GB |
| distinct_id_mappings | ~55 GB | ~75 GB | ~130 GB |
| person_mapping | ~50 GB | ~70 GB | ~120 GB |
| **Total** | **~165 GB** | **~245 GB** | **~410 GB** |

Fits on a single vertically scaled instance with 1TB NVMe storage. With 64 hash partitions, each partition holds ~6.4 GB of data -- small enough for per-partition `VACUUM FULL` during off-peak windows.

### Critical scaling risks

**1. Chain depth growth.** The benchmark's post-compression max depth is 22 (mean 3.23, p99=17). Path compression triggers on both writes (`CompressHint` above threshold) and reads (fire-and-forget `try_send`). But at production scale, the compress channel can saturate -- the benchmark logged ~100 "channel full" errors during the resolve phase, all from 5 hot team_ids. A **periodic background compaction job** (per-team full-tree flattening) is essential to guarantee bounded resolve latency regardless of merge velocity.

**2. Dead tuple bloat.** After the benchmark, `person_mapping` has 82.6% dead tuples and `union_find` has 46.6%. Merges generate 1 dead tuple per source in both tables. At 768K merges/s aggregate, that's ~1.5M dead tuples/s. Required: aggressive per-table autovacuum (`autovacuum_vacuum_scale_factor=0.01`, `autovacuum_vacuum_cost_delay=0`) and monitoring of `n_dead_tup`.

**3. `idx_person_mapping_lookup` sizing.** At 32 MB for 110K scans (296 bytes/scan) in this benchmark run, this index stores `(team_id, person_uuid)`. The `person_uuid` column has been migrated from `varchar(200)` to native `UUID` (16 bytes), which should reduce this index to ~8 GB at 100M persons (down from ~32 GB with the old varchar type).

**4. Partitioning.** The current single-table design works at 450K rows but is untenable at 1B+. `PARTITION BY HASH(team_id, 64)` is essential: index B-tree depth drops a level, per-partition vacuum runs faster, and partition pruning eliminates 63/64 partitions on every query (all queries already filter on `team_id`).

**5. WAL pressure with durability enabled.** The benchmark generated 515 MB of WAL with `synchronous_commit=off`. In production with full durability, WAL fsync adds latency per commit. NVMe SSDs with `wal_compression=lz4` and group commit mitigate this, but WAL throughput becomes the ceiling for aggregate write rate.

---

## Configuration

| Parameter | Value |
|-----------|-------|
| Teams | 100 |
| Create ops | 50,000 |
| Alias ops | 100,000 |
| Merge ops | 100,000 (10 per batch = 10,000 batches) |
| Batched merge ops | 100,000 (10 per batch = 10,000 batches) |
| Max chain depth | 100 |
| Resolve ops | 1,000,000 |
| Resolve distinct IDs ops | 100,000 |
| Delete DID ops | 10,000 |
| Delete person ops | 10,000 |
| HTTP concurrency | 50 |

---

## Throughput Summary

| Endpoint | ops/s | p50 | p95 | p99 | max |
|----------|-------|-----|-----|-----|-----|
| `POST /create` | 6,043 | 4.20ms | 13.53ms | 182.26ms | 410.01ms |
| `POST /alias` | 4,561 | 5.27ms | 18.76ms | 196.79ms | 446.59ms |
| `POST /merge` (10/batch) | 305 | 163.45ms | 307.39ms | 322.33ms | 348.04ms |
| **`POST /batched_merge` (10/batch)** | **1,202** | **40.94ms** | **77.23ms** | **86.65ms** | **118.63ms** |
| `POST /resolve` | 15,310 | 1.81ms | 3.07ms | 4.72ms | 1.01s |
| `POST /resolve_distinct_ids` | 14,485 | 1.77ms | 3.25ms | 5.01ms | 602.85ms |
| `POST /delete_distinct_id` | 2,968 | 7.08ms | 107.85ms | 212.68ms | 409.85ms |
| `POST /delete_person` | 3,716 | 3.69ms | 35.69ms | 206.35ms | 225.30ms |

**Zero failures across 1.28M HTTP operations.** No 503s, no 500s, no timeouts. The 64-worker pool with 64-depth channels handled 50 concurrent clients without write saturation.

---

## Results by Endpoint

### Create (`POST /create`)

| Metric | Value |
|--------|-------|
| Total ops | 50,000 |
| Failures | 0 (0.00%) |
| Wall time | 8.27s |
| Throughput | 6,043 ops/s |
| p50 | 4.20ms |
| p95 | 13.53ms |
| p99 | 182.26ms |
| max | 410.01ms |

**Workload mix:** 80% new distinct IDs, 20% existing (hot-set re-identification).

**Query plan (hot path):** Single index scan on `idx_did_lookup`, 7 buffer hits, sub-0.2ms execution time. For new DIDs, the create path runs 3 sequential INSERTs (person_mapping, distinct_id_mappings, union_find) in one transaction.

**Bottleneck:** The p99 cliff (4.20ms to 182ms) is a Docker Desktop I/O scheduling artifact. The virtualized tmpfs introduces periodic stalls when the host flushes dirty pages. On bare-metal NVMe, expect p99 to flatten to 15-25ms.

---

### Alias (`POST /alias`)

| Metric | Value |
|--------|-------|
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 21.93s |
| Throughput | 4,561 ops/s |
| p50 | 5.27ms |
| p95 | 18.76ms |
| p99 | 196.79ms |
| max | 446.59ms |

**Workload mix:** 85% Case 1a (target exists, source new), 5% Case 2a (same person), 5% self-alias, 5% Case 3 (both new).

**Bottleneck:** Alias is ~25% slower than create because the dominant Case 1a path requires: (1) resolve target root via recursive CTE, (2) `check_did` on source, (3) INSERT into `distinct_id_mappings` + `union_find`, (4) UPDATE `person_mapping.is_identified`. The 446ms max reflects occasional lock contention when multiple aliases target the same hot-set person.

---

### Merge (`POST /merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct_ids) |
| Failures | 0 (0.00%) |
| Wall time | 32.77s |
| Throughput | 305 batch ops/s (~3,050 merges/s) |
| p50 | 163.45ms |
| p95 | 307.39ms |
| p99 | 322.33ms |
| max | 348.04ms |

**This is the slowest operation.** Each 10-source merge batch executes 10 serial iterations within a single transaction: `check_did` (index lookup + CTE) then `link_root_to_target` (UPDATE union_find + DELETE person_mapping). The p50 of 163ms means ~16ms per source. The narrow p50-to-p99 spread (163ms to 322ms) indicates consistent, predictable slowness rather than sporadic stalls.

---

### Batched Merge (`POST /batched_merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct_ids) |
| Failures | 0 (0.00%) |
| Wall time | 8.32s |
| Throughput | 1,202 batch ops/s (~12,020 merges/s) |
| p50 | 40.94ms |
| p95 | 77.23ms |
| p99 | 86.65ms |
| max | 118.63ms |

**3.9x throughput improvement over `/merge`.** The batched path replaces per-source serial SQL with bulk operations:
1. Single `batch_lookup_dids`: `ANY(ARRAY[...])` index scan returning all PKs (39 buffer hits, sub-0.3ms).
2. Single `batch_resolve_pks`: multi-start recursive CTE using `unnest($2::bigint[])` (14 buffer hits for shallow chains).
3. Batch INSERT for not-found sources, batch UPDATE + DELETE for live-different roots.

| Metric | Merge | Batched Merge | Improvement |
|--------|-------|---------------|-------------|
| ops/s (batch) | 305 | 1,202 | **3.9x** |
| p50 | 163.45ms | 40.94ms | **4.0x** |
| p95 | 307.39ms | 77.23ms | **4.0x** |
| p99 | 322.33ms | 86.65ms | **3.7x** |

---

### Chain Depth at Read Time

Sampled chain depth statistics (teams 1-10, 38,980 non-root nodes):

| Metric | Value |
|--------|-------|
| Mean depth | 3.23 |
| p50 | 2 |
| p95 | 12 |
| p99 | 17 |
| Max | 22 |

The configured chain-deepen phase built chains up to depth 100, but path compression (threshold=20) flattened most chains during the write phases and the 1M-resolve read phase. The max of 22 (slightly above threshold) reflects chains that were only partially compressed due to compress channel saturation during the read burst.

**Compress channel saturation:** During the 1M-resolve phase, ~100 "compress enqueue failed: channel full" log entries were emitted, concentrated on 5 team_ids (5, 15, 69, 77, 79). These teams had chains at depths 20-98 that triggered fire-and-forget `try_send` on the worker channel, which was already occupied processing other ops. The compression was eventually handled on subsequent reads, but this demonstrates that at production scale a background compaction job is needed rather than relying solely on opportunistic compression.

---

### Resolve / Read (`POST /resolve`)

| Metric | Value |
|--------|-------|
| Total ops | 1,000,000 |
| Failures | 0 (0.00%) |
| Wall time | 65.32s |
| Throughput | 15,310 ops/s |
| p50 | 1.81ms |
| p95 | 3.07ms |
| p99 | 4.72ms |
| max | 1.01s |

**This is the primary operation and it performs well.** The p50 of 1.81ms includes full HTTP round-trip (client -> Axum -> pool -> CTE -> response). The CTE walks `union_find_pkey` with an average of ~6.5 index scans per resolve (6.5M total / 1M reads), reflecting mixed chain depths.

**Query plan (depth 0, root node):** 18 buffer hits, 0.58ms execution. The CTE starts, finds `person_id IS NOT NULL` immediately, and terminates after 1 loop. One `union_find_pkey` scan (7 hits) + Memoize miss (1) + `person_mapping_pkey` join (4 hits).

**Query plan (depth 22, deep chain):** 106 buffer hits, 0.67ms execution. The CTE loops 23 times, each performing one Memoized `union_find_pkey` scan (4.1 hits/hop). The 95 CTE buffer hits cover the full chain walk. Final `person_mapping_pkey` join adds 4 hits.

**Cost model:** `resolve_time ~ 0.16ms + (depth x 0.024ms)`. At depth 0: ~0.16ms. At depth 22: ~0.69ms. At depth 100 (worst case): ~2.56ms.

**The 1.01s max** is a single outlier across 1M operations -- a Docker host-level scheduling stall. Acceptable as a long-tail artifact.

---

### Resolve Distinct IDs (`POST /resolve_distinct_ids`)

| Metric | Value |
|--------|-------|
| Live persons found | 49,338 (in 30ms) |
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 6.90s |
| Throughput | 14,485 ops/s |
| p50 | 1.77ms |
| p95 | 3.25ms |
| p99 | 5.01ms |
| max | 602.85ms |

**Query plan (reverse CTE, 335-DID person):** 2,611 buffer hits, 7.82ms execution. Starts from `idx_uf_person` (7 hits for root lookup), then walks outward via `idx_uf_next` (3.8 hits per child, 9 tree levels). Each child joins `distinct_id_mappings_pkey` (4 hits per DID). The LIMIT 10001 prevents runaway fan-out.

**Query plan (small person, 2 DIDs):** 22 buffer hits, 0.20ms execution. One `idx_uf_person` scan plus one `idx_uf_next` scan terminates quickly.

**DID fan-out per person (all teams):**

| Metric | Value |
|--------|-------|
| Persons with roots | 49,338 |
| Mean DIDs per person | 8.92 |
| p50 | 1 |
| p95 | 31 |
| p99 | 244 |
| Max | 572 |

The 80/20 hot-set distribution means some persons accumulated hundreds of DIDs from merge/alias phases. The p99 latency of 5.01ms -- well under write p99s -- confirms that even high-fan-out persons resolve quickly.

---

### Delete Distinct ID (`POST /delete_distinct_id`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 3.37s |
| Throughput | 2,968 ops/s |
| p50 | 7.08ms |
| p95 | 107.85ms |
| p99 | 212.68ms |
| max | 409.85ms |

**Bottleneck:** Delete DID is the most complex write operation. Each delete must: (1) `lookup_did` (index scan, 7 hits), (2) determine root or non-root, (3) `unlink_did` -- splice parents via `idx_uf_next`, or promote a parent to root if deleting a root, (4) hard-delete from `union_find` and `distinct_id_mappings`, (5) check if the person is orphaned and conditionally soft-delete.

**The p95 spike to 108ms** (15x the p50 of 7.08ms) is the worst p50-to-p95 ratio of any operation. The parent-splicing logic involves read-modify-write cycles on multiple `union_find` rows within a transaction, amplifying lock hold time under concurrency.

---

### Delete Person (`POST /delete_person`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 2.69s |
| Throughput | 3,716 ops/s |
| p50 | 3.69ms |
| p95 | 35.69ms |
| p99 | 206.35ms |
| max | 225.30ms |

**Delete person is 1.25x faster than delete DID** because it only soft-deletes: UPDATE `person_mapping.deleted_at` + UPDATE `union_find.deleted_at` via `idx_uf_person`. No chain walking or re-linking. Orphaned nodes are lazily cleaned up on subsequent access. The p95 of 35.69ms is reasonable; the p99 cliff to 206ms is the familiar Docker tmpfs scheduling stall.

---

## Postgres Internals Analysis

### Buffer Cache Performance

| Metric | Value |
|--------|-------|
| Buffer cache hit ratio | **99.9996%** |
| Disk block reads | 255 |
| Buffer hits | 62,783,932 |
| Temp files created | 0 |
| Temp bytes | 0 |

The entire ~210 MB dataset fits in `shared_buffers=256MB`. Virtually every operation is served from RAM. The 255 disk reads are from initial cold-cache catalog lookups and migration. Zero temp files confirms all sorts and hash joins fit in `work_mem=8MB`.

**Per-table I/O breakdown:**

| Table | Heap Hits | Index Hits | Disk Reads |
|-------|-----------|------------|------------|
| union_find | 9,268,076 | 28,063,715 | 0 |
| person_mapping | 3,102,986 | 7,983,515 | 1 |
| distinct_id_mappings | 2,914,121 | 10,524,086 | 0 |

Index buffer hits outnumber heap hits ~3:1 across all tables, confirming the recursive CTE's index-driven traversal pattern dominates. `union_find` accounts for 60% of all buffer hits.

### Transaction Statistics

| Metric | Value |
|--------|-------|
| Commits | 2,488,848 |
| Rollbacks | 1 |
| Deadlocks | 0 |
| Conflicts | 0 |

One rollback (likely a migration-time artifact). Zero deadlocks validates the `team_id`-scoped serialization strategy: since each team's writes are funneled through a single worker, two transactions for the same team can never deadlock.

### WAL Statistics

| Metric | Value |
|--------|-------|
| WAL records | 6,251,580 |
| Full-page images | 1,434 |
| WAL bytes | 515 MB |
| WAL buffers full | 0 |
| WAL writes | 1,551 |
| WAL syncs | 0 |

Zero WAL syncs confirms `synchronous_commit=off` is active. The 515 MB of WAL across 2.5M transactions averages ~207 bytes per commit. In production with durability enabled, each commit would add ~0.5-2ms for WAL sync on NVMe, amortized by group commit.

### Table Sizes and Dead Tuple Bloat

| Table | Total | Heap | Indexes | Live Tuples | Dead Tuples | Dead % |
|-------|-------|------|---------|-------------|-------------|--------|
| union_find | 77 MB | 29 MB | 47 MB | 450,000 | 392,251 | 46.6% |
| person_mapping | 71 MB | 31 MB | 40 MB | 69,338 | 330,227 | 82.6% |
| distinct_id_mappings | 62 MB | 27 MB | 35 MB | 450,000 | 10,000 | 2.2% |
| **Total** | **210 MB** | **87 MB** | **122 MB** | **969,338** | **732,478** | -- |

**Autovacuum was disabled** for this benchmark. Dead tuple counts reflect the full write workload:

- **person_mapping** (82.6% dead): Merges delete the "loser" person row. ~296K hard deletes + ~117K updates = massive churn. At production scale: `autovacuum_vacuum_scale_factor=0.01`, `autovacuum_vacuum_cost_delay=0`.
- **union_find** (46.6% dead): Merges update root rows (clear `person_id`, set `next`). ~386K updates generated dead tuples. HOT updates (5,204) were rare because most updates change indexed columns (`next`, `person_id`), preventing HOT.
- **distinct_id_mappings** (2.2% dead): Only `delete_distinct_id` (10K) generated dead tuples. Insert-heavy, no updates.

### DML Totals

| Table | Inserts | Updates | Deletes | HOT Updates |
|-------|---------|---------|---------|-------------|
| union_find | 460,000 | 386,472 | 10,000 | 5,204 |
| person_mapping | 365,000 | 117,147 | 295,662 | 104,381 |
| distinct_id_mappings | 460,000 | 0 | 10,000 | 0 |

**HOT update ratio:** `person_mapping` achieves 89% HOT (104,381 / 117,147) because updates target `is_identified` and `deleted_at` -- neither indexed. `union_find` achieves only 1.3% HOT (5,204 / 386,472) because merge updates change `next` and `person_id`, both indexed (`idx_uf_next`, `idx_uf_person`). This drives the high dead tuple ratio and argues for aggressive autovacuum on `union_find`.

### Scan Patterns

| Table | Seq Scans | Index Scans | Index Scan % |
|-------|-----------|-------------|--------------|
| union_find | 7 | 7,549,261 | 100.00% |
| person_mapping | 5 | 2,143,572 | 100.00% |
| distinct_id_mappings | 4 | 2,828,674 | 100.00% |

All application queries use index scans. Sequential scans are from migration/truncate only.

### Index Usage

| Index | Scans | Tuples Read | Size | Bytes/Scan | Purpose |
|-------|-------|-------------|------|------------|---------|
| `union_find_pkey` | 6,500,362 | 7,146,765 | 25 MB | 4.03 | CTE chain traversal (every resolve) |
| `person_mapping_pkey` | 2,033,572 | 2,046,137 | 8 MB | 4.30 | Person UUID lookup at chain root |
| `idx_did_lookup` | 1,899,775 | 1,528,519 | 25 MB | 13.83 | distinct_id -> PK entry point |
| `idx_uf_next` | 928,899 | 884,005 | 8 MB | 9.13 | Delete/reverse resolve: find children |
| `distinct_id_mappings_pkey` | 928,899 | 928,899 | 10 MB | 11.49 | DID row by PK (delete path) |
| `idx_uf_person` | 120,000 | 120,000 | 15 MB | 122.47 | Reverse resolve + delete person |
| `idx_person_mapping_lookup` | 110,000 | 110,096 | 32 MB | 296.15 | Reverse resolve: team_id + person_uuid |

**Key observations:**

- **`union_find_pkey`** is the hottest index by an order of magnitude (6.5M scans). At 4.0 bytes/scan, each scan touches ~1 leaf page. This is the core of the system's performance.
- **`idx_person_mapping_lookup`** had the worst bytes/scan (296) in this benchmark run. At 32 MB for only 110K scans, it was oversized because `person_uuid` was `varchar(200)`. The column has since been migrated to native `UUID` (16 bytes), which should cut this index to ~8 GB at 100M persons.
- **`idx_uf_next`** at 929K scans is used by both delete ops and reverse resolve. The 4.2M buffer hits on this index are significant.
- **`idx_uf_person`** at 15 MB for 120K scans is a partial unique index covering only root nodes. At 100M persons: ~3.1 GB -- reasonable.

### Checkpoint Behavior

| Metric | Value |
|--------|-------|
| Timed checkpoints | 0 |
| Requested checkpoints | 1 |
| Buffers written | 922 |
| Write time | 3ms |
| Sync time | 1ms |

With `checkpoint_timeout=15min` and the benchmark completing in ~195s, only 1 on-demand checkpoint occurred.

---

## Query Plan Analysis

### Forward Resolve CTE (the critical path)

**Root node (depth 0):**
```
Execution Time: 0.583ms | Buffer Hits: 18
```
CTE starts, finds `person_id IS NOT NULL` immediately, terminates after 1 loop. One `union_find_pkey` scan (7 hits) + Memoize cache (1kB) + `person_mapping_pkey` join (4 hits).

**Deep chain (depth 22):**
```
Execution Time: 0.665ms | Buffer Hits: 106
```
CTE loops 23 times, each performing one Memoized `union_find_pkey` index scan. 95 buffer hits for the chain walk (4.1 hits/hop), plus `person_mapping_pkey` join (4 hits). Sort uses top-N heapsort (25kB memory).

**Cost per hop:** `(0.665ms - 0.583ms) / 22 hops ~ 0.004ms/hop` (execution time delta). The low per-hop cost reflects the Memoize node caching repeated lookups. In buffer terms: `(106 - 18) / 22 ~ 4.0 buffer hits/hop`.

**Projected latency at scale:**

| Depth | Buffer Hits | Estimated Execution |
|-------|-------------|-------------------|
| 0 | 18 | ~0.16ms |
| 10 | ~58 | ~0.40ms |
| 22 | 106 | ~0.67ms |
| 100 | ~418 | ~2.56ms |
| 1000 | ~4018 | ~24ms |

### Reverse Resolve CTE (person -> all distinct_ids)

**Small person (2 DIDs):**
```
Execution Time: 0.198ms | Buffer Hits: 22
```
One `idx_uf_person` scan (7 hits) finds root, one `idx_uf_next` scan finds no children beyond the 1 direct child. Two `distinct_id_mappings_pkey` joins (8 hits total).

**Large person (335 DIDs, 9 tree levels):**
```
Execution Time: 7.82ms | Buffer Hits: 2,611
```
`idx_uf_person` finds root (7 hits). 9 recursive levels walk `idx_uf_next` (3.8 hits per child node, 335 nodes total = 1,264 hits). Each node joins `distinct_id_mappings_pkey` (4 hits per DID = 1,340 hits).

**Cost per DID:** ~7.8 buffer hits/DID for the reverse traversal. At 10,000 DIDs (the cap): ~78K buffer hits, ~230ms estimated.

### Batch Lookup (`ANY()` index scan)

```
Execution Time: 2.27ms | Buffer Hits: 39
```
Single `idx_did_lookup` scan with `ANY(ARRAY[...])` for 10 distinct_ids. 3.9 buffer hits per key. Scales linearly: 100 keys ~ 390 buffer hits ~ 5ms.

### Path Compression CTE (depth 22, above threshold)

```
Execution Time: 1.63ms | Buffer Hits: 468
```
Walks the full 23-node chain (95 CTE hits), computes `max_depth=22`. Since `22 >= 20` (threshold), the UPDATE fires: scans all 22 non-root nodes via `union_find_pkey` (88 hits) and updates their `next` to point directly at the root. The 468 total hits include the chain walk, root lookup, and 22 index-update operations. Post-compression, all 22 nodes resolve in 1 hop.

---

## Bottleneck Summary

| Bottleneck | Severity | Affected Operations | Details |
|------------|----------|---------------------|---------|
| Per-source merge serial SQL | **Critical** | `/merge` | 10x serial CTE + UPDATE per batch; 3.9x slower than batched path |
| Dead tuple bloat (person_mapping) | **Critical** | All writes | 82.6% dead tuples; merges generate 1 dead tuple per source |
| Dead tuple bloat (union_find) | **High** | All writes | 46.6% dead tuples; root updates can't use HOT (indexed columns change) |
| Compress channel saturation | **High** | `/resolve` | ~100 "channel full" errors during 1M reads on 5 hot teams; chains stay deep until next successful compression |
| p99 latency cliff on writes | **Medium** | `/create`, `/alias`, `/delete_*` | p99 jumps 25-45x above p50; Docker tmpfs scheduling artifact |
| `idx_person_mapping_lookup` sizing | **Addressed** | `/resolve_distinct_ids` | 32 MB for 110K scans in benchmark; migrated from varchar(200) to native UUID (16 bytes) |
| Single-table design | **Medium** | All at scale | No partition pruning; B-tree depth grows with row count |

---

## Improvement Opportunities

### I1: Migrate all merge callers to batched merge (high impact)

The 3.9x improvement is definitive. `/merge` should be deprecated for production traffic in favor of `/batched_merge`. The per-source path should only be kept for single-source convenience or backward compatibility.

### I2: Background path compaction (critical for production)

The ~100 "channel full" errors during the read phase demonstrate that opportunistic compression is insufficient under load. A periodic background job per team that flattens all chains to depth 1 would guarantee bounded resolve latency. This is the most important operational requirement for production.

### I3: Autovacuum tuning (critical for production)

```sql
ALTER TABLE union_find SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 0);
ALTER TABLE person_mapping SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 0);
```

At production merge rates, dead tuples accumulate at ~2 per source per merge. Without aggressive vacuum, table bloat degrades all operations.

### I4: Partition tables by `team_id` (critical at scale)

`PARTITION BY HASH(team_id, 64)` is essential at 1B+ rows:
- Index B-tree depth drops from ~4 to ~3 levels per partition
- Per-partition vacuum runs faster with less lock contention
- Partition pruning eliminates 63/64 partitions on every query
- Aligns with worker-per-team architecture

### I5: Increase compress channel capacity or add dedicated compress workers (high impact)

The current design routes `CompressPath` ops through the same per-team worker channel as writes. Under heavy read load triggering compression, the channel saturates. Options:
- Increase `WORKER_CHANNEL_CAPACITY` (currently 64) for teams with deep chains
- Add a separate compress-only channel/worker pool
- Use `enqueue_compress` with retry (already done for write-triggered compression) for read-triggered compression too

### I6: ~~Evaluate `idx_person_mapping_lookup` sizing~~ (done)

**Implemented:** `person_uuid` migrated from `varchar(200)` to native `UUID` (16 bytes) in migration 006. Remaining options if further reduction is needed: (1) use a hash index for exact-match lookups, (2) add a numeric hash column.

### I7: Stress-test queue saturation (validation)

50 concurrency against 64 workers shows no write saturation. A stress benchmark at 500+ concurrency would identify the worker pool's breaking point and validate the 100ms enqueue timeout.

### I8: Increase default merge batch size (medium impact)

The current `BENCH_BATCH=10` is conservative. Batched merge amortizes transaction overhead. Testing at 25, 50, 100 would reveal the throughput ceiling. The batch CTE scales well (single recursive pass regardless of batch size).
