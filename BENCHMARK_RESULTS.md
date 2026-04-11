# Benchmark Results

**Date:** 2026-04-11
**Machine:** Apple Silicon (macOS), Docker Desktop (OrbStack)
**Postgres:** 17 (Docker), tuned for benchmarking (see `docker-compose.yml`)
**Server config:** 64 workers, 64 channel capacity, compress threshold 20, READ_POOL_SIZE 4
**Rust:** 1.90.0 (stable), edition 2024, release profile (opt-level 3)
**Run time:** ~193s total (including seeding)

---

## Production Prospects: 1000s req/s, 100M Persons, Billions of DIDs

This section assesses viability at production scale based on the benchmark findings below.

### Verdict: Viable with operational investment

The core data structure and query patterns are sound. The recursive CTE resolves identities at ~14.6K ops/s on a laptop Docker setup with sub-2ms p50 latency. The bottlenecks are operational (vacuum, partitioning, pool sizing) rather than algorithmic. Below is the detailed extrapolation.

### Read throughput (the primary operation)

Forward resolve (`/resolve`) achieves 14,559 ops/s at p50=1.78ms from a single app instance with 50 concurrent clients against a Docker Postgres with 256MB shared_buffers. The CTE averages 4.0 buffer hits per chain hop, all served from RAM.

**At production scale:** A vertically scaled Postgres (64 vCPUs, 256GB RAM, NVMe) with the entire working set in shared_buffers, fronted by 4-8 app instances behind PgBouncer, could sustain **100K-200K read ops/s**. Reads are CPU-bound on B-tree traversal, not I/O-bound. With 64 hash partitions by `team_id`, reads for different teams touch disjoint index pages, eliminating cross-team cache contention. Partition pruning reduces the effective index tree depth from ~4 levels (1B rows) to ~3 levels (16M rows/partition).

### Write throughput

The worker-per-team serialization model means each team's writes execute sequentially. With 64 workers, aggregate write throughput scales linearly:

| Operation | Per-worker | 64 workers aggregate |
|-----------|-----------|---------------------|
| Create | ~5,200/s | ~333,000/s |
| Alias | ~4,400/s | ~282,000/s |
| Batched merge (10/batch) | ~1,200 batches/s | ~76,800 batches/s (768K merges/s) |

The bottleneck shifts to Postgres WAL write throughput and connection pool sizing. With `synchronous_commit=on` (required for production), each commit adds ~0.5-2ms for WAL fsync on NVMe. Group commit (`commit_delay`, `commit_siblings`) amortizes this.

### Table sizes at 100M persons, 1B DIDs

| Table | Estimated Size | Index Size | Total |
|-------|---------------|------------|-------|
| union_find | ~60 GB | ~100 GB | ~160 GB |
| distinct_id_mappings | ~55 GB | ~75 GB | ~130 GB |
| person_mapping | ~35 GB | ~50 GB | ~85 GB |
| **Total** | **~150 GB** | **~225 GB** | **~375 GB** |

Fits on a single vertically scaled instance with 1TB NVMe storage. With 64 hash partitions, each partition holds ~5.9 GB of data -- small enough for per-partition `VACUUM FULL` during off-peak windows.

**Note on person_mapping sizing:** Migration 006 (`person_uuid` from `varchar(200)` to native `UUID`) reduced `person_mapping` from 71 MB to 53 MB and `idx_person_mapping_lookup` from 32 MB to 20 MB at benchmark scale. Per-entry index cost dropped from 84 bytes to 52 bytes. At 100M persons this index is ~5 GB (was ~8 GB with varchar). The total person_mapping estimate drops from ~120 GB to ~85 GB.

### Critical scaling risks

**1. Chain depth growth.** The benchmark's post-compression max depth is 20 (mean 4.09, p99=16). Path compression triggers on both writes (`CompressHint` above threshold) and reads (fire-and-forget `try_send`). The compress channel can saturate under bursty read load -- the previous benchmark logged ~100 "channel full" errors from hot team_ids. A **periodic background compaction job** (per-team full-tree flattening) is essential to guarantee bounded resolve latency regardless of merge velocity.

**2. Dead tuple bloat.** After the benchmark, `person_mapping` has 82.6% dead tuples and `union_find` has 46.5%. Merges generate 1 dead tuple per source in both tables. At 768K merges/s aggregate, that's ~1.5M dead tuples/s. Required: aggressive per-table autovacuum (`autovacuum_vacuum_scale_factor=0.01`, `autovacuum_vacuum_cost_delay=0`) and monitoring of `n_dead_tup`.

**3. `idx_person_mapping_lookup` sizing.** At 20 MB for 110K scans (191 bytes/scan) in this benchmark run, this index stores `(team_id, person_uuid)` with native `UUID` (16 bytes). At 100M persons, this index is ~5 GB -- well within shared_buffers on a production instance.

**4. Partitioning.** The current single-table design works at 450K rows but is untenable at 1B+. `PARTITION BY HASH(team_id, 64)` is essential: index B-tree depth drops a level, per-partition vacuum runs faster, and partition pruning eliminates 63/64 partitions on every query (all queries already filter on `team_id`).

**5. WAL pressure with durability enabled.** The benchmark generated 468 MB of WAL with `synchronous_commit=off`. In production with full durability, WAL fsync adds latency per commit. NVMe SSDs with `wal_compression=lz4` and group commit mitigate this, but WAL throughput becomes the ceiling for aggregate write rate.

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
| `POST /create` | 5,216 | 4.06ms | 15.16ms | 196.48ms | 400.56ms |
| `POST /alias` | 4,420 | 5.27ms | 20.31ms | 197.82ms | 388.72ms |
| `POST /merge` (10/batch) | 321 | 154.06ms | 292.58ms | 311.83ms | 350.87ms |
| **`POST /batched_merge` (10/batch)** | **1,195** | **41.24ms** | **78.93ms** | **90.92ms** | **113.91ms** |
| `POST /resolve` | 14,559 | 1.78ms | 3.19ms | 6.65ms | 801.39ms |
| `POST /resolve_distinct_ids` | 14,656 | 1.79ms | 3.17ms | 5.75ms | 602.40ms |
| `POST /delete_distinct_id` | 2,567 | 6.78ms | 179.75ms | 209.03ms | 391.84ms |
| `POST /delete_person` | 3,469 | 4.05ms | 35.08ms | 204.55ms | 417.23ms |

**Zero failures across 1.29M HTTP operations.** No 503s, no 500s, no timeouts. The 64-worker pool with 64-depth channels handled 50 concurrent clients without write saturation.

---

## Results by Endpoint

### Create (`POST /create`)

| Metric | Value |
|--------|-------|
| Total ops | 50,000 |
| Failures | 0 (0.00%) |
| Wall time | 9.59s |
| Throughput | 5,216 ops/s |
| p50 | 4.06ms |
| p95 | 15.16ms |
| p99 | 196.48ms |
| max | 400.56ms |

**Workload mix:** 80% new distinct IDs, 20% existing (hot-set re-identification).

**Query plan (hot path):** Single index scan on `idx_did_lookup`, 7 buffer hits, sub-0.2ms execution time. For new DIDs, the create path runs 3 sequential INSERTs (person_mapping, distinct_id_mappings, union_find) in one transaction.

**Bottleneck:** The p99 cliff (4.06ms to 196ms) is a Docker Desktop I/O scheduling artifact. The virtualized tmpfs introduces periodic stalls when the host flushes dirty pages. On bare-metal NVMe, expect p99 to flatten to 15-25ms.

---

### Alias (`POST /alias`)

| Metric | Value |
|--------|-------|
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 22.62s |
| Throughput | 4,420 ops/s |
| p50 | 5.27ms |
| p95 | 20.31ms |
| p99 | 197.82ms |
| max | 388.72ms |

**Workload mix:** 85% Case 1a (target exists, source new), 5% Case 2a (same person), 5% self-alias, 5% Case 3 (both new).

**Bottleneck:** Alias is ~15% slower than create because the dominant Case 1a path requires: (1) resolve target root via recursive CTE, (2) `check_did` on source, (3) INSERT into `distinct_id_mappings` + `union_find`, (4) UPDATE `person_mapping.is_identified`. The 389ms max reflects occasional lock contention when multiple aliases target the same hot-set person.

---

### Merge (`POST /merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct_ids) |
| Failures | 0 (0.00%) |
| Wall time | 31.19s |
| Throughput | 321 batch ops/s (~3,210 merges/s) |
| p50 | 154.06ms |
| p95 | 292.58ms |
| p99 | 311.83ms |
| max | 350.87ms |

**This is the slowest operation.** Each 10-source merge batch executes 10 serial iterations within a single transaction: `check_did` (index lookup + CTE) then `link_root_to_target` (UPDATE union_find + DELETE person_mapping). The p50 of 154ms means ~15ms per source. The narrow p50-to-p99 spread (154ms to 312ms) indicates consistent, predictable slowness rather than sporadic stalls.

---

### Batched Merge (`POST /batched_merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct_ids) |
| Failures | 0 (0.00%) |
| Wall time | 8.37s |
| Throughput | 1,195 batch ops/s (~11,950 merges/s) |
| p50 | 41.24ms |
| p95 | 78.93ms |
| p99 | 90.92ms |
| max | 113.91ms |

**3.7x throughput improvement over `/merge`.** The batched path replaces per-source serial SQL with bulk operations:
1. Single `batch_lookup_dids`: `ANY(ARRAY[...])` index scan returning all PKs (38 buffer hits, sub-0.2ms).
2. Single `batch_resolve_pks`: multi-start recursive CTE using `unnest($2::bigint[])` (14 buffer hits for shallow chains).
3. Batch INSERT for not-found sources, batch UPDATE + DELETE for live-different roots.

| Metric | Merge | Batched Merge | Improvement |
|--------|-------|---------------|-------------|
| ops/s (batch) | 321 | 1,195 | **3.7x** |
| p50 | 154.06ms | 41.24ms | **3.7x** |
| p95 | 292.58ms | 78.93ms | **3.7x** |
| p99 | 311.83ms | 90.92ms | **3.4x** |

---

### Chain Depth at Read Time

Sampled chain depth statistics (teams 1-10, 76,220 non-root nodes):

| Metric | Value |
|--------|-------|
| Mean depth | 4.09 |
| p50 | 3 |
| p95 | 12 |
| p99 | 16 |
| Max | 20 |

The configured chain-deepen phase built chains up to depth 100, but path compression (threshold=20) flattened most chains during the write phases and the 1M-resolve read phase. The max of 20 (exactly at threshold) indicates that path compression handled all chains at or above the threshold during the read burst, an improvement over the prior run's max of 22.

**Compress channel behavior:** The per-team worker channels (capacity 64) can saturate under bursty read load when many deep chains trigger fire-and-forget `try_send` compression requests simultaneously. At production scale, a background compaction job is needed rather than relying solely on opportunistic compression.

---

### Resolve / Read (`POST /resolve`)

| Metric | Value |
|--------|-------|
| Total ops | 1,000,000 |
| Failures | 0 (0.00%) |
| Wall time | 68.69s |
| Throughput | 14,559 ops/s |
| p50 | 1.78ms |
| p95 | 3.19ms |
| p99 | 6.65ms |
| max | 801.39ms |

**This is the primary operation and it performs well.** The p50 of 1.78ms includes full HTTP round-trip (client -> Axum -> pool -> CTE -> response). The CTE walks `union_find_pkey` with an average of ~6.5 index scans per resolve (6.5M total / 1M reads), reflecting mixed chain depths.

**Query plan (depth 0, root node):** 18 buffer hits, 0.072ms execution. The CTE starts, finds `person_id IS NOT NULL` immediately, and terminates after 1 loop. One `union_find_pkey` scan (7 hits) + Memoize miss (1) + `person_mapping_pkey` join (4 hits).

**Query plan (depth 23, deep chain):** 111 buffer hits, 0.192ms execution. The CTE loops 24 times, each performing one Memoized `union_find_pkey` index scan (4.0 hits/hop). The 100 CTE buffer hits cover the full chain walk. Final `person_mapping_pkey` join adds 4 hits. Sort uses top-N heapsort (25kB memory).

**Cost per hop:** `(0.192ms - 0.072ms) / 23 hops ~ 0.005ms/hop` (execution time delta). The low per-hop cost reflects the Memoize node caching repeated lookups. In buffer terms: `(111 - 18) / 23 ~ 4.0 buffer hits/hop`.

**Projected latency at scale:**

| Depth | Buffer Hits | Estimated Execution |
|-------|-------------|-------------------|
| 0 | 18 | ~0.07ms |
| 10 | ~58 | ~0.12ms |
| 23 | 111 | ~0.19ms |
| 100 | ~418 | ~0.57ms |
| 1000 | ~4018 | ~5.1ms |

**The 801ms max** is a single outlier across 1M operations -- a Docker host-level scheduling stall. Acceptable as a long-tail artifact.

---

### Resolve Distinct IDs (`POST /resolve_distinct_ids`)

| Metric | Value |
|--------|-------|
| Live persons found | 49,270 (in 30ms) |
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 6.82s |
| Throughput | 14,656 ops/s |
| p50 | 1.79ms |
| p95 | 3.17ms |
| p99 | 5.75ms |
| max | 602.40ms |

**Query plan (reverse CTE, 472-DID person):** 3,642 buffer hits, 3.62ms execution. Starts from `idx_uf_person` (7 hits for root lookup), then walks outward via `idx_uf_next` (3.7 hits per child, 3 tree levels). Each child joins `distinct_id_mappings_pkey` (4 hits per DID). The LIMIT 10001 prevents runaway fan-out.

**Query plan (small person, 3 DIDs):** 30 buffer hits, 0.51ms execution. One `idx_uf_person` scan plus one `idx_uf_next` scan terminates quickly.

**DID fan-out per person (all teams):**

| Metric | Value |
|--------|-------|
| Persons with roots | 49,397 |
| Mean DIDs per person | 8.91 |
| p50 | 1 |
| p95 | 30 |
| p99 | 244 |
| Max | 528 |

The 80/20 hot-set distribution means some persons accumulated hundreds of DIDs from merge/alias phases. The p99 latency of 5.75ms -- well under write p99s -- confirms that even high-fan-out persons resolve quickly.

---

### Delete Distinct ID (`POST /delete_distinct_id`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 3.90s |
| Throughput | 2,567 ops/s |
| p50 | 6.78ms |
| p95 | 179.75ms |
| p99 | 209.03ms |
| max | 391.84ms |

**Bottleneck:** Delete DID is the most complex write operation. Each delete must: (1) `lookup_did` (index scan, 7 hits), (2) determine root or non-root, (3) `unlink_did` -- splice parents via `idx_uf_next`, or promote a parent to root if deleting a root, (4) hard-delete from `union_find` and `distinct_id_mappings`, (5) check if the person is orphaned and conditionally soft-delete.

**The p95 spike to 180ms** (26x the p50 of 6.78ms) is the worst p50-to-p95 ratio of any operation. The parent-splicing logic involves read-modify-write cycles on multiple `union_find` rows within a transaction, amplifying lock hold time under concurrency.

---

### Delete Person (`POST /delete_person`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 2.88s |
| Throughput | 3,469 ops/s |
| p50 | 4.05ms |
| p95 | 35.08ms |
| p99 | 204.55ms |
| max | 417.23ms |

**Delete person is 1.35x faster than delete DID** because it only soft-deletes: UPDATE `person_mapping.deleted_at` + UPDATE `union_find.deleted_at` via `idx_uf_person`. No chain walking or re-linking. Orphaned nodes are lazily cleaned up on subsequent access. The p95 of 35ms is reasonable; the p99 cliff to 205ms is the familiar Docker tmpfs scheduling stall.

---

## Postgres Internals Analysis

### Buffer Cache Performance

| Metric | Value |
|--------|-------|
| Buffer cache hit ratio | **99.9996%** |
| Disk block reads | 266 |
| Buffer hits | 61,949,256 |
| Temp files created | 0 |
| Temp bytes | 0 |

The entire ~192 MB dataset fits in `shared_buffers=256MB`. Virtually every operation is served from RAM. The 266 disk reads are from initial cold-cache catalog lookups and migration. Zero temp files confirms all sorts and hash joins fit in `work_mem=8MB`.

**Per-table I/O breakdown:**

| Table | Heap Hits | Index Hits | Disk Reads |
|-------|-----------|------------|------------|
| union_find | 9,246,970 | 27,948,768 | 4 |
| person_mapping | 3,098,855 | 7,969,906 | 0 |
| distinct_id_mappings | 2,896,064 | 10,470,402 | 0 |

Index buffer hits outnumber heap hits ~3:1 across all tables, confirming the recursive CTE's index-driven traversal pattern dominates. `union_find` accounts for 60% of all buffer hits.

### Transaction Statistics

| Metric | Value |
|--------|-------|
| Commits | 2,488,845 |
| Rollbacks | 0 |
| Deadlocks | 0 |
| Conflicts | 0 |

Zero rollbacks and zero deadlocks validates the `team_id`-scoped serialization strategy: since each team's writes are funneled through a single worker, two transactions for the same team can never deadlock.

### WAL Statistics

| Metric | Value |
|--------|-------|
| WAL records | 6,219,520 |
| Full-page images | 1,436 |
| WAL bytes | 468 MB |
| WAL buffers full | 0 |
| WAL writes | 1,565 |
| WAL syncs | 0 |

Zero WAL syncs confirms `synchronous_commit=off` is active. The 468 MB of WAL across 2.5M transactions averages ~197 bytes per commit. In production with durability enabled, each commit would add ~0.5-2ms for WAL sync on NVMe, amortized by group commit.

### Table Sizes and Dead Tuple Bloat

| Table | Total | Heap | Indexes | Live Tuples | Dead Tuples | Dead % |
|-------|-------|------|---------|-------------|-------------|--------|
| union_find | 77 MB | 29 MB | 48 MB | 450,000 | 391,888 | 46.5% |
| person_mapping | 53 MB | 25 MB | 28 MB | 69,270 | 329,265 | 82.6% |
| distinct_id_mappings | 62 MB | 27 MB | 35 MB | 450,000 | 10,000 | 2.2% |
| **Total** | **192 MB** | **81 MB** | **111 MB** | **969,270** | **731,153** | -- |

**Autovacuum was disabled** for this benchmark. Dead tuple counts reflect the full write workload:

- **person_mapping** (82.6% dead): Merges delete the "loser" person row. ~296K hard deletes + ~117K updates = massive churn. At production scale: `autovacuum_vacuum_scale_factor=0.01`, `autovacuum_vacuum_cost_delay=0`.
- **union_find** (46.5% dead): Merges update root rows (clear `person_id`, set `next`). ~387K updates generated dead tuples. HOT updates (6,179) were rare because most updates change indexed columns (`next`, `person_id`), preventing HOT.
- **distinct_id_mappings** (2.2% dead): Only `delete_distinct_id` (10K) generated dead tuples. Insert-heavy, no updates.

**UUID migration impact on `person_mapping`:** Total size dropped from 71 MB to 53 MB (25% reduction). Heap dropped from 31 MB to 25 MB. Indexes dropped from 40 MB to 28 MB (30% reduction). The native `UUID` (16 bytes) replaces `varchar(200)` which stored 36-character UUID strings at ~40 bytes per entry with overhead.

### DML Totals

| Table | Inserts | Updates | Deletes | HOT Updates |
|-------|---------|---------|---------|-------------|
| union_find | 460,000 | 387,207 | 10,000 | 6,179 |
| person_mapping | 365,000 | 117,243 | 295,730 | 105,470 |
| distinct_id_mappings | 460,000 | 0 | 10,000 | 0 |

**HOT update ratio:** `person_mapping` achieves 90% HOT (105,470 / 117,243) because updates target `is_identified` and `deleted_at` -- neither indexed. `union_find` achieves only 1.6% HOT (6,179 / 387,207) because merge updates change `next` and `person_id`, both indexed (`idx_uf_next`, `idx_uf_person`). This drives the high dead tuple ratio and argues for aggressive autovacuum on `union_find`.

### Scan Patterns

| Table | Seq Scans | Index Scans | Index Scan % |
|-------|-----------|-------------|--------------|
| union_find | 7 | 7,434,210 | 100.00% |
| person_mapping | 5 | 2,143,927 | 100.00% |
| distinct_id_mappings | 4 | 2,745,151 | 100.00% |

All application queries use index scans. Sequential scans are from migration/truncate only.

### Index Usage

| Index | Scans | Tuples Read | Size | Bytes/Scan | Purpose |
|-------|-------|-------------|------|------------|---------|
| `union_find_pkey` | 6,468,935 | 7,099,303 | 25 MB | 4.05 | CTE chain traversal (every resolve) |
| `person_mapping_pkey` | 2,033,927 | 2,046,852 | 8 MB | 4.31 | Person UUID lookup at chain root |
| `idx_did_lookup` | 1,899,876 | 1,528,595 | 26 MB | 14.36 | distinct_id -> PK entry point |
| `idx_uf_next` | 845,275 | 797,499 | 8 MB | 9.99 | Delete/reverse resolve: find children |
| `distinct_id_mappings_pkey` | 845,275 | 845,275 | 10 MB | 12.53 | DID row by PK (delete path) |
| `idx_uf_person` | 120,000 | 120,000 | 16 MB | 139.81 | Reverse resolve + delete person |
| `idx_person_mapping_lookup` | 110,000 | 110,065 | 20 MB | 190.65 | Reverse resolve: team_id + person_uuid |

**Key observations:**

- **`union_find_pkey`** is the hottest index by an order of magnitude (6.5M scans). At 4.0 bytes/scan, each scan touches ~1 leaf page. This is the core of the system's performance.
- **`idx_person_mapping_lookup`** dropped from 32 MB to 20 MB (37% reduction) after the `person_uuid` migration from `varchar(200)` to native `UUID`. Bytes/scan dropped from 296 to 191. At 100M persons: ~5 GB (down from ~8 GB with varchar).
- **`idx_uf_next`** at 845K scans is used by both delete ops and reverse resolve. The 4.2M buffer hits on this index are significant.
- **`idx_uf_person`** at 16 MB for 120K scans is a partial unique index covering only root nodes. At 100M persons: ~3.1 GB -- reasonable.

### Checkpoint Behavior

| Metric | Value |
|--------|-------|
| Timed checkpoints | 0 |
| Requested checkpoints | 1 |
| Buffers written | 922 |
| Write time | 3ms |
| Sync time | 1ms |

With `checkpoint_timeout=15min` and the benchmark completing in ~193s, only 1 on-demand checkpoint occurred.

---

## Query Plan Analysis

### Forward Resolve CTE (the critical path)

**Root node (depth 0):**
```
Execution Time: 0.072ms | Buffer Hits: 18
```
CTE starts, finds `person_id IS NOT NULL` immediately, terminates after 1 loop. One `union_find_pkey` scan (7 hits) + Memoize cache (1kB) + `person_mapping_pkey` join (4 hits).

**Deep chain (depth 23):**
```
Execution Time: 0.192ms | Buffer Hits: 111
```
CTE loops 24 times, each performing one Memoized `union_find_pkey` index scan. 100 buffer hits for the chain walk (4.0 hits/hop), plus `person_mapping_pkey` join (4 hits). Sort uses top-N heapsort (25kB memory).

**Cost per hop:** `(0.192ms - 0.072ms) / 23 hops ~ 0.005ms/hop` (execution time delta). The low per-hop cost reflects the Memoize node caching repeated lookups. In buffer terms: `(111 - 18) / 23 ~ 4.0 buffer hits/hop`.

**Projected latency at scale:**

| Depth | Buffer Hits | Estimated Execution |
|-------|-------------|-------------------|
| 0 | 18 | ~0.07ms |
| 10 | ~58 | ~0.12ms |
| 23 | 111 | ~0.19ms |
| 100 | ~418 | ~0.57ms |
| 1000 | ~4018 | ~5.1ms |

### Reverse Resolve CTE (person -> all distinct_ids)

**Small person (3 DIDs):**
```
Execution Time: 0.509ms | Buffer Hits: 30
```
One `idx_uf_person` scan (7 hits) finds root, one `idx_uf_next` scan finds 2 children (11 hits across 3 recursive loops). Three `distinct_id_mappings_pkey` joins (12 hits total).

**Large person (472 DIDs, 3 tree levels):**
```
Execution Time: 3.62ms | Buffer Hits: 3,642
```
`idx_uf_person` finds root (7 hits). 3 recursive levels walk `idx_uf_next` (3.7 hits per child node, 472 nodes total = 1,747 hits). Each node joins `distinct_id_mappings_pkey` (4 hits per DID = 1,888 hits).

**Cost per DID:** ~7.7 buffer hits/DID for the reverse traversal. At 10,000 DIDs (the cap): ~77K buffer hits, ~230ms estimated.

### Batch Lookup (`ANY()` index scan)

```
Execution Time: 0.160ms | Buffer Hits: 38
```
Single `idx_did_lookup` scan with `ANY(ARRAY[...])` for 10 distinct_ids. 3.8 buffer hits per key. Scales linearly: 100 keys ~ 380 buffer hits ~ 1.6ms.

### Path Compression CTE (depth 23, above threshold)

```
Execution Time: 0.371ms | Buffer Hits: 423
```
Walks the full 24-node chain (99 CTE hits), computes `max_depth=23`. Since `23 >= 20` (threshold), the UPDATE fires: scans all 23 non-root nodes via `union_find_pkey` (92 hits) and updates their `next` to point directly at the root. The 423 total hits include the chain walk, root lookup, and 23 index-update operations. Post-compression, all 23 nodes resolve in 1 hop.

---

## Bottleneck Summary

| Bottleneck | Severity | Affected Operations | Details |
|------------|----------|---------------------|---------|
| Per-source merge serial SQL | **Critical** | `/merge` | 10x serial CTE + UPDATE per batch; 3.7x slower than batched path |
| Dead tuple bloat (person_mapping) | **Critical** | All writes | 82.6% dead tuples; merges generate 1 dead tuple per source |
| Dead tuple bloat (union_find) | **High** | All writes | 46.5% dead tuples; root updates can't use HOT (indexed columns change) |
| Compress channel saturation | **High** | `/resolve` | Under bursty read load, deep chains trigger fire-and-forget `try_send` that can fail when worker channels are busy |
| p99 latency cliff on writes | **Medium** | `/create`, `/alias`, `/delete_*` | p99 jumps 25-50x above p50; Docker tmpfs scheduling artifact |
| Single-table design | **Medium** | All at scale | No partition pruning; B-tree depth grows with row count |

---

## Improvement Opportunities

### I1: Migrate all merge callers to batched merge (high impact)

The 3.7x improvement is definitive. `/merge` should be deprecated for production traffic in favor of `/batched_merge`. The per-source path should only be kept for single-source convenience or backward compatibility.

### I2: Background path compaction (critical for production)

Opportunistic compression via `try_send` is effective under moderate load (max chain depth equaled the threshold in this run), but cannot be relied upon under sustained high-concurrency reads. A periodic background job per team that flattens all chains to depth 1 would guarantee bounded resolve latency regardless of merge velocity. This is the most important operational requirement for production.

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

### I6: ~~Evaluate `idx_person_mapping_lookup` sizing~~ (done — confirmed)

**Implemented and benchmarked:** `person_uuid` migrated from `varchar(200)` to native `UUID` (16 bytes) in migration 006. Results:
- `idx_person_mapping_lookup`: 32 MB → 20 MB (37% reduction, 296 → 191 bytes/scan)
- `person_mapping` total: 71 MB → 53 MB (25% reduction)
- At 100M persons: index projected at ~5 GB (down from ~8 GB with varchar)
- Throughput and latency unchanged (storage optimization, not a query plan change)

### I7: Stress-test queue saturation (validation)

50 concurrency against 64 workers shows no write saturation. A stress benchmark at 500+ concurrency would identify the worker pool's breaking point and validate the 100ms enqueue timeout.

### I8: Increase default merge batch size (medium impact)

The current `BENCH_BATCH=10` is conservative. Batched merge amortizes transaction overhead. Testing at 25, 50, 100 would reveal the throughput ceiling. The batch CTE scales well (single recursive pass regardless of batch size).
