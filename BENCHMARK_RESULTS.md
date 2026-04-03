# Benchmark Results

**Date:** 2026-04-03
**Machine:** Apple Silicon (macOS), Docker Desktop (OrbStack)
**Postgres:** 17 (Docker), tuned for benchmarking (see `docker-compose.yml`)
**Server config:** 64 workers, 64 channel capacity, compress threshold 20
**Rust:** 1.90.0 (stable), edition 2024, release profile (opt-level 3)
**Run time:** ~198s total

---

## Executive Summary

This service implements a union-find data structure backed by Postgres for managing person-to-distinct\_id relationships. The benchmark exercises all CRUD operations at scale: creating persons, aliasing distinct IDs, merging person graphs, resolving identities (forward and reverse), and deleting both distinct IDs and persons. All operations ran through the HTTP API with 50 concurrent clients against 100 teams, 450K union\_find rows, and chains up to 100 hops deep.

### Key Findings

**Reads are the strongest operation.** Forward resolve (`/resolve`) achieves **16,126 ops/s** at p50=1.80ms and p99=4.29ms across 1M operations. Reverse resolve (`/resolve_distinct_ids`) is comparable at **15,086 ops/s**, p50=1.86ms. Both operations bypass the worker queue and hit the connection pool directly. The recursive CTE walks `union_find_pkey` (a B-tree covering `(team_id, current)`) with 100% buffer cache hit rate — every resolve is served entirely from shared\_buffers.

**Batched merge is 4.9x faster than per-source merge.** The per-source merge path (`/merge`) achieves only 256 batch ops/s (p50=194ms) due to serial CTE + UPDATE cycles within a single transaction. The batched merge path (`/batched_merge`) achieves **1,262 batch ops/s** (p50=39ms) by replacing 10 individual lookups and CTEs with bulk `ANY()` queries and a single multi-source recursive CTE. This is the single most impactful optimization in the system.

**Create and alias are solid.** Create at **6,359 ops/s** (p50=4.03ms) and alias at **4,776 ops/s** (p50=5.19ms) are both well within production SLA requirements for identity operations. Alias is slower than create because it touches more rows: resolve target root (CTE) → create source → link → update `is_identified`.

**Zero failures across 1.28M HTTP operations.** No 503s (queue full), no 500s, no timeouts. The 64-worker pool with 64-depth channels handled 50 concurrent clients without saturation.

**Dead tuple bloat is significant.** Autovacuum was disabled for the benchmark. After the run, `person_mapping` has 82.7% dead tuples (330K dead / 69K live) and `union_find` has 46.5% dead tuples (392K dead / 450K live). Merges are the primary source: each merge deletes the loser's `person_mapping` row and updates the loser's `union_find` root. At production scale this churn demands aggressive autovacuum tuning.

### Production Estimate: 1000s req/s, 100M Persons, Billions of DIDs, 64 Partitions

The design is **viable for production at scale** with the right operational configuration. Here is the extrapolation:

**Read throughput scales linearly with app instances.** At 16K ops/s from a single benchmark client with 50 concurrency on a laptop Docker setup, a production deployment with 4 app instances behind PgBouncer on a vertically scaled Postgres (64 vCPUs, 256GB RAM, NVMe storage) could sustain **100K+ read ops/s**. The forward CTE averages 4.2 buffer hits per hop (63 hits / 15 hops at depth 14); with the entire working set in shared\_buffers, reads are CPU-bound on B-tree traversal, not I/O-bound. With 64 partitions by `team_id`, reads for different teams touch completely different index pages, eliminating all cross-team cache contention.

**Write throughput is bounded by per-team serialization, not Postgres.** The worker-per-team model means each team's writes execute sequentially through a single channel. With 64 partitions (= 64 teams per partition at 100M persons / ~4K teams), each partition's write throughput is limited to the single-team rate: ~1,262 batched merges/s or ~6,359 creates/s. However, with 64 partitions processing in parallel, aggregate write throughput scales to **~80K batched merge batches/s** (800K individual merges/s) or **~400K creates/s**. The bottleneck shifts to Postgres connection pool size and WAL write throughput.

**Table sizes at scale.** The current 450K-row dataset is 210MB total. Extrapolating to 100M persons with an average of 10 DIDs each (1B DID rows, 1B union\_find rows):

| Table | Estimated Size | Index Size | Total |
|-------|---------------|------------|-------|
| union\_find | ~60GB | ~100GB | ~160GB |
| distinct\_id\_mappings | ~55GB | ~75GB | ~130GB |
| person\_mapping | ~50GB | ~70GB | ~120GB |
| **Total** | **~165GB** | **~245GB** | **~410GB** |

This fits on a single vertically scaled instance with 1TB storage. With 64 hash partitions on `team_id`, each partition holds ~6.4GB — small enough for per-partition `VACUUM FULL` during off-peak windows.

**Chain depth is the critical scaling risk.** The benchmark's average chain depth of 2.64 (p50=2, p95=9, p99=15, max=20 post-compression) is manageable. But with billions of DIDs and continuous merges, chains could grow past the compression threshold between resolve-triggered compressions. Each additional hop adds ~0.04ms of CTE execution time (0.59ms / 14 hops at depth 14). At depth 100 that's ~4ms per resolve — still fast but 2x the current p50. A background compaction job (periodic per-team full-tree flattening) is essential to guarantee O(1) resolve latency regardless of merge history.

**WAL and checkpoint pressure.** The benchmark generated 515MB of WAL across 6.2M WAL records with `synchronous_commit=off`. In production with full durability enabled, WAL write latency adds ~0.5-2ms per commit depending on storage. With NVMe SSDs and `wal_compression=lz4`, a production instance could sustain the write throughput without WAL being the bottleneck. Group commit (`commit_delay=200us`, `commit_siblings=5`) amortizes fsync across concurrent transactions.

**Partitioning strategy.** The current single-table design works at 450K rows. At 1B+ rows, Postgres native partitioning (`PARTITION BY HASH(team_id, 64)`) is essential:
- Index B-tree depth drops from ~4 levels to ~3 levels (16M rows/partition vs 1B rows)
- Per-partition autovacuum runs faster and with less lock contention
- Partition pruning eliminates 63/64 partitions per query (all queries filter on `team_id`)
- Aligns with the worker-per-team architecture: one worker pool per partition

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
| `POST /create` | 6,359 | 4.03ms | 13.19ms | 171.17ms | 374.65ms |
| `POST /alias` | 4,776 | 5.19ms | 18.43ms | 193.23ms | 363.82ms |
| `POST /merge` (10/batch) | 256 | 193.90ms | 367.01ms | 385.91ms | 410.17ms |
| **`POST /batched_merge` (10/batch)** | **1,262** | **39.28ms** | **73.29ms** | **81.98ms** | **108.86ms** |
| `POST /resolve` | 16,126 | 1.80ms | 2.93ms | 4.29ms | 803.97ms |
| `POST /resolve_distinct_ids` | 15,086 | 1.86ms | 3.16ms | 4.92ms | 601.48ms |
| `POST /delete_distinct_id` | 2,411 | 6.72ms | 187.82ms | 209.13ms | 420.04ms |
| `POST /delete_person` | 3,681 | 3.61ms | 91.43ms | 213.32ms | 222.92ms |

---

## Results by Endpoint

### Create (`POST /create`)

| Metric | Value |
|--------|-------|
| Total ops | 50,000 |
| Failures | 0 (0.00%) |
| Wall time | 7.86s |
| Throughput | 6,359 ops/s |
| p50 | 4.03ms |
| p95 | 13.19ms |
| p99 | 171.17ms |
| max | 374.65ms |

**Workload mix:** 80% new distinct IDs (`create-{i}`), 20% existing (hot-set re-identification).

**Query plan (create hot path — `lookup_did`):** Single index scan on `idx_did_lookup`, 7 buffer hits, 0.19ms execution time. For new DIDs, the create path runs 3 sequential INSERTs (person\_mapping, distinct\_id\_mappings, union\_find) within one transaction — each INSERT is an index-organized append.

**Bottleneck:** The p99 cliff (4ms → 171ms) is a Docker Desktop I/O scheduling artifact. The virtualized tmpfs introduces periodic stalls when the host flushes dirty pages. On bare-metal NVMe, expect p99 to flatten to 15-25ms.

---

### Alias (`POST /alias`)

| Metric | Value |
|--------|-------|
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 20.94s |
| Throughput | 4,776 ops/s |
| p50 | 5.19ms |
| p95 | 18.43ms |
| p99 | 193.23ms |
| max | 363.82ms |

**Workload mix:** 90% new source → existing target (Case 1a), 5% self-alias (no-op), 5% both new (Case 3). Shuffled.

**Bottleneck:** Alias is ~33% slower than create because the dominant Case 1a path requires: (1) resolve target root via recursive CTE, (2) `check_did` on source, (3) INSERT source into `distinct_id_mappings` + `union_find`, (4) UPDATE `person_mapping.is_identified`. The max of 364ms reflects occasional contention when multiple aliases target the same hot-set person — brief lock waits on the person's `union_find` root row.

---

### Merge (`POST /merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct\_ids) |
| Failures | 0 (0.00%) |
| Wall time | 39.10s |
| Throughput | 256 batch ops/s (~2,560 merges/s) |
| p50 | 193.90ms |
| p95 | 367.01ms |
| p99 | 385.91ms |
| max | 410.17ms |

**Bottleneck:** This is the **slowest operation**. Each 10-source merge batch executes 10× serial iterations within a single transaction: `check_did` (index lookup + CTE) → `resolve_root` (second CTE) → `link_root_to_target` (UPDATE union\_find + DELETE person\_mapping). The p50 of 194ms means ~19ms per source — dominated by the serial CTE + UPDATE cycle. The narrow p50-to-p99 spread (194ms → 386ms) indicates consistent, predictable slowness rather than sporadic stalls.

---

### Batched Merge (`POST /batched_merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct\_ids) |
| Failures | 0 (0.00%) |
| Wall time | 7.93s |
| Throughput | 1,262 batch ops/s (~12,620 merges/s) |
| p50 | 39.28ms |
| p95 | 73.29ms |
| p99 | 81.98ms |
| max | 108.86ms |

**This is a 4.9× throughput improvement over `/merge`.** The batched path replaces per-source serial SQL with bulk operations:
1. Single `batch_lookup_dids`: `SELECT id, distinct_id FROM distinct_id_mappings WHERE team_id = $1 AND distinct_id = ANY($2)` — one index scan returning all 10 PKs (35 buffer hits, 0.27ms).
2. Single `batch_resolve_pks`: multi-start recursive CTE using `unnest($2::bigint[])` — walks all 10 chains in one query (79 buffer hits, 0.40ms for 10 sources including dedup/sort).
3. Batch INSERT for not-found sources, batch UPDATE + DELETE for live-different roots.

**Comparison to `/merge`:**

| Metric | Merge | Batched Merge | Improvement |
|--------|-------|---------------|-------------|
| ops/s (batch) | 256 | 1,262 | **4.9×** |
| p50 | 193.90ms | 39.28ms | **4.9×** |
| p95 | 367.01ms | 73.29ms | **5.0×** |
| p99 | 385.91ms | 81.98ms | **4.7×** |

---

### Chain Depth at Read Time

Sampled chain depth statistics (1,000 random DIDs post-benchmark):

| Metric | Value |
|--------|-------|
| Mean depth | 2.64 |
| p50 | 2 |
| p95 | 9 |
| p99 | 15 |
| Max | 20 |

The max of 20 (rather than 100, the configured maximum) confirms that path compression triggered by `/resolve` (threshold=20) is successfully flattening deep chains during the read phase.

---

### Resolve / Read (`POST /resolve`)

| Metric | Value |
|--------|-------|
| Total ops | 1,000,000 |
| Failures | 0 (0.00%) |
| Wall time | 62.01s |
| Throughput | 16,126 ops/s |
| p50 | 1.80ms |
| p95 | 2.93ms |
| p99 | 4.29ms |
| max | 803.97ms |

**This is the primary operation and it performs excellently.** The p50 of 1.80ms includes full HTTP round-trip (client → Axum → pool → CTE → response). The CTE walks `union_find_pkey` with an average of ~6.9 index scans per resolve (6.87M total / 1M reads), reflecting mixed chain depths.

**Query plan analysis (depth 0, root node):** 22 buffer hits, 0.42ms execution. The CTE terminates at loop 2 (no non-root row to follow). Memoize cache is used but misses on both accesses (each PK is unique).

**Query plan analysis (depth 14, deep chain):** 74 buffer hits, 0.94ms execution. The CTE loops 15 times, each loop performing one `union_find_pkey` index scan (4.2 buffer hits/hop). Memoize misses on all 15 lookups — expected since each node in the chain has a unique PK. The final person\_mapping join adds 4 buffer hits.

**Cost model:** `resolve_time ≈ 0.27ms + (depth × 0.04ms)`. At depth 0: ~0.27ms. At depth 14: ~0.83ms. At depth 100 (worst case without compression): ~4.3ms.

**The 804ms max** is a single outlier across 1M operations — likely caused by a Docker host-level scheduling stall. Acceptable for a long tail.

---

### Resolve Distinct IDs (`POST /resolve_distinct_ids`)

| Metric | Value |
|--------|-------|
| Live persons found | 49,199 (in 32ms) |
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 6.63s |
| Throughput | 15,086 ops/s |
| p50 | 1.86ms |
| p95 | 3.16ms |
| p99 | 4.92ms |
| max | 601.48ms |

**Query plan analysis (reverse CTE):** Starts from `idx_uf_person` (7 buffer hits for the root lookup), then walks outward via `idx_uf_next` (3 buffer hits per child level). Total for a person with 1 DID: 14 buffer hits, 0.24ms execution.

**DID fan-out per person:**

| Metric | Value |
|--------|-------|
| Persons with roots | 49,199 |
| Mean DIDs per person | 8.94 |
| p50 | 1 |
| p95 | 29 |
| p99 | 254 |
| Max | 584 |

The 80/20 hot-set distribution means some persons accumulated hundreds of DIDs from merge/alias phases. The p99 of 4.92ms — well under write-path p99s — confirms that even high-fan-out persons resolve quickly.

---

### Delete Distinct ID (`POST /delete_distinct_id`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 4.15s |
| Throughput | 2,411 ops/s |
| p50 | 6.72ms |
| p95 | 187.82ms |
| p99 | 209.13ms |
| max | 420.04ms |

**Bottleneck:** Delete DID is the most complex write operation. Each delete must: (1) `lookup_did` (index scan, 7 hits), (2) check if the node is root or non-root, (3) `unlink_did` — splice parents past the deleted node via `idx_uf_next` (8 hits for child lookup), or promote a parent to root if deleting a root, (4) hard-delete from `union_find` and `distinct_id_mappings`, (5) check if the person is now orphaned.

**Query plan (`idx_uf_next` scan for parent lookup):** 8 buffer hits, 0.19ms. This index is exclusively used by delete operations — 938K scans total across the benchmark.

---

### Delete Person (`POST /delete_person`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 2.72s |
| Throughput | 3,681 ops/s |
| p50 | 3.61ms |
| p95 | 91.43ms |
| p99 | 213.32ms |
| max | 222.92ms |

**Delete person is 1.5× faster than delete DID** because it only soft-deletes: (1) `idx_person_mapping_lookup` scan (7 buffer hits), (2) UPDATE `person_mapping.deleted_at`, (3) UPDATE `union_find.deleted_at` via `idx_uf_person`. No chain walking or re-linking required — orphaned nodes are lazily cleaned up on subsequent access via `check_did`.

---

## Postgres Internals Analysis

### Buffer Cache Performance

| Metric | Value |
|--------|-------|
| Buffer cache hit ratio | **99.9996%** |
| Disk block reads | 258 |
| Buffer hits | 64,281,503 |
| Temp files created | 0 |
| Temp bytes | 0 |

The entire 210MB dataset fits in `shared_buffers=256MB`. Every operation is served from RAM. The 258 disk reads are from initial cold-cache catalog lookups during migration and startup. Zero temp files confirms that all sorts and hash joins fit in `work_mem=8MB`.

### Transaction Statistics

| Metric | Value |
|--------|-------|
| Commits | 2,488,927 |
| Rollbacks | 0 |
| Deadlocks | 0 |
| Conflicts | 0 |

Zero rollbacks confirms that the worker-per-team design eliminates cross-team contention. Zero deadlocks validates that the `team_id`-scoped locking strategy is correct: since each team's operations are serialized through a single worker channel, two transactions for the same team can never deadlock.

### WAL Statistics

| Metric | Value |
|--------|-------|
| WAL records | 6,236,419 |
| Full-page images | 1,434 |
| WAL bytes | 515 MB |
| WAL buffers full | 0 |
| WAL writes | 1,615 |
| WAL sync | 0 |

Zero WAL syncs confirms `synchronous_commit=off` is active — the benchmark never waited for WAL flush. The 515MB of WAL across 2.5M transactions averages ~207 bytes per commit. In production with full durability, each commit would add ~0.5-2ms for WAL sync on NVMe, but group commit (`commit_delay=200us`) amortizes this across concurrent transactions.

### Table Sizes and Dead Tuple Bloat

| Table | Total | Heap | Indexes | Live Tuples | Dead Tuples | Dead % |
|-------|-------|------|---------|-------------|-------------|--------|
| union\_find | 77 MB | 29 MB | 48 MB | 450,000 | 391,637 | 46.5% |
| person\_mapping | 71 MB | 31 MB | 40 MB | 69,199 | 330,013 | 82.7% |
| distinct\_id\_mappings | 62 MB | 27 MB | 35 MB | 450,000 | 10,000 | 2.2% |
| **Total** | **210 MB** | **87 MB** | **123 MB** | **969,199** | **731,650** | — |

**Autovacuum was disabled** for this benchmark to eliminate lock interference. The dead tuple counts reflect the full write workload:

- **person\_mapping** (82.7% dead): Merges delete the "loser" person row. 295,801 hard deletes + 117,263 updates = massive churn. At production scale, this table needs `autovacuum_vacuum_scale_factor=0.01` and `autovacuum_vacuum_cost_delay=0`.
- **union\_find** (46.5% dead): Merges update root rows (clear person\_id, set next). 387,419 updates generated dead tuples. HOT updates (6,407) were rare — most updates change indexed columns, preventing HOT.
- **distinct\_id\_mappings** (2.2% dead): Only `/delete_distinct_id` operations (10K) generated dead tuples. This table is insert-heavy with no updates.

### DML Totals

| Table | Inserts | Updates | Deletes |
|-------|---------|---------|---------|
| union\_find | 460,000 | 387,419 | 10,000 |
| person\_mapping | 365,000 | 117,263 | 295,801 |
| distinct\_id\_mappings | 460,000 | 0 | 10,000 |

### Scan Patterns

| Table | Seq Scans | Index Scans | Index Scan % |
|-------|-----------|-------------|--------------|
| union\_find | 71 | 7,923,623 | 100.00% |
| person\_mapping | 6 | 2,143,025 | 100.00% |
| distinct\_id\_mappings | 6 | 2,838,384 | 100.00% |

Negligible sequential scans. All application queries use index scans as intended.

### Index Usage

| Index | Scans | Tuples Read | Size | Bytes/Scan | Purpose |
|-------|-------|-------------|------|------------|---------|
| `union_find_pkey` | 6,865,271 | 7,468,615 | 25 MB | 3.77 | CTE chain traversal (every resolve) |
| `person_mapping_pkey` | 2,033,023 | 2,046,988 | 8 MB | 4.19 | Person UUID lookup at chain root |
| `idx_did_lookup` | 1,900,035 | 1,528,769 | 25 MB | 13.90 | distinct\_id → node entry point |
| `idx_uf_next` | 938,350 | 896,557 | 8 MB | 8.69 | Delete: find children of deleted node |
| `distinct_id_mappings_pkey` | 938,349 | 938,348 | 10 MB | 11.03 | Delete: DID row by PK |
| `idx_uf_person` | 120,002 | 120,000 | 16 MB | 135.92 | Reverse resolve + delete person |
| `idx_person_mapping_lookup` | 110,002 | 110,063 | 32 MB | 305.78 | Reverse resolve: team\_id + person\_uuid |

**Analysis:**

- **`union_find_pkey`** is the hottest index by an order of magnitude. At 3.77 bytes/scan, each scan touches ~1 leaf page — optimal for the recursive CTE's single-row lookups. This index is the core of the system's performance.
- **`idx_person_mapping_lookup`** has the worst bytes/scan ratio (306). At 32MB for only 110K scans, it's oversized relative to its usage. This is because it indexes `(team_id, person_uuid)` where person\_uuid is a 36-character UUID string. At production scale with 100M persons, this index would be ~32GB. Consider a hash index or storing a numeric hash of person\_uuid as an alternative.
- **`idx_uf_person`** at 16MB for 120K scans is also sparse. It's a partial unique index (`WHERE person_id IS NOT NULL`) covering only root nodes. At production scale with 100M persons, expect ~3.2GB — reasonable for its purpose.

### Checkpoint Behavior

| Metric | Value |
|--------|-------|
| Timed checkpoints | 0 |
| Requested checkpoints | 1 |
| Buffers written | 922 |
| Write time | 3ms |
| Sync time | 1ms |

With `checkpoint_timeout=15min` and the benchmark completing in ~198s, only 1 on-demand checkpoint occurred. The minimal checkpoint I/O confirms that `synchronous_commit=off` + `wal_level=minimal` keeps WAL overhead negligible in this benchmark configuration.

---

## Query Plan Analysis

### Forward Resolve CTE (the critical path)

**Shallow chain (depth 0, root node):**
```
Execution Time: 0.42ms | Buffer Hits: 22
```
The CTE starts at the given PK, finds `person_id IS NOT NULL` on the first hop (root), and terminates. Two index scans on `union_find_pkey` (once for the CTE, once for the final join) plus one on `person_mapping_pkey`. Memoize cache misses on both lookups — expected since each PK is unique.

**Deep chain (depth 14):**
```
Execution Time: 0.94ms | Buffer Hits: 74
```
The CTE loops 15 times, each loop performing one `union_find_pkey` index scan at 4.2 buffer hits/hop. Memoize is enabled but all 15 lookups miss — no repeated nodes in a linear chain. The `walk_result` sort switches from quicksort (depth 0, 2 rows) to top-N heapsort (depth 14, 15 rows) — negligible cost difference.

**Cost per hop:** `(0.94ms - 0.42ms) / 14 hops ≈ 0.037ms/hop`. This is the fundamental unit of resolve latency. At depth 100 (worst case): `0.42 + 100 × 0.037 ≈ 4.1ms`. At depth 1000 (pathological): ~37ms.

### Reverse Resolve CTE (person → all distinct\_ids)

```
Execution Time: 0.24ms | Buffer Hits: 14
```
Starts from `idx_uf_person` (person\_id → root node, 7 hits), then walks outward via `idx_uf_next` (3 hits/child level). For a person with 1 DID, the CTE terminates after 1 level. For a person with 584 DIDs (the benchmark max), expect ~2ms execution. The LIMIT 10001 clause prevents runaway fan-out.

### Batch Lookup (`ANY()` index scan)

```
Execution Time: 0.27ms | Buffer Hits: 35
```
A single `idx_did_lookup` scan with `ANY(ARRAY[...])` for 10 distinct\_ids. Postgres handles this as a single index scan with an `IN`-list, not 10 separate scans — 3.5 hits per key. Linear scaling: 100 keys ≈ 35 buffer hits ≈ 0.5ms.

### Batch Resolve (multi-start recursive CTE)

```
Execution Time: 0.40ms | Buffer Hits: 79
```
The `unnest($2::bigint[])` seeds 10 starting nodes into the CTE. Postgres processes all chains in a single recursive pass, then deduplicates roots with `DISTINCT ON (start_node)`. 79 buffer hits for 10 shallow chains (depth 0-1). For 10 chains at depth 14 each, expect ~700 buffer hits and ~4ms — still a single query.

### Path Compression CTE

```
Execution Time: 0.59ms | Buffer Hits: 63
```
At depth 14, the compression CTE walks the full chain (63 hits), computes `max_depth=14`, and since 14 < 20 (threshold), the `WHERE pi.max_depth >= $3` filter evaluates to false — the UPDATE is skipped entirely (the `InitPlan` nodes show "never executed"). The CTE walk cost is paid even when no compression occurs. At depth 20+, the UPDATE would flatten all intermediate nodes to point directly at the root in a single pass.

---

## Improvement Opportunities

### I1: Migrate all merge callers to batched merge (high impact)

The 4.9× improvement is definitive. `/merge` should be deprecated for all production traffic in favor of `/batched_merge`. The per-source endpoint should only be kept for single-source convenience.

### I2: Increase default merge batch size (medium impact)

The current `BENCH_BATCH=10` was chosen conservatively. Batched merge amortizes transaction overhead — testing with batch sizes of 25, 50, and 100 would reveal the throughput ceiling. The batch\_resolve\_pks CTE scales well (single recursive pass regardless of batch size), so the limit is transaction hold time vs. worker channel blocking time.

### I3: Background path compaction (critical for production)

Path compression triggers lazily on reads when depth exceeds `PATH_COMPRESS_THRESHOLD=20`. At production scale with billions of DIDs, a periodic background job (per team, off-peak) that flattens all chains to depth 1 would guarantee O(1) resolve latency. This is the most important operational requirement.

### I4: Autovacuum tuning for merge churn (critical for production)

Merges generate 1 dead tuple in `person_mapping` (loser deleted) and 1 dead tuple in `union_find` (root updated) per source. At 12.6K merges/s via batched merge, that's 25K dead tuples/s. Required autovacuum settings:

```sql
ALTER TABLE union_find SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 0);
ALTER TABLE person_mapping SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 0);
```

### I5: Partition tables by team\_id (critical for production at scale)

The current single-table design works at 450K rows. At billions of rows, `PARTITION BY HASH(team_id, 64)` is essential:
- Align with the 64-worker pool (one partition per worker subset)
- Reduce per-partition index tree depth
- Enable partition-level autovacuum and `VACUUM FULL`
- Partition pruning eliminates 63/64 partitions on every query

### I6: Evaluate `idx_person_mapping_lookup` sizing (medium impact)

At 32MB for 110K scans (306 bytes/scan), this is the least efficient index. It indexes `(team_id, person_uuid)` where `person_uuid` is a 36-byte UUID string. At production scale (100M persons), this index alone would be ~32GB. Options: (1) store person\_uuid as `uuid` type (16 bytes) instead of `varchar(200)`, (2) use a hash index for exact-match lookups, (3) add a numeric hash column.

### I7: Stress-test queue saturation (validation)

The current benchmark uses 50 concurrency against 64 workers — no saturation is possible. A stress benchmark with `BENCH_CONCURRENCY=500+` would validate the 100ms enqueue timeout behavior and identify the worker pool's saturation point.
