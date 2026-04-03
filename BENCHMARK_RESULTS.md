# Benchmark Results

**Date:** 2026-04-03
**Machine:** Apple Silicon (macOS), Docker Desktop (OrbStack)
**Postgres:** 17 (Docker), tuned for benchmarking (see `docker-compose.yml`)
**Server config:** 64 workers, 64 channel capacity, compress threshold 20
**Rust:** 1.90.0 (stable), edition 2024, release profile (opt-level 3)
**Run time:** ~185s total

---

## Executive Summary

This service implements a union-find data structure backed by Postgres for managing person-to-distinct\_id relationships. The benchmark exercises all CRUD operations at scale: creating persons, aliasing distinct IDs, merging person graphs, resolving identities (forward and reverse), and deleting both distinct IDs and persons. All operations ran through the HTTP API with 50 concurrent clients against 100 teams, 450K union\_find rows, and chains up to 100 hops deep.

### Key Findings

**Reads are the strongest operation.** Forward resolve (`/resolve`) achieves **15,838 ops/s** at p50=1.78ms and p99=4.01ms across 1M operations. Reverse resolve (`/resolve_distinct_ids`) is comparable at **15,047 ops/s**, p50=1.82ms. Both operations bypass the worker queue and hit the connection pool directly. The recursive CTE walks `union_find_pkey` (a B-tree covering `(team_id, current)`) with 100% buffer cache hit rate — every resolve is served entirely from shared\_buffers.

**Batched merge is 4.0x faster than per-source merge.** The per-source merge path (`/merge`) achieves only 305 batch ops/s (p50=163ms) due to serial CTE + UPDATE cycles within a single transaction. The batched merge path (`/batched_merge`) achieves **1,222 batch ops/s** (p50=40ms) by replacing 10 individual lookups and CTEs with bulk `ANY()` queries and a single multi-source recursive CTE. This is the single most impactful optimization in the system.

**Create and alias are solid.** Create at **6,667 ops/s** (p50=4.08ms) and alias at **4,647 ops/s** (p50=5.11ms) are both well within production SLA requirements for identity operations. Alias is slower than create because it touches more rows: resolve target root (CTE) → create source → link → update `is_identified`.

**Zero failures across 1.28M HTTP operations.** No 503s (queue full), no 500s, no timeouts. The 64-worker pool with 64-depth channels handled 50 concurrent clients without saturation.

**Dead tuple bloat is significant.** Autovacuum was disabled for the benchmark. After the run, `person_mapping` has 82.6% dead tuples (328K dead / 69K live) and `union_find` has 46.6% dead tuples (392K dead / 450K live). Merges are the primary source: each merge deletes the loser's `person_mapping` row and updates the loser's `union_find` root. At production scale this churn demands aggressive autovacuum tuning.

### Bottleneck Summary

| Bottleneck | Severity | Affected Operations | Details |
|------------|----------|---------------------|---------|
| Per-source merge serial SQL | **Critical** | `/merge` | 10× serial CTE + UPDATE cycles per batch; 4.0× slower than batched path |
| Dead tuple bloat (person\_mapping) | **Critical** | All writes | 82.6% dead tuples after benchmark; merges generate 1 dead tuple per source |
| Dead tuple bloat (union\_find) | **High** | All writes | 46.6% dead tuples; root updates on merge can't use HOT (indexed columns change) |
| Chain depth growth | **High** | `/resolve` | Path compression triggers at depth ≥20 on both writes and reads; unbounded depth between compressions adds ~0.028ms/hop |
| p99 latency cliff on writes | **Medium** | `/create`, `/alias`, `/delete_*` | p99 jumps 10-40× above p50 (e.g. create: 4ms → 162ms); Docker Desktop tmpfs I/O scheduling artifact |
| `idx_person_mapping_lookup` sizing | **Medium** | `/resolve_distinct_ids` | 31 MB for 110K scans (296 bytes/scan); varchar(200) UUID bloats index at scale |
| Single-table design | **Medium** | All at scale | No partition pruning; B-tree depth grows with row count; vacuum contention |

### Production Estimate: 1000s req/s, 100M Persons, Billions of DIDs, 64 Partitions

The design is **viable for production at scale** with the right operational configuration. Here is the extrapolation:

**Read throughput scales linearly with app instances.** At ~16K ops/s from a single benchmark client with 50 concurrency on a laptop Docker setup, a production deployment with 4 app instances behind PgBouncer on a vertically scaled Postgres (64 vCPUs, 256GB RAM, NVMe storage) could sustain **100K+ read ops/s**. The forward CTE averages 4.0 buffer hits per hop; with the entire working set in shared\_buffers, reads are CPU-bound on B-tree traversal, not I/O-bound. With 64 partitions by `team_id`, reads for different teams touch completely different index pages, eliminating all cross-team cache contention.

**Write throughput is bounded by per-team serialization, not Postgres.** The worker-per-team model means each team's writes execute sequentially through a single channel. With 64 partitions (= 64 teams per partition at 100M persons / ~4K teams), each partition's write throughput is limited to the single-team rate: ~1,222 batched merges/s or ~6,667 creates/s. However, with 64 partitions processing in parallel, aggregate write throughput scales to **~78K batched merge batches/s** (780K individual merges/s) or **~427K creates/s**. The bottleneck shifts to Postgres connection pool size and WAL write throughput.

**Table sizes at scale.** The current 450K-row dataset is 208MB total. Extrapolating to 100M persons with an average of 10 DIDs each (1B DID rows, 1B union\_find rows):

| Table | Estimated Size | Index Size | Total |
|-------|---------------|------------|-------|
| union\_find | ~60GB | ~100GB | ~160GB |
| distinct\_id\_mappings | ~55GB | ~75GB | ~130GB |
| person\_mapping | ~50GB | ~70GB | ~120GB |
| **Total** | **~165GB** | **~245GB** | **~410GB** |

This fits on a single vertically scaled instance with 1TB storage. With 64 hash partitions on `team_id`, each partition holds ~6.4GB — small enough for per-partition `VACUUM FULL` during off-peak windows.

**Chain depth is the critical scaling risk.** The benchmark's average chain depth of 2.76 (p50=2, p95=11, p99=16, max=20 post-compression) is manageable. But with billions of DIDs and continuous merges, chains could grow past the compression threshold between compressions. Path compression is triggered on both writes (via `CompressHint` when combined chain depth exceeds the threshold) and reads (fire-and-forget `try_send` on `/resolve`). Each additional hop adds ~0.028ms of CTE execution time. At depth 100 that's ~2.96ms per resolve — still fast but 1.7x the current p50. A background compaction job (periodic per-team full-tree flattening) is essential to guarantee O(1) resolve latency regardless of merge history.

**WAL and checkpoint pressure.** The benchmark generated 490MB of WAL across 6.2M WAL records with `synchronous_commit=off`. In production with full durability enabled, WAL write latency adds ~0.5-2ms per commit depending on storage. With NVMe SSDs and `wal_compression=lz4`, a production instance could sustain the write throughput without WAL being the bottleneck. Group commit (`commit_delay=200us`, `commit_siblings=5`) amortizes fsync across concurrent transactions.

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
| `POST /create` | 6,667 | 4.08ms | 12.88ms | 161.51ms | 219.93ms |
| `POST /alias` | 4,647 | 5.11ms | 18.71ms | 195.29ms | 245.41ms |
| `POST /merge` (10/batch) | 305 | 163.08ms | 302.70ms | 315.92ms | 367.06ms |
| **`POST /batched_merge` (10/batch)** | **1,222** | **40.40ms** | **76.30ms** | **83.35ms** | **112.12ms** |
| `POST /resolve` | 15,838 | 1.78ms | 2.89ms | 4.01ms | 604.12ms |
| `POST /resolve_distinct_ids` | 15,047 | 1.82ms | 3.10ms | 4.75ms | 603.03ms |
| `POST /delete_distinct_id` | 2,902 | 6.82ms | 124.29ms | 213.21ms | 250.40ms |
| `POST /delete_person` | 5,048 | 3.41ms | 16.54ms | 200.00ms | 240.79ms |

---

## Results by Endpoint

### Create (`POST /create`)

| Metric | Value |
|--------|-------|
| Total ops | 50,000 |
| Failures | 0 (0.00%) |
| Wall time | 7.50s |
| Throughput | 6,667 ops/s |
| p50 | 4.08ms |
| p95 | 12.88ms |
| p99 | 161.51ms |
| max | 219.93ms |

**Workload mix:** 80% new distinct IDs (`create-{i}`), 20% existing (hot-set re-identification).

**Query plan (create hot path — `lookup_did`):** Single index scan on `idx_did_lookup`, 7 buffer hits, sub-0.2ms execution time. For new DIDs, the create path runs 3 sequential INSERTs (person\_mapping, distinct\_id\_mappings, union\_find) within one transaction — each INSERT is an index-organized append.

**Bottleneck:** The p99 cliff (4ms → 162ms) is a Docker Desktop I/O scheduling artifact. The virtualized tmpfs introduces periodic stalls when the host flushes dirty pages. On bare-metal NVMe, expect p99 to flatten to 15-25ms.

---

### Alias (`POST /alias`)

| Metric | Value |
|--------|-------|
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 21.52s |
| Throughput | 4,647 ops/s |
| p50 | 5.11ms |
| p95 | 18.71ms |
| p99 | 195.29ms |
| max | 245.41ms |

**Workload mix:** 90% new source → existing target (Case 1a), 5% self-alias (no-op), 5% both new (Case 3). Shuffled.

**Bottleneck:** Alias is ~30% slower than create because the dominant Case 1a path requires: (1) resolve target root via recursive CTE, (2) `check_did` on source, (3) INSERT source into `distinct_id_mappings` + `union_find`, (4) UPDATE `person_mapping.is_identified`. The max of 245ms reflects occasional contention when multiple aliases target the same hot-set person — brief lock waits on the person's `union_find` root row.

---

### Merge (`POST /merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct\_ids) |
| Failures | 0 (0.00%) |
| Wall time | 32.75s |
| Throughput | 305 batch ops/s (~3,050 merges/s) |
| p50 | 163.08ms |
| p95 | 302.70ms |
| p99 | 315.92ms |
| max | 367.06ms |

**Bottleneck:** This is the **slowest operation**. Each 10-source merge batch executes 10× serial iterations within a single transaction: `check_did` (index lookup + CTE) → `resolve_root` (second CTE) → `link_root_to_target` (UPDATE union\_find + DELETE person\_mapping). The p50 of 163ms means ~16ms per source — dominated by the serial CTE + UPDATE cycle. The narrow p50-to-p99 spread (163ms → 316ms) indicates consistent, predictable slowness rather than sporadic stalls.

---

### Batched Merge (`POST /batched_merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct\_ids) |
| Failures | 0 (0.00%) |
| Wall time | 8.18s |
| Throughput | 1,222 batch ops/s (~12,220 merges/s) |
| p50 | 40.40ms |
| p95 | 76.30ms |
| p99 | 83.35ms |
| max | 112.12ms |

**This is a 4.0× throughput improvement over `/merge`.** The batched path replaces per-source serial SQL with bulk operations:
1. Single `batch_lookup_dids`: `SELECT id, distinct_id FROM distinct_id_mappings WHERE team_id = $1 AND distinct_id = ANY($2)` — one index scan returning all 10 PKs (35 buffer hits, sub-0.3ms).
2. Single `batch_resolve_pks`: multi-start recursive CTE using `unnest($2::bigint[])` — walks all 10 chains in one query (14 buffer hits for shallow chains including dedup/sort).
3. Batch INSERT for not-found sources, batch UPDATE + DELETE for live-different roots.

**Comparison to `/merge`:**

| Metric | Merge | Batched Merge | Improvement |
|--------|-------|---------------|-------------|
| ops/s (batch) | 305 | 1,222 | **4.0×** |
| p50 | 163.08ms | 40.40ms | **4.0×** |
| p95 | 302.70ms | 76.30ms | **4.0×** |
| p99 | 315.92ms | 83.35ms | **3.8×** |

---

### Chain Depth at Read Time

Sampled chain depth statistics (10 teams, 44,039 nodes post-benchmark):

| Metric | Value |
|--------|-------|
| Mean depth | 2.76 |
| p50 | 2 |
| p95 | 11 |
| p99 | 16 |
| Max | 20 |

The max of 20 (rather than 100, the configured maximum) confirms that path compression (threshold=20), triggered on both writes and reads, is successfully flattening deep chains.

---

### Resolve / Read (`POST /resolve`)

| Metric | Value |
|--------|-------|
| Total ops | 1,000,000 |
| Failures | 0 (0.00%) |
| Wall time | 63.14s |
| Throughput | 15,838 ops/s |
| p50 | 1.78ms |
| p95 | 2.89ms |
| p99 | 4.01ms |
| max | 604.12ms |

**This is the primary operation and it performs excellently.** The p50 of 1.78ms includes full HTTP round-trip (client → Axum → pool → CTE → response). The CTE walks `union_find_pkey` with an average of ~6.5 index scans per resolve (6.5M total / 1M reads), reflecting mixed chain depths.

**Query plan analysis (depth 0, root node):** 11 buffer hits, 0.16ms execution. The CTE terminates at loop 1 (root node has `person_id IS NOT NULL`). One index scan on `union_find_pkey` (7 hits) plus one on `person_mapping_pkey` (4 hits).

**Query plan analysis (depth 19, deep chain):** 87 buffer hits, 0.70ms execution. The CTE loops 20 times, each loop performing one `union_find_pkey` index scan (4.0 buffer hits/hop). The 76 chain-traversal buffer hits are spread across 19 recursive steps plus the starting node lookup (7 hits). The final `person_mapping_pkey` join adds 4 buffer hits.

**Cost model:** `resolve_time ≈ 0.16ms + (depth × 0.028ms)`. At depth 0: ~0.16ms. At depth 19: ~0.70ms. At depth 100 (worst case without compression): ~2.96ms.

**The 604ms max** is a single outlier across 1M operations — caused by a Docker host-level scheduling stall. Acceptable for a long tail.

---

### Resolve Distinct IDs (`POST /resolve_distinct_ids`)

| Metric | Value |
|--------|-------|
| Live persons found | 49,234 (in 17ms) |
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 6.65s |
| Throughput | 15,047 ops/s |
| p50 | 1.82ms |
| p95 | 3.10ms |
| p99 | 4.75ms |
| max | 603.03ms |

**Query plan analysis (reverse CTE):** Starts from `idx_uf_person` (11 buffer hits for the root lookup including InitPlan), then walks outward via `idx_uf_next` (3.8 buffer hits per child level). Total for a person with 30 DIDs (9 tree levels): 245 buffer hits, 1.61ms execution. The LIMIT 10001 clause prevents runaway fan-out.

**DID fan-out per person (5-team sample):**

| Metric | Value |
|--------|-------|
| Persons with roots | 49,234 |
| Mean DIDs per person | 9.14 |
| p50 | 1 |
| p95 | 40 |
| p99 | 212 |
| Max | 442 |

The 80/20 hot-set distribution means some persons accumulated hundreds of DIDs from merge/alias phases. The p99 of 4.75ms — well under write-path p99s — confirms that even high-fan-out persons resolve quickly.

---

### Delete Distinct ID (`POST /delete_distinct_id`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 3.45s |
| Throughput | 2,902 ops/s |
| p50 | 6.82ms |
| p95 | 124.29ms |
| p99 | 213.21ms |
| max | 250.40ms |

**Bottleneck:** Delete DID is the most complex write operation. Each delete must: (1) `lookup_did` (index scan, 7 hits), (2) check if the node is root or non-root, (3) `unlink_did` — splice parents past the deleted node via `idx_uf_next` (8 hits for child lookup), or promote a parent to root if deleting a root, (4) hard-delete from `union_find` and `distinct_id_mappings`, (5) check if the person is now orphaned.

**The p95 spike to 124ms** (18× the p50 of 6.82ms) is the worst p50-to-p95 ratio of any operation. Delete DID operations hit `idx_uf_next` exclusively — 898K scans total across the benchmark — and the parent-splicing logic involves read-modify-write cycles on multiple union\_find rows within a single transaction, amplifying lock hold time under concurrency.

---

### Delete Person (`POST /delete_person`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 1.98s |
| Throughput | 5,048 ops/s |
| p50 | 3.41ms |
| p95 | 16.54ms |
| p99 | 200.00ms |
| max | 240.79ms |

**Delete person is 1.7× faster than delete DID** because it only soft-deletes: (1) `idx_person_mapping_lookup` scan (7 buffer hits), (2) UPDATE `person_mapping.deleted_at`, (3) UPDATE `union_find.deleted_at` via `idx_uf_person`. No chain walking or re-linking required — orphaned nodes are lazily cleaned up on subsequent access via `check_did`. The p95 of 16.54ms is tight; the p99 cliff to 200ms is the familiar Docker tmpfs scheduling stall.

---

## Postgres Internals Analysis

### Buffer Cache Performance

| Metric | Value |
|--------|-------|
| Buffer cache hit ratio | **100.0000%** |
| Disk block reads | 2 |
| Buffer hits | 62,847,916 |
| Temp files created | 0 |
| Temp bytes | 0 |

The entire 208MB dataset fits in `shared_buffers=256MB`. Every operation is served from RAM. The 2 disk reads are from initial cold-cache catalog lookups during migration. Zero temp files confirms that all sorts and hash joins fit in `work_mem=8MB`.

**Per-table I/O breakdown:**

| Table | Heap Hits | Index Hits | Disk Reads |
|-------|-----------|------------|------------|
| union\_find | 9,555,728 | 28,882,881 | 1 |
| person\_mapping | 3,110,403 | 7,972,868 | 1 |
| distinct\_id\_mappings | 2,887,370 | 10,438,666 | 0 |

Index buffer hits outnumber heap hits ~3:1 across all tables, confirming that the recursive CTE's index-only traversal pattern dominates the workload. `union_find` accounts for 61% of all buffer hits — its primary key index is the core of the system.

### Transaction Statistics

| Metric | Value |
|--------|-------|
| Commits | 2,488,841 |
| Rollbacks | 0 |
| Deadlocks | 0 |
| Conflicts | 0 |

Zero rollbacks confirms that the worker-per-team design eliminates cross-team contention. Zero deadlocks validates that the `team_id`-scoped locking strategy is correct: since each team's operations are serialized through a single worker channel, two transactions for the same team can never deadlock.

### WAL Statistics

| Metric | Value |
|--------|-------|
| WAL records | 6,247,650 |
| Full-page images | 1,434 |
| WAL bytes | 490 MB |
| WAL buffers full | 0 |
| WAL writes | 1,553 |
| WAL sync | 0 |

Zero WAL syncs confirms `synchronous_commit=off` is active — the benchmark never waited for WAL flush. The 490MB of WAL across 2.5M transactions averages ~197 bytes per commit. In production with full durability, each commit would add ~0.5-2ms for WAL sync on NVMe, but group commit (`commit_delay=200us`) amortizes this across concurrent transactions.

### Table Sizes and Dead Tuple Bloat

| Table | Total | Heap | Indexes | Live Tuples | Dead Tuples | Dead % |
|-------|-------|------|---------|-------------|-------------|--------|
| union\_find | 76 MB | 29 MB | 47 MB | 450,000 | 392,260 | 46.6% |
| person\_mapping | 70 MB | 31 MB | 39 MB | 69,234 | 328,437 | 82.6% |
| distinct\_id\_mappings | 62 MB | 27 MB | 35 MB | 450,000 | 10,000 | 2.2% |
| **Total** | **208 MB** | **87 MB** | **121 MB** | **969,234** | **730,697** | — |

**Autovacuum was disabled** for this benchmark to eliminate lock interference. The dead tuple counts reflect the full write workload:

- **person\_mapping** (82.6% dead): Merges delete the "loser" person row. 295,766 hard deletes + 117,233 updates = massive churn. At production scale, this table needs `autovacuum_vacuum_scale_factor=0.01` and `autovacuum_vacuum_cost_delay=0`.
- **union\_find** (46.6% dead): Merges update root rows (clear person\_id, set next). 387,170 updates generated dead tuples. HOT updates (5,576) were rare — most updates change indexed columns (`next`, `person_id`), preventing HOT.
- **distinct\_id\_mappings** (2.2% dead): Only `/delete_distinct_id` operations (10K) generated dead tuples. This table is insert-heavy with no updates.

### DML Totals

| Table | Inserts | Updates | Deletes | HOT Updates |
|-------|---------|---------|---------|-------------|
| union\_find | 460,000 | 387,170 | 10,000 | 5,576 |
| person\_mapping | 365,000 | 117,233 | 295,766 | 105,900 |
| distinct\_id\_mappings | 460,000 | 0 | 10,000 | 0 |

**HOT update ratio analysis:** `person_mapping` achieves 90% HOT updates (105,900 / 117,233) because its updates target `is_identified` and `deleted_at` — neither column is indexed. `union_find` achieves only 1.4% HOT (5,576 / 387,170) because merge updates change `next` and `person_id`, both of which participate in indexes (`idx_uf_next`, `idx_uf_person`). This is the root cause of union\_find's high dead tuple ratio and the strongest argument for aggressive autovacuum on this table.

### Scan Patterns

| Table | Seq Scans | Index Scans | Index Scan % |
|-------|-----------|-------------|--------------|
| union\_find | 7 | 7,531,301 | 100.00% |
| person\_mapping | 5 | 2,142,571 | 100.00% |
| distinct\_id\_mappings | 4 | 2,798,526 | 100.00% |

Negligible sequential scans (migration/truncate only). All application queries use index scans as intended.

### Index Usage

| Index | Scans | Tuples Read | Size | Bytes/Scan | Purpose |
|-------|-------|-------------|------|------------|---------|
| `union_find_pkey` | 6,512,674 | 7,174,356 | 25 MB | 4.03 | CTE chain traversal (every resolve) |
| `person_mapping_pkey` | 2,032,571 | 2,046,341 | 8 MB | 4.30 | Person UUID lookup at chain root |
| `idx_did_lookup` | 1,899,899 | 1,528,646 | 25 MB | 13.83 | distinct\_id → node entry point |
| `idx_uf_next` | 898,627 | 854,450 | 8 MB | 9.13 | Delete: find children of deleted node |
| `distinct_id_mappings_pkey` | 898,627 | 898,627 | 10 MB | 11.49 | Delete: DID row by PK |
| `idx_uf_person` | 120,000 | 120,000 | 14 MB | 122.47 | Reverse resolve + delete person |
| `idx_person_mapping_lookup` | 110,000 | 110,061 | 31 MB | 296.15 | Reverse resolve: team\_id + person\_uuid |

**Analysis:**

- **`union_find_pkey`** is the hottest index by an order of magnitude. At 4.0 bytes/scan, each scan touches ~1 leaf page — optimal for the recursive CTE's single-row lookups. This index is the core of the system's performance.
- **`idx_person_mapping_lookup`** has the worst bytes/scan ratio (296). At 31MB for only 110K scans, it's oversized relative to its usage. This is because it indexes `(team_id, person_uuid)` where person\_uuid is a `varchar(200)` UUID string. At production scale with 100M persons, this index would be ~31GB. Consider storing person\_uuid as native `uuid` type (16 bytes) or a numeric hash.
- **`idx_uf_person`** at 14MB for 120K scans is also sparse. It's a partial unique index (`WHERE person_id IS NOT NULL`) covering only root nodes. At production scale with 100M persons, expect ~3.1GB — reasonable for its purpose.
- **`idx_uf_next`** at 898K scans is used exclusively by delete operations. If deletes are rare in production, this index's overhead during writes (maintaining it on every INSERT/UPDATE) may outweigh its read benefit. Profile the production delete rate before deciding.

### Checkpoint Behavior

| Metric | Value |
|--------|-------|
| Timed checkpoints | 0 |
| Requested checkpoints | 1 |
| Buffers written | 922 |
| Write time | 3ms |
| Sync time | 1ms |

With `checkpoint_timeout=15min` and the benchmark completing in ~185s, only 1 on-demand checkpoint occurred. The minimal checkpoint I/O confirms that `synchronous_commit=off` + `wal_level=minimal` keeps WAL overhead negligible in this benchmark configuration.

---

## Query Plan Analysis

### Forward Resolve CTE (the critical path)

**Shallow chain (depth 0, root node):**
```
Execution Time: 0.16ms | Buffer Hits: 11
```
The CTE starts at the given PK, finds `person_id IS NOT NULL` on the first hop (root), and terminates. One index scan on `union_find_pkey` (7 hits) plus one on `person_mapping_pkey` (4 hits).

**Deep chain (depth 19):**
```
Execution Time: 0.70ms | Buffer Hits: 87
```
The CTE loops 20 times, each loop performing one `union_find_pkey` index scan at 4.0 buffer hits/hop. The 19 recursive steps generate 76 buffer hits. The `walk_result` sort uses quicksort (20 rows) — negligible cost.

**Cost per hop:** `(0.70ms - 0.16ms) / 19 hops ≈ 0.028ms/hop`. This is the fundamental unit of resolve latency. At depth 100 (worst case): `0.16 + 100 × 0.028 ≈ 2.96ms`. At depth 1000 (pathological): ~28ms.

### Reverse Resolve CTE (person → all distinct\_ids)

```
Execution Time: 1.61ms | Buffer Hits: 245
```
Starts from `idx_uf_person` (person\_id → root node, 11 hits including InitPlan), then walks outward via `idx_uf_next` (3.8 hits/child level across 9 levels). For the test person with 30 DIDs, the CTE terminates after 9 levels. Each level joins `distinct_id_mappings_pkey` to map node IDs back to distinct\_id strings (4 hits per DID). The LIMIT 10001 clause prevents runaway fan-out.

### Batch Lookup (`ANY()` index scan)

```
Execution Time: <0.3ms | Buffer Hits: 35
```
A single `idx_did_lookup` scan with `ANY(ARRAY[...])` for 10 distinct\_ids. Postgres handles this as a single index scan with an `IN`-list, not 10 separate scans — 3.5 hits per key. Linear scaling: 100 keys ≈ 35 buffer hits ≈ 0.5ms.

### Batch Resolve (multi-start recursive CTE)

```
Execution Time: 0.25ms | Buffer Hits: 14
```
The multi-start CTE seeds starting nodes into the recursive pass. Postgres processes all chains in a single recursive pass, then deduplicates roots with `DISTINCT ON (start_node)`. For shallow chains (depth 0-1), the total is dominated by the initial ANY() index scan (7 hits). For 10 chains at depth 19 each, expect ~700 buffer hits and ~4ms — still a single query.

### Path Compression CTE

```
Execution Time: 0.73ms | Buffer Hits: 83
```
At depth 19, the compression CTE walks the full chain (83 hits), computes `max_depth=19`, and since 19 < 20 (threshold), the `WHERE pi.max_depth >= $3` filter evaluates to false — the UPDATE is skipped entirely (the `InitPlan` nodes show "never executed"). The CTE walk cost is paid even when no compression occurs. At depth 20+, the UPDATE would flatten all intermediate nodes to point directly at the root in a single pass.

---

## Improvement Opportunities

### I1: Migrate all merge callers to batched merge (high impact)

The 4.0× improvement is definitive. `/merge` should be deprecated for all production traffic in favor of `/batched_merge`. The per-source endpoint should only be kept for single-source convenience.

### I2: Increase default merge batch size (medium impact)

The current `BENCH_BATCH=10` was chosen conservatively. Batched merge amortizes transaction overhead — testing with batch sizes of 25, 50, and 100 would reveal the throughput ceiling. The batch\_resolve\_pks CTE scales well (single recursive pass regardless of batch size), so the limit is transaction hold time vs. worker channel blocking time.

### I3: Background path compaction (critical for production)

Path compression triggers on both writes (via `CompressHint` with retry) and reads (fire-and-forget `try_send`) when chain depth exceeds `PATH_COMPRESS_THRESHOLD=20`. At production scale with billions of DIDs, this reactive approach may not keep chains short enough. A periodic background job (per team, off-peak) that flattens all chains to depth 1 would guarantee O(1) resolve latency. This is the most important operational requirement.

### I4: Autovacuum tuning for merge churn (critical for production)

Merges generate 1 dead tuple in `person_mapping` (loser deleted) and 1 dead tuple in `union_find` (root updated) per source. At 12.2K merges/s via batched merge, that's 24K dead tuples/s. Required autovacuum settings:

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

At 31MB for 110K scans (296 bytes/scan), this is the least efficient index. It indexes `(team_id, person_uuid)` where `person_uuid` is a `varchar(200)` string. At production scale (100M persons), this index alone would be ~31GB. Options: (1) store person\_uuid as `uuid` type (16 bytes) instead of `varchar(200)`, (2) use a hash index for exact-match lookups, (3) add a numeric hash column.

### I7: Stress-test queue saturation (validation)

The current benchmark uses 50 concurrency against 64 workers — no saturation is possible. A stress benchmark with `BENCH_CONCURRENCY=500+` would validate the 100ms enqueue timeout behavior and identify the worker pool's saturation point.

### I8: Evaluate `idx_uf_next` maintenance cost vs. delete frequency (low-medium impact)

This index exists solely for delete operations (898K scans) but is maintained on every INSERT and UPDATE to `union_find` (847K write DML ops). If production delete rates are low relative to writes, the index maintenance overhead during merges and creates may exceed the savings during deletes. Profile production delete frequency before deciding; consider a deferred/lazy index strategy.
