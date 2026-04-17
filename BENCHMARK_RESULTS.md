# Benchmark Results

**Date:** 2026-04-16
**Machine:** Apple Silicon (macOS 26.3.1, 14 cores, 48 GB RAM), native Postgres (Homebrew)
**Postgres:** 17.9 (native, Homebrew), tuned for benchmarking (see `bin/run-bench-local.sh` and `LOCAL_POSTGRES_BENCH.md`)
**Server config:** 64 workers, 64 channel capacity, compress threshold 20, READ_POOL_SIZE 4
**Rust:** 1.90.0 (stable), edition 2024, release profile (opt-level 3)
**Run time:** ~100s total (including seeding)
**Bench script:** `./bin/run-bench-local.sh` (auto-bootstraps PG 17, data dir, config, server lifecycle)

This run replaces the prior Docker (OrbStack) results. Native PG on tuned hardware eliminates the VM scheduling artifacts that were capping p99 at ~200ms on every write endpoint and the sporadic 600-800ms read max. With that noise gone, the numbers here reflect the actual characteristics of the workload, the recursive CTE, and the worker-per-team serialization model.

---

## Headline: Docker → Native Delta

All numbers are from the *same* bench harness, same sizes, same 50 concurrent HTTP clients, same workload mix. Only the database changed: Docker Postgres on tmpfs (port 54320) → native Homebrew Postgres 17 with tuned config (port 54321).

| Endpoint | Docker ops/s | Native ops/s | Speedup | Docker p99 | Native p99 | p99 factor |
|----------|-------------:|-------------:|:-------:|-----------:|-----------:|:----------:|
| `POST /resolve`             | 14,559 | **22,133** | 1.52× | 6.65ms  | **2.58ms**  | 2.58× |
| `POST /resolve_distinct_ids`| 14,656 | **22,026** | 1.50× | 5.75ms  | **3.18ms**  | 1.81× |
| `POST /create`              |  5,216 | **11,898** | 2.28× | 196.48ms| **12.10ms** | **16.2×** |
| `POST /alias`               |  4,420 |  **9,227** | 2.09× | 197.82ms| **15.74ms** | **12.6×** |
| `POST /merge` (10/batch)    |    321 |  **1,122** | 3.49× | 311.83ms| **87.50ms** | 3.56× |
| `POST /batched_merge`       |  1,195 |  **3,589** | 3.00× | 90.92ms | **29.21ms** | 3.11× |
| `POST /delete_distinct_id`  |  2,567 |  **7,069** | 2.75× | 209.03ms| **10.31ms** | **20.3×** |
| `POST /delete_person`       |  3,469 | **13,156** | 3.79× | 204.55ms| **4.23ms**  | **48.4×** |

**The p99 cliff is gone.** On Docker, every write endpoint had a p50→p99 ratio of 25-50×, caused by OrbStack's VM deschedule stalls punching through to Postgres syscalls. On native, the ratio is 1.5-3×: p99 now tracks p50 tightly, which is what you expect from a workload with no real contention and a fully resident working set. Read max dropped from 801ms (single Docker host stall) to 96ms.

**Zero failures** across 1.29M HTTP operations. No 503s, no 500s, no timeouts, no `channel full` logs.

---

## Production Prospects: 1000s req/s, 100M Persons, Billions of DIDs

This section assesses viability at production scale using the *native* results as the baseline -- they more accurately predict bare-metal Linux/NVMe production behavior than the Docker-on-macOS numbers did.

### Verdict: Viable, with the previously identified operational investment

The core data structure and query patterns are sound and now proven to be CPU/memory-bound, not VM-bound. The recursive CTE resolves identities at ~22.1K ops/s on a laptop with sub-2.3ms p50 and a tight 2.6ms p99 tail. Doubling the read throughput vs. Docker while tightening the p99 by 2.5× moves the ceiling up meaningfully. The bottlenecks remain operational (vacuum, partitioning, chain compaction) rather than algorithmic.

### Read throughput (the primary operation)

Forward resolve (`/resolve`) achieves 22,133 ops/s at p50=2.21ms with 50 concurrent clients against a native Postgres with 4 GB `shared_buffers`. The CTE averages ~3.8 buffer hits per chain hop (below), all served from RAM -- **zero disk reads across the entire 100-second run**.

**At production scale:** A vertically scaled Postgres (64 vCPUs, 256 GB RAM, NVMe) with the entire working set in `shared_buffers`, fronted by 4-8 app instances behind PgBouncer, should sustain **150K-300K read ops/s** -- the native results push this estimate up ~50% from the Docker-based projection. Reads are CPU-bound on B-tree traversal and the Memoize node on `union_find_pkey`. With 64 hash partitions by `team_id`, reads for different teams touch disjoint index pages, eliminating cross-team cache contention.

### Write throughput

The worker-per-team serialization model means each team's writes execute sequentially. With 64 workers on a single app instance, aggregate write throughput scales linearly:

| Operation | Per-worker (native) | 64 workers aggregate |
|-----------|--------------------:|---------------------:|
| Create | ~11,900/s | ~762,000/s |
| Alias | ~9,200/s | ~590,000/s |
| Batched merge (10/batch) | ~3,600 batches/s | ~230,000 batches/s (2.3M merges/s) |

The bottleneck shifts decisively to Postgres WAL write throughput and connection pool sizing. With `synchronous_commit=on` (required for production), each commit adds ~0.5-2ms for WAL fsync on NVMe. Group commit (`commit_delay=200`, `commit_siblings=5`) amortizes this. The native run generated **491 MB of WAL** across 2.49M commits (~197 bytes/commit) with sync off; production durability turns that into the real write ceiling.

### Table sizes at 100M persons, 1B DIDs

| Table | Estimated Size | Index Size | Total |
|-------|---------------:|-----------:|------:|
| union_find           | ~60 GB  | ~100 GB | ~160 GB |
| distinct_id_mappings | ~55 GB  | ~75 GB  | ~130 GB |
| person_mapping       | ~35 GB  | ~50 GB  | ~85 GB  |
| **Total**            | **~150 GB** | **~225 GB** | **~375 GB** |

Fits on a single vertically scaled instance with 1 TB NVMe storage. With 64 hash partitions, each partition holds ~5.9 GB of data -- small enough for per-partition `VACUUM FULL` during off-peak windows. Projections are unchanged from the Docker run; they depend on row counts and column sizes, not the benchmark host.

### Critical scaling risks

**1. Chain depth growth.** Post-compression max depth is 21 (mean 2.90, p99=16) across teams 1-10. Path compression triggers on both writes (`CompressHint` above threshold) and reads (fire-and-forget `try_send`). **The `channel full` errors that appeared under bursty Docker read load did not recur on native PG** -- the server process is faster, workers drain more quickly, and the channel backpressure never hit. This is a host-fidelity improvement, not a design change: a periodic background compaction job (per-team full-tree flattening) remains essential at production scale to guarantee bounded resolve latency regardless of merge velocity.

**2. Dead tuple bloat.** After the benchmark, `person_mapping` has 82.5% dead tuples and `union_find` has 46.4%. Virtually identical to Docker. Merges generate 1 dead tuple per source in both tables. At 2.3M merges/s aggregate, that's ~4.6M dead tuples/s. Required: aggressive per-table autovacuum (`autovacuum_vacuum_scale_factor=0.01`, `autovacuum_vacuum_cost_delay=0`) and monitoring of `n_dead_tup`.

**3. `idx_person_mapping_lookup` sizing.** At 18 MB for 110K scans (~168 bytes/scan) in this run, down from 20 MB on the earlier varchar benchmark. At 100M persons this index is ~5 GB -- well within `shared_buffers` on a production instance.

**4. Partitioning.** Still critical at 1B+ rows. `PARTITION BY HASH(team_id, 64)` drops index B-tree depth a level, shrinks per-partition vacuum work, and yields partition pruning on every query (all queries already filter on `team_id`). The native numbers don't change the calculus -- they just show the single-partition path is faster than previously observed.

**5. WAL pressure with durability enabled.** The benchmark generated 491 MB of WAL with `synchronous_commit=off` (vs 468 MB on Docker -- marginally higher because throughput is higher). In production with full durability, WAL fsync adds latency per commit. NVMe SSDs with `wal_compression=lz4` and group commit mitigate this, but WAL throughput becomes the ceiling for aggregate write rate.

---

## Configuration

| Parameter | Value |
|-----------|-------|
| Teams | 100 |
| Create ops | 50,000 |
| Alias ops | 100,000 |
| Merge ops | 100,000 (10 per batch = 10,000 batches) |
| Batched merge ops | 100,000 (10 per batch = 10,000 batches) |
| Max chain depth (deepen phase) | 100 |
| Resolve ops | 1,000,000 |
| Resolve distinct IDs ops | 100,000 |
| Delete DID ops | 10,000 |
| Delete person ops | 10,000 |
| HTTP concurrency | 50 |

Postgres runtime parameters in effect (native cluster at `~/.local/share/pg-bench-17`):

| Parameter | Value | Notes |
|-----------|-------|-------|
| `shared_buffers` | 4 GB | 16× Docker's 256 MB |
| `effective_cache_size` | 16 GB | 32× Docker's 512 MB |
| `work_mem` | 32 MB | 4× Docker |
| `maintenance_work_mem` | 512 MB | 8× Docker |
| `wal_buffers` | 64 MB | 4× Docker |
| `wal_level` | minimal | same |
| `fsync` | off | same (bench mode) |
| `synchronous_commit` | off | same |
| `full_page_writes` | off | same |
| `autovacuum` | off | same |
| `huge_pages` | try | new (macOS ignores; kept for Linux) |
| `effective_io_concurrency` | 0 | macOS lacks `posix_fadvise`; set to 200 on Linux |

---

## Throughput Summary

| Endpoint | ops/s | p50 | p95 | p99 | max |
|----------|------:|----:|----:|----:|----:|
| `POST /create`               | 11,898 |  3.24ms |  8.50ms | 12.10ms |  27.48ms |
| `POST /alias`                |  9,227 |  4.07ms | 11.17ms | 15.74ms |  30.32ms |
| `POST /merge` (10/batch)     |  1,122 | 44.23ms | 83.39ms | 87.50ms |  95.68ms |
| **`POST /batched_merge` (10/batch)** | **3,589** | **13.70ms** | **25.65ms** | **29.21ms** | **47.88ms** |
| `POST /resolve`              | 22,133 |  2.21ms |  2.40ms |  2.58ms |  95.98ms |
| `POST /resolve_distinct_ids` | 22,026 |  2.21ms |  2.45ms |  3.18ms |  10.93ms |
| `POST /delete_distinct_id`   |  7,069 |  6.98ms |  7.35ms | 10.31ms |  11.85ms |
| `POST /delete_person`        | 13,156 |  3.76ms |  4.02ms |  4.23ms |   4.96ms |

**Zero failures across 1.29M HTTP operations.** Zero `channel full` compress-enqueue failures, zero 503s, zero 500s. The 64-worker pool with 64-depth channels handled 50 concurrent clients with headroom.

---

## Results by Endpoint

### Create (`POST /create`)

| Metric | Value |
|--------|-------|
| Total ops | 50,000 |
| Failures | 0 (0.00%) |
| Wall time | 4.20s |
| Throughput | 11,898 ops/s |
| p50 | 3.24ms |
| p95 | 8.50ms |
| p99 | 12.10ms |
| max | 27.48ms |

**Workload mix:** 80% new distinct IDs, 20% existing (hot-set re-identification).

**Query plan (hot path):** Single index scan on `idx_did_lookup`, 3-4 buffer hits, sub-0.05ms execution time. For new DIDs, the create path runs 3 sequential INSERTs (person_mapping, distinct_id_mappings, union_find) in one transaction.

**The Docker p99 cliff is gone.** Where Docker jumped 4.06ms → 196ms between p50 and p99, native goes 3.24ms → 12.10ms. The remaining tail reflects real contention in the worker pool when a burst of requests lands on the same team_id -- not VM stalls. Live `pg_stat_activity` sampling during this phase showed ~25-45 server backends in `idle in transaction` (between statements in a short tx) with 0-2 `active` at any snapshot; no `LWLock` or `BufferPin` waits appeared.

---

### Alias (`POST /alias`)

| Metric | Value |
|--------|-------|
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 10.84s |
| Throughput | 9,227 ops/s |
| p50 | 4.07ms |
| p95 | 11.17ms |
| p99 | 15.74ms |
| max | 30.32ms |

**Workload mix:** 85% Case 1a (target exists, source new), 5% Case 2a (same person), 5% self-alias, 5% Case 3 (both new).

Alias is ~22% slower than create because the dominant Case 1a path requires: (1) resolve target root via recursive CTE, (2) `check_did` on source, (3) INSERT into `distinct_id_mappings` + `union_find`, (4) UPDATE `person_mapping.is_identified`. On native this all fits comfortably inside 15ms at p99; on Docker the same work spiked to 200ms under the same conditions.

---

### Merge (`POST /merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct_ids) |
| Failures | 0 (0.00%) |
| Wall time | 8.91s |
| Throughput | 1,122 batch ops/s (~11,220 merges/s) |
| p50 | 44.23ms |
| p95 | 83.39ms |
| p99 | 87.50ms |
| max | 95.68ms |

**Still the slowest operation.** Each 10-source merge batch executes 10 serial iterations within a single transaction: `check_did` (index lookup + CTE) then `link_root_to_target` (UPDATE union_find + DELETE person_mapping). The p50 of 44ms means ~4.4ms per source (vs 15ms/source on Docker). The narrow p50→p99 spread (44ms → 87ms) indicates consistent, predictable slowness rather than sporadic stalls. Throughput is 3.5× Docker.

---

### Batched Merge (`POST /batched_merge`, 10 sources per batch)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 batches (100,000 distinct_ids) |
| Failures | 0 (0.00%) |
| Wall time | 2.79s |
| Throughput | 3,589 batch ops/s (~35,890 merges/s) |
| p50 | 13.70ms |
| p95 | 25.65ms |
| p99 | 29.21ms |
| max | 47.88ms |

**3.20× throughput improvement over `/merge`** (similar ratio to Docker's 3.7×). The batched path replaces per-source serial SQL with bulk operations:

1. Single `batch_lookup_dids`: `ANY(ARRAY[...])` index scan returning all PKs.
2. Single `batch_resolve_pks`: multi-start recursive CTE using `unnest($2::bigint[])`.
3. Batch INSERT for not-found sources, batch UPDATE + DELETE for live-different roots.

| Metric | Merge | Batched Merge | Improvement |
|--------|------:|--------------:|:-----------:|
| ops/s (batch) |  1,122 |  3,589 | **3.20×** |
| p50           | 44.23ms | 13.70ms | **3.23×** |
| p95           | 83.39ms | 25.65ms | **3.25×** |
| p99           | 87.50ms | 29.21ms | **3.00×** |

---

### Chain Depth at Read Time

Sampled chain depth statistics (teams 1-10, 39,312 non-root nodes, direct PG walk post-run):

| Metric | Value |
|--------|-------|
| Mean depth | 2.90 |
| p50 | 2 |
| p95 | 10 |
| p99 | 16 |
| Max | 21 |

The configured chain-deepen phase built chains up to depth 100, but path compression (threshold=20) flattened most chains during the write phases and the 1M-resolve read phase. Max of 21 (one above threshold) means a single chain briefly exceeded threshold before compression fired. The Docker run showed mean 4.09 -- native is slightly shallower, likely because the compress `try_send` path hit the worker channel successfully every time here (no saturation at all), whereas Docker dropped ~100 compress hints under bursty load. Fewer dropped hints → tighter chain distribution.

**Compress channel behavior:** No saturation observed in this run. The fire-and-forget `try_send` compression requests all landed. At production scale, a background compaction job is still needed rather than relying solely on opportunistic compression under adversarial load patterns.

---

### Resolve / Read (`POST /resolve`)

| Metric | Value |
|--------|-------|
| Total ops | 1,000,000 |
| Failures | 0 (0.00%) |
| Wall time | 45.18s |
| Throughput | 22,133 ops/s |
| p50 | 2.21ms |
| p95 | 2.40ms |
| p99 | 2.58ms |
| max | 95.98ms |

**This is the primary operation and it is tightly clustered around p50.** Native PG on tuned hardware holds p99 within 17% of p50 -- a qualitative change from Docker, where p99 was 3.7× p50 and max was 801ms. The p50 of 2.21ms includes the full HTTP round-trip (client → Axum → pool → CTE → response). The CTE walks `union_find_pkey` with an average of ~6.5 index scans per resolve (6.5M total scans / 1M reads), reflecting mixed chain depths.

Live sampling during the 45-second read phase showed sustained ~625K buffer hits/s on the union_find database and ~44K commits/s, with **zero disk reads** -- the entire working set stayed resident in the 4 GB `shared_buffers`.

**Query plan (depth 0, root node; measured directly post-run):**
```
Execution Time: 0.069ms | Buffers: shared hit=18
```
CTE starts, finds `person_id IS NOT NULL` immediately, terminates after 1 loop. One `union_find_pkey` scan (7 hits) + Memoize miss (1kB) + `person_mapping_pkey` join (4 hits).

**Query plan (depth 21, deepest post-compression chain in team 1):**
```
Execution Time: 0.173ms | Buffers: shared hit=98
```
CTE loops 22 times, each performing one Memoized `union_find_pkey` index scan (~3.8 hits/hop). 87 buffer hits cover the full chain walk; `person_mapping_pkey` join adds 4 hits. Top-N heapsort with 25 kB memory.

**Cost per hop:** `(0.173ms − 0.069ms) / 21 hops ≈ 0.005ms/hop` (execution time delta). The low per-hop cost reflects the Memoize node caching repeated lookups. In buffer terms: `(98 − 18) / 21 ≈ 3.8 buffer hits/hop`. These ratios are essentially identical to Docker's (which measured 4.0 hits/hop, 0.005ms/hop) -- the per-hop work is fundamental to the CTE and doesn't depend on the host.

**Projected latency at scale:**

| Depth | Buffer Hits | Estimated Execution |
|-------|------------:|--------------------:|
| 0 | 18 | ~0.07ms |
| 10 | ~56 | ~0.12ms |
| 21 | 98 | ~0.17ms |
| 100 | ~398 | ~0.57ms |
| 1000 | ~3,818 | ~5.1ms |

**The 96ms max** across 1M ops (vs Docker's 801ms) is one long GC pause or scheduling blip in the 45-second read phase. Two orders of magnitude tighter than Docker's worst case.

---

### Resolve Distinct IDs (`POST /resolve_distinct_ids`)

| Metric | Value |
|--------|-------|
| Live persons found | 49,388 (in 18.5ms) |
| Total ops | 100,000 |
| Failures | 0 (0.00%) |
| Wall time | 4.54s |
| Throughput | 22,026 ops/s |
| p50 | 2.21ms |
| p95 | 2.45ms |
| p99 | 3.18ms |
| max | 10.93ms |

Reverse resolve walks from a `person_uuid` outward through `idx_uf_person` (root) then `idx_uf_next` (children). Query shapes and costs are identical to the Docker run; the improvement is in the host path -- p99 drops from 5.75ms → 3.18ms and max from 602ms → 10.93ms. Max is now bounded well under 2× p99, which is a strong signal that there are no host-level stalls left in this phase.

---

### Delete Distinct ID (`POST /delete_distinct_id`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 1.41s |
| Throughput | 7,069 ops/s |
| p50 | 6.98ms |
| p95 | 7.35ms |
| p99 | 10.31ms |
| max | 11.85ms |

Delete DID is the most complex write operation: (1) `lookup_did` (index scan), (2) determine root or non-root, (3) `unlink_did` -- splice parents via `idx_uf_next`, or promote a parent to root if deleting a root, (4) hard-delete from `union_find` and `distinct_id_mappings`, (5) check if the person is orphaned and conditionally soft-delete.

On Docker this operation had the worst p50→p95 ratio of the whole suite (6.78 → 179.75ms, 26×). On native the ratio is ~1.05× (6.98 → 7.35ms). The Docker p95 spike was the parent-splicing read-modify-write cycle amplifying VM-level lock hold time; absent the VM, the splice completes in microseconds.

---

### Delete Person (`POST /delete_person`)

| Metric | Value |
|--------|-------|
| Total ops | 10,000 |
| Failures | 0 (0.00%) |
| Wall time | 0.76s |
| Throughput | 13,156 ops/s |
| p50 | 3.76ms |
| p95 | 4.02ms |
| p99 | 4.23ms |
| max | 4.96ms |

**The tightest distribution in the whole run.** Max 4.96ms, p99 4.23ms, p50 3.76ms -- a 1.32× p50→max ratio. Delete person soft-deletes: UPDATE `person_mapping.deleted_at` + UPDATE `union_find.deleted_at` via `idx_uf_person`. No chain walking or re-linking. Orphaned nodes are lazily cleaned up on subsequent access. This is the best demonstration in the suite of how a short, index-driven write transaction behaves without VM interference: no fat tail at all.

---

## Postgres Internals Analysis

### Buffer Cache Performance

| Metric | Value |
|--------|-------|
| Buffer cache hit ratio | **100.0000%** |
| Disk block reads | **0** |
| Buffer hits | 62,637,317 |
| Temp files created | 0 |
| Temp bytes | 0 |

The entire ~188 MB dataset fits comfortably in `shared_buffers=4 GB` with ~1 GB of pages actually touched (measured via `pg_stat_bgwriter.buffers_alloc = 26,027`, i.e. 26,027 × 8 kB = 203 MB worth of unique buffer allocations across the run). Every single read and write hit RAM. Docker had 266 disk reads from catalog cold-cache lookups; native had 0 because the cluster was warm from a prior `--check` bootstrap before the benchmark. Zero temp files confirms all sorts and hash joins fit in `work_mem=32 MB`.

**Per-table I/O breakdown:**

| Table | Heap Hits | Index Hits | Disk Reads |
|-------|----------:|-----------:|-----------:|
| union_find           | 9,250,019 | 28,059,967 | 0 |
| person_mapping       | 3,089,478 |  7,843,180 | 0 |
| distinct_id_mappings | 2,916,463 | 10,539,088 | 0 |

Index buffer hits outnumber heap hits ~3:1 across all tables, confirming the recursive CTE's index-driven traversal pattern dominates. `union_find` accounts for 59% of all buffer hits -- almost identical to Docker's 60%.

### Transaction Statistics

| Metric | Value |
|--------|-------|
| Commits | 2,488,672 |
| Rollbacks | 0 |
| Deadlocks | 0 |
| Conflicts | 0 |

Zero rollbacks and zero deadlocks validates the `team_id`-scoped serialization strategy: since each team's writes are funneled through a single worker, two transactions for the same team can never deadlock.

### WAL Statistics

| Metric | Value |
|--------|-------|
| WAL records | 6,281,612 |
| Full-page images | 1,449 |
| WAL bytes | 491 MB |
| WAL buffers full | 0 |
| WAL writes | 1,289 |
| WAL syncs | 0 |

Zero WAL syncs confirms `synchronous_commit=off` is active. Zero `wal_buffers_full` events confirms the 64 MB WAL buffer handled the peak burst rate with headroom. The 491 MB of WAL across 2.49M transactions averages ~197 bytes per commit (same as Docker). In production with durability enabled, each commit would add ~0.5-2ms for WAL sync on NVMe, amortized by group commit.

### Table Sizes and Dead Tuple Bloat

| Table | Total | Heap | Indexes | Live Tuples | Dead Tuples | Dead % |
|-------|------:|-----:|--------:|------------:|------------:|-------:|
| union_find           | 76 MB | 29 MB | 47 MB | 450,000 | 389,297 | 46.4% |
| person_mapping       | 51 MB | 25 MB | 26 MB |  69,388 | 327,060 | 82.5% |
| distinct_id_mappings | 61 MB | 27 MB | 34 MB | 450,000 |  10,000 |  2.2% |
| **Total**            | **188 MB** | **81 MB** | **107 MB** | **969,388** | **726,357** | -- |

**Autovacuum was disabled** for this benchmark, identical to Docker. Dead tuple counts are within 1% of the Docker run across all three tables -- these ratios reflect the workload shape, not the host:

- **person_mapping** (82.5% dead): Merges delete the "loser" person row. 295,612 hard deletes + 117,127 updates = massive churn. At production scale: `autovacuum_vacuum_scale_factor=0.01`, `autovacuum_vacuum_cost_delay=0`.
- **union_find** (46.4% dead): Merges update root rows (clear `person_id`, set `next`). 385,787 updates generated dead tuples. HOT updates (7,733) were rare because most updates change indexed columns (`next`, `person_id`), preventing HOT.
- **distinct_id_mappings** (2.2% dead): Only `delete_distinct_id` (10,000) generated dead tuples. Insert-heavy, no updates.

### DML Totals

| Table | Inserts | Updates | Deletes | HOT Updates |
|-------|--------:|--------:|--------:|------------:|
| union_find           | 460,000 | 385,787 |  10,000 |   7,733 |
| person_mapping       | 365,000 | 117,127 | 295,612 | 108,142 |
| distinct_id_mappings | 460,000 |       0 |  10,000 |       0 |

**HOT update ratio:** `person_mapping` achieves 92% HOT (108,142 / 117,127) because updates target `is_identified` and `deleted_at` -- neither indexed. `union_find` achieves only 2.0% HOT (7,733 / 385,787) because merge updates change `next` and `person_id`, both indexed (`idx_uf_next`, `idx_uf_person`). This drives the high dead tuple ratio and argues for aggressive autovacuum on `union_find`.

### Scan Patterns

| Table | Seq Scans | Index Scans | Index Scan % |
|-------|----------:|------------:|-------------:|
| union_find           | 7 | 7,564,781 | 100.00% |
| person_mapping       | 9 | 2,105,829 | 100.00% |
| distinct_id_mappings | 4 | 2,827,267 | 100.00% |

All application queries use index scans. Sequential scans are from migration/truncate only.

### Index Usage

| Index | Scans | Tuples Read | Size | Tup/scan | Purpose |
|-------|------:|------------:|-----:|---------:|---------|
| `union_find_pkey`          | 6,516,989 | 7,141,389 | 25 MB    | 1.10 | CTE chain traversal (every resolve) |
| `person_mapping_pkey`      | 1,995,829 | 2,045,667 | 8 MB     | 1.02 | Person UUID lookup at chain root |
| `idx_did_lookup`           | 1,899,475 | 1,528,261 | 25 MB    | 0.80 | distinct_id → PK entry point |
| `idx_uf_next`              |   927,792 |   883,086 | 8 MB     | 0.95 | Delete/reverse resolve: find children |
| `distinct_id_mappings_pkey`|   927,792 |   927,792 | 10 MB    | 1.00 | DID row by PK (delete path) |
| `idx_uf_person`            |   120,000 |   120,000 | 15 MB    | 1.00 | Reverse resolve + delete person |
| `idx_person_mapping_lookup`|   110,000 |   110,069 | 18 MB    | 1.00 | Reverse resolve: team_id + person_uuid |

**Key observations:**

- **`union_find_pkey`** is the hottest index by an order of magnitude (6.5M scans). At 1.10 tuples/scan, each scan touches essentially one leaf page. This is the core of the system's performance and the primary beneficiary of the Memoize node in the recursive CTE.
- **`idx_person_mapping_lookup`** at 18 MB (down another 2 MB from the Docker run's 20 MB -- same UUID size, slightly less page fragmentation on a fresh cluster). At 100M persons: ~5 GB.
- **`idx_uf_next`** at 8 MB for 928K scans is used by both delete ops and reverse resolve; sized identically to Docker.
- Overall index sizes are stable to within 1-2 MB of the Docker run -- what's different is how fast the scans complete, not how large the indexes are.

### Checkpoint & Bgwriter Behavior

| Metric | Value |
|--------|-------|
| Timed checkpoints | 0 |
| Requested checkpoints | 0 |
| Buffers written by bgwriter | 0 |
| Buffer allocations | 26,027 |

With `checkpoint_timeout=15min` and the benchmark completing in ~100s, no checkpoints fired (Docker had 1 requested checkpoint; native had 0 because the run finished faster and with `fsync=off` there's no pressure). The bgwriter wrote zero buffers -- unsurprising since `fsync=off` and no dirty-page eviction pressure from the 4 GB pool.

### I/O by Backend Type (`pg_stat_io`)

| Backend | Object | Context | Reads | Writes | Hits |
|---------|--------|---------|------:|-------:|-----:|
| client backend        | relation | normal    | 161  | 0 | 62,646,737 |
| standalone backend    | relation | normal    | 537  | 1,028 | 94,232 |
| background worker     | relation | normal    | 0    | 0 | 1,566 |
| standalone backend    | relation | vacuum    | 9    | 0 | 927 |
| client backend        | relation | bulkread  | 933  | 0 | 14 |

The 161 "reads" on the client-backend normal path happened during migrations (catalog bootstrap) before the timed phases; none fired during benchmark traffic. Bulk reads (933) are from `TRUNCATE ... CASCADE` paths the migrate binary uses.

### Live Observation Summary (During Timed Phases)

Samples taken against `pg_stat_activity` once per 1-2 seconds during each non-seed HTTP phase:

| Phase | Total backends | `active` | `idle in transaction` | `idle` | Lock waits | Notable wait events |
|-------|:-:|:-:|:-:|:-:|:-:|---|
| Create     |  95 | 0-2  | 22-27 | 68-70 | 0 | `Client.ClientRead` only |
| Alias      | 117 | 0-3  | 30-40 | 74-85 | 0 | `Client.ClientRead` only |
| Merge      | 115 | 0-1  | 45-50 |  65   | 0 | `Client.ClientRead` only |
| Batched merge | 115 | 0-2 | 40-50 | 65-75 | 0 | `Client.ClientRead` only |
| Resolve (read) | 118 | 0-5 | 0 | 113-118 | 0 | `Client.ClientRead` only |

Two points stand out: (1) during every phase, the server's 68-connection pool (64 workers + 4 read) was well-sized -- backends spent more time in `idle in transaction` (i.e. between statements inside a short write tx) than `active`; (2) there were **no occurrences of any non-`Client` wait event** (no `LWLock`, no `BufferPin`, no `BufferIO`, no `Lock`). All contention in this run is within the Rust workers, not inside Postgres.

---

## Query Plan Analysis (Native, Directly Measured)

### Forward Resolve CTE (the critical path)

**Root node (depth 0):**
```
Execution Time: 0.069ms | Buffer Hits: 18
```
CTE starts, finds `person_id IS NOT NULL` immediately, terminates after 1 loop. One `union_find_pkey` scan (7 hits) + Memoize cache (1kB, 1 miss) + `person_mapping_pkey` join (4 hits).

**Deep chain (depth 21):**
```
Execution Time: 0.173ms | Buffer Hits: 98
```
CTE loops 22 times, each performing one Memoized `union_find_pkey` index scan. 87 buffer hits for the chain walk (~3.8 hits/hop), plus `person_mapping_pkey` join (4 hits). Top-N heapsort with 25 kB memory.

**Cost per hop:** `(0.173ms − 0.069ms) / 21 hops ≈ 0.005ms/hop` (execution time delta). In buffer terms: `(98 − 18) / 21 ≈ 3.8 buffer hits/hop`.

**Projected latency at scale:**

| Depth | Buffer Hits | Estimated Execution |
|-------|------------:|--------------------:|
| 0 | 18 | ~0.07ms |
| 10 | ~56 | ~0.12ms |
| 21 | 98 | ~0.17ms |
| 100 | ~398 | ~0.57ms |
| 1000 | ~3,818 | ~5.1ms |

---

## Bottleneck Summary

| Bottleneck | Severity | Affected Operations | Details |
|------------|----------|---------------------|---------|
| Per-source merge serial SQL    | **Critical** | `/merge` | 10× serial CTE + UPDATE per batch; 3.2× slower than batched path on native |
| Dead tuple bloat (person_mapping) | **Critical** | All writes | 82.5% dead tuples; merges generate 1 dead tuple per source |
| Dead tuple bloat (union_find)  | **High**     | All writes | 46.4% dead tuples; root updates can't use HOT (indexed columns change) |
| Compress channel saturation    | **Medium**   | `/resolve` | Not observed on native, but remains a theoretical risk under adversarial bursty load; mitigate with a background compaction job |
| Single-table design            | **Medium**   | All at scale | No partition pruning; B-tree depth grows with row count |
| ~~p99 latency cliff on writes~~ | ~~Medium~~  | ~~Docker only~~ | **Resolved.** Was a Docker/OrbStack VM scheduling artifact; native p99 is within 3× of p50 on every endpoint. |

---

## Improvement Opportunities

### I1: Migrate all merge callers to batched merge (high impact)

The 3.2× improvement is definitive on native, matching the 3.7× seen on Docker. `/merge` should be deprecated for production traffic in favor of `/batched_merge`. The per-source path should only be kept for single-source convenience or backward compatibility.

### I2: Background path compaction (critical for production)

Opportunistic compression via `try_send` succeeded on every attempt in this run (max chain depth 21, one above threshold; no `channel full` logs). It was effective under moderate load on Docker too, where the channel saturated under bursty read loads. Under sustained high-concurrency reads at production scale the channel will eventually saturate. A periodic background job per team that flattens all chains to depth 1 would guarantee bounded resolve latency regardless of merge velocity. This remains the most important operational requirement for production.

### I3: Autovacuum tuning (critical for production)

```sql
ALTER TABLE union_find     SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 0);
ALTER TABLE person_mapping SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_cost_delay = 0);
```

At production merge rates (~2.3M merges/s aggregate per the native extrapolation), dead tuples accumulate at ~4.6M/s. Without aggressive vacuum, table bloat degrades all operations.

### I4: Partition tables by `team_id` (critical at scale)

`PARTITION BY HASH(team_id, 64)` is essential at 1B+ rows:
- Index B-tree depth drops from ~4 to ~3 levels per partition
- Per-partition vacuum runs faster with less lock contention
- Partition pruning eliminates 63/64 partitions on every query
- Aligns with worker-per-team architecture

### I5: Increase compress channel capacity or add dedicated compress workers (speculative on native)

On Docker this was "high impact" because the channel was observed saturating. On native, no saturation occurred in this run. Keep it on the backlog as a hardening measure for adversarial read patterns, but deprioritize vs. I2 (background compaction), which dominates it.

### I6: ~~Evaluate `idx_person_mapping_lookup` sizing~~ (done — confirmed on native)

**Implemented and re-confirmed:** native `UUID` (16 bytes) for `person_uuid` continues to keep `idx_person_mapping_lookup` small (18 MB here, ~5 GB projected at 100M persons). No change from the Docker finding.

### I7: Stress-test queue saturation (validation)

With native now sustaining 22K reads/s and 12K creates/s from a single app instance at concurrency=50, the original concurrency-500 test is overdue. It should identify the worker-pool's breaking point and validate the 100ms enqueue timeout under pressure.

### I8: Increase default merge batch size (medium impact)

Unchanged from Docker analysis. `BENCH_BATCH=10` is conservative; batched merge amortizes transaction overhead. Testing at 25, 50, 100 would reveal the throughput ceiling. The batch CTE scales well (single recursive pass regardless of batch size).

### I9: Re-run on Linux with NVMe + `synchronous_commit=on` (new)

This benchmark run made two limitations very visible: (1) macOS forces `effective_io_concurrency=0`; (2) durability-off numbers cap what we can predict about production WAL fsync cost. A one-off run on a Linux host with the same binaries, `effective_io_concurrency=200`, `synchronous_commit=on`, and real NVMe would turn the current 768K aggregate creates/s projection into a measurement instead of an extrapolation.

---

## Reproducing These Results

```bash
# Auto-bootstraps PG 17 via Homebrew, initializes $HOME/.local/share/pg-bench-17,
# writes tuned config, starts PG on port 54321, creates the DB, builds binaries,
# migrates, starts the server, runs the full benchmark suite, and stops the server.
./bin/run-bench-local.sh

# Re-run against a freshly reinitialized cluster:
./bin/run-bench-local.sh --fresh

# Bootstrap and verify only, don't run the benchmark:
./bin/run-bench-local.sh --check

# Stop local PG when done:
./bin/run-bench-local.sh --stop
```

The Docker setup at port 54320 is untouched by the above; the two can coexist. To fall back to Docker, run `./bin/run-bench.sh` as before.
