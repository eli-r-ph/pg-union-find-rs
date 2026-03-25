# pg-union-find-rs

A Postgres-backed union-find service for person/distinct_id resolution, modeled after PostHog's identity merge system. Built with Rust (Axum + SQLx + Tokio).

## Architecture

- **Three tables:** `persons`, `distinct_ids`, `person_overrides` — the override table forms a union-find chain traversed by a recursive CTE.
- **Worker pool:** HTTP handlers partition operations by `team_id` into N bounded channels, serializing same-team writes while different teams run in parallel.
- **Endpoints:** `/identify` (get-or-create person), `/create_alias` (merge two distinct_ids respecting `is_identified`), `/merge` (force-merge N distinct_ids).

## Running

```bash
docker compose up -d
cargo run --release --bin migrate
cargo run --release
```

## Benchmark

```bash
# Reset DB and run the benchmark directly:
bin/reset-db.sh
cargo run --release --bin bench

# Or use the full bootstrap script (tears down Docker, rebuilds, runs):
bin/run-bench.sh
```

Tunable via env vars (defaults in parentheses):

| Env Var | Default | Description |
|---------|---------|-------------|
| `BENCH_TEAMS` | auto (N_WARM/1000) | Number of team_ids |
| `BENCH_WARM` | 100,000 | Phase 1 person count |
| `BENCH_ALIAS` | 100,000 | Phase 2 alias count |
| `BENCH_MERGE` | 100,000 | Phase 3 merge distinct_id count |
| `BENCH_BATCH` | 10 | Phase 3 sub-batch size |
| `BENCH_CHAIN_DEPTH` | 100 | Phase 3b max override chain depth |
| `BENCH_READS` | 1,000,000 | Phase 4 read count |
| `BENCH_DB_POOL` | 50 | Max DB connections |

---

## Benchmark Report (2025-03-25)

Ran on: macOS (darwin/arm64), Postgres 17.9 in Docker (OrbStack), default PG config + `wal_compression=lz4`, `commit_delay=10`, `commit_siblings=5`.

**Total runtime: ~18.2 minutes** (1,091s) for 100K warm + 100K alias + 100K merge + 1M reads.

### Phase Results

| Phase | Ops | Wall Time | Throughput | p50 | p95 | p99 | Max |
|-------|-----|-----------|------------|-----|-----|-----|-----|
| **1. Warm-up** (seed) | 100K | 233.0s | 429 ops/s | — | — | — | — |
| **2. Alias** | 100K | 194.6s | 514 ops/s | 1.56ms | 2.60ms | 7.44ms | 536ms |
| **3. Merge** (per batch of 10) | 10K batches | 163.1s | 61 batch/s | 11.54ms | 27.39ms | 134.5ms | 1.59s |
| **3. Merge** (seed only) | 100K | 225.3s | 444 ops/s | — | — | — | — |
| **4. Read** (resolve) | 1M | 274.8s | 3,639 ops/s | 259µs | 400µs | 533µs | 22.9ms |

### DB Final State

| Table | Rows | Total Size |
|-------|------|------------|
| `persons` | 200K | 39 MB |
| `distinct_ids` | 300K | 47 MB |
| `person_overrides` | 100K | 8.5 MB |

Buffer cache hit rate: **100%** — entire working set fits in the 128MB `shared_buffers`.

### Bottleneck #1: WAL Sync Dominates Write Latency

Every write phase was bottlenecked on **`IO/WalSync`** — Postgres fsyncing the WAL to disk on every transaction commit. Confirmed by sampling `pg_stat_activity` throughout phases 1–3.

The PG config was all defaults: `synchronous_commit=on`, `commit_delay=0`, `wal_compression=off`. For a local Docker container, this makes every commit ~1–2ms of pure fsync overhead.

**Impact:** The 514 ops/s alias throughput is capped almost entirely by fsync latency, not by query execution time.

**Suggestions:**
- For benchmarking, set `synchronous_commit=off` to remove fsync from the critical path (would likely 5–10x write throughput).
- Set `wal_compression=lz4` and `commit_delay=10`µs / `commit_siblings=5` to reduce WAL I/O and help group commits.
- For production realism, keep `synchronous_commit=on` but understand the benchmark is measuring fsync, not application logic.

### Bottleneck #2: Sequential, Single-Connection Benchmark

All phases run sequentially on a **single PG connection** — no concurrency. The pool has 50 connections but only 1 is ever active during writes.

**Impact:** This completely misses how the app works in production (the worker-pool architecture with N concurrent workers, team-id-based sharding). The benchmark can never saturate PG.

**Suggestions:**
- Use `tokio::spawn` or `futures::stream::buffer_unordered` to drive concurrent operations across multiple connections, modeling the HTTP server under load.
- Parameterize concurrency level (e.g. `BENCH_CONCURRENCY`) and measure throughput scaling.

### Bottleneck #3: Merge is N+1 Queries Per Batch

Each merge batch of 10 does: 1 `resolve_tx` for the primary + 10 `resolve_tx` calls for each "other" + up to 10 `INSERT INTO person_overrides` + 1 `UPDATE persons` — all inside one transaction. At p50=11.5ms for a batch of 10, that's ~1.15ms per element.

**Impact:** The merge p99 of 134ms and max of 1.59s suggest occasional contention or checkpoint-induced latency spikes.

**Suggestion:** The per-element `resolve_tx` calls inside the merge loop could be batched into a single query (`WHERE distinct_id = ANY($1)`), reducing round-trips from N+1 to 2.

### Bottleneck #4: Read Phase Bound by Client-Side Serialization

Reads achieved 3,639 ops/s with p50=259µs, but PG-side execution was only 0.072–0.092ms. The gap (~170µs) is Rust-to-PG round-trip overhead per query.

**Suggestions:**
- Add concurrent reads to the benchmark (batch of N in-flight) to pipeline the round-trip latency.
- This would likely push throughput to 30K–50K+ ops/s given the 100% cache hit rate.

### Benchmark Realism Issues

1. **Override chain depth is always 1.** Every merge target is a fresh person never previously merged. Real PostHog workloads create chains of depth 2–5+ when users get re-merged. The recursive CTE's performance with deeper chains is never tested.

2. **Uniform team distribution.** The benchmark distributes exactly 1,000 overrides per team. Real workloads have power-law distributions — a few teams have thousands of persons, most have very few. The bitmap scan on `person_overrides_pkey` read 150M tuples across the run, which would behave differently with skewed distributions.

3. **No contention modeling.** Single-threaded execution means zero concurrent transactions on the same `team_id` — precisely the scenario the worker-pool architecture was designed to handle. Write conflicts, lock waits, and serialization failures are never exercised.

4. **Hot-set bias is invisible at this scale.** The 80/20 hot-set model is good, but at 100% buffer cache hit rate it creates no differentiation. At larger scales (millions of persons) it would matter.

5. **No mixed read/write workload.** Real systems do reads and writes concurrently. The phased approach means reads never compete with writes for connections or locks.

---

## Production Scalability Assessment

Could this system handle **100M persons, 1B distinct_ids, and 100s of requests/second** on production Postgres?

**Short answer: not without significant changes.** The architecture is correct but several things break down at that scale.

### The Recursive CTE Doesn't Scale with Chain Depth

The resolve query does one B-tree index lookup per override hop. At benchmark scale (300K rows, 128MB shared_buffers) this runs in 0.072ms with 100% cache hits. At production scale:

- `distinct_ids` with 1B rows: the PK index on `(team_id, distinct_id)` VARCHAR(200) would be **100–300 GB**. Each B-tree lookup traverses 4–5 levels of 8KB pages.
- `person_overrides` with tens of millions of rows: each recursive iteration does another B-tree traversal.
- A chain of depth 100 = 100 sequential index lookups. If even a few pages are uncached, you're hitting SSD at ~0.1ms per I/O. A single cold resolve at depth 100 could take **10–50ms**.

At 500 RPS, you'd be doing 5,000–50,000 random index lookups/second just for resolves. When the working set exceeds RAM, this falls apart.

**The fundamental problem:** this is a linked-list traversal, not a true union-find. Real union-find achieves amortized O(α(n)) ≈ O(1) via **path compression** — after resolving A→B→C→D, you rewrite A→D, B→D, C→D directly. This implementation never compresses paths, so chains grow monotonically.

### Index Sizes vs Memory

At production scale, rough sizes:

| Table | Rows | Est. Table + Index Size |
|-------|------|------------------------|
| `persons` | 100M | ~15 GB table + ~25 GB indexes |
| `distinct_ids` | 1B | ~100 GB table + ~150 GB indexes |
| `person_overrides` | 50–100M | ~5 GB table + ~5 GB indexes |

Total: **~300 GB**. Even with 32 GB `shared_buffers` (typical production), cache hit rate would plummet from the 100% seen in benchmarks to maybe 10–30% for cold paths. The hot-set would stay cached, but the long tail would hammer disk.

### The Serial Worker Bottleneck

Each team's requests serialize through one worker. If a popular team generates 50 RPS and each op takes 10ms (realistic at scale with uncached reads), that worker maxes out at 100 ops/s — barely enough, and any latency spike cascades.

### The N+1 Merge Pattern

A merge batch of 10 does ~22 queries (1 resolve for primary + 10 resolves + 10 inserts + 1 update) inside one transaction. At depth-100 chains, each resolve does 100 index lookups. A single merge batch could trigger **1,000+ index lookups** and hold a transaction open for 100ms+, blocking all other operations for that team.

### What Would Need to Change for Production

1. **Path compression (critical).** After resolving A→B→C→D, rewrite all intermediate overrides to point directly to the root (A→D, B→D, C→D). This is the core of union-find and bounds effective chain depth to ~1–2 for the vast majority of lookups. Could be done eagerly (during each resolve) or lazily (background flattening job).

2. **Materialized canonical mapping.** Maintain a `canonical_person_id` directly on `distinct_ids`. Reads become a single index lookup. Writes update all affected rows eagerly (fan-out on merge).

3. **Caching layer.** Redis/memcached in front of PG for resolved person IDs. With 80/20 hot-set patterns, a cache with 10M entries would handle 95%+ of reads.

4. **Batch resolve.** Replace per-element `resolve_tx` with a single `WHERE (team_id, distinct_id) IN (...)` + set-based override traversal. Reduces round-trips from N+1 to 2.

5. **Index optimization.** The `persons` PK is `(team_id, person_id)` on VARCHAR(200), but the internal bigint `id` is what everything references. Making `id` the PK and using a UNIQUE constraint on `(team_id, person_id)` only for the upsert path would shrink the primary index dramatically.

6. **Partitioning.** Hash-partitioning `distinct_ids` by `team_id` would keep individual partition indexes small enough to stay cached for active teams.

### Summary

| Concern | Status at Target Scale |
|---------|----------------------|
| Read latency (cold) | Breaks — O(depth) random I/O per resolve |
| Read latency (hot) | OK if cached, but cache hit rate drops |
| Write throughput | Marginal — serial workers + N+1 pattern |
| Override chain growth | Unbounded — no path compression |
| Index sizes vs RAM | ~300 GB vs 32 GB shared_buffers |
| Correctness | Sound — team_id serialization prevents races |

The correctness model (team_id sharding, override chain with recursive CTE) is solid. To serve 100M/1B scale at 100s RPS, the system needs **path compression** (to bound chain depth), a **caching layer** (to avoid hitting PG on every read), and **batch query patterns** (to reduce per-request round-trips).
