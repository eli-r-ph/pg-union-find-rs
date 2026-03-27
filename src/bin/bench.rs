//! Benchmark harness for the union-find service.
//!
//! Exercises the DB layer directly (no HTTP overhead) through four phases:
//!
//!   Phase 1 — Warm-up:  create N_WARM persons via /create across N_TEAMS
//!   Phase 2 — Alias:    create N_ALIAS new distinct_ids, alias each to an existing person
//!   Phase 3 — Merge:    create N_MERGE new distinct_ids, merge in sub-batches
//!   Phase 4 — Read:     resolve N_READS random non-primary distinct_ids through union_find chains
//!
//! Tune via env vars (defaults in parentheses):
//!   BENCH_TEAMS       (auto: N_WARM/1000) — number of team_ids to distribute across
//!   BENCH_WARM        (100_000)           — phase 1 person count
//!   BENCH_ALIAS       (100_000)           — phase 2 alias count
//!   BENCH_MERGE       (100_000)           — phase 3 merge distinct_id count
//!   BENCH_BATCH       (10)                — phase 3 sub-batch size
//!   BENCH_CHAIN_DEPTH (100)               — phase 3b: max override chain depth
//!   BENCH_READS       (1_000_000)         — phase 4 read count
//!   BENCH_DB_POOL     (50)                — max DB connections for the benchmark pool
//!
//! Run:
//!   cargo run --release --bin bench

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rand::Rng;
use rand::seq::SliceRandom;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use pg_union_find_rs::db;

// ---------------------------------------------------------------------------
// Configurable sizes — override with env vars for large runs.
// ---------------------------------------------------------------------------

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.replace('_', "").parse().ok())
        .unwrap_or(default)
}

/// Transaction batch size for bulk seeding (warm-up & merge precreation).
const SEED_TX_BATCH: usize = 500;

// ---------------------------------------------------------------------------
// A team-scoped distinct_id used throughout the benchmark.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ScopedId {
    team_id: i64,
    distinct_id: String,
}

// ---------------------------------------------------------------------------
// Latency stats
// ---------------------------------------------------------------------------

struct Stats {
    total: Duration,
    count: usize,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
}

fn compute_stats(mut latencies: Vec<Duration>) -> Stats {
    latencies.sort();
    let n = latencies.len();
    let total: Duration = latencies.iter().sum();
    Stats {
        total,
        count: n,
        p50: latencies[n / 2],
        p95: latencies[n * 95 / 100],
        p99: latencies[n * 99 / 100],
        max: latencies[n - 1],
    }
}

fn print_stats(label: &str, stats: &Stats) {
    let ops_sec = if stats.total.as_secs_f64() > 0.0 {
        stats.count as f64 / stats.total.as_secs_f64()
    } else {
        0.0
    };
    println!("  [{label}]");
    println!("    ops:    {}", stats.count);
    println!("    total:  {:.2?}", stats.total);
    println!("    ops/s:  {ops_sec:.0}");
    println!("    p50:    {:.2?}", stats.p50);
    println!("    p95:    {:.2?}", stats.p95);
    println!("    p99:    {:.2?}", stats.p99);
    println!("    max:    {:.2?}", stats.max);
    println!();
}

// ---------------------------------------------------------------------------
// Pick a primary with 80/20 hot-set bias.
// ---------------------------------------------------------------------------

fn pick_primary<'a>(
    rng: &mut impl Rng,
    all_primaries: &'a [ScopedId],
    hot_set: &'a [ScopedId],
) -> &'a ScopedId {
    if rng.random_bool(0.8) {
        &hot_set[rng.random_range(0..hot_set.len())]
    } else {
        &all_primaries[rng.random_range(0..all_primaries.len())]
    }
}

/// Pick a primary that belongs to a specific team.
fn pick_primary_for_team<'a>(
    rng: &mut impl Rng,
    team_id: i64,
    hot_by_team: &'a HashMap<i64, Vec<String>>,
    primaries_by_team: &'a HashMap<i64, Vec<String>>,
) -> &'a str {
    if rng.random_bool(0.8)
        && let Some(hot) = hot_by_team.get(&team_id)
        && !hot.is_empty()
    {
        return &hot[rng.random_range(0..hot.len())];
    }
    let team_prims = primaries_by_team
        .get(&team_id)
        .expect("team has no primaries");
    &team_prims[rng.random_range(0..team_prims.len())]
}

// ---------------------------------------------------------------------------
// Batched seeding helper — runs identify_tx for a slice of (team_id, did)
// pairs inside a single transaction (one WAL sync per batch).
// ---------------------------------------------------------------------------

async fn seed_batch(pool: &PgPool, items: &[(i64, String)]) {
    let mut tx = pool.begin().await.expect("begin tx");
    for (team_id, did) in items {
        db::identify_tx(&mut tx, *team_id, did)
            .await
            .expect("identify_tx in seed batch");
    }
    tx.commit().await.expect("commit seed batch");
}

// ---------------------------------------------------------------------------
// Phase 1: warm-up — create N persons distributed round-robin across teams.
// ---------------------------------------------------------------------------

struct WarmupResult {
    all_primaries: Vec<ScopedId>,
    hot_set: Vec<ScopedId>,
    primaries_by_team: HashMap<i64, Vec<String>>,
    hot_by_team: HashMap<i64, Vec<String>>,
}

async fn phase_warm(pool: &PgPool, n: usize, team_ids: &[i64]) -> WarmupResult {
    println!(
        "Phase 1: warming up with {n} persons across {} teams (tx batch {SEED_TX_BATCH})...",
        team_ids.len()
    );
    let t0 = Instant::now();

    let mut all_primaries = Vec::with_capacity(n);
    let mut primaries_by_team: HashMap<i64, Vec<String>> = HashMap::new();

    let pairs: Vec<(i64, String)> = (0..n)
        .map(|i| (team_ids[i % team_ids.len()], format!("primary-{i}")))
        .collect();

    for chunk in pairs.chunks(SEED_TX_BATCH) {
        seed_batch(pool, chunk).await;

        for (team_id, did) in chunk {
            all_primaries.push(ScopedId {
                team_id: *team_id,
                distinct_id: did.clone(),
            });
            primaries_by_team
                .entry(*team_id)
                .or_default()
                .push(did.clone());
        }
    }

    let elapsed = t0.elapsed();
    println!(
        "  created {n} persons in {elapsed:.2?} ({:.0} ops/s)\n",
        n as f64 / elapsed.as_secs_f64()
    );

    let mut rng = rand::rng();
    let hot_count = std::cmp::max(1, n / 5);
    let hot_set: Vec<ScopedId> = (0..hot_count)
        .map(|_| all_primaries[rng.random_range(0..all_primaries.len())].clone())
        .collect();

    let mut hot_by_team: HashMap<i64, Vec<String>> = HashMap::new();
    for s in &hot_set {
        hot_by_team
            .entry(s.team_id)
            .or_default()
            .push(s.distinct_id.clone());
    }

    WarmupResult {
        all_primaries,
        hot_set,
        primaries_by_team,
        hot_by_team,
    }
}

// ---------------------------------------------------------------------------
// Phase 2: alias benchmark
// ---------------------------------------------------------------------------

struct AliasOp {
    team_id: i64,
    src: String,
    dest: String,
}

/// Pregenerate alias operations covering three of the four alias cases:
///   - ~90% Case 1a: src (primary) exists, dest is new
///   - ~5%  Case 2a: both exist, same person (src == dest are the same primary)
///   - ~5%  Case 3:  neither exists (both src and dest are fresh)
struct AliasPregen {
    ops: Vec<AliasOp>,
    fresh_dest_ids: Vec<ScopedId>,
}

fn pregen_alias_ops(
    n: usize,
    all_primaries: &[ScopedId],
    hot_set: &[ScopedId],
    team_ids: &[i64],
) -> AliasPregen {
    let mut rng = rand::rng();
    let n_case2a = n / 20; // 5%
    let n_case3 = n / 20; // 5%
    let n_case1a = n - n_case2a - n_case3;

    let mut ops = Vec::with_capacity(n);
    let mut fresh_dest_ids = Vec::new();

    // Case 1a: src exists (primary), dest is new
    for i in 0..n_case1a {
        let primary = pick_primary(&mut rng, all_primaries, hot_set);
        ops.push(AliasOp {
            team_id: primary.team_id,
            src: primary.distinct_id.clone(),
            dest: format!("alias-{i}"),
        });
    }

    // Case 2a: both exist, same person (use same primary for both — no-op)
    for _ in 0..n_case2a {
        let primary = pick_primary(&mut rng, all_primaries, hot_set);
        ops.push(AliasOp {
            team_id: primary.team_id,
            src: primary.distinct_id.clone(),
            dest: primary.distinct_id.clone(),
        });
    }

    // Case 3: neither exists — both are fresh distinct_ids
    for i in 0..n_case3 {
        let team_id = team_ids[rng.random_range(0..team_ids.len())];
        let src_id = format!("fresh-src-{i}");
        let dest_id = format!("fresh-dest-{i}");
        fresh_dest_ids.push(ScopedId {
            team_id,
            distinct_id: dest_id.clone(),
        });
        ops.push(AliasOp {
            team_id,
            src: src_id,
            dest: dest_id,
        });
    }

    AliasPregen {
        ops,
        fresh_dest_ids,
    }
}

async fn phase_alias(pool: &PgPool, ops: &[AliasOp]) {
    println!("Phase 2: aliasing {} distinct_ids (mixed cases)...", ops.len());
    let mut latencies = Vec::with_capacity(ops.len());

    for op in ops {
        let t0 = Instant::now();
        db::handle_alias(pool, op.team_id, &op.src, &op.dest)
            .await
            .expect("alias failed");
        latencies.push(t0.elapsed());
    }

    print_stats("alias", &compute_stats(latencies));
}

// ---------------------------------------------------------------------------
// Phase 3: merge benchmark
// ---------------------------------------------------------------------------

struct MergeOp {
    team_id: i64,
    src: String,
    dests: Vec<String>,
}

struct MergePregen {
    seed_pairs: Vec<(i64, String)>,
    ops: Vec<MergeOp>,
    all_merge_ids: Vec<ScopedId>,
}

fn pregen_merge(
    n: usize,
    batch_size: usize,
    team_ids: &[i64],
    primaries_by_team: &HashMap<i64, Vec<String>>,
    hot_by_team: &HashMap<i64, Vec<String>>,
) -> MergePregen {
    let mut rng = rand::rng();

    let seed_pairs: Vec<(i64, String)> = (0..n)
        .map(|i| (team_ids[i % team_ids.len()], format!("merge-{i}")))
        .collect();

    let mut merge_by_team: HashMap<i64, Vec<String>> = HashMap::new();
    let mut all_merge_ids = Vec::with_capacity(n);

    for (team_id, did) in &seed_pairs {
        merge_by_team.entry(*team_id).or_default().push(did.clone());
        all_merge_ids.push(ScopedId {
            team_id: *team_id,
            distinct_id: did.clone(),
        });
    }

    let mut ops = Vec::with_capacity(n / batch_size + 1);
    for (&team_id, dids) in &merge_by_team {
        for chunk in dids.chunks(batch_size) {
            let src = pick_primary_for_team(&mut rng, team_id, hot_by_team, primaries_by_team);
            ops.push(MergeOp {
                team_id,
                src: src.to_owned(),
                dests: chunk.to_vec(),
            });
        }
    }

    MergePregen {
        seed_pairs,
        ops,
        all_merge_ids,
    }
}

async fn phase_merge(pool: &PgPool, pregen: &MergePregen) {
    let n = pregen.seed_pairs.len();
    let n_ops = pregen.ops.len();
    println!("Phase 3: merging {n} distinct_ids in {n_ops} batches...");

    println!("  seeding {n} merge distinct_ids (tx batch {SEED_TX_BATCH})...");
    let t_pre = Instant::now();
    for chunk in pregen.seed_pairs.chunks(SEED_TX_BATCH) {
        seed_batch(pool, chunk).await;
    }
    println!("  seeded in {:.2?}", t_pre.elapsed());

    let mut latencies = Vec::with_capacity(n_ops);
    for op in &pregen.ops {
        let t0 = Instant::now();
        db::handle_merge(pool, op.team_id, &op.src, &op.dests)
            .await
            .expect("merge failed");
        latencies.push(t0.elapsed());
    }

    print_stats("merge (per batch)", &compute_stats(latencies));
}

// ---------------------------------------------------------------------------
// Phase 3b: chain deepening — merge primaries into each other to create
// union_find chains of realistic, varying depths.
// ---------------------------------------------------------------------------

fn generate_chain_lengths(rng: &mut impl Rng, n_primaries: usize, max_depth: usize) -> Vec<usize> {
    if max_depth <= 1 || n_primaries <= 1 {
        return vec![1; n_primaries];
    }

    let mut lengths = Vec::new();
    let mut remaining = n_primaries;
    let ln_max = (max_depth as f64).ln();

    if remaining >= max_depth {
        lengths.push(max_depth);
        remaining -= max_depth;
    }

    while remaining > 0 {
        let u: f64 = rng.random();
        let len = ((u * ln_max).exp().ceil() as usize).clamp(1, remaining.min(max_depth));
        lengths.push(len);
        remaining -= len;
    }

    lengths
}

async fn phase_chain_deepen(
    pool: &PgPool,
    max_depth: usize,
    primaries_by_team: &HashMap<i64, Vec<String>>,
) {
    if max_depth <= 1 {
        println!("Phase 3b: chain deepening skipped (max_depth=1)\n");
        return;
    }

    let mut rng = rand::rng();

    let mut team_plans: Vec<(i64, Vec<Vec<String>>)> = Vec::new();
    let mut total_chains = 0usize;
    let mut total_links = 0usize;
    let mut max_actual = 0usize;
    let mut depth_counts: Vec<usize> = vec![0; max_depth + 1];

    for (&team_id, prims) in primaries_by_team {
        let mut shuffled = prims.clone();
        shuffled.shuffle(&mut rng);

        let chain_lengths = generate_chain_lengths(&mut rng, shuffled.len(), max_depth);

        let mut offset = 0;
        let mut chains = Vec::with_capacity(chain_lengths.len());
        for len in &chain_lengths {
            let chain: Vec<String> = shuffled[offset..offset + len].to_vec();
            offset += len;
            if chain.len() > 1 {
                total_links += chain.len() - 1;
                if chain.len() > max_actual {
                    max_actual = chain.len();
                }
            }
            depth_counts[chain.len().saturating_sub(1)] += 1;
            chains.push(chain);
        }
        total_chains += chains.len();
        team_plans.push((team_id, chains));
    }

    println!(
        "Phase 3b: deepening union_find chains (max depth {max_depth}, \
         {total_chains} chains, {total_links} link ops, deepest {max_actual})..."
    );

    let buckets: &[(usize, usize)] = &[
        (0, 0),
        (1, 1),
        (2, 4),
        (5, 9),
        (10, 24),
        (25, 49),
        (50, 99),
        (100, max_depth),
    ];
    println!("  chain depth distribution (union_find hops):");
    for &(lo, hi) in buckets {
        if lo > max_depth {
            break;
        }
        let hi = hi.min(max_depth);
        let count: usize = depth_counts[lo..=hi].iter().sum();
        if count > 0 {
            if lo == hi {
                println!("    depth {lo:>6}: {count} chains");
            } else {
                println!("    depth {lo:>3}-{hi:<3}: {count} chains");
            }
        }
    }

    let t0 = Instant::now();
    let mut latencies = Vec::with_capacity(total_links);

    for (team_id, chains) in &team_plans {
        for chain in chains {
            for i in 0..chain.len().saturating_sub(1) {
                let t_op = Instant::now();
                db::handle_merge(pool, *team_id, &chain[i + 1], &[chain[i].clone()])
                    .await
                    .expect("chain deepen merge failed");
                latencies.push(t_op.elapsed());
            }
        }
    }

    if !latencies.is_empty() {
        print_stats("chain deepen", &compute_stats(latencies));
    }
    println!("  total chain deepening wall time: {:.2?}\n", t0.elapsed());
}

// ---------------------------------------------------------------------------
// Phase 4: read benchmark
// ---------------------------------------------------------------------------

fn pregen_read_indices(n: usize, pool_size: usize) -> Vec<usize> {
    let mut rng = rand::rng();
    (0..n).map(|_| rng.random_range(0..pool_size)).collect()
}

async fn phase_read(pool: &PgPool, lookup_ids: &[ScopedId], indices: &[usize]) {
    let n = indices.len();
    println!("Phase 4: reading {n} random non-primary distinct_ids...");
    let mut latencies = Vec::with_capacity(n);

    for &idx in indices {
        let sid = &lookup_ids[idx];

        let t0 = Instant::now();
        let resolved = db::resolve(pool, sid.team_id, &sid.distinct_id)
            .await
            .expect("resolve failed");
        latencies.push(t0.elapsed());

        assert!(
            resolved.is_some(),
            "expected {}:{} to resolve",
            sid.team_id,
            sid.distinct_id
        );
    }

    print_stats("resolve (read)", &compute_stats(latencies));
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let n_warm = env_usize("BENCH_WARM", 100_000);
    let n_alias = env_usize("BENCH_ALIAS", 100_000);
    let n_merge = env_usize("BENCH_MERGE", 100_000);
    let batch_size = env_usize("BENCH_BATCH", 10);
    let chain_depth = env_usize("BENCH_CHAIN_DEPTH", 100);
    let n_reads = env_usize("BENCH_READS", 1_000_000);
    let n_teams = env_usize("BENCH_TEAMS", std::cmp::max(1, n_warm / 1000));
    let db_pool_size = env_usize("BENCH_DB_POOL", 50);

    let team_ids: Vec<i64> = (1..=n_teams as i64).collect();

    println!("=== Union-Find Benchmark ===");
    println!("  BENCH_TEAMS       = {n_teams}");
    println!("  BENCH_WARM        = {n_warm}");
    println!("  BENCH_ALIAS       = {n_alias}");
    println!("  BENCH_MERGE       = {n_merge}");
    println!("  BENCH_BATCH       = {batch_size}");
    println!("  BENCH_CHAIN_DEPTH = {chain_depth}");
    println!("  BENCH_READS       = {n_reads}");
    println!("  BENCH_DB_POOL     = {db_pool_size}");
    println!();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:54320/union_find".into());

    let pool = PgPoolOptions::new()
        .max_connections(db_pool_size as u32)
        .connect(&database_url)
        .await
        .expect("failed to connect to database");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations");

    sqlx::query("TRUNCATE person_mapping, distinct_id_mappings, union_find")
        .execute(&pool)
        .await
        .expect("failed to truncate");

    // Phase 1: warm-up (seeding, not latency-benchmarked)
    let warm = phase_warm(&pool, n_warm, &team_ids).await;

    // Pregenerate all operation data so timed loops measure only DB work.
    let t_pregen = Instant::now();

    let alias_pregen = pregen_alias_ops(n_alias, &warm.all_primaries, &warm.hot_set, &team_ids);
    let merge_pregen = pregen_merge(
        n_merge,
        batch_size,
        &team_ids,
        &warm.primaries_by_team,
        &warm.hot_by_team,
    );

    // Build read lookup pool from alias dest distinct_ids (Case 1a only —
    // Case 2a are no-ops on existing primaries, Case 3 fresh dests are added
    // separately below).
    let mut lookup_ids: Vec<ScopedId> = alias_pregen
        .ops
        .iter()
        .filter(|op| op.src != op.dest) // skip Case 2a no-ops
        .map(|op| ScopedId {
            team_id: op.team_id,
            distinct_id: op.dest.clone(),
        })
        .collect();
    lookup_ids.extend(alias_pregen.fresh_dest_ids.iter().cloned());
    lookup_ids.extend(merge_pregen.all_merge_ids.iter().cloned());

    let read_indices = pregen_read_indices(n_reads, lookup_ids.len());

    println!(
        "pregenerated {} alias + {} merge + {} read ops in {:.2?}\n",
        alias_pregen.ops.len(),
        merge_pregen.ops.len(),
        read_indices.len(),
        t_pregen.elapsed(),
    );

    // Phase 2: alias benchmark
    phase_alias(&pool, &alias_pregen.ops).await;

    // Phase 3: merge benchmark
    phase_merge(&pool, &merge_pregen).await;

    // Phase 3b: chain deepening
    phase_chain_deepen(&pool, chain_depth, &warm.primaries_by_team).await;

    // Phase 4: read benchmark
    phase_read(&pool, &lookup_ids, &read_indices).await;

    println!("=== Done ===");
}
