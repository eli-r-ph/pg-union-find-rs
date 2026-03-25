//! Benchmark harness for the union-find service.
//!
//! Exercises the DB layer directly (no HTTP overhead) through four phases:
//!
//!   Phase 1 — Warm-up:  create N_WARM persons via /identify across N_TEAMS
//!   Phase 2 — Alias:    create N_ALIAS new distinct_ids, alias each to an existing person
//!   Phase 3 — Merge:    create N_MERGE new distinct_ids, merge in sub-batches
//!   Phase 4 — Read:     resolve N_READS random non-primary distinct_ids through override chains
//!
//! Tune via env vars (defaults in parentheses):
//!   BENCH_TEAMS   (auto: N_WARM/1000) — number of team_ids to distribute across
//!   BENCH_WARM    (100_000)           — phase 1 person count
//!   BENCH_ALIAS   (100_000)           — phase 2 alias count
//!   BENCH_MERGE   (100_000)           — phase 3 merge distinct_id count
//!   BENCH_BATCH   (10)                — phase 3 sub-batch size
//!   BENCH_READS   (1_000_000)         — phase 4 read count
//!   BENCH_DB_POOL (50)                — max DB connections for the benchmark pool
//!
//! Run:
//!   cargo run --release --bin bench

use std::collections::HashMap;
use std::time::{Duration, Instant};

use rand::Rng;
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
/// Balances WAL-sync amortisation against transaction size.
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

/// Pick a primary that belongs to a specific team. Falls back to any primary
/// in the team if the hot set has none for it.
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

async fn seed_batch(pool: &PgPool, items: &[(i64, String)]) -> Vec<String> {
    let mut tx = pool.begin().await.expect("begin tx");
    let mut person_ids = Vec::with_capacity(items.len());
    for (team_id, did) in items {
        let resp = db::identify_tx(&mut tx, *team_id, did)
            .await
            .expect("identify_tx in seed batch");
        person_ids.push(resp.person_id);
    }
    tx.commit().await.expect("commit seed batch");
    person_ids
}

// ---------------------------------------------------------------------------
// Phase 1: warm-up — create N persons distributed round-robin across teams.
//
// Batched in groups of SEED_TX_BATCH to amortise WAL syncs.
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

    // Build the full list of (team_id, distinct_id) pairs up front, then
    // process in transaction-sized chunks.
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

    // Hot set: 20% of primaries chosen at random.
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
    known: String,
    unknown: String,
}

fn pregen_alias_ops(
    n: usize,
    all_primaries: &[ScopedId],
    hot_set: &[ScopedId],
) -> Vec<AliasOp> {
    let mut rng = rand::rng();
    (0..n)
        .map(|i| {
            let primary = pick_primary(&mut rng, all_primaries, hot_set);
            AliasOp {
                team_id: primary.team_id,
                known: primary.distinct_id.clone(),
                unknown: format!("alias-{i}"),
            }
        })
        .collect()
}

async fn phase_alias(pool: &PgPool, ops: &[AliasOp]) {
    println!("Phase 2: aliasing {} new distinct_ids...", ops.len());
    let mut latencies = Vec::with_capacity(ops.len());

    for op in ops {
        let t0 = Instant::now();
        db::handle_create_alias(pool, op.team_id, &op.known, &op.unknown)
            .await
            .expect("create_alias failed");
        latencies.push(t0.elapsed());
    }

    print_stats("create_alias", &compute_stats(latencies));
}

// ---------------------------------------------------------------------------
// Phase 3: merge benchmark
// ---------------------------------------------------------------------------

struct MergeOp {
    team_id: i64,
    primary: String,
    others: Vec<String>,
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
        merge_by_team
            .entry(*team_id)
            .or_default()
            .push(did.clone());
        all_merge_ids.push(ScopedId {
            team_id: *team_id,
            distinct_id: did.clone(),
        });
    }

    let mut ops = Vec::with_capacity(n / batch_size + 1);
    for (&team_id, dids) in &merge_by_team {
        for chunk in dids.chunks(batch_size) {
            let primary =
                pick_primary_for_team(&mut rng, team_id, hot_by_team, primaries_by_team);
            ops.push(MergeOp {
                team_id,
                primary: primary.to_owned(),
                others: chunk.to_vec(),
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
        db::handle_merge(pool, op.team_id, &op.primary, &op.others)
            .await
            .expect("merge failed");
        latencies.push(t0.elapsed());
    }

    print_stats("merge (per batch)", &compute_stats(latencies));
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
    let n_reads = env_usize("BENCH_READS", 1_000_000);
    let n_teams = env_usize("BENCH_TEAMS", std::cmp::max(1, n_warm / 1000));
    let db_pool_size = env_usize("BENCH_DB_POOL", 50);

    let team_ids: Vec<i64> = (1..=n_teams as i64).collect();

    println!("=== Union-Find Benchmark ===");
    println!("  BENCH_TEAMS   = {n_teams}");
    println!("  BENCH_WARM    = {n_warm}");
    println!("  BENCH_ALIAS   = {n_alias}");
    println!("  BENCH_MERGE   = {n_merge}");
    println!("  BENCH_BATCH   = {batch_size}");
    println!("  BENCH_READS   = {n_reads}");
    println!("  BENCH_DB_POOL = {db_pool_size}");
    println!();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:54320/union_find".into());

    let pool = PgPoolOptions::new()
        .max_connections(db_pool_size as u32)
        .connect(&database_url)
        .await
        .expect("failed to connect to database");

    // Run migrations then clean slate.
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations");

    sqlx::query("TRUNCATE persons, distinct_ids, person_overrides")
        .execute(&pool)
        .await
        .expect("failed to truncate");

    // Phase 1: warm-up (seeding, not latency-benchmarked)
    let warm = phase_warm(&pool, n_warm, &team_ids).await;

    // Pregenerate all operation data so timed loops measure only DB work.
    let t_pregen = Instant::now();

    let alias_ops = pregen_alias_ops(n_alias, &warm.all_primaries, &warm.hot_set);
    let merge_pregen = pregen_merge(
        n_merge,
        batch_size,
        &team_ids,
        &warm.primaries_by_team,
        &warm.hot_by_team,
    );

    // Build the read lookup pool from alias + merge distinct_ids.
    let mut lookup_ids: Vec<ScopedId> = alias_ops
        .iter()
        .map(|op| ScopedId {
            team_id: op.team_id,
            distinct_id: op.unknown.clone(),
        })
        .collect();
    lookup_ids.extend(merge_pregen.all_merge_ids.iter().cloned());

    let read_indices = pregen_read_indices(n_reads, lookup_ids.len());

    println!(
        "pregenerated {} alias + {} merge + {} read ops in {:.2?}\n",
        alias_ops.len(),
        merge_pregen.ops.len(),
        read_indices.len(),
        t_pregen.elapsed(),
    );

    // Phase 2: alias benchmark
    phase_alias(&pool, &alias_ops).await;

    // Phase 3: merge benchmark (state accumulates on top of phases 1+2)
    phase_merge(&pool, &merge_pregen).await;

    // Phase 4: read benchmark — resolve alias and merge distinct_ids through override chains.
    phase_read(&pool, &lookup_ids, &read_indices).await;

    println!("=== Done ===");
}
