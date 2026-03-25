//! Benchmark harness for the union-find service.
//!
//! Exercises the DB layer directly (no HTTP overhead) through four phases:
//!
//!   Phase 1 — Warm-up:  create N_WARM persons via /identify
//!   Phase 2 — Alias:    create N_ALIAS new distinct_ids, alias each to an existing person
//!   Phase 3 — Merge:    create N_MERGE new distinct_ids, merge in sub-batches
//!   Phase 4 — Read:     resolve N_READS random non-primary distinct_ids through override chains
//!
//! Tune via env vars (defaults in parentheses):
//!   BENCH_WARM   (100_000)   — phase 1 person count
//!   BENCH_ALIAS  (100_000)   — phase 2 alias count
//!   BENCH_MERGE  (100_000)   — phase 3 merge distinct_id count
//!   BENCH_BATCH  (10)        — phase 3 sub-batch size
//!   BENCH_READS  (1_000_000) — phase 4 read count
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

const TEAM_ID: i64 = 1;

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
// Helper: create N persons via handle_identify, return distinct_id -> person_id.
// ---------------------------------------------------------------------------

async fn precreate_persons(
    pool: &PgPool,
    prefix: &str,
    count: usize,
) -> HashMap<String, String> {
    let mut mapping = HashMap::with_capacity(count);
    for i in 0..count {
        let did = format!("{prefix}-{i}");
        let resp = db::handle_identify(pool, TEAM_ID, &did)
            .await
            .expect("precreate identify failed");
        mapping.insert(did, resp.person_id);
    }
    mapping
}

// ---------------------------------------------------------------------------
// Pick a primary distinct_id with 80/20 hot-set bias.
// ---------------------------------------------------------------------------

fn pick_primary<'a>(
    rng: &mut impl Rng,
    all_primaries: &'a [String],
    hot_set: &'a [String],
) -> &'a str {
    if rng.random_bool(0.8) {
        &hot_set[rng.random_range(0..hot_set.len())]
    } else {
        &all_primaries[rng.random_range(0..all_primaries.len())]
    }
}

// ---------------------------------------------------------------------------
// Phase 1: warm-up
// ---------------------------------------------------------------------------

async fn phase_warm(pool: &PgPool, n: usize) -> (Vec<String>, Vec<String>) {
    println!("Phase 1: warming up with {n} persons...");
    let t0 = Instant::now();
    let mapping = precreate_persons(pool, "primary", n).await;
    let elapsed = t0.elapsed();
    println!(
        "  created {n} persons in {elapsed:.2?} ({:.0} ops/s)\n",
        n as f64 / elapsed.as_secs_f64()
    );

    let all_primaries: Vec<String> = mapping.keys().cloned().collect();

    // Hot set: 20% of primaries chosen at random.
    let mut rng = rand::rng();
    let hot_count = std::cmp::max(1, n / 5);
    let hot_set: Vec<String> = (0..hot_count)
        .map(|_| all_primaries[rng.random_range(0..all_primaries.len())].clone())
        .collect();

    (all_primaries, hot_set)
}

// ---------------------------------------------------------------------------
// Phase 2: alias benchmark
// ---------------------------------------------------------------------------

async fn phase_alias(
    pool: &PgPool,
    n: usize,
    all_primaries: &[String],
    hot_set: &[String],
) -> Vec<String> {
    println!("Phase 2: aliasing {n} new distinct_ids...");
    let mut rng = rand::rng();
    let mut latencies = Vec::with_capacity(n);
    let mut alias_ids = Vec::with_capacity(n);

    for i in 0..n {
        let alias_id = format!("alias-{i}");
        let primary = pick_primary(&mut rng, all_primaries, hot_set);

        let t0 = Instant::now();
        db::handle_create_alias(pool, TEAM_ID, primary, &alias_id)
            .await
            .expect("create_alias failed");
        latencies.push(t0.elapsed());

        alias_ids.push(alias_id);
    }

    print_stats("create_alias", &compute_stats(latencies));
    alias_ids
}

// ---------------------------------------------------------------------------
// Phase 3: merge benchmark
// ---------------------------------------------------------------------------

async fn phase_merge(
    pool: &PgPool,
    n: usize,
    batch_size: usize,
    all_primaries: &[String],
    hot_set: &[String],
) -> Vec<String> {
    println!("Phase 3: merging {n} distinct_ids in batches of {batch_size}...");
    let mut rng = rand::rng();
    let mut latencies = Vec::with_capacity(n / batch_size + 1);
    let mut merge_ids = Vec::with_capacity(n);

    // Pre-create the merge distinct_ids (each gets its own person).
    println!("  pre-creating {n} merge distinct_ids...");
    let t_pre = Instant::now();
    for i in 0..n {
        let did = format!("merge-{i}");
        db::handle_identify(pool, TEAM_ID, &did)
            .await
            .expect("precreate merge distinct_id failed");
        merge_ids.push(did);
    }
    println!("  pre-created in {:.2?}", t_pre.elapsed());

    // Now merge in sub-batches.
    for chunk in merge_ids.chunks(batch_size) {
        let primary = pick_primary(&mut rng, all_primaries, hot_set);
        let others: Vec<String> = chunk.to_vec();

        let t0 = Instant::now();
        db::handle_merge(pool, TEAM_ID, primary, &others)
            .await
            .expect("merge failed");
        latencies.push(t0.elapsed());
    }

    print_stats("merge (per batch)", &compute_stats(latencies));
    merge_ids
}

// ---------------------------------------------------------------------------
// Phase 4: read benchmark
// ---------------------------------------------------------------------------

async fn phase_read(pool: &PgPool, n: usize, lookup_ids: &[String]) {
    println!("Phase 4: reading {n} random non-primary distinct_ids...");
    let mut rng = rand::rng();
    let mut latencies = Vec::with_capacity(n);

    for _ in 0..n {
        let did = &lookup_ids[rng.random_range(0..lookup_ids.len())];

        let t0 = Instant::now();
        let resolved = db::resolve(pool, TEAM_ID, did)
            .await
            .expect("resolve failed");
        latencies.push(t0.elapsed());

        assert!(resolved.is_some(), "expected {did} to resolve");
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

    println!("=== Union-Find Benchmark ===");
    println!("  BENCH_WARM  = {n_warm}");
    println!("  BENCH_ALIAS = {n_alias}");
    println!("  BENCH_MERGE = {n_merge}");
    println!("  BENCH_BATCH = {batch_size}");
    println!("  BENCH_READS = {n_reads}");
    println!();

    let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5432/union_find".into()
    });

    let pool = PgPoolOptions::new()
        .max_connections(2)
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

    // Phase 1: warm-up
    let (all_primaries, hot_set) = phase_warm(&pool, n_warm).await;

    // Phase 2: alias benchmark
    let alias_ids = phase_alias(&pool, n_alias, &all_primaries, &hot_set).await;

    // Phase 3: merge benchmark (state accumulates on top of phases 1+2)
    let merge_ids = phase_merge(&pool, n_merge, batch_size, &all_primaries, &hot_set).await;

    // Phase 4: read benchmark — resolve alias and merge distinct_ids through override chains.
    let mut lookup_ids = alias_ids;
    lookup_ids.extend(merge_ids);
    phase_read(&pool, n_reads, &lookup_ids).await;

    println!("=== Done ===");
}
