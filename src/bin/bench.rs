//! Benchmark harness for the union-find service.
//!
//! Sends parallel HTTP requests to a running server instance and measures
//! throughput and latency. State is pre-seeded via direct DB calls so the
//! timed sections only measure server performance under concurrent load.
//!
//!   Phase 1  — Warm-up:  batch-seed N_WARM persons via identify_tx across N_TEAMS (DB-direct, untimed)
//!   Phase 1b — Create:   benchmark N_CREATE individual POST /create calls (80% new, 20% existing)
//!   Phase 2  — Alias:    run N_ALIAS alias ops via POST /alias (mixed cases)
//!   Phase 3  — Merge:    seed N_MERGE distinct_ids, then merge in sub-batches via POST /merge
//!   Phase 3a — Batched Merge: seed N distinct_ids, merge via POST /batched_merge (same batch size)
//!   Phase 3b — Deepen:   merge targets into each other for realistic chain depths (DB-direct, untimed)
//!   Phase 4  — Read:     resolve N_READS random distinct_ids via POST /resolve
//!   Phase 4a — Resolve distinct IDs: resolve N person_uuids via POST /resolve_distinct_ids
//!   Phase 5  — Delete distinct_id: benchmark N delete_distinct_id calls via POST /delete_distinct_id
//!   Phase 6  — Delete person: benchmark N delete_person calls via POST /delete_person
//!
//! Tune via env vars (defaults in parentheses):
//!   BENCH_TEAMS            (auto: N_WARM/1000)  — number of team_ids to distribute across
//!   BENCH_WARM             (100_000)            — phase 1 person count
//!   BENCH_CREATE           (50_000)             — phase 1b create count
//!   BENCH_ALIAS            (100_000)            — phase 2 alias count
//!   BENCH_MERGE            (100_000)            — phase 3 merge distinct_id count
//!   BENCH_BATCHED_MERGE    (100_000)            — phase 3a batched merge distinct_id count
//!   BENCH_BATCH            (10)                 — phase 3/3a sub-batch size
//!   BENCH_CHAIN_DEPTH      (100)                — phase 3b: max override chain depth
//!   BENCH_READS            (1_000_000)          — phase 4 read count
//!   BENCH_RESOLVE_DIDS     (100_000)            — phase 4a resolve_distinct_ids count
//!   BENCH_DELETE_DID       (10_000)             — phase 5 delete_distinct_id count
//!   BENCH_DELETE_PERSON    (10_000)             — phase 6 delete_person count
//!   BENCH_DB_POOL          (50)                 — max DB connections for seeding pool
//!   BENCH_CONCURRENCY      (50)                 — max in-flight HTTP requests
//!   BENCH_BASE_URL         (http://127.0.0.1:3000) — server base URL
//!
//! Run:
//!   cargo run --release --bin bench

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;
use rand::seq::SliceRandom;
use reqwest::Client;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::Semaphore;

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

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

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
// Latency stats with failure tracking
// ---------------------------------------------------------------------------

struct Stats {
    wall_time: Duration,
    count: usize,
    failures: u64,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
}

fn compute_stats(mut latencies: Vec<Duration>, wall_time: Duration, failures: u64) -> Stats {
    latencies.sort();
    let n = latencies.len();
    if n == 0 {
        return Stats {
            wall_time,
            count: 0,
            failures,
            p50: Duration::ZERO,
            p95: Duration::ZERO,
            p99: Duration::ZERO,
            max: Duration::ZERO,
        };
    }
    Stats {
        wall_time,
        count: n,
        failures,
        p50: latencies[n / 2],
        p95: latencies[n * 95 / 100],
        p99: latencies[n * 99 / 100],
        max: latencies[n - 1],
    }
}

fn print_stats(label: &str, stats: &Stats) {
    let total_ops = stats.count as u64 + stats.failures;
    let ops_sec = if stats.wall_time.as_secs_f64() > 0.0 {
        total_ops as f64 / stats.wall_time.as_secs_f64()
    } else {
        0.0
    };
    let fail_pct = if total_ops > 0 {
        stats.failures as f64 / total_ops as f64 * 100.0
    } else {
        0.0
    };
    println!("  [{label}]");
    println!("    ops:       {total_ops}");
    println!("    failures:  {} ({fail_pct:.2}%)", stats.failures);
    println!("    wall time: {:.2?}", stats.wall_time);
    println!("    ops/s:     {ops_sec:.0}");
    println!("    p50:       {:.2?}", stats.p50);
    println!("    p95:       {:.2?}", stats.p95);
    println!("    p99:       {:.2?}", stats.p99);
    println!("    max:       {:.2?}", stats.max);
    println!();
}

// ---------------------------------------------------------------------------
// Pick a target with 80/20 hot-set bias.
// ---------------------------------------------------------------------------

fn pick_target<'a>(
    rng: &mut impl Rng,
    all_targets: &'a [ScopedId],
    hot_set: &'a [ScopedId],
) -> &'a ScopedId {
    if rng.random_bool(0.8) {
        &hot_set[rng.random_range(0..hot_set.len())]
    } else {
        &all_targets[rng.random_range(0..all_targets.len())]
    }
}

fn pick_target_for_team<'a>(
    rng: &mut impl Rng,
    team_id: i64,
    hot_by_team: &'a HashMap<i64, Vec<String>>,
    targets_by_team: &'a HashMap<i64, Vec<String>>,
) -> &'a str {
    if rng.random_bool(0.8)
        && let Some(hot) = hot_by_team.get(&team_id)
        && !hot.is_empty()
    {
        return &hot[rng.random_range(0..hot.len())];
    }
    let team_tgts = targets_by_team.get(&team_id).expect("team has no targets");
    &team_tgts[rng.random_range(0..team_tgts.len())]
}

// ---------------------------------------------------------------------------
// Parallel HTTP runner — sends requests with bounded concurrency.
// ---------------------------------------------------------------------------

struct OpResult {
    latency: Duration,
    success: bool,
}

async fn run_parallel(
    client: &Client,
    sem: &Arc<Semaphore>,
    requests: Vec<(String, serde_json::Value)>,
) -> (Vec<Duration>, u64) {
    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(requests.len());

    for (url, body) in requests {
        let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
        let client = client.clone();
        handles.push(tokio::spawn(async move {
            let t0 = Instant::now();
            let result = client.post(&url).json(&body).send().await;
            let latency = t0.elapsed();
            drop(permit);
            let success = match result {
                Ok(resp) => resp.status().is_success(),
                Err(_) => false,
            };
            OpResult { latency, success }
        }));
    }

    let mut latencies = Vec::with_capacity(handles.len());
    let mut failures = 0u64;
    for handle in handles {
        let result = handle.await.expect("task panicked");
        if result.success {
            latencies.push(result.latency);
        } else {
            failures += 1;
        }
    }

    let _ = wall_start; // wall_time computed by caller
    (latencies, failures)
}

// ---------------------------------------------------------------------------
// Batched seeding helper — DB-direct, not timed.
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

async fn seed_parallel(pool: &PgPool, pairs: &[(i64, String)]) {
    let mut set = tokio::task::JoinSet::new();
    for chunk in pairs.chunks(SEED_TX_BATCH) {
        let pool = pool.clone();
        let chunk: Vec<(i64, String)> = chunk.to_vec();
        set.spawn(async move { seed_batch(&pool, &chunk).await });
    }
    while let Some(result) = set.join_next().await {
        result.expect("seed task panicked");
    }
}

/// Like seed_parallel but returns (team_id, person_uuid) for each seeded row.
async fn seed_parallel_with_uuids(pool: &PgPool, pairs: &[(i64, String)]) -> Vec<(i64, String)> {
    let mut set = tokio::task::JoinSet::new();
    for chunk in pairs.chunks(SEED_TX_BATCH) {
        let pool = pool.clone();
        let chunk: Vec<(i64, String)> = chunk.to_vec();
        set.spawn(async move {
            let mut results = Vec::with_capacity(chunk.len());
            let mut tx = pool.begin().await.expect("begin tx");
            for (team_id, did) in &chunk {
                let resolved = db::identify_tx(&mut tx, *team_id, did)
                    .await
                    .expect("identify_tx in seed batch");
                results.push((*team_id, resolved.person_uuid));
            }
            tx.commit().await.expect("commit seed batch");
            results
        });
    }
    let mut all_results = Vec::with_capacity(pairs.len());
    while let Some(result) = set.join_next().await {
        all_results.extend(result.expect("seed task panicked"));
    }
    all_results
}

// ---------------------------------------------------------------------------
// Phase 1: warm-up — create N persons distributed round-robin across teams.
// ---------------------------------------------------------------------------

struct WarmupResult {
    all_targets: Vec<ScopedId>,
    hot_set: Vec<ScopedId>,
    targets_by_team: HashMap<i64, Vec<String>>,
    hot_by_team: HashMap<i64, Vec<String>>,
}

async fn phase_warm(pool: &PgPool, n: usize, team_ids: &[i64]) -> WarmupResult {
    println!(
        "Phase 1: warming up with {n} persons across {} teams (tx batch {SEED_TX_BATCH})...",
        team_ids.len()
    );
    let t0 = Instant::now();

    let mut all_targets = Vec::with_capacity(n);
    let mut targets_by_team: HashMap<i64, Vec<String>> = HashMap::new();

    let pairs: Vec<(i64, String)> = (0..n)
        .map(|i| (team_ids[i % team_ids.len()], format!("primary-{i}")))
        .collect();

    seed_parallel(pool, &pairs).await;

    for (team_id, did) in &pairs {
        all_targets.push(ScopedId {
            team_id: *team_id,
            distinct_id: did.clone(),
        });
        targets_by_team
            .entry(*team_id)
            .or_default()
            .push(did.clone());
    }

    let elapsed = t0.elapsed();
    println!(
        "  created {n} persons in {elapsed:.2?} ({:.0} ops/s)\n",
        n as f64 / elapsed.as_secs_f64()
    );

    let mut rng = rand::rng();
    let hot_count = std::cmp::max(1, n / 5);
    let hot_set: Vec<ScopedId> = (0..hot_count)
        .map(|_| all_targets[rng.random_range(0..all_targets.len())].clone())
        .collect();

    let mut hot_by_team: HashMap<i64, Vec<String>> = HashMap::new();
    for s in &hot_set {
        hot_by_team
            .entry(s.team_id)
            .or_default()
            .push(s.distinct_id.clone());
    }

    WarmupResult {
        all_targets,
        hot_set,
        targets_by_team,
        hot_by_team,
    }
}

// ---------------------------------------------------------------------------
// Phase 1b: /create benchmark — parallel HTTP POST /create
// ---------------------------------------------------------------------------

fn pregen_create_ops(
    n: usize,
    all_targets: &[ScopedId],
    hot_set: &[ScopedId],
    team_ids: &[i64],
) -> Vec<ScopedId> {
    let mut rng = rand::rng();
    let n_new = n * 4 / 5;
    let n_get = n - n_new;

    let mut ops = Vec::with_capacity(n);

    for i in 0..n_new {
        let team_id = team_ids[i % team_ids.len()];
        ops.push(ScopedId {
            team_id,
            distinct_id: format!("create-{i}"),
        });
    }

    for _ in 0..n_get {
        let target = pick_target(&mut rng, all_targets, hot_set);
        ops.push(target.clone());
    }

    ops.shuffle(&mut rng);
    ops
}

async fn phase_create(client: &Client, sem: &Arc<Semaphore>, base_url: &str, ops: &[ScopedId]) {
    println!("Phase 1b: benchmarking {} POST /create calls...", ops.len());

    let url = format!("{base_url}/create");
    let requests: Vec<(String, serde_json::Value)> = ops
        .iter()
        .map(|op| {
            (
                url.clone(),
                serde_json::json!({
                    "team_id": op.team_id,
                    "distinct_id": op.distinct_id,
                }),
            )
        })
        .collect();

    let wall_start = Instant::now();
    let (latencies, failures) = run_parallel(client, sem, requests).await;
    let wall_time = wall_start.elapsed();

    print_stats("create", &compute_stats(latencies, wall_time, failures));
}

// ---------------------------------------------------------------------------
// Phase 2: alias benchmark — parallel HTTP POST /alias
// ---------------------------------------------------------------------------

struct AliasOp {
    team_id: i64,
    target: String,
    source: String,
}

struct AliasPregen {
    ops: Vec<AliasOp>,
}

fn pregen_alias_ops(
    n: usize,
    all_targets: &[ScopedId],
    hot_set: &[ScopedId],
    team_ids: &[i64],
) -> AliasPregen {
    let mut rng = rand::rng();
    let n_tgt_eq_src = n / 20;
    let n_case3 = n / 20;
    let n_case1a = n - n_tgt_eq_src - n_case3;

    let mut ops = Vec::with_capacity(n);

    for i in 0..n_case1a {
        let tgt = pick_target(&mut rng, all_targets, hot_set);
        ops.push(AliasOp {
            team_id: tgt.team_id,
            target: tgt.distinct_id.clone(),
            source: format!("alias-{i}"),
        });
    }

    for _ in 0..n_tgt_eq_src {
        let tgt = pick_target(&mut rng, all_targets, hot_set);
        ops.push(AliasOp {
            team_id: tgt.team_id,
            target: tgt.distinct_id.clone(),
            source: tgt.distinct_id.clone(),
        });
    }

    for i in 0..n_case3 {
        let team_id = team_ids[rng.random_range(0..team_ids.len())];
        ops.push(AliasOp {
            team_id,
            target: format!("fresh-tgt-{i}"),
            source: format!("fresh-src-{i}"),
        });
    }

    ops.shuffle(&mut rng);

    AliasPregen { ops }
}

async fn phase_alias(client: &Client, sem: &Arc<Semaphore>, base_url: &str, ops: &[AliasOp]) {
    println!(
        "Phase 2: aliasing {} distinct_ids via POST /alias (mixed cases)...",
        ops.len()
    );

    let url = format!("{base_url}/alias");
    let requests: Vec<(String, serde_json::Value)> = ops
        .iter()
        .map(|op| {
            (
                url.clone(),
                serde_json::json!({
                    "team_id": op.team_id,
                    "target": op.target,
                    "alias": op.source,
                }),
            )
        })
        .collect();

    let wall_start = Instant::now();
    let (latencies, failures) = run_parallel(client, sem, requests).await;
    let wall_time = wall_start.elapsed();

    print_stats("alias", &compute_stats(latencies, wall_time, failures));
}

// ---------------------------------------------------------------------------
// Phase 3: merge benchmark — parallel HTTP POST /merge
// ---------------------------------------------------------------------------

struct MergeOp {
    team_id: i64,
    target: String,
    sources: Vec<String>,
}

struct MergePregen {
    seed_pairs: Vec<(i64, String)>,
    ops: Vec<MergeOp>,
    all_merge_ids: Vec<ScopedId>,
}

/// Cross-team safety: person_id is a BIGSERIAL PK on person_mapping (globally
/// unique, one row = one team). All merge SQL filters by the single team_id in
/// each request. `pick_target_for_team` selects targets from the same team's
/// warm-up primaries, and `merge_by_team` groups sources per team. Person IDs
/// are never shared across teams.
fn pregen_merge(
    n: usize,
    batch_size: usize,
    team_ids: &[i64],
    targets_by_team: &HashMap<i64, Vec<String>>,
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
            let tgt = pick_target_for_team(&mut rng, team_id, hot_by_team, targets_by_team);
            ops.push(MergeOp {
                team_id,
                target: tgt.to_owned(),
                sources: chunk.to_vec(),
            });
        }
    }

    MergePregen {
        seed_pairs,
        ops,
        all_merge_ids,
    }
}

async fn phase_merge(
    pool: &PgPool,
    client: &Client,
    sem: &Arc<Semaphore>,
    base_url: &str,
    pregen: &MergePregen,
) {
    let n = pregen.seed_pairs.len();
    let n_ops = pregen.ops.len();
    println!("Phase 3: merging {n} distinct_ids in {n_ops} batches via POST /merge...");

    println!("  seeding {n} merge distinct_ids (parallel, tx batch {SEED_TX_BATCH})...");
    let t_pre = Instant::now();
    seed_parallel(pool, &pregen.seed_pairs).await;
    println!("  seeded in {:.2?}", t_pre.elapsed());

    let url = format!("{base_url}/merge");
    let requests: Vec<(String, serde_json::Value)> = pregen
        .ops
        .iter()
        .map(|op| {
            (
                url.clone(),
                serde_json::json!({
                    "team_id": op.team_id,
                    "target": op.target,
                    "sources": op.sources,
                }),
            )
        })
        .collect();

    let wall_start = Instant::now();
    let (latencies, failures) = run_parallel(client, sem, requests).await;
    let wall_time = wall_start.elapsed();

    print_stats(
        "merge (per batch)",
        &compute_stats(latencies, wall_time, failures),
    );
}

// ---------------------------------------------------------------------------
// Phase 3a: batched merge benchmark — parallel HTTP POST /batched_merge
// ---------------------------------------------------------------------------

fn pregen_batched_merge(
    n: usize,
    batch_size: usize,
    team_ids: &[i64],
    targets_by_team: &HashMap<i64, Vec<String>>,
    hot_by_team: &HashMap<i64, Vec<String>>,
) -> MergePregen {
    let mut rng = rand::rng();

    let seed_pairs: Vec<(i64, String)> = (0..n)
        .map(|i| (team_ids[i % team_ids.len()], format!("bmerge-{i}")))
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
            let tgt = pick_target_for_team(&mut rng, team_id, hot_by_team, targets_by_team);
            ops.push(MergeOp {
                team_id,
                target: tgt.to_owned(),
                sources: chunk.to_vec(),
            });
        }
    }

    MergePregen {
        seed_pairs,
        ops,
        all_merge_ids,
    }
}

async fn phase_batched_merge(
    pool: &PgPool,
    client: &Client,
    sem: &Arc<Semaphore>,
    base_url: &str,
    pregen: &MergePregen,
) {
    let n = pregen.seed_pairs.len();
    let n_ops = pregen.ops.len();
    println!("Phase 3a: merging {n} distinct_ids in {n_ops} batches via POST /batched_merge...");

    println!("  seeding {n} batched-merge distinct_ids (parallel, tx batch {SEED_TX_BATCH})...");
    let t_pre = Instant::now();
    seed_parallel(pool, &pregen.seed_pairs).await;
    println!("  seeded in {:.2?}", t_pre.elapsed());

    let url = format!("{base_url}/batched_merge");
    let requests: Vec<(String, serde_json::Value)> = pregen
        .ops
        .iter()
        .map(|op| {
            (
                url.clone(),
                serde_json::json!({
                    "team_id": op.team_id,
                    "target": op.target,
                    "sources": op.sources,
                }),
            )
        })
        .collect();

    let wall_start = Instant::now();
    let (latencies, failures) = run_parallel(client, sem, requests).await;
    let wall_time = wall_start.elapsed();

    print_stats(
        "batched_merge (per batch)",
        &compute_stats(latencies, wall_time, failures),
    );
}

// ---------------------------------------------------------------------------
// Phase 3b: chain deepening — DB-direct, not HTTP-benchmarked.
// ---------------------------------------------------------------------------

fn generate_chain_lengths(rng: &mut impl Rng, n_targets: usize, max_depth: usize) -> Vec<usize> {
    if max_depth <= 1 || n_targets <= 1 {
        return vec![1; n_targets];
    }

    let mut lengths = Vec::new();
    let mut remaining = n_targets;
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
    targets_by_team: &HashMap<i64, Vec<String>>,
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

    for (&team_id, tgts) in targets_by_team {
        let mut shuffled = tgts.clone();
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
    let mut set = tokio::task::JoinSet::new();
    for (team_id, chains) in team_plans {
        let pool = pool.clone();
        set.spawn(async move {
            for chain in &chains {
                for i in 0..chain.len().saturating_sub(1) {
                    db::handle_merge(&pool, team_id, &chain[i + 1], &[chain[i].clone()], i32::MAX)
                        .await
                        .expect("chain deepen merge failed");
                }
            }
        });
    }
    while let Some(result) = set.join_next().await {
        result.expect("chain deepen task panicked");
    }

    println!("  total chain deepening wall time: {:.2?}\n", t0.elapsed());
}

// ---------------------------------------------------------------------------
// Phase 4: read benchmark — parallel HTTP POST /resolve
// ---------------------------------------------------------------------------

async fn phase_read(
    client: &Client,
    sem: &Arc<Semaphore>,
    base_url: &str,
    lookup_ids: &[ScopedId],
    n_reads: usize,
) {
    println!("Phase 4: reading {n_reads} random distinct_ids via POST /resolve...");

    let mut rng = rand::rng();
    let url = format!("{base_url}/resolve");
    let requests: Vec<(String, serde_json::Value)> = (0..n_reads)
        .map(|_| {
            let sid = &lookup_ids[rng.random_range(0..lookup_ids.len())];
            (
                url.clone(),
                serde_json::json!({
                    "team_id": sid.team_id,
                    "distinct_id": sid.distinct_id,
                }),
            )
        })
        .collect();

    let wall_start = Instant::now();
    let (latencies, failures) = run_parallel(client, sem, requests).await;
    let wall_time = wall_start.elapsed();

    print_stats(
        "resolve (read)",
        &compute_stats(latencies, wall_time, failures),
    );
}

// ---------------------------------------------------------------------------
// Phase 4a: resolve_distinct_ids benchmark — parallel HTTP POST /resolve_distinct_ids
// Reuses persons created by prior phases; no additional seeding required.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct PersonTarget {
    team_id: i64,
    person_uuid: String,
}

async fn collect_person_targets(pool: &PgPool) -> Vec<PersonTarget> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT team_id, person_uuid FROM person_mapping WHERE deleted_at IS NULL",
    )
    .fetch_all(pool)
    .await
    .expect("failed to query person_mapping for benchmark targets");
    rows.into_iter()
        .map(|(team_id, person_uuid)| PersonTarget {
            team_id,
            person_uuid,
        })
        .collect()
}

async fn phase_resolve_distinct_ids(
    pool: &PgPool,
    client: &Client,
    sem: &Arc<Semaphore>,
    base_url: &str,
    n: usize,
) {
    println!("Phase 4a: collecting person_uuid targets from DB...");
    let t_collect = Instant::now();
    let person_targets = collect_person_targets(pool).await;
    println!(
        "  found {} live persons in {:.2?}",
        person_targets.len(),
        t_collect.elapsed()
    );

    if person_targets.is_empty() {
        println!("  no persons to resolve — skipping phase 4a\n");
        return;
    }

    let hot_count = std::cmp::max(1, person_targets.len() / 5);
    let mut rng = rand::rng();
    let hot_set: Vec<PersonTarget> = (0..hot_count)
        .map(|_| person_targets[rng.random_range(0..person_targets.len())].clone())
        .collect();

    println!("Phase 4a: resolving {n} person_uuids via POST /resolve_distinct_ids...");

    let url = format!("{base_url}/resolve_distinct_ids");
    let requests: Vec<(String, serde_json::Value)> = (0..n)
        .map(|_| {
            let pt = if rng.random_bool(0.8) {
                &hot_set[rng.random_range(0..hot_set.len())]
            } else {
                &person_targets[rng.random_range(0..person_targets.len())]
            };
            (
                url.clone(),
                serde_json::json!({
                    "team_id": pt.team_id,
                    "person_uuid": pt.person_uuid,
                }),
            )
        })
        .collect();

    let wall_start = Instant::now();
    let (latencies, failures) = run_parallel(client, sem, requests).await;
    let wall_time = wall_start.elapsed();

    print_stats(
        "resolve_distinct_ids",
        &compute_stats(latencies, wall_time, failures),
    );
}

// ---------------------------------------------------------------------------
// Phase 5: delete_distinct_id benchmark
// ---------------------------------------------------------------------------

async fn phase_delete_distinct_id(
    pool: &PgPool,
    client: &Client,
    sem: &Arc<Semaphore>,
    base_url: &str,
    n: usize,
    team_ids: &[i64],
) {
    println!("Phase 5: benchmarking {n} POST /delete_distinct_id calls...");

    println!("  seeding {n} distinct_ids for deletion (parallel)...");
    let t_pre = Instant::now();
    let pairs: Vec<(i64, String)> = (0..n)
        .map(|i| (team_ids[i % team_ids.len()], format!("del-did-{i}")))
        .collect();
    seed_parallel(pool, &pairs).await;
    println!("  seeded in {:.2?}", t_pre.elapsed());

    let url = format!("{base_url}/delete_distinct_id");
    let requests: Vec<(String, serde_json::Value)> = pairs
        .iter()
        .map(|(team_id, did)| {
            (
                url.clone(),
                serde_json::json!({
                    "team_id": team_id,
                    "distinct_id": did,
                }),
            )
        })
        .collect();

    let wall_start = Instant::now();
    let (latencies, failures) = run_parallel(client, sem, requests).await;
    let wall_time = wall_start.elapsed();

    print_stats(
        "delete_distinct_id",
        &compute_stats(latencies, wall_time, failures),
    );
}

// ---------------------------------------------------------------------------
// Phase 6: delete_person benchmark
// ---------------------------------------------------------------------------

async fn phase_delete_person(
    pool: &PgPool,
    client: &Client,
    sem: &Arc<Semaphore>,
    base_url: &str,
    n: usize,
    team_ids: &[i64],
) {
    println!("Phase 6: benchmarking {n} POST /delete_person calls...");

    println!("  seeding {n} persons for deletion (parallel)...");
    let t_pre = Instant::now();
    let pairs: Vec<(i64, String)> = (0..n)
        .map(|i| (team_ids[i % team_ids.len()], format!("del-person-{i}")))
        .collect();
    let delete_targets = seed_parallel_with_uuids(pool, &pairs).await;
    println!("  seeded in {:.2?}", t_pre.elapsed());

    let url = format!("{base_url}/delete_person");
    let requests: Vec<(String, serde_json::Value)> = delete_targets
        .iter()
        .map(|(team_id, person_uuid)| {
            (
                url.clone(),
                serde_json::json!({
                    "team_id": team_id,
                    "person_uuid": person_uuid,
                }),
            )
        })
        .collect();

    let wall_start = Instant::now();
    let (latencies, failures) = run_parallel(client, sem, requests).await;
    let wall_time = wall_start.elapsed();

    print_stats(
        "delete_person",
        &compute_stats(latencies, wall_time, failures),
    );
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let n_warm = env_usize("BENCH_WARM", 100_000);
    let n_create = env_usize("BENCH_CREATE", 50_000);
    let n_alias = env_usize("BENCH_ALIAS", 100_000);
    let n_merge = env_usize("BENCH_MERGE", 100_000);
    let n_batched_merge = env_usize("BENCH_BATCHED_MERGE", 100_000);
    let batch_size = env_usize("BENCH_BATCH", 10);
    let chain_depth = env_usize("BENCH_CHAIN_DEPTH", 100);
    let n_reads = env_usize("BENCH_READS", 1_000_000);
    let n_resolve_dids = env_usize("BENCH_RESOLVE_DIDS", 100_000);
    let n_delete_did = env_usize("BENCH_DELETE_DID", 10_000);
    let n_delete_person = env_usize("BENCH_DELETE_PERSON", 10_000);
    let n_teams = env_usize("BENCH_TEAMS", std::cmp::max(1, n_warm / 1000));
    let db_pool_size = env_usize("BENCH_DB_POOL", 50);
    let concurrency = env_usize("BENCH_CONCURRENCY", 50);
    let base_url = env_string("BENCH_BASE_URL", "http://127.0.0.1:3000");

    let team_ids: Vec<i64> = (1..=n_teams as i64).collect();

    println!("=== Union-Find Benchmark (parallel HTTP) ===");
    println!("  BENCH_TEAMS          = {n_teams}");
    println!("  BENCH_WARM           = {n_warm}");
    println!("  BENCH_CREATE         = {n_create}");
    println!("  BENCH_ALIAS          = {n_alias}");
    println!("  BENCH_MERGE          = {n_merge}");
    println!("  BENCH_BATCHED_MERGE  = {n_batched_merge}");
    println!("  BENCH_BATCH          = {batch_size}");
    println!("  BENCH_CHAIN_DEPTH    = {chain_depth}");
    println!("  BENCH_READS          = {n_reads}");
    println!("  BENCH_RESOLVE_DIDS   = {n_resolve_dids}");
    println!("  BENCH_DELETE_DID     = {n_delete_did}");
    println!("  BENCH_DELETE_PERSON  = {n_delete_person}");
    println!("  BENCH_DB_POOL        = {db_pool_size}");
    println!("  BENCH_CONCURRENCY    = {concurrency}");
    println!("  BENCH_BASE_URL       = {base_url}");
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

    let client = Client::builder()
        .pool_max_idle_per_host(concurrency)
        .build()
        .expect("failed to build HTTP client");
    let sem = Arc::new(Semaphore::new(concurrency));

    // Phase 1: warm-up (DB-direct seeding, not benchmarked)
    let warm = phase_warm(&pool, n_warm, &team_ids).await;

    // Pregenerate all operation data so timed loops measure only server work.
    let t_pregen = Instant::now();

    let create_ops = pregen_create_ops(n_create, &warm.all_targets, &warm.hot_set, &team_ids);
    let alias_pregen = pregen_alias_ops(n_alias, &warm.all_targets, &warm.hot_set, &team_ids);
    let merge_pregen = pregen_merge(
        n_merge,
        batch_size,
        &team_ids,
        &warm.targets_by_team,
        &warm.hot_by_team,
    );
    let batched_merge_pregen = pregen_batched_merge(
        n_batched_merge,
        batch_size,
        &team_ids,
        &warm.targets_by_team,
        &warm.hot_by_team,
    );

    // Build read lookup pool from DB-seeded IDs only — no dependency on HTTP phases.
    // Warm-up primaries test root-node resolution; merge IDs test chain traversal.
    let mut lookup_ids: Vec<ScopedId> = warm.all_targets.clone();
    lookup_ids.extend(merge_pregen.all_merge_ids.iter().cloned());
    lookup_ids.extend(batched_merge_pregen.all_merge_ids.iter().cloned());

    println!(
        "pregenerated {} create + {} alias + {} merge + {} batched_merge ops in {:.2?}\n",
        create_ops.len(),
        alias_pregen.ops.len(),
        merge_pregen.ops.len(),
        batched_merge_pregen.ops.len(),
        t_pregen.elapsed(),
    );

    // Phase 1b: create benchmark (parallel HTTP)
    phase_create(&client, &sem, &base_url, &create_ops).await;

    // Phase 2: alias benchmark (parallel HTTP)
    phase_alias(&client, &sem, &base_url, &alias_pregen.ops).await;

    // Phase 3: merge benchmark (parallel HTTP, DB-direct seeding)
    phase_merge(&pool, &client, &sem, &base_url, &merge_pregen).await;

    // Phase 3a: batched merge benchmark (parallel HTTP, DB-direct seeding)
    phase_batched_merge(&pool, &client, &sem, &base_url, &batched_merge_pregen).await;

    // Phase 3b: chain deepening (DB-direct, not benchmarked)
    phase_chain_deepen(&pool, chain_depth, &warm.targets_by_team).await;

    // Phase 4: read benchmark (parallel HTTP)
    phase_read(&client, &sem, &base_url, &lookup_ids, n_reads).await;

    // Phase 4a: resolve_distinct_ids benchmark (parallel HTTP, reuses existing data)
    phase_resolve_distinct_ids(&pool, &client, &sem, &base_url, n_resolve_dids).await;

    // Phase 5: delete_distinct_id benchmark (parallel HTTP)
    phase_delete_distinct_id(&pool, &client, &sem, &base_url, n_delete_did, &team_ids).await;

    // Phase 6: delete_person benchmark (parallel HTTP)
    phase_delete_person(&pool, &client, &sem, &base_url, n_delete_person, &team_ids).await;

    println!("=== Done ===");
}
