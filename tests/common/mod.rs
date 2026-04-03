use std::sync::atomic::{AtomicI64, Ordering};

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use pg_union_find_rs::db::{self, ResolvedPerson};
use pg_union_find_rs::models::{AliasResponse, DbResult, MergeResponse};

static TEAM_COUNTER: AtomicI64 = AtomicI64::new(0);

pub fn next_team_id() -> i64 {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos() as i64
            * 1_000;
        TEAM_COUNTER.store(seed, Ordering::Relaxed);
    });
    TEAM_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub async fn test_pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:54320/union_find".into());

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("failed to connect to test database");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations");

    pool
}

// ---- Row-count helpers (per team) -------------------------------------------

pub async fn count_person_mappings(pool: &PgPool, team_id: i64) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM person_mapping WHERE team_id = $1")
        .bind(team_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub async fn count_distinct_ids(pool: &PgPool, team_id: i64) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM distinct_id_mappings WHERE team_id = $1")
        .bind(team_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

pub async fn count_union_find(pool: &PgPool, team_id: i64) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM union_find WHERE team_id = $1")
        .bind(team_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

// ---- Graph structure helpers ------------------------------------------------

#[derive(Debug)]
#[allow(dead_code)]
pub struct UfRow {
    pub current: i64,
    pub next: Option<i64>,
    pub person_id: Option<i64>,
    pub is_deleted: bool,
}

/// Return the union_find row for a given distinct_id string.
pub async fn get_uf_row(pool: &PgPool, team_id: i64, distinct_id: &str) -> Option<UfRow> {
    sqlx::query_as::<_, (i64, Option<i64>, Option<i64>, bool)>(
        "SELECT uf.current, uf.next, uf.person_id, (uf.deleted_at IS NOT NULL) \
         FROM union_find uf \
         JOIN distinct_id_mappings d ON d.id = uf.current \
         WHERE d.team_id = $1 AND d.distinct_id = $2 AND uf.team_id = $1",
    )
    .bind(team_id)
    .bind(distinct_id)
    .fetch_optional(pool)
    .await
    .unwrap()
    .map(|(current, next, person_id, is_deleted)| UfRow {
        current,
        next,
        person_id,
        is_deleted,
    })
}

// ---- CTE-based chain and parent collection ----------------------------------

#[derive(Debug)]
#[allow(dead_code)]
pub struct ChainLink {
    pub current: i64,
    pub distinct_id: String,
    pub next: Option<i64>,
    pub person_id: Option<i64>,
    pub is_deleted: bool,
    pub depth: i32,
}

/// Collect the full chain from a distinct_id to root using a single recursive
/// CTE (atomic snapshot). Returns links ordered by depth (0 = starting node).
pub async fn collect_chain(pool: &PgPool, team_id: i64, distinct_id: &str) -> Vec<ChainLink> {
    sqlx::query_as::<_, (i64, String, Option<i64>, Option<i64>, bool, i32)>(
        r#"
        WITH RECURSIVE chain AS (
            SELECT uf.current, uf.next, uf.person_id, uf.deleted_at, 0 AS depth
            FROM union_find uf
            JOIN distinct_id_mappings d ON d.id = uf.current AND d.team_id = uf.team_id
            WHERE d.team_id = $1 AND d.distinct_id = $2

            UNION ALL

            SELECT uf.current, uf.next, uf.person_id, uf.deleted_at, c.depth + 1
            FROM chain c
            JOIN union_find uf ON uf.team_id = $1 AND uf.current = c.next
            WHERE c.person_id IS NULL AND c.depth < 1000
        )
        SELECT c.current, d.distinct_id, c.next, c.person_id,
               (c.deleted_at IS NOT NULL) AS is_deleted, c.depth
        FROM chain c
        JOIN distinct_id_mappings d ON d.id = c.current AND d.team_id = $1
        ORDER BY c.depth
        "#,
    )
    .bind(team_id)
    .bind(distinct_id)
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(
        |(current, distinct_id, next, person_id, is_deleted, depth)| ChainLink {
            current,
            distinct_id,
            next,
            person_id,
            is_deleted,
            depth,
        },
    )
    .collect()
}

/// Collect all union_find rows whose `next` points to a given node, ordered by
/// `current ASC`. Useful for inspecting fan-in topology before/after root
/// deletion. The production `unlink_did` picks the promoted parent via
/// `LIMIT 1` without `ORDER BY` — which parent is chosen is nondeterministic.
pub async fn collect_parents(pool: &PgPool, team_id: i64, did_pk: i64) -> Vec<ChainLink> {
    sqlx::query_as::<_, (i64, String, Option<i64>, Option<i64>, bool)>(
        "SELECT uf.current, d.distinct_id, uf.next, uf.person_id, \
                (uf.deleted_at IS NOT NULL) AS is_deleted \
         FROM union_find uf \
         JOIN distinct_id_mappings d ON d.id = uf.current AND d.team_id = uf.team_id \
         WHERE uf.team_id = $1 AND uf.next = $2 \
         ORDER BY uf.current",
    )
    .bind(team_id)
    .bind(did_pk)
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .map(
        |(current, distinct_id, next, person_id, is_deleted)| ChainLink {
            current,
            distinct_id,
            next,
            person_id,
            is_deleted,
            depth: 0,
        },
    )
    .collect()
}

// ---- Chain assertion helpers ------------------------------------------------

/// Verify that the chain from `start_did` has exactly the expected sequence of
/// distinct_ids, with proper link structure at each step, terminating at a root
/// whose person_uuid matches `expected_person_uuid`.
pub async fn assert_chain_matches(
    pool: &PgPool,
    team_id: i64,
    start_did: &str,
    expected_dids: &[&str],
    expected_person_uuid: &str,
) {
    let chain = collect_chain(pool, team_id, start_did).await;
    assert_eq!(
        chain.len(),
        expected_dids.len(),
        "chain from '{start_did}': expected {} links, got {} (dids: {:?})",
        expected_dids.len(),
        chain.len(),
        chain.iter().map(|l| &l.distinct_id).collect::<Vec<_>>()
    );

    for (i, (link, expected_did)) in chain.iter().zip(expected_dids).enumerate() {
        assert_eq!(
            &link.distinct_id, expected_did,
            "chain from '{start_did}': link {i} expected did '{expected_did}', got '{}'",
            link.distinct_id
        );
        assert!(
            !link.is_deleted,
            "chain from '{start_did}': link {i} ('{}') has unexpected deleted_at",
            link.distinct_id
        );

        let is_last = i == chain.len() - 1;
        if is_last {
            assert!(
                link.person_id.is_some(),
                "chain from '{start_did}': root '{}' missing person_id",
                link.distinct_id
            );
            assert!(
                link.next.is_none(),
                "chain from '{start_did}': root '{}' has non-NULL next",
                link.distinct_id
            );
        } else {
            assert!(
                link.person_id.is_none(),
                "chain from '{start_did}': non-root '{}' at depth {i} has person_id set",
                link.distinct_id
            );
            assert_eq!(
                link.next,
                Some(chain[i + 1].current),
                "chain from '{start_did}': link {i} ('{}') next should point to link {} ('{}')",
                link.distinct_id,
                i + 1,
                chain[i + 1].distinct_id
            );
        }
    }

    let root = chain.last().unwrap();
    let person_uuid: String = sqlx::query_scalar(
        "SELECT person_uuid FROM person_mapping \
         WHERE person_id = $1 AND deleted_at IS NULL",
    )
    .bind(root.person_id.unwrap())
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        person_uuid, expected_person_uuid,
        "chain from '{start_did}': root person_uuid mismatch"
    );
}

/// Shortcut: verify that a DID is its own root with the expected person_uuid.
pub async fn assert_chain_is_root(
    pool: &PgPool,
    team_id: i64,
    did: &str,
    expected_person_uuid: &str,
) {
    assert_chain_matches(pool, team_id, did, &[did], expected_person_uuid).await;
}

/// Get all distinct person_ids referenced by union_find roots for a team.
#[allow(dead_code)]
pub async fn all_root_person_ids(pool: &PgPool, team_id: i64) -> Vec<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT DISTINCT person_id FROM union_find \
         WHERE team_id = $1 AND person_id IS NOT NULL \
         ORDER BY person_id",
    )
    .bind(team_id)
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Check is_identified for a person_id.
pub async fn is_person_identified(pool: &PgPool, person_id: i64) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT is_identified FROM person_mapping WHERE person_id = $1")
        .bind(person_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Check if a person_id exists in person_mapping.
pub async fn person_exists(pool: &PgPool, person_id: i64) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM person_mapping WHERE person_id = $1)",
    )
    .bind(person_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Count only live (non-soft-deleted) person_mapping rows for a team.
pub async fn count_live_person_mappings(pool: &PgPool, team_id: i64) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM person_mapping WHERE team_id = $1 AND deleted_at IS NULL",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Check if a person_mapping row is soft-deleted.
pub async fn is_person_deleted(pool: &PgPool, person_id: i64) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT deleted_at IS NOT NULL FROM person_mapping WHERE person_id = $1",
    )
    .bind(person_id)
    .fetch_one(pool)
    .await
    .unwrap()
}

// ---- Graph invariant checks -------------------------------------------------

/// Every root row (person_id IS NOT NULL) must have next IS NULL.
pub async fn assert_roots_have_null_next(pool: &PgPool, team_id: i64) {
    let bad: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM union_find \
         WHERE team_id = $1 AND person_id IS NOT NULL AND next IS NOT NULL",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(bad, 0, "found {bad} root rows with non-NULL next");
}

/// Every non-root row (person_id IS NULL) must have next IS NOT NULL.
pub async fn assert_non_roots_have_next(pool: &PgPool, team_id: i64) {
    let bad: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM union_find \
         WHERE team_id = $1 AND person_id IS NULL AND next IS NULL",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(bad, 0, "found {bad} non-root rows with NULL next");
}

/// Every person_id in union_find roots must exist in person_mapping.
pub async fn assert_person_refs_valid(pool: &PgPool, team_id: i64) {
    let bad: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM union_find uf \
         WHERE uf.team_id = $1 AND uf.person_id IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM person_mapping pm WHERE pm.person_id = uf.person_id)",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        bad, 0,
        "found {bad} root rows referencing non-existent person_mapping"
    );
}

/// Every current and non-NULL next in union_find must exist in distinct_id_mappings.
pub async fn assert_did_refs_valid(pool: &PgPool, team_id: i64) {
    let bad_current: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM union_find uf \
         WHERE uf.team_id = $1 \
           AND NOT EXISTS (SELECT 1 FROM distinct_id_mappings d WHERE d.id = uf.current)",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        bad_current, 0,
        "found {bad_current} union_find rows with invalid current ref"
    );

    let bad_next: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM union_find uf \
         WHERE uf.team_id = $1 AND uf.next IS NOT NULL \
           AND NOT EXISTS (SELECT 1 FROM distinct_id_mappings d WHERE d.id = uf.next)",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        bad_next, 0,
        "found {bad_next} union_find rows with invalid next ref"
    );
}

/// No union_find or person_mapping rows should have deleted_at set.
/// Use this to catch normal operations accidentally soft-deleting rows.
pub async fn assert_no_deleted_rows(pool: &PgPool, team_id: i64) {
    let bad_uf: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM union_find \
         WHERE team_id = $1 AND deleted_at IS NOT NULL",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        bad_uf, 0,
        "found {bad_uf} union_find rows with unexpected deleted_at"
    );

    let bad_pm: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM person_mapping \
         WHERE team_id = $1 AND deleted_at IS NOT NULL",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        bad_pm, 0,
        "found {bad_pm} person_mapping rows with unexpected deleted_at"
    );
}

/// Each person_id must appear at most once in union_find (per team).
pub async fn assert_unique_person_ids(pool: &PgPool, team_id: i64) {
    let bad: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM (\
           SELECT person_id FROM union_find \
           WHERE team_id = $1 AND person_id IS NOT NULL \
           GROUP BY person_id HAVING COUNT(*) > 1\
         ) AS dupes",
    )
    .bind(team_id)
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(
        bad, 0,
        "found {bad} person_ids appearing in multiple union_find rows"
    );
}

/// Structural graph invariant checks only. Safe to call after delete operations
/// that legitimately leave soft-deleted rows.
pub async fn assert_structural_invariants(pool: &PgPool, team_id: i64) {
    assert_roots_have_null_next(pool, team_id).await;
    assert_non_roots_have_next(pool, team_id).await;
    assert_person_refs_valid(pool, team_id).await;
    assert_did_refs_valid(pool, team_id).await;
    assert_unique_person_ids(pool, team_id).await;
}

/// Run all graph invariant checks for a team, including that no rows are
/// soft-deleted. Use this in tests that do NOT involve delete operations.
/// For tests that involve deletions, use assert_structural_invariants instead.
pub async fn assert_all_invariants(pool: &PgPool, team_id: i64) {
    assert_structural_invariants(pool, team_id).await;
    assert_no_deleted_rows(pool, team_id).await;
}

// ---- Test wrappers that hide the compress_threshold parameter ---------------

pub async fn handle_alias(
    pool: &PgPool,
    team_id: i64,
    target: &str,
    source: &str,
) -> DbResult<AliasResponse> {
    db::handle_alias(pool, team_id, target, source, i32::MAX)
        .await
        .map(|(r, _)| r)
}

pub async fn handle_merge(
    pool: &PgPool,
    team_id: i64,
    target: &str,
    sources: &[String],
) -> DbResult<MergeResponse> {
    db::handle_merge(pool, team_id, target, sources, i32::MAX)
        .await
        .map(|(r, _)| r)
}

pub async fn handle_batched_merge(
    pool: &PgPool,
    team_id: i64,
    target: &str,
    sources: &[String],
) -> DbResult<MergeResponse> {
    db::handle_batched_merge(pool, team_id, target, sources, i32::MAX)
        .await
        .map(|(r, _)| r)
}

pub async fn resolve(
    pool: &PgPool,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<Option<ResolvedPerson>> {
    db::resolve(pool, team_id, distinct_id)
        .await
        .map(|opt| opt.map(|(p, _)| p))
}

/// Walk the chain and return its depth (number of hops from start to root).
pub async fn chain_depth(pool: &PgPool, team_id: i64, distinct_id: &str) -> i32 {
    let chain = collect_chain(pool, team_id, distinct_id).await;
    (chain.len() as i32).saturating_sub(1)
}
