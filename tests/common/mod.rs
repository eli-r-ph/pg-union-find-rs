use std::sync::atomic::{AtomicI64, Ordering};

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

static TEAM_COUNTER: AtomicI64 = AtomicI64::new(10_000);

pub fn next_team_id() -> i64 {
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
pub struct UfRow {
    pub current: i64,
    pub next: Option<i64>,
    pub person_id: Option<i64>,
}

/// Return the union_find row for a given distinct_id string.
pub async fn get_uf_row(pool: &PgPool, team_id: i64, distinct_id: &str) -> Option<UfRow> {
    sqlx::query_as::<_, (i64, Option<i64>, Option<i64>)>(
        "SELECT uf.current, uf.next, uf.person_id \
         FROM union_find uf \
         JOIN distinct_id_mappings d ON d.id = uf.current \
         WHERE d.team_id = $1 AND d.distinct_id = $2 AND uf.team_id = $1",
    )
    .bind(team_id)
    .bind(distinct_id)
    .fetch_optional(pool)
    .await
    .unwrap()
    .map(|(current, next, person_id)| UfRow {
        current,
        next,
        person_id,
    })
}

/// Walk the chain from a distinct_id to the root via iterative SQL lookups.
/// Returns the sequence of union_find rows from start to root.
pub async fn walk_chain(pool: &PgPool, team_id: i64, distinct_id: &str) -> Vec<UfRow> {
    let mut chain = Vec::new();

    let start_pk: Option<i64> = sqlx::query_scalar(
        "SELECT id FROM distinct_id_mappings WHERE team_id = $1 AND distinct_id = $2",
    )
    .bind(team_id)
    .bind(distinct_id)
    .fetch_optional(pool)
    .await
    .unwrap();

    let Some(mut current_pk) = start_pk else {
        return chain;
    };

    for _ in 0..1001 {
        let row = sqlx::query_as::<_, (i64, Option<i64>, Option<i64>)>(
            "SELECT current, next, person_id FROM union_find \
             WHERE team_id = $1 AND current = $2",
        )
        .bind(team_id)
        .bind(current_pk)
        .fetch_optional(pool)
        .await
        .unwrap();

        match row {
            Some((current, next, person_id)) => {
                let is_root = person_id.is_some();
                chain.push(UfRow {
                    current,
                    next,
                    person_id,
                });
                if is_root {
                    break;
                }
                match next {
                    Some(n) => current_pk = n,
                    None => break,
                }
            }
            None => break,
        }
    }

    chain
}

/// Get the root node's person_id for a distinct_id by walking the chain.
pub async fn get_root_person_id(pool: &PgPool, team_id: i64, distinct_id: &str) -> Option<i64> {
    let chain = walk_chain(pool, team_id, distinct_id).await;
    chain.last().and_then(|r| r.person_id)
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

/// Run all graph invariant checks for a team.
pub async fn assert_all_invariants(pool: &PgPool, team_id: i64) {
    assert_roots_have_null_next(pool, team_id).await;
    assert_non_roots_have_next(pool, team_id).await;
    assert_person_refs_valid(pool, team_id).await;
    assert_did_refs_valid(pool, team_id).await;
}
