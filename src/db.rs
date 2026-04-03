use std::collections::HashMap;

use sqlx::PgPool;
use sqlx::postgres::PgConnection;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::models::{
    AliasResponse, CompressHint, CreateResponse, DbError, DbOp, DbResult, DeleteDistinctIdResponse,
    DeletePersonResponse, MergeResponse, ResolveDistinctIdsResponse,
};

// ---------------------------------------------------------------------------
// Distinct ID validation — reject obviously-bad IDs before they hit the DB.
// Mirrors PostHog's isDistinctIdIllegal blocklist, plus character and length
// restrictions to prevent log injection, storage abuse, and garbage data.
// ---------------------------------------------------------------------------

const MAX_DISTINCT_ID_LEN: usize = 200;

const ILLEGAL_CHARS: &[char] = &[
    '\'', '"', '`',  // quotes
    '\\', // backslash
];

const CASE_INSENSITIVE_ILLEGAL: &[&str] = &[
    "anonymous",
    "guest",
    "distinctid",
    "distinct_id",
    "id",
    "not_authenticated",
    "email",
    "undefined",
    "true",
    "false",
];

const CASE_SENSITIVE_ILLEGAL: &[&str] = &[
    "[object Object]",
    "NaN",
    "None",
    "none",
    "null",
    "0",
    "undefined",
];

fn is_illegal_distinct_id(id: &str) -> bool {
    if id.trim().is_empty() {
        return true;
    }
    if id.len() > MAX_DISTINCT_ID_LEN {
        return true;
    }
    if id
        .chars()
        .any(|c| c.is_control() || ILLEGAL_CHARS.contains(&c))
    {
        return true;
    }

    let lower = id.to_lowercase();
    CASE_INSENSITIVE_ILLEGAL.contains(&lower.as_str()) || CASE_SENSITIVE_ILLEGAL.contains(&id)
}

fn validate_distinct_id(id: &str) -> DbResult<()> {
    if is_illegal_distinct_id(id) {
        return Err(DbError::IllegalDistinctId(format!(
            "'{id}' is not a valid distinct_id"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Resolved person — returned by the recursive CTE
// ---------------------------------------------------------------------------

pub struct ResolvedPerson {
    pub person_uuid: String,
    pub person_id: i64,
    pub is_identified: bool,
}

// ---------------------------------------------------------------------------
// Worker loop — pulls DbOps from the channel and executes them sequentially.
// ---------------------------------------------------------------------------

/// Compress the chain starting at `distinct_id` if its depth exceeds `threshold`.
/// Runs in an explicit transaction so partial failures roll back cleanly.
pub async fn handle_compress_path(
    pool: &PgPool,
    team_id: i64,
    distinct_id: &str,
    threshold: i32,
) -> DbResult<()> {
    let mut tx = pool.begin().await?;

    let did_pk = match lookup_did(&mut tx, team_id, distinct_id).await? {
        Some(pk) => pk,
        None => return Ok(()),
    };

    sqlx::query(
        r#"
        WITH RECURSIVE walk(node, depth) AS (
            SELECT $2::bigint, 0
            UNION ALL
            SELECT uf.next, w.depth + 1
            FROM walk w
            JOIN union_find uf
              ON uf.team_id = $1 AND uf.current = w.node AND uf.person_id IS NULL
            WHERE w.depth < 1000
        ),
        path_info AS (
            SELECT
                (SELECT node FROM walk ORDER BY depth DESC LIMIT 1) AS root_pk,
                (SELECT MAX(depth) FROM walk) AS max_depth
        )
        UPDATE union_find uf
        SET next = pi.root_pk
        FROM path_info pi
        WHERE pi.max_depth >= $3
          AND uf.team_id = $1
          AND uf.current IN (SELECT node FROM walk WHERE node != pi.root_pk)
          AND uf.person_id IS NULL
        "#,
    )
    .bind(team_id)
    .bind(did_pk)
    .bind(threshold)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

pub async fn worker_loop(pool: PgPool, mut rx: mpsc::Receiver<DbOp>, compress_threshold: i32) {
    while let Some(op) = rx.recv().await {
        match op {
            DbOp::Create {
                team_id,
                distinct_id,
                reply,
            } => {
                let _ = reply.send(handle_create(&pool, team_id, &distinct_id).await);
            }
            DbOp::Alias {
                team_id,
                target,
                source,
                reply,
            } => {
                let _ = reply
                    .send(handle_alias(&pool, team_id, &target, &source, compress_threshold).await);
            }
            DbOp::Merge {
                team_id,
                target,
                sources,
                reply,
            } => {
                let _ = reply.send(
                    handle_merge(&pool, team_id, &target, &sources, compress_threshold).await,
                );
            }
            DbOp::BatchedMerge {
                team_id,
                target,
                sources,
                reply,
            } => {
                let _ = reply.send(
                    handle_batched_merge(&pool, team_id, &target, &sources, compress_threshold)
                        .await,
                );
            }
            DbOp::DeletePerson {
                team_id,
                person_uuid,
                reply,
            } => {
                let _ = reply.send(handle_delete_person(&pool, team_id, &person_uuid).await);
            }
            DbOp::DeleteDistinctId {
                team_id,
                distinct_id,
                reply,
            } => {
                let _ = reply.send(handle_delete_distinct_id(&pool, team_id, &distinct_id).await);
            }
            DbOp::CompressPath {
                team_id,
                distinct_id,
                depth,
                reply,
            } => {
                let result =
                    handle_compress_path(&pool, team_id, &distinct_id, compress_threshold).await;
                if let Err(ref e) = result {
                    tracing::error!(team_id, %distinct_id, depth, %e, "path compression failed");
                }
                let _ = reply.send(result);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Transaction-level primitives.
// ---------------------------------------------------------------------------

/// Look up a distinct_id's internal PK from distinct_id_mappings.
async fn lookup_did(
    conn: &mut PgConnection,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<Option<i64>> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT id FROM distinct_id_mappings WHERE team_id = $1 AND distinct_id = $2",
    )
    .bind(team_id)
    .bind(distinct_id)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(|(id,)| id))
}

/// Splice a node out of its union_find chain, then clear it to orphan state.
///
/// Non-root nodes (person_id IS NULL): all parents inherit the node's `next`,
/// splicing past it. Safe because person_id is NULL — no duplication.
///
/// Root nodes (person_id IS NOT NULL): exactly one parent is promoted to root
/// (inherits next/person_id/deleted_at) and all other parents are redirected
/// to the promoted parent. This preserves the invariant that each person_id
/// appears at most once in union_find.
async fn unlink_did(conn: &mut PgConnection, team_id: i64, did_pk: i64) -> DbResult<()> {
    let is_root: bool = sqlx::query_scalar(
        "SELECT COALESCE(person_id IS NOT NULL, false) \
         FROM union_find WHERE team_id = $1 AND current = $2",
    )
    .bind(team_id)
    .bind(did_pk)
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or(false);

    if !is_root {
        // Non-root: bulk-splice all parents through this node.
        // Parents inherit (next, person_id, deleted_at) from the unlinked node.
        // Safe because person_id is NULL here — no duplication possible.
        sqlx::query(
            "UPDATE union_find AS parent \
             SET next = target.next, \
                 person_id = target.person_id, \
                 deleted_at = target.deleted_at \
             FROM (SELECT next, person_id, deleted_at \
                   FROM union_find WHERE team_id = $1 AND current = $2) AS target \
             WHERE parent.team_id = $1 AND parent.next = $2",
        )
        .bind(team_id)
        .bind(did_pk)
        .execute(&mut *conn)
        .await?;

        // Clear the unlinked node to orphan state.
        sqlx::query(
            "UPDATE union_find \
             SET next = NULL, person_id = NULL, deleted_at = NULL \
             WHERE team_id = $1 AND current = $2",
        )
        .bind(team_id)
        .bind(did_pk)
        .execute(&mut *conn)
        .await?;
    } else {
        // Root: promote exactly one parent to root, redirect all others to it.
        let promoted_pk: Option<i64> = sqlx::query_scalar(
            "SELECT current FROM union_find \
             WHERE team_id = $1 AND next = $2 \
             LIMIT 1",
        )
        .bind(team_id)
        .bind(did_pk)
        .fetch_optional(&mut *conn)
        .await?;

        if let Some(promoted_pk) = promoted_pk {
            // Single UPDATE clears old root and promotes parent in one pass.
            // A single UPDATE defers unique constraint checks until all row
            // modifications are applied, preventing transient person_id duplicates.
            // The read-only CTE captures the old root's values from the snapshot.
            sqlx::query(
                "WITH old AS ( \
                     SELECT person_id, deleted_at \
                     FROM union_find WHERE team_id = $1 AND current = $2 \
                 ) \
                 UPDATE union_find AS uf \
                 SET next = NULL, \
                     person_id = CASE WHEN uf.current = $2 THEN NULL ELSE old.person_id END, \
                     deleted_at = CASE WHEN uf.current = $2 THEN NULL ELSE old.deleted_at END \
                 FROM old \
                 WHERE uf.team_id = $1 AND uf.current IN ($2, $3)",
            )
            .bind(team_id)
            .bind(did_pk)
            .bind(promoted_pk)
            .execute(&mut *conn)
            .await?;

            // Redirect all other parents to the promoted node.
            sqlx::query(
                "UPDATE union_find SET next = $3 \
                 WHERE team_id = $1 AND next = $2 AND current != $3",
            )
            .bind(team_id)
            .bind(did_pk)
            .bind(promoted_pk)
            .execute(&mut *conn)
            .await?;
        } else {
            // No parents: clear old root (person_id is lost).
            sqlx::query(
                "UPDATE union_find \
                 SET next = NULL, person_id = NULL, deleted_at = NULL \
                 WHERE team_id = $1 AND current = $2",
            )
            .bind(team_id)
            .bind(did_pk)
            .execute(&mut *conn)
            .await?;
        }
    }

    Ok(())
}

/// Walk the union_find chain from a distinct_id PK to the root, returning
/// the root's person_id, the corresponding person_uuid, and the chain depth.
async fn resolve_by_pk(
    conn: &mut PgConnection,
    team_id: i64,
    did_pk: i64,
) -> DbResult<Option<(ResolvedPerson, i32)>> {
    let row = sqlx::query_as::<_, (String, i64, bool, i32)>(
        r#"
        WITH RECURSIVE walk(node, depth) AS (
            SELECT $2::bigint, 0

            UNION ALL

            SELECT uf.next, w.depth + 1
            FROM walk w
            JOIN union_find uf
              ON uf.team_id = $1 AND uf.current = w.node AND uf.person_id IS NULL
            WHERE w.depth < 1000
        ),
        walk_result AS (
            SELECT node, depth FROM walk ORDER BY depth DESC LIMIT 1
        )
        SELECT pm.person_uuid, uf.person_id, pm.is_identified, wr.depth
        FROM walk_result wr
        JOIN union_find uf ON uf.team_id = $1 AND uf.current = wr.node
        JOIN person_mapping pm ON pm.person_id = uf.person_id
        WHERE uf.person_id IS NOT NULL
          AND pm.deleted_at IS NULL
        "#,
    )
    .bind(team_id)
    .bind(did_pk)
    .fetch_optional(&mut *conn)
    .await?;

    Ok(row.map(|(person_uuid, person_id, is_identified, depth)| {
        (
            ResolvedPerson {
                person_uuid,
                person_id,
                is_identified,
            },
            depth,
        )
    }))
}

/// Walk the union_find chain from a distinct_id PK to the root, returning
/// the root's (current, person_id, depth).
async fn resolve_root(
    conn: &mut PgConnection,
    team_id: i64,
    did_pk: i64,
) -> DbResult<Option<(i64, i64, i32)>> {
    let row = sqlx::query_as::<_, (i64, i64, i32)>(
        r#"
        WITH RECURSIVE walk(node, depth) AS (
            SELECT $2::bigint, 0

            UNION ALL

            SELECT uf.next, w.depth + 1
            FROM walk w
            JOIN union_find uf
              ON uf.team_id = $1 AND uf.current = w.node AND uf.person_id IS NULL
            WHERE w.depth < 1000
        ),
        walk_result AS (
            SELECT node, depth FROM walk ORDER BY depth DESC LIMIT 1
        )
        SELECT uf.current, uf.person_id, wr.depth
        FROM walk_result wr
        JOIN union_find uf ON uf.team_id = $1 AND uf.current = wr.node
        WHERE uf.person_id IS NOT NULL
          AND uf.deleted_at IS NULL
        "#,
    )
    .bind(team_id)
    .bind(did_pk)
    .fetch_optional(&mut *conn)
    .await?;

    Ok(row)
}

/// Batch-lookup multiple distinct_id strings, returning a map from
/// distinct_id string to its `distinct_id_mappings.id` PK.
/// Strings not present in the DB are simply absent from the result.
async fn batch_lookup_dids(
    conn: &mut PgConnection,
    team_id: i64,
    dids: &[String],
) -> DbResult<HashMap<String, i64>> {
    if dids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query_as::<_, (i64, String)>(
        "SELECT id, distinct_id FROM distinct_id_mappings \
         WHERE team_id = $1 AND distinct_id = ANY($2)",
    )
    .bind(team_id)
    .bind(dids)
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows.into_iter().map(|(id, did)| (did, id)).collect())
}

/// Result of resolving one PK through the union-find chain in a batch.
struct BatchResolveRow {
    start_node: i64,
    root_current: i64,
    person_id: i64,
    depth: i32,
}

/// Batch-resolve multiple distinct_id PKs to their roots in a single
/// recursive CTE. Each returned row carries the `start_node` so the
/// caller can map results back to individual sources.
///
/// PKs whose chain leads to a soft-deleted person (or has no root)
/// are absent from the result — the caller should treat them as orphaned.
async fn batch_resolve_pks(
    conn: &mut PgConnection,
    team_id: i64,
    pks: &[i64],
) -> DbResult<Vec<BatchResolveRow>> {
    if pks.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query_as::<_, (i64, i64, i64, i32)>(
        r#"
        WITH RECURSIVE walk(start_node, node, depth) AS (
            SELECT v, v, 0 FROM unnest($2::bigint[]) AS v

            UNION ALL

            SELECT w.start_node, uf.next, w.depth + 1
            FROM walk w
            JOIN union_find uf
              ON uf.team_id = $1 AND uf.current = w.node AND uf.person_id IS NULL
            WHERE w.depth < 1000
        ),
        roots AS (
            SELECT DISTINCT ON (start_node) start_node, node AS root_current, depth
            FROM walk
            ORDER BY start_node, depth DESC
        )
        SELECT r.start_node, r.root_current, uf.person_id, r.depth
        FROM roots r
        JOIN union_find uf ON uf.team_id = $1 AND uf.current = r.root_current
        JOIN person_mapping pm ON pm.person_id = uf.person_id
        WHERE uf.person_id IS NOT NULL
          AND pm.deleted_at IS NULL
        "#,
    )
    .bind(team_id)
    .bind(pks)
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(start_node, root_current, person_id, depth)| BatchResolveRow {
                start_node,
                root_current,
                person_id,
                depth,
            },
        )
        .collect())
}

/// Resolve a distinct_id string all the way to a ResolvedPerson and chain depth.
pub async fn resolve_tx(
    conn: &mut PgConnection,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<Option<(ResolvedPerson, i32)>> {
    let did_pk = match lookup_did(&mut *conn, team_id, distinct_id).await? {
        Some(pk) => pk,
        None => return Ok(None),
    };
    resolve_by_pk(&mut *conn, team_id, did_pk).await
}

// ---------------------------------------------------------------------------
// Lazy-unlink primitives — used by all write paths to handle distinct_ids
// that belong to soft-deleted persons.
// ---------------------------------------------------------------------------

/// Three-state lookup result for a distinct_id.
enum DidState {
    NotFound,
    Orphaned(i64),
    Live(i64, ResolvedPerson, i32),
}

/// Check whether a distinct_id is missing, orphaned (exists but its person is
/// deleted), or live. Orphaned nodes are automatically unlinked from their
/// dead chain so callers can immediately re-use the did_pk.
async fn check_did(conn: &mut PgConnection, team_id: i64, distinct_id: &str) -> DbResult<DidState> {
    let did_pk = match lookup_did(&mut *conn, team_id, distinct_id).await? {
        Some(pk) => pk,
        None => return Ok(DidState::NotFound),
    };
    match resolve_by_pk(&mut *conn, team_id, did_pk).await? {
        Some((person, depth)) => Ok(DidState::Live(did_pk, person, depth)),
        None => {
            unlink_did(&mut *conn, team_id, did_pk).await?;
            Ok(DidState::Orphaned(did_pk))
        }
    }
}

/// Point an orphaned union_find row into an existing chain.
async fn link_did(
    conn: &mut PgConnection,
    team_id: i64,
    did_pk: i64,
    next_pk: i64,
) -> DbResult<()> {
    sqlx::query(
        "UPDATE union_find SET next = $3, person_id = NULL, deleted_at = NULL \
         WHERE team_id = $1 AND current = $2",
    )
    .bind(team_id)
    .bind(did_pk)
    .bind(next_pk)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Promote an orphaned union_find row to a root for the given person.
async fn root_did(
    conn: &mut PgConnection,
    team_id: i64,
    did_pk: i64,
    person_id: i64,
) -> DbResult<()> {
    sqlx::query(
        "UPDATE union_find SET next = NULL, person_id = $3, deleted_at = NULL \
         WHERE team_id = $1 AND current = $2",
    )
    .bind(team_id)
    .bind(did_pk)
    .bind(person_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Attach a NotFound or Orphaned distinct_id into an existing chain.
/// Returns the did_pk (newly created for NotFound, existing for Orphaned).
async fn attach_did(
    conn: &mut PgConnection,
    team_id: i64,
    distinct_id: &str,
    state: DidState,
    next_pk: i64,
) -> DbResult<i64> {
    match state {
        DidState::NotFound => insert_did_and_link(&mut *conn, team_id, distinct_id, next_pk).await,
        DidState::Orphaned(pk) => {
            link_did(&mut *conn, team_id, pk, next_pk).await?;
            Ok(pk)
        }
        DidState::Live(..) => Err(DbError::Internal(
            "attach_did called with Live state".into(),
        )),
    }
}

/// Attach a NotFound or Orphaned distinct_id as a new root.
/// Returns the did_pk.
async fn attach_did_as_root(
    conn: &mut PgConnection,
    team_id: i64,
    distinct_id: &str,
    state: DidState,
    person_id: i64,
) -> DbResult<i64> {
    match state {
        DidState::NotFound => {
            let did_pk: i64 = sqlx::query_scalar(
                "INSERT INTO distinct_id_mappings (team_id, distinct_id) \
                 VALUES ($1, $2) RETURNING id",
            )
            .bind(team_id)
            .bind(distinct_id)
            .fetch_one(&mut *conn)
            .await?;

            sqlx::query(
                "INSERT INTO union_find (team_id, current, next, person_id) \
                 VALUES ($1, $2, NULL, $3)",
            )
            .bind(team_id)
            .bind(did_pk)
            .bind(person_id)
            .execute(&mut *conn)
            .await?;

            Ok(did_pk)
        }
        DidState::Orphaned(pk) => {
            root_did(&mut *conn, team_id, pk, person_id).await?;
            Ok(pk)
        }
        DidState::Live(..) => Err(DbError::Internal(
            "attach_did_as_root called with Live state".into(),
        )),
    }
}

/// Get-or-create: if the distinct_id resolves to a live person, return it.
/// If the distinct_id is orphaned (person was deleted), unlink it and create
/// a fresh person reusing the existing did_pk. If the distinct_id doesn't
/// exist at all, create everything from scratch.
/// The person is created with is_identified = false (DB default).
pub async fn identify_tx(
    conn: &mut PgConnection,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<ResolvedPerson> {
    match check_did(&mut *conn, team_id, distinct_id).await? {
        DidState::Live(_pk, resolved, _depth) => Ok(resolved),

        DidState::NotFound => {
            let person_uuid = Uuid::new_v4().to_string();

            let person_id: i64 = sqlx::query_scalar(
                "INSERT INTO person_mapping (team_id, person_uuid) \
                 VALUES ($1, $2) RETURNING person_id",
            )
            .bind(team_id)
            .bind(&person_uuid)
            .fetch_one(&mut *conn)
            .await?;

            let did_pk: i64 = sqlx::query_scalar(
                "INSERT INTO distinct_id_mappings (team_id, distinct_id) \
                 VALUES ($1, $2) RETURNING id",
            )
            .bind(team_id)
            .bind(distinct_id)
            .fetch_one(&mut *conn)
            .await?;

            sqlx::query(
                "INSERT INTO union_find (team_id, current, next, person_id) \
                 VALUES ($1, $2, NULL, $3)",
            )
            .bind(team_id)
            .bind(did_pk)
            .bind(person_id)
            .execute(&mut *conn)
            .await?;

            Ok(ResolvedPerson {
                person_uuid,
                person_id,
                is_identified: false,
            })
        }

        DidState::Orphaned(did_pk) => {
            let person_uuid = Uuid::new_v4().to_string();

            let person_id: i64 = sqlx::query_scalar(
                "INSERT INTO person_mapping (team_id, person_uuid) \
                 VALUES ($1, $2) RETURNING person_id",
            )
            .bind(team_id)
            .bind(&person_uuid)
            .fetch_one(&mut *conn)
            .await?;

            root_did(&mut *conn, team_id, did_pk, person_id).await?;

            Ok(ResolvedPerson {
                person_uuid,
                person_id,
                is_identified: false,
            })
        }
    }
}

/// Mark a person as identified (idempotent). Skips soft-deleted persons.
async fn set_identified(conn: &mut PgConnection, person_id: i64) -> DbResult<()> {
    sqlx::query(
        "UPDATE person_mapping SET is_identified = true \
         WHERE person_id = $1 AND NOT is_identified AND deleted_at IS NULL",
    )
    .bind(person_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Link a source root into the target's tree, converting it from a root to a
/// non-root. This properly merges the two union-find components so that all
/// nodes formerly reachable from the source root now resolve through the target
/// tree. The unique person_id invariant guarantees no other union_find row
/// holds old_person, so the orphaned person_mapping is unconditionally deleted.
async fn link_root_to_target(
    conn: &mut PgConnection,
    team_id: i64,
    source_root_current: i64,
    old_person: i64,
    target_pk: i64,
) -> DbResult<()> {
    sqlx::query(
        "UPDATE union_find SET person_id = NULL, next = $3 \
         WHERE team_id = $1 AND current = $2",
    )
    .bind(team_id)
    .bind(source_root_current)
    .bind(target_pk)
    .execute(&mut *conn)
    .await?;

    sqlx::query("DELETE FROM person_mapping WHERE person_id = $1")
        .bind(old_person)
        .execute(&mut *conn)
        .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Public API — each operation wrapped in an explicit transaction.
// ---------------------------------------------------------------------------

/// Resolve a distinct_id to its canonical person and chain depth.
pub async fn resolve(
    pool: &PgPool,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<Option<(ResolvedPerson, i32)>> {
    let mut conn = pool.acquire().await?;
    resolve_tx(&mut conn, team_id, distinct_id).await
}

const RESOLVE_DISTINCT_IDS_LIMIT: usize = 10_000;

/// Resolve all distinct_ids belonging to a person, capped at 10,000.
pub async fn resolve_distinct_ids(
    pool: &PgPool,
    team_id: i64,
    person_uuid: &str,
) -> DbResult<ResolveDistinctIdsResponse> {
    let mut conn = pool.acquire().await?;

    let person_id: i64 = sqlx::query_scalar(
        "SELECT person_id FROM person_mapping \
         WHERE team_id = $1 AND person_uuid = $2 AND deleted_at IS NULL",
    )
    .bind(team_id)
    .bind(person_uuid)
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| DbError::NotFound(format!("person '{person_uuid}' not found")))?;

    let rows: Vec<(String,)> = sqlx::query_as(
        r#"
        WITH RECURSIVE tree(node) AS (
            SELECT uf.current
            FROM union_find uf
            WHERE uf.team_id = $1 AND uf.person_id = $2 AND uf.deleted_at IS NULL

            UNION ALL

            SELECT uf.current
            FROM tree t
            JOIN union_find uf ON uf.team_id = $1 AND uf.next = t.node
            WHERE uf.person_id IS NULL
        )
        SELECT d.distinct_id
        FROM tree t
        JOIN distinct_id_mappings d ON d.id = t.node AND d.team_id = $1
        LIMIT $3
        "#,
    )
    .bind(team_id)
    .bind(person_id)
    .bind((RESOLVE_DISTINCT_IDS_LIMIT + 1) as i64)
    .fetch_all(&mut *conn)
    .await?;

    let is_truncated = rows.len() > RESOLVE_DISTINCT_IDS_LIMIT;
    let distinct_ids: Vec<String> = rows
        .into_iter()
        .take(RESOLVE_DISTINCT_IDS_LIMIT)
        .map(|(did,)| did)
        .collect();

    Ok(ResolveDistinctIdsResponse {
        person_uuid: person_uuid.to_string(),
        distinct_ids,
        is_truncated,
    })
}

/// /create — get-or-create a person for a single distinct_id.
/// New persons start with is_identified = false; existing persons return their
/// current is_identified status.
pub async fn handle_create(
    pool: &PgPool,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<CreateResponse> {
    validate_distinct_id(distinct_id)?;
    let mut tx = pool.begin().await?;
    let resolved = identify_tx(&mut tx, team_id, distinct_id).await?;
    tx.commit().await?;
    Ok(CreateResponse {
        person_uuid: resolved.person_uuid,
        is_identified: resolved.is_identified,
    })
}

/// Insert a new distinct_id_mappings row and a union_find link row pointing
/// at an existing chain member. Returns the new distinct_id_mappings PK.
async fn insert_did_and_link(
    conn: &mut PgConnection,
    team_id: i64,
    distinct_id: &str,
    next_pk: i64,
) -> DbResult<i64> {
    let new_pk: i64 = sqlx::query_scalar(
        "INSERT INTO distinct_id_mappings (team_id, distinct_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(team_id)
    .bind(distinct_id)
    .fetch_one(&mut *conn)
    .await?;

    sqlx::query(
        "INSERT INTO union_find (team_id, current, next, person_id) VALUES ($1, $2, $3, NULL)",
    )
    .bind(team_id)
    .bind(new_pk)
    .bind(next_pk)
    .execute(&mut *conn)
    .await?;

    Ok(new_pk)
}

/// /alias and /identify — merge two distinct_ids per PostHog semantics.
///
/// Uses check_did to transparently handle distinct_ids belonging to deleted
/// persons (lazy unlink). Orphaned distinct_ids are treated like new ones
/// but reuse their existing did_pk via attach_did/attach_did_as_root.
///
/// On success, sets is_identified = true on the resulting person.
/// Returns a CompressHint when the resulting chain exceeds compress_threshold.
pub async fn handle_alias(
    pool: &PgPool,
    team_id: i64,
    target: &str,
    source: &str,
    compress_threshold: i32,
) -> DbResult<(AliasResponse, Option<CompressHint>)> {
    validate_distinct_id(target)?;
    validate_distinct_id(source)?;

    if target == source {
        let mut tx = pool.begin().await?;
        let resolved = identify_tx(&mut tx, team_id, target).await?;
        set_identified(&mut tx, resolved.person_id).await?;
        tx.commit().await?;
        return Ok((
            AliasResponse {
                person_uuid: resolved.person_uuid,
                is_identified: true,
            },
            None,
        ));
    }

    let mut tx = pool.begin().await?;

    let target_state = check_did(&mut tx, team_id, target).await?;
    let source_state = check_did(&mut tx, team_id, source).await?;

    let (person_uuid, compress_hint) = match (target_state, source_state) {
        (DidState::Live(tpk, t_resolved, t_depth), DidState::Live(spk, s_resolved, s_depth)) => {
            if t_resolved.person_id == s_resolved.person_id {
                set_identified(&mut tx, t_resolved.person_id).await?;
                (t_resolved.person_uuid, None)
            } else {
                if s_resolved.is_identified {
                    return Err(DbError::AlreadyIdentified(
                        "source person is already identified; use /merge to force".into(),
                    ));
                }

                let source_root = resolve_root(&mut tx, team_id, spk)
                    .await?
                    .ok_or_else(|| DbError::Internal("source chain has no root".into()))?;

                link_root_to_target(&mut tx, team_id, source_root.0, source_root.1, tpk).await?;

                set_identified(&mut tx, t_resolved.person_id).await?;

                let combined_depth = s_depth + t_depth + 1;
                let hint = if combined_depth > compress_threshold {
                    Some(CompressHint {
                        distinct_id: source.to_string(),
                        depth: combined_depth,
                    })
                } else {
                    None
                };
                (t_resolved.person_uuid, hint)
            }
        }

        (DidState::Live(tpk, t_resolved, t_depth), source_state) => {
            attach_did(&mut tx, team_id, source, source_state, tpk).await?;
            set_identified(&mut tx, t_resolved.person_id).await?;
            let combined_depth = t_depth + 1;
            let hint = if combined_depth > compress_threshold {
                Some(CompressHint {
                    distinct_id: source.to_string(),
                    depth: combined_depth,
                })
            } else {
                None
            };
            (t_resolved.person_uuid, hint)
        }

        (target_state, DidState::Live(spk, s_resolved, s_depth)) => {
            attach_did(&mut tx, team_id, target, target_state, spk).await?;
            set_identified(&mut tx, s_resolved.person_id).await?;
            let combined_depth = s_depth + 1;
            let hint = if combined_depth > compress_threshold {
                Some(CompressHint {
                    distinct_id: target.to_string(),
                    depth: combined_depth,
                })
            } else {
                None
            };
            (s_resolved.person_uuid, hint)
        }

        (target_state, source_state) => {
            let person_uuid = Uuid::new_v4().to_string();

            let person_id: i64 = sqlx::query_scalar(
                "INSERT INTO person_mapping (team_id, person_uuid, is_identified) \
                 VALUES ($1, $2, true) RETURNING person_id",
            )
            .bind(team_id)
            .bind(&person_uuid)
            .fetch_one(&mut *tx)
            .await?;

            let target_pk =
                attach_did_as_root(&mut tx, team_id, target, target_state, person_id).await?;
            attach_did(&mut tx, team_id, source, source_state, target_pk).await?;

            (person_uuid, None)
        }
    };

    tx.commit().await?;

    Ok((
        AliasResponse {
            person_uuid,
            is_identified: true,
        },
        compress_hint,
    ))
}

/// /merge — merge N source distinct_ids into target's person ($merge_dangerously).
/// Ignores is_identified — always merges. Sets is_identified = true on the result.
///
/// Uses check_did for the target and each source so that distinct_ids belonging
/// to deleted persons are lazily unlinked and handled transparently.
/// Returns a CompressHint for the deepest source chain when it exceeds threshold.
pub async fn handle_merge(
    pool: &PgPool,
    team_id: i64,
    target: &str,
    sources: &[String],
    compress_threshold: i32,
) -> DbResult<(MergeResponse, Option<CompressHint>)> {
    validate_distinct_id(target)?;
    for src in sources {
        validate_distinct_id(src)?;
    }

    let mut tx = pool.begin().await?;

    let (target_pk, target_person_id, target_person_uuid, target_depth) =
        match check_did(&mut tx, team_id, target).await? {
            DidState::NotFound => {
                return Err(DbError::NotFound(format!(
                    "target distinct_id '{target}' not found"
                )));
            }
            DidState::Orphaned(pk) => {
                let person_uuid = Uuid::new_v4().to_string();
                let person_id: i64 = sqlx::query_scalar(
                    "INSERT INTO person_mapping (team_id, person_uuid) \
                     VALUES ($1, $2) RETURNING person_id",
                )
                .bind(team_id)
                .bind(&person_uuid)
                .fetch_one(&mut *tx)
                .await?;
                root_did(&mut tx, team_id, pk, person_id).await?;
                (pk, person_id, person_uuid, 0i32)
            }
            DidState::Live(pk, resolved, depth) => {
                (pk, resolved.person_id, resolved.person_uuid, depth)
            }
        };

    let mut max_combined_depth = 0i32;
    let mut deepest_source: Option<String> = None;

    for src in sources {
        match check_did(&mut tx, team_id, src).await? {
            DidState::NotFound => {
                insert_did_and_link(&mut tx, team_id, src, target_pk).await?;
                let combined = target_depth + 1;
                if combined > max_combined_depth {
                    max_combined_depth = combined;
                    deepest_source = Some(src.clone());
                }
            }
            DidState::Orphaned(pk) => {
                link_did(&mut tx, team_id, pk, target_pk).await?;
                let combined = target_depth + 1;
                if combined > max_combined_depth {
                    max_combined_depth = combined;
                    deepest_source = Some(src.clone());
                }
            }
            DidState::Live(spk, _, s_depth) => {
                let root = resolve_root(&mut tx, team_id, spk).await?;
                match root {
                    Some((_, root_person, _)) if root_person == target_person_id => {}
                    Some((root_current, old_person, _)) => {
                        link_root_to_target(&mut tx, team_id, root_current, old_person, target_pk)
                            .await?;
                        let combined = s_depth + target_depth + 1;
                        if combined > max_combined_depth {
                            max_combined_depth = combined;
                            deepest_source = Some(src.clone());
                        }
                    }
                    None => {
                        return Err(DbError::Internal(format!(
                            "source '{src}' exists but chain has no root"
                        )));
                    }
                }
            }
        }
    }

    set_identified(&mut tx, target_person_id).await?;
    tx.commit().await?;

    let compress_hint = if max_combined_depth > compress_threshold {
        deepest_source.map(|did| CompressHint {
            distinct_id: did,
            depth: max_combined_depth,
        })
    } else {
        None
    };

    Ok((
        MergeResponse {
            person_uuid: target_person_uuid,
            is_identified: true,
        },
        compress_hint,
    ))
}

/// /batched_merge — same semantics as handle_merge but uses batched SQL
/// to resolve and link all sources in bulk rather than one-at-a-time.
///
/// The algorithm:
///   1. Resolve target via check_did (same as handle_merge).
///   2. Batch-lookup all source distinct_ids in one query.
///   3. Batch-resolve all found PKs to their roots in one recursive CTE.
///   4. Classify each source: NotFound / Orphaned / Live-same / Live-different.
///   5. Batch-insert all NotFound sources (2 queries).
///   6. Process orphaned sources sequentially (rare; reuses unlink_did + link_did).
///   7. Batch-link all Live-different roots to target (2 queries, deduped by root).
///   8. set_identified + commit.
pub async fn handle_batched_merge(
    pool: &PgPool,
    team_id: i64,
    target: &str,
    sources: &[String],
    compress_threshold: i32,
) -> DbResult<(MergeResponse, Option<CompressHint>)> {
    validate_distinct_id(target)?;
    for src in sources {
        validate_distinct_id(src)?;
    }

    let mut tx = pool.begin().await?;

    // --- Step 1: resolve target (identical to handle_merge) -----------------
    let (target_pk, target_person_id, target_person_uuid, target_depth) =
        match check_did(&mut tx, team_id, target).await? {
            DidState::NotFound => {
                return Err(DbError::NotFound(format!(
                    "target distinct_id '{target}' not found"
                )));
            }
            DidState::Orphaned(pk) => {
                let person_uuid = Uuid::new_v4().to_string();
                let person_id: i64 = sqlx::query_scalar(
                    "INSERT INTO person_mapping (team_id, person_uuid) \
                     VALUES ($1, $2) RETURNING person_id",
                )
                .bind(team_id)
                .bind(&person_uuid)
                .fetch_one(&mut *tx)
                .await?;
                root_did(&mut tx, team_id, pk, person_id).await?;
                (pk, person_id, person_uuid, 0i32)
            }
            DidState::Live(pk, resolved, depth) => {
                (pk, resolved.person_id, resolved.person_uuid, depth)
            }
        };

    if sources.is_empty() {
        set_identified(&mut tx, target_person_id).await?;
        tx.commit().await?;
        return Ok((
            MergeResponse {
                person_uuid: target_person_uuid,
                is_identified: true,
            },
            None,
        ));
    }

    // Dedup sources while preserving first-occurrence order.
    let unique_sources: Vec<&String> = {
        let mut seen = std::collections::HashSet::new();
        sources.iter().filter(|s| seen.insert(s.as_str())).collect()
    };

    // --- Step 2: batch lookup -----------------------------------------------
    let source_strs: Vec<String> = unique_sources.iter().map(|s| s.to_string()).collect();
    let did_map = batch_lookup_dids(&mut tx, team_id, &source_strs).await?;

    // --- Step 3: batch resolve found PKs ------------------------------------
    let found_pks: Vec<i64> = did_map.values().copied().collect();
    let resolve_rows = batch_resolve_pks(&mut tx, team_id, &found_pks).await?;

    let resolve_map: HashMap<i64, &BatchResolveRow> =
        resolve_rows.iter().map(|r| (r.start_node, r)).collect();

    // --- Step 4: classify ---------------------------------------------------
    let mut not_found: Vec<&str> = Vec::new();
    let mut orphaned_pks: Vec<i64> = Vec::new();
    // (root_current, old_person_id) — will be deduped before writing
    let mut live_diff: Vec<(i64, i64)> = Vec::new();

    let mut max_combined_depth = 0i32;
    let mut deepest_source: Option<String> = None;

    for src in &unique_sources {
        let src_str = src.as_str();
        match did_map.get(src_str) {
            None => {
                not_found.push(src_str);
                let combined = target_depth + 1;
                if combined > max_combined_depth {
                    max_combined_depth = combined;
                    deepest_source = Some(src_str.to_string());
                }
            }
            Some(&pk) => match resolve_map.get(&pk) {
                None => {
                    orphaned_pks.push(pk);
                    let combined = target_depth + 1;
                    if combined > max_combined_depth {
                        max_combined_depth = combined;
                        deepest_source = Some(src_str.to_string());
                    }
                }
                Some(row) if row.person_id == target_person_id => {
                    // already same person — skip
                }
                Some(row) => {
                    live_diff.push((row.root_current, row.person_id));
                    let combined = row.depth + target_depth + 1;
                    if combined > max_combined_depth {
                        max_combined_depth = combined;
                        deepest_source = Some(src_str.to_string());
                    }
                }
            },
        }
    }

    // --- Step 5: batch insert NotFound sources ------------------------------
    if !not_found.is_empty() {
        let new_rows = sqlx::query_as::<_, (i64,)>(
            "INSERT INTO distinct_id_mappings (team_id, distinct_id) \
             SELECT $1, unnest($2::text[]) RETURNING id",
        )
        .bind(team_id)
        .bind(&not_found)
        .fetch_all(&mut *tx)
        .await?;

        let new_pks: Vec<i64> = new_rows.into_iter().map(|(id,)| id).collect();

        for &new_pk in &new_pks {
            sqlx::query(
                "INSERT INTO union_find (team_id, current, next, person_id) \
                 VALUES ($1, $2, $3, NULL)",
            )
            .bind(team_id)
            .bind(new_pk)
            .bind(target_pk)
            .execute(&mut *tx)
            .await?;
        }
    }

    // --- Step 6: orphaned sources (sequential — rare case) ------------------
    for pk in &orphaned_pks {
        unlink_did(&mut tx, team_id, *pk).await?;
        link_did(&mut tx, team_id, *pk, target_pk).await?;
    }

    // --- Step 7: batch link Live-different roots ----------------------------
    // Dedup by root_current so we only link each root once.
    let mut seen_roots = std::collections::HashSet::new();
    let mut unique_roots: Vec<i64> = Vec::new();
    let mut old_person_ids: Vec<i64> = Vec::new();
    for (root_current, old_person) in &live_diff {
        if seen_roots.insert(*root_current) {
            unique_roots.push(*root_current);
            old_person_ids.push(*old_person);
        }
    }

    if !unique_roots.is_empty() {
        sqlx::query(
            "UPDATE union_find SET person_id = NULL, next = $3 \
             WHERE team_id = $1 AND current = ANY($2::bigint[])",
        )
        .bind(team_id)
        .bind(&unique_roots)
        .bind(target_pk)
        .execute(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM person_mapping WHERE person_id = ANY($1::bigint[])")
            .bind(&old_person_ids)
            .execute(&mut *tx)
            .await?;
    }

    // --- Step 8: set_identified + commit ------------------------------------
    set_identified(&mut tx, target_person_id).await?;
    tx.commit().await?;

    let compress_hint = if max_combined_depth > compress_threshold {
        deepest_source.map(|did| CompressHint {
            distinct_id: did,
            depth: max_combined_depth,
        })
    } else {
        None
    };

    Ok((
        MergeResponse {
            person_uuid: target_person_uuid,
            is_identified: true,
        },
        compress_hint,
    ))
}

/// /delete_person — soft-delete a person by setting deleted_at on the
/// person_mapping row and all union_find roots that reference it.
/// The distinct_ids are left in place and lazily cleaned up by future
/// write operations via check_did.
pub async fn handle_delete_person(
    pool: &PgPool,
    team_id: i64,
    person_uuid: &str,
) -> DbResult<DeletePersonResponse> {
    let mut tx = pool.begin().await?;

    let person_id: i64 = sqlx::query_scalar(
        "SELECT person_id FROM person_mapping \
         WHERE team_id = $1 AND person_uuid = $2 AND deleted_at IS NULL",
    )
    .bind(team_id)
    .bind(person_uuid)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| DbError::NotFound(format!("person '{person_uuid}' not found")))?;

    sqlx::query("UPDATE person_mapping SET deleted_at = now() WHERE person_id = $1")
        .bind(person_id)
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        "UPDATE union_find SET deleted_at = now() \
         WHERE team_id = $1 AND person_id = $2 AND deleted_at IS NULL",
    )
    .bind(team_id)
    .bind(person_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(DeletePersonResponse {
        person_uuid: person_uuid.to_string(),
    })
}

/// /delete_distinct_id — unlink a distinct_id from its chain, then hard-delete
/// the union_find row and distinct_id_mappings row. If the deleted node was the
/// last root for a person, the person_mapping is soft-deleted as a side effect.
pub async fn handle_delete_distinct_id(
    pool: &PgPool,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<DeleteDistinctIdResponse> {
    let mut tx = pool.begin().await?;

    let did_pk = lookup_did(&mut tx, team_id, distinct_id)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("distinct_id '{distinct_id}' not found")))?;

    let original_person_id: Option<i64> =
        sqlx::query_scalar("SELECT person_id FROM union_find WHERE team_id = $1 AND current = $2")
            .bind(team_id)
            .bind(did_pk)
            .fetch_optional(&mut *tx)
            .await?
            .unwrap_or(None);

    unlink_did(&mut tx, team_id, did_pk).await?;

    sqlx::query("DELETE FROM union_find WHERE team_id = $1 AND current = $2")
        .bind(team_id)
        .bind(did_pk)
        .execute(&mut *tx)
        .await?;

    sqlx::query("DELETE FROM distinct_id_mappings WHERE id = $1")
        .bind(did_pk)
        .execute(&mut *tx)
        .await?;

    let mut person_deleted = false;
    if let Some(pid) = original_person_id {
        let still_referenced: bool = sqlx::query_scalar(
            "SELECT EXISTS(\
               SELECT 1 FROM union_find \
               WHERE team_id = $1 AND person_id = $2 AND deleted_at IS NULL\
             )",
        )
        .bind(team_id)
        .bind(pid)
        .fetch_one(&mut *tx)
        .await?;

        if !still_referenced {
            sqlx::query(
                "UPDATE person_mapping SET deleted_at = now() \
                 WHERE person_id = $1 AND deleted_at IS NULL",
            )
            .bind(pid)
            .execute(&mut *tx)
            .await?;
            person_deleted = true;
        }
    }

    tx.commit().await?;

    Ok(DeleteDistinctIdResponse {
        distinct_id: distinct_id.to_string(),
        person_deleted,
    })
}
