use sqlx::PgPool;
use sqlx::postgres::PgConnection;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::models::{AliasResponse, CreateResponse, DbError, DbOp, DbResult, MergeResponse};

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

pub async fn worker_loop(pool: PgPool, mut rx: mpsc::Receiver<DbOp>) {
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
                let _ = reply.send(handle_alias(&pool, team_id, &target, &source).await);
            }
            DbOp::Merge {
                team_id,
                target,
                sources,
                reply,
            } => {
                let _ = reply.send(handle_merge(&pool, team_id, &target, &sources).await);
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

/// Walk the union_find chain from a distinct_id PK to the root, returning
/// the root's person_id and the corresponding person_uuid.
async fn resolve_by_pk(
    conn: &mut PgConnection,
    team_id: i64,
    did_pk: i64,
) -> DbResult<Option<ResolvedPerson>> {
    let row = sqlx::query_as::<_, (String, i64, bool)>(
        r#"
        WITH RECURSIVE walk(node, depth) AS (
            SELECT $2::bigint, 0

            UNION ALL

            SELECT uf.next, w.depth + 1
            FROM walk w
            JOIN union_find uf
              ON uf.team_id = $1 AND uf.current = w.node AND uf.person_id IS NULL
            WHERE w.depth < 1000
        )
        SELECT pm.person_uuid, uf.person_id, pm.is_identified
        FROM union_find uf
        JOIN person_mapping pm ON pm.person_id = uf.person_id
        WHERE uf.team_id = $1
          AND uf.current = (SELECT node FROM walk ORDER BY depth DESC LIMIT 1)
        "#,
    )
    .bind(team_id)
    .bind(did_pk)
    .fetch_optional(&mut *conn)
    .await?;

    Ok(
        row.map(|(person_uuid, person_id, is_identified)| ResolvedPerson {
            person_uuid,
            person_id,
            is_identified,
        }),
    )
}

/// Walk the union_find chain from a distinct_id PK to the root, returning
/// the root's (team_id, current) composite key and person_id.
async fn resolve_root(
    conn: &mut PgConnection,
    team_id: i64,
    did_pk: i64,
) -> DbResult<Option<(i64, i64)>> {
    let row = sqlx::query_as::<_, (i64, i64)>(
        r#"
        WITH RECURSIVE walk(node, depth) AS (
            SELECT $2::bigint, 0

            UNION ALL

            SELECT uf.next, w.depth + 1
            FROM walk w
            JOIN union_find uf
              ON uf.team_id = $1 AND uf.current = w.node AND uf.person_id IS NULL
            WHERE w.depth < 1000
        )
        SELECT uf.current, uf.person_id
        FROM union_find uf
        WHERE uf.team_id = $1
          AND uf.current = (SELECT node FROM walk ORDER BY depth DESC LIMIT 1)
          AND uf.person_id IS NOT NULL
        "#,
    )
    .bind(team_id)
    .bind(did_pk)
    .fetch_optional(&mut *conn)
    .await?;

    Ok(row)
}

/// Resolve a distinct_id string all the way to a ResolvedPerson.
pub async fn resolve_tx(
    conn: &mut PgConnection,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<Option<ResolvedPerson>> {
    let did_pk = match lookup_did(&mut *conn, team_id, distinct_id).await? {
        Some(pk) => pk,
        None => return Ok(None),
    };
    resolve_by_pk(&mut *conn, team_id, did_pk).await
}

/// Get-or-create: if the distinct_id exists, resolve it; otherwise create a
/// new person_mapping + distinct_id_mappings + union_find root row.
/// Returns the person_uuid string so callers can wrap it in any response type.
/// The person is created with is_identified = false (DB default).
pub async fn identify_tx(
    conn: &mut PgConnection,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<String> {
    if let Some(resolved) = resolve_tx(&mut *conn, team_id, distinct_id).await? {
        return Ok(resolved.person_uuid);
    }

    let person_uuid = Uuid::new_v4().to_string();

    let person_id: i64 = sqlx::query_scalar(
        "INSERT INTO person_mapping (team_id, person_uuid) VALUES ($1, $2) RETURNING person_id",
    )
    .bind(team_id)
    .bind(&person_uuid)
    .fetch_one(&mut *conn)
    .await?;

    let did_pk: i64 = sqlx::query_scalar(
        "INSERT INTO distinct_id_mappings (team_id, distinct_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(team_id)
    .bind(distinct_id)
    .fetch_one(&mut *conn)
    .await?;

    sqlx::query(
        "INSERT INTO union_find (team_id, current, next, person_id) VALUES ($1, $2, NULL, $3)",
    )
    .bind(team_id)
    .bind(did_pk)
    .bind(person_id)
    .execute(&mut *conn)
    .await?;

    Ok(person_uuid)
}

/// Mark a person as identified (idempotent).
async fn set_identified(conn: &mut PgConnection, person_id: i64) -> DbResult<()> {
    sqlx::query(
        "UPDATE person_mapping SET is_identified = true \
         WHERE person_id = $1 AND NOT is_identified",
    )
    .bind(person_id)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Re-point a root's person_id and clean up orphaned person_mapping rows.
/// Used by both handle_alias (Case 2b) and handle_merge.
async fn repoint_root(
    conn: &mut PgConnection,
    team_id: i64,
    root_current: i64,
    old_person: i64,
    new_person: i64,
) -> DbResult<()> {
    sqlx::query(
        "UPDATE union_find SET person_id = $3 \
         WHERE team_id = $1 AND current = $2",
    )
    .bind(team_id)
    .bind(root_current)
    .bind(new_person)
    .execute(&mut *conn)
    .await?;

    let still_referenced: bool = sqlx::query_scalar(
        "SELECT EXISTS(\
           SELECT 1 FROM union_find \
           WHERE team_id = $1 AND person_id = $2\
         )",
    )
    .bind(team_id)
    .bind(old_person)
    .fetch_one(&mut *conn)
    .await?;

    if !still_referenced {
        sqlx::query("DELETE FROM person_mapping WHERE person_id = $1")
            .bind(old_person)
            .execute(&mut *conn)
            .await?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Public API — each operation wrapped in an explicit transaction.
// ---------------------------------------------------------------------------

/// Resolve a distinct_id to its canonical person through the union_find chain.
pub async fn resolve(
    pool: &PgPool,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<Option<ResolvedPerson>> {
    let mut conn = pool.acquire().await?;
    resolve_tx(&mut conn, team_id, distinct_id).await
}

/// /create — get-or-create a person for a single distinct_id.
/// New persons start with is_identified = false; existing persons return their
/// current is_identified status.
pub async fn handle_create(
    pool: &PgPool,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<CreateResponse> {
    let mut tx = pool.begin().await?;
    identify_tx(&mut tx, team_id, distinct_id).await?;
    let resolved = resolve_tx(&mut tx, team_id, distinct_id)
        .await?
        .ok_or_else(|| DbError::Internal("just created but can't resolve".into()))?;
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
/// Parameters:
///   target = the keeper (primary / identified side)
///   source = the one being absorbed (anonymous / alias side)
///
/// Handles 5 cases:
///   target==source: get-or-create shortcut
///   1a. target exists, source doesn't → link source into target's chain
///   1b. source exists, target doesn't → link target into source's chain
///   2a. both exist, same person       → no-op
///   2b. both exist, diff persons      → merge if source is unidentified; reject otherwise
///   3.  neither exists                → create person with both distinct_ids
///
/// On success, sets is_identified = true on the resulting person.
pub async fn handle_alias(
    pool: &PgPool,
    team_id: i64,
    target: &str,
    source: &str,
) -> DbResult<AliasResponse> {
    if target == source {
        let mut tx = pool.begin().await?;
        let person_uuid = identify_tx(&mut tx, team_id, target).await?;
        let resolved = resolve_tx(&mut tx, team_id, target)
            .await?
            .ok_or_else(|| DbError::Internal("just created but can't resolve".into()))?;
        set_identified(&mut tx, resolved.person_id).await?;
        tx.commit().await?;
        return Ok(AliasResponse {
            person_uuid,
            is_identified: true,
        });
    }

    let mut tx = pool.begin().await?;

    let target_pk = lookup_did(&mut tx, team_id, target).await?;
    let source_pk = lookup_did(&mut tx, team_id, source).await?;

    let person_uuid = match (target_pk, source_pk) {
        // Case 1a: target exists, source doesn't — link source into target's chain
        (Some(tpk), None) => {
            insert_did_and_link(&mut tx, team_id, source, tpk).await?;
            let resolved = resolve_by_pk(&mut tx, team_id, tpk)
                .await?
                .ok_or_else(|| DbError::Internal("target chain has no root".into()))?;
            set_identified(&mut tx, resolved.person_id).await?;
            resolved.person_uuid
        }

        // Case 1b: source exists, target doesn't — link target into source's chain
        (None, Some(spk)) => {
            insert_did_and_link(&mut tx, team_id, target, spk).await?;
            let resolved = resolve_by_pk(&mut tx, team_id, spk)
                .await?
                .ok_or_else(|| DbError::Internal("source chain has no root".into()))?;
            set_identified(&mut tx, resolved.person_id).await?;
            resolved.person_uuid
        }

        // Case 2: both exist — check if same or different person
        (Some(tpk), Some(spk)) => {
            let target_root = resolve_root(&mut tx, team_id, tpk)
                .await?
                .ok_or_else(|| DbError::Internal("target chain has no root".into()))?;
            let source_root = resolve_root(&mut tx, team_id, spk)
                .await?
                .ok_or_else(|| DbError::Internal("source chain has no root".into()))?;

            if target_root.1 == source_root.1 {
                // Case 2a: same person — no-op, just ensure identified
                set_identified(&mut tx, target_root.1).await?;
                resolve_by_pk(&mut tx, team_id, tpk)
                    .await?
                    .ok_or_else(|| DbError::Internal("target chain has no root".into()))?
                    .person_uuid
            } else {
                // Case 2b: different persons — check source's is_identified
                let source_identified: bool = sqlx::query_scalar(
                    "SELECT is_identified FROM person_mapping WHERE person_id = $1",
                )
                .bind(source_root.1)
                .fetch_one(&mut *tx)
                .await?;

                if source_identified {
                    return Err(DbError::AlreadyIdentified(
                        "source person is already identified; use /merge to force".into(),
                    ));
                }

                // Merge source into target
                repoint_root(
                    &mut tx,
                    team_id,
                    source_root.0,
                    source_root.1,
                    target_root.1,
                )
                .await?;

                set_identified(&mut tx, target_root.1).await?;
                resolve_by_pk(&mut tx, team_id, tpk)
                    .await?
                    .ok_or_else(|| DbError::Internal("target chain has no root".into()))?
                    .person_uuid
            }
        }

        // Case 3: neither exists — create person with both distinct_ids
        (None, None) => {
            let person_uuid = Uuid::new_v4().to_string();

            let person_id: i64 = sqlx::query_scalar(
                "INSERT INTO person_mapping (team_id, person_uuid, is_identified) \
                 VALUES ($1, $2, true) RETURNING person_id",
            )
            .bind(team_id)
            .bind(&person_uuid)
            .fetch_one(&mut *tx)
            .await?;

            let target_new_pk: i64 = sqlx::query_scalar(
                "INSERT INTO distinct_id_mappings (team_id, distinct_id) VALUES ($1, $2) \
                 RETURNING id",
            )
            .bind(team_id)
            .bind(target)
            .fetch_one(&mut *tx)
            .await?;

            sqlx::query(
                "INSERT INTO union_find (team_id, current, next, person_id) \
                 VALUES ($1, $2, NULL, $3)",
            )
            .bind(team_id)
            .bind(target_new_pk)
            .bind(person_id)
            .execute(&mut *tx)
            .await?;

            insert_did_and_link(&mut tx, team_id, source, target_new_pk).await?;

            person_uuid
        }
    };

    tx.commit().await?;

    Ok(AliasResponse {
        person_uuid,
        is_identified: true,
    })
}

/// /merge — merge N source distinct_ids into target's person ($merge_dangerously).
/// Ignores is_identified — always merges. Sets is_identified = true on the result.
///
/// For each source:
///   - If source doesn't exist: create mapping + link to target.
///   - If source exists and already shares target's person: skip.
///   - If source exists with a different person: re-point source's root to target's person.
pub async fn handle_merge(
    pool: &PgPool,
    team_id: i64,
    target: &str,
    sources: &[String],
) -> DbResult<MergeResponse> {
    let mut tx = pool.begin().await?;

    let target_pk = lookup_did(&mut tx, team_id, target)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("target distinct_id '{target}' not found")))?;

    let target_resolved = resolve_by_pk(&mut tx, team_id, target_pk)
        .await?
        .ok_or_else(|| DbError::Internal("target chain has no root".into()))?;

    let target_person_id = target_resolved.person_id;

    for src in sources {
        let src_pk = lookup_did(&mut tx, team_id, src).await?;

        match src_pk {
            None => {
                insert_did_and_link(&mut tx, team_id, src, target_pk).await?;
            }
            Some(spk) => {
                let root = resolve_root(&mut tx, team_id, spk).await?;
                match root {
                    Some((root_current, root_person)) if root_person == target_person_id => {
                        let _ = root_current;
                    }
                    Some((root_current, old_person)) => {
                        repoint_root(&mut tx, team_id, root_current, old_person, target_person_id)
                            .await?;
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

    Ok(MergeResponse {
        person_uuid: target_resolved.person_uuid,
        is_identified: true,
    })
}
