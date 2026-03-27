use sqlx::PgPool;
use sqlx::postgres::PgConnection;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::models::{
    CreateAliasResponse, DbError, DbOp, DbResult, IdentifyResponse, MergeResponse,
};

// ---------------------------------------------------------------------------
// Resolved person — returned by the recursive CTE
// ---------------------------------------------------------------------------

pub struct ResolvedPerson {
    pub person_uuid: String,
    pub person_id: i64,
}

// ---------------------------------------------------------------------------
// Worker loop — pulls DbOps from the channel and executes them sequentially.
// ---------------------------------------------------------------------------

pub async fn worker_loop(pool: PgPool, mut rx: mpsc::Receiver<DbOp>) {
    while let Some(op) = rx.recv().await {
        match op {
            DbOp::Identify {
                team_id,
                distinct_id,
                reply,
            } => {
                let _ = reply.send(handle_identify(&pool, team_id, &distinct_id).await);
            }
            DbOp::CreateAlias {
                team_id,
                src,
                dest,
                reply,
            } => {
                let _ = reply.send(handle_create_alias(&pool, team_id, &src, &dest).await);
            }
            DbOp::Merge {
                team_id,
                src,
                dests,
                reply,
            } => {
                let _ = reply.send(handle_merge(&pool, team_id, &src, &dests).await);
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
    let row = sqlx::query_as::<_, (String, i64)>(
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
        SELECT pm.person_uuid, uf.person_id
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

    Ok(row.map(|(person_uuid, person_id)| ResolvedPerson {
        person_uuid,
        person_id,
    }))
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
pub async fn identify_tx(
    conn: &mut PgConnection,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<IdentifyResponse> {
    if let Some(resolved) = resolve_tx(&mut *conn, team_id, distinct_id).await? {
        return Ok(IdentifyResponse {
            person_uuid: resolved.person_uuid,
        });
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

    Ok(IdentifyResponse { person_uuid })
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

/// /identify — get-or-create a person for a single distinct_id.
pub async fn handle_identify(
    pool: &PgPool,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<IdentifyResponse> {
    let mut tx = pool.begin().await?;
    let result = identify_tx(&mut tx, team_id, distinct_id).await?;
    tx.commit().await?;
    Ok(result)
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

/// /create_alias — merge two distinct_ids, matching PostHog's $identify and
/// $create_alias semantics.
///
/// Handles 4 cases:
///   1a. src exists, dest doesn't → link dest into src's chain
///   1b. dest exists, src doesn't → link src into dest's chain
///   2a. both exist, same person  → no-op
///   2b. both exist, diff persons → reject (caller must use /merge)
///   3.  neither exists           → create person with both distinct_ids
pub async fn handle_create_alias(
    pool: &PgPool,
    team_id: i64,
    src: &str,
    dest: &str,
) -> DbResult<CreateAliasResponse> {
    let mut tx = pool.begin().await?;

    let src_pk = lookup_did(&mut tx, team_id, src).await?;
    let dest_pk = lookup_did(&mut tx, team_id, dest).await?;

    let person_uuid = match (src_pk, dest_pk) {
        // Case 1a: src exists, dest doesn't — link dest to src's chain
        (Some(spk), None) => {
            insert_did_and_link(&mut tx, team_id, dest, spk).await?;
            resolve_by_pk(&mut tx, team_id, spk)
                .await?
                .ok_or_else(|| DbError::Internal("src chain has no root".into()))?
                .person_uuid
        }

        // Case 1b: dest exists, src doesn't — link src to dest's chain
        (None, Some(dpk)) => {
            insert_did_and_link(&mut tx, team_id, src, dpk).await?;
            resolve_by_pk(&mut tx, team_id, dpk)
                .await?
                .ok_or_else(|| DbError::Internal("dest chain has no root".into()))?
                .person_uuid
        }

        // Case 2: both exist — check if same or different person
        (Some(spk), Some(dpk)) => {
            let src_root = resolve_root(&mut tx, team_id, spk)
                .await?
                .ok_or_else(|| DbError::Internal("src chain has no root".into()))?;
            let dest_root = resolve_root(&mut tx, team_id, dpk)
                .await?
                .ok_or_else(|| DbError::Internal("dest chain has no root".into()))?;

            if src_root.1 == dest_root.1 {
                // Case 2a: same person — no-op
                resolve_by_pk(&mut tx, team_id, spk)
                    .await?
                    .ok_or_else(|| DbError::Internal("src chain has no root".into()))?
                    .person_uuid
            } else {
                // Case 2b: different persons — reject, caller must use /merge
                return Err(DbError::AlreadyIdentified(
                    "both distinct_ids exist with different persons; use /merge to force".into(),
                ));
            }
        }

        // Case 3: neither exists — create person with both distinct_ids
        (None, None) => {
            let person_uuid = Uuid::new_v4().to_string();

            let person_id: i64 = sqlx::query_scalar(
                "INSERT INTO person_mapping (team_id, person_uuid) VALUES ($1, $2) \
                 RETURNING person_id",
            )
            .bind(team_id)
            .bind(&person_uuid)
            .fetch_one(&mut *tx)
            .await?;

            let src_new_pk: i64 = sqlx::query_scalar(
                "INSERT INTO distinct_id_mappings (team_id, distinct_id) VALUES ($1, $2) \
                 RETURNING id",
            )
            .bind(team_id)
            .bind(src)
            .fetch_one(&mut *tx)
            .await?;

            sqlx::query(
                "INSERT INTO union_find (team_id, current, next, person_id) \
                 VALUES ($1, $2, NULL, $3)",
            )
            .bind(team_id)
            .bind(src_new_pk)
            .bind(person_id)
            .execute(&mut *tx)
            .await?;

            insert_did_and_link(&mut tx, team_id, dest, src_new_pk).await?;

            person_uuid
        }
    };

    tx.commit().await?;

    Ok(CreateAliasResponse { person_uuid })
}

/// /merge — merge N dest distinct_ids into src's person ($merge_dangerously).
///
/// For each dest:
///   - If dest doesn't exist: create mapping + link to src (like create_alias).
///   - If dest exists and already shares src's person: skip.
///   - If dest exists with a different person: re-point dest's root to src's person.
pub async fn handle_merge(
    pool: &PgPool,
    team_id: i64,
    src: &str,
    dests: &[String],
) -> DbResult<MergeResponse> {
    let mut tx = pool.begin().await?;

    let src_pk = lookup_did(&mut tx, team_id, src)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("src distinct_id '{src}' not found")))?;

    let src_resolved = resolve_by_pk(&mut tx, team_id, src_pk)
        .await?
        .ok_or_else(|| DbError::Internal("src chain has no root".into()))?;

    let src_person_id = src_resolved.person_id;

    for dest in dests {
        let dest_pk = lookup_did(&mut tx, team_id, dest).await?;

        match dest_pk {
            None => {
                insert_did_and_link(&mut tx, team_id, dest, src_pk).await?;
            }
            Some(dpk) => {
                let root = resolve_root(&mut tx, team_id, dpk).await?;
                match root {
                    Some((root_current, root_person)) if root_person == src_person_id => {
                        let _ = root_current;
                    }
                    Some((root_current, _old_person)) => {
                        sqlx::query(
                            "UPDATE union_find SET person_id = $3 \
                             WHERE team_id = $1 AND current = $2",
                        )
                        .bind(team_id)
                        .bind(root_current)
                        .bind(src_person_id)
                        .execute(&mut *tx)
                        .await?;
                    }
                    None => {
                        return Err(DbError::Internal(format!(
                            "dest '{dest}' exists but chain has no root"
                        )));
                    }
                }
            }
        }
    }

    tx.commit().await?;

    Ok(MergeResponse {
        person_uuid: src_resolved.person_uuid,
    })
}
