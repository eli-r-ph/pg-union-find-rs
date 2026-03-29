use sqlx::PgPool;
use sqlx::postgres::PgConnection;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::models::{
    AliasResponse, CreateResponse, DbError, DbOp, DbResult, DeleteDistinctIdResponse,
    DeletePersonResponse, MergeResponse,
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
          AND pm.deleted_at IS NULL
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
          AND uf.deleted_at IS NULL
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

// ---------------------------------------------------------------------------
// Lazy-unlink primitives — used by all write paths to handle distinct_ids
// that belong to soft-deleted persons.
// ---------------------------------------------------------------------------

/// Three-state lookup result for a distinct_id.
enum DidState {
    NotFound,
    Orphaned(i64),
    Live(i64, ResolvedPerson),
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
        Some(person) => Ok(DidState::Live(did_pk, person)),
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
        DidState::Live(_pk, resolved) => Ok(resolved),

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
pub async fn handle_alias(
    pool: &PgPool,
    team_id: i64,
    target: &str,
    source: &str,
) -> DbResult<AliasResponse> {
    validate_distinct_id(target)?;
    validate_distinct_id(source)?;

    if target == source {
        let mut tx = pool.begin().await?;
        let resolved = identify_tx(&mut tx, team_id, target).await?;
        set_identified(&mut tx, resolved.person_id).await?;
        tx.commit().await?;
        return Ok(AliasResponse {
            person_uuid: resolved.person_uuid,
            is_identified: true,
        });
    }

    let mut tx = pool.begin().await?;

    let target_state = check_did(&mut tx, team_id, target).await?;
    let source_state = check_did(&mut tx, team_id, source).await?;

    let person_uuid = match (target_state, source_state) {
        // Both live — compare persons, merge if different
        (DidState::Live(tpk, t_resolved), DidState::Live(spk, s_resolved)) => {
            if t_resolved.person_id == s_resolved.person_id {
                set_identified(&mut tx, t_resolved.person_id).await?;
                t_resolved.person_uuid
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
                t_resolved.person_uuid
            }
        }

        // Target live, source not — attach source into target's chain
        (DidState::Live(tpk, t_resolved), source_state) => {
            attach_did(&mut tx, team_id, source, source_state, tpk).await?;
            set_identified(&mut tx, t_resolved.person_id).await?;
            t_resolved.person_uuid
        }

        // Source live, target not — attach target into source's chain
        (target_state, DidState::Live(spk, s_resolved)) => {
            attach_did(&mut tx, team_id, target, target_state, spk).await?;
            set_identified(&mut tx, s_resolved.person_id).await?;
            s_resolved.person_uuid
        }

        // Neither live — create new person with both distinct_ids
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
/// Uses check_did for the target and each source so that distinct_ids belonging
/// to deleted persons are lazily unlinked and handled transparently.
pub async fn handle_merge(
    pool: &PgPool,
    team_id: i64,
    target: &str,
    sources: &[String],
) -> DbResult<MergeResponse> {
    validate_distinct_id(target)?;
    for src in sources {
        validate_distinct_id(src)?;
    }

    let mut tx = pool.begin().await?;

    let (target_pk, target_person_id, target_person_uuid) =
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
                (pk, person_id, person_uuid)
            }
            DidState::Live(pk, resolved) => (pk, resolved.person_id, resolved.person_uuid),
        };

    for src in sources {
        match check_did(&mut tx, team_id, src).await? {
            DidState::NotFound => {
                insert_did_and_link(&mut tx, team_id, src, target_pk).await?;
            }
            DidState::Orphaned(pk) => {
                link_did(&mut tx, team_id, pk, target_pk).await?;
            }
            DidState::Live(spk, _) => {
                let root = resolve_root(&mut tx, team_id, spk).await?;
                match root {
                    Some((_, root_person)) if root_person == target_person_id => {}
                    Some((root_current, old_person)) => {
                        link_root_to_target(&mut tx, team_id, root_current, old_person, target_pk)
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
        person_uuid: target_person_uuid,
        is_identified: true,
    })
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
