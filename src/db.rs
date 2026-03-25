use sqlx::PgPool;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::models::{
    CreateAliasResponse, DbError, DbOp, DbResult, IdentifyResponse, MergeResponse,
};

// ---------------------------------------------------------------------------
// Resolved person — returned by the recursive CTE
// ---------------------------------------------------------------------------

struct ResolvedPerson {
    person_id: String,
    internal_id: i64,
    is_identified: bool,
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
                known,
                unknown,
                reply,
            } => {
                let _ = reply.send(handle_create_alias(&pool, team_id, &known, &unknown).await);
            }
            DbOp::Merge {
                team_id,
                primary,
                others,
                reply,
            } => {
                let _ = reply.send(handle_merge(&pool, team_id, &primary, &others).await);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Resolve: recursive CTE following the person_overrides chain.
// Returns None when the distinct_id is unknown to the system.
// ---------------------------------------------------------------------------

async fn resolve(
    pool: &PgPool,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<Option<ResolvedPerson>> {
    let row = sqlx::query_as::<_, (String, i64, bool)>(
        r#"
        WITH RECURSIVE resolve(pid, depth) AS (
            SELECT d.person_id, 0
            FROM distinct_ids d
            WHERE d.team_id = $1 AND d.distinct_id = $2

            UNION ALL

            SELECT o.override_person_id, r.depth + 1
            FROM resolve r
            JOIN person_overrides o
              ON o.team_id = $1 AND o.old_person_id = r.pid
        )
        SELECT p.person_id, r.pid AS internal_id, p.is_identified
        FROM resolve r
        JOIN persons p ON p.id = r.pid
        ORDER BY r.depth DESC
        LIMIT 1
        "#,
    )
    .bind(team_id)
    .bind(distinct_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(person_id, internal_id, is_identified)| ResolvedPerson {
        person_id,
        internal_id,
        is_identified,
    }))
}

// ---------------------------------------------------------------------------
// /identify — get-or-create a person for a single distinct_id.
// ---------------------------------------------------------------------------

async fn handle_identify(
    pool: &PgPool,
    team_id: i64,
    distinct_id: &str,
) -> DbResult<IdentifyResponse> {
    if let Some(resolved) = resolve(pool, team_id, distinct_id).await? {
        return Ok(IdentifyResponse {
            person_id: resolved.person_id,
        });
    }

    let person_id_str = Uuid::new_v4().to_string();

    let internal_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO persons (team_id, person_id, is_identified)
        VALUES ($1, $2, false)
        ON CONFLICT (team_id, person_id) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(team_id)
    .bind(&person_id_str)
    .fetch_optional(pool)
    .await?
    .unwrap_or_else(|| {
        // Conflict on the generated UUID is astronomically unlikely, but handle it.
        panic!("UUID collision for person_id {person_id_str}");
    });

    sqlx::query(
        r#"
        INSERT INTO distinct_ids (team_id, distinct_id, person_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (team_id, distinct_id) DO NOTHING
        "#,
    )
    .bind(team_id)
    .bind(distinct_id)
    .bind(internal_id)
    .execute(pool)
    .await?;

    Ok(IdentifyResponse {
        person_id: person_id_str,
    })
}

// ---------------------------------------------------------------------------
// /create_alias — merge two distinct_ids, respecting is_identified.
//
// Replicates PostHog $identify / $create_alias behaviour:
//   known  = PostHog "target" (mergeIntoDistinctId)  — person that survives
//   unknown = PostHog "source" (otherPersonDistinctId) — person that is absorbed
//
// The is_identified check gates on the *unknown/source* person.
// ---------------------------------------------------------------------------

async fn handle_create_alias(
    pool: &PgPool,
    team_id: i64,
    known: &str,
    unknown: &str,
) -> DbResult<CreateAliasResponse> {
    // Resolve the known (target) distinct_id — must already exist.
    let known_person = resolve(pool, team_id, known)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("known distinct_id '{known}' not found")))?;

    // Resolve the unknown (source) distinct_id — may not exist.
    let unknown_person = resolve(pool, team_id, unknown).await?;

    match unknown_person {
        None => {
            // Unknown doesn't exist yet — just associate it with the known person.
            sqlx::query(
                "INSERT INTO distinct_ids (team_id, distinct_id, person_id)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (team_id, distinct_id) DO NOTHING",
            )
            .bind(team_id)
            .bind(unknown)
            .bind(known_person.internal_id)
            .execute(pool)
            .await?;

            mark_identified(pool, known_person.internal_id).await?;

            Ok(CreateAliasResponse {
                person_id: known_person.person_id,
                refused: false,
            })
        }
        Some(unk) if unk.internal_id == known_person.internal_id => {
            // Already the same person — no-op, just mark identified.
            mark_identified(pool, known_person.internal_id).await?;

            Ok(CreateAliasResponse {
                person_id: known_person.person_id,
                refused: false,
            })
        }
        Some(unk) if unk.is_identified => {
            // Source person is already identified — refuse merge.
            // Matches PostHog's isMergeAllowed: $create_alias / $identify won't
            // merge a person that is already identified.
            Ok(CreateAliasResponse {
                person_id: known_person.person_id,
                refused: true,
            })
        }
        Some(unk) => {
            // Different, unidentified source person — insert override.
            sqlx::query(
                "INSERT INTO person_overrides (team_id, old_person_id, override_person_id)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (team_id, old_person_id) DO NOTHING",
            )
            .bind(team_id)
            .bind(unk.internal_id)
            .bind(known_person.internal_id)
            .execute(pool)
            .await?;

            mark_identified(pool, known_person.internal_id).await?;

            Ok(CreateAliasResponse {
                person_id: known_person.person_id,
                refused: false,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// /merge — merge N distinct_ids into a primary, ignoring is_identified.
//
// Replicates PostHog $merge_dangerously behaviour.
// ---------------------------------------------------------------------------

async fn handle_merge(
    pool: &PgPool,
    team_id: i64,
    primary: &str,
    others: &[String],
) -> DbResult<MergeResponse> {
    let primary_person = resolve(pool, team_id, primary)
        .await?
        .ok_or_else(|| DbError::NotFound(format!("primary distinct_id '{primary}' not found")))?;

    for other in others {
        let other_person = resolve(pool, team_id, other).await?;

        match other_person {
            None => {
                // Doesn't exist — associate directly with the primary person.
                sqlx::query(
                    "INSERT INTO distinct_ids (team_id, distinct_id, person_id)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (team_id, distinct_id) DO NOTHING",
                )
                .bind(team_id)
                .bind(other.as_str())
                .bind(primary_person.internal_id)
                .execute(pool)
                .await?;
            }
            Some(op) if op.internal_id == primary_person.internal_id => {
                // Already the same person — nothing to do.
            }
            Some(op) => {
                // Different person — override it into the primary. No is_identified check.
                sqlx::query(
                    "INSERT INTO person_overrides (team_id, old_person_id, override_person_id)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (team_id, old_person_id) DO NOTHING",
                )
                .bind(team_id)
                .bind(op.internal_id)
                .bind(primary_person.internal_id)
                .execute(pool)
                .await?;
            }
        }
    }

    mark_identified(pool, primary_person.internal_id).await?;

    Ok(MergeResponse {
        person_id: primary_person.person_id,
    })
}

// ---------------------------------------------------------------------------
// Helper: mark a person as identified.
// ---------------------------------------------------------------------------

async fn mark_identified(pool: &PgPool, internal_id: i64) -> DbResult<()> {
    sqlx::query("UPDATE persons SET is_identified = true WHERE id = $1 AND NOT is_identified")
        .bind(internal_id)
        .execute(pool)
        .await?;
    Ok(())
}
