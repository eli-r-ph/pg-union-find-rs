use std::sync::Arc;
use std::time::Duration;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use tokio::sync::{mpsc, oneshot};

use sqlx::PgPool;

use crate::db;
use crate::models::{
    AliasRequest, CompressHint, CreateRequest, DbError, DbOp, DeleteDistinctIdRequest,
    DeletePersonRequest, IdentifyRequest, MergeRequest, ResolveRequest, ResolveResponse,
};

const ENQUEUE_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct AppState {
    pub workers: Arc<[mpsc::Sender<DbOp>]>,
    pub pool: PgPool,
    pub compress_threshold: i32,
}

impl AppState {
    fn sender_for(&self, team_id: i64) -> &mpsc::Sender<DbOp> {
        let idx = (team_id as u64 as usize) % self.workers.len();
        &self.workers[idx]
    }
}

async fn enqueue_op(sender: &mpsc::Sender<DbOp>, op: DbOp) -> Result<(), axum::response::Response> {
    match tokio::time::timeout(ENQUEUE_TIMEOUT, sender.send(op)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "worker unavailable"})),
        )
            .into_response()),
        Err(_) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "queue full"})),
        )
            .into_response()),
    }
}

fn enqueue_compress(state: &AppState, team_id: i64, hint: CompressHint) {
    let sender = state.sender_for(team_id).clone();
    tokio::spawn(async move {
        for attempt in 0u64..3 {
            let (reply_tx, _reply_rx) = oneshot::channel();
            let op = DbOp::CompressPath {
                team_id,
                distinct_id: hint.distinct_id.clone(),
                depth: hint.depth,
                reply: reply_tx,
            };
            match tokio::time::timeout(ENQUEUE_TIMEOUT, sender.send(op)).await {
                Ok(Ok(())) => return,
                Ok(Err(_)) => {
                    tracing::error!(
                        team_id,
                        distinct_id = %hint.distinct_id,
                        depth = hint.depth,
                        "compress enqueue failed: worker closed"
                    );
                    return;
                }
                Err(_) => {
                    if attempt < 2 {
                        tokio::time::sleep(Duration::from_millis(50 * (attempt + 1))).await;
                    } else {
                        tracing::error!(
                            team_id,
                            distinct_id = %hint.distinct_id,
                            depth = hint.depth,
                            "compress enqueue failed after 3 attempts"
                        );
                    }
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// POST /create
// ---------------------------------------------------------------------------

pub async fn create(
    State(state): State<AppState>,
    Json(req): Json<CreateRequest>,
) -> impl IntoResponse {
    let (reply_tx, reply_rx) = oneshot::channel();

    let team_id = req.team_id;
    let op = DbOp::Create {
        team_id,
        distinct_id: req.distinct_id,
        reply: reply_tx,
    };

    if let Err(resp) = enqueue_op(state.sender_for(team_id), op).await {
        return resp;
    }

    match reply_rx.await {
        Ok(Ok(resp)) => (StatusCode::OK, Json(serde_json::json!(resp))).into_response(),
        Ok(Err(e)) => db_error_response(e),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "worker dropped reply"})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST /identify
// ---------------------------------------------------------------------------

pub async fn identify(
    State(state): State<AppState>,
    Json(req): Json<IdentifyRequest>,
) -> impl IntoResponse {
    let (reply_tx, reply_rx) = oneshot::channel();

    let team_id = req.team_id;
    let op = DbOp::Alias {
        team_id,
        target: req.target,
        source: req.anonymous,
        reply: reply_tx,
    };

    if let Err(resp) = enqueue_op(state.sender_for(team_id), op).await {
        return resp;
    }

    match reply_rx.await {
        Ok(Ok((resp, hint))) => {
            if let Some(hint) = hint {
                enqueue_compress(&state, team_id, hint);
            }
            (StatusCode::OK, Json(serde_json::json!(resp))).into_response()
        }
        Ok(Err(e)) => db_error_response(e),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "worker dropped reply"})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST /alias
// ---------------------------------------------------------------------------

pub async fn alias(
    State(state): State<AppState>,
    Json(req): Json<AliasRequest>,
) -> impl IntoResponse {
    let (reply_tx, reply_rx) = oneshot::channel();

    let team_id = req.team_id;
    let op = DbOp::Alias {
        team_id,
        target: req.target,
        source: req.alias,
        reply: reply_tx,
    };

    if let Err(resp) = enqueue_op(state.sender_for(team_id), op).await {
        return resp;
    }

    match reply_rx.await {
        Ok(Ok((resp, hint))) => {
            if let Some(hint) = hint {
                enqueue_compress(&state, team_id, hint);
            }
            (StatusCode::OK, Json(serde_json::json!(resp))).into_response()
        }
        Ok(Err(e)) => db_error_response(e),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "worker dropped reply"})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST /merge
// ---------------------------------------------------------------------------

pub async fn merge(
    State(state): State<AppState>,
    Json(req): Json<MergeRequest>,
) -> impl IntoResponse {
    let (reply_tx, reply_rx) = oneshot::channel();

    let team_id = req.team_id;
    let op = DbOp::Merge {
        team_id,
        target: req.target,
        sources: req.sources,
        reply: reply_tx,
    };

    if let Err(resp) = enqueue_op(state.sender_for(team_id), op).await {
        return resp;
    }

    match reply_rx.await {
        Ok(Ok((resp, hint))) => {
            if let Some(hint) = hint {
                enqueue_compress(&state, team_id, hint);
            }
            (StatusCode::OK, Json(serde_json::json!(resp))).into_response()
        }
        Ok(Err(e)) => db_error_response(e),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "worker dropped reply"})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// GET /health — lightweight check (bypasses the worker channel)
// ---------------------------------------------------------------------------

pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    if state.workers.iter().any(|tx| tx.is_closed()) {
        return (StatusCode::SERVICE_UNAVAILABLE, "worker stopped");
    }
    (StatusCode::OK, "ok")
}

// ---------------------------------------------------------------------------
// POST /delete_person
// ---------------------------------------------------------------------------

pub async fn delete_person(
    State(state): State<AppState>,
    Json(req): Json<DeletePersonRequest>,
) -> impl IntoResponse {
    let (reply_tx, reply_rx) = oneshot::channel();

    let team_id = req.team_id;
    let op = DbOp::DeletePerson {
        team_id,
        person_uuid: req.person_uuid,
        reply: reply_tx,
    };

    if let Err(resp) = enqueue_op(state.sender_for(team_id), op).await {
        return resp;
    }

    match reply_rx.await {
        Ok(Ok(resp)) => (StatusCode::OK, Json(serde_json::json!(resp))).into_response(),
        Ok(Err(e)) => db_error_response(e),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "worker dropped reply"})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST /delete_distinct_id
// ---------------------------------------------------------------------------

pub async fn delete_distinct_id(
    State(state): State<AppState>,
    Json(req): Json<DeleteDistinctIdRequest>,
) -> impl IntoResponse {
    let (reply_tx, reply_rx) = oneshot::channel();

    let team_id = req.team_id;
    let op = DbOp::DeleteDistinctId {
        team_id,
        distinct_id: req.distinct_id,
        reply: reply_tx,
    };

    if let Err(resp) = enqueue_op(state.sender_for(team_id), op).await {
        return resp;
    }

    match reply_rx.await {
        Ok(Ok(resp)) => (StatusCode::OK, Json(serde_json::json!(resp))).into_response(),
        Ok(Err(e)) => db_error_response(e),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "worker dropped reply"})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST /resolve — read-only lookup, bypasses worker channels
// ---------------------------------------------------------------------------

pub async fn resolve(
    State(state): State<AppState>,
    Json(req): Json<ResolveRequest>,
) -> impl IntoResponse {
    match db::resolve(&state.pool, req.team_id, &req.distinct_id).await {
        Ok(Some((person, depth))) => {
            if depth > state.compress_threshold {
                let (reply_tx, _) = oneshot::channel();
                if state
                    .sender_for(req.team_id)
                    .try_send(DbOp::CompressPath {
                        team_id: req.team_id,
                        distinct_id: req.distinct_id.clone(),
                        depth,
                        reply: reply_tx,
                    })
                    .is_err()
                {
                    tracing::error!(
                        team_id = req.team_id,
                        distinct_id = %req.distinct_id,
                        depth,
                        "compress enqueue failed: channel full"
                    );
                }
            }
            (
                StatusCode::OK,
                Json(serde_json::json!(ResolveResponse {
                    person_uuid: person.person_uuid,
                    is_identified: person.is_identified,
                })),
            )
                .into_response()
        }
        Ok(None) => db_error_response(DbError::NotFound(format!(
            "distinct_id '{}' not found",
            req.distinct_id
        ))),
        Err(e) => db_error_response(e),
    }
}

// ---------------------------------------------------------------------------
// Map DbError -> HTTP response
// ---------------------------------------------------------------------------

fn db_error_response(e: DbError) -> axum::response::Response {
    let (status, msg) = match &e {
        DbError::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
        DbError::AlreadyIdentified(m) => (StatusCode::CONFLICT, m.clone()),
        DbError::IllegalDistinctId(m) => (StatusCode::BAD_REQUEST, m.clone()),
        DbError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
    };
    (status, Json(serde_json::json!({"error": msg}))).into_response()
}
