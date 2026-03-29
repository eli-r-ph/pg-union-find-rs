use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use tokio::sync::{mpsc, oneshot};

use sqlx::PgPool;

use crate::db;
use crate::models::{
    AliasRequest, CreateRequest, DbError, DbOp, DeleteDistinctIdRequest, DeletePersonRequest,
    IdentifyRequest, MergeRequest, ResolveRequest, ResolveResponse,
};

#[derive(Clone)]
pub struct AppState {
    pub workers: Arc<[mpsc::Sender<DbOp>]>,
    pub pool: PgPool,
}

impl AppState {
    fn sender_for(&self, team_id: i64) -> &mpsc::Sender<DbOp> {
        let idx = (team_id as u64 as usize) % self.workers.len();
        &self.workers[idx]
    }
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

    if state.sender_for(team_id).send(op).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "worker unavailable"})),
        )
            .into_response();
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

    if state.sender_for(team_id).send(op).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "worker unavailable"})),
        )
            .into_response();
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

    if state.sender_for(team_id).send(op).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "worker unavailable"})),
        )
            .into_response();
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

    if state.sender_for(team_id).send(op).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "worker unavailable"})),
        )
            .into_response();
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

    if state.sender_for(team_id).send(op).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "worker unavailable"})),
        )
            .into_response();
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

    if state.sender_for(team_id).send(op).await.is_err() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "worker unavailable"})),
        )
            .into_response();
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
        Ok(Some(person)) => (
            StatusCode::OK,
            Json(serde_json::json!(ResolveResponse {
                person_uuid: person.person_uuid,
                is_identified: person.is_identified,
            })),
        )
            .into_response(),
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
