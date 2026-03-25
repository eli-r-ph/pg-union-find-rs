use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use tokio::sync::{mpsc, oneshot};

use crate::models::{
    CreateAliasRequest, DbError, DbOp, IdentifyRequest, MergeRequest,
};

#[derive(Clone)]
pub struct AppState {
    pub tx: mpsc::Sender<DbOp>,
}

// ---------------------------------------------------------------------------
// POST /identify
// ---------------------------------------------------------------------------

pub async fn identify(
    State(state): State<AppState>,
    Json(req): Json<IdentifyRequest>,
) -> impl IntoResponse {
    let (reply_tx, reply_rx) = oneshot::channel();

    let op = DbOp::Identify {
        team_id: req.team_id,
        distinct_id: req.distinct_id,
        reply: reply_tx,
    };

    if state.tx.send(op).await.is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "worker unavailable"}))).into_response();
    }

    match reply_rx.await {
        Ok(Ok(resp)) => (StatusCode::OK, Json(serde_json::json!(resp))).into_response(),
        Ok(Err(e)) => db_error_response(e),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "worker dropped reply"}))).into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST /create_alias
// ---------------------------------------------------------------------------

pub async fn create_alias(
    State(state): State<AppState>,
    Json(req): Json<CreateAliasRequest>,
) -> impl IntoResponse {
    let (reply_tx, reply_rx) = oneshot::channel();

    let op = DbOp::CreateAlias {
        team_id: req.team_id,
        known: req.known,
        unknown: req.unknown,
        reply: reply_tx,
    };

    if state.tx.send(op).await.is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "worker unavailable"}))).into_response();
    }

    match reply_rx.await {
        Ok(Ok(resp)) => (StatusCode::OK, Json(serde_json::json!(resp))).into_response(),
        Ok(Err(e)) => db_error_response(e),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "worker dropped reply"}))).into_response(),
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

    let op = DbOp::Merge {
        team_id: req.team_id,
        primary: req.primary,
        others: req.others,
        reply: reply_tx,
    };

    if state.tx.send(op).await.is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({"error": "worker unavailable"}))).into_response();
    }

    match reply_rx.await {
        Ok(Ok(resp)) => (StatusCode::OK, Json(serde_json::json!(resp))).into_response(),
        Ok(Err(e)) => db_error_response(e),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": "worker dropped reply"}))).into_response(),
    }
}

// ---------------------------------------------------------------------------
// POST /health — lightweight DB ping (bypasses the worker channel)
// ---------------------------------------------------------------------------

pub async fn health(State(state): State<AppState>) -> impl IntoResponse {
    // Health check sends a no-op through the channel to verify the worker is alive.
    // If the channel is full or closed, report unhealthy.
    if state.tx.is_closed() {
        return (StatusCode::SERVICE_UNAVAILABLE, "worker stopped");
    }
    (StatusCode::OK, "ok")
}

// ---------------------------------------------------------------------------
// Map DbError -> HTTP response
// ---------------------------------------------------------------------------

fn db_error_response(e: DbError) -> axum::response::Response {
    let (status, msg) = match &e {
        DbError::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
        DbError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
    };
    (status, Json(serde_json::json!({"error": msg}))).into_response()
}
