use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct CreateRequest {
    pub team_id: i64,
    pub distinct_id: String,
}

#[derive(Debug, Serialize)]
pub struct CreateResponse {
    pub person_uuid: String,
    pub is_identified: bool,
}

#[derive(Debug, Deserialize)]
pub struct IdentifyRequest {
    pub team_id: i64,
    pub target: String,
    pub anonymous: String,
}

#[derive(Debug, Deserialize)]
pub struct AliasRequest {
    pub team_id: i64,
    pub target: String,
    pub alias: String,
}

#[derive(Debug, Serialize)]
pub struct AliasResponse {
    pub person_uuid: String,
    pub is_identified: bool,
}

#[derive(Debug, Deserialize)]
pub struct MergeRequest {
    pub team_id: i64,
    pub target: String,
    pub sources: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct MergeResponse {
    pub person_uuid: String,
    pub is_identified: bool,
}

// ---------------------------------------------------------------------------
// Internal error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum DbError {
    NotFound(String),
    AlreadyIdentified(String),
    IllegalDistinctId(String),
    Internal(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::NotFound(msg) => write!(f, "not found: {msg}"),
            DbError::AlreadyIdentified(msg) => write!(f, "already identified: {msg}"),
            DbError::IllegalDistinctId(msg) => write!(f, "illegal distinct_id: {msg}"),
            DbError::Internal(msg) => write!(f, "internal: {msg}"),
        }
    }
}

impl From<sqlx::Error> for DbError {
    fn from(e: sqlx::Error) -> Self {
        DbError::Internal(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Channel protocol — the single-threaded worker processes these sequentially.
// ---------------------------------------------------------------------------

pub type DbResult<T> = Result<T, DbError>;

pub enum DbOp {
    Create {
        team_id: i64,
        distinct_id: String,
        reply: oneshot::Sender<DbResult<CreateResponse>>,
    },
    Alias {
        team_id: i64,
        target: String,
        source: String,
        reply: oneshot::Sender<DbResult<AliasResponse>>,
    },
    Merge {
        team_id: i64,
        target: String,
        sources: Vec<String>,
        reply: oneshot::Sender<DbResult<MergeResponse>>,
    },
}
