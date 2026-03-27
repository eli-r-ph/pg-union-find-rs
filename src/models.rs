use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct IdentifyRequest {
    pub team_id: i64,
    pub distinct_id: String,
}

#[derive(Debug, Serialize)]
pub struct IdentifyResponse {
    pub person_uuid: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateAliasRequest {
    pub team_id: i64,
    pub src: String,
    pub dest: String,
}

#[derive(Debug, Serialize)]
pub struct CreateAliasResponse {
    pub person_uuid: String,
}

#[derive(Debug, Deserialize)]
pub struct MergeRequest {
    pub team_id: i64,
    pub src: String,
    pub dests: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct MergeResponse {
    pub person_uuid: String,
}

// ---------------------------------------------------------------------------
// Internal error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum DbError {
    NotFound(String),
    AlreadyIdentified(String),
    Internal(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::NotFound(msg) => write!(f, "not found: {msg}"),
            DbError::AlreadyIdentified(msg) => write!(f, "already identified: {msg}"),
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
    Identify {
        team_id: i64,
        distinct_id: String,
        reply: oneshot::Sender<DbResult<IdentifyResponse>>,
    },
    CreateAlias {
        team_id: i64,
        src: String,
        dest: String,
        reply: oneshot::Sender<DbResult<CreateAliasResponse>>,
    },
    Merge {
        team_id: i64,
        src: String,
        dests: Vec<String>,
        reply: oneshot::Sender<DbResult<MergeResponse>>,
    },
}
