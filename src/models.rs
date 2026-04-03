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

#[derive(Debug, Deserialize)]
pub struct ResolveRequest {
    pub team_id: i64,
    pub distinct_id: String,
}

#[derive(Debug, Serialize)]
pub struct ResolveResponse {
    pub person_uuid: String,
    pub is_identified: bool,
}

#[derive(Debug, Deserialize)]
pub struct DeletePersonRequest {
    pub team_id: i64,
    pub person_uuid: String,
}

#[derive(Debug, Serialize)]
pub struct DeletePersonResponse {
    pub person_uuid: String,
}

#[derive(Debug, Deserialize)]
pub struct DeleteDistinctIdRequest {
    pub team_id: i64,
    pub distinct_id: String,
}

#[derive(Debug, Serialize)]
pub struct DeleteDistinctIdResponse {
    pub distinct_id: String,
    pub person_deleted: bool,
}

#[derive(Debug, Deserialize)]
pub struct ResolveDistinctIdsRequest {
    pub team_id: i64,
    pub person_uuid: String,
}

#[derive(Debug, Serialize)]
pub struct ResolveDistinctIdsResponse {
    pub person_uuid: String,
    pub distinct_ids: Vec<String>,
    pub is_truncated: bool,
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
// Compression hint — returned by mutations to signal a chain needs compression.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CompressHint {
    pub distinct_id: String,
    pub depth: i32,
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
        reply: oneshot::Sender<DbResult<(AliasResponse, Option<CompressHint>)>>,
    },
    Merge {
        team_id: i64,
        target: String,
        sources: Vec<String>,
        reply: oneshot::Sender<DbResult<(MergeResponse, Option<CompressHint>)>>,
    },
    BatchedMerge {
        team_id: i64,
        target: String,
        sources: Vec<String>,
        reply: oneshot::Sender<DbResult<(MergeResponse, Option<CompressHint>)>>,
    },
    DeletePerson {
        team_id: i64,
        person_uuid: String,
        reply: oneshot::Sender<DbResult<DeletePersonResponse>>,
    },
    DeleteDistinctId {
        team_id: i64,
        distinct_id: String,
        reply: oneshot::Sender<DbResult<DeleteDistinctIdResponse>>,
    },
    CompressPath {
        team_id: i64,
        distinct_id: String,
        depth: i32,
        reply: Option<oneshot::Sender<DbResult<()>>>,
    },
}
