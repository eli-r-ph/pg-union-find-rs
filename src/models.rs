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
    pub person_id: String,
}

/// Terminology mapping to PostHog ingestion:
///   `known`   = PostHog "target" (`mergeIntoDistinctId`) — the distinct_id whose person survives
///   `unknown` = PostHog "source" (`otherPersonDistinctId`) — the distinct_id whose person is absorbed
#[derive(Debug, Deserialize)]
pub struct CreateAliasRequest {
    pub team_id: i64,
    pub known: String,
    pub unknown: String,
}

#[derive(Debug, Serialize)]
pub struct CreateAliasResponse {
    pub person_id: String,
    /// When true, the merge was refused because the unknown/source person is already identified.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub refused: bool,
}

#[derive(Debug, Deserialize)]
pub struct MergeRequest {
    pub team_id: i64,
    pub primary: String,
    pub others: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct MergeResponse {
    pub person_id: String,
}

// ---------------------------------------------------------------------------
// Internal error type
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum DbError {
    NotFound(String),
    Internal(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::NotFound(msg) => write!(f, "not found: {msg}"),
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
// Channel protocol — the single‐threaded worker processes these sequentially.
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
        /// PostHog "target" (mergeIntoDistinctId) — person that survives.
        known: String,
        /// PostHog "source" (otherPersonDistinctId) — person that is absorbed.
        unknown: String,
        reply: oneshot::Sender<DbResult<CreateAliasResponse>>,
    },
    Merge {
        team_id: i64,
        primary: String,
        others: Vec<String>,
        reply: oneshot::Sender<DbResult<MergeResponse>>,
    },
}
