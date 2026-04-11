mod common;

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use axum_test::TestServer;
use serde_json::json;
use tokio::sync::mpsc;
use uuid::Uuid;

use common::{next_team_id, test_pool};
use pg_union_find_rs::handlers::AppState;
use pg_union_find_rs::{db, handlers};

async fn test_server() -> TestServer {
    let pool = test_pool().await;
    let mut senders = Vec::new();
    for _ in 0..2 {
        let (tx, rx) = mpsc::channel(64);
        let p = pool.clone();
        tokio::spawn(async move {
            db::worker_loop(p, rx, i32::MAX).await;
        });
        senders.push(tx);
    }
    let state = AppState {
        workers: Arc::from(senders.into_boxed_slice()),
        pool,
        compress_threshold: i32::MAX,
    };
    let app = Router::new()
        .route("/health", get(handlers::health))
        .route("/create", post(handlers::create))
        .route("/identify", post(handlers::identify))
        .route("/alias", post(handlers::alias))
        .route("/merge", post(handlers::merge))
        .route("/batched_merge", post(handlers::batched_merge))
        .route("/delete_person", post(handlers::delete_person))
        .route("/delete_distinct_id", post(handlers::delete_distinct_id))
        .route("/resolve", post(handlers::resolve))
        .route(
            "/resolve_distinct_ids",
            post(handlers::resolve_distinct_ids),
        )
        .with_state(state);
    TestServer::builder().expect_success_by_default().build(app)
}

fn assert_error_body(body: &serde_json::Value) {
    let err = body
        .get("error")
        .expect("response should have 'error' field");
    assert!(
        err.as_str().is_some_and(|s| !s.is_empty()),
        "error field should be a non-empty string, got: {err}"
    );
}

// ===========================================================================
// GET /health
// ===========================================================================

#[tokio::test]
async fn health_ok() {
    let server = test_server().await;
    let resp = server.get("/health").await;
    resp.assert_status_ok();
    resp.assert_text("ok");
}

// ===========================================================================
// POST /create
// ===========================================================================

#[tokio::test]
async fn create_success() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "user-1"}))
        .await;
    resp.assert_status_ok();
    let body: serde_json::Value = resp.json();
    assert!(body["person_uuid"].is_string());
    Uuid::parse_str(body["person_uuid"].as_str().unwrap()).expect("should be valid UUID");
    assert_eq!(body["is_identified"], false);
}

#[tokio::test]
async fn create_idempotent() {
    let server = test_server().await;
    let t = next_team_id();

    let r1: serde_json::Value = server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "idem-1"}))
        .await
        .json();
    let r2: serde_json::Value = server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "idem-1"}))
        .await
        .json();
    assert_eq!(r1["person_uuid"], r2["person_uuid"]);
}

#[tokio::test]
async fn create_illegal_distinct_id() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "null"}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn create_missing_field() {
    let server = test_server().await;

    let resp = server
        .post("/create")
        .json(&json!({"team_id": 1}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn create_wrong_type() {
    let server = test_server().await;

    let resp = server
        .post("/create")
        .json(&json!({"team_id": "not-a-number", "distinct_id": "x"}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn create_empty_body() {
    let server = test_server().await;

    let resp = server
        .post("/create")
        .json(&json!({}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn create_not_json() {
    let server = test_server().await;

    let resp = server
        .post("/create")
        .text("this is not json")
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

// ===========================================================================
// POST /identify
// ===========================================================================

#[tokio::test]
async fn identify_success() {
    let server = test_server().await;
    let t = next_team_id();

    server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "anon-1"}))
        .await;

    let resp = server
        .post("/identify")
        .json(&json!({"team_id": t, "target": "known-1", "anonymous": "anon-1"}))
        .await;
    resp.assert_status_ok();
    let body: serde_json::Value = resp.json();
    assert!(body["person_uuid"].is_string());
    assert_eq!(body["is_identified"], true);
}

#[tokio::test]
async fn identify_illegal_distinct_id() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/identify")
        .json(&json!({"team_id": t, "target": "null", "anonymous": "a"}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn identify_already_identified_source() {
    let server = test_server().await;
    let t = next_team_id();

    server
        .post("/identify")
        .json(&json!({"team_id": t, "target": "src", "anonymous": "src-anon"}))
        .await;

    server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "other"}))
        .await;

    let resp = server
        .post("/identify")
        .json(&json!({"team_id": t, "target": "other", "anonymous": "src"}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::CONFLICT);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn identify_malformed_json() {
    let server = test_server().await;

    let resp = server
        .post("/identify")
        .json(&json!({"team_id": 1}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

// ===========================================================================
// POST /alias
// ===========================================================================

#[tokio::test]
async fn alias_success() {
    let server = test_server().await;
    let t = next_team_id();

    server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "primary"}))
        .await;

    let resp = server
        .post("/alias")
        .json(&json!({"team_id": t, "target": "primary", "alias": "alt"}))
        .await;
    resp.assert_status_ok();
    let body: serde_json::Value = resp.json();
    assert!(body["person_uuid"].is_string());
    assert_eq!(body["is_identified"], true);
}

#[tokio::test]
async fn alias_illegal_distinct_id() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/alias")
        .json(&json!({"team_id": t, "target": "ok", "alias": "anonymous"}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn alias_already_identified_source() {
    let server = test_server().await;
    let t = next_team_id();

    server
        .post("/identify")
        .json(&json!({"team_id": t, "target": "src", "anonymous": "src-anon"}))
        .await;

    server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "other"}))
        .await;

    let resp = server
        .post("/alias")
        .json(&json!({"team_id": t, "target": "other", "alias": "src"}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::CONFLICT);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn alias_malformed_json() {
    let server = test_server().await;

    let resp = server
        .post("/alias")
        .json(&json!({"team_id": 1}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

// ===========================================================================
// POST /merge
// ===========================================================================

#[tokio::test]
async fn merge_success() {
    let server = test_server().await;
    let t = next_team_id();

    server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "tgt"}))
        .await;
    server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "src"}))
        .await;

    let resp = server
        .post("/merge")
        .json(&json!({"team_id": t, "target": "tgt", "sources": ["src"]}))
        .await;
    resp.assert_status_ok();
    let body: serde_json::Value = resp.json();
    assert!(body["person_uuid"].is_string());
    assert_eq!(body["is_identified"], true);
}

#[tokio::test]
async fn merge_target_not_found() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/merge")
        .json(&json!({"team_id": t, "target": "missing", "sources": ["a"]}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::NOT_FOUND);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn merge_illegal_distinct_id() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/merge")
        .json(&json!({"team_id": t, "target": "null", "sources": ["a"]}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn merge_malformed_json() {
    let server = test_server().await;

    let resp = server
        .post("/merge")
        .json(&json!({"team_id": 1}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

// ===========================================================================
// POST /batched_merge
// ===========================================================================

#[tokio::test]
async fn batched_merge_success() {
    let server = test_server().await;
    let t = next_team_id();

    server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "tgt"}))
        .await;
    server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "src"}))
        .await;

    let resp = server
        .post("/batched_merge")
        .json(&json!({"team_id": t, "target": "tgt", "sources": ["src"]}))
        .await;
    resp.assert_status_ok();
    let body: serde_json::Value = resp.json();
    assert!(body["person_uuid"].is_string());
    assert_eq!(body["is_identified"], true);
}

#[tokio::test]
async fn batched_merge_target_not_found() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/batched_merge")
        .json(&json!({"team_id": t, "target": "missing", "sources": ["a"]}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::NOT_FOUND);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn batched_merge_illegal_distinct_id() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/batched_merge")
        .json(&json!({"team_id": t, "target": "null", "sources": ["a"]}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::BAD_REQUEST);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn batched_merge_malformed_json() {
    let server = test_server().await;

    let resp = server
        .post("/batched_merge")
        .json(&json!({"team_id": 1}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

// ===========================================================================
// POST /resolve
// ===========================================================================

#[tokio::test]
async fn resolve_success() {
    let server = test_server().await;
    let t = next_team_id();

    server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "r-1"}))
        .await;

    let resp = server
        .post("/resolve")
        .json(&json!({"team_id": t, "distinct_id": "r-1"}))
        .await;
    resp.assert_status_ok();
    let body: serde_json::Value = resp.json();
    assert!(body["person_uuid"].is_string());
    Uuid::parse_str(body["person_uuid"].as_str().unwrap()).expect("should be valid UUID");
    assert!(body["is_identified"].is_boolean());
}

#[tokio::test]
async fn resolve_not_found() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/resolve")
        .json(&json!({"team_id": t, "distinct_id": "no-such-did"}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::NOT_FOUND);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn resolve_malformed_json() {
    let server = test_server().await;

    let resp = server
        .post("/resolve")
        .json(&json!({"team_id": 1}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

// ===========================================================================
// POST /delete_person
// ===========================================================================

#[tokio::test]
async fn delete_person_success() {
    let server = test_server().await;
    let t = next_team_id();

    let created: serde_json::Value = server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "dp-1"}))
        .await
        .json();
    let puuid = created["person_uuid"].as_str().unwrap();

    let resp = server
        .post("/delete_person")
        .json(&json!({"team_id": t, "person_uuid": puuid}))
        .await;
    resp.assert_status_ok();
    let body: serde_json::Value = resp.json();
    assert_eq!(body["person_uuid"].as_str().unwrap(), puuid);
}

#[tokio::test]
async fn delete_person_not_found() {
    let server = test_server().await;
    let t = next_team_id();
    let fake = Uuid::nil().to_string();

    let resp = server
        .post("/delete_person")
        .json(&json!({"team_id": t, "person_uuid": fake}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::NOT_FOUND);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn delete_person_bad_uuid() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/delete_person")
        .json(&json!({"team_id": t, "person_uuid": "not-a-uuid"}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn delete_person_malformed_json() {
    let server = test_server().await;

    let resp = server
        .post("/delete_person")
        .json(&json!({"team_id": 1}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

// ===========================================================================
// POST /delete_distinct_id
// ===========================================================================

#[tokio::test]
async fn delete_did_success() {
    let server = test_server().await;
    let t = next_team_id();

    server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "dd-1"}))
        .await;

    let resp = server
        .post("/delete_distinct_id")
        .json(&json!({"team_id": t, "distinct_id": "dd-1"}))
        .await;
    resp.assert_status_ok();
    let body: serde_json::Value = resp.json();
    assert_eq!(body["distinct_id"].as_str().unwrap(), "dd-1");
    assert!(body["person_deleted"].is_boolean());
}

#[tokio::test]
async fn delete_did_not_found() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/delete_distinct_id")
        .json(&json!({"team_id": t, "distinct_id": "no-such"}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::NOT_FOUND);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn delete_did_malformed_json() {
    let server = test_server().await;

    let resp = server
        .post("/delete_distinct_id")
        .json(&json!({"team_id": 1}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

// ===========================================================================
// POST /resolve_distinct_ids
// ===========================================================================

#[tokio::test]
async fn resolve_dids_success() {
    let server = test_server().await;
    let t = next_team_id();

    let created: serde_json::Value = server
        .post("/create")
        .json(&json!({"team_id": t, "distinct_id": "rd-1"}))
        .await
        .json();
    let puuid = created["person_uuid"].as_str().unwrap();

    let resp = server
        .post("/resolve_distinct_ids")
        .json(&json!({"team_id": t, "person_uuid": puuid}))
        .await;
    resp.assert_status_ok();
    let body: serde_json::Value = resp.json();
    assert_eq!(body["person_uuid"].as_str().unwrap(), puuid);
    assert!(body["distinct_ids"].is_array());
    let dids = body["distinct_ids"].as_array().unwrap();
    assert!(dids.iter().any(|d| d.as_str() == Some("rd-1")));
    assert_eq!(body["is_truncated"], false);
}

#[tokio::test]
async fn resolve_dids_not_found() {
    let server = test_server().await;
    let t = next_team_id();
    let fake = Uuid::nil().to_string();

    let resp = server
        .post("/resolve_distinct_ids")
        .json(&json!({"team_id": t, "person_uuid": fake}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::NOT_FOUND);
    assert_error_body(&resp.json());
}

#[tokio::test]
async fn resolve_dids_bad_uuid() {
    let server = test_server().await;
    let t = next_team_id();

    let resp = server
        .post("/resolve_distinct_ids")
        .json(&json!({"team_id": t, "person_uuid": "garbage"}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn resolve_dids_malformed_json() {
    let server = test_server().await;

    let resp = server
        .post("/resolve_distinct_ids")
        .json(&json!({"team_id": 1}))
        .expect_failure()
        .await;
    resp.assert_status(axum::http::StatusCode::UNPROCESSABLE_ENTITY);
}
