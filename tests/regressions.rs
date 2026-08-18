//! Regression tests for defects found in the 2026-08 correctness review,
//! plus coverage for previously untested topologies. Each section names the
//! finding it locks down.

mod common;

use common::*;
use pg_union_find_rs::db;
use pg_union_find_rs::models::DbError;

// ===========================================================================
// Finding 1 — root promotion must not trip the unique person_id index when
// the promoted parent's PK is smaller than the old root's PK. All prior tests
// happened to create roots before their parents, hiding the scan-order bug.
// ===========================================================================

#[tokio::test]
async fn delete_did_root_with_older_parent() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Parent "b" is created BEFORE root "a", so b's PK < a's PK.
    db::handle_create(&pool, t, "b").await.unwrap();
    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    handle_merge(&pool, t, "a", &["b".into()]).await.unwrap(); // b -> a (root)
    assert_chain_matches(&pool, t, "b", &["b", "a"], pa.person_uuid).await;

    let resp = db::handle_delete_distinct_id(&pool, t, "a")
        .await
        .expect("deleting a root whose promoted parent has a smaller PK must succeed");
    assert!(!resp.person_deleted);

    assert!(resolve(&pool, t, "a").await.unwrap().is_none());
    assert_chain_is_root(&pool, t, "b", pa.person_uuid).await;

    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_root_with_multiple_older_parents() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    handle_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap(); // b -> a, c -> a

    let resp = db::handle_delete_distinct_id(&pool, t, "a")
        .await
        .expect("root deletion with multiple older parents must succeed");
    assert!(!resp.person_deleted);

    // One of b/c promoted to root, the other re-pointed to it.
    for did in &["b", "c"] {
        let r = resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(r.person_uuid, pa.person_uuid);
    }
    assert_eq!(count_union_find(&pool, t).await, 2);
    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn lazy_unlink_dead_root_with_older_parent_via_create() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "b").await.unwrap();
    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    handle_merge(&pool, t, "a", &["b".into()]).await.unwrap(); // b -> a (root)
    db::handle_delete_person(&pool, t, pa.person_uuid)
        .await
        .unwrap();

    // create("a") lazy-unlinks the dead root "a"; the promoted parent "b"
    // has a smaller PK — this must not abort.
    let new = db::handle_create(&pool, t, "a")
        .await
        .expect("reusing an orphaned dead root with an older parent must succeed");
    assert_ne!(new.person_uuid, pa.person_uuid);
    assert_chain_is_root(&pool, t, "a", new.person_uuid).await;

    // "b" was promoted to the dead tree's root and stays an orphan.
    assert!(resolve(&pool, t, "b").await.unwrap().is_none());

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn lazy_unlink_dead_root_with_older_parent_via_merge() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "b").await.unwrap();
    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    handle_merge(&pool, t, "a", &["b".into()]).await.unwrap();
    db::handle_delete_person(&pool, t, pa.person_uuid)
        .await
        .unwrap();

    let pt = db::handle_create(&pool, t, "tgt").await.unwrap();
    let resp = handle_merge(&pool, t, "tgt", &["a".into()])
        .await
        .expect("merging an orphaned dead root with an older parent must succeed");
    assert_eq!(resp.person_uuid, pt.person_uuid);

    assert_chain_matches(&pool, t, "a", &["a", "tgt"], pt.person_uuid).await;
    assert!(resolve(&pool, t, "b").await.unwrap().is_none());

    assert_structural_invariants(&pool, t).await;
}

// ===========================================================================
// Finding 2 — the 1000-hop walk cap must fail loudly, never mis-classify a
// live distinct_id as "not found" / orphaned (which silently re-assigned it
// to a different person).
// ===========================================================================

/// Matches MAX_WALK_DEPTH in src/db.rs.
const WALK_CAP: usize = 1000;

#[tokio::test]
async fn resolve_at_walk_cap_succeeds() {
    let pool = test_pool().await;
    let t = next_team_id();

    let (leaf, _root, person_uuid) = build_deep_chain(&pool, t, WALK_CAP).await;
    let r = resolve(&pool, t, &leaf)
        .await
        .expect("chain at exactly the walk cap must resolve")
        .expect("leaf at the cap must resolve to its person");
    assert_eq!(r.person_uuid, person_uuid);
}

#[tokio::test]
async fn resolve_beyond_walk_cap_errors_instead_of_not_found() {
    let pool = test_pool().await;
    let t = next_team_id();

    let (leaf, _root, _person_uuid) = build_deep_chain(&pool, t, WALK_CAP + 1).await;
    match resolve(&pool, t, &leaf).await {
        Err(DbError::Internal(_)) => {}
        other => panic!("expected Internal error for over-cap chain, got: {other:?}"),
    }
}

#[tokio::test]
async fn write_beyond_walk_cap_errors_instead_of_orphaning() {
    let pool = test_pool().await;
    let t = next_team_id();

    let (leaf, _root, _person_uuid) = build_deep_chain(&pool, t, WALK_CAP + 1).await;
    let leaf_before = get_uf_row(&pool, t, &leaf).await.unwrap();

    let result = db::handle_create(&pool, t, &leaf).await;
    match result {
        Err(DbError::Internal(_)) => {}
        other => panic!("expected Internal error, got: {other:?}"),
    }

    // The live chain must be untouched: same single person, leaf still linked
    // exactly as before (no orphan-splice, no fresh person minted).
    assert_eq!(count_live_person_mappings(&pool, t).await, 1);
    let leaf_after = get_uf_row(&pool, t, &leaf).await.unwrap();
    assert_eq!(leaf_before.next, leaf_after.next);
    assert_eq!(leaf_before.person_id, leaf_after.person_id);
}

#[tokio::test]
async fn compression_recovers_over_cap_chain() {
    let pool = test_pool().await;
    let t = next_team_id();

    let (leaf, _root, person_uuid) = build_deep_chain(&pool, t, WALK_CAP + 200).await;

    // One compression pass flattens the first 1000 hops; the leaf then sits
    // well under the cap and resolves correctly again.
    db::handle_compress_path(&pool, t, &leaf, 5).await.unwrap();
    let r = resolve(&pool, t, &leaf).await.unwrap().unwrap();
    assert_eq!(r.person_uuid, person_uuid);

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// Finding 3 — a distinct_id_mappings row without its union_find row is
// corruption and must surface as an error, not silently mint rootless persons.
// ===========================================================================

#[tokio::test]
async fn missing_union_find_row_is_a_loud_error() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Corrupt state by hand: mapping row with no union_find node.
    sqlx::query("INSERT INTO distinct_id_mappings (team_id, distinct_id) VALUES ($1, 'ghost-uf')")
        .bind(t)
        .execute(&pool)
        .await
        .unwrap();

    match resolve(&pool, t, "ghost-uf").await {
        Err(DbError::Internal(_)) => {}
        other => panic!("expected Internal error on resolve, got: {other:?}"),
    }

    let result = db::handle_create(&pool, t, "ghost-uf").await;
    match result {
        Err(DbError::Internal(_)) => {}
        other => panic!("expected Internal error on create, got: {other:?}"),
    }
    // No rootless person may have been minted.
    assert_eq!(count_person_mappings(&pool, t).await, 0);
}

// ===========================================================================
// Finding 7 — /resolve and /delete_distinct_id validate distinct_ids like
// every other endpoint (illegal ids can never exist, so reject with 400
// rather than scanning the DB and answering 404).
// ===========================================================================

#[tokio::test]
async fn resolve_rejects_illegal_distinct_id() {
    let pool = test_pool().await;
    let t = next_team_id();

    match resolve(&pool, t, "null").await {
        Err(DbError::IllegalDistinctId(_)) => {}
        other => panic!("expected IllegalDistinctId, got: {other:?}"),
    }
}

#[tokio::test]
async fn delete_distinct_id_rejects_illegal_distinct_id() {
    let pool = test_pool().await;
    let t = next_team_id();

    match db::handle_delete_distinct_id(&pool, t, "guest").await {
        Err(DbError::IllegalDistinctId(_)) => {}
        other => panic!("expected IllegalDistinctId, got: {other:?}"),
    }
}

// ===========================================================================
// Gap C — multiple orphan sources living in the SAME dead chain, in one merge
// call. Unlinking one orphan re-shapes the dead tree that later orphans in
// the same loop still live in.
// ===========================================================================

async fn setup_dead_chain(pool: &sqlx::PgPool, t: i64) {
    // a -> b -> c (root), then soft-delete the person.
    let pc = db::handle_create(pool, t, "c").await.unwrap();
    handle_alias(pool, t, "c", "b").await.unwrap();
    handle_alias(pool, t, "b", "a").await.unwrap();
    db::handle_delete_person(pool, t, pc.person_uuid)
        .await
        .unwrap();
}

async fn assert_dead_chain_merge_outcome(pool: &sqlx::PgPool, t: i64, target_uuid: uuid::Uuid) {
    // The two merged sources resolve to the target; "b" (never merged)
    // remains an orphan of the dead tree and is individually recoverable.
    for did in &["c", "a"] {
        let r = resolve(pool, t, did).await.unwrap().unwrap();
        assert_eq!(r.person_uuid, target_uuid, "{did} should follow the target");
    }
    assert!(resolve(pool, t, "b").await.unwrap().is_none());
    let pb = db::handle_create(pool, t, "b").await.unwrap();
    assert_ne!(pb.person_uuid, target_uuid);
    assert_structural_invariants(pool, t).await;
}

#[tokio::test]
async fn merge_two_orphans_same_dead_chain_root_first() {
    let pool = test_pool().await;
    let t = next_team_id();
    setup_dead_chain(&pool, t).await;
    let pt = db::handle_create(&pool, t, "tgt").await.unwrap();

    handle_merge(&pool, t, "tgt", &["c".into(), "a".into()])
        .await
        .unwrap();
    assert_dead_chain_merge_outcome(&pool, t, pt.person_uuid).await;
}

#[tokio::test]
async fn merge_two_orphans_same_dead_chain_leaf_first() {
    let pool = test_pool().await;
    let t = next_team_id();
    setup_dead_chain(&pool, t).await;
    let pt = db::handle_create(&pool, t, "tgt").await.unwrap();

    handle_merge(&pool, t, "tgt", &["a".into(), "c".into()])
        .await
        .unwrap();
    assert_dead_chain_merge_outcome(&pool, t, pt.person_uuid).await;
}

#[tokio::test]
async fn batched_merge_two_orphans_same_dead_chain_root_first() {
    let pool = test_pool().await;
    let t = next_team_id();
    setup_dead_chain(&pool, t).await;
    let pt = db::handle_create(&pool, t, "tgt").await.unwrap();

    handle_batched_merge(&pool, t, "tgt", &["c".into(), "a".into()])
        .await
        .unwrap();
    assert_dead_chain_merge_outcome(&pool, t, pt.person_uuid).await;
}

#[tokio::test]
async fn batched_merge_two_orphans_same_dead_chain_leaf_first() {
    let pool = test_pool().await;
    let t = next_team_id();
    setup_dead_chain(&pool, t).await;
    let pt = db::handle_create(&pool, t, "tgt").await.unwrap();

    handle_batched_merge(&pool, t, "tgt", &["a".into(), "c".into()])
        .await
        .unwrap();
    assert_dead_chain_merge_outcome(&pool, t, pt.person_uuid).await;
}

// ===========================================================================
// Gap D — hard-deleting leaf / mid-chain nodes of a DEAD tree.
// ===========================================================================

#[tokio::test]
async fn delete_did_leaf_of_dead_chain() {
    let pool = test_pool().await;
    let t = next_team_id();
    setup_dead_chain(&pool, t).await; // a -> b -> c (dead root)

    let resp = db::handle_delete_distinct_id(&pool, t, "a").await.unwrap();
    assert!(!resp.person_deleted, "person was already soft-deleted");

    assert!(resolve(&pool, t, "b").await.unwrap().is_none());
    assert!(resolve(&pool, t, "c").await.unwrap().is_none());
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_mid_node_of_dead_chain() {
    let pool = test_pool().await;
    let t = next_team_id();
    setup_dead_chain(&pool, t).await; // a -> b -> c (dead root)

    let resp = db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();
    assert!(!resp.person_deleted);

    // "a" was spliced past "b" and stays an orphan; both remain recoverable.
    assert!(resolve(&pool, t, "a").await.unwrap().is_none());
    assert!(resolve(&pool, t, "c").await.unwrap().is_none());
    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    assert_chain_is_root(&pool, t, "a", pa.person_uuid).await;

    assert_structural_invariants(&pool, t).await;
}

// ===========================================================================
// Gap E — alias with mixed Orphaned + NotFound operands (both non-live).
// ===========================================================================

#[tokio::test]
async fn alias_orphaned_target_new_source() {
    let pool = test_pool().await;
    let t = next_team_id();

    let old = db::handle_create(&pool, t, "x").await.unwrap();
    db::handle_delete_person(&pool, t, old.person_uuid)
        .await
        .unwrap();

    let resp = handle_alias(&pool, t, "x", "fresh-src").await.unwrap();
    assert_ne!(resp.person_uuid, old.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "x", resp.person_uuid).await;
    assert_chain_matches(&pool, t, "fresh-src", &["fresh-src", "x"], resp.person_uuid).await;
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_new_target_orphaned_source() {
    let pool = test_pool().await;
    let t = next_team_id();

    let old = db::handle_create(&pool, t, "x").await.unwrap();
    db::handle_delete_person(&pool, t, old.person_uuid)
        .await
        .unwrap();

    let resp = handle_alias(&pool, t, "fresh-tgt", "x").await.unwrap();
    assert_ne!(resp.person_uuid, old.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "fresh-tgt", resp.person_uuid).await;
    assert_chain_matches(&pool, t, "x", &["x", "fresh-tgt"], resp.person_uuid).await;
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_structural_invariants(&pool, t).await;
}
