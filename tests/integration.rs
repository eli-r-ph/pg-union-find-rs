mod common;

use common::*;
use pg_union_find_rs::db;
use pg_union_find_rs::models::DbError;

// ===========================================================================
// A. /create (get-or-create)
// ===========================================================================

#[tokio::test]
async fn create_new() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp = db::handle_create(&pool, t, "user-1").await.unwrap();
    assert!(!resp.person_uuid.is_empty());

    let resolved = db::resolve(&pool, t, "user-1").await.unwrap();
    assert!(resolved.is_some());
    assert_eq!(resolved.unwrap().person_uuid, resp.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    let uf = get_uf_row(&pool, t, "user-1").await.unwrap();
    assert!(uf.next.is_none(), "root must have next=NULL");
    assert!(uf.person_id.is_some(), "root must have person_id set");

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn create_idempotent() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r1 = db::handle_create(&pool, t, "user-1").await.unwrap();
    let r2 = db::handle_create(&pool, t, "user-1").await.unwrap();
    assert_eq!(r1.person_uuid, r2.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn create_different_teams() {
    let pool = test_pool().await;
    let t1 = next_team_id();
    let t2 = next_team_id();

    let r1 = db::handle_create(&pool, t1, "shared-did").await.unwrap();
    let r2 = db::handle_create(&pool, t2, "shared-did").await.unwrap();
    assert_ne!(r1.person_uuid, r2.person_uuid);

    assert_eq!(count_person_mappings(&pool, t1).await, 1);
    assert_eq!(count_person_mappings(&pool, t2).await, 1);

    assert_all_invariants(&pool, t1).await;
    assert_all_invariants(&pool, t2).await;
}

// ===========================================================================
// B. /alias and /identify (4-case merge + src==dest)
// ===========================================================================

#[tokio::test]
async fn alias_src_eq_dest_new() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp = db::handle_alias(&pool, t, "x", "x").await.unwrap();
    assert!(!resp.person_uuid.is_empty());

    let resolved = db::resolve(&pool, t, "x").await.unwrap();
    assert_eq!(resolved.unwrap().person_uuid, resp.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_src_eq_dest_existing() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r1 = db::handle_create(&pool, t, "x").await.unwrap();
    let r2 = db::handle_alias(&pool, t, "x", "x").await.unwrap();
    assert_eq!(r1.person_uuid, r2.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case1a_src_exists() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "primary").await.unwrap();
    let aliased = db::handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert_eq!(created.person_uuid, aliased.person_uuid);

    let resolved_anon = db::resolve(&pool, t, "anon").await.unwrap().unwrap();
    assert_eq!(resolved_anon.person_uuid, created.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    let uf_primary = get_uf_row(&pool, t, "primary").await.unwrap();
    assert!(uf_primary.person_id.is_some(), "primary should be root");
    assert!(uf_primary.next.is_none());

    let uf_anon = get_uf_row(&pool, t, "anon").await.unwrap();
    assert!(uf_anon.person_id.is_none(), "anon should not be root");
    assert_eq!(uf_anon.next, Some(uf_primary.current));

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case1b_dest_exists() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "anon").await.unwrap();
    // src="primary" doesn't exist, dest="anon" exists → link primary to anon's chain
    let aliased = db::handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert_eq!(created.person_uuid, aliased.person_uuid);

    let resolved_primary = db::resolve(&pool, t, "primary").await.unwrap().unwrap();
    assert_eq!(resolved_primary.person_uuid, created.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    let uf_anon = get_uf_row(&pool, t, "anon").await.unwrap();
    assert!(uf_anon.person_id.is_some(), "anon should be root");

    let uf_primary = get_uf_row(&pool, t, "primary").await.unwrap();
    assert!(uf_primary.person_id.is_none(), "primary should not be root");
    assert_eq!(uf_primary.next, Some(uf_anon.current));

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2a_same_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    let before_dids = count_distinct_ids(&pool, t).await;
    let before_uf = count_union_find(&pool, t).await;

    // b already linked to a's person — calling alias(a, b) again is a no-op
    let resp = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resp.person_uuid, resolved_a.person_uuid);

    assert_eq!(count_distinct_ids(&pool, t).await, before_dids);
    assert_eq!(count_union_find(&pool, t).await, before_uf);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2b_diff_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    let before_pm = count_person_mappings(&pool, t).await;
    let before_dids = count_distinct_ids(&pool, t).await;

    let result = db::handle_alias(&pool, t, "a", "b").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::AlreadyIdentified(_) => {}
        other => panic!("expected AlreadyIdentified, got: {other}"),
    }

    // Graph unchanged
    assert_eq!(count_person_mappings(&pool, t).await, before_pm);
    assert_eq!(count_distinct_ids(&pool, t).await, before_dids);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case3_neither_exists() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp = db::handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert!(!resp.person_uuid.is_empty());

    let resolved_primary = db::resolve(&pool, t, "primary").await.unwrap().unwrap();
    let resolved_anon = db::resolve(&pool, t, "anon").await.unwrap().unwrap();
    assert_eq!(resolved_primary.person_uuid, resp.person_uuid);
    assert_eq!(resolved_anon.person_uuid, resp.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    let uf_primary = get_uf_row(&pool, t, "primary").await.unwrap();
    assert!(uf_primary.person_id.is_some(), "primary should be root");
    assert!(uf_primary.next.is_none());

    let uf_anon = get_uf_row(&pool, t, "anon").await.unwrap();
    assert!(uf_anon.person_id.is_none(), "anon should not be root");
    assert_eq!(uf_anon.next, Some(uf_primary.current));

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// C. /merge
// ===========================================================================

#[tokio::test]
async fn merge_src_not_found() {
    let pool = test_pool().await;
    let t = next_team_id();

    let result = db::handle_merge(&pool, t, "nonexistent", &["a".into()]).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::NotFound(_) => {}
        other => panic!("expected NotFound, got: {other}"),
    }
}

#[tokio::test]
async fn merge_dest_not_found() {
    let pool = test_pool().await;
    let t = next_team_id();

    let src = db::handle_create(&pool, t, "src").await.unwrap();
    let resp = db::handle_merge(&pool, t, "src", &["new-dest".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, src.person_uuid);

    let resolved = db::resolve(&pool, t, "new-dest").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, src.person_uuid);

    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    let uf_dest = get_uf_row(&pool, t, "new-dest").await.unwrap();
    assert!(uf_dest.person_id.is_none());
    let uf_src = get_uf_row(&pool, t, "src").await.unwrap();
    assert_eq!(uf_dest.next, Some(uf_src.current));

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_same_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    let before_uf = count_union_find(&pool, t).await;
    let resp = db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resp.person_uuid, resolved_a.person_uuid);

    assert_eq!(count_union_find(&pool, t).await, before_uf);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_different_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    let old_root_pid = get_root_person_id(&pool, t, "b").await.unwrap();

    let resp = db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, person_a.person_uuid);

    // Old person_mapping should be deleted (no other roots reference it)
    assert!(
        !person_exists(&pool, old_root_pid).await,
        "orphaned person_mapping should have been deleted"
    );

    assert_eq!(count_person_mappings(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_shared_person_cleanup() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Create person A, person B; link c to b (both under person B)
    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_alias(&pool, t, "b", "c").await.unwrap();

    let old_person_b_id = get_root_person_id(&pool, t, "b").await.unwrap();

    // Merge both b and c into a. They share person B — b's root gets re-pointed,
    // c resolves to the same (already re-pointed) root → no-op for c.
    let resp = db::handle_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    let resolved_c = db::resolve(&pool, t, "c").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, person_a.person_uuid);
    assert_eq!(resolved_c.person_uuid, person_a.person_uuid);

    assert!(
        !person_exists(&pool, old_person_b_id).await,
        "person B should be deleted after merge"
    );
    assert_eq!(count_person_mappings(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_empty_dests() {
    let pool = test_pool().await;
    let t = next_team_id();

    let src = db::handle_create(&pool, t, "src").await.unwrap();
    let resp = db::handle_merge(&pool, t, "src", &[]).await.unwrap();
    assert_eq!(resp.person_uuid, src.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_multiple_dests() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap(); // different person
    db::handle_alias(&pool, t, "a", "c").await.unwrap(); // c linked to a (same person)

    let resp = db::handle_merge(&pool, t, "a", &["b".into(), "c".into(), "brand-new".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);

    for did in &["a", "b", "c", "brand-new"] {
        let resolved = db::resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            resolved.person_uuid, person_a.person_uuid,
            "failed for {did}"
        );
    }

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_src_in_dests() {
    let pool = test_pool().await;
    let t = next_team_id();

    let src = db::handle_create(&pool, t, "a").await.unwrap();
    let resp = db::handle_merge(&pool, t, "a", &["a".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, src.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_repoint_preserves_shared_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Create persons A, B, C
    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();

    // Merge b into a → b's root re-pointed to person A. Now both a's root and
    // b's root have person_id = person_A.
    db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();

    let person_a_id = get_root_person_id(&pool, t, "a").await.unwrap();
    let person_b_root_pid = get_root_person_id(&pool, t, "b").await.unwrap();
    assert_eq!(
        person_a_id, person_b_root_pid,
        "both roots should reference person A"
    );

    // Now merge a into c → a's root re-pointed from person A to person C.
    // But b's root still references person A → person A must NOT be deleted.
    db::handle_merge(&pool, t, "c", &["a".into()])
        .await
        .unwrap();

    assert!(
        person_exists(&pool, person_a_id).await,
        "person A must survive because b's root still references it"
    );

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(
        resolved_a.person_uuid,
        db::resolve(&pool, t, "c")
            .await
            .unwrap()
            .unwrap()
            .person_uuid
    );
    assert_eq!(
        resolved_b.person_uuid, person_a.person_uuid,
        "b still resolves to original person A"
    );

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// D. Resolve and chain traversal
// ===========================================================================

#[tokio::test]
async fn resolve_nonexistent() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resolved = db::resolve(&pool, t, "ghost").await.unwrap();
    assert!(resolved.is_none());
}

#[tokio::test]
async fn resolve_root() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "root-did").await.unwrap();
    let resolved = db::resolve(&pool, t, "root-did").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, created.person_uuid);

    let chain = walk_chain(&pool, t, "root-did").await;
    assert_eq!(chain.len(), 1);
    assert!(chain[0].person_id.is_some());
}

#[tokio::test]
async fn resolve_one_hop() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "root").await.unwrap();
    db::handle_alias(&pool, t, "root", "leaf").await.unwrap();

    let resolved = db::resolve(&pool, t, "leaf").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, created.person_uuid);

    let chain = walk_chain(&pool, t, "leaf").await;
    assert_eq!(chain.len(), 2);
    assert!(chain[0].person_id.is_none(), "leaf should not be root");
    assert!(chain[1].person_id.is_some(), "second node should be root");
}

#[tokio::test]
async fn resolve_multi_hop() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Build chain: d -> c -> b -> a (root)
    let created = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();
    db::handle_alias(&pool, t, "b", "c").await.unwrap();
    db::handle_alias(&pool, t, "c", "d").await.unwrap();

    let resolved = db::resolve(&pool, t, "d").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, created.person_uuid);

    let chain = walk_chain(&pool, t, "d").await;
    assert_eq!(chain.len(), 4, "chain should be d -> c -> b -> a");
    assert!(chain[3].person_id.is_some(), "last node (a) should be root");
    for (i, node) in chain.iter().enumerate().take(3) {
        assert!(node.person_id.is_none(), "node {i} should not be root");
    }

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// E. Cross-operation workflows
// ===========================================================================

#[tokio::test]
async fn create_then_alias() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, person_a.person_uuid);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn create_alias_merge_workflow() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    // Alias should be rejected (different persons)
    let alias_result = db::handle_alias(&pool, t, "a", "b").await;
    assert!(matches!(alias_result, Err(DbError::AlreadyIdentified(_))));

    // Force merge
    db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, person_a.person_uuid);
    assert_eq!(resolved_b.person_uuid, person_a.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn team_isolation() {
    let pool = test_pool().await;
    let t1 = next_team_id();
    let t2 = next_team_id();

    let p1 = db::handle_create(&pool, t1, "shared").await.unwrap();
    let p2 = db::handle_create(&pool, t2, "shared").await.unwrap();
    assert_ne!(p1.person_uuid, p2.person_uuid);

    db::handle_alias(&pool, t1, "shared", "alias-1")
        .await
        .unwrap();

    // alias-1 should not exist on t2
    let resolved_t2 = db::resolve(&pool, t2, "alias-1").await.unwrap();
    assert!(resolved_t2.is_none());

    let resolved_t1 = db::resolve(&pool, t1, "alias-1").await.unwrap().unwrap();
    assert_eq!(resolved_t1.person_uuid, p1.person_uuid);

    assert_all_invariants(&pool, t1).await;
    assert_all_invariants(&pool, t2).await;
}

// ===========================================================================
// F. Graph integrity invariants (after complex multi-op sequence)
// ===========================================================================

#[tokio::test]
async fn invariants_after_complex_operations() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Build a complex graph
    db::handle_create(&pool, t, "p1").await.unwrap();
    db::handle_create(&pool, t, "p2").await.unwrap();
    db::handle_create(&pool, t, "p3").await.unwrap();

    // Alias chains
    db::handle_alias(&pool, t, "p1", "a1").await.unwrap();
    db::handle_alias(&pool, t, "p1", "a2").await.unwrap();
    db::handle_alias(&pool, t, "p2", "b1").await.unwrap();

    // Case 3: neither exists → creates new person
    db::handle_alias(&pool, t, "fresh-src", "fresh-dest")
        .await
        .unwrap();

    // Merge p2 into p1
    db::handle_merge(&pool, t, "p1", &["p2".into()])
        .await
        .unwrap();

    // Merge p3 into p1 with a new distinct_id
    db::handle_merge(&pool, t, "p1", &["p3".into(), "brand-new".into()])
        .await
        .unwrap();

    // All dids under p1/p2/p3 should resolve to p1's person
    let p1_uuid = db::resolve(&pool, t, "p1")
        .await
        .unwrap()
        .unwrap()
        .person_uuid;
    for did in &["p1", "p2", "p3", "a1", "a2", "b1", "brand-new"] {
        let r = db::resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            r.person_uuid, p1_uuid,
            "expected {did} to resolve to p1's person"
        );
    }

    // fresh-src/fresh-dest are a separate person
    let fresh_uuid = db::resolve(&pool, t, "fresh-src")
        .await
        .unwrap()
        .unwrap()
        .person_uuid;
    let fresh_dest_uuid = db::resolve(&pool, t, "fresh-dest")
        .await
        .unwrap()
        .unwrap()
        .person_uuid;
    assert_eq!(fresh_uuid, fresh_dest_uuid);
    assert_ne!(fresh_uuid, p1_uuid);

    // Run all invariant checks
    assert_roots_have_null_next(&pool, t).await;
    assert_non_roots_have_next(&pool, t).await;
    assert_person_refs_valid(&pool, t).await;
    assert_did_refs_valid(&pool, t).await;

    // Verify chains terminate: walk every distinct_id and confirm it reaches a root
    let all_dids: Vec<String> =
        sqlx::query_scalar("SELECT distinct_id FROM distinct_id_mappings WHERE team_id = $1")
            .bind(t)
            .fetch_all(&pool)
            .await
            .unwrap();

    for did in &all_dids {
        let chain = walk_chain(&pool, t, did).await;
        assert!(!chain.is_empty(), "chain for {did} should not be empty");
        let root = chain.last().unwrap();
        assert!(
            root.person_id.is_some(),
            "chain for {did} should terminate at a root"
        );
    }
}
