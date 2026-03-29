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
    assert!(!resp.is_identified);

    let resolved = db::resolve(&pool, t, "user-1").await.unwrap();
    assert!(resolved.is_some());
    assert_eq!(resolved.unwrap().person_uuid, resp.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    let uf = get_uf_row(&pool, t, "user-1").await.unwrap();
    assert!(uf.next.is_none(), "root must have next=NULL");
    assert!(uf.person_id.is_some(), "root must have person_id set");
    assert!(!uf.is_deleted, "root must not be deleted");

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

#[tokio::test]
async fn create_produces_unidentified() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp = db::handle_create(&pool, t, "newbie").await.unwrap();
    assert!(!resp.is_identified);

    let pid = get_root_person_id(&pool, t, "newbie").await.unwrap();
    assert!(!is_person_identified(&pool, pid).await);
}

// ===========================================================================
// B. /alias and /identify (4-case merge + target==source)
// ===========================================================================

#[tokio::test]
async fn alias_target_eq_source_new() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp = db::handle_alias(&pool, t, "x", "x").await.unwrap();
    assert!(!resp.person_uuid.is_empty());
    assert!(resp.is_identified);

    let resolved = db::resolve(&pool, t, "x").await.unwrap();
    assert_eq!(resolved.unwrap().person_uuid, resp.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    let pid = get_root_person_id(&pool, t, "x").await.unwrap();
    assert!(is_person_identified(&pool, pid).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_target_eq_source_existing() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r1 = db::handle_create(&pool, t, "x").await.unwrap();
    let r2 = db::handle_alias(&pool, t, "x", "x").await.unwrap();
    assert_eq!(r1.person_uuid, r2.person_uuid);
    assert!(r2.is_identified);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case1a_target_exists() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "primary").await.unwrap();
    let aliased = db::handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert_eq!(created.person_uuid, aliased.person_uuid);
    assert!(aliased.is_identified);

    let resolved_anon = db::resolve(&pool, t, "anon").await.unwrap().unwrap();
    assert_eq!(resolved_anon.person_uuid, created.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    let uf_primary = get_uf_row(&pool, t, "primary").await.unwrap();
    assert!(uf_primary.person_id.is_some(), "primary should be root");
    assert!(uf_primary.next.is_none());
    assert!(!uf_primary.is_deleted);

    let uf_anon = get_uf_row(&pool, t, "anon").await.unwrap();
    assert!(uf_anon.person_id.is_none(), "anon should not be root");
    assert_eq!(uf_anon.next, Some(uf_primary.current));
    assert!(!uf_anon.is_deleted);

    let pid = get_root_person_id(&pool, t, "primary").await.unwrap();
    assert!(is_person_identified(&pool, pid).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case1b_source_exists() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "anon").await.unwrap();
    // target="primary" doesn't exist, source="anon" exists → link primary to anon's chain
    let aliased = db::handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert_eq!(created.person_uuid, aliased.person_uuid);
    assert!(aliased.is_identified);

    let resolved_primary = db::resolve(&pool, t, "primary").await.unwrap().unwrap();
    assert_eq!(resolved_primary.person_uuid, created.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    let uf_anon = get_uf_row(&pool, t, "anon").await.unwrap();
    assert!(uf_anon.person_id.is_some(), "anon should be root");
    assert!(!uf_anon.is_deleted);

    let uf_primary = get_uf_row(&pool, t, "primary").await.unwrap();
    assert!(uf_primary.person_id.is_none(), "primary should not be root");
    assert_eq!(uf_primary.next, Some(uf_anon.current));
    assert!(!uf_primary.is_deleted);

    let pid = get_root_person_id(&pool, t, "anon").await.unwrap();
    assert!(is_person_identified(&pool, pid).await);

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
    assert!(resp.is_identified);
    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resp.person_uuid, resolved_a.person_uuid);

    assert_eq!(count_distinct_ids(&pool, t).await, before_dids);
    assert_eq!(count_union_find(&pool, t).await, before_uf);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2b_both_unidentified_succeeds() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    // Both persons are unidentified (created via /create) so merge should succeed
    let result = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_eq!(result.person_uuid, person_a.person_uuid);
    assert!(result.is_identified);

    // b now resolves to a's person
    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, person_a.person_uuid);

    // Orphaned person cleaned up
    assert_eq!(count_person_mappings(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2b_source_identified_rejected() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Create A and B
    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    // Identify B by aliasing B to C — B's person becomes is_identified=true
    db::handle_alias(&pool, t, "b", "c").await.unwrap();
    let pid_b = get_root_person_id(&pool, t, "b").await.unwrap();
    assert!(is_person_identified(&pool, pid_b).await);

    // Now alias(target=a, source=b): source (b) is identified → reject
    let result = db::handle_alias(&pool, t, "a", "b").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::AlreadyIdentified(_) => {}
        other => panic!("expected AlreadyIdentified, got: {other}"),
    }

    // Graph unchanged: a and b still have different persons
    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_ne!(resolved_a.person_uuid, resolved_b.person_uuid);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2b_target_identified_source_not() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Create A and B
    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    // Identify A by aliasing A to C
    let alias_resp = db::handle_alias(&pool, t, "a", "c").await.unwrap();
    let pid_a = get_root_person_id(&pool, t, "a").await.unwrap();
    assert!(is_person_identified(&pool, pid_a).await);
    assert!(alias_resp.is_identified);

    // B is still unidentified
    let pid_b = get_root_person_id(&pool, t, "b").await.unwrap();
    assert!(!is_person_identified(&pool, pid_b).await);

    // alias(target=a, source=b): source (b) is NOT identified → merge succeeds
    let result = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert!(result.is_identified);

    // b now resolves to a's person
    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, resolved_b.person_uuid);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case3_neither_exists() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp = db::handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert!(!resp.person_uuid.is_empty());
    assert!(resp.is_identified);

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
    assert!(!uf_primary.is_deleted);

    let uf_anon = get_uf_row(&pool, t, "anon").await.unwrap();
    assert!(uf_anon.person_id.is_none(), "anon should not be root");
    assert_eq!(uf_anon.next, Some(uf_primary.current));
    assert!(!uf_anon.is_deleted);

    let pid = get_root_person_id(&pool, t, "primary").await.unwrap();
    assert!(is_person_identified(&pool, pid).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_sets_identified() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "x").await.unwrap();
    db::handle_create(&pool, t, "y").await.unwrap();

    let pid_x = get_root_person_id(&pool, t, "x").await.unwrap();
    let pid_y = get_root_person_id(&pool, t, "y").await.unwrap();
    assert!(!is_person_identified(&pool, pid_x).await);
    assert!(!is_person_identified(&pool, pid_y).await);

    let resp = db::handle_alias(&pool, t, "x", "y").await.unwrap();
    assert!(resp.is_identified);

    // The surviving person (x's) should be identified
    let pid_result = get_root_person_id(&pool, t, "x").await.unwrap();
    assert!(is_person_identified(&pool, pid_result).await);
}

// ===========================================================================
// C. /merge
// ===========================================================================

#[tokio::test]
async fn merge_target_not_found() {
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
async fn merge_source_not_found() {
    let pool = test_pool().await;
    let t = next_team_id();

    let tgt = db::handle_create(&pool, t, "tgt").await.unwrap();
    let resp = db::handle_merge(&pool, t, "tgt", &["new-source".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, tgt.person_uuid);
    assert!(resp.is_identified);

    let resolved = db::resolve(&pool, t, "new-source").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, tgt.person_uuid);

    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    let uf_source = get_uf_row(&pool, t, "new-source").await.unwrap();
    assert!(uf_source.person_id.is_none());
    assert!(!uf_source.is_deleted);
    let uf_tgt = get_uf_row(&pool, t, "tgt").await.unwrap();
    assert_eq!(uf_source.next, Some(uf_tgt.current));
    assert!(!uf_tgt.is_deleted);

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
    assert!(resp.is_identified);
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
    assert!(resp.is_identified);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, person_a.person_uuid);

    assert!(
        !person_exists(&pool, old_root_pid).await,
        "orphaned person_mapping should have been deleted"
    );

    assert_eq!(count_person_mappings(&pool, t).await, 1);

    let pid = get_root_person_id(&pool, t, "a").await.unwrap();
    assert!(is_person_identified(&pool, pid).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_shared_person_cleanup() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_alias(&pool, t, "b", "c").await.unwrap();

    let old_person_b_id = get_root_person_id(&pool, t, "b").await.unwrap();

    let resp = db::handle_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(resp.is_identified);

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
async fn merge_empty_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let tgt = db::handle_create(&pool, t, "tgt").await.unwrap();
    let resp = db::handle_merge(&pool, t, "tgt", &[]).await.unwrap();
    assert_eq!(resp.person_uuid, tgt.person_uuid);
    assert!(resp.is_identified);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_multiple_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_alias(&pool, t, "a", "c").await.unwrap();

    let resp = db::handle_merge(&pool, t, "a", &["b".into(), "c".into(), "brand-new".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(resp.is_identified);

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
async fn merge_target_in_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let tgt = db::handle_create(&pool, t, "a").await.unwrap();
    let resp = db::handle_merge(&pool, t, "a", &["a".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, tgt.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_chain_all_ids_follow() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    let person_c = db::handle_create(&pool, t, "c").await.unwrap();

    // Step 1: merge b into a — b's tree is now linked into a's tree
    db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, resolved_b.person_uuid);

    // Step 2: merge a into c — since b is part of a's tree, b should
    // also resolve to c's person after this.
    db::handle_merge(&pool, t, "c", &["a".into()])
        .await
        .unwrap();

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    let resolved_c = db::resolve(&pool, t, "c").await.unwrap().unwrap();

    assert_eq!(
        resolved_a.person_uuid, person_c.person_uuid,
        "a should resolve to c's person"
    );
    assert_eq!(
        resolved_b.person_uuid, person_c.person_uuid,
        "b should follow the chain through a to c's person"
    );
    assert_eq!(resolved_c.person_uuid, person_c.person_uuid);

    assert_eq!(
        count_person_mappings(&pool, t).await,
        1,
        "only c's person should remain"
    );

    assert_all_invariants(&pool, t).await;
}

/// Verify that a longer chain of merges (a<-b, a<-c, then d<-a) correctly
/// moves all previously merged distinct_ids to the final target.
#[tokio::test]
async fn merge_chain_deep_transitive() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    let person_d = db::handle_create(&pool, t, "d").await.unwrap();

    db::handle_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();

    // a, b, c all share a person
    for did in &["a", "b", "c"] {
        let r = db::resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            r.person_uuid,
            db::resolve(&pool, t, "a")
                .await
                .unwrap()
                .unwrap()
                .person_uuid,
            "{did} should resolve to a's person after first merge"
        );
    }

    // Now merge a into d — all of a's tree (including b and c) should follow
    db::handle_merge(&pool, t, "d", &["a".into()])
        .await
        .unwrap();

    for did in &["a", "b", "c", "d"] {
        let r = db::resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            r.person_uuid, person_d.person_uuid,
            "{did} should resolve to d's person after second merge"
        );
    }

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_sets_identified() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "x").await.unwrap();
    db::handle_create(&pool, t, "y").await.unwrap();

    let pid_x = get_root_person_id(&pool, t, "x").await.unwrap();
    assert!(!is_person_identified(&pool, pid_x).await);

    let resp = db::handle_merge(&pool, t, "x", &["y".into()])
        .await
        .unwrap();
    assert!(resp.is_identified);

    assert!(is_person_identified(&pool, pid_x).await);
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
    assert!(!chain[0].is_deleted);
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
    assert!(!chain[0].is_deleted);
    assert!(chain[1].person_id.is_some(), "second node should be root");
    assert!(!chain[1].is_deleted);
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
    for (i, node) in chain.iter().enumerate() {
        assert!(!node.is_deleted, "node {i} should not be deleted");
        if i < 3 {
            assert!(node.person_id.is_none(), "node {i} should not be root");
        }
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

    // Both are unidentified, so alias succeeds (merges b into a)
    let alias_result = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_eq!(alias_result.person_uuid, person_a.person_uuid);
    assert!(alias_result.is_identified);

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

    db::handle_create(&pool, t, "p1").await.unwrap();
    db::handle_create(&pool, t, "p2").await.unwrap();
    db::handle_create(&pool, t, "p3").await.unwrap();

    db::handle_alias(&pool, t, "p1", "a1").await.unwrap();
    db::handle_alias(&pool, t, "p1", "a2").await.unwrap();
    db::handle_alias(&pool, t, "p2", "b1").await.unwrap();

    db::handle_alias(&pool, t, "fresh-src", "fresh-dest")
        .await
        .unwrap();

    db::handle_merge(&pool, t, "p1", &["p2".into()])
        .await
        .unwrap();

    db::handle_merge(&pool, t, "p1", &["p3".into(), "brand-new".into()])
        .await
        .unwrap();

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

    assert_roots_have_null_next(&pool, t).await;
    assert_non_roots_have_next(&pool, t).await;
    assert_person_refs_valid(&pool, t).await;
    assert_did_refs_valid(&pool, t).await;

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
        for node in &chain {
            assert!(
                !node.is_deleted,
                "chain for {did} should have no deleted nodes"
            );
        }
    }

    assert_no_deleted_rows(&pool, t).await;
}

// ===========================================================================
// G. Illegal distinct_id validation (exhaustive)
// ===========================================================================

#[tokio::test]
async fn create_rejects_illegal_ids_exhaustive() {
    let pool = test_pool().await;
    let t = next_team_id();

    let bad_ids: &[&str] = &[
        // empty / whitespace
        "",
        "  ",
        "\t",
        // case-insensitive blocklist (+ uppercase variants)
        "anonymous",
        "ANONYMOUS",
        "Anonymous",
        "guest",
        "Guest",
        "GUEST",
        "distinctid",
        "DISTINCTID",
        "distinct_id",
        "DISTINCT_ID",
        "id",
        "ID",
        "not_authenticated",
        "NOT_AUTHENTICATED",
        "email",
        "EMAIL",
        "undefined",
        "UNDEFINED",
        "Undefined",
        "true",
        "TRUE",
        "True",
        "false",
        "FALSE",
        "False",
        // case-sensitive blocklist
        "[object Object]",
        "NaN",
        "None",
        "none",
        "null",
        "0",
        // illegal characters: quotes, backslash, backtick
        "'",
        "\"",
        "`",
        "\\",
        "'null'",
        "\"null\"",
        "it's",
        "back\\slash",
        // control characters
        "\0",
        "\n",
        "\r",
        "hello\x1b[31m",
    ];

    for bad_id in bad_ids {
        let result = db::handle_create(&pool, t, bad_id).await;
        assert!(result.is_err(), "expected create to reject {:?}", bad_id);
        match result.unwrap_err() {
            DbError::IllegalDistinctId(_) => {}
            other => panic!("expected IllegalDistinctId for {:?}, got: {other}", bad_id),
        }
    }
}

#[tokio::test]
async fn create_rejects_too_long_id() {
    let pool = test_pool().await;
    let t = next_team_id();

    let long_id = "x".repeat(201);
    let result = db::handle_create(&pool, t, &long_id).await;
    assert!(result.is_err(), "expected create to reject 201-char ID");
    match result.unwrap_err() {
        DbError::IllegalDistinctId(_) => {}
        other => panic!("expected IllegalDistinctId, got: {other}"),
    }
}

#[tokio::test]
async fn create_accepts_legal_ids() {
    let pool = test_pool().await;
    let t = next_team_id();

    let at_limit = "x".repeat(200);
    let ok_ids: Vec<&str> = vec![
        "user-123",
        "alice@example.com",
        "00000",
        "my_id_99",
        "a",
        &at_limit,
    ];
    for ok_id in &ok_ids {
        let result = db::handle_create(&pool, t, ok_id).await;
        assert!(
            result.is_ok(),
            "expected create to accept {:?}, got: {:?}",
            ok_id,
            result.unwrap_err()
        );
    }
}

#[tokio::test]
async fn alias_rejects_illegal_ids() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "legit").await.unwrap();

    let result = db::handle_alias(&pool, t, "legit", "null").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::IllegalDistinctId(_) => {}
        other => panic!("expected IllegalDistinctId, got: {other}"),
    }

    let result = db::handle_alias(&pool, t, "guest", "legit").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::IllegalDistinctId(_) => {}
        other => panic!("expected IllegalDistinctId, got: {other}"),
    }
}

#[tokio::test]
async fn merge_rejects_illegal_ids() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "legit").await.unwrap();

    let result = db::handle_merge(&pool, t, "anonymous", &[]).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::IllegalDistinctId(_) => {}
        other => panic!("expected IllegalDistinctId, got: {other}"),
    }

    let result = db::handle_merge(&pool, t, "legit", &["undefined".into()]).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::IllegalDistinctId(_) => {}
        other => panic!("expected IllegalDistinctId, got: {other}"),
    }
}

// ===========================================================================
// H. Union-find structure after link_root_to_target
// ===========================================================================

#[tokio::test]
async fn merge_link_structure() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    let uf_a_before = get_uf_row(&pool, t, "a").await.unwrap();

    db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();

    // b's root should now be a non-root linked to a's node
    let uf_b = get_uf_row(&pool, t, "b").await.unwrap();
    assert!(uf_b.person_id.is_none(), "b should be non-root after merge");
    assert!(!uf_b.is_deleted);
    assert_eq!(
        uf_b.next,
        Some(uf_a_before.current),
        "b should point to a's UF node"
    );

    // a is still the root
    let uf_a = get_uf_row(&pool, t, "a").await.unwrap();
    assert!(uf_a.person_id.is_some(), "a should still be root");
    assert!(uf_a.next.is_none());
    assert!(!uf_a.is_deleted);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2b_link_structure() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    let uf_a_before = get_uf_row(&pool, t, "a").await.unwrap();

    // Both unidentified, so alias case 2b merges b into a
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    // b's root should now be a non-root linked to a's node
    let uf_b = get_uf_row(&pool, t, "b").await.unwrap();
    assert!(uf_b.person_id.is_none(), "b should be non-root after alias");
    assert!(!uf_b.is_deleted);
    assert_eq!(
        uf_b.next,
        Some(uf_a_before.current),
        "b should point to a's UF node"
    );

    let uf_a = get_uf_row(&pool, t, "a").await.unwrap();
    assert!(uf_a.person_id.is_some(), "a should still be root");
    assert!(!uf_a.is_deleted);

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// I. Re-alias and fan-out after merges
// ===========================================================================

#[tokio::test]
async fn alias_after_merge() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    let person_c = db::handle_create(&pool, t, "c").await.unwrap();

    // merge a into c (a's root now linked under c)
    db::handle_merge(&pool, t, "c", &["a".into()])
        .await
        .unwrap();

    // now alias a with a new distinct_id d — d should resolve to c's person
    db::handle_alias(&pool, t, "a", "d-new").await.unwrap();

    let resolved_d = db::resolve(&pool, t, "d-new").await.unwrap().unwrap();
    assert_eq!(
        resolved_d.person_uuid, person_c.person_uuid,
        "d should resolve to c's person through the merged chain"
    );

    for did in &["a", "b", "c", "d-new"] {
        let r = db::resolve(&pool, t, did).await.unwrap();
        if did == &"b" {
            // b was never merged into anything in this test
            assert!(r.is_some());
        } else {
            assert_eq!(
                r.unwrap().person_uuid,
                person_c.person_uuid,
                "{did} should resolve to c's person"
            );
        }
    }

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_reverse_direction() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp1 = db::handle_alias(&pool, t, "a", "b").await.unwrap();

    let dids_before = count_distinct_ids(&pool, t).await;
    let uf_before = count_union_find(&pool, t).await;

    // Reverse: alias(b, a) — both exist, same person → case 2a no-op
    let resp2 = db::handle_alias(&pool, t, "b", "a").await.unwrap();
    assert_eq!(resp1.person_uuid, resp2.person_uuid);
    assert!(resp2.is_identified);

    assert_eq!(count_distinct_ids(&pool, t).await, dids_before);
    assert_eq!(count_union_find(&pool, t).await, uf_before);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_fan_out() {
    let pool = test_pool().await;
    let t = next_team_id();

    // a is root; b, c are leaves of a; d is a leaf of b
    let resp_ab = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    db::handle_alias(&pool, t, "a", "c").await.unwrap();
    db::handle_alias(&pool, t, "b", "d").await.unwrap();

    for did in &["a", "b", "c", "d"] {
        let r = db::resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            r.person_uuid, resp_ab.person_uuid,
            "{did} should resolve to a's person"
        );
    }

    // d should traverse: d -> b -> a (3 nodes)
    let chain_d = walk_chain(&pool, t, "d").await;
    assert_eq!(chain_d.len(), 3, "d chain should be d -> b -> a");
    assert!(chain_d[0].person_id.is_none(), "d should not be root");
    assert!(chain_d[1].person_id.is_none(), "b should not be root");
    assert!(chain_d[2].person_id.is_some(), "a should be root");
    for node in &chain_d {
        assert!(!node.is_deleted);
    }

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// J. Merge source-list edge cases
// ===========================================================================

#[tokio::test]
async fn merge_duplicate_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    // b appears twice — second iteration should be a same-person skip
    let resp = db::handle_merge(&pool, t, "a", &["b".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(resp.is_identified);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, person_a.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_target_plus_others_in_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    // target "a" also appears in sources alongside real source "b"
    let resp = db::handle_merge(&pool, t, "a", &["a".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, person_a.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// K. Cross-operation pipeline
// ===========================================================================

#[tokio::test]
async fn full_pipeline_create_alias_merge_alias_resolve() {
    let pool = test_pool().await;
    let t = next_team_id();

    // create(a) -> alias(a, b) -> create(c) -> merge(a, [c]) -> alias(a, d)
    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    db::handle_merge(&pool, t, "a", &["c".into()])
        .await
        .unwrap();
    db::handle_alias(&pool, t, "a", "d").await.unwrap();

    for did in &["a", "b", "c", "d"] {
        let r = db::resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            r.person_uuid, person_a.person_uuid,
            "{did} should resolve to a's person"
        );
    }

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// L. is_identified state transitions
// ===========================================================================

#[tokio::test]
async fn create_idempotent_after_identify() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r1 = db::handle_create(&pool, t, "x").await.unwrap();
    assert!(!r1.is_identified);

    // Alias makes x's person identified
    db::handle_alias(&pool, t, "x", "y").await.unwrap();

    // Re-creating x should reflect the identified state
    let r2 = db::handle_create(&pool, t, "x").await.unwrap();
    assert_eq!(r1.person_uuid, r2.person_uuid);
    assert!(r2.is_identified);
}

#[tokio::test]
async fn merge_already_identified_target() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Make x identified via self-alias
    let resp_x = db::handle_alias(&pool, t, "x", "x").await.unwrap();
    assert!(resp_x.is_identified);

    db::handle_create(&pool, t, "y").await.unwrap();

    // Merge y into x — x was already identified, should stay identified
    let resp = db::handle_merge(&pool, t, "x", &["y".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, resp_x.person_uuid);
    assert!(resp.is_identified);

    let pid = get_root_person_id(&pool, t, "x").await.unwrap();
    assert!(is_person_identified(&pool, pid).await);

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// M. Orphan cleanup verification
// ===========================================================================

#[tokio::test]
async fn orphan_cleanup_stepwise() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    assert_eq!(count_person_mappings(&pool, t).await, 3);

    // Step 1: merge b into a — b's person should be deleted
    db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_eq!(
        count_person_mappings(&pool, t).await,
        2,
        "b's person should be cleaned up after first merge"
    );

    // Step 2: merge c into a — c's person should be deleted
    db::handle_merge(&pool, t, "a", &["c".into()])
        .await
        .unwrap();
    assert_eq!(
        count_person_mappings(&pool, t).await,
        1,
        "c's person should be cleaned up after second merge"
    );

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// N. /delete_person
// ===========================================================================

#[tokio::test]
async fn delete_person_basic() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    let pid = get_root_person_id(&pool, t, "a").await.unwrap();

    let resp = db::handle_delete_person(&pool, t, &created.person_uuid)
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, created.person_uuid);

    let resolved = db::resolve(&pool, t, "a").await.unwrap();
    assert!(
        resolved.is_none(),
        "resolve should return None after delete"
    );

    assert!(is_person_deleted(&pool, pid).await);

    let uf = get_uf_row(&pool, t, "a").await.unwrap();
    assert!(uf.is_deleted, "union_find root should be marked deleted");

    assert_eq!(count_live_person_mappings(&pool, t).await, 0);
    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_person_not_found() {
    let pool = test_pool().await;
    let t = next_team_id();

    let result = db::handle_delete_person(&pool, t, "nonexistent-uuid").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::NotFound(_) => {}
        other => panic!("expected NotFound, got: {other}"),
    }
}

#[tokio::test]
async fn delete_person_already_deleted() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_delete_person(&pool, t, &created.person_uuid)
        .await
        .unwrap();

    let result = db::handle_delete_person(&pool, t, &created.person_uuid).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::NotFound(_) => {}
        other => panic!("expected NotFound on second delete, got: {other}"),
    }
}

#[tokio::test]
async fn delete_person_with_chain() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();
    db::handle_alias(&pool, t, "a", "c").await.unwrap();

    db::handle_delete_person(&pool, t, &created.person_uuid)
        .await
        .unwrap();

    for did in &["a", "b", "c"] {
        let resolved = db::resolve(&pool, t, did).await.unwrap();
        assert!(
            resolved.is_none(),
            "{did} should resolve to None after person delete"
        );
    }

    assert_eq!(count_live_person_mappings(&pool, t).await, 0);
    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_person_team_isolation() {
    let pool = test_pool().await;
    let t1 = next_team_id();
    let t2 = next_team_id();

    let p1 = db::handle_create(&pool, t1, "shared").await.unwrap();
    let p2 = db::handle_create(&pool, t2, "shared").await.unwrap();

    db::handle_delete_person(&pool, t1, &p1.person_uuid)
        .await
        .unwrap();

    let resolved_t1 = db::resolve(&pool, t1, "shared").await.unwrap();
    assert!(resolved_t1.is_none(), "team1 person should be deleted");

    let resolved_t2 = db::resolve(&pool, t2, "shared").await.unwrap();
    assert!(resolved_t2.is_some(), "team2 person should be unaffected");
    assert_eq!(resolved_t2.unwrap().person_uuid, p2.person_uuid);

    assert_structural_invariants(&pool, t1).await;
    assert_all_invariants(&pool, t2).await;
}

// ===========================================================================
// O. /delete_distinct_id
// ===========================================================================

#[tokio::test]
async fn delete_did_sole_node() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    let pid = get_root_person_id(&pool, t, "a").await.unwrap();

    let resp = db::handle_delete_distinct_id(&pool, t, "a").await.unwrap();
    assert_eq!(resp.distinct_id, "a");
    assert!(
        resp.person_deleted,
        "person should be soft-deleted when last DID removed"
    );

    assert_eq!(count_distinct_ids(&pool, t).await, 0);
    assert_eq!(count_union_find(&pool, t).await, 0);
    assert!(is_person_deleted(&pool, pid).await);

    let resolved = db::resolve(&pool, t, "a").await.unwrap();
    assert!(resolved.is_none());

    // person_mapping row still exists (soft-deleted)
    assert!(person_exists(&pool, pid).await);
    let _ = created;

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_not_found() {
    let pool = test_pool().await;
    let t = next_team_id();

    let result = db::handle_delete_distinct_id(&pool, t, "ghost").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::NotFound(_) => {}
        other => panic!("expected NotFound, got: {other}"),
    }
}

#[tokio::test]
async fn delete_did_leaf() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    // b is the leaf, a is the root
    let resp = db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();
    assert_eq!(resp.distinct_id, "b");
    assert!(
        !resp.person_deleted,
        "person should NOT be deleted (a still references it)"
    );

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap();
    assert!(resolved_a.is_some());
    assert_eq!(resolved_a.unwrap().person_uuid, created.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap();
    assert!(resolved_b.is_none(), "b should no longer resolve");

    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_root() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    // a is the root, b is the leaf pointing to a
    // Deleting a should unlink a (b inherits a's person_id) then hard-delete a
    let resp = db::handle_delete_distinct_id(&pool, t, "a").await.unwrap();
    assert_eq!(resp.distinct_id, "a");
    assert!(
        !resp.person_deleted,
        "person should NOT be deleted (b inherited root)"
    );

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap();
    assert!(resolved_b.is_some(), "b should still resolve");
    assert_eq!(resolved_b.unwrap().person_uuid, created.person_uuid);

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap();
    assert!(resolved_a.is_none(), "a should no longer resolve");

    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    let uf_b = get_uf_row(&pool, t, "b").await.unwrap();
    assert!(uf_b.person_id.is_some(), "b should now be the root");
    assert!(uf_b.next.is_none());

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_mid_chain() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Build chain: c -> b -> a (root)
    let created = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();
    db::handle_alias(&pool, t, "b", "c").await.unwrap();

    // Delete b (mid-chain) — c should be spliced past b to point at a
    let resp = db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();
    assert_eq!(resp.distinct_id, "b");
    assert!(!resp.person_deleted);

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, created.person_uuid);

    let resolved_c = db::resolve(&pool, t, "c").await.unwrap().unwrap();
    assert_eq!(resolved_c.person_uuid, created.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap();
    assert!(resolved_b.is_none());

    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    // Verify chain structure: c should now point directly to a
    let uf_c = get_uf_row(&pool, t, "c").await.unwrap();
    let uf_a = get_uf_row(&pool, t, "a").await.unwrap();
    assert_eq!(
        uf_c.next,
        Some(uf_a.current),
        "c should point directly to a after b removed"
    );

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_preserves_siblings() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();

    let resp = db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();
    assert_eq!(resp.distinct_id, "b");
    assert!(!resp.person_deleted);

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, created.person_uuid);

    let resolved_c = db::resolve(&pool, t, "c").await.unwrap().unwrap();
    assert_eq!(resolved_c.person_uuid, created.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap();
    assert!(resolved_b.is_none());

    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);
    assert_eq!(count_live_person_mappings(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// P. Lazy unlink / post-delete recovery
// ===========================================================================

#[tokio::test]
async fn create_after_delete_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let old = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_delete_person(&pool, t, &old.person_uuid)
        .await
        .unwrap();

    let new = db::handle_create(&pool, t, "a").await.unwrap();
    assert_ne!(old.person_uuid, new.person_uuid, "should get a new person");

    let resolved = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, new.person_uuid);

    assert_eq!(count_live_person_mappings(&pool, t).await, 1);
    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn create_after_delete_person_no_duplicate_rows() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    let old_uuid = db::resolve(&pool, t, "a")
        .await
        .unwrap()
        .unwrap()
        .person_uuid;
    db::handle_delete_person(&pool, t, &old_uuid).await.unwrap();

    db::handle_create(&pool, t, "a").await.unwrap();

    assert_eq!(
        count_distinct_ids(&pool, t).await,
        1,
        "DID row should be reused, not duplicated"
    );
    assert_eq!(
        count_union_find(&pool, t).await,
        1,
        "union_find row should be reused, not duplicated"
    );
    assert_eq!(count_live_person_mappings(&pool, t).await, 1);

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_with_deleted_target() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    let pb = db::handle_create(&pool, t, "b").await.unwrap();

    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();

    // a is orphaned, b is live → a should attach into b's chain
    let resp = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_eq!(resp.person_uuid, pb.person_uuid);
    assert!(resp.is_identified);

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, pb.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, pb.person_uuid);

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_with_deleted_source() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    let pb = db::handle_create(&pool, t, "b").await.unwrap();

    db::handle_delete_person(&pool, t, &pb.person_uuid)
        .await
        .unwrap();

    // a is live, b is orphaned → b should attach into a's chain
    let resp = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_eq!(resp.person_uuid, pa.person_uuid);
    assert!(resp.is_identified);

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, pa.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, pa.person_uuid);

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_both_deleted() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    let pb = db::handle_create(&pool, t, "b").await.unwrap();

    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();
    db::handle_delete_person(&pool, t, &pb.person_uuid)
        .await
        .unwrap();

    // Both orphaned → new person created, both DIDs reused
    let resp = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_ne!(resp.person_uuid, pa.person_uuid);
    assert_ne!(resp.person_uuid, pb.person_uuid);
    assert!(resp.is_identified);

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, resp.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, resp.person_uuid);

    assert_eq!(
        count_distinct_ids(&pool, t).await,
        2,
        "DID rows should be reused"
    );
    assert_eq!(count_live_person_mappings(&pool, t).await, 1);

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_with_deleted_target() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();

    // Target orphaned → should create new person for a, then link b
    let resp = db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_ne!(resp.person_uuid, pa.person_uuid);
    assert!(resp.is_identified);

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, resp.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, resp.person_uuid);

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_with_deleted_source() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    let pb = db::handle_create(&pool, t, "b").await.unwrap();

    db::handle_delete_person(&pool, t, &pb.person_uuid)
        .await
        .unwrap();

    // b is orphaned → should be linked into a's chain
    let resp = db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, pa.person_uuid);
    assert!(resp.is_identified);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, pa.person_uuid);

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn resolve_after_delete_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_delete_person(&pool, t, &created.person_uuid)
        .await
        .unwrap();

    let resolved = db::resolve(&pool, t, "a").await.unwrap();
    assert!(
        resolved.is_none(),
        "resolve should return None for deleted person"
    );
}

// ===========================================================================
// Q. Cross-operation delete workflows
// ===========================================================================

#[tokio::test]
async fn delete_person_then_full_lifecycle() {
    let pool = test_pool().await;
    let t = next_team_id();

    let old = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    db::handle_delete_person(&pool, t, &old.person_uuid)
        .await
        .unwrap();

    // Re-create a (lazy unlink), then alias with c
    let new = db::handle_create(&pool, t, "a").await.unwrap();
    assert_ne!(old.person_uuid, new.person_uuid);

    db::handle_alias(&pool, t, "a", "c").await.unwrap();

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    let resolved_c = db::resolve(&pool, t, "c").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, new.person_uuid);
    assert_eq!(resolved_c.person_uuid, new.person_uuid);

    assert_eq!(count_live_person_mappings(&pool, t).await, 1);
    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_then_recreate() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    // Hard-delete b's DID
    db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();

    // b is now fully gone — create(b) should get a fresh person
    let pb = db::handle_create(&pool, t, "b").await.unwrap();
    assert_ne!(pa.person_uuid, pb.person_uuid, "b should be a new person");

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, pa.person_uuid);
    assert_eq!(resolved_b.person_uuid, pb.person_uuid);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_person_merge_recovery() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    let pb = db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_alias(&pool, t, "a", "c").await.unwrap();

    // Delete a's person (a and c become orphaned)
    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();

    // Merge orphaned a and c into live b
    let resp = db::handle_merge(&pool, t, "b", &["a".into(), "c".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, pb.person_uuid);
    assert!(resp.is_identified);

    for did in &["a", "b", "c"] {
        let resolved = db::resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            resolved.person_uuid, pb.person_uuid,
            "{did} should resolve to b's person after merge recovery"
        );
    }

    assert_structural_invariants(&pool, t).await;
}

// ===========================================================================
// R. Unique person_id invariant — root unlink with multiple parents
// ===========================================================================

#[tokio::test]
async fn delete_did_root_with_multiple_parents() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Build fan-out: A -> R, B -> R, C -> R  (R is root)
    let created = db::handle_create(&pool, t, "r").await.unwrap();
    db::handle_alias(&pool, t, "r", "a").await.unwrap();
    db::handle_alias(&pool, t, "r", "b").await.unwrap();
    db::handle_alias(&pool, t, "r", "c").await.unwrap();

    let pid_before = get_root_person_id(&pool, t, "r").await.unwrap();

    // Delete the root distinct_id
    let resp = db::handle_delete_distinct_id(&pool, t, "r").await.unwrap();
    assert_eq!(resp.distinct_id, "r");
    assert!(
        !resp.person_deleted,
        "person should survive via promoted parent"
    );

    // r should no longer resolve
    let resolved_r = db::resolve(&pool, t, "r").await.unwrap();
    assert!(resolved_r.is_none());

    // All remaining distinct_ids should resolve to the same person
    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    let resolved_c = db::resolve(&pool, t, "c").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, created.person_uuid);
    assert_eq!(resolved_b.person_uuid, created.person_uuid);
    assert_eq!(resolved_c.person_uuid, created.person_uuid);

    // Exactly one root should exist with the original person_id
    let roots: Vec<(i64,)> = sqlx::query_as(
        "SELECT current FROM union_find \
         WHERE team_id = $1 AND person_id = $2",
    )
    .bind(t)
    .bind(pid_before)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(roots.len(), 1, "exactly one root should hold the person_id");

    assert_eq!(count_distinct_ids(&pool, t).await, 3);
    assert_eq!(count_union_find(&pool, t).await, 3);
    assert_eq!(count_live_person_mappings(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_root_with_multiple_parents_and_chain() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Build: D -> A -> R (root), B -> R, C -> R
    let created = db::handle_create(&pool, t, "r").await.unwrap();
    db::handle_alias(&pool, t, "r", "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "d").await.unwrap();
    db::handle_alias(&pool, t, "r", "b").await.unwrap();
    db::handle_alias(&pool, t, "r", "c").await.unwrap();

    // Delete the root
    let resp = db::handle_delete_distinct_id(&pool, t, "r").await.unwrap();
    assert_eq!(resp.distinct_id, "r");
    assert!(!resp.person_deleted);

    // All remaining distinct_ids should resolve to the same person
    for did in &["a", "b", "c", "d"] {
        let resolved = db::resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            resolved.person_uuid, created.person_uuid,
            "{did} should resolve to original person after root deletion"
        );
    }

    // D's chain should still work (D -> A -> ... -> new root)
    let chain_d = walk_chain(&pool, t, "d").await;
    assert!(
        chain_d.len() >= 2,
        "d should traverse at least 2 nodes to reach root"
    );
    assert!(
        chain_d.last().unwrap().person_id.is_some(),
        "chain should terminate at a root"
    );

    assert_eq!(count_distinct_ids(&pool, t).await, 4);
    assert_eq!(count_union_find(&pool, t).await, 4);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_root_no_parents() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "solo").await.unwrap();
    let pid = get_root_person_id(&pool, t, "solo").await.unwrap();

    let resp = db::handle_delete_distinct_id(&pool, t, "solo")
        .await
        .unwrap();
    assert_eq!(resp.distinct_id, "solo");
    assert!(
        resp.person_deleted,
        "person should be soft-deleted when last DID removed"
    );

    assert_eq!(count_distinct_ids(&pool, t).await, 0);
    assert_eq!(count_union_find(&pool, t).await, 0);
    assert!(is_person_deleted(&pool, pid).await);

    let resolved = db::resolve(&pool, t, "solo").await.unwrap();
    assert!(resolved.is_none());

    let _ = created;
    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn unlink_root_after_delete_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Build chain: B -> A (root, person P1)
    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    // Soft-delete the person
    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();

    // Both a and b should resolve to None now (person is soft-deleted)
    assert!(db::resolve(&pool, t, "a").await.unwrap().is_none());
    assert!(db::resolve(&pool, t, "b").await.unwrap().is_none());

    // Trigger check_did on "b" by re-creating it — this should lazily unlink b
    // from the dead chain and give it a fresh person.
    let new_b = db::handle_create(&pool, t, "b").await.unwrap();
    assert_ne!(new_b.person_uuid, pa.person_uuid);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, new_b.person_uuid);

    // a is still orphaned (part of the dead chain)
    assert!(db::resolve(&pool, t, "a").await.unwrap().is_none());

    // Now trigger check_did on "a" by re-creating it
    let new_a = db::handle_create(&pool, t, "a").await.unwrap();
    assert_ne!(new_a.person_uuid, pa.person_uuid);

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, new_a.person_uuid);

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn unlink_root_with_fan_out_after_delete_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Build fan-out: A -> R, B -> R, C -> R  (R is root)
    let pr = db::handle_create(&pool, t, "r").await.unwrap();
    db::handle_alias(&pool, t, "r", "a").await.unwrap();
    db::handle_alias(&pool, t, "r", "b").await.unwrap();
    db::handle_alias(&pool, t, "r", "c").await.unwrap();

    // Soft-delete the person
    db::handle_delete_person(&pool, t, &pr.person_uuid)
        .await
        .unwrap();

    // All should resolve to None
    for did in &["r", "a", "b", "c"] {
        assert!(
            db::resolve(&pool, t, did).await.unwrap().is_none(),
            "{did} should resolve to None after person delete"
        );
    }

    // Trigger lazy unlink on "a" via create — the root R is soft-deleted,
    // so check_did should unlink "a" and give it a fresh person.
    let new_a = db::handle_create(&pool, t, "a").await.unwrap();
    assert_ne!(new_a.person_uuid, pr.person_uuid);

    let resolved_a = db::resolve(&pool, t, "a").await.unwrap().unwrap();
    assert_eq!(resolved_a.person_uuid, new_a.person_uuid);

    // The uniqueness invariant should still hold even though we're in the
    // middle of lazily cleaning up the soft-deleted chain.
    assert_structural_invariants(&pool, t).await;
}
