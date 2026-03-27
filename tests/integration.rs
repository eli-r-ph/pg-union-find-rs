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

    let uf_anon = get_uf_row(&pool, t, "anon").await.unwrap();
    assert!(uf_anon.person_id.is_none(), "anon should not be root");
    assert_eq!(uf_anon.next, Some(uf_primary.current));

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

    let uf_primary = get_uf_row(&pool, t, "primary").await.unwrap();
    assert!(uf_primary.person_id.is_none(), "primary should not be root");
    assert_eq!(uf_primary.next, Some(uf_anon.current));

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

    let uf_anon = get_uf_row(&pool, t, "anon").await.unwrap();
    assert!(uf_anon.person_id.is_none(), "anon should not be root");
    assert_eq!(uf_anon.next, Some(uf_primary.current));

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
    let uf_tgt = get_uf_row(&pool, t, "tgt").await.unwrap();
    assert_eq!(uf_source.next, Some(uf_tgt.current));

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
    }
}

// ===========================================================================
// G. Illegal distinct_id validation
// ===========================================================================

#[tokio::test]
async fn create_rejects_illegal_ids() {
    let pool = test_pool().await;
    let t = next_team_id();

    for bad_id in &[
        "",
        "  ",
        "anonymous",
        "null",
        "undefined",
        "0",
        "NaN",
        "[object Object]",
    ] {
        let result = db::handle_create(&pool, t, bad_id).await;
        assert!(result.is_err(), "expected create to reject '{bad_id}'");
        match result.unwrap_err() {
            DbError::IllegalDistinctId(_) => {}
            other => panic!("expected IllegalDistinctId for '{bad_id}', got: {other}"),
        }
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

#[tokio::test]
async fn quoted_illegal_ids_rejected() {
    let pool = test_pool().await;
    let t = next_team_id();

    for bad_id in &["'null'", "\"null\"", "'anonymous'", "\"undefined\""] {
        let result = db::handle_create(&pool, t, bad_id).await;
        assert!(result.is_err(), "expected create to reject {bad_id}");
        match result.unwrap_err() {
            DbError::IllegalDistinctId(_) => {}
            other => panic!("expected IllegalDistinctId for {bad_id}, got: {other}"),
        }
    }
}
