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

    assert_chain_is_root(&pool, t, "user-1", &resp.person_uuid).await;

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

    assert_chain_is_root(&pool, t, "user-1", &r1.person_uuid).await;

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

    assert_chain_is_root(&pool, t1, "shared-did", &r1.person_uuid).await;
    assert_chain_is_root(&pool, t2, "shared-did", &r2.person_uuid).await;

    assert_all_invariants(&pool, t1).await;
    assert_all_invariants(&pool, t2).await;
}

#[tokio::test]
async fn create_produces_unidentified() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp = db::handle_create(&pool, t, "newbie").await.unwrap();
    assert!(!resp.is_identified);

    assert_chain_is_root(&pool, t, "newbie", &resp.person_uuid).await;

    let chain = collect_chain(&pool, t, "newbie").await;
    let root_person_id = chain.last().unwrap().person_id.unwrap();
    assert!(!is_person_identified(&pool, root_person_id).await);
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

    assert_chain_is_root(&pool, t, "x", &resp.person_uuid).await;
    let chain = collect_chain(&pool, t, "x").await;
    assert!(is_person_identified(&pool, chain[0].person_id.unwrap()).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_target_eq_source_existing() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r1 = db::handle_create(&pool, t, "x").await.unwrap();
    assert_chain_is_root(&pool, t, "x", &r1.person_uuid).await;

    let r2 = db::handle_alias(&pool, t, "x", "x").await.unwrap();
    assert_eq!(r1.person_uuid, r2.person_uuid);
    assert!(r2.is_identified);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);

    assert_chain_is_root(&pool, t, "x", &r1.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case1a_target_exists() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "primary").await.unwrap();
    assert_chain_is_root(&pool, t, "primary", &created.person_uuid).await;

    let aliased = db::handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert_eq!(created.person_uuid, aliased.person_uuid);
    assert!(aliased.is_identified);

    let resolved_anon = db::resolve(&pool, t, "anon").await.unwrap().unwrap();
    assert_eq!(resolved_anon.person_uuid, created.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    assert_chain_is_root(&pool, t, "primary", &created.person_uuid).await;
    assert_chain_matches(&pool, t, "anon", &["anon", "primary"], &created.person_uuid).await;

    let chain = collect_chain(&pool, t, "primary").await;
    assert!(is_person_identified(&pool, chain[0].person_id.unwrap()).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case1b_source_exists() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "anon").await.unwrap();
    assert_chain_is_root(&pool, t, "anon", &created.person_uuid).await;

    let aliased = db::handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert_eq!(created.person_uuid, aliased.person_uuid);
    assert!(aliased.is_identified);

    let resolved_primary = db::resolve(&pool, t, "primary").await.unwrap().unwrap();
    assert_eq!(resolved_primary.person_uuid, created.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    assert_chain_is_root(&pool, t, "anon", &created.person_uuid).await;
    assert_chain_matches(
        &pool,
        t,
        "primary",
        &["primary", "anon"],
        &created.person_uuid,
    )
    .await;

    let chain = collect_chain(&pool, t, "anon").await;
    assert!(is_person_identified(&pool, chain[0].person_id.unwrap()).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2a_same_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    let before_dids = count_distinct_ids(&pool, t).await;
    let before_uf = count_union_find(&pool, t).await;

    // b already linked to a's person — calling alias(a, b) again is a no-op
    let resp = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert!(resp.is_identified);
    assert_eq!(resp.person_uuid, r_a.person_uuid);

    assert_eq!(count_distinct_ids(&pool, t).await, before_dids);
    assert_eq!(count_union_find(&pool, t).await, before_uf);

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2b_both_unidentified_succeeds() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    let person_b = db::handle_create(&pool, t, "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &person_b.person_uuid).await;

    let result = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_eq!(result.person_uuid, person_a.person_uuid);
    assert!(result.is_identified);

    let resolved_b = db::resolve(&pool, t, "b").await.unwrap().unwrap();
    assert_eq!(resolved_b.person_uuid, person_a.person_uuid);

    assert_eq!(count_person_mappings(&pool, t).await, 1);

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2b_source_identified_rejected() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_a = db::handle_create(&pool, t, "a").await.unwrap();
    let r_b = db::handle_create(&pool, t, "b").await.unwrap();

    // Identify B by aliasing B to C
    db::handle_alias(&pool, t, "b", "c").await.unwrap();
    let chain_b = collect_chain(&pool, t, "b").await;
    let pid_b = chain_b.last().unwrap().person_id.unwrap();
    assert!(is_person_identified(&pool, pid_b).await);

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &r_b.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b"], &r_b.person_uuid).await;

    // alias(target=a, source=b): source (b) is identified → reject
    let result = db::handle_alias(&pool, t, "a", "b").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::AlreadyIdentified(_) => {}
        other => panic!("expected AlreadyIdentified, got: {other}"),
    }

    // Graph unchanged
    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &r_b.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b"], &r_b.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2b_target_identified_source_not() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_a = db::handle_create(&pool, t, "a").await.unwrap();
    let r_b = db::handle_create(&pool, t, "b").await.unwrap();

    // Identify A by aliasing A to C
    let alias_resp = db::handle_alias(&pool, t, "a", "c").await.unwrap();
    assert!(alias_resp.is_identified);

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &r_a.person_uuid).await;
    let chain_a = collect_chain(&pool, t, "a").await;
    assert!(is_person_identified(&pool, chain_a[0].person_id.unwrap()).await);

    assert_chain_is_root(&pool, t, "b", &r_b.person_uuid).await;
    let chain_b = collect_chain(&pool, t, "b").await;
    assert!(!is_person_identified(&pool, chain_b[0].person_id.unwrap()).await);

    // alias(target=a, source=b): source (b) is NOT identified → merge succeeds
    let result = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert!(result.is_identified);

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &r_a.person_uuid).await;

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

    assert_chain_is_root(&pool, t, "primary", &resp.person_uuid).await;
    assert_chain_matches(&pool, t, "anon", &["anon", "primary"], &resp.person_uuid).await;

    let chain = collect_chain(&pool, t, "primary").await;
    assert!(is_person_identified(&pool, chain[0].person_id.unwrap()).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_sets_identified() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_x = db::handle_create(&pool, t, "x").await.unwrap();
    let r_y = db::handle_create(&pool, t, "y").await.unwrap();

    assert_chain_is_root(&pool, t, "x", &r_x.person_uuid).await;
    assert_chain_is_root(&pool, t, "y", &r_y.person_uuid).await;

    let chain_x = collect_chain(&pool, t, "x").await;
    let chain_y = collect_chain(&pool, t, "y").await;
    assert!(!is_person_identified(&pool, chain_x[0].person_id.unwrap()).await);
    assert!(!is_person_identified(&pool, chain_y[0].person_id.unwrap()).await);

    let resp = db::handle_alias(&pool, t, "x", "y").await.unwrap();
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "x", &r_x.person_uuid).await;
    assert_chain_matches(&pool, t, "y", &["y", "x"], &r_x.person_uuid).await;

    let chain = collect_chain(&pool, t, "x").await;
    assert!(is_person_identified(&pool, chain[0].person_id.unwrap()).await);
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
    assert_chain_is_root(&pool, t, "tgt", &tgt.person_uuid).await;

    let resp = db::handle_merge(&pool, t, "tgt", &["new-source".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, tgt.person_uuid);
    assert!(resp.is_identified);

    let resolved = db::resolve(&pool, t, "new-source").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, tgt.person_uuid);

    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    assert_chain_is_root(&pool, t, "tgt", &tgt.person_uuid).await;
    assert_chain_matches(
        &pool,
        t,
        "new-source",
        &["new-source", "tgt"],
        &tgt.person_uuid,
    )
    .await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_same_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    let before_uf = count_union_find(&pool, t).await;
    let resp = db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert!(resp.is_identified);
    assert_eq!(resp.person_uuid, r_a.person_uuid);

    assert_eq!(count_union_find(&pool, t).await, before_uf);

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_different_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    let person_b = db::handle_create(&pool, t, "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &person_b.person_uuid).await;

    let old_root_pid = collect_chain(&pool, t, "b").await[0].person_id.unwrap();

    let resp = db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(resp.is_identified);

    assert!(
        !person_exists(&pool, old_root_pid).await,
        "orphaned person_mapping should have been deleted"
    );

    assert_eq!(count_person_mappings(&pool, t).await, 1);

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    let chain = collect_chain(&pool, t, "a").await;
    assert!(is_person_identified(&pool, chain[0].person_id.unwrap()).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_shared_person_cleanup() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    let person_b = db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_alias(&pool, t, "b", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &person_b.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b"], &person_b.person_uuid).await;

    let old_person_b_id = collect_chain(&pool, t, "b").await[0].person_id.unwrap();

    let resp = db::handle_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(resp.is_identified);

    assert!(
        !person_exists(&pool, old_person_b_id).await,
        "person B should be deleted after merge"
    );
    assert_eq!(count_person_mappings(&pool, t).await, 1);

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b", "a"], &person_a.person_uuid).await;

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

    assert_chain_is_root(&pool, t, "tgt", &tgt.person_uuid).await;

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

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &person_a.person_uuid).await;
    assert_chain_matches(
        &pool,
        t,
        "brand-new",
        &["brand-new", "a"],
        &person_a.person_uuid,
    )
    .await;

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

    assert_chain_is_root(&pool, t, "a", &tgt.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_chain_all_ids_follow() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    let person_c = db::handle_create(&pool, t, "c").await.unwrap();

    // Step 1: merge b into a
    db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    // Step 2: merge a into c — b is part of a's tree, so b should also
    // resolve to c's person through the chain b -> a -> c.
    db::handle_merge(&pool, t, "c", &["a".into()])
        .await
        .unwrap();

    assert_chain_is_root(&pool, t, "c", &person_c.person_uuid).await;
    assert_chain_matches(&pool, t, "a", &["a", "c"], &person_c.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a", "c"], &person_c.person_uuid).await;

    assert_eq!(
        count_person_mappings(&pool, t).await,
        1,
        "only c's person should remain"
    );

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_chain_deep_transitive() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    let person_d = db::handle_create(&pool, t, "d").await.unwrap();

    db::handle_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &person_a.person_uuid).await;

    // Merge a into d — all of a's tree (b, c) should follow through a -> d
    db::handle_merge(&pool, t, "d", &["a".into()])
        .await
        .unwrap();

    assert_chain_is_root(&pool, t, "d", &person_d.person_uuid).await;
    assert_chain_matches(&pool, t, "a", &["a", "d"], &person_d.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a", "d"], &person_d.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a", "d"], &person_d.person_uuid).await;

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_sets_identified() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_x = db::handle_create(&pool, t, "x").await.unwrap();
    db::handle_create(&pool, t, "y").await.unwrap();

    let chain_x = collect_chain(&pool, t, "x").await;
    let pid_x = chain_x[0].person_id.unwrap();
    assert!(!is_person_identified(&pool, pid_x).await);

    let resp = db::handle_merge(&pool, t, "x", &["y".into()])
        .await
        .unwrap();
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "x", &r_x.person_uuid).await;
    assert_chain_matches(&pool, t, "y", &["y", "x"], &r_x.person_uuid).await;
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

    assert_chain_is_root(&pool, t, "root-did", &created.person_uuid).await;
}

#[tokio::test]
async fn resolve_one_hop() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "root").await.unwrap();
    db::handle_alias(&pool, t, "root", "leaf").await.unwrap();

    let resolved = db::resolve(&pool, t, "leaf").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, created.person_uuid);

    assert_chain_matches(&pool, t, "leaf", &["leaf", "root"], &created.person_uuid).await;
}

#[tokio::test]
async fn resolve_multi_hop() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();
    db::handle_alias(&pool, t, "b", "c").await.unwrap();
    db::handle_alias(&pool, t, "c", "d").await.unwrap();

    let resolved = db::resolve(&pool, t, "d").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, created.person_uuid);

    assert_chain_matches(&pool, t, "d", &["d", "c", "b", "a"], &created.person_uuid).await;

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
    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;

    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn create_alias_merge_workflow() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    let alias_result = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_eq!(alias_result.person_uuid, person_a.person_uuid);
    assert!(alias_result.is_identified);

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

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

    assert_chain_matches(
        &pool,
        t1,
        "alias-1",
        &["alias-1", "shared"],
        &p1.person_uuid,
    )
    .await;
    assert_chain_is_root(&pool, t2, "shared", &p2.person_uuid).await;

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
        let chain = collect_chain(&pool, t, did).await;
        assert!(!chain.is_empty(), "chain for {did} should not be empty");
        let root = chain.last().unwrap();
        assert!(
            root.person_id.is_some(),
            "chain for {did} should terminate at a root"
        );
        assert!(
            root.next.is_none(),
            "chain for {did}: root should have next=NULL"
        );
        for (i, link) in chain.iter().enumerate() {
            assert!(
                !link.is_deleted,
                "chain for {did}: link {i} ('{}') should not be deleted",
                link.distinct_id
            );
            if i < chain.len() - 1 {
                assert!(
                    link.person_id.is_none(),
                    "chain for {did}: non-root '{}' should have person_id=NULL",
                    link.distinct_id
                );
                assert_eq!(
                    link.next,
                    Some(chain[i + 1].current),
                    "chain for {did}: link {i} ('{}') next mismatch",
                    link.distinct_id
                );
            }
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

    let r_a = db::handle_create(&pool, t, "a").await.unwrap();
    let r_b = db::handle_create(&pool, t, "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &r_b.person_uuid).await;

    db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_case2b_link_structure() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_a = db::handle_create(&pool, t, "a").await.unwrap();
    let r_b = db::handle_create(&pool, t, "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &r_b.person_uuid).await;

    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

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
    let person_b = db::handle_create(&pool, t, "b").await.unwrap();
    let person_c = db::handle_create(&pool, t, "c").await.unwrap();

    db::handle_merge(&pool, t, "c", &["a".into()])
        .await
        .unwrap();

    assert_chain_is_root(&pool, t, "c", &person_c.person_uuid).await;
    assert_chain_matches(&pool, t, "a", &["a", "c"], &person_c.person_uuid).await;

    db::handle_alias(&pool, t, "a", "d-new").await.unwrap();

    assert_chain_matches(
        &pool,
        t,
        "d-new",
        &["d-new", "a", "c"],
        &person_c.person_uuid,
    )
    .await;
    assert_chain_is_root(&pool, t, "b", &person_b.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_reverse_direction() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp1 = db::handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &resp1.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &resp1.person_uuid).await;

    // Reverse: alias(b, a) — both exist, same person → case 2a no-op
    let resp2 = db::handle_alias(&pool, t, "b", "a").await.unwrap();
    assert_eq!(resp1.person_uuid, resp2.person_uuid);
    assert!(resp2.is_identified);

    assert_chain_is_root(&pool, t, "a", &resp1.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &resp1.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_fan_out() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp_ab = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    db::handle_alias(&pool, t, "a", "c").await.unwrap();
    db::handle_alias(&pool, t, "b", "d").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &resp_ab.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &resp_ab.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &resp_ab.person_uuid).await;
    assert_chain_matches(&pool, t, "d", &["d", "b", "a"], &resp_ab.person_uuid).await;

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

    let resp = db::handle_merge(&pool, t, "a", &["b".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn merge_target_plus_others_in_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    let resp = db::handle_merge(&pool, t, "a", &["a".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

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

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    db::handle_merge(&pool, t, "a", &["c".into()])
        .await
        .unwrap();
    db::handle_alias(&pool, t, "a", "d").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "d", &["d", "a"], &person_a.person_uuid).await;

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
    assert_chain_is_root(&pool, t, "x", &r1.person_uuid).await;

    db::handle_alias(&pool, t, "x", "y").await.unwrap();
    assert_chain_is_root(&pool, t, "x", &r1.person_uuid).await;
    assert_chain_matches(&pool, t, "y", &["y", "x"], &r1.person_uuid).await;

    let r2 = db::handle_create(&pool, t, "x").await.unwrap();
    assert_eq!(r1.person_uuid, r2.person_uuid);
    assert!(r2.is_identified);
}

#[tokio::test]
async fn merge_already_identified_target() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp_x = db::handle_alias(&pool, t, "x", "x").await.unwrap();
    assert!(resp_x.is_identified);
    assert_chain_is_root(&pool, t, "x", &resp_x.person_uuid).await;

    db::handle_create(&pool, t, "y").await.unwrap();

    let resp = db::handle_merge(&pool, t, "x", &["y".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, resp_x.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "x", &resp_x.person_uuid).await;
    assert_chain_matches(&pool, t, "y", &["y", "x"], &resp_x.person_uuid).await;

    let chain = collect_chain(&pool, t, "x").await;
    assert!(is_person_identified(&pool, chain[0].person_id.unwrap()).await);

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// M. Orphan cleanup verification
// ===========================================================================

#[tokio::test]
async fn orphan_cleanup_stepwise() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    assert_eq!(count_person_mappings(&pool, t).await, 3);

    db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_eq!(count_person_mappings(&pool, t).await, 2);
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    db::handle_merge(&pool, t, "a", &["c".into()])
        .await
        .unwrap();
    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_chain_matches(&pool, t, "c", &["c", "a"], &r_a.person_uuid).await;

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
    assert_chain_is_root(&pool, t, "a", &created.person_uuid).await;

    let chain = collect_chain(&pool, t, "a").await;
    let pid = chain[0].person_id.unwrap();

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

    assert_chain_is_root(&pool, t, "a", &created.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &created.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &created.person_uuid).await;

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
    assert_chain_is_root(&pool, t, "a", &created.person_uuid).await;
    let chain = collect_chain(&pool, t, "a").await;
    let pid = chain[0].person_id.unwrap();

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

    assert!(person_exists(&pool, pid).await);

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

    assert_chain_matches(&pool, t, "b", &["b", "a"], &created.person_uuid).await;

    let resp = db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();
    assert_eq!(resp.distinct_id, "b");
    assert!(!resp.person_deleted);

    assert_chain_is_root(&pool, t, "a", &created.person_uuid).await;
    assert!(db::resolve(&pool, t, "b").await.unwrap().is_none());

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

    assert_chain_matches(&pool, t, "b", &["b", "a"], &created.person_uuid).await;

    let resp = db::handle_delete_distinct_id(&pool, t, "a").await.unwrap();
    assert_eq!(resp.distinct_id, "a");
    assert!(!resp.person_deleted);

    assert!(db::resolve(&pool, t, "a").await.unwrap().is_none());
    assert_chain_is_root(&pool, t, "b", &created.person_uuid).await;

    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_mid_chain() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();
    db::handle_alias(&pool, t, "b", "c").await.unwrap();

    assert_chain_matches(&pool, t, "c", &["c", "b", "a"], &created.person_uuid).await;

    let resp = db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();
    assert_eq!(resp.distinct_id, "b");
    assert!(!resp.person_deleted);

    assert!(db::resolve(&pool, t, "b").await.unwrap().is_none());

    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    assert_chain_is_root(&pool, t, "a", &created.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &created.person_uuid).await;

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

    assert_chain_matches(&pool, t, "b", &["b", "a"], &created.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &created.person_uuid).await;

    let resp = db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();
    assert_eq!(resp.distinct_id, "b");
    assert!(!resp.person_deleted);

    assert!(db::resolve(&pool, t, "b").await.unwrap().is_none());
    assert_chain_is_root(&pool, t, "a", &created.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &created.person_uuid).await;

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
    assert_chain_is_root(&pool, t, "a", &old.person_uuid).await;

    db::handle_delete_person(&pool, t, &old.person_uuid)
        .await
        .unwrap();

    let new = db::handle_create(&pool, t, "a").await.unwrap();
    assert_ne!(old.person_uuid, new.person_uuid, "should get a new person");

    assert_chain_is_root(&pool, t, "a", &new.person_uuid).await;

    assert_eq!(count_live_person_mappings(&pool, t).await, 1);
    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn create_after_delete_person_no_duplicate_rows() {
    let pool = test_pool().await;
    let t = next_team_id();

    let old = db::handle_create(&pool, t, "a").await.unwrap();
    assert_chain_is_root(&pool, t, "a", &old.person_uuid).await;
    db::handle_delete_person(&pool, t, &old.person_uuid)
        .await
        .unwrap();

    let new = db::handle_create(&pool, t, "a").await.unwrap();
    assert_chain_is_root(&pool, t, "a", &new.person_uuid).await;

    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);
    assert_eq!(count_live_person_mappings(&pool, t).await, 1);

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_with_deleted_target() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    let pb = db::handle_create(&pool, t, "b").await.unwrap();
    assert_chain_is_root(&pool, t, "a", &pa.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &pb.person_uuid).await;

    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();

    let resp = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_eq!(resp.person_uuid, pb.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "b", &pb.person_uuid).await;
    assert_chain_matches(&pool, t, "a", &["a", "b"], &pb.person_uuid).await;

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn alias_with_deleted_source() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    let pb = db::handle_create(&pool, t, "b").await.unwrap();
    assert_chain_is_root(&pool, t, "a", &pa.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &pb.person_uuid).await;

    db::handle_delete_person(&pool, t, &pb.person_uuid)
        .await
        .unwrap();

    let resp = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_eq!(resp.person_uuid, pa.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "a", &pa.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &pa.person_uuid).await;

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

    let resp = db::handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_ne!(resp.person_uuid, pa.person_uuid);
    assert_ne!(resp.person_uuid, pb.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "a", &resp.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &resp.person_uuid).await;

    assert_eq!(count_distinct_ids(&pool, t).await, 2);
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

    let resp = db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_ne!(resp.person_uuid, pa.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "a", &resp.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &resp.person_uuid).await;

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

    let resp = db::handle_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, pa.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "a", &pa.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &pa.person_uuid).await;

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

    assert_chain_is_root(&pool, t, "a", &old.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &old.person_uuid).await;

    db::handle_delete_person(&pool, t, &old.person_uuid)
        .await
        .unwrap();

    let new = db::handle_create(&pool, t, "a").await.unwrap();
    assert_ne!(old.person_uuid, new.person_uuid);

    db::handle_alias(&pool, t, "a", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &new.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &new.person_uuid).await;

    assert_eq!(count_live_person_mappings(&pool, t).await, 1);
    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_then_recreate() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_matches(&pool, t, "b", &["b", "a"], &pa.person_uuid).await;

    db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();

    let pb = db::handle_create(&pool, t, "b").await.unwrap();
    assert_ne!(pa.person_uuid, pb.person_uuid, "b should be a new person");

    assert_chain_is_root(&pool, t, "a", &pa.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &pb.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_person_merge_recovery() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    let pb = db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_alias(&pool, t, "a", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &pa.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &pa.person_uuid).await;

    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();

    let resp = db::handle_merge(&pool, t, "b", &["a".into(), "c".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, pb.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "b", &pb.person_uuid).await;
    assert_chain_matches(&pool, t, "a", &["a", "b"], &pb.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b"], &pb.person_uuid).await;

    assert_structural_invariants(&pool, t).await;
}

// ===========================================================================
// R. Unique person_id invariant — root unlink with multiple parents
// ===========================================================================

#[tokio::test]
async fn delete_did_root_with_multiple_parents() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "r").await.unwrap();
    db::handle_alias(&pool, t, "r", "a").await.unwrap();
    db::handle_alias(&pool, t, "r", "b").await.unwrap();
    db::handle_alias(&pool, t, "r", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "r", &created.person_uuid).await;
    assert_chain_matches(&pool, t, "a", &["a", "r"], &created.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "r"], &created.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "r"], &created.person_uuid).await;

    let root_uf = get_uf_row(&pool, t, "r").await.unwrap();
    let parents_before = collect_parents(&pool, t, root_uf.current).await;
    assert_eq!(parents_before.len(), 3, "r should have 3 parents: a, b, c");

    let resp = db::handle_delete_distinct_id(&pool, t, "r").await.unwrap();
    assert_eq!(resp.distinct_id, "r");
    assert!(!resp.person_deleted);

    assert!(db::resolve(&pool, t, "r").await.unwrap().is_none());

    // Structural verification: exactly one former parent became root.
    // Which parent was promoted is nondeterministic -- we verify invariants.
    let mut promoted_did: Option<String> = None;
    for did in &["a", "b", "c"] {
        let chain = collect_chain(&pool, t, did).await;
        let root = chain.last().unwrap();
        assert!(
            root.person_id.is_some(),
            "{did} chain should terminate at a root"
        );
        assert!(
            root.next.is_none(),
            "{did} chain root should have next=NULL"
        );
        let resolved = db::resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            resolved.person_uuid, created.person_uuid,
            "{did} should resolve to original person"
        );
        if chain.len() == 1 {
            assert!(
                promoted_did.is_none(),
                "multiple nodes appear to be root: {} and {did}",
                promoted_did.as_deref().unwrap_or("?")
            );
            promoted_did = Some(did.to_string());
        }
    }
    assert!(
        promoted_did.is_some(),
        "one parent should have been promoted to root"
    );

    assert_eq!(count_distinct_ids(&pool, t).await, 3);
    assert_eq!(count_union_find(&pool, t).await, 3);
    assert_eq!(count_live_person_mappings(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_root_with_multiple_parents_and_chain() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "r").await.unwrap();
    db::handle_alias(&pool, t, "r", "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "d").await.unwrap();
    db::handle_alias(&pool, t, "r", "b").await.unwrap();
    db::handle_alias(&pool, t, "r", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "r", &created.person_uuid).await;
    assert_chain_matches(&pool, t, "d", &["d", "a", "r"], &created.person_uuid).await;

    let resp = db::handle_delete_distinct_id(&pool, t, "r").await.unwrap();
    assert_eq!(resp.distinct_id, "r");
    assert!(!resp.person_deleted);

    // Verify structural consistency: all DIDs resolve to original person,
    // exactly one former parent became root, d's chain still terminates.
    for did in &["a", "b", "c", "d"] {
        let chain = collect_chain(&pool, t, did).await;
        let root = chain.last().unwrap();
        assert!(
            root.person_id.is_some(),
            "{did} chain should terminate at root"
        );
        assert!(
            root.next.is_none(),
            "{did} chain root should have next=NULL"
        );
        let resolved = db::resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            resolved.person_uuid, created.person_uuid,
            "{did} should resolve to original person after root deletion"
        );
    }

    let chain_d = collect_chain(&pool, t, "d").await;
    assert!(
        chain_d.len() >= 2,
        "d should traverse at least 2 nodes to reach root"
    );
    assert_eq!(
        chain_d[1].distinct_id, "a",
        "d's second link should still be 'a' (d -> a link was never disrupted)"
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
    assert_chain_is_root(&pool, t, "solo", &created.person_uuid).await;
    let chain = collect_chain(&pool, t, "solo").await;
    let pid = chain[0].person_id.unwrap();

    let resp = db::handle_delete_distinct_id(&pool, t, "solo")
        .await
        .unwrap();
    assert_eq!(resp.distinct_id, "solo");
    assert!(resp.person_deleted);

    assert_eq!(count_distinct_ids(&pool, t).await, 0);
    assert_eq!(count_union_find(&pool, t).await, 0);
    assert!(is_person_deleted(&pool, pid).await);

    assert!(db::resolve(&pool, t, "solo").await.unwrap().is_none());

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn unlink_root_after_delete_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_matches(&pool, t, "b", &["b", "a"], &pa.person_uuid).await;

    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();

    assert!(db::resolve(&pool, t, "a").await.unwrap().is_none());
    assert!(db::resolve(&pool, t, "b").await.unwrap().is_none());

    let new_b = db::handle_create(&pool, t, "b").await.unwrap();
    assert_ne!(new_b.person_uuid, pa.person_uuid);
    assert_chain_is_root(&pool, t, "b", &new_b.person_uuid).await;

    assert!(db::resolve(&pool, t, "a").await.unwrap().is_none());

    let new_a = db::handle_create(&pool, t, "a").await.unwrap();
    assert_ne!(new_a.person_uuid, pa.person_uuid);
    assert_chain_is_root(&pool, t, "a", &new_a.person_uuid).await;

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn unlink_root_with_fan_out_after_delete_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pr = db::handle_create(&pool, t, "r").await.unwrap();
    db::handle_alias(&pool, t, "r", "a").await.unwrap();
    db::handle_alias(&pool, t, "r", "b").await.unwrap();
    db::handle_alias(&pool, t, "r", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "r", &pr.person_uuid).await;
    assert_chain_matches(&pool, t, "a", &["a", "r"], &pr.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "r"], &pr.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "r"], &pr.person_uuid).await;

    db::handle_delete_person(&pool, t, &pr.person_uuid)
        .await
        .unwrap();

    for did in &["r", "a", "b", "c"] {
        assert!(
            db::resolve(&pool, t, did).await.unwrap().is_none(),
            "{did} should resolve to None after person delete"
        );
    }

    let new_a = db::handle_create(&pool, t, "a").await.unwrap();
    assert_ne!(new_a.person_uuid, pr.person_uuid);
    assert_chain_is_root(&pool, t, "a", &new_a.person_uuid).await;

    assert_structural_invariants(&pool, t).await;
}
