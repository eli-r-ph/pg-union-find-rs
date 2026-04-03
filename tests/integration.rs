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

    let resolved = resolve(&pool, t, "user-1").await.unwrap();
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

    let resp = handle_alias(&pool, t, "x", "x").await.unwrap();
    assert!(!resp.person_uuid.is_empty());
    assert!(resp.is_identified);

    let resolved = resolve(&pool, t, "x").await.unwrap();
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

    let r2 = handle_alias(&pool, t, "x", "x").await.unwrap();
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

    let aliased = handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert_eq!(created.person_uuid, aliased.person_uuid);
    assert!(aliased.is_identified);

    let resolved_anon = resolve(&pool, t, "anon").await.unwrap().unwrap();
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

    let aliased = handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert_eq!(created.person_uuid, aliased.person_uuid);
    assert!(aliased.is_identified);

    let resolved_primary = resolve(&pool, t, "primary").await.unwrap().unwrap();
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
    handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    let before_dids = count_distinct_ids(&pool, t).await;
    let before_uf = count_union_find(&pool, t).await;

    // b already linked to a's person — calling alias(a, b) again is a no-op
    let resp = handle_alias(&pool, t, "a", "b").await.unwrap();
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

    let result = handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_eq!(result.person_uuid, person_a.person_uuid);
    assert!(result.is_identified);

    let resolved_b = resolve(&pool, t, "b").await.unwrap().unwrap();
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
    handle_alias(&pool, t, "b", "c").await.unwrap();
    let chain_b = collect_chain(&pool, t, "b").await;
    let pid_b = chain_b.last().unwrap().person_id.unwrap();
    assert!(is_person_identified(&pool, pid_b).await);

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &r_b.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b"], &r_b.person_uuid).await;

    // alias(target=a, source=b): source (b) is identified → reject
    let result = handle_alias(&pool, t, "a", "b").await;
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
    let alias_resp = handle_alias(&pool, t, "a", "c").await.unwrap();
    assert!(alias_resp.is_identified);

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &r_a.person_uuid).await;
    let chain_a = collect_chain(&pool, t, "a").await;
    assert!(is_person_identified(&pool, chain_a[0].person_id.unwrap()).await);

    assert_chain_is_root(&pool, t, "b", &r_b.person_uuid).await;
    let chain_b = collect_chain(&pool, t, "b").await;
    assert!(!is_person_identified(&pool, chain_b[0].person_id.unwrap()).await);

    // alias(target=a, source=b): source (b) is NOT identified → merge succeeds
    let result = handle_alias(&pool, t, "a", "b").await.unwrap();
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

    let resp = handle_alias(&pool, t, "primary", "anon").await.unwrap();
    assert!(!resp.person_uuid.is_empty());
    assert!(resp.is_identified);

    let resolved_primary = resolve(&pool, t, "primary").await.unwrap().unwrap();
    let resolved_anon = resolve(&pool, t, "anon").await.unwrap().unwrap();
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

    let resp = handle_alias(&pool, t, "x", "y").await.unwrap();
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

    let result = handle_merge(&pool, t, "nonexistent", &["a".into()]).await;
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

    let resp = handle_merge(&pool, t, "tgt", &["new-source".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, tgt.person_uuid);
    assert!(resp.is_identified);

    let resolved = resolve(&pool, t, "new-source").await.unwrap().unwrap();
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
    handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    let before_uf = count_union_find(&pool, t).await;
    let resp = handle_merge(&pool, t, "a", &["b".into()]).await.unwrap();
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

    let resp = handle_merge(&pool, t, "a", &["b".into()]).await.unwrap();
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
    handle_alias(&pool, t, "b", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &person_b.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b"], &person_b.person_uuid).await;

    let old_person_b_id = collect_chain(&pool, t, "b").await[0].person_id.unwrap();

    let resp = handle_merge(&pool, t, "a", &["b".into(), "c".into()])
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
    let resp = handle_merge(&pool, t, "tgt", &[]).await.unwrap();
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
    handle_alias(&pool, t, "a", "c").await.unwrap();

    let resp = handle_merge(&pool, t, "a", &["b".into(), "c".into(), "brand-new".into()])
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
    let resp = handle_merge(&pool, t, "a", &["a".into()]).await.unwrap();
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
    handle_merge(&pool, t, "a", &["b".into()]).await.unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    // Step 2: merge a into c — b is part of a's tree, so b should also
    // resolve to c's person through the chain b -> a -> c.
    handle_merge(&pool, t, "c", &["a".into()]).await.unwrap();

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

    handle_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &person_a.person_uuid).await;

    // Merge a into d — all of a's tree (b, c) should follow through a -> d
    handle_merge(&pool, t, "d", &["a".into()]).await.unwrap();

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

    let resp = handle_merge(&pool, t, "x", &["y".into()]).await.unwrap();
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

    let resolved = resolve(&pool, t, "ghost").await.unwrap();
    assert!(resolved.is_none());
}

#[tokio::test]
async fn resolve_root() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "root-did").await.unwrap();
    let resolved = resolve(&pool, t, "root-did").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, created.person_uuid);

    assert_chain_is_root(&pool, t, "root-did", &created.person_uuid).await;
}

#[tokio::test]
async fn resolve_one_hop() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "root").await.unwrap();
    handle_alias(&pool, t, "root", "leaf").await.unwrap();

    let resolved = resolve(&pool, t, "leaf").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, created.person_uuid);

    assert_chain_matches(&pool, t, "leaf", &["leaf", "root"], &created.person_uuid).await;
}

#[tokio::test]
async fn resolve_multi_hop() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    handle_alias(&pool, t, "a", "b").await.unwrap();
    handle_alias(&pool, t, "b", "c").await.unwrap();
    handle_alias(&pool, t, "c", "d").await.unwrap();

    let resolved = resolve(&pool, t, "d").await.unwrap().unwrap();
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

    handle_alias(&pool, t, "a", "b").await.unwrap();

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

    let alias_result = handle_alias(&pool, t, "a", "b").await.unwrap();
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

    handle_alias(&pool, t1, "shared", "alias-1").await.unwrap();

    let resolved_t2 = resolve(&pool, t2, "alias-1").await.unwrap();
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

    handle_alias(&pool, t, "p1", "a1").await.unwrap();
    handle_alias(&pool, t, "p1", "a2").await.unwrap();
    handle_alias(&pool, t, "p2", "b1").await.unwrap();

    handle_alias(&pool, t, "fresh-src", "fresh-dest")
        .await
        .unwrap();

    handle_merge(&pool, t, "p1", &["p2".into()]).await.unwrap();

    handle_merge(&pool, t, "p1", &["p3".into(), "brand-new".into()])
        .await
        .unwrap();

    let p1_uuid = resolve(&pool, t, "p1").await.unwrap().unwrap().person_uuid;
    for did in &["p1", "p2", "p3", "a1", "a2", "b1", "brand-new"] {
        let r = resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            r.person_uuid, p1_uuid,
            "expected {did} to resolve to p1's person"
        );
    }

    let fresh_uuid = resolve(&pool, t, "fresh-src")
        .await
        .unwrap()
        .unwrap()
        .person_uuid;
    let fresh_dest_uuid = resolve(&pool, t, "fresh-dest")
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

    let result = handle_alias(&pool, t, "legit", "null").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::IllegalDistinctId(_) => {}
        other => panic!("expected IllegalDistinctId, got: {other}"),
    }

    let result = handle_alias(&pool, t, "guest", "legit").await;
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

    let result = handle_merge(&pool, t, "anonymous", &[]).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::IllegalDistinctId(_) => {}
        other => panic!("expected IllegalDistinctId, got: {other}"),
    }

    let result = handle_merge(&pool, t, "legit", &["undefined".into()]).await;
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

    handle_merge(&pool, t, "a", &["b".into()]).await.unwrap();

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

    handle_alias(&pool, t, "a", "b").await.unwrap();

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

    handle_merge(&pool, t, "c", &["a".into()]).await.unwrap();

    assert_chain_is_root(&pool, t, "c", &person_c.person_uuid).await;
    assert_chain_matches(&pool, t, "a", &["a", "c"], &person_c.person_uuid).await;

    handle_alias(&pool, t, "a", "d-new").await.unwrap();

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

    let resp1 = handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &resp1.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &resp1.person_uuid).await;

    // Reverse: alias(b, a) — both exist, same person → case 2a no-op
    let resp2 = handle_alias(&pool, t, "b", "a").await.unwrap();
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

    let resp_ab = handle_alias(&pool, t, "a", "b").await.unwrap();
    handle_alias(&pool, t, "a", "c").await.unwrap();
    handle_alias(&pool, t, "b", "d").await.unwrap();

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

    let resp = handle_merge(&pool, t, "a", &["b".into(), "b".into()])
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

    let resp = handle_merge(&pool, t, "a", &["a".into(), "b".into()])
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
    handle_alias(&pool, t, "a", "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    handle_merge(&pool, t, "a", &["c".into()]).await.unwrap();
    handle_alias(&pool, t, "a", "d").await.unwrap();

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

    handle_alias(&pool, t, "x", "y").await.unwrap();
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

    let resp_x = handle_alias(&pool, t, "x", "x").await.unwrap();
    assert!(resp_x.is_identified);
    assert_chain_is_root(&pool, t, "x", &resp_x.person_uuid).await;

    db::handle_create(&pool, t, "y").await.unwrap();

    let resp = handle_merge(&pool, t, "x", &["y".into()]).await.unwrap();
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

    handle_merge(&pool, t, "a", &["b".into()]).await.unwrap();
    assert_eq!(count_person_mappings(&pool, t).await, 2);
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    handle_merge(&pool, t, "a", &["c".into()]).await.unwrap();
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

    let resolved = resolve(&pool, t, "a").await.unwrap();
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
    handle_alias(&pool, t, "a", "b").await.unwrap();
    handle_alias(&pool, t, "a", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &created.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &created.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &created.person_uuid).await;

    db::handle_delete_person(&pool, t, &created.person_uuid)
        .await
        .unwrap();

    for did in &["a", "b", "c"] {
        let resolved = resolve(&pool, t, did).await.unwrap();
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

    let resolved_t1 = resolve(&pool, t1, "shared").await.unwrap();
    assert!(resolved_t1.is_none(), "team1 person should be deleted");

    let resolved_t2 = resolve(&pool, t2, "shared").await.unwrap();
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

    let resolved = resolve(&pool, t, "a").await.unwrap();
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
    handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_matches(&pool, t, "b", &["b", "a"], &created.person_uuid).await;

    let resp = db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();
    assert_eq!(resp.distinct_id, "b");
    assert!(!resp.person_deleted);

    assert_chain_is_root(&pool, t, "a", &created.person_uuid).await;
    assert!(resolve(&pool, t, "b").await.unwrap().is_none());

    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn delete_did_root() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_matches(&pool, t, "b", &["b", "a"], &created.person_uuid).await;

    let resp = db::handle_delete_distinct_id(&pool, t, "a").await.unwrap();
    assert_eq!(resp.distinct_id, "a");
    assert!(!resp.person_deleted);

    assert!(resolve(&pool, t, "a").await.unwrap().is_none());
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
    handle_alias(&pool, t, "a", "b").await.unwrap();
    handle_alias(&pool, t, "b", "c").await.unwrap();

    assert_chain_matches(&pool, t, "c", &["c", "b", "a"], &created.person_uuid).await;

    let resp = db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();
    assert_eq!(resp.distinct_id, "b");
    assert!(!resp.person_deleted);

    assert!(resolve(&pool, t, "b").await.unwrap().is_none());

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
    handle_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();

    assert_chain_matches(&pool, t, "b", &["b", "a"], &created.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &created.person_uuid).await;

    let resp = db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();
    assert_eq!(resp.distinct_id, "b");
    assert!(!resp.person_deleted);

    assert!(resolve(&pool, t, "b").await.unwrap().is_none());
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

    let resp = handle_alias(&pool, t, "a", "b").await.unwrap();
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

    let resp = handle_alias(&pool, t, "a", "b").await.unwrap();
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

    let resp = handle_alias(&pool, t, "a", "b").await.unwrap();
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

    let resp = handle_merge(&pool, t, "a", &["b".into()]).await.unwrap();
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

    let resp = handle_merge(&pool, t, "a", &["b".into()]).await.unwrap();
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

    let resolved = resolve(&pool, t, "a").await.unwrap();
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
    handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &old.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &old.person_uuid).await;

    db::handle_delete_person(&pool, t, &old.person_uuid)
        .await
        .unwrap();

    let new = db::handle_create(&pool, t, "a").await.unwrap();
    assert_ne!(old.person_uuid, new.person_uuid);

    handle_alias(&pool, t, "a", "c").await.unwrap();

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
    handle_alias(&pool, t, "a", "b").await.unwrap();

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
    handle_alias(&pool, t, "a", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &pa.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &pa.person_uuid).await;

    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();

    let resp = handle_merge(&pool, t, "b", &["a".into(), "c".into()])
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
    handle_alias(&pool, t, "r", "a").await.unwrap();
    handle_alias(&pool, t, "r", "b").await.unwrap();
    handle_alias(&pool, t, "r", "c").await.unwrap();

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

    assert!(resolve(&pool, t, "r").await.unwrap().is_none());

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
        let resolved = resolve(&pool, t, did).await.unwrap().unwrap();
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
    handle_alias(&pool, t, "r", "a").await.unwrap();
    handle_alias(&pool, t, "a", "d").await.unwrap();
    handle_alias(&pool, t, "r", "b").await.unwrap();
    handle_alias(&pool, t, "r", "c").await.unwrap();

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
        let resolved = resolve(&pool, t, did).await.unwrap().unwrap();
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

    assert!(resolve(&pool, t, "solo").await.unwrap().is_none());

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn unlink_root_after_delete_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_matches(&pool, t, "b", &["b", "a"], &pa.person_uuid).await;

    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();

    assert!(resolve(&pool, t, "a").await.unwrap().is_none());
    assert!(resolve(&pool, t, "b").await.unwrap().is_none());

    let new_b = db::handle_create(&pool, t, "b").await.unwrap();
    assert_ne!(new_b.person_uuid, pa.person_uuid);
    assert_chain_is_root(&pool, t, "b", &new_b.person_uuid).await;

    assert!(resolve(&pool, t, "a").await.unwrap().is_none());

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
    handle_alias(&pool, t, "r", "a").await.unwrap();
    handle_alias(&pool, t, "r", "b").await.unwrap();
    handle_alias(&pool, t, "r", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "r", &pr.person_uuid).await;
    assert_chain_matches(&pool, t, "a", &["a", "r"], &pr.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "r"], &pr.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "r"], &pr.person_uuid).await;

    db::handle_delete_person(&pool, t, &pr.person_uuid)
        .await
        .unwrap();

    for did in &["r", "a", "b", "c"] {
        assert!(
            resolve(&pool, t, did).await.unwrap().is_none(),
            "{did} should resolve to None after person delete"
        );
    }

    let new_a = db::handle_create(&pool, t, "a").await.unwrap();
    assert_ne!(new_a.person_uuid, pr.person_uuid);
    assert_chain_is_root(&pool, t, "a", &new_a.person_uuid).await;

    assert_structural_invariants(&pool, t).await;
}

// ===========================================================================
// S. Path compression
// ===========================================================================

#[tokio::test]
async fn compress_deep_alias_chain() {
    let pool = test_pool().await;
    let t = next_team_id();

    let root = db::handle_create(&pool, t, "n0").await.unwrap();

    for i in 1..=25 {
        let prev = format!("n{}", i - 1);
        let curr = format!("n{i}");
        handle_alias(&pool, t, &prev, &curr).await.unwrap();
    }

    let depth_before = chain_depth(&pool, t, "n25").await;
    assert!(
        depth_before >= 25,
        "chain should be at least 25 deep before compression, got {depth_before}"
    );

    db::handle_compress_path(&pool, t, "n25", 5).await.unwrap();

    for i in 1..=25 {
        let did = format!("n{i}");
        let depth = chain_depth(&pool, t, &did).await;
        assert_eq!(
            depth, 1,
            "{did} should be at depth 1 after compression, got {depth}"
        );
        let resolved = resolve(&pool, t, &did).await.unwrap().unwrap();
        assert_eq!(
            resolved.person_uuid, root.person_uuid,
            "{did} should still resolve to the original person"
        );
    }

    assert_chain_is_root(&pool, t, "n0", &root.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn compress_deep_merge_chain() {
    let pool = test_pool().await;
    let t = next_team_id();

    let root = db::handle_create(&pool, t, "m0").await.unwrap();

    for i in 1..=25 {
        let prev = format!("m{}", i - 1);
        let curr = format!("m{i}");
        db::handle_create(&pool, t, &curr).await.unwrap();
        handle_merge(&pool, t, &prev, std::slice::from_ref(&curr))
            .await
            .unwrap();
    }

    let depth_before = chain_depth(&pool, t, "m25").await;
    assert!(depth_before >= 25);

    db::handle_compress_path(&pool, t, "m25", 5).await.unwrap();

    for i in 1..=25 {
        let did = format!("m{i}");
        let depth = chain_depth(&pool, t, &did).await;
        assert_eq!(
            depth, 1,
            "{did} should be at depth 1 after compression, got {depth}"
        );
    }

    for i in 0..=25 {
        let did = format!("m{i}");
        let resolved = resolve(&pool, t, &did).await.unwrap().unwrap();
        assert_eq!(
            resolved.person_uuid, root.person_uuid,
            "{did} should still resolve to the original person"
        );
    }

    assert_chain_is_root(&pool, t, "m0", &root.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn compress_noop_short_chain() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "a").await.unwrap();
    handle_alias(&pool, t, "a", "b").await.unwrap();
    handle_alias(&pool, t, "b", "c").await.unwrap();
    handle_alias(&pool, t, "c", "d").await.unwrap();
    handle_alias(&pool, t, "d", "e").await.unwrap();

    let depth_before = chain_depth(&pool, t, "e").await;
    assert_eq!(depth_before, 4);

    let chain_before = collect_chain(&pool, t, "e").await;

    db::handle_compress_path(&pool, t, "e", 20).await.unwrap();

    let chain_after = collect_chain(&pool, t, "e").await;
    assert_eq!(
        chain_before.len(),
        chain_after.len(),
        "chain should be unchanged when depth < threshold"
    );
    for (before, after) in chain_before.iter().zip(chain_after.iter()) {
        assert_eq!(before.current, after.current);
        assert_eq!(before.next, after.next);
    }

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn compress_bail_out_already_compressed() {
    let pool = test_pool().await;
    let t = next_team_id();

    let root = db::handle_create(&pool, t, "r0").await.unwrap();

    for i in 1..=30 {
        let prev = format!("r{}", i - 1);
        let curr = format!("r{i}");
        handle_alias(&pool, t, &prev, &curr).await.unwrap();
    }

    db::handle_compress_path(&pool, t, "r30", 5).await.unwrap();
    let depth_after_first = chain_depth(&pool, t, "r30").await;
    assert_eq!(depth_after_first, 1);

    let chain_snapshot = collect_chain(&pool, t, "r30").await;

    db::handle_compress_path(&pool, t, "r30", 5).await.unwrap();

    let chain_after_second = collect_chain(&pool, t, "r30").await;
    assert_eq!(chain_snapshot.len(), chain_after_second.len());
    for (a, b) in chain_snapshot.iter().zip(chain_after_second.iter()) {
        assert_eq!(a.current, b.current);
        assert_eq!(a.next, b.next);
    }

    let resolved = resolve(&pool, t, "r30").await.unwrap().unwrap();
    assert_eq!(resolved.person_uuid, root.person_uuid);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn compress_preserves_soft_deleted_root() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "s0").await.unwrap();
    for i in 1..=25 {
        let prev = format!("s{}", i - 1);
        let curr = format!("s{i}");
        handle_alias(&pool, t, &prev, &curr).await.unwrap();
    }

    db::handle_delete_person(&pool, t, &created.person_uuid)
        .await
        .unwrap();

    db::handle_compress_path(&pool, t, "s25", 5).await.unwrap();

    for i in 0..=25 {
        let did = format!("s{i}");
        let resolved = resolve(&pool, t, &did).await.unwrap();
        assert!(
            resolved.is_none(),
            "{did} should still resolve to None after compression of deleted chain"
        );
    }

    // Lazy unlink must still work: writing to an orphaned node after compression
    // should create a fresh person and re-root the node.
    let recovered = db::handle_create(&pool, t, "s25").await.unwrap();
    assert_ne!(
        recovered.person_uuid, created.person_uuid,
        "s25 should get a new person after recovery"
    );
    assert_chain_is_root(&pool, t, "s25", &recovered.person_uuid).await;

    // A sibling from the old compressed chain should also be recoverable.
    let recovered_mid = db::handle_create(&pool, t, "s10").await.unwrap();
    assert_ne!(recovered_mid.person_uuid, created.person_uuid);
    assert_chain_is_root(&pool, t, "s10", &recovered_mid.person_uuid).await;

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn compress_idempotent() {
    let pool = test_pool().await;
    let t = next_team_id();

    let root = db::handle_create(&pool, t, "i0").await.unwrap();

    for i in 1..=25 {
        let prev = format!("i{}", i - 1);
        let curr = format!("i{i}");
        handle_alias(&pool, t, &prev, &curr).await.unwrap();
    }

    db::handle_compress_path(&pool, t, "i25", 5).await.unwrap();

    let chain_first = collect_chain(&pool, t, "i25").await;

    db::handle_compress_path(&pool, t, "i25", 5).await.unwrap();
    db::handle_compress_path(&pool, t, "i25", 5).await.unwrap();

    let chain_third = collect_chain(&pool, t, "i25").await;
    assert_eq!(chain_first.len(), chain_third.len());
    for (a, b) in chain_first.iter().zip(chain_third.iter()) {
        assert_eq!(a.current, b.current);
        assert_eq!(a.next, b.next);
    }

    for i in 0..=25 {
        let did = format!("i{i}");
        let resolved = resolve(&pool, t, &did).await.unwrap().unwrap();
        assert_eq!(resolved.person_uuid, root.person_uuid);
    }

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn compress_nonexistent_did_is_noop() {
    let pool = test_pool().await;
    let t = next_team_id();

    let result = db::handle_compress_path(&pool, t, "no-such-did", 5).await;
    assert!(
        result.is_ok(),
        "compressing nonexistent DID should be a no-op"
    );
}

// Tests for CompressHint return from mutations were removed: mutations no
// longer return compression hints (read-side lazy compression only).
// Removed: alias_returns_compress_hint_above_threshold,
//          alias_no_compress_hint_below_threshold,
//          merge_returns_compress_hint_above_threshold.

#[tokio::test]
async fn compress_mid_chain_only_flattens_below() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Build chain: n25 -> n24 -> ... -> n1 -> n0 (root)
    let root = db::handle_create(&pool, t, "n0").await.unwrap();
    for i in 1..=25 {
        let prev = format!("n{}", i - 1);
        let curr = format!("n{i}");
        handle_alias(&pool, t, &prev, &curr).await.unwrap();
    }

    // Compress from n10 (mid-chain). Should flatten n10..n1 to point at n0,
    // but leave n25..n11 pointing at their original next nodes.
    db::handle_compress_path(&pool, t, "n10", 5).await.unwrap();

    // n10 and below should be at depth 1
    for i in 1..=10 {
        let did = format!("n{i}");
        let depth = chain_depth(&pool, t, &did).await;
        assert_eq!(
            depth, 1,
            "{did} (below compress point) should be at depth 1, got {depth}"
        );
    }

    // n11..n25 still form a chain above the compress point.
    // n11.next was originally n10. That's unchanged -- compression only
    // touched nodes in the walk from n10 to root. n11 was NOT in that walk.
    // So the chain from n25 is: n25 -> n24 -> ... -> n11 -> n10 -> n0
    // (n10 was compressed to point directly at n0, shortening the chain)
    let chain_n25 = collect_chain(&pool, t, "n25").await;
    let dids: Vec<&str> = chain_n25.iter().map(|l| l.distinct_id.as_str()).collect();

    assert_eq!(
        *dids.last().unwrap(),
        "n0",
        "chain should still terminate at root n0"
    );
    // n25 through n11 should be in order (15 nodes), then n10 -> n0 (compressed)
    assert_eq!(
        chain_n25.len(),
        17,
        "chain from n25 should be 15 uncompressed (n25..n11) + n10 + n0 = 17, got {:?}",
        dids
    );

    // All nodes still resolve correctly
    for i in 0..=25 {
        let did = format!("n{i}");
        let resolved = resolve(&pool, t, &did).await.unwrap().unwrap();
        assert_eq!(resolved.person_uuid, root.person_uuid);
    }

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn compress_fan_out_preserves_siblings() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Build a tree:
    //   root
    //    ├── branch_a (depth 25 via sequential aliases)
    //    └── branch_b (depth 3, short)
    let root_resp = db::handle_create(&pool, t, "root").await.unwrap();

    // Build branch A: a1 -> a2 -> ... -> a25 -> root
    handle_alias(&pool, t, "root", "a1").await.unwrap();
    for i in 2..=25 {
        let prev = format!("a{}", i - 1);
        let curr = format!("a{i}");
        handle_alias(&pool, t, &prev, &curr).await.unwrap();
    }

    // Build branch B: b1 -> b2 -> b3 -> root
    handle_alias(&pool, t, "root", "b1").await.unwrap();
    handle_alias(&pool, t, "b1", "b2").await.unwrap();
    handle_alias(&pool, t, "b2", "b3").await.unwrap();

    let depth_a = chain_depth(&pool, t, "a25").await;
    let depth_b = chain_depth(&pool, t, "b3").await;
    assert!(depth_a >= 25, "branch A should be deep, got {depth_a}");
    assert_eq!(depth_b, 3, "branch B should be 3 deep");

    // Snapshot branch B before compression
    let chain_b_before = collect_chain(&pool, t, "b3").await;

    // Compress branch A only
    db::handle_compress_path(&pool, t, "a25", 5).await.unwrap();

    // Branch A nodes should all be at depth 1
    for i in 1..=25 {
        let did = format!("a{i}");
        let depth = chain_depth(&pool, t, &did).await;
        assert_eq!(depth, 1, "branch A {did} should be at depth 1, got {depth}");
    }

    // Branch B must be completely untouched
    let chain_b_after = collect_chain(&pool, t, "b3").await;
    assert_eq!(
        chain_b_before.len(),
        chain_b_after.len(),
        "branch B chain length should be unchanged"
    );
    for (before, after) in chain_b_before.iter().zip(chain_b_after.iter()) {
        assert_eq!(
            before.current, after.current,
            "branch B node PKs should be unchanged"
        );
        assert_eq!(
            before.next, after.next,
            "branch B next pointers should be unchanged"
        );
    }

    // All nodes still resolve to the same person
    for did in &["root", "a1", "a25", "b1", "b3"] {
        let resolved = resolve(&pool, t, did).await.unwrap().unwrap();
        assert_eq!(
            resolved.person_uuid, root_resp.person_uuid,
            "{did} should resolve to root's person"
        );
    }

    assert_all_invariants(&pool, t).await;
}

// ===========================================================================
// Batched merge — mirrors of all /merge tests above
// ===========================================================================

#[tokio::test]
async fn batched_merge_target_not_found() {
    let pool = test_pool().await;
    let t = next_team_id();

    let result = handle_batched_merge(&pool, t, "nonexistent", &["a".into()]).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::NotFound(_) => {}
        other => panic!("expected NotFound, got: {other}"),
    }
}

#[tokio::test]
async fn batched_merge_source_not_found() {
    let pool = test_pool().await;
    let t = next_team_id();

    let tgt = db::handle_create(&pool, t, "tgt").await.unwrap();
    assert_chain_is_root(&pool, t, "tgt", &tgt.person_uuid).await;

    let resp = handle_batched_merge(&pool, t, "tgt", &["new-source".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, tgt.person_uuid);
    assert!(resp.is_identified);

    let resolved = resolve(&pool, t, "new-source").await.unwrap().unwrap();
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
async fn batched_merge_same_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_a = db::handle_create(&pool, t, "a").await.unwrap();
    handle_alias(&pool, t, "a", "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    let before_uf = count_union_find(&pool, t).await;
    let resp = handle_batched_merge(&pool, t, "a", &["b".into()])
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
async fn batched_merge_different_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    let person_b = db::handle_create(&pool, t, "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &person_b.person_uuid).await;

    let old_root_pid = collect_chain(&pool, t, "b").await[0].person_id.unwrap();

    let resp = handle_batched_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(resp.is_identified);

    assert!(
        !person_exists(&pool, old_root_pid).await,
        "orphaned person_mapping should have been deleted"
    );

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 2);
    assert_eq!(count_union_find(&pool, t).await, 2);

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    let chain = collect_chain(&pool, t, "a").await;
    assert!(is_person_identified(&pool, chain[0].person_id.unwrap()).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_shared_person_cleanup() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    let person_b = db::handle_create(&pool, t, "b").await.unwrap();
    handle_alias(&pool, t, "b", "c").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &person_b.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b"], &person_b.person_uuid).await;

    let old_person_b_id = collect_chain(&pool, t, "b").await[0].person_id.unwrap();

    let resp = handle_batched_merge(&pool, t, "a", &["b".into(), "c".into()])
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
async fn batched_merge_empty_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let tgt = db::handle_create(&pool, t, "tgt").await.unwrap();
    let resp = handle_batched_merge(&pool, t, "tgt", &[]).await.unwrap();
    assert_eq!(resp.person_uuid, tgt.person_uuid);
    assert!(resp.is_identified);

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_eq!(count_distinct_ids(&pool, t).await, 1);
    assert_eq!(count_union_find(&pool, t).await, 1);

    assert_chain_is_root(&pool, t, "tgt", &tgt.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_multiple_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    handle_alias(&pool, t, "a", "c").await.unwrap();

    let resp = handle_batched_merge(&pool, t, "a", &["b".into(), "c".into(), "brand-new".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(resp.is_identified);

    assert_eq!(
        count_person_mappings(&pool, t).await,
        1,
        "b's person should be merged into a's"
    );

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
async fn batched_merge_target_in_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let tgt = db::handle_create(&pool, t, "a").await.unwrap();
    let resp = handle_batched_merge(&pool, t, "a", &["a".into()])
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
async fn batched_merge_chain_all_ids_follow() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    let person_c = db::handle_create(&pool, t, "c").await.unwrap();

    handle_batched_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    handle_batched_merge(&pool, t, "c", &["a".into()])
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
async fn batched_merge_chain_deep_transitive() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    let person_d = db::handle_create(&pool, t, "d").await.unwrap();

    handle_batched_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &person_a.person_uuid).await;

    handle_batched_merge(&pool, t, "d", &["a".into()])
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
async fn batched_merge_sets_identified() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_x = db::handle_create(&pool, t, "x").await.unwrap();
    db::handle_create(&pool, t, "y").await.unwrap();

    let chain_x = collect_chain(&pool, t, "x").await;
    let pid_x = chain_x[0].person_id.unwrap();
    assert!(!is_person_identified(&pool, pid_x).await);

    let resp = handle_batched_merge(&pool, t, "x", &["y".into()])
        .await
        .unwrap();
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "x", &r_x.person_uuid).await;
    assert_chain_matches(&pool, t, "y", &["y", "x"], &r_x.person_uuid).await;
    assert!(is_person_identified(&pool, pid_x).await);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_rejects_illegal_ids() {
    let pool = test_pool().await;
    let t = next_team_id();

    db::handle_create(&pool, t, "legit").await.unwrap();

    let result = handle_batched_merge(&pool, t, "anonymous", &[]).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::IllegalDistinctId(_) => {}
        other => panic!("expected IllegalDistinctId, got: {other}"),
    }

    let result = handle_batched_merge(&pool, t, "legit", &["undefined".into()]).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::IllegalDistinctId(_) => {}
        other => panic!("expected IllegalDistinctId, got: {other}"),
    }
}

#[tokio::test]
async fn batched_merge_link_structure() {
    let pool = test_pool().await;
    let t = next_team_id();

    let r_a = db::handle_create(&pool, t, "a").await.unwrap();
    let r_b = db::handle_create(&pool, t, "b").await.unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_is_root(&pool, t, "b", &r_b.person_uuid).await;

    handle_batched_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();

    assert_chain_is_root(&pool, t, "a", &r_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &r_a.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_duplicate_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    let resp = handle_batched_merge(&pool, t, "a", &["b".into(), "b".into()])
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
async fn batched_merge_target_plus_others_in_sources() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();

    let resp = handle_batched_merge(&pool, t, "a", &["a".into(), "b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_already_identified_target() {
    let pool = test_pool().await;
    let t = next_team_id();

    let resp_x = handle_alias(&pool, t, "x", "x").await.unwrap();
    assert!(resp_x.is_identified);
    assert_chain_is_root(&pool, t, "x", &resp_x.person_uuid).await;

    db::handle_create(&pool, t, "y").await.unwrap();

    let resp = handle_batched_merge(&pool, t, "x", &["y".into()])
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

#[tokio::test]
async fn batched_merge_with_deleted_target() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_delete_person(&pool, t, &pa.person_uuid)
        .await
        .unwrap();

    let resp = handle_batched_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_ne!(resp.person_uuid, pa.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "a", &resp.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &resp.person_uuid).await;

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_with_deleted_source() {
    let pool = test_pool().await;
    let t = next_team_id();

    let pa = db::handle_create(&pool, t, "a").await.unwrap();
    let pb = db::handle_create(&pool, t, "b").await.unwrap();

    db::handle_delete_person(&pool, t, &pb.person_uuid)
        .await
        .unwrap();

    let resp = handle_batched_merge(&pool, t, "a", &["b".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, pa.person_uuid);
    assert!(resp.is_identified);

    assert_chain_is_root(&pool, t, "a", &pa.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &pa.person_uuid).await;

    assert_structural_invariants(&pool, t).await;
}

// Test for CompressHint return from batched_merge was removed: mutations no
// longer return compression hints (read-side lazy compression only).
// Removed: batched_merge_returns_compress_hint_above_threshold.

// ===========================================================================
// Batched merge — new tests specific to batched behavior
// ===========================================================================

#[tokio::test]
async fn batched_merge_multiple_sources_same_root() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    let person_b = db::handle_create(&pool, t, "b").await.unwrap();
    handle_alias(&pool, t, "b", "c").await.unwrap();
    handle_alias(&pool, t, "b", "d").await.unwrap();

    assert_chain_is_root(&pool, t, "b", &person_b.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b"], &person_b.person_uuid).await;
    assert_chain_matches(&pool, t, "d", &["d", "b"], &person_b.person_uuid).await;

    let old_person_b_id = collect_chain(&pool, t, "b").await[0].person_id.unwrap();

    let resp = handle_batched_merge(&pool, t, "a", &["b".into(), "c".into(), "d".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(resp.is_identified);

    assert!(
        !person_exists(&pool, old_person_b_id).await,
        "person B should be deleted exactly once"
    );
    assert_eq!(count_person_mappings(&pool, t).await, 1);

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "d", &["d", "b", "a"], &person_a.person_uuid).await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_large_batch() {
    let pool = test_pool().await;
    let t = next_team_id();

    let target = db::handle_create(&pool, t, "target").await.unwrap();

    let mut sources: Vec<String> = Vec::new();
    for i in 0..60 {
        let did = format!("src-{i}");
        if i < 30 {
            db::handle_create(&pool, t, &did).await.unwrap();
        }
        sources.push(did);
    }

    let resp = handle_batched_merge(&pool, t, "target", &sources)
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, target.person_uuid);
    assert!(resp.is_identified);

    assert_eq!(
        count_person_mappings(&pool, t).await,
        1,
        "all persons should be merged into one"
    );
    assert_eq!(count_distinct_ids(&pool, t).await, 61); // target + 60 sources
    assert_eq!(count_union_find(&pool, t).await, 61);

    for src in &sources {
        let resolved = resolve(&pool, t, src).await.unwrap().unwrap();
        assert_eq!(resolved.person_uuid, target.person_uuid);
    }

    assert_chain_is_root(&pool, t, "target", &target.person_uuid).await;
    assert_chain_matches(&pool, t, "src-0", &["src-0", "target"], &target.person_uuid).await;
    assert_chain_matches(
        &pool,
        t,
        "src-29",
        &["src-29", "target"],
        &target.person_uuid,
    )
    .await;
    assert_chain_matches(
        &pool,
        t,
        "src-30",
        &["src-30", "target"],
        &target.person_uuid,
    )
    .await;
    assert_chain_matches(
        &pool,
        t,
        "src-59",
        &["src-59", "target"],
        &target.person_uuid,
    )
    .await;

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_matches_merge_output() {
    let pool = test_pool().await;

    let setup = |team_id: i64| {
        let pool = pool.clone();
        async move {
            db::handle_create(&pool, team_id, "tgt").await.unwrap();
            db::handle_create(&pool, team_id, "s1").await.unwrap();
            db::handle_create(&pool, team_id, "s2").await.unwrap();
            db::handle_create(&pool, team_id, "s3").await.unwrap();
            handle_alias(&pool, team_id, "s2", "s2-child")
                .await
                .unwrap();
        }
    };

    let sources: Vec<String> = vec![
        "s1".into(),
        "s2".into(),
        "s3".into(),
        "s2-child".into(),
        "brand-new".into(),
    ];

    let t1 = next_team_id();
    setup(t1).await;
    let resp1 = handle_merge(&pool, t1, "tgt", &sources).await.unwrap();

    let t2 = next_team_id();
    setup(t2).await;
    let resp2 = handle_batched_merge(&pool, t2, "tgt", &sources)
        .await
        .unwrap();

    assert!(resp1.is_identified);
    assert!(resp2.is_identified);

    assert_eq!(
        count_person_mappings(&pool, t1).await,
        count_person_mappings(&pool, t2).await,
        "person_mapping count mismatch"
    );
    assert_eq!(
        count_distinct_ids(&pool, t1).await,
        count_distinct_ids(&pool, t2).await,
        "distinct_id count mismatch"
    );
    assert_eq!(
        count_union_find(&pool, t1).await,
        count_union_find(&pool, t2).await,
        "union_find count mismatch"
    );

    for did in &["tgt", "s1", "s2", "s3", "s2-child", "brand-new"] {
        let r1 = resolve(&pool, t1, did).await.unwrap().unwrap();
        let r2 = resolve(&pool, t2, did).await.unwrap().unwrap();
        assert_eq!(
            r1.person_uuid, resp1.person_uuid,
            "merge: {did} should resolve to target person"
        );
        assert_eq!(
            r2.person_uuid, resp2.person_uuid,
            "batched_merge: {did} should resolve to target person"
        );

        let chain1 = collect_chain(&pool, t1, did).await;
        let chain2 = collect_chain(&pool, t2, did).await;
        assert_eq!(
            chain1.len(),
            chain2.len(),
            "chain length mismatch for '{did}': merge={} vs batched={}",
            chain1.len(),
            chain2.len()
        );
        let root1 = chain1.last().unwrap();
        let root2 = chain2.last().unwrap();
        assert!(
            root1.person_id.is_some(),
            "merge: chain from '{did}' has no person_id at root"
        );
        assert!(
            root2.person_id.is_some(),
            "batched_merge: chain from '{did}' has no person_id at root"
        );
    }

    assert_all_invariants(&pool, t1).await;
    assert_all_invariants(&pool, t2).await;
}

// ===========================================================================
// Batched merge — structural rigor tests
// ===========================================================================

#[tokio::test]
async fn batched_merge_uf_row_inspection() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();

    let resp = handle_batched_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);

    let uf_a = get_uf_row(&pool, t, "a")
        .await
        .expect("a should exist in union_find");
    assert!(
        uf_a.next.is_none(),
        "root 'a' should have next=NULL, got {:?}",
        uf_a.next
    );
    assert!(
        uf_a.person_id.is_some(),
        "root 'a' should have person_id set"
    );

    let a_pk = uf_a.current;

    let uf_b = get_uf_row(&pool, t, "b")
        .await
        .expect("b should exist in union_find");
    assert_eq!(
        uf_b.next,
        Some(a_pk),
        "b's next should point to a's PK ({a_pk}), got {:?}",
        uf_b.next
    );
    assert!(
        uf_b.person_id.is_none(),
        "b should have person_id=NULL after merge (non-root)"
    );

    let uf_c = get_uf_row(&pool, t, "c")
        .await
        .expect("c should exist in union_find");
    assert_eq!(
        uf_c.next,
        Some(a_pk),
        "c's next should point to a's PK ({a_pk}), got {:?}",
        uf_c.next
    );
    assert!(
        uf_c.person_id.is_none(),
        "c should have person_id=NULL after merge (non-root)"
    );

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_fan_in_topology() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    db::handle_create(&pool, t, "d").await.unwrap();

    handle_batched_merge(&pool, t, "a", &["b".into(), "c".into(), "d".into()])
        .await
        .unwrap();

    let uf_a = get_uf_row(&pool, t, "a").await.unwrap();
    let a_pk = uf_a.current;

    let parents = collect_parents(&pool, t, a_pk).await;
    let parent_dids: Vec<&str> = parents.iter().map(|p| p.distinct_id.as_str()).collect();
    assert_eq!(
        parents.len(),
        3,
        "target 'a' should have exactly 3 parents (b, c, d), got: {parent_dids:?}"
    );
    for p in &parents {
        assert!(
            ["b", "c", "d"].contains(&p.distinct_id.as_str()),
            "unexpected parent: {}",
            p.distinct_id
        );
        assert!(
            p.person_id.is_none(),
            "parent '{}' should have person_id=NULL",
            p.distinct_id
        );
        assert_eq!(
            p.next,
            Some(a_pk),
            "parent '{}' next should point to a's PK",
            p.distinct_id
        );
    }

    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "a"], &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "d", &["d", "a"], &person_a.person_uuid).await;

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_deep_subtrees_transitive() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_t = db::handle_create(&pool, t, "t").await.unwrap();

    // Build deep chain: e -> d -> c -> b (b is root)
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();
    db::handle_create(&pool, t, "d").await.unwrap();
    db::handle_create(&pool, t, "e").await.unwrap();
    handle_merge(&pool, t, "b", &["c".into()]).await.unwrap(); // c -> b
    handle_merge(&pool, t, "c", &["d".into()]).await.unwrap(); // d -> c -> b
    handle_merge(&pool, t, "d", &["e".into()]).await.unwrap(); // e -> d -> c -> b

    // Build deep chain: h -> g -> f (f is root)
    db::handle_create(&pool, t, "f").await.unwrap();
    db::handle_create(&pool, t, "g").await.unwrap();
    db::handle_create(&pool, t, "h").await.unwrap();
    handle_merge(&pool, t, "f", &["g".into()]).await.unwrap(); // g -> f
    handle_merge(&pool, t, "g", &["h".into()]).await.unwrap(); // h -> g -> f

    // Verify pre-merge chain shapes
    assert_chain_matches(&pool, t, "e", &["e", "d", "c", "b"], &{
        resolve(&pool, t, "b").await.unwrap().unwrap().person_uuid
    })
    .await;
    assert_chain_matches(&pool, t, "h", &["h", "g", "f"], &{
        resolve(&pool, t, "f").await.unwrap().unwrap().person_uuid
    })
    .await;

    // Batched merge: link both subtree roots into t
    let resp = handle_batched_merge(&pool, t, "t", &["b".into(), "f".into()])
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_t.person_uuid);

    // Verify all chains resolve transitively through their subtrees to t
    assert_chain_is_root(&pool, t, "t", &person_t.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "t"], &person_t.person_uuid).await;
    assert_chain_matches(&pool, t, "c", &["c", "b", "t"], &person_t.person_uuid).await;
    assert_chain_matches(&pool, t, "d", &["d", "c", "b", "t"], &person_t.person_uuid).await;
    assert_chain_matches(
        &pool,
        t,
        "e",
        &["e", "d", "c", "b", "t"],
        &person_t.person_uuid,
    )
    .await;
    assert_chain_matches(&pool, t, "f", &["f", "t"], &person_t.person_uuid).await;
    assert_chain_matches(&pool, t, "g", &["g", "f", "t"], &person_t.person_uuid).await;
    assert_chain_matches(&pool, t, "h", &["h", "g", "f", "t"], &person_t.person_uuid).await;

    assert_eq!(count_person_mappings(&pool, t).await, 1);
    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn batched_merge_mixed_states() {
    let pool = test_pool().await;
    let t = next_team_id();

    // Target
    let person_a = db::handle_create(&pool, t, "a").await.unwrap();

    // b: same person as a (via alias) — will be Live-same
    handle_alias(&pool, t, "a", "b").await.unwrap();
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    // c: different person — will be Live-different
    db::handle_create(&pool, t, "c").await.unwrap();
    let old_c_pid = collect_chain(&pool, t, "c").await[0].person_id.unwrap();

    // d: orphaned (person deleted) — will be Orphaned
    let person_d = db::handle_create(&pool, t, "d").await.unwrap();
    db::handle_delete_person(&pool, t, &person_d.person_uuid)
        .await
        .unwrap();

    // e: does not exist — will be NotFound

    let resp = handle_batched_merge(
        &pool,
        t,
        "a",
        &["b".into(), "c".into(), "d".into(), "e".into()],
    )
    .await
    .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(resp.is_identified);

    // b was same-person — chain unchanged
    assert_chain_is_root(&pool, t, "a", &person_a.person_uuid).await;
    assert_chain_matches(&pool, t, "b", &["b", "a"], &person_a.person_uuid).await;

    // c was different-person — root demoted, linked to a
    assert_chain_matches(&pool, t, "c", &["c", "a"], &person_a.person_uuid).await;
    assert!(
        !person_exists(&pool, old_c_pid).await,
        "c's old person should be deleted"
    );

    // d was orphaned — relinked to a
    assert_chain_matches(&pool, t, "d", &["d", "a"], &person_a.person_uuid).await;

    // e was not-found — newly created and linked to a
    assert_chain_matches(&pool, t, "e", &["e", "a"], &person_a.person_uuid).await;

    assert_eq!(count_live_person_mappings(&pool, t).await, 1);
    assert_structural_invariants(&pool, t).await;
}

// ===========================================================================
// /resolve_distinct_ids
// ===========================================================================

#[tokio::test]
async fn resolve_distinct_ids_single() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "user-1").await.unwrap();

    let resp = resolve_distinct_ids(&pool, t, &created.person_uuid)
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, created.person_uuid);
    assert_eq!(resp.distinct_ids, vec!["user-1"]);
    assert!(!resp.is_truncated);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn resolve_distinct_ids_after_alias() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    handle_alias(&pool, t, "a", "b").await.unwrap();
    handle_alias(&pool, t, "a", "c").await.unwrap();

    let resp = resolve_distinct_ids(&pool, t, &created.person_uuid)
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, created.person_uuid);
    assert!(!resp.is_truncated);

    let mut dids = resp.distinct_ids.clone();
    dids.sort();
    assert_eq!(dids, vec!["a", "b", "c"]);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn resolve_distinct_ids_after_merge() {
    let pool = test_pool().await;
    let t = next_team_id();

    let person_a = db::handle_create(&pool, t, "a").await.unwrap();
    db::handle_create(&pool, t, "b").await.unwrap();
    db::handle_create(&pool, t, "c").await.unwrap();

    handle_merge(&pool, t, "a", &["b".into(), "c".into()])
        .await
        .unwrap();

    let resp = resolve_distinct_ids(&pool, t, &person_a.person_uuid)
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, person_a.person_uuid);
    assert!(!resp.is_truncated);

    let mut dids = resp.distinct_ids.clone();
    dids.sort();
    assert_eq!(dids, vec!["a", "b", "c"]);

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn resolve_distinct_ids_fan_out() {
    let pool = test_pool().await;
    let t = next_team_id();

    let root = db::handle_create(&pool, t, "root").await.unwrap();
    for i in 0..20 {
        let did = format!("leaf-{i}");
        handle_alias(&pool, t, "root", &did).await.unwrap();
    }

    let resp = resolve_distinct_ids(&pool, t, &root.person_uuid)
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, root.person_uuid);
    assert!(!resp.is_truncated);
    assert_eq!(resp.distinct_ids.len(), 21);

    assert!(resp.distinct_ids.contains(&"root".to_string()));
    for i in 0..20 {
        assert!(resp.distinct_ids.contains(&format!("leaf-{i}")));
    }

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn resolve_distinct_ids_not_found() {
    let pool = test_pool().await;
    let t = next_team_id();

    let result = resolve_distinct_ids(&pool, t, "nonexistent-uuid").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::NotFound(_) => {}
        other => panic!("expected NotFound, got: {other}"),
    }
}

#[tokio::test]
async fn resolve_distinct_ids_deleted_person() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    handle_alias(&pool, t, "a", "b").await.unwrap();

    db::handle_delete_person(&pool, t, &created.person_uuid)
        .await
        .unwrap();

    let result = resolve_distinct_ids(&pool, t, &created.person_uuid).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::NotFound(_) => {}
        other => panic!("expected NotFound for deleted person, got: {other}"),
    }

    assert_structural_invariants(&pool, t).await;
}

#[tokio::test]
async fn resolve_distinct_ids_team_isolation() {
    let pool = test_pool().await;
    let t1 = next_team_id();
    let t2 = next_team_id();

    let p1 = db::handle_create(&pool, t1, "shared").await.unwrap();
    let p2 = db::handle_create(&pool, t2, "shared").await.unwrap();

    handle_alias(&pool, t1, "shared", "extra-1").await.unwrap();

    let resp1 = resolve_distinct_ids(&pool, t1, &p1.person_uuid)
        .await
        .unwrap();
    let resp2 = resolve_distinct_ids(&pool, t2, &p2.person_uuid)
        .await
        .unwrap();

    let mut dids1 = resp1.distinct_ids.clone();
    dids1.sort();
    assert_eq!(dids1, vec!["extra-1", "shared"]);
    assert_eq!(resp2.distinct_ids, vec!["shared"]);

    let cross = resolve_distinct_ids(&pool, t2, &p1.person_uuid).await;
    assert!(
        cross.is_err(),
        "person_uuid from t1 should not resolve in t2"
    );

    assert_all_invariants(&pool, t1).await;
    assert_all_invariants(&pool, t2).await;
}

#[tokio::test]
async fn resolve_distinct_ids_truncation() {
    let pool = test_pool().await;
    let t = next_team_id();

    let root = db::handle_create(&pool, t, "root").await.unwrap();

    let batch_size = 500;
    let total_leaves = 10_001;
    for batch_start in (0..total_leaves).step_by(batch_size) {
        let batch_end = std::cmp::min(batch_start + batch_size, total_leaves);
        let sources: Vec<String> = (batch_start..batch_end)
            .map(|i| format!("did-{i}"))
            .collect();
        handle_merge(&pool, t, "root", &sources).await.unwrap();
    }

    let resp = resolve_distinct_ids(&pool, t, &root.person_uuid)
        .await
        .unwrap();
    assert!(
        resp.is_truncated,
        "should be truncated with >10000 distinct_ids"
    );
    assert_eq!(resp.distinct_ids.len(), 10_000);
}

#[tokio::test]
async fn resolve_distinct_ids_deep_chain() {
    let pool = test_pool().await;
    let t = next_team_id();

    let root = db::handle_create(&pool, t, "n0").await.unwrap();
    for i in 1..=50 {
        let prev = format!("n{}", i - 1);
        let curr = format!("n{i}");
        handle_alias(&pool, t, &prev, &curr).await.unwrap();
    }

    let resp = resolve_distinct_ids(&pool, t, &root.person_uuid)
        .await
        .unwrap();
    assert_eq!(resp.person_uuid, root.person_uuid);
    assert!(!resp.is_truncated);
    assert_eq!(resp.distinct_ids.len(), 51);

    for i in 0..=50 {
        assert!(resp.distinct_ids.contains(&format!("n{i}")));
    }

    assert_all_invariants(&pool, t).await;
}

#[tokio::test]
async fn resolve_distinct_ids_after_delete_did() {
    let pool = test_pool().await;
    let t = next_team_id();

    let created = db::handle_create(&pool, t, "a").await.unwrap();
    handle_alias(&pool, t, "a", "b").await.unwrap();
    handle_alias(&pool, t, "a", "c").await.unwrap();

    db::handle_delete_distinct_id(&pool, t, "b").await.unwrap();

    let resp = resolve_distinct_ids(&pool, t, &created.person_uuid)
        .await
        .unwrap();
    assert!(!resp.is_truncated);

    let mut dids = resp.distinct_ids.clone();
    dids.sort();
    assert_eq!(dids, vec!["a", "c"]);
    assert!(!resp.distinct_ids.contains(&"b".to_string()));

    assert_all_invariants(&pool, t).await;
}
