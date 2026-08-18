//! Model-based randomized testing: replay a random sequence of API operations
//! against both the real service and a trivial in-memory model of the intended
//! semantics, then assert the two agree on every distinct_id's fate.
//!
//! The model tracks only *who belongs to whom* — no trees, no pointers — so any
//! divergence points at a structural bug in the union-find maintenance. Seeds
//! are fixed for reproducibility; every assertion names the seed and step.

mod common;

use std::collections::HashMap;

use common::*;
use pg_union_find_rs::db;
use pg_union_find_rs::models::DbError;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sqlx::PgPool;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// The in-memory model
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct PersonKey(u64);

#[derive(Clone, Copy, Debug)]
enum MDid {
    /// Belongs to a live person.
    Live(PersonKey),
    /// Its person was deleted; resolves to nothing until a write reclaims it.
    Orphan,
}

#[derive(Default)]
struct Model {
    dids: HashMap<String, MDid>,
    /// Live persons only: key -> is_identified.
    identified: HashMap<PersonKey, bool>,
    next_key: u64,
}

impl Model {
    fn new_person(&mut self, identified: bool) -> PersonKey {
        let k = PersonKey(self.next_key);
        self.next_key += 1;
        self.identified.insert(k, identified);
        k
    }

    fn state(&self, d: &str) -> Option<MDid> {
        self.dids.get(d).copied()
    }

    fn live_person(&self, d: &str) -> Option<PersonKey> {
        match self.dids.get(d) {
            Some(MDid::Live(p)) => Some(*p),
            _ => None,
        }
    }

    /// Move every distinct_id of `from` onto `to` and delete `from`.
    fn move_group(&mut self, from: PersonKey, to: PersonKey) {
        for s in self.dids.values_mut() {
            if let MDid::Live(p) = s
                && *p == from
            {
                *p = to;
            }
        }
        self.identified.remove(&from);
    }

    fn create(&mut self, d: &str) {
        match self.state(d) {
            Some(MDid::Live(_)) => {}
            Some(MDid::Orphan) | None => {
                let p = self.new_person(false);
                self.dids.insert(d.to_string(), MDid::Live(p));
            }
        }
    }

    /// Err(()) means the real API is expected to refuse with AlreadyIdentified.
    fn alias(&mut self, target: &str, source: &str) -> Result<(), ()> {
        if target == source {
            self.create(target);
            let p = self.live_person(target).unwrap();
            self.identified.insert(p, true);
            return Ok(());
        }
        match (self.live_person(target), self.live_person(source)) {
            (Some(tp), Some(sp)) if tp == sp => {
                self.identified.insert(tp, true);
            }
            (Some(tp), Some(sp)) => {
                if self.identified[&sp] {
                    return Err(());
                }
                self.move_group(sp, tp);
                self.identified.insert(tp, true);
            }
            (Some(tp), None) => {
                self.dids.insert(source.into(), MDid::Live(tp));
                self.identified.insert(tp, true);
            }
            (None, Some(sp)) => {
                self.dids.insert(target.into(), MDid::Live(sp));
                self.identified.insert(sp, true);
            }
            (None, None) => {
                let p = self.new_person(true);
                self.dids.insert(target.into(), MDid::Live(p));
                self.dids.insert(source.into(), MDid::Live(p));
            }
        }
        Ok(())
    }

    /// Err(()) means the real API is expected to refuse with NotFound.
    fn merge(&mut self, target: &str, sources: &[String]) -> Result<(), ()> {
        let tp = match self.state(target) {
            None => return Err(()),
            Some(MDid::Orphan) => {
                let p = self.new_person(false);
                self.dids.insert(target.into(), MDid::Live(p));
                p
            }
            Some(MDid::Live(p)) => p,
        };
        for s in sources {
            match self.state(s) {
                None | Some(MDid::Orphan) => {
                    self.dids.insert(s.clone(), MDid::Live(tp));
                }
                Some(MDid::Live(sp)) if sp == tp => {}
                Some(MDid::Live(sp)) => self.move_group(sp, tp),
            }
        }
        self.identified.insert(tp, true);
        Ok(())
    }

    fn delete_person(&mut self, p: PersonKey) {
        for s in self.dids.values_mut() {
            if matches!(s, MDid::Live(q) if *q == p) {
                *s = MDid::Orphan;
            }
        }
        self.identified.remove(&p);
    }

    /// Ok(person_deleted) on success; Err(()) means expected NotFound.
    fn delete_did(&mut self, d: &str) -> Result<bool, ()> {
        match self.dids.remove(d) {
            None => Err(()),
            Some(MDid::Orphan) => Ok(false),
            Some(MDid::Live(p)) => {
                let has_others = self
                    .dids
                    .values()
                    .any(|s| matches!(s, MDid::Live(q) if *q == p));
                if has_others {
                    Ok(false)
                } else {
                    self.identified.remove(&p);
                    Ok(true)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Verification: DB and model must induce the same partition of live ids,
// the same orphan/unknown set, and the same identified flags.
// ---------------------------------------------------------------------------

async fn verify(pool: &PgPool, t: i64, model: &Model, universe: &[String], seed: u64, step: usize) {
    let mut key_to_uuid: HashMap<PersonKey, Uuid> = HashMap::new();
    let mut uuid_to_key: HashMap<Uuid, PersonKey> = HashMap::new();

    for d in universe {
        let db_res = resolve(pool, t, d)
            .await
            .unwrap_or_else(|e| panic!("seed {seed} step {step}: resolve({d}) errored: {e}"));
        match model.state(d) {
            None | Some(MDid::Orphan) => assert!(
                db_res.is_none(),
                "seed {seed} step {step}: '{d}' should not resolve, got {:?}",
                db_res.map(|r| r.person_uuid)
            ),
            Some(MDid::Live(p)) => {
                let r = db_res.unwrap_or_else(|| {
                    panic!("seed {seed} step {step}: '{d}' should resolve to a live person")
                });
                if let Some(u) = key_to_uuid.get(&p) {
                    assert_eq!(
                        *u, r.person_uuid,
                        "seed {seed} step {step}: '{d}' split from its model group"
                    );
                } else {
                    key_to_uuid.insert(p, r.person_uuid);
                }
                if let Some(k) = uuid_to_key.get(&r.person_uuid) {
                    assert_eq!(
                        *k, p,
                        "seed {seed} step {step}: '{d}' merged into a foreign model group"
                    );
                } else {
                    uuid_to_key.insert(r.person_uuid, p);
                }
                assert_eq!(
                    r.is_identified, model.identified[&p],
                    "seed {seed} step {step}: '{d}' is_identified mismatch"
                );
            }
        }
    }

    assert_eq!(
        count_live_person_mappings(pool, t).await,
        model.identified.len() as i64,
        "seed {seed} step {step}: live person count mismatch"
    );
    assert_structural_invariants(pool, t).await;
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

async fn run_random_ops(seed: u64, num_ops: usize) {
    let pool = test_pool().await;
    let t = next_team_id();
    let mut rng = StdRng::seed_from_u64(seed);
    let universe: Vec<String> = (0..10).map(|i| format!("d{i}")).collect();
    let mut model = Model::default();

    let pick = |rng: &mut StdRng, universe: &[String]| -> String {
        universe[rng.random_range(0..universe.len())].clone()
    };

    for step in 0..num_ops {
        match rng.random_range(0..100u32) {
            // create — 20%
            0..20 => {
                let d = pick(&mut rng, &universe);
                db::handle_create(&pool, t, &d)
                    .await
                    .unwrap_or_else(|e| panic!("seed {seed} step {step}: create({d}): {e}"));
                model.create(&d);
            }
            // alias — 25%
            20..45 => {
                let tg = pick(&mut rng, &universe);
                let src = pick(&mut rng, &universe);
                let db_r = handle_alias(&pool, t, &tg, &src).await;
                match model.alias(&tg, &src) {
                    Ok(()) => {
                        db_r.unwrap_or_else(|e| {
                            panic!("seed {seed} step {step}: alias({tg},{src}): {e}")
                        });
                    }
                    Err(()) => match db_r {
                        Err(DbError::AlreadyIdentified(_)) => {}
                        other => panic!(
                            "seed {seed} step {step}: alias({tg},{src}) expected \
                             AlreadyIdentified, got {other:?}"
                        ),
                    },
                }
            }
            // merge / batched_merge — 15% each
            45..75 => {
                let batched = rng.random_range(0..2) == 0;
                let tg = pick(&mut rng, &universe);
                let n = rng.random_range(1..=3);
                let sources: Vec<String> = (0..n).map(|_| pick(&mut rng, &universe)).collect();
                let db_r = if batched {
                    handle_batched_merge(&pool, t, &tg, &sources).await
                } else {
                    handle_merge(&pool, t, &tg, &sources).await
                };
                match model.merge(&tg, &sources) {
                    Ok(()) => {
                        db_r.unwrap_or_else(|e| {
                            panic!(
                                "seed {seed} step {step}: merge(batched={batched}, {tg}, \
                                 {sources:?}): {e}"
                            )
                        });
                    }
                    Err(()) => match db_r {
                        Err(DbError::NotFound(_)) => {}
                        other => panic!(
                            "seed {seed} step {step}: merge({tg}) expected NotFound, got {other:?}"
                        ),
                    },
                }
            }
            // delete_person — 10%
            75..85 => {
                let d = pick(&mut rng, &universe);
                if let Some(p) = model.live_person(&d) {
                    let uuid = resolve(&pool, t, &d).await.unwrap().unwrap().person_uuid;
                    db::handle_delete_person(&pool, t, uuid)
                        .await
                        .unwrap_or_else(|e| {
                            panic!("seed {seed} step {step}: delete_person via {d}: {e}")
                        });
                    model.delete_person(p);
                }
            }
            // delete_distinct_id — 15%
            _ => {
                let d = pick(&mut rng, &universe);
                let db_r = db::handle_delete_distinct_id(&pool, t, &d).await;
                match model.delete_did(&d) {
                    Ok(expected_person_deleted) => {
                        let r = db_r.unwrap_or_else(|e| {
                            panic!("seed {seed} step {step}: delete_distinct_id({d}): {e}")
                        });
                        assert_eq!(
                            r.person_deleted, expected_person_deleted,
                            "seed {seed} step {step}: delete_distinct_id({d}) person_deleted"
                        );
                    }
                    Err(()) => match db_r {
                        Err(DbError::NotFound(_)) => {}
                        other => panic!(
                            "seed {seed} step {step}: delete_distinct_id({d}) expected \
                             NotFound, got {other:?}"
                        ),
                    },
                }
            }
        }

        if step % 25 == 24 {
            verify(&pool, t, &model, &universe, seed, step).await;
        }
    }

    verify(&pool, t, &model, &universe, seed, num_ops).await;
}

#[tokio::test]
async fn model_random_ops_seed_a() {
    run_random_ops(0xC0FFEE, 300).await;
}

#[tokio::test]
async fn model_random_ops_seed_b() {
    run_random_ops(0xDECAF5, 300).await;
}

#[tokio::test]
async fn model_random_ops_seed_c() {
    run_random_ops(0xB01DFACE, 300).await;
}
