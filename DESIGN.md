# Design

This document describes the architecture and operation of `pg-union-find-rs`, a Postgres-backed identity resolution service. It maps **distinct_ids** (device fingerprints, login handles, anonymous tokens) to **persons** using a union-find data structure stored entirely in Postgres. The design goal is to minimize DB load and round-trips while maintaining a correct, transactional identity graph.

---

## Table of Contents

- [System Architecture](#system-architecture)
- [Data Model](#data-model)
- [Union-Find Representation](#union-find-representation)
- [Read Path: Resolve](#read-path-resolve)
- [Read Path: Resolve Distinct IDs](#read-path-resolve-distinct-ids)
- [Write Path: Shared Worker Design](#write-path-shared-worker-design)
- [Operations](#operations)
  - [Create](#create)
  - [Alias / Identify](#alias--identify)
  - [Merge](#merge)
  - [Batched Merge](#batched-merge)
  - [Delete Person](#delete-person)
  - [Delete Distinct ID](#delete-distinct-id)
- [Path Compression](#path-compression)
- [Lazy Unlink: Orphan Handling](#lazy-unlink-orphan-handling)

---

## System Architecture

```
                        HTTP clients
                 ┌──────────┼──────────┐
                 │          │          │
                 ▼          ▼          ▼
            ┌──────────────────────────────┐
            │        Axum HTTP Server      │
            │        (POST JSON APIs)      │
            └────┬─────────────────┬───────┘
                 │                 │
         ┌───── │ ─────┐          │
         │  WRITE PATH │     READ PATH
         │             │          │
         ▼             │          ▼
  ┌─────────────┐      │   ┌─────────────┐
  │  Shard by   │      │   │  Direct DB  │
  │  team_id %N │      │   │  pool.acquire│
  └──┬──┬──┬──┬─┘      │   └──────┬──────┘
     │  │  │  │        │          │
     ▼  ▼  ▼  ▼        │          │  ┌──────────────────┐
  ┌────┬────┬────┐      │          │  │ Recursive CTE    │
  │ W0 │ W1 │ WN │◄─────┘          ├─►│ walks chain to   │
  └─┬──┴─┬──┴─┬──┘                 │  │ root, joins      │
    │    │    │                    │  │ person_mapping   │
    │    │    │  sequential FIFO   │  └──────────────────┘
    │    │    │  per worker         │
    ▼    ▼    ▼                    │
  ┌────────────────────────────────┤
  │        PostgreSQL              │
  │  ┌──────────────────────────┐  │
  │  │    person_mapping        │  │
  │  │    distinct_id_mappings  │  │
  │  │    union_find            │  │
  │  └──────────────────────────┘  │
  └────────────────────────────────┘
```

**Key design properties:**

- **Writes are serialized per shard.** Each worker task owns a bounded `mpsc` channel and processes operations strictly in FIFO order. `team_id % N` determines the shard, so writes for the same team are always serialized. Different teams on the same shard also serialize, which is an acceptable trade-off for simplicity.
- **Reads bypass the worker pool.** `/resolve` and `/resolve_distinct_ids` acquire a connection directly from the Postgres pool and run read-only recursive CTEs. This allows reads to scale independently and never block behind queued writes.
- **Backpressure via bounded channels.** Each worker channel has a bounded capacity (default 64). Enqueue attempts timeout after 100ms, returning HTTP 503 rather than allowing unbounded memory growth.
- **DB connection budget.** `max_connections = WORKER_POOL_SIZE + 1`. Each worker holds at most one connection; the +1 covers direct-pool reads (`/resolve`, `/resolve_distinct_ids`).

---

## Data Model

Three tables form the storage layer:

```
┌─────────────────────────────┐     ┌────────────────────────────────┐
│      person_mapping         │     │     distinct_id_mappings       │
├─────────────────────────────┤     ├────────────────────────────────┤
│ person_id   BIGSERIAL  PK   │     │ id           BIGSERIAL  PK    │
│ team_id     BIGINT          │     │ team_id      BIGINT           │
│ person_uuid VARCHAR(200)    │     │ distinct_id  VARCHAR(200)     │
│ is_identified BOOLEAN       │     └────────────────────────────────┘
│ deleted_at    TIMESTAMPTZ   │          UNIQUE (team_id, distinct_id)
└─────────────────────────────┘
     UNIQUE (team_id, person_uuid)

┌──────────────────────────────────────────────────────────────────┐
│                         union_find                               │
├──────────────────────────────────────────────────────────────────┤
│ team_id    BIGINT   ─┐                                           │
│ current    BIGINT   ─┘ PK     ── references distinct_id_mappings │
│ next       BIGINT   NULLABLE  ── references distinct_id_mappings │
│ person_id  BIGINT   NULLABLE  ── references person_mapping       │
│ deleted_at TIMESTAMPTZ                                           │
└──────────────────────────────────────────────────────────────────┘
  UNIQUE (team_id, person_id) WHERE person_id IS NOT NULL
  INDEX  (team_id, next)      WHERE next IS NOT NULL
```

**Separation of concerns:**
- `person_mapping` is the authoritative record for a person's external UUID, identified status, and soft-delete state.
- `distinct_id_mappings` translates string distinct_ids to compact bigint PKs used internally.
- `union_find` encodes the graph structure using only bigint references. This keeps chain traversal fast (integer joins, no string comparisons).

---

## Union-Find Representation

The `union_find` table represents a **directed forest** of distinct_id nodes. Each node is a row keyed by `(team_id, current)`. Chains are walked by following `current → next` pointers until reaching a **root** node.

A **root** is identified by: `next IS NULL AND person_id IS NOT NULL`. The root's `person_id` joins to `person_mapping` to retrieve the external UUID.

```
Example: three distinct_ids belonging to the same person

distinct_id_mappings:            union_find:
┌────┬──────────┐               ┌─────────┬──────┬───────────┐
│ id │ distinct  │               │ current │ next │ person_id │
├────┼──────────┤               ├─────────┼──────┼───────────┤
│ 10 │ "anon_1" │               │   10    │  11  │   NULL    │  ← leaf
│ 11 │ "anon_2" │               │   11    │  12  │   NULL    │  ← intermediate
│ 12 │ "user@x" │               │   12    │ NULL │    7      │  ← root
└────┴──────────┘               └─────────┴──────┴───────────┘

Chain traversal:  anon_1(10) ──► anon_2(11) ──► user@x(12) ──► person_id=7
                                                                    │
                                                    person_mapping: ▼
                                                  ┌───────────┬────────────────┐
                                                  │ person_id │  person_uuid   │
                                                  ├───────────┼────────────────┤
                                                  │     7     │ "abc-def-123"  │
                                                  └───────────┴────────────────┘
```

**Why not classical union-by-rank?** We don't store rank. Chains grow by appending; depth is bounded by periodic path compression. This keeps the write path simple (one `UPDATE` per merge) and avoids an extra column. Compression flattens chains in bulk when depth exceeds a threshold.

**Recursive CTE traversal** (used by all resolve operations):

```sql
WITH RECURSIVE walk(node, depth) AS (
    SELECT start_pk, 0                                 -- seed
    UNION ALL
    SELECT uf.next, w.depth + 1                        -- follow link
    FROM walk w
    JOIN union_find uf
      ON uf.team_id = $team AND uf.current = w.node
     AND uf.person_id IS NULL                          -- stop at root
    WHERE w.depth < 1000                               -- safety cap
)
SELECT node, depth FROM walk ORDER BY depth DESC LIMIT 1
```

The CTE terminates when it hits a row where `person_id IS NOT NULL` (the JOIN condition fails). The last row produced is the root. This is then joined to `person_mapping` to return the UUID.

---

## Read Path: Resolve

`POST /resolve` is the primary read operation. It bypasses the worker pool entirely.

```
Client                     Server                          PostgreSQL
  │                          │                                │
  │  POST /resolve           │                                │
  │  {team_id, distinct_id}  │                                │
  │─────────────────────────►│                                │
  │                          │   pool.acquire()               │
  │                          │───────────────────────────────►│
  │                          │                                │
  │                          │   SELECT id FROM               │
  │                          │     distinct_id_mappings       │
  │                          │   WHERE team_id=$1             │
  │                          │     AND distinct_id=$2         │
  │                          │───────────────────────────────►│
  │                          │◄───────────────────────────────│ did_pk
  │                          │                                │
  │                          │   Recursive CTE:               │
  │                          │   walk chain → root,           │
  │                          │   JOIN person_mapping           │
  │                          │───────────────────────────────►│
  │                          │◄───────────────────────────────│ (uuid, depth)
  │                          │                                │
  │                          │   if depth > threshold:        │
  │                          │     try_send(CompressPath)     │
  │                          │     to worker (fire-and-forget)│
  │                          │                                │
  │  200 {person_uuid,       │                                │
  │       is_identified}     │                                │
  │◄─────────────────────────│                                │
```

**Performance:** Two SQL statements per resolve (lookup + CTE). The CTE walks integer-indexed rows; typical chains are shallow (depth 1-3 after compression). If the chain is deeper than `PATH_COMPRESS_THRESHOLD`, a background compression task is enqueued via `try_send` (non-blocking, dropped if the channel is full).

**Correctness:** Resolves see a consistent snapshot of the chain. Concurrent writes may extend or compress the chain, but the CTE always walks to a valid root or returns nothing. Soft-deleted persons are filtered out by `deleted_at IS NULL` on the `person_mapping` join.

---

## Read Path: Resolve Distinct IDs

`POST /resolve_distinct_ids` is the reverse of `/resolve`: given a `person_uuid`, it returns all distinct_ids belonging to that person. Like `/resolve`, it bypasses the worker pool and reads directly from the connection pool.

```
Client                     Server                          PostgreSQL
  │                          │                                │
  │  POST /resolve_distinct  │                                │
  │  _ids                    │                                │
  │  {team_id, person_uuid}  │                                │
  │─────────────────────────►│                                │
  │                          │   pool.acquire()               │
  │                          │───────────────────────────────►│
  │                          │                                │
  │                          │   SELECT person_id FROM        │
  │                          │     person_mapping             │
  │                          │   WHERE team_id=$1             │
  │                          │     AND person_uuid=$2         │
  │                          │     AND deleted_at IS NULL     │
  │                          │───────────────────────────────►│
  │                          │◄───────────────────────────────│ person_id
  │                          │                                │
  │                          │   Reverse recursive CTE:       │
  │                          │   find root by person_id,      │
  │                          │   walk backward through all    │
  │                          │   nodes whose next points to   │
  │                          │   collected nodes, join        │
  │                          │   distinct_id_mappings         │
  │                          │   LIMIT 10001                  │
  │                          │───────────────────────────────►│
  │                          │◄───────────────────────────────│ [distinct_ids]
  │                          │                                │
  │  200 {person_uuid,       │                                │
  │       distinct_ids,      │                                │
  │       is_truncated}      │                                │
  │◄─────────────────────────│                                │
```

**SQL strategy — reverse recursive CTE:**

```sql
WITH RECURSIVE tree(node) AS (
    -- Base: find the root node for this person
    SELECT uf.current
    FROM union_find uf
    WHERE uf.team_id = $1 AND uf.person_id = $2 AND uf.deleted_at IS NULL

    UNION ALL

    -- Recurse: find all nodes whose next points to an already-collected node
    SELECT uf.current
    FROM tree t
    JOIN union_find uf ON uf.team_id = $1 AND uf.next = t.node
    WHERE uf.person_id IS NULL
)
SELECT d.distinct_id
FROM tree t
JOIN distinct_id_mappings d ON d.id = t.node AND d.team_id = $1
LIMIT 10001
```

The CTE starts at the root (identified by `person_id`), then recursively expands to every node in the tree by following `next` pointers in reverse. The `LIMIT 10001` allows detection of truncation: if more than 10,000 rows are returned, the response sets `is_truncated: true` and returns exactly 10,000.

**Performance:** Two SQL statements per call (person_mapping lookup + reverse CTE). The CTE traverses the full tree for the person. For typical workloads (tens to hundreds of distinct_ids per person) this is fast. The 10,000 cap prevents pathological cases from producing unbounded result sets.

**Correctness under concurrent writes:** The reverse CTE executes as a single SQL statement and sees a consistent MVCC snapshot. A concurrent merge, alias, or delete cannot produce a partial or inconsistent tree view within the CTE. The only subtle case is between the `person_mapping` lookup (query 1) and the CTE (query 2): a concurrent `delete_person` could soft-delete the person between the two queries, causing the CTE to return zero rows instead of a 404. This is a benign inconsistency — the caller sees an empty result for a person that was deleted mid-flight.

**Error handling:**
- Person not found or soft-deleted → 404 (`DbError::NotFound`)
- Wrong team for UUID → 404 (scoped by `team_id`)
- DB connection/query failure → 500 (`DbError::Internal`)

---

## Write Path: Shared Worker Design

All mutations (`/create`, `/alias`, `/identify`, `/merge`, `/batched_merge`, `/delete_person`, `/delete_distinct_id`) flow through the worker pool.

```
Client                     Handler                     Worker[i]         PostgreSQL
  │                          │                            │                  │
  │  POST /alias {...}       │                            │                  │
  │─────────────────────────►│                            │                  │
  │                          │                            │                  │
  │                          │  idx = team_id % N         │                  │
  │                          │  sender = workers[idx]     │                  │
  │                          │                            │                  │
  │                          │  DbOp::Alias {             │                  │
  │                          │    ..., reply: oneshot_tx   │                  │
  │                          │  }                         │                  │
  │                          │                            │                  │
  │                          │──── send(op) ─────────────►│                  │
  │                          │    (100ms timeout)         │                  │
  │                          │                            │                  │
  │                          │        (handler awaits     │  BEGIN           │
  │                          │         oneshot_rx)        │─────────────────►│
  │                          │                            │  ...SQL ops...   │
  │                          │                            │─────────────────►│
  │                          │                            │  COMMIT          │
  │                          │                            │─────────────────►│
  │                          │                            │                  │
  │                          │◄─── reply_tx.send(result) ─│                  │
  │                          │                            │                  │
  │  200 {person_uuid, ...}  │                            │                  │
  │◄─────────────────────────│                            │                  │
```

The `oneshot` channel bridges the async HTTP handler to the worker. The handler creates a `(oneshot::Sender, oneshot::Receiver)` pair, packages the sender into the `DbOp`, sends the op over the bounded `mpsc` channel, and awaits the receiver. The worker processes operations sequentially and sends the result back.

**Why serialize writes?** Identity operations for the same team must not race. Two concurrent `/alias` calls for the same team could both read the chain, decide to merge, and create conflicting link structures. Serialization avoids this without row-level locking or retry loops.

---

## Operations

### Create

`POST /create` — Get-or-create a person for a single distinct_id. This is the simplest write: it either returns an existing mapping or inserts a new person + distinct_id + root row.

**Case 1: distinct_id already exists and is live**

```
Before:                              After (no change):
  "anon_1" ──► root(person_id=7)      "anon_1" ──► root(person_id=7)

Response: { person_uuid: <uuid-of-7>, is_identified: false }
```

No rows are inserted. The existing chain is traversed and the person is returned.

**Case 2: distinct_id does not exist**

```
Before:                              After:
  (nothing)                            "anon_1" ──► root(person_id=NEW)
                                                         │
                                         person_mapping: NEW row
                                           { person_uuid: <new-uuid>,
                                             is_identified: false }

  Inserts: 1 person_mapping + 1 distinct_id_mappings + 1 union_find
```

**Case 3: distinct_id exists but its person was soft-deleted (orphaned)**

```
Before:                              After:
  "anon_1" ──► root(person_id=7)      "anon_1" ──► root(person_id=NEW)
               deleted_at=<ts>                           │
                                         person_mapping: NEW row
  Steps:                                   { person_uuid: <new-uuid>,
  1. Detect orphan via resolve             is_identified: false }
  2. unlink_did (detach from dead chain)
  3. Insert new person_mapping
  4. Promote did_pk to root for new person
```

**DB operations:** 1 transaction, 2-4 SQL statements depending on case.

---

### Alias / Identify

`POST /alias` and `POST /identify` share the same handler logic. They link two distinct_ids together under a single person and mark the result as `is_identified = true`.

The `/identify` endpoint maps `{ target, anonymous }` → `{ target, source=anonymous }`. The `/alias` endpoint maps `{ target, alias }` → `{ target, source=alias }`.

There are four cases based on the state of the target and source distinct_ids.

#### Case 1: Both live, same person (no-op)

```
Before:                               After (no chain change):
  "target" ──► root(P=7)                "target" ──► root(P=7, identified)
  "source" ──┘                           "source" ──┘

  Only change: set is_identified = true on person 7
```

#### Case 2: Both live, different persons (merge)

```
Before:
  "target" ──► root_T(P=7)             "source" ──► root_S(P=9)

After:
  "target" ──► root_T(P=7, identified)
  "source" ──► root_S ──┘
                  │
                  └─ person_id cleared to NULL, next set to root_T's did_pk
                     person_mapping row for P=9 DELETED

  Constraint: source person must NOT be is_identified already.
              If it is, return 409 — caller must use /merge to force.
```

**Step by step:**

```
Step 1: resolve_root(source) → (root_S_current, old_person_id=9)

Step 2: UPDATE union_find                          -- demote source root
        SET person_id = NULL, next = target_pk
        WHERE team_id = $1 AND current = root_S_current

Step 3: DELETE FROM person_mapping                 -- clean up merged person
        WHERE person_id = 9

Step 4: UPDATE person_mapping                      -- mark identified
        SET is_identified = true
        WHERE person_id = 7
```

**A deeper example showing chain structure:**

```
Before:
  "a1" ──► "a2" ──► root_T(P=7)        "b1" ──► "b2" ──► root_S(P=9)

After:
  "a1" ──► "a2" ──► root_T(P=7)
  "b1" ──► "b2" ──► old_root_S ──┘
                        │
                        next = root_T's did_pk
                        person_id = NULL

  All nodes formerly reaching P=9 now walk through to P=7.
  Combined chain depth = depth(source) + depth(target) + 1
```

If the combined depth exceeds `PATH_COMPRESS_THRESHOLD`, a `CompressHint` is returned and the handler enqueues background compression.

#### Case 3: One live, the other new or orphaned

```
Before:                               After:
  "target" ──► root(P=7)                "target" ──► root(P=7, identified)
  "source" does not exist                "source" ──┘

  Insert source into distinct_id_mappings.
  Insert union_find row: { current=source_pk, next=target_pk, person_id=NULL }
```

The symmetric case (source live, target new) works identically — the new/orphaned distinct_id is attached into the existing live chain.

#### Case 4: Neither exists

```
Before:                               After:
  (nothing for either)                   "target" ──► root(P=NEW, identified)
                                         "source" ──┘

  Insert new person_mapping (is_identified=true).
  Insert/promote target as root.
  Insert/link source pointing to target.
```

**DB operations:** 1 transaction, 3-8 SQL statements depending on case. The dominant cost is the two `check_did` calls (each is a lookup + CTE resolve).

---

### Merge

`POST /merge` — Force-merge N source distinct_ids into a target's person. Unlike `/alias`, this **ignores `is_identified`** and always merges. The target must exist or be orphaned (not NotFound).

```
Request: { team_id: 1, target: "user@x", sources: ["anon_1", "anon_2", "anon_3"] }
```

**Before:**

```
  "user@x" ──► root(P=7)      "anon_1" ──► root(P=9)      "anon_2" ──► root(P=11)

  "anon_3" does not exist
```

**After:**

```
  "user@x" ──► root(P=7, identified)
  "anon_1" ──► old_root_9 ──┘         person_mapping P=9  DELETED
  "anon_2" ──► old_root_11 ──┘        person_mapping P=11 DELETED
  "anon_3" ──┘                         (new DID, linked directly)
```

**Step by step (per source):**

```
For each source in [anon_1, anon_2, anon_3]:
  ┌─────────────────────────────────────────────────────────────────────┐
  │ state = check_did(source)                                          │
  │                                                                     │
  │ NotFound ──► insert_did_and_link(source, target_pk)                │
  │              (new DID row + union_find link to target)              │
  │                                                                     │
  │ Orphaned ──► link_did(source_pk, target_pk)                        │
  │              (reuse existing DID, update union_find link)           │
  │                                                                     │
  │ Live ──────► resolve_root(source)                                  │
  │              if same person as target: skip (already merged)       │
  │              else: link_root_to_target(source_root, target_pk)     │
  │                    DELETE old person_mapping                        │
  └─────────────────────────────────────────────────────────────────────┘

Finally: set_identified(target_person_id)
```

**All sources are merged within a single transaction.** The target's root is reused; each source's root is demoted to a non-root pointing at the target. The unique `(team_id, person_id)` constraint on `union_find` guarantees no duplicate roots survive the merge.

**DB operations:** 1 transaction, `2 + 2*len(sources)` SQL statements in the typical case (target check_did + per-source check_did + link + set_identified).

---

### Batched Merge

`POST /batched_merge` — Same semantics as `/merge` (identical request/response shape), but replaces the per-source serial SQL with batched queries. This reduces DB round-trips from `2 + 2*N` to roughly 6-8 regardless of source count.

```
Request: { team_id: 1, target: "user@x", sources: ["anon_1", "anon_2", "anon_3"] }
```

The observable result is identical to `/merge`. The difference is internal: instead of N individual `check_did` + link operations, the batched path runs bulk SQL.

**Algorithm (all within a single transaction):**

```
Step 1: check_did(target)                          ← same as /merge
        NotFound → error, Orphaned → create person, Live → use existing

Step 2: Batch lookup all sources (1 query)
        SELECT id, distinct_id FROM distinct_id_mappings
        WHERE team_id = $1 AND distinct_id = ANY($2::text[])
        → HashMap<distinct_id_string, did_pk>
        Sources absent from result → NotFound

Step 3: Batch resolve all found PKs (1 recursive CTE)
        Walk all chains simultaneously using unnest($2::bigint[]) as seed.
        Each row returns (start_node, root_current, person_id, depth).
        PKs present in lookup but absent from resolve → Orphaned

Step 4: Classify each source
        ┌──────────────────────────────────────────────────────────────┐
        │ NotFound    → batch insert (Step 5)                         │
        │ Orphaned    → sequential unlink + link (Step 6, rare)       │
        │ Live, same person as target → skip                          │
        │ Live, different person → batch link root (Step 7)           │
        └──────────────────────────────────────────────────────────────┘

Step 5: Batch insert NotFound sources (2 queries)
        INSERT INTO distinct_id_mappings ... SELECT unnest($2::text[])
        INSERT INTO union_find ... for each new PK, pointing at target

Step 6: Process orphaned sources sequentially
        For each orphan: unlink_did + link_did (reuses existing helpers)
        Orphans are rare — only after delete_person — so sequential is fine.

Step 7: Batch link Live-different roots (2 queries, deduped by root)
        UPDATE union_find SET person_id = NULL, next = target_pk
        WHERE current = ANY(unique_root_currents)

        DELETE FROM person_mapping
        WHERE person_id = ANY(old_person_ids)

Step 8: set_identified + commit                    ← same as /merge
```

**Deduplication:** Sources are deduped before processing. Multiple sources sharing the same root (e.g., `b`, `c`, `d` all under person B) produce a single root link operation — root B is linked to target once, person B is deleted once. The target appearing in the source list resolves to "same person" and is skipped.

**SQL statement count comparison:**

| Scenario (S sources)       | `/merge` | `/batched_merge`      |
| -------------------------- | -------- | --------------------- |
| All Live, different person | 2 + 4S   | ~8                    |
| All NotFound               | 2 + 3S   | ~6                    |
| Mixed                      | 2 + 2-4S | ~8 + orphan_count * 3 |

**Correctness invariant:** Within a valid union-find tree, source chains never share intermediate nodes — they can only share roots. Deduplicating by `root_current` before the batch link step handles this correctly, producing identical observable results to the serial `/merge` path.

---

### Delete Person

`POST /delete_person` — Soft-delete a person and poison its union_find root. The distinct_ids are **not** eagerly cleaned up; they are lazily unlinked when next accessed by a write operation.

```
Before:
  "a1" ──► "a2" ──► root(P=7)

After:
  "a1" ──► "a2" ──► root(P=7, deleted_at=now())
                          │
           person_mapping: P=7 deleted_at=now()
```

**Step by step:**

```
Step 1: SELECT person_id FROM person_mapping
        WHERE team_id=$1 AND person_uuid=$2 AND deleted_at IS NULL

Step 2: UPDATE person_mapping SET deleted_at = now()
        WHERE person_id = $result

Step 3: UPDATE union_find SET deleted_at = now()
        WHERE team_id=$1 AND person_id=$result AND deleted_at IS NULL
```

**Why soft-delete?** Eagerly walking every chain to clean up all linked distinct_ids would be expensive and hold a long transaction. Instead, the root and person are timestamped. Future writes that encounter these nodes via `check_did` will see that `resolve_by_pk` returns `None` (the `person_mapping` join filters on `deleted_at IS NULL`) and trigger `unlink_did` to detach the orphaned distinct_id. This amortizes cleanup cost across future operations.

**DB operations:** 1 transaction, 3 SQL statements.

---

### Delete Distinct ID

`POST /delete_distinct_id` — Hard-delete a single distinct_id from the graph. This requires splicing the node out of the chain to maintain connectivity, then removing the `union_find` and `distinct_id_mappings` rows.

#### Deleting a leaf node (non-root, no children):

```
Before:                               After:
  "a1" ──► "a2" ──► root(P=7)          "a2" ──► root(P=7)

  Steps:
  1. unlink_did("a1"): parents of a1 inherit a1's next pointer
     (a1 has no parents in this example, so no splice needed)
  2. DELETE union_find WHERE current = a1_pk
  3. DELETE distinct_id_mappings WHERE id = a1_pk
```

#### Deleting a mid-chain node:

```
Before:                               After:
  "a1" ──► "a2" ──► "a3" ──► root      "a1" ──►──► "a3" ──► root

  unlink_did("a2"):
    UPDATE union_find SET next = a2.next   -- parents of a2 skip past it
    WHERE next = a2_pk

  Then hard-delete a2's rows.
```

#### Deleting a root node (most complex):

```
Before:
  "a1" ──► "a2" ──► root(P=7, current=12)
                      ▲
  "b1" ──────────────┘

After (a2 is promoted to root):
  "a1" ──► "a2" ──► root(P=7, current=11)    ← a2 promoted: inherits person_id
  "b1" ──┘                                     ← b1 redirected to new root

  Steps:
  1. Pick one parent of root (a2) as the promoted node
  2. Single UPDATE: clear old root's person_id, set promoted node's person_id
     (done atomically to avoid transient unique constraint violation)
  3. UPDATE remaining parents (b1): set next = promoted_pk
  4. DELETE old root's union_find and distinct_id_mappings rows
```

**If the deleted node was the last root for its person** (no remaining `union_find` row with that `person_id`), the `person_mapping` is also soft-deleted as a side effect.

**DB operations:** 1 transaction, 5-7 SQL statements depending on chain position.

---

## Path Compression

Path compression is the mechanism that keeps chains short. Without it, repeated merges could build arbitrarily deep chains, degrading resolve performance (each hop is a recursive CTE iteration).

Compression is triggered in two ways:
1. **After mutations** (`/alias`, `/identify`, `/merge`): the handler receives a `CompressHint` when the combined chain depth exceeds `PATH_COMPRESS_THRESHOLD`. It enqueues a `DbOp::CompressPath` to the worker channel in a background task (up to 3 retry attempts).
2. **After reads** (`/resolve`): if the resolved chain depth exceeds the threshold, a `try_send` enqueues compression (fire-and-forget; dropped if the channel is full).

**What compression does:**

```
Before compression (depth = 5):
  A ──► B ──► C ──► D ──► E ──► root(P=7)

After compression (depth = 1 for all):
  A ──► root(P=7)
  B ──┘
  C ──┘
  D ──┘
  E ──┘
```

**SQL implementation** (single statement within a transaction):

```
WITH RECURSIVE walk(node, depth) AS (
    SELECT start_pk, 0
    UNION ALL
    SELECT uf.next, w.depth + 1
    FROM walk w JOIN union_find uf ON ...
    WHERE w.depth < 1000
),
path_info AS (
    SELECT root_pk, max_depth FROM ...
)
UPDATE union_find uf
SET next = root_pk                           -- point directly at root
FROM path_info
WHERE max_depth >= threshold                 -- only if chain is deep enough
  AND uf.current IN (SELECT node FROM walk   -- all non-root nodes on the path
                     WHERE node != root_pk)
  AND uf.person_id IS NULL                   -- never touch the root row
```

The entire walk + update executes as one SQL statement inside one transaction. If compression races with a concurrent write that restructures the chain, the transaction sees a consistent snapshot and either compresses correctly or is a no-op.

**Compression is best-effort.** If the worker channel is full, compression is skipped and retried on the next access. This is safe because chains always remain correct regardless of depth — compression is purely an optimization.

---

## Lazy Unlink: Orphan Handling

When a person is soft-deleted (`/delete_person`), the distinct_ids that pointed to it are **not** immediately cleaned up. They become **orphans**: their union_find chain terminates at a root whose `person_mapping.deleted_at` is non-NULL.

Every write path (`/create`, `/alias`, `/identify`, `/merge`) uses `check_did` to detect orphans transparently:

```
check_did(distinct_id):
  1. lookup_did → did_pk
  2. resolve_by_pk (recursive CTE + person_mapping join)
     └─ if person_mapping.deleted_at IS NOT NULL → join fails → returns None
  3. If None: call unlink_did(did_pk) to detach from dead chain
  4. Return DidState::Orphaned(did_pk)
```

The caller then treats an orphaned distinct_id the same as a new one — it can be re-linked or promoted to a new root — but **reuses the existing `distinct_id_mappings` PK** instead of inserting a new row.

```
Example: create after person deletion

Before (person 7 was deleted):
  "anon_1" ──► root(P=7, deleted_at=<ts>)

POST /create { distinct_id: "anon_1" }

Step 1: check_did("anon_1") → Orphaned(pk=10)
  └─ unlink_did(pk=10): detach from dead chain, clear to orphan state

Step 2: identify_tx handles Orphaned:
  └─ Insert new person_mapping → P=NEW
  └─ root_did(pk=10, NEW) → promote orphaned row to root

After:
  "anon_1" ──► root(P=NEW)     (reused did_pk=10, new person)
```

This design amortizes deletion cleanup across future writes, avoiding expensive eager graph walks during the delete operation itself.
