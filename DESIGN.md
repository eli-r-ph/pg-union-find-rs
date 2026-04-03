# Design

This document describes the architecture and operation of `pg-union-find-rs`, a Postgres-backed identity resolution service. It maps **distinct_ids** — device fingerprints, login handles, anonymous tokens, or any other string identifier — to canonical **person** records. The mapping is stored as a union-find data structure inside Postgres, queried with recursive CTEs, and served over a JSON HTTP API.

If you are unfamiliar with union-find or recursive CTEs, the two background sections below give you enough context to follow the rest of the document.

---

## Table of Contents

- [Background: Union-Find in 60 Seconds](#background-union-find-in-60-seconds)
- [Background: Recursive CTEs in 60 Seconds](#background-recursive-ctes-in-60-seconds)
- [System Architecture](#system-architecture)
- [Data Model](#data-model)
- [How Chains Work](#how-chains-work)
- [Write Path: Worker Design](#write-path-worker-design)
- [API Operations](#api-operations)
  - [POST /resolve](#post-resolve)
  - [POST /resolve_distinct_ids](#post-resolve_distinct_ids)
  - [POST /create](#post-create)
  - [POST /alias and POST /identify](#post-alias-and-post-identify)
  - [POST /merge](#post-merge)
  - [POST /batched_merge](#post-batched_merge)
  - [POST /delete_person](#post-delete_person)
  - [POST /delete_distinct_id](#post-delete_distinct_id)
- [Path Compression](#path-compression)
- [Lazy Unlink: Orphan Handling](#lazy-unlink-orphan-handling)

---

## Background: Union-Find in 60 Seconds

Union-find (also called "disjoint-set") is a data structure for tracking which items belong to the same group. It supports two operations:

**Find** — given an item, follow pointers until you reach the group's representative (the "root"). If two items share the same root, they belong to the same group.

```
Find: which group does C belong to?

  C ──► B ──► A (root)        answer: A
        follow pointers ──►
```

**Union** — merge two groups by making one root point to the other. Every item that used to reach the old root now reaches the new one.

```
Before union:                     After union(A, X):

  Group 1       Group 2            Group 1 (merged)
  C ──► B ──► A   Y ──► X          C ──► B ──► A
                                    Y ──► X ──► A   ← X now points to A
```

Over time, chains can grow long. **Path compression** is an optimization that shortens chains by pointing every node directly at the root (covered in [Path Compression](#path-compression)).

In this project, each "item" is a distinct_id and each "root" carries a pointer to a person record. Finding a distinct_id's root tells you which person it belongs to. Unioning two distinct_ids merges their persons.

---

## Background: Recursive CTEs in 60 Seconds

A recursive CTE (Common Table Expression) is a SQL query that builds its result set iteratively, row by row. It has two parts: a **seed** query that produces the first row, and a **recursive** query that produces additional rows by referencing the rows already collected. Iteration stops when the recursive part produces no new rows.

This project uses recursive CTEs to walk union-find chains. Here is the core pattern, annotated:

```sql
WITH RECURSIVE walk(node, depth) AS (
    --  Seed: start at the given node
    SELECT start_pk, 0

    UNION ALL

    -- Recursive step: follow the chain one hop
    SELECT uf.next, w.depth + 1
    FROM walk w
    JOIN union_find uf
      ON uf.team_id = $team AND uf.current = w.node
     AND uf.person_id IS NULL    -- stop when we reach a root
    WHERE w.depth < 1000         -- safety cap
)
SELECT node, depth FROM walk ORDER BY depth DESC LIMIT 1
```

Walking through an example chain:

```
Chain:   10 ──► 11 ──► 12 (root, person_id = 7)

Iteration 0 (seed):     node = 10, depth = 0
Iteration 1 (recurse):  node = 11, depth = 1    (10's next is 11)
Iteration 2 (recurse):  node = 12, depth = 2    (11's next is 12)
Iteration 3:            stops — node 12 has person_id = 7, so the
                         JOIN condition (person_id IS NULL) fails.

Result: node = 12 (the root), depth = 2.
The root's person_id (7) is then joined to person_mapping to get the UUID.
```

The CTE runs as a single SQL statement — Postgres handles the iteration internally. No application-level loop is needed.

---

## System Architecture

```
                        HTTP clients
                 ┌──────────┼──────────┐
                 │          │          │
                 ▼          ▼          ▼
            ┌──────────────────────────────┐
            │        Axum HTTP Server       │
            │        (JSON POST APIs)       │
            └────┬─────────────────┬───────┘
                 │                 │
         ┌───── │ ─────┐          │
         │  WRITE PATH │     READ PATH
         │             │          │
         ▼             │          ▼
  ┌─────────────┐      │   ┌──────────────┐
  │  Shard by   │      │   │  Direct DB   │
  │  team_id %N │      │   │  pool.acquire │
  └──┬──┬──┬──┬─┘      │   └──────┬───────┘
     │  │  │  │        │          │
     ▼  ▼  ▼  ▼        │          │
  ┌────┬────┬────┐      │          │
  │ W0 │ W1 │ WN │◄─────┘          │
  └─┬──┴─┬──┴─┬──┘                 │
    │    │    │  sequential FIFO    │
    │    │    │  per worker          │
    ▼    ▼    ▼                     │
  ┌─────────────────────────────────┤
  │         PostgreSQL              │
  │  ┌───────────────────────────┐  │
  │  │    person_mapping         │  │
  │  │    distinct_id_mappings   │  │
  │  │    union_find             │  │
  │  └───────────────────────────┘  │
  └─────────────────────────────────┘
```

**Key design properties:**

- **Writes are serialized per shard.** Each worker task owns a bounded `mpsc` channel and processes operations strictly in FIFO order. `team_id % N` determines the shard, so writes for the same team are always serialized. Different teams on the same shard also serialize — an acceptable trade-off for simplicity.
- **Reads bypass the worker pool.** `/resolve` and `/resolve_distinct_ids` acquire a connection directly from the Postgres pool and run read-only recursive CTEs. Reads scale independently and never block behind queued writes.
- **Backpressure via bounded channels.** Each worker channel has a bounded capacity (default 64). Enqueue attempts timeout after 100ms, returning HTTP 503 rather than allowing unbounded memory growth.
- **DB connection budget.** `max_connections = WORKER_POOL_SIZE + 1`. Each worker holds at most one connection; the +1 covers direct-pool reads.

---

## Data Model

Three tables form the storage layer:

```
┌──────────────────────────────┐     ┌────────────────────────────────┐
│       person_mapping          │     │     distinct_id_mappings        │
├──────────────────────────────┤     ├────────────────────────────────┤
│ person_id    BIGSERIAL  PK    │     │ id            BIGSERIAL  PK    │
│ team_id      BIGINT           │     │ team_id       BIGINT           │
│ person_uuid  VARCHAR(200)     │     │ distinct_id   VARCHAR(200)     │
│ is_identified BOOLEAN         │     └────────────────────────────────┘
│ deleted_at   TIMESTAMPTZ      │          UNIQUE (team_id, distinct_id)
└──────────────────────────────┘
     UNIQUE (team_id, person_uuid)

┌───────────────────────────────────────────────────────────────────┐
│                          union_find                                │
├───────────────────────────────────────────────────────────────────┤
│ team_id     BIGINT  ─┐                                            │
│ current     BIGINT  ─┘ PK      ── references distinct_id_mappings │
│ next        BIGINT  NULLABLE   ── references distinct_id_mappings │
│ person_id   BIGINT  NULLABLE   ── references person_mapping       │
│ deleted_at  TIMESTAMPTZ                                           │
└───────────────────────────────────────────────────────────────────┘
  UNIQUE (team_id, person_id) WHERE person_id IS NOT NULL
  INDEX  (team_id, next)      WHERE next IS NOT NULL
```

**What each table does:**

- **`person_mapping`** is the authoritative record for a person: their external UUID, whether they have been identified, and soft-delete state. One row per person per team.
- **`distinct_id_mappings`** translates string distinct_ids to compact bigint PKs. This keeps union_find chain walks fast — they use integer joins, never string comparisons.
- **`union_find`** encodes the graph structure. Each row is a node in the union-find forest. Chains are walked by following `current → next` pointers until a root is reached.

---

## How Chains Work

The `union_find` table represents a **directed forest** — a collection of trees where each node points toward its tree's root. Each node is a row keyed by `(team_id, current)`.

A node is a **root** when: `next IS NULL AND person_id IS NOT NULL`.
A node is **non-root** when: `next IS NOT NULL AND person_id IS NULL`.

The root's `person_id` joins to `person_mapping` to retrieve the external UUID.

```
Example: three distinct_ids belonging to the same person

distinct_id_mappings:             union_find:
┌────┬───────────┐               ┌─────────┬──────┬───────────┐
│ id │ distinct   │               │ current │ next │ person_id │
├────┼───────────┤               ├─────────┼──────┼───────────┤
│ 10 │ "anon_1"  │               │   10    │  11  │   NULL    │  ← non-root
│ 11 │ "anon_2"  │               │   11    │  12  │   NULL    │  ← non-root
│ 12 │ "user@x"  │               │   12    │ NULL │     7     │  ← root
└────┴───────────┘               └─────────┴──────┴───────────┘

Resolving "anon_1":

  anon_1 (pk=10)            anon_2 (pk=11)           user@x (pk=12)
  ┌──────────────┐          ┌──────────────┐         ┌──────────────┐
  │ next = 11    │────►     │ next = 12    │────►    │ next = NULL  │
  │ person_id =  │          │ person_id =  │         │ person_id = 7│──► person_mapping
  │    NULL      │          │    NULL      │         └──────────────┘    person_uuid =
  └──────────────┘          └──────────────┘              ROOT          "abc-def-123"
```

**Why no rank?** Classical union-find uses "union by rank" to keep trees balanced. This implementation skips rank — chains grow by simple appending, and depth is bounded by periodic [path compression](#path-compression). This keeps the write path simple (one UPDATE per merge) and avoids an extra column.

---

## Write Path: Worker Design

All write endpoints (`/create`, `/alias`, `/identify`, `/merge`, `/batched_merge`, `/delete_person`, `/delete_distinct_id`) flow through a pool of worker tasks. Each worker owns a bounded `mpsc` channel and processes operations sequentially.

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

The handler creates a `(oneshot::Sender, oneshot::Receiver)` pair, wraps the sender into a `DbOp` enum variant, sends it over the bounded mpsc channel, and awaits the receiver. The worker executes the operation inside a Postgres transaction and sends the result back through the oneshot.

**Why serialize writes?** Identity operations for the same team must not race. Two concurrent `/alias` calls could both read the chain, decide to merge, and produce conflicting link structures. Serialization avoids this without row-level locking or retry loops. Different teams may land on the same shard and serialize against each other, but this is an acceptable trade-off.

---

## API Operations

### POST /resolve

**Purpose:** Look up the canonical person for a distinct_id. This is the primary read operation.

**Request:**
```json
{ "team_id": 1, "distinct_id": "anon_1" }
```

**Response (200):**
```json
{ "person_uuid": "abc-def-123", "is_identified": false }
```

**How it works:**

Resolve bypasses the worker pool entirely — it acquires a DB connection directly from the pool.

```
Step 1: Look up the distinct_id's internal PK

  SELECT id FROM distinct_id_mappings
  WHERE team_id = $1 AND distinct_id = $2
                                                         ┌──────────────┐
Step 2: Walk the chain via recursive CTE                 │  "anon_1"    │
                                                         │  pk = 10     │
  Start at pk=10, follow next pointers                   └──────┬───────┘
  until a root is reached (person_id IS NOT NULL).              │ next
  Join the root's person_id to person_mapping.                  ▼
                                                         ┌──────────────┐
  If the chain depth exceeds PATH_COMPRESS_THRESHOLD,    │  "user@x"    │
  a background compression task is enqueued               │  pk = 12     │
  (fire-and-forget; dropped if the channel is full).     │  person_id=7 │ ROOT
                                                         └──────┬───────┘
                                                                │
                                                                ▼
                                                         person_mapping
                                                         person_uuid = "abc-def-123"
```

**Errors:**

| Status | Condition |
|--------|-----------|
| 404    | distinct_id not found or resolves to a soft-deleted person |
| 500    | DB connection or query failure |

---

### POST /resolve_distinct_ids

**Purpose:** Reverse lookup — given a person UUID, return all distinct_ids that belong to that person. Like `/resolve`, this bypasses the worker pool.

**Request:**
```json
{ "team_id": 1, "person_uuid": "abc-def-123" }
```

**Response (200):**
```json
{ "person_uuid": "abc-def-123", "distinct_ids": ["user@x", "anon_1", "anon_2"], "is_truncated": false }
```

**How it works:**

```
Step 1: Look up the person's internal person_id

  SELECT person_id FROM person_mapping
  WHERE team_id = $1 AND person_uuid = $2 AND deleted_at IS NULL

Step 2: Walk the tree in reverse via recursive CTE

  The CTE starts at the root (the union_find row with that person_id),
  then recursively finds every node whose "next" points to an already-
  collected node. This expands outward from root to all leaves.

  Example tree for person_id = 7:

                  ┌──────────┐
                  │  root    │ ◄── start here (person_id = 7)
                  │  pk = 12  │
                  └─┬──────┬─┘
            next=12 │      │ next=12
                    ▼      ▼
              ┌────────┐ ┌────────┐
              │ pk=11  │ │ pk=13  │   ◄── found in iteration 1
              └───┬────┘ └────────┘
          next=11 │
                  ▼
              ┌────────┐
              │ pk=10  │                  ◄── found in iteration 2
              └────────┘

  Result distinct_ids: ["user@x", "anon_2", "anon_3", "anon_1"]
```

The reverse CTE SQL:

```sql
WITH RECURSIVE tree(node) AS (
    SELECT uf.current
    FROM union_find uf
    WHERE uf.team_id = $1 AND uf.person_id = $2 AND uf.deleted_at IS NULL

    UNION ALL

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

The `LIMIT 10001` allows truncation detection: if more than 10,000 rows come back, the response sets `is_truncated: true` and returns exactly 10,000.

**Errors:**

| Status | Condition |
|--------|-----------|
| 404    | person not found or soft-deleted |
| 500    | DB connection or query failure |

---

### POST /create

**Purpose:** Get-or-create a person for a single distinct_id. If the distinct_id already maps to a live person, return it. Otherwise create a new person.

**Request:**
```json
{ "team_id": 1, "distinct_id": "anon_1" }
```

**Response (200):**
```json
{ "person_uuid": "abc-def-123", "is_identified": false }
```

**How it works (three cases):**

**Case 1 — distinct_id already exists and is live:**

No rows are inserted. The chain is traversed and the existing person is returned.

```
Before:                              After (no change):
  "anon_1" ──► root(P=7)              "anon_1" ──► root(P=7)

Response: person_uuid of P=7
```

**Case 2 — distinct_id does not exist:**

A new person, distinct_id mapping, and root union_find row are created.

```
Before:                              After:
  (nothing)                            "anon_1" ──► root(P=NEW)

Inserts: 1 person_mapping + 1 distinct_id_mappings + 1 union_find
```

**Case 3 — distinct_id exists but its person was soft-deleted (orphaned):**

The distinct_id is detached from the dead chain, and a new person is created reusing the existing distinct_id PK. See [Lazy Unlink](#lazy-unlink-orphan-handling) for details.

```
Before:                              After:
  "anon_1" ──► root(P=7)              "anon_1" ──► root(P=NEW)
               deleted_at=<ts>

Steps:
  1. check_did detects orphan (person_mapping.deleted_at IS NOT NULL)
  2. unlink_did: detach from dead chain
  3. Insert new person_mapping row
  4. Promote did_pk to root for the new person
```

**DB operations:** 1 transaction, 2-4 SQL statements depending on case.

**Errors:**

| Status | Condition |
|--------|-----------|
| 400    | distinct_id fails validation (empty, too long, control chars, blocklist) |
| 503    | worker queue full or timeout |
| 500    | DB error |

---

### POST /alias and POST /identify

**Purpose:** Link two distinct_ids under a single person and mark the result as `is_identified = true`. This is how an anonymous session gets connected to a known login.

`/identify` accepts `{ team_id, target, anonymous }` and maps it to the same handler as `/alias` with `source = anonymous`.
`/alias` accepts `{ team_id, target, alias }` and maps it to `source = alias`.

**Request (/alias):**
```json
{ "team_id": 1, "target": "user@example.com", "alias": "anon_session_42" }
```

**Request (/identify):**
```json
{ "team_id": 1, "target": "user@example.com", "anonymous": "anon_session_42" }
```

**Response (200):**
```json
{ "person_uuid": "abc-def-123", "is_identified": true }
```

**How it works (four cases):**

Both target and source are looked up via `check_did`, which transparently handles orphaned distinct_ids (see [Lazy Unlink](#lazy-unlink-orphan-handling)). The result depends on the state of each:

**Case 1 — Both live, same person (no-op merge):**

They already belong to the same person. Just mark identified.

```
Before:                                   After:
  "target" ──► root(P=7)                   "target" ──► root(P=7, identified=true)
  "source" ──┘                              "source" ──┘

Only change: UPDATE person_mapping SET is_identified = true WHERE person_id = 7
```

**Case 2 — Both live, different persons (merge):**

The source's root is demoted to a non-root pointing at the target's chain. The source person is deleted.

```
Before:
  "target" ──► root_T(P=7)               "source" ──► root_S(P=9)

After:
  "target" ──► root_T(P=7, identified)
  "source" ──► root_S ──┘
                  │
                  person_id = NULL (demoted)
                  next = root_T's pk
                  person_mapping for P=9 DELETED

A deeper example:
  Before:
    "a1" ──► "a2" ──► root_T(P=7)      "b1" ──► "b2" ──► root_S(P=9)

  After:
    "a1" ──► "a2" ──► root_T(P=7, identified)
    "b1" ──► "b2" ──► old_root_S ──┘
                          │
                          next = root_T's pk, person_id = NULL
```

Steps:
1. `resolve_root(source)` to find `root_S`
2. `UPDATE union_find SET person_id = NULL, next = target_pk WHERE current = root_S`
3. `DELETE FROM person_mapping WHERE person_id = 9`
4. `UPDATE person_mapping SET is_identified = true WHERE person_id = 7`

If the combined chain depth exceeds `PATH_COMPRESS_THRESHOLD`, background compression is enqueued.

**Constraint:** if the source person is already `is_identified = true`, the merge is refused with HTTP 409. The caller must use `/merge` to force-merge identified persons.

**Case 3 — One live, the other new or orphaned:**

The non-live distinct_id is created (or reused if orphaned) and linked into the live chain.

```
Before:                                   After:
  "target" ──► root(P=7)                   "target" ──► root(P=7, identified)
  "source" does not exist                   "source" ──┘

  Insert source into distinct_id_mappings.
  Insert union_find row: { current=source_pk, next=target_pk, person_id=NULL }
```

The symmetric case (source is live, target is new) works identically — the new distinct_id is attached into the existing chain.

**Case 4 — Neither exists:**

A new person is created along with both distinct_ids.

```
Before:                                   After:
  (nothing for either)                      "target" ──► root(P=NEW, identified)
                                            "source" ──┘
```

**DB operations:** 1 transaction, 3-8 SQL statements depending on case.

**Errors:**

| Status | Condition |
|--------|-----------|
| 400    | either distinct_id fails validation |
| 409    | source person is already identified (use /merge to force) |
| 503    | worker queue full or timeout |
| 500    | DB error |

---

### POST /merge

**Purpose:** Force-merge N source distinct_ids into a target's person. Unlike `/alias`, this **ignores `is_identified`** and always merges. Equivalent to PostHog's `$merge_dangerously`.

**Request:**
```json
{ "team_id": 1, "target": "user@x", "sources": ["anon_1", "anon_2", "anon_3"] }
```

**Response (200):**
```json
{ "person_uuid": "abc-def-123", "is_identified": true }
```

**How it works:**

The target is resolved first. It must exist (live or orphaned); NotFound returns 404. Each source is then classified and handled:

```
Before:
  "user@x" ──► root(P=7)        "anon_1" ──► root(P=9)       "anon_2" ──► root(P=11)

  "anon_3" does not exist

After:
  "user@x" ──► root(P=7, identified)
  "anon_1" ──► old_root_9 ──┘            person_mapping P=9  DELETED
  "anon_2" ──► old_root_11 ──┘           person_mapping P=11 DELETED
  "anon_3" ──┘                            (new DID, linked directly to target)
```

Per-source classification:

```
For each source:
  ┌─────────────────────────────────────────────────────────────────────┐
  │ check_did(source) →                                                │
  │                                                                     │
  │ NotFound ──► insert new distinct_id_mappings + union_find row       │
  │              pointing at target_pk                                  │
  │                                                                     │
  │ Orphaned ──► reuse existing DID pk, link union_find to target_pk   │
  │                                                                     │
  │ Live, same person as target ──► skip (already merged)              │
  │                                                                     │
  │ Live, different person ──► demote source root:                     │
  │                             set person_id=NULL, next=target_pk     │
  │                             DELETE source person_mapping           │
  └─────────────────────────────────────────────────────────────────────┘

Finally: set_identified(target_person_id)
```

All sources are merged within a single transaction.

**DB operations:** 1 transaction, `2 + 2-4 * len(sources)` SQL statements (target check_did + per-source check_did + link + set_identified).

**Errors:**

| Status | Condition |
|--------|-----------|
| 400    | any distinct_id fails validation |
| 404    | target distinct_id not found |
| 503    | worker queue full or timeout |
| 500    | DB error |

---

### POST /batched_merge

**Purpose:** Same semantics as `/merge` — identical request and response shapes, identical observable result — but uses batched SQL to reduce DB round-trips.

**Request:**
```json
{ "team_id": 1, "target": "user@x", "sources": ["anon_1", "anon_2", "anon_3"] }
```

**Response (200):**
```json
{ "person_uuid": "abc-def-123", "is_identified": true }
```

**How it works:**

Instead of N individual check_did + link operations, the batched path resolves and links sources in bulk:

```
Step 1: check_did(target)               ← same as /merge
        NotFound → 404 error
        Orphaned → create new person, promote to root
        Live     → use existing

Step 2: Batch lookup all source strings (1 query)
        SELECT id, distinct_id FROM distinct_id_mappings
        WHERE team_id = $1 AND distinct_id = ANY($2::text[])
        → builds map: string → did_pk
        Strings absent from result → classified NotFound

Step 3: Batch resolve all found PKs to roots (1 recursive CTE)
        Walks all chains simultaneously using unnest($2::bigint[]) as seed.
        Returns (start_node, root_current, person_id, depth) per chain.
        PKs found in step 2 but absent from resolve → classified Orphaned

Step 4: Classify each source
        ┌─────────────────────────────────────────────────────────┐
        │ NotFound               → batch insert (Step 5)          │
        │ Orphaned               → sequential unlink+link (Step 6)│
        │ Live, same person      → skip                           │
        │ Live, different person → batch link root (Step 7)       │
        └─────────────────────────────────────────────────────────┘

Step 5: Batch insert NotFound sources (2 queries)
        INSERT INTO distinct_id_mappings ... SELECT unnest($2::text[])
        INSERT INTO union_find row per new PK, each pointing at target

Step 6: Process orphaned sources sequentially (rare — only after delete_person)
        For each orphan: unlink_did + link_did

Step 7: Batch link Live-different roots (2 queries, deduped by root)
        UPDATE union_find SET person_id = NULL, next = target_pk
        WHERE current = ANY(unique_root_currents)

        DELETE FROM person_mapping WHERE person_id = ANY(old_person_ids)

Step 8: set_identified(target) + COMMIT
```

**Deduplication:** Sources sharing the same root produce a single root-link operation. The target appearing in the source list resolves to "same person" and is skipped.

**SQL statement count comparison:**

| Scenario (S sources)       | `/merge`   | `/batched_merge`      |
| -------------------------- | ---------- | --------------------- |
| All Live, different person | 2 + 4S     | ~8                    |
| All NotFound               | 2 + 3S     | ~6                    |
| Mixed                      | 2 + 2-4S   | ~8 + orphan_count * 3 |

**Errors:** Same as `/merge`.

---

### POST /delete_person

**Purpose:** Soft-delete a person. The person_mapping and union_find root rows get a `deleted_at` timestamp. The distinct_ids that belonged to this person are **not** eagerly cleaned up — they become orphans and are lazily detached by future write operations.

**Request:**
```json
{ "team_id": 1, "person_uuid": "abc-def-123" }
```

**Response (200):**
```json
{ "person_uuid": "abc-def-123" }
```

**How it works:**

```
Before:
  "a1" ──► "a2" ──► root(P=7)
                     person_mapping: P=7, deleted_at = NULL

After:
  "a1" ──► "a2" ──► root(P=7, deleted_at = now())
                     person_mapping: P=7, deleted_at = now()
```

Steps:
1. `SELECT person_id FROM person_mapping WHERE team_id=$1 AND person_uuid=$2 AND deleted_at IS NULL`
2. `UPDATE person_mapping SET deleted_at = now() WHERE person_id = $result`
3. `UPDATE union_find SET deleted_at = now() WHERE team_id=$1 AND person_id=$result`

**Why soft-delete?** Eagerly walking every chain to clean up all linked distinct_ids would be expensive and hold a long transaction. Instead, future writes that encounter these nodes detect the deleted person via `check_did` and detach the orphaned distinct_id on the spot. See [Lazy Unlink](#lazy-unlink-orphan-handling).

**DB operations:** 1 transaction, 3 SQL statements.

**Errors:**

| Status | Condition |
|--------|-----------|
| 404    | person not found or already deleted |
| 503    | worker queue full or timeout |
| 500    | DB error |

---

### POST /delete_distinct_id

**Purpose:** Hard-delete a single distinct_id from the graph. The union_find row and distinct_id_mappings row are permanently removed. If the distinct_id was the last root for its person, the person is soft-deleted as a side effect.

**Request:**
```json
{ "team_id": 1, "distinct_id": "anon_1" }
```

**Response (200):**
```json
{ "distinct_id": "anon_1", "person_deleted": false }
```

**How it works (three cases depending on the node's position in the chain):**

**Case 1 — Deleting a leaf node (non-root with no children):**

The simplest case. The node has no parents pointing to it (or its parents are redirected past it), then its rows are deleted.

```
Before:                                  After:
  "a1" ──► "a2" ──► root(P=7)             "a2" ──► root(P=7)

  unlink_did("a1"): a1 has no parents, so no splice needed.
  DELETE union_find WHERE current = a1_pk
  DELETE distinct_id_mappings WHERE id = a1_pk
```

**Case 2 — Deleting a mid-chain node:**

Parents of the deleted node must be redirected to skip past it.

```
Before:                                  After:
  "a1" ──► "a2" ──► "a3" ──► root         "a1" ──────► "a3" ──► root

  unlink_did("a2"):
    UPDATE union_find SET next = a2.next    -- a1 now skips past a2
    WHERE team_id=$1 AND next = a2_pk

  Then hard-delete a2's union_find and distinct_id_mappings rows.
```

**Case 3 — Deleting a root node (most complex):**

One parent is promoted to become the new root. All other parents are redirected to the promoted node.

```
Before:
  "a1" ──► "a2" ──► root(P=7, current=12)
                      ▲
  "b1" ──────────────┘

After (a2 is promoted to root):
  "a1" ──► "a2"(P=7)    ← a2 promoted: inherits person_id, becomes root
  "b1" ──┘                ← b1 redirected to point at a2

Steps:
  1. Pick one parent of root (a2) as the promoted node
  2. Single atomic UPDATE: clear old root's person_id, set promoted node's
     person_id and next=NULL (done in one statement to avoid transient
     unique constraint violations on the person_id index)
  3. UPDATE remaining parents (b1): set next = promoted_pk
  4. DELETE old root's union_find and distinct_id_mappings rows
```

**Cascading person soft-delete:** After deletion, if no union_find row still references the person_id (no live root exists for that person), the person_mapping row is soft-deleted. This sets `person_deleted: true` in the response.

**DB operations:** 1 transaction, 5-7 SQL statements depending on chain position.

**Errors:**

| Status | Condition |
|--------|-----------|
| 404    | distinct_id not found |
| 503    | worker queue full or timeout |
| 500    | DB error |

---

## Path Compression

Path compression keeps chains short. Without it, repeated merges could build arbitrarily deep chains, and each extra hop is one more iteration of the recursive CTE during resolve.

**What compression does:**

```
Before compression (depth = 5):
  A ──► B ──► C ──► D ──► E ──► root(P=7)

After compression (depth = 1 for all non-root nodes):
  A ──► root(P=7)
  B ──┘
  C ──┘
  D ──┘
  E ──┘
```

Every non-root node on the path is updated to point directly at the root.

**When compression is triggered:**

1. **After writes** (`/alias`, `/identify`, `/merge`): the operation returns a `CompressHint` when the combined chain depth exceeds `PATH_COMPRESS_THRESHOLD` (default 20). The handler enqueues a `CompressPath` operation to the worker channel in a background task (up to 3 retry attempts with backoff).
2. **After reads** (`/resolve`): if the resolved chain depth exceeds the threshold, a `try_send` fires compression into the worker channel. This is fire-and-forget — if the channel is full, the attempt is silently dropped.

**SQL implementation** (single statement within a transaction):

```sql
WITH RECURSIVE walk(node, depth) AS (
    SELECT $2::bigint, 0
    UNION ALL
    SELECT uf.next, w.depth + 1
    FROM walk w
    JOIN union_find uf
      ON uf.team_id = $1 AND uf.current = w.node AND uf.person_id IS NULL
    WHERE w.depth < 1000
),
path_info AS (
    SELECT
        (SELECT node FROM walk ORDER BY depth DESC LIMIT 1) AS root_pk,
        (SELECT MAX(depth) FROM walk) AS max_depth
)
UPDATE union_find uf
SET next = pi.root_pk
FROM path_info pi
WHERE pi.max_depth >= $3
  AND uf.team_id = $1
  AND uf.current IN (SELECT node FROM walk WHERE node != pi.root_pk)
  AND uf.person_id IS NULL
```

The entire walk + update runs as one SQL statement in one transaction. If compression races with a concurrent write, it sees a consistent MVCC snapshot and either compresses correctly or becomes a no-op.

**Compression is best-effort.** If the worker channel is full, compression is skipped and retried on the next access. Chains are always correct regardless of depth — compression is purely a performance optimization.

---

## Lazy Unlink: Orphan Handling

When a person is soft-deleted (`/delete_person`), the distinct_ids that pointed to it are **not** immediately cleaned up. They become **orphans** — their chain terminates at a root whose `person_mapping.deleted_at` is non-NULL.

Every write operation (`/create`, `/alias`, `/identify`, `/merge`) uses `check_did` to detect and handle orphans transparently:

```
check_did(distinct_id):

  1. lookup_did(distinct_id) → did_pk
     ┌──────────────┐
     │ If not found  │──► return NotFound
     └───────┬──────┘
             │ found pk
             ▼
  2. resolve_by_pk(did_pk): recursive CTE walks chain to root,
     JOINs person_mapping WHERE deleted_at IS NULL
     ┌──────────────┐
     │ If person is  │
     │ live (join    │──► return Live(did_pk, person, depth)
     │ succeeds)     │
     └───────┬──────┘
             │ join fails (person deleted)
             ▼
  3. unlink_did(did_pk): detach from the dead chain
  4. Return Orphaned(did_pk)
```

The caller then treats an orphaned distinct_id like a new one — it can be re-linked to a new root or promoted to root itself — but **reuses the existing `distinct_id_mappings` PK** instead of inserting a new row.

**Example — create after person deletion:**

```
Before (person 7 was soft-deleted):
  "anon_1" ──► root(P=7, deleted_at=<ts>)

POST /create { distinct_id: "anon_1" }

  Step 1: check_did("anon_1") → Orphaned(pk=10)
          └─ unlink_did(pk=10): detach from dead chain

  Step 2: identify_tx handles Orphaned:
          └─ Insert new person_mapping → P=NEW
          └─ root_did(pk=10, NEW): promote orphaned row to root

After:
  "anon_1" ──► root(P=NEW)       (reused did_pk=10, fresh person)
```

This design amortizes deletion cleanup across future writes, avoiding expensive eager graph walks during the delete operation itself.
