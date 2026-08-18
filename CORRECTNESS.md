# Correctness Guide

This document shows, with worked examples, why every read and mutation this service performs keeps the person ↔ distinct_id mappings correct. It is written for a reader who wants to *trust* the system, not implement it — for internals see [DESIGN.md](DESIGN.md).

The central idea: all mappings live in a forest of trees. Every mutation rewrites the **fewest possible pointers** — usually exactly one — and correctness of everyone else's mapping follows automatically from tree structure. Each section below shows one operation: the tree before, the tree after, and exactly which rows changed.

---

## Table of Contents

- [The Model in One Diagram](#the-model-in-one-diagram)
- [The Invariants](#the-invariants)
- [Reading: /resolve](#reading-resolve)
- [Reading: /resolve_distinct_ids](#reading-resolve_distinct_ids)
- [Mutation: /create](#mutation-create)
- [Mutation: /alias and /identify](#mutation-alias-and-identify)
- [Mutation: /merge and /batched_merge](#mutation-merge-and-batched_merge)
- [Mutation: /delete_person](#mutation-delete_person)
- [Mutation: /delete_distinct_id — variants by tree position](#mutation-delete_distinct_id--variants-by-tree-position)
- [Moving a distinct_id to a new person — variants by tree position](#moving-a-distinct_id-to-a-new-person--variants-by-tree-position)
- [Why So Few Rows? The Minimality Budget](#why-so-few-rows-the-minimality-budget)
- [Path Compression Is Correctness-Neutral](#path-compression-is-correctness-neutral)
- [Known Sharp Edges](#known-sharp-edges)

---

## The Model in One Diagram

Every distinct_id is one node. Nodes form trees. Each tree has exactly one **root**, and the root — only the root — carries the pointer to a person.

```
        Person P42 ("abc-def-123")
              ▲
              │ person_id          ┌── LEGEND ─────────────────────────┐
        ┌───────────┐             │ root:  next = NULL, person set    │
        │  user@x   │  ROOT       │ link:  next = set,  person NULL   │
        └───────────┘             │ arrows point child ──► parent     │
           ▲       ▲              └───────────────────────────────────┘
           │       │
     ┌─────────┐ ┌─────────┐
     │ anon_1  │ │ anon_2  │      ◄── links
     └─────────┘ └─────────┘
           ▲
           │
     ┌─────────┐
     │ anon_3  │                  ◄── link (a "grandchild")
     └─────────┘
```

To answer *"who is anon_3?"*, follow arrows upward until you hit the root: `anon_3 → anon_1 → user@x → Person P42`. Every question and every change in this system is some manipulation of these arrows.

---

## The Invariants

Every operation, at commit time, preserves all five. The test suite asserts them after every scenario.

1. **One node per distinct_id.** Each distinct_id maps to exactly one tree node.
2. **Root or link, never both.** A node either points at a person (root) or points at another node (link) — exactly one of the two.
3. **One root per person.** A person is referenced by at most one root per team (enforced by a unique index).
4. **No cycles.** Following arrows always terminates at a root. Merges only ever point a root of one tree into a *different* tree, so a loop cannot form.
5. **Isolation per team.** All keys include `team_id`; trees never span teams.

Why these five are enough: if every distinct_id sits in exactly one tree and every tree has exactly one person at its root, then "walk to the root" is a total, unambiguous answer for every distinct_id. Every proof in this document reduces to "the operation kept all five true."

---

## Reading: /resolve

**Question:** which person owns this distinct_id?
**Method:** walk upward to the root; return the root's person.

```
resolve("anon_3"):

   anon_3 ──► anon_1 ──► user@x ──► Person P42     answer: P42
     hop 1      hop 2      root
```

Reads never modify the tree. A read runs as one atomic SQL statement, so it always sees a complete, consistent tree — never a half-applied mutation. If the walk ends at a root whose person was soft-deleted, the answer is 404 (the distinct_id is an *orphan* — see [moving a distinct_id](#moving-a-distinct_id-to-a-new-person--variants-by-tree-position)).

---

## Reading: /resolve_distinct_ids

**Question:** which distinct_ids belong to this person?
**Method:** start at the person's root and walk *downward*, collecting every node whose arrows lead to it.

```
resolve_distinct_ids(P42):

        user@x  (root of P42)          collected, in waves outward:
         ▲   ▲
    anon_1   anon_2                    wave 1: anon_1, anon_2
      ▲
    anon_3                             wave 2: anon_3

   answer: [user@x, anon_1, anon_2, anon_3]
```

Correct for the same reason `/resolve` is: membership in a person is *defined* as "your walk ends at that person's root," and this traversal enumerates exactly those nodes. Results are capped at 10,000 with an `is_truncated` flag.

---

## Mutation: /create

**Goal:** get-or-create a person for one distinct_id. Three cases:

```
Case 1 — already exists:              nothing changes; resolve and return.

Case 2 — brand new:                   Case 3 — exists, but person deleted:
  before: (nothing)                     before:  a ──► P7 (deleted)
  after:  a ──► P_new                   after:   a ──► P_new

  rows: insert person, id, node         rows: insert person; rewrite a's
                                              one node to point at P_new
```

Case 3 is the first appearance of **orphan reuse**: the distinct_id row is kept, only its single pointer is rewritten. No other node is touched.

---

## Mutation: /alias and /identify

**Goal:** state that two distinct_ids are the same human. Both endpoints run the same logic (`/identify` names its second id `anonymous`, `/alias` names it `alias`). Four cases, by what already exists:

**Case 1 — both exist, already same person.** No structural change; just mark the person identified.

**Case 2 — both exist, different persons.** This is a true merge. The *entire* source tree changes hands via **one pointer rewrite**:

```
before:                                  after alias(target=t1, source=s1):

  t2 ──► t1 (root, P7)                    t2 ──► t1 (root, P7 ✓identified)
                                                  ▲
  s2 ──► s1 (root, P9)                    s2 ──► s1        ◄── was a root;
                                                               now a link to t1
rows touched: s1's node (1 UPDATE), delete person P9, mark P7 identified.
s2 — and any number of nodes below s1 — are untouched, yet all now
resolve to P7, because their walks pass through s1 into t1's tree.
```

That is the core minimality win: merging trees of *any* size is one row update, and correctness for every descendant is transitive — their arrows didn't change, only where the path ultimately lands.

**Guard:** if the source person was already identified, the call is refused (409). Silently merging two *known* humans is dangerous; the caller must use `/merge` to force it.

**Case 3 — one exists, the other doesn't (or is an orphan).** The missing id is created (or the orphan reused) and linked to the existing node. One insert or one pointer rewrite.

**Case 4 — neither exists.** Create one new person with the target as root and the source linked under it.

---

## Mutation: /merge and /batched_merge

**Goal:** force-merge N source distinct_ids into the target's person, ignoring the identified guard (PostHog's `$merge_dangerously`). Each source is classified independently and gets the cheapest correct treatment:

```
merge(target = "user@x" (person P7), sources = [a, b, c, d]):

  a: unknown id            ──► insert one new node linked to user@x
  b: same person already   ──► skip (already correct)
  c: root/member of P9     ──► rewrite P9's ROOT to link to user@x;
                               delete person P9 (its whole tree moves)
  d: orphan (person dead)  ──► detach and relink its one node

before:                              after:
                                            user@x (root, P7 ✓)
  user@x (P7)    c ──► c_root(P9)           ▲   ▲   ▲    ▲
  b ──┘                                     b   a  c_root d
                                                    ▲
                                                    c    (untouched)
```

Note the source `c` case: the operation resolves `c` to its **root** and rewrites the root — not `c` itself. `c`'s own pointer never changes; it comes along because its path now flows through the rewritten root. Merging is always "re-point the root."

`/batched_merge` produces the **identical end state** — it only batches the SQL (one lookup, one bulk tree-walk, bulk inserts/updates) instead of handling sources one at a time. A dedicated test asserts merge and batched_merge yield structurally identical trees for the same input.

---

## Mutation: /delete_person

**Goal:** delete a person. This is a *soft* delete — two timestamps, no tree surgery:

```
before:                              after:
  a ──► b ──► root(P7)                a ──► b ──► root(P7, deleted ✝)
                                                   person P7 deleted ✝

rows touched: person row + its one root row.
```

The tree is left standing but *dead*: every walk still terminates at the root, sees the deleted person, and answers 404. The member distinct_ids (`a`, `b`) are now **orphans**. They are not cleaned up here — cleanup would mean walking a potentially huge tree inside the delete. Instead each orphan is detached lazily the next time something writes to it (next section). Deletion cost stays O(1) no matter the tree size, and correctness holds throughout because dead trees answer consistently: nobody.

---

## Mutation: /delete_distinct_id — variants by tree position

**Goal:** hard-delete one distinct_id, keeping everyone else's mapping intact. What must change depends entirely on **where the node sits in its tree**. In every variant, only the node's *direct children* are ever rewritten — the rest of the tree never moves.

**Variant 1 — leaf (nothing points at it):**

```
before:  a ──► b ──► root(P7)          after:  b ──► root(P7)

delete "a": no children to fix. Delete its 2 rows. Done.
rows rewritten: 0
```

**Variant 2 — interior node (children point at it): splice.**

Children are re-pointed *past* the node, to the node's own parent. Grandchildren don't move — their paths still flow through the (re-pointed) children.

```
before:                                after delete "b":
  a1 ──► b ──► c ──► root(P7)            a1 ──► c ──► root(P7)
  a2 ──┘                                 a2 ──┘
   ▲                                      ▲
  a3  (grandchild)                       a3   (untouched, still correct)

rows rewritten: a1, a2   (the direct children only)
```

**Variant 3 — root with children: promote one child.**

The person pointer must survive, and exactly one node may hold it (invariant 3). So one child is promoted to become the new root in a single atomic step, and the *other* direct children are re-pointed to it.

```
before:                                after delete root "r":
       r (root, P7)                          b (root, P7)   ◄── promoted
     ▲   ▲   ▲                              ▲   ▲
     a   b   c                              a   c            ◄── re-pointed
     ▲                                      ▲
     a1                                     a1               (untouched)

rows rewritten: b (promoted), a and c (re-pointed). Which child is
promoted is arbitrary — any choice yields a correct tree.
```

**Variant 4 — sole root (no children):** the person's last distinct_id is gone, so the person is soft-deleted as a side effect (`person_deleted: true` in the response).

```
before:  solo ──► P7                   after:  (rows deleted)  P7 deleted ✝
```

---

## Moving a distinct_id to a new person — variants by tree position

This is the subtlest path in the system. After `/delete_person`, its distinct_ids sit in a **dead tree**. When a later write (`/create`, `/alias`, `/identify`, `/merge`) touches one of them, that single id must move to a live person — **without dragging its old tree-mates along**, because they were only ever related through the now-deleted person.

The move is two steps: **detach** from the dead tree (variant depends on position, mirroring the delete variants above), then **reattach** wherever the write says.

**Position A — leaf or interior of the dead tree: splice out.**

The moving node's direct children are re-pointed to its former parent. Everything else in the dead tree stays exactly where it was — still dead, still resolving to 404, still individually recoverable later.

```
dead tree before:                     write: create("b")

  a ──► b ──► c ──► root(P7 ✝)        detach b (splice):    reattach b:
        ▲
        a2                              a ────► c ──► root(P7 ✝)   b ──► P_new
                                        a2 ──┘

rows rewritten: a, a2 (b's direct children), b itself. Nothing else.
a, a2, c remain orphans of the dead tree — each will get this same
treatment if and when a write touches *it*.
```

**Position B — root of the dead tree: promote, then move.**

The dead root can't just vanish — its children still need a dead tree to belong to. So one child is promoted to be the new *dead* root (it inherits the deleted-person pointer and the deletion mark), the other children are re-pointed to it, and only then does the original node move.

```
dead tree before:                     write: alias(target="live", source="r")

       r (root, P7 ✝)                 detach r (promote a):     reattach r:
     ▲   ▲
     a   b                                 a (root, P7 ✝)        r ──► live ──► P42
                                           ▲
                                           b

rows rewritten: a (promoted dead root), b (re-pointed), r (moved).
```

**Position C — sole node of the dead tree:** nothing to splice or promote; rewrite the node's one pointer and go.

**Why this is correct:** the moved id resolves to its new live person; every id left behind still resolves to the deleted person (404) — no orphan ever silently inherits a *live* identity it wasn't explicitly given. And in all three variants the write touched only the moving node and its direct children.

---

## Why So Few Rows? The Minimality Budget

Every mutation has a fixed, position-dependent row budget — never proportional to tree size (with the one noted exception):

| Operation | union_find rows written | Depends on tree size? |
|---|---|---|
| `/create` (new) | 1 insert | no |
| `/create` (orphan reuse) | 1 update + detach cost | no — direct children only |
| `/alias` — merge two persons | **1** (source root) | **no — any size tree moves** |
| `/alias` — attach new id | 1 insert | no |
| `/merge` per source | ≤ 1 (that source's root) | no |
| `/delete_person` | 1 (root) + person row | no |
| `/delete_distinct_id` | promoted + direct children | children count only |
| orphan detach (lazy unlink) | direct children only | children count only |
| path compression | every node on one path | yes — but optional, see below |

Two structural facts make this possible:

1. **Identity is transitive through pointers.** A node's person is defined by where its walk *ends*, not by anything stored on the node. Re-pointing one ancestor re-maps an entire subtree at zero cost to the subtree.
2. **Only direct children ever reference a node.** So removing or moving a node requires fixing exactly its direct children — grandchildren reference the children, which didn't move.

---

## Path Compression Is Correctness-Neutral

Long chains make reads slower (more hops), so a background task periodically flattens them:

```
before (5 hops):                       after (1 hop each):
  a ─► b ─► c ─► d ─► root               a ─► root
                                         b ─► root
                                         c ─► root
                                         d ─► root
```

Every node still walks to the *same* root, so every answer is unchanged — compression rewrites path shape, never membership. It is fired best-effort after deep reads and writes; if it never runs, answers are still correct, just slower. Compression of a dead tree points nodes at the dead root and changes nothing observable.

---

## Known Sharp Edges

Two implementation-level edge cases are known and tracked (found in review, reproduced against a live database); they bound the guarantees above:

1. **Root promotion can abort.** The atomic promote step (delete-root variant 3, move variant B) can fail with a unique-index error when the promoted child's internal id sorts before the old root's. The transaction rolls back cleanly — no corruption — but the operation returns 500 for that topology.
2. **Walks cap at 1000 hops.** A chain deeper than 1000 (possible only if compression persistently fails to run) resolves as "not found," and a write could then mistake a live id for an orphan. Compression normally keeps depth near the threshold (default 20), far from the cap.
