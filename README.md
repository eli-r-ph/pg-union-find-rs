# pg-union-find-rs

A Postgres-backed union-find service for person/distinct_id resolution, modeled after PostHog's identity merge system. Built with Rust (Axum + SQLx + Tokio).

## Architecture

- **Three tables:** `person_mapping`, `distinct_id_mappings`, `union_find` — the union_find table forms a linked chain of distinct_id PKs traversed by a recursive CTE. Root rows carry the `person_id`.
- **Worker pool:** HTTP handlers partition operations by `team_id` into N bounded channels, serializing same-team writes while different teams run in parallel.
- **Endpoints:** `/create` (get-or-create person), `/identify` (link anonymous to primary), `/alias` (link alias to primary), `/merge` (force-merge N distinct_ids).

### Schema

```
person_mapping:       person_id (PK bigserial), team_id, person_uuid
distinct_id_mappings: id (PK bigserial), team_id, distinct_id
union_find:           (team_id, current) PK, next (nullable), person_id (nullable)
```

- `person_mapping` maps internal bigint PKs to external person UUIDs.
- `distinct_id_mappings` maps `(team_id, distinct_id)` strings to internal bigint PKs.
- `union_find` rows are chain links: `current -> next -> ... -> root`. A root row has `next = NULL` and `person_id` set.

### Operations

- **Read (resolve):** Look up distinct_id PK, walk union_find chain via recursive CTE to root, join person_mapping to return `person_uuid`.
- **`/create`** `{ team_id, distinct_id }`: Get-or-create a person for a single distinct_id. If the distinct_id exists, returns the resolved person. Otherwise creates `person_mapping` + `distinct_id_mappings` + `union_find` root row.
- **`/identify`** `{ team_id, primary, anonymous }`: Link an anonymous distinct_id to a primary. Routes to the same 4-case merge logic as `/alias`. When `primary == anonymous`, acts as get-or-create for that single distinct_id.
- **`/alias`** `{ team_id, primary, alias }`: Link an alias distinct_id to a primary. Handles 4 cases:
  - One exists, the other doesn't: link the new one into the existing chain.
  - Both exist, same person: no-op.
  - Both exist, different persons: reject (caller must use `/merge`).
  - Neither exists: create a new person with both distinct_ids.
  - When `primary == alias`, acts as get-or-create for that single distinct_id.
- **`/merge`** `{ team_id, src, dests }`: Force-merge N dests into src (`$merge_dangerously`). For each dest: if new, create link to src; if existing with a different person, re-point dest's chain root to src's person.

## Running

```bash
docker compose up -d
cargo run --release --bin migrate
cargo run --release
```

## Benchmark

```bash
# Reset DB and run the benchmark directly:
bin/reset-db.sh
cargo run --release --bin bench

# Or use the full bootstrap script (tears down Docker, rebuilds, runs):
bin/run-bench.sh
```

Tunable via env vars (defaults in parentheses):

| Env Var | Default | Description |
|---------|---------|-------------|
| `BENCH_TEAMS` | auto (N_WARM/1000) | Number of team_ids |
| `BENCH_WARM` | 100,000 | Phase 1 person count |
| `BENCH_ALIAS` | 100,000 | Phase 2 alias count |
| `BENCH_MERGE` | 100,000 | Phase 3 merge distinct_id count |
| `BENCH_BATCH` | 10 | Phase 3 sub-batch size |
| `BENCH_CHAIN_DEPTH` | 100 | Phase 3b max union_find chain depth |
| `BENCH_READS` | 1,000,000 | Phase 4 read count |
| `BENCH_DB_POOL` | 50 | Max DB connections |
