-- Union-find schema for person/distinct_id resolution.
--
-- Terminology mapping to PostHog ingestion:
--   "known"   = PostHog's "target" (mergeIntoDistinctId) — the distinct_id whose person survives a merge
--   "unknown"  = PostHog's "source" (otherPersonDistinctId) — the distinct_id whose person is absorbed
--
-- String identifiers live in `persons` and `distinct_ids`. The union-find
-- override table (`person_overrides`) operates purely on internal bigint PKs
-- for fast CTE traversal.

CREATE TABLE persons (
    team_id        BIGINT       NOT NULL,
    person_id      VARCHAR(200) NOT NULL,
    id             BIGSERIAL    NOT NULL UNIQUE,
    is_identified  BOOLEAN      NOT NULL DEFAULT FALSE,
    PRIMARY KEY (team_id, person_id)
);

CREATE TABLE distinct_ids (
    team_id     BIGINT       NOT NULL,
    distinct_id VARCHAR(200) NOT NULL,
    person_id   BIGINT       NOT NULL,  -- references persons.id (internal bigint)
    PRIMARY KEY (team_id, distinct_id)
);

-- Reverse lookup: find all distinct_ids belonging to a given internal person.
CREATE INDEX idx_distinct_ids_person ON distinct_ids (team_id, person_id);

-- Union-find override chain. Each row says "old_person_id was merged into
-- override_person_id". The recursive CTE follows these hops to find the
-- canonical (root) person.
CREATE TABLE person_overrides (
    team_id            BIGINT NOT NULL,
    old_person_id      BIGINT NOT NULL,  -- references persons.id
    override_person_id BIGINT NOT NULL,  -- references persons.id
    PRIMARY KEY (team_id, old_person_id),
    CHECK (old_person_id != override_person_id)
);
