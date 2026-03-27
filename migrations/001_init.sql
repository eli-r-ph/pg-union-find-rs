-- Union-find schema for person/distinct_id resolution.
--
-- Three tables:
--   person_mapping       — maps internal bigint PKs to external person UUIDs
--   distinct_id_mappings — maps (team_id, distinct_id) strings to internal bigint PKs
--   union_find           — linked chain of distinct_id PKs; root rows carry person_id
--
-- The recursive CTE walks union_find rows by following `current -> next` until
-- it reaches a root (person_id IS NOT NULL, next IS NULL).

CREATE TABLE person_mapping (
    person_id   BIGSERIAL    PRIMARY KEY,
    team_id     BIGINT       NOT NULL,
    person_uuid VARCHAR(200) NOT NULL
);

CREATE UNIQUE INDEX idx_person_mapping_lookup ON person_mapping (team_id, person_uuid);

CREATE TABLE distinct_id_mappings (
    id          BIGSERIAL    PRIMARY KEY,
    team_id     BIGINT       NOT NULL,
    distinct_id VARCHAR(200) NOT NULL
);

CREATE UNIQUE INDEX idx_did_lookup ON distinct_id_mappings (team_id, distinct_id);

CREATE TABLE union_find (
    team_id    BIGINT NOT NULL,
    current    BIGINT NOT NULL,  -- references distinct_id_mappings.id
    next       BIGINT,           -- references distinct_id_mappings.id (NULL = root)
    person_id  BIGINT,           -- references person_mapping.person_id (non-NULL = root)
    PRIMARY KEY (team_id, current)
);

CREATE INDEX idx_uf_person ON union_find (team_id, person_id) WHERE person_id IS NOT NULL;
