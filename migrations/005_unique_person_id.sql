DROP INDEX idx_uf_person;
CREATE UNIQUE INDEX idx_uf_person ON union_find (team_id, person_id) WHERE person_id IS NOT NULL;
