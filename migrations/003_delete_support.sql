CREATE INDEX idx_uf_next ON union_find (team_id, next) WHERE next IS NOT NULL;

ALTER TABLE person_mapping ADD COLUMN deleted_at TIMESTAMPTZ;
