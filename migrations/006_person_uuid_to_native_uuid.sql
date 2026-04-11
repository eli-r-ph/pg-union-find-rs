-- Migrate person_uuid from VARCHAR(200) to native UUID type.
-- Reduces index size from ~200 bytes to 16 bytes per entry.
-- PostgreSQL automatically rebuilds idx_person_mapping_lookup.

ALTER TABLE person_mapping
    ALTER COLUMN person_uuid TYPE UUID USING person_uuid::uuid;
