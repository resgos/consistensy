-- Detected mismatches between clusters.

CREATE TABLE consistency_mismatch (
    id           BIGSERIAL    PRIMARY KEY,
    run_id       BIGINT       NOT NULL REFERENCES consistency_run(id) ON DELETE CASCADE,
    cache_name   VARCHAR(64)  NOT NULL,
    business_key VARCHAR(512) NOT NULL,
    hashes_json  JSONB        NOT NULL,  -- {"cluster-1": "abc...", "cluster-2": "xyz...", "cluster-3": "MISSING"}
    detected_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    resolved_at  TIMESTAMPTZ,
    notes        TEXT
);

CREATE INDEX idx_mismatch_unresolved_recent
    ON consistency_mismatch(detected_at DESC)
    WHERE resolved_at IS NULL;

CREATE INDEX idx_mismatch_by_cache
    ON consistency_mismatch(cache_name, detected_at DESC);
