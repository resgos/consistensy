-- Consistency check runs and per-record hashes from each cluster.

CREATE TABLE consistency_run (
    id             BIGSERIAL    PRIMARY KEY,
    started_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    cache_name     VARCHAR(64),                  -- NULL = all caches
    status         VARCHAR(16)  NOT NULL,        -- RUNNING | OK | MISMATCH | ERROR
    mismatch_count INTEGER      NOT NULL DEFAULT 0,
    error_message  TEXT
);

CREATE INDEX idx_run_status_started ON consistency_run(status, started_at DESC);

CREATE TABLE consistency_hash (
    run_id        BIGINT       NOT NULL REFERENCES consistency_run(id) ON DELETE CASCADE,
    cluster_id    VARCHAR(32)  NOT NULL,
    cache_name    VARCHAR(64)  NOT NULL,
    business_key  VARCHAR(512) NOT NULL,
    hash_hex      CHAR(32)     NOT NULL,
    PRIMARY KEY (run_id, cluster_id, cache_name, business_key)
);

CREATE INDEX idx_hash_lookup ON consistency_hash(cache_name, business_key, run_id);
