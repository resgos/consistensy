-- Unified error registry: a single place for every operational error/warning,
-- regardless of source (consistency_job, admin_fanout, cluster_reader, debug_seed, db).

CREATE TABLE consistency_error (
    id                BIGSERIAL    PRIMARY KEY,
    occurred_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    source            VARCHAR(32)  NOT NULL,           -- consistency_job | admin_fanout | cluster_reader | debug_seed | db | scheduler
    level             VARCHAR(16)  NOT NULL,           -- ERROR | WARN | INFO
    code              VARCHAR(64),                     -- semantic: CLUSTER_DOWN, SCHEMA_NOT_FOUND, HASH_MISMATCH, ...
    message           TEXT         NOT NULL,
    details_json      JSONB,                            -- {cluster, cache, register, exception_class, ...}
    related_run_id    BIGINT,                           -- nullable; if tied to a consistency_run
    cluster_id        VARCHAR(32),                      -- shortcut for filter
    cache_name        VARCHAR(64),                      -- shortcut for filter
    resolved_at       TIMESTAMPTZ,
    resolution_notes  TEXT
);

CREATE INDEX idx_err_recent     ON consistency_error(occurred_at DESC);
CREATE INDEX idx_err_unresolved ON consistency_error(occurred_at DESC) WHERE resolved_at IS NULL;
CREATE INDEX idx_err_by_source  ON consistency_error(source, occurred_at DESC);
CREATE INDEX idx_err_by_level   ON consistency_error(level, occurred_at DESC);
