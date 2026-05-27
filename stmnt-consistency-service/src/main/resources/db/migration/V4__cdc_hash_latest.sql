-- Зеркало текущего состояния кешей по CDC из Kafka.
--
-- Структура: одна строка на (cluster_id, cache_name, business_key). На каждый
-- CDC-event делаем UPSERT с обновлением hash + last_ts + op.
--
-- Sweep-job делает GROUP BY (cache_name, business_key) и помечает строки где
-- COUNT(DISTINCT hash) > 1 как mismatch.
--
-- Эта таблица — не source of truth (она eventually consistent с Ignite), а
-- проекция CDC-потока. Если consumer отстал — её состояние тоже отстаёт.
CREATE TABLE consistency_hash_latest (
    cluster_id    VARCHAR(64)  NOT NULL,
    cache_name    VARCHAR(64)  NOT NULL,
    business_key  VARCHAR(256) NOT NULL,
    hash          VARCHAR(64)  NOT NULL,
    op            VARCHAR(16)  NOT NULL,           -- PUT | REMOVED
    event_ts      TIMESTAMPTZ  NOT NULL,           -- момент события на Ignite
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (cluster_id, cache_name, business_key)
);

-- Sweep ходит по (cache_name, business_key); индекс для GROUP BY.
CREATE INDEX idx_hash_latest_cache_key
    ON consistency_hash_latest (cache_name, business_key);

-- Иногда нужно "вычистить" REMOVED-tombstones когда они уже подтверждены
-- всеми кластерами — индекс по op + updated_at для retention job.
CREATE INDEX idx_hash_latest_op_updated
    ON consistency_hash_latest (op, updated_at)
    WHERE op = 'REMOVED';


-- Offset storage: храним последний прочитанный (topic, partition) -> offset.
-- Используется только при ручном seek (REST /api/consistency/offsets/seek);
-- штатный committed offset хранит Kafka consumer group.
CREATE TABLE consistency_cdc_offset (
    cluster_id  VARCHAR(64)  NOT NULL,
    topic       VARCHAR(128) NOT NULL,
    partition   INT          NOT NULL,
    last_offset BIGINT       NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (cluster_id, topic, partition)
);


-- Sweep-runs переиспользуют consistency_run (та же таблица, что и pull-режим):
-- статусы расширены: SWEEP_OK | SWEEP_MISMATCH | SWEEP_ERROR. Колонка cache_name
-- хранит cache-фильтр (NULL = все). Это позволяет MismatchRepository.saveBatch
-- работать без изменений: run_id один, неважно sweep это или pull.
