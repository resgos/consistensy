package ru.sbrf.pprb.stmnt.consistency.lib.events;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Доменные события consistency-service.
 *
 * Каждое событие — Java record, сериализуется в JSON для Kafka. Структура
 * стабильная (часть контракта с внешними consumer'ами). Если меняешь поле —
 * либо новая версия события (новое имя/топик), либо backward-compat миграция.
 *
 * Топик в Kafka = {prefix}.{entity}.{event}, prefix настраивается в
 * application.yml (consistency.events.topic-prefix), по умолчанию
 * "stmnt-consistency". Партиционирование по eventKey() для упорядоченности
 * в рамках одного runId / mismatchId / errorId.
 */
public sealed interface DomainEvents {

    /** Тип события (для маршрутизации в топик). */
    String topicSuffix();

    /** Ключ для Kafka-партиционирования. Гарантирует упорядоченность по сущности. */
    String eventKey();

    // ---- Run lifecycle ------------------------------------------------------

    record RunStarted(long runId, String cacheName, Instant startedAt) implements DomainEvents {
        public String topicSuffix() { return "run.started"; }
        public String eventKey()    { return String.valueOf(runId); }
    }

    record RunFinished(long runId, String status, int mismatchCount,
                       String errorMessage, Instant finishedAt) implements DomainEvents {
        public String topicSuffix() { return "run.finished"; }
        public String eventKey()    { return String.valueOf(runId); }
    }

    // ---- Mismatch -----------------------------------------------------------

    record MismatchDetected(long runId, String cacheName, String businessKey,
                            Map<String, String> clusterHashes,
                            Instant detectedAt) implements DomainEvents {
        public String topicSuffix() { return "mismatch.detected"; }
        public String eventKey()    { return cacheName + ":" + businessKey; }
    }

    record MismatchResolved(long mismatchId, String notes,
                             Instant resolvedAt) implements DomainEvents {
        public String topicSuffix() { return "mismatch.resolved"; }
        public String eventKey()    { return String.valueOf(mismatchId); }
    }

    // ---- Error registry -----------------------------------------------------

    record ErrorRecorded(String source, String level, String code, String message,
                          Map<String, Object> details,
                          Long relatedRunId, String clusterId, String cacheName,
                          Instant occurredAt) implements DomainEvents {
        public String topicSuffix() { return "error.recorded"; }
        public String eventKey()    { return source + ":" + (code == null ? "_" : code); }
    }

    // ---- Admin operations ---------------------------------------------------

    record AdminOperationCompleted(String operation, String registerId,
                                    List<String> clusterIds,
                                    Map<String, Map<String, Object>> perCluster,
                                    Instant completedAt) implements DomainEvents {
        public String topicSuffix() { return "admin.completed"; }
        public String eventKey()    { return operation + ":" + (registerId == null ? "_" : registerId); }
    }
}
