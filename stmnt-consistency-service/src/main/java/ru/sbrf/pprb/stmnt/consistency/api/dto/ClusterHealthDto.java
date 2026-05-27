package ru.sbrf.pprb.stmnt.consistency.api.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Снимок здоровья кластеров для consumer'ов (например IgniteClusterPool
 * в getStatementSummary-сервисе).
 *
 *   asOf            — момент сборки снимка
 *   stalenessMs     — порог, после которого кластер считается "молчащим"
 *                     (last_event_ts < now() - stalenessMs)
 *   clusters        — карта clusterId → состояние
 *
 * Сериализуется как JSON, потребители строят weighted-random:
 *   weight = 1 / (max(0, mismatches) + 1)   — больше mismatches → реже выбирается
 *   isHealthy == false                       — кластер исключается из round'а
 */
public record ClusterHealthDto(
        Instant asOf,
        long stalenessMs,
        Map<String, ClusterState> clusters
) {
    public record ClusterState(
            String clusterId,
            /** Количество unresolved mismatch'ей в которых ЭТОТ кластер
             *  отличается от других. */
            int mismatches,
            /** max(event_ts) от CDC из hash_latest. null если ни одного события не было. */
            Instant lastEventTs,
            /** true если последнее CDC-событие свежее stalenessMs. */
            boolean isHealthy,
            /** Опционально: список логических cache'ей где у этого кластера расхождения. */
            List<String> divergentCaches
    ) {}
}
