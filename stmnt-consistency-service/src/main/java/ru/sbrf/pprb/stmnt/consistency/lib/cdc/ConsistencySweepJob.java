package ru.sbrf.pprb.stmnt.consistency.lib.cdc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.sbrf.pprb.stmnt.consistency.lib.ErrorRegistry;
import ru.sbrf.pprb.stmnt.consistency.lib.events.DomainEvents;
import ru.sbrf.pprb.stmnt.consistency.lib.events.EventPublisher;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.ConsistencyRunRepository;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.HashLatestRepository;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.MismatchRepository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Sweep по consistency_hash_latest: GROUP BY (cache, key) HAVING
 * COUNT(DISTINCT hash) > 1 → mismatch.
 *
 * Активируется только когда consistency.cdc.enabled=true. Иначе работает
 * старый ConsistencyJob с SQL-pull через thin-client.
 *
 * Это лёгкая операция (один SQL в Postgres), безопасно запускать часто.
 * Cron по умолчанию каждые 5 минут.
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "consistency.cdc.enabled", havingValue = "true")
@RequiredArgsConstructor
public class ConsistencySweepJob {

    private final HashLatestRepository hashLatest;
    private final MismatchRepository mismatches;
    private final ConsistencyRunRepository runs;
    private final ErrorRegistry errors;
    private final EventPublisher events;

    @Scheduled(cron = "${consistency.cdc.sweep-cron:0 */5 * * * *}")
    public void scheduledSweep() {
        sweepOnce(null);
    }

    /** @param cacheFilter null = все кеши, иначе только указанный. */
    public long sweepOnce(String cacheFilter) {
        long runId = runs.createRunning(cacheFilter);
        events.publish(new DomainEvents.RunStarted(runId, cacheFilter, Instant.now()));
        int totalMismatches = 0;
        String status = "SWEEP_OK";
        String error = null;
        try {
            List<HashLatestRepository.Divergence> divs =
                    hashLatest.findDivergences(cacheFilter, 10_000);

            // Группируем по cache_name для batch-сохранения.
            java.util.Map<String, List<MismatchRepository.Mismatch>> byCache = new java.util.LinkedHashMap<>();
            for (var d : divs) {
                byCache.computeIfAbsent(d.cacheName(), k -> new ArrayList<>())
                        .add(new MismatchRepository.Mismatch(d.businessKey(), d.clusterHashes()));
            }

            Instant detectedAt = Instant.now();
            for (var entry : byCache.entrySet()) {
                mismatches.saveBatch(runId, entry.getKey(), entry.getValue());
                totalMismatches += entry.getValue().size();
                for (var mm : entry.getValue()) {
                    events.publish(new DomainEvents.MismatchDetected(
                            runId, entry.getKey(), mm.businessKey(),
                            mm.clusterHashes(), detectedAt));
                }
            }
            if (totalMismatches > 0) status = "SWEEP_MISMATCH";
            log.info("Sweep {} done: mismatches={} cacheFilter={}",
                    runId, totalMismatches, cacheFilter);
        } catch (Exception e) {
            status = "SWEEP_ERROR";
            error = e.getMessage();
            log.error("Sweep {} failed", runId, e);
            errors.error("cdc_sweep", "SWEEP_FAILED",
                    "Sweep " + runId + " failed", e,
                    Map.of("runId", runId, "cacheFilter", String.valueOf(cacheFilter)));
        } finally {
            runs.markFinished(runId, status, totalMismatches, error);
            events.publish(new DomainEvents.RunFinished(
                    runId, status, totalMismatches, error, Instant.now()));
        }
        return runId;
    }
}
