package ru.sbrf.pprb.stmnt.consistency.lib;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;
import ru.sbrf.pprb.stmnt.consistency.lib.hash.HashCalculator;
import ru.sbrf.pprb.stmnt.consistency.lib.hash.Hashers;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.ConsistencyRunRepository;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.HashRepository;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.MismatchRepository;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Orchestrates consistency runs:
 *  - for each cache, reads hashes from all clusters in parallel
 *  - detects mismatches and persists everything to PG
 *
 * Trigger: hourly cron + REST-initiated ad-hoc.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ConsistencyJob {

    private final ConsistencyProperties props;
    private final ClusterReader reader;
    private final ConsistencyRunRepository runs;
    private final HashRepository hashStore;
    private final MismatchRepository mismatches;
    private final Hashers hashers;
    private final ErrorRegistry errors;

    public static final String MISSING = "MISSING";

    /** Thread per cluster — read in parallel, but each cluster sequentially over caches. */
    private final ExecutorService clusterPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "consistency-cluster");
        t.setDaemon(true);
        return t;
    });

    @Scheduled(cron = "${consistency.cron:0 5 * * * *}")
    public void scheduledRun() {
        log.info("Scheduled consistency run starting");
        runAll(null);
    }

    /**
     * Synchronously run consistency check.
     * @param onlyCache  if non-null, run only this cache; else all configured caches.
     * @return runId
     */
    public long runAll(String onlyCache) {
        long runId = runs.createRunning(onlyCache);
        int totalMismatches = 0;
        String status = "OK";
        String error = null;
        try {
            Map<String, HashCalculator> registry = hashers.registry();
            List<String> targets = onlyCache != null
                    ? List.of(onlyCache)
                    : props.getCaches();

            for (String cacheName : targets) {
                HashCalculator h = registry.get(cacheName);
                if (h == null) {
                    log.warn("No hasher for cacheName={}, skipping", cacheName);
                    continue;
                }
                int m = processOneCache(runId, h);
                totalMismatches += m;
            }
            if (totalMismatches > 0) status = "MISMATCH";
        } catch (Exception e) {
            log.error("Run {} failed", runId, e);
            status = "ERROR";
            error = e.getMessage();
            errors.error("consistency_job", "RUN_FAILED",
                    "Run " + runId + " failed", e,
                    java.util.Map.of("runId", runId, "cache", String.valueOf(onlyCache)));
        } finally {
            runs.markFinished(runId, status, totalMismatches, error);
            log.info("Run {} done: status={} mismatches={}", runId, status, totalMismatches);
        }
        return runId;
    }

    private int processOneCache(long runId, HashCalculator h) {
        log.info("Run {} cache {} starting", runId, h.cacheName());

        // Read from each cluster in parallel.
        List<String> clusterIds = props.getClusters().stream()
                .map(ConsistencyProperties.Cluster::getId).toList();
        Map<String, CompletableFuture<Map<String, String>>> futures = new LinkedHashMap<>();
        for (String cid : clusterIds) {
            futures.put(cid, CompletableFuture.supplyAsync(
                    () -> reader.computeHashes(cid, h), clusterPool));
        }

        Map<String, Map<String, String>> byCluster = new LinkedHashMap<>();
        for (var entry : futures.entrySet()) {
            byCluster.put(entry.getKey(), entry.getValue().join());
        }

        // Persist raw hashes for audit.
        for (var entry : byCluster.entrySet()) {
            hashStore.saveBatch(runId, entry.getKey(), h.cacheName(), entry.getValue());
        }

        // Union of business keys.
        Set<String> allKeys = new HashSet<>();
        for (Map<String, String> m : byCluster.values()) allKeys.addAll(m.keySet());

        List<MismatchRepository.Mismatch> detected = new ArrayList<>();
        for (String bk : allKeys) {
            Map<String, String> hashesForKey = new LinkedHashMap<>();
            String firstHash = null;
            boolean allEqual = true;
            for (String cid : clusterIds) {
                String hash = byCluster.get(cid).getOrDefault(bk, MISSING);
                hashesForKey.put(cid, hash);
                if (firstHash == null) firstHash = hash;
                else if (!firstHash.equals(hash)) allEqual = false;
            }
            if (!allEqual) {
                detected.add(new MismatchRepository.Mismatch(bk, hashesForKey));
            }
        }
        mismatches.saveBatch(runId, h.cacheName(), detected);
        if (!detected.isEmpty()) {
            errors.record("consistency_job", "WARN", "HASH_MISMATCH",
                    "Detected " + detected.size() + " hash mismatches",
                    java.util.Map.of("cache", h.cacheName(), "count", detected.size(),
                            "examples", detected.stream().limit(3)
                                    .map(MismatchRepository.Mismatch::businessKey).toList()),
                    runId, null, h.cacheName());
        }
        log.info("Run {} cache {} mismatches={}", runId, h.cacheName(), detected.size());
        return detected.size();
    }

    /** Daily cleanup: purge old runs (and cascaded hashes/mismatches). */
    @Scheduled(cron = "0 30 3 * * *")
    public void purge() {
        int deleted = runs.deleteOlderThan(props.getHashRetentionDays());
        log.info("Retention purge: deleted {} runs older than {} days",
                deleted, props.getHashRetentionDays());
    }
}
