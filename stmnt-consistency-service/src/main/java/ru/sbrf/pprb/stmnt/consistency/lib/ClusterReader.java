package ru.sbrf.pprb.stmnt.consistency.lib;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.springframework.stereotype.Component;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;
import ru.sbrf.pprb.stmnt.consistency.lib.hash.HashCalculator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a cache from a single cluster via thin client SQL and builds
 * businessKey -> hashHex map using the provided HashCalculator.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ClusterReader {

    private final IgniteClientFactory clientFactory;

    /**
     * @return businessKey -> hashHex; empty if cluster unavailable.
     */
    public Map<String, String> computeHashes(String clusterId, HashCalculator hasher) {
        IgniteClient client = clientFactory.get(clusterId);
        if (client == null) {
            log.warn("Cluster {} not connected, returning empty hash map for cache {}",
                    clusterId, hasher.cacheName());
            return Map.of();
        }

        Map<String, String> hashes = new HashMap<>();
        SqlFieldsQuery q = new SqlFieldsQuery(hasher.selectSql())
                .setArgs(hasher.queryParams())
                .setTimeout(60_000, java.util.concurrent.TimeUnit.MILLISECONDS);

        long t0 = System.nanoTime();
        try (FieldsQueryCursor<List<?>> cursor = client.query(q)) {
            for (List<?> row : cursor) {
                String bk = hasher.rowToBusinessKey(row);
                if (bk == null) continue;
                hashes.put(bk, hasher.rowToHash(row));
            }
        } catch (Exception e) {
            log.error("computeHashes failed cluster={} cache={}: {}",
                    clusterId, hasher.cacheName(), e.toString(), e);
            return Map.of();
        }
        long ms = (System.nanoTime() - t0) / 1_000_000;
        log.info("Hashed cluster={} cache={} records={} timeMs={}",
                clusterId, hasher.cacheName(), hashes.size(), ms);
        return hashes;
    }
}
