package ru.sbrf.pprb.stmnt.consistency.lib;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.springframework.stereotype.Component;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;
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
    private final ErrorRegistry errors;
    private final ConsistencyProperties props;

    /**
     * @return businessKey -> hashHex; empty if cluster unavailable.
     */
    public Map<String, String> computeHashes(String clusterId, HashCalculator hasher) {
        IgniteClient client = clientFactory.get(clusterId);
        if (client == null) {
            log.warn("Cluster {} not connected, returning empty hash map for cache {}",
                    clusterId, hasher.cacheName());
            errors.record("cluster_reader", "WARN", "CLUSTER_DOWN",
                    "Cluster client not initialized",
                    Map.of("cluster", clusterId, "cache", hasher.cacheName()),
                    null, clusterId, hasher.cacheName());
            return Map.of();
        }

        Map<String, String> hashes = new HashMap<>();
        long t0 = System.nanoTime();

        // Try with hasher-specified schema first (production cache layout);
        // fall back to PUBLIC if the schema doesn't exist (sql-DDL-created tables).
        String sql = hasher.selectSql();
        if (!tryRunInto(client, sql, hasher, hashes)) {
            String stripped = stripSchemaPrefix(sql, hasher.cacheName());
            if (!stripped.equals(sql)) {
                log.debug("Cluster {} cache {}: fallback to PUBLIC schema",
                        clusterId, hasher.cacheName());
                if (!tryRunInto(client, stripped, hasher, hashes)) {
                    errors.record("cluster_reader", "ERROR", "QUERY_FAILED",
                            "Both schema and PUBLIC fallback failed",
                            Map.of("cluster", clusterId, "cache", hasher.cacheName(), "sql", sql),
                            null, clusterId, hasher.cacheName());
                }
            } else {
                errors.record("cluster_reader", "ERROR", "QUERY_FAILED",
                        "SQL execution failed",
                        Map.of("cluster", clusterId, "cache", hasher.cacheName(), "sql", sql),
                        null, clusterId, hasher.cacheName());
            }
        }
        long ms = (System.nanoTime() - t0) / 1_000_000;
        log.info("Hashed cluster={} cache={} records={} timeMs={}",
                clusterId, hasher.cacheName(), hashes.size(), ms);
        return hashes;
    }

    /** @return true if query succeeded (even if 0 rows); false on exception. */
    private boolean tryRunInto(IgniteClient client, String sql, HashCalculator hasher,
                                Map<String, String> hashes) {
        if (props.isDebugExplainPlans()) {
            explainAndLog(client, sql, hasher);
        }
        SqlFieldsQuery q = new SqlFieldsQuery(sql)
                .setArgs(hasher.queryParams())
                .setTimeout(60_000, java.util.concurrent.TimeUnit.MILLISECONDS);
        try (FieldsQueryCursor<List<?>> cursor = client.query(q)) {
            for (List<?> row : cursor) {
                String bk = hasher.rowToBusinessKey(row);
                if (bk == null) continue;
                hashes.put(bk, hasher.rowToHash(row));
            }
            return true;
        } catch (Exception e) {
            log.debug("query failed sql={}: {}", sql, e.toString());
            return false;
        }
    }

    /**
     * Run EXPLAIN against the same SQL+params and log the plan.
     * Runs only when consistency.debug.explain-plans=true. Errors are swallowed —
     * EXPLAIN is informational only.
     */
    private void explainAndLog(IgniteClient client, String sql, HashCalculator hasher) {
        // Some engines reject EXPLAIN on parameterized queries with unfilled args;
        // pass the same args. PUBLIC fallback (stripSchemaPrefix) also works for EXPLAIN.
        try (FieldsQueryCursor<List<?>> cursor = client.query(
                new SqlFieldsQuery("EXPLAIN " + sql).setArgs(hasher.queryParams()))) {
            StringBuilder sb = new StringBuilder();
            int line = 0;
            for (List<?> row : cursor) {
                if (line++ > 0) sb.append("\n  ");
                sb.append(row.get(0));
            }
            log.info("EXPLAIN cache={} sql='{}' plan:\n  {}",
                    hasher.cacheName(), abbreviate(sql), sb);
        } catch (Exception e) {
            log.debug("EXPLAIN failed (ignored): {}", e.toString());
        }
    }

    private static String abbreviate(String s) {
        return s.length() > 120 ? s.substring(0, 117) + "..." : s;
    }

    /**
     * Strip "CACHE_NAME." prefixes from SQL so it can run against PUBLIC schema.
     */
    static String stripSchemaPrefix(String sql, String cacheName) {
        return sql
                .replaceAll("(?i)\\b" + java.util.regex.Pattern.quote(cacheName) + "\\.", "")
                .replaceAll("(?i)\"" + java.util.regex.Pattern.quote(cacheName) + "\"\\.", "");
    }
}
