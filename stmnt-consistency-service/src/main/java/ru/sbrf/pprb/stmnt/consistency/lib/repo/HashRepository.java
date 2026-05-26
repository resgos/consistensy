package ru.sbrf.pprb.stmnt.consistency.lib.repo;

import java.util.Map;

/**
 * Append-only storage of (run, cluster, cache, businessKey) -> hashHex entries.
 * Used for audit, ad-hoc forensics and historical re-comparisons.
 */
public interface HashRepository {

    /**
     * Batch insert. Implementations should be idempotent on (run, cluster, cache, businessKey).
     */
    void saveBatch(long runId, String clusterId, String cacheName, Map<String, String> hashes);
}
