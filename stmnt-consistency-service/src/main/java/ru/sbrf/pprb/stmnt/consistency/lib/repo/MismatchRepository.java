package ru.sbrf.pprb.stmnt.consistency.lib.repo;

import ru.sbrf.pprb.stmnt.consistency.api.dto.MismatchDto;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Detected mismatches between clusters.
 */
public interface MismatchRepository {

    /** One entry per business key with diverging hashes. */
    record Mismatch(String businessKey, Map<String, String> clusterHashes) {}

    /** Batch save of detected mismatches for one run + cache. */
    void saveBatch(long runId, String cacheName, List<Mismatch> mismatches);

    /**
     * Search.
     *  cacheName       — optional filter
     *  since           — only mismatches detected at or after this instant
     *  unresolvedOnly  — exclude those that were resolved
     */
    List<MismatchDto> find(String cacheName, Instant since, boolean unresolvedOnly, int limit);

    /** Mark resolved with optional notes. Returns updated row count. */
    int resolve(long id, String notes);
}
