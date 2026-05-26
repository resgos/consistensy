package ru.sbrf.pprb.stmnt.consistency.lib.repo;

import ru.sbrf.pprb.stmnt.consistency.api.dto.ConsistencyRunDto;

import java.util.List;
import java.util.Optional;

/**
 * Lifecycle of consistency runs. Designed as a swappable abstraction:
 * the JDBC implementation can be replaced by a DataSpace-based one
 * without touching consumers.
 */
public interface ConsistencyRunRepository {

    /** Create a new run in RUNNING state, return its id. cacheName=null means "all caches". */
    long createRunning(String cacheName);

    /** Mark finished with terminal status (OK | MISMATCH | ERROR). */
    void markFinished(long id, String status, int mismatchCount, String errorMessage);

    Optional<ConsistencyRunDto> findById(long id);

    /** Most recent runs descending by started_at. If statusFilter==null — all. */
    List<ConsistencyRunDto> findRecent(int limit, String statusFilter);

    /** Cascade-deletes runs (and their hashes/mismatches) finished before now()-retentionDays. */
    int deleteOlderThan(int retentionDays);
}
