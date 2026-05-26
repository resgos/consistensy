package ru.sbrf.pprb.stmnt.consistency.lib.repo;

import ru.sbrf.pprb.stmnt.consistency.api.dto.ErrorEntryDto;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Unified error registry. Append-only events from any source
 * (consistency_job | cluster_reader | admin_fanout | debug_seed | db | scheduler).
 */
public interface ErrorRepository {

    /** Filter spec for find(). */
    record Filter(
            String source,
            String level,
            String code,
            String clusterId,
            String cacheName,
            Instant since,
            boolean unresolvedOnly
    ) {}

    /** Append a new error/warning event. Must NOT throw — failure to write must be swallowed and logged. */
    void insert(String source, String level, String code, String message,
                String detailsJson, Long relatedRunId, String clusterId, String cacheName);

    List<ErrorEntryDto> find(Filter filter, int limit);

    /** by_source / by_level / by_code → list of {value, count}. */
    Map<String, List<Map<String, Object>>> aggregateStats(Instant since);

    int resolve(long id, String notes);
}
