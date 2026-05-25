package ru.sbrf.pprb.stmnt.consistency.lib;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Central place to record any operational error or warning.
 *
 * Append-only; intentionally infallible — if the registry itself fails, we
 * log to slf4j and proceed (don't break business operations because PG is down).
 *
 * Sources (free-form, but please reuse):
 *   - consistency_job
 *   - cluster_reader
 *   - admin_fanout
 *   - debug_seed
 *   - db
 *   - scheduler
 *
 * Common codes (free-form):
 *   - CLUSTER_DOWN, SCHEMA_NOT_FOUND, QUERY_TIMEOUT, HASH_MISMATCH,
 *     INIT_FAILED, CLEANUP_FAILED, MIGRATION_FAILED, SQL_ERROR
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ErrorRegistry {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public void record(String source, String level, String code, String message,
                       Map<String, Object> details) {
        record(source, level, code, message, details, null, null, null);
    }

    public void record(String source, String level, String code, String message,
                       Map<String, Object> details,
                       Long relatedRunId, String clusterId, String cacheName) {
        try {
            String json = details == null || details.isEmpty()
                    ? null
                    : mapper.writeValueAsString(details);
            jdbc.update(
                    "INSERT INTO consistency_error " +
                            "  (source, level, code, message, details_json, related_run_id, cluster_id, cache_name) " +
                            "VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?)",
                    source, level, code, message, json, relatedRunId, clusterId, cacheName);
        } catch (JsonProcessingException | DataAccessException e) {
            log.warn("ErrorRegistry insert failed (source={}, code={}): {}",
                    source, code, e.toString());
        }
    }

    public void error(String source, String code, String message,
                      Throwable t, Map<String, Object> extraDetails) {
        Map<String, Object> d = extraDetails == null ? new HashMap<>() : new HashMap<>(extraDetails);
        if (t != null) {
            d.put("exception_class", t.getClass().getName());
            d.put("exception_message", t.getMessage());
        }
        record(source, "ERROR", code, message, d);
    }

    public void warn(String source, String code, String message, Map<String, Object> details) {
        record(source, "WARN", code, message, details);
    }
}
