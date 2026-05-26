package ru.sbrf.pprb.stmnt.consistency.lib;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.ErrorRepository;

import java.util.HashMap;
import java.util.Map;

/**
 * Thin facade over {@link ErrorRepository}: turns Java objects into details_json and
 * forwards. Infallible — never throws.
 *
 * Sources (free-form, but please reuse):
 *   consistency_job | cluster_reader | admin_fanout | debug_seed | db | scheduler
 *
 * Common codes:
 *   CLUSTER_DOWN | SCHEMA_NOT_FOUND | QUERY_FAILED | HASH_MISMATCH |
 *   INIT_FAILED | CLEANUP_FAILED | RECALC_FAILED | SQL_ERROR
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ErrorRegistry {

    private final ErrorRepository repo;
    private final ObjectMapper mapper;

    public void record(String source, String level, String code, String message,
                       Map<String, Object> details) {
        record(source, level, code, message, details, null, null, null);
    }

    public void record(String source, String level, String code, String message,
                       Map<String, Object> details,
                       Long relatedRunId, String clusterId, String cacheName) {
        String json = null;
        if (details != null && !details.isEmpty()) {
            try { json = mapper.writeValueAsString(details); }
            catch (JsonProcessingException e) {
                log.warn("ErrorRegistry json serialize failed (source={}, code={}): {}",
                        source, code, e.toString());
            }
        }
        repo.insert(source, level, code, message, json, relatedRunId, clusterId, cacheName);
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
