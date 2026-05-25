package ru.sbrf.pprb.stmnt.consistency.rpc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.api.dto.ErrorEntryDto;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unified error registry endpoints.
 *
 * GET  /api/errors?source=&level=&since=&unresolvedOnly=true&limit=100
 * GET  /api/errors/stats
 * POST /api/errors/{id}/resolve   {"notes":"fixed"}
 */
@RestController
@RequestMapping("/api/errors")
@RequiredArgsConstructor
public class ErrorController {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    @GetMapping
    @SneakyThrows
    public List<ErrorEntryDto> list(@RequestParam(required = false) String source,
                                     @RequestParam(required = false) String level,
                                     @RequestParam(required = false) String code,
                                     @RequestParam(required = false) String clusterId,
                                     @RequestParam(required = false) String cacheName,
                                     @RequestParam(required = false) String since,
                                     @RequestParam(defaultValue = "false") boolean unresolvedOnly,
                                     @RequestParam(defaultValue = "100") int limit) {
        StringBuilder sql = new StringBuilder(
                "SELECT id, occurred_at, source, level, code, message, details_json, " +
                        "       related_run_id, cluster_id, cache_name, resolved_at, resolution_notes " +
                        "FROM consistency_error WHERE 1=1 ");
        List<Object> args = new ArrayList<>();
        if (source != null)    { sql.append("AND source=? ");     args.add(source); }
        if (level != null)     { sql.append("AND level=? ");      args.add(level); }
        if (code != null)      { sql.append("AND code=? ");       args.add(code); }
        if (clusterId != null) { sql.append("AND cluster_id=? "); args.add(clusterId); }
        if (cacheName != null) { sql.append("AND cache_name=? "); args.add(cacheName); }
        if (since != null && !since.isBlank()) {
            sql.append("AND occurred_at >= ? ");
            args.add(Timestamp.from(Instant.parse(since)));
        }
        if (unresolvedOnly)    sql.append("AND resolved_at IS NULL ");
        sql.append("ORDER BY occurred_at DESC LIMIT ?");
        args.add(limit);
        return jdbc.query(sql.toString(), args.toArray(), (rs, n) -> mapRow(rs));
    }

    @GetMapping("/stats")
    public Map<String, Object> stats(@RequestParam(required = false) String since) {
        String[] cols = {"source", "level", "code"};
        Map<String, Object> result = new HashMap<>();
        for (String col : cols) {
            String sql = "SELECT " + col + " AS k, COUNT(*) AS c FROM consistency_error " +
                    (since != null ? "WHERE occurred_at >= ? " : "") +
                    "GROUP BY " + col + " ORDER BY c DESC";
            Object[] args = since != null ? new Object[]{Timestamp.from(Instant.parse(since))} : new Object[0];
            List<Map<String, Object>> rows = jdbc.query(sql, args, (rs, n) -> Map.of(
                    "value", rs.getString("k") == null ? "null" : rs.getString("k"),
                    "count", rs.getLong("c")));
            result.put("by_" + col, rows);
        }
        return result;
    }

    @PostMapping("/{id}/resolve")
    public Map<String, Object> resolve(@PathVariable long id,
                                        @RequestBody(required = false) Map<String, String> body) {
        String notes = body != null ? body.get("notes") : null;
        int updated = jdbc.update(
                "UPDATE consistency_error SET resolved_at=now(), resolution_notes=? " +
                        "WHERE id=? AND resolved_at IS NULL",
                notes, id);
        return Map.of("updated", updated);
    }

    @SneakyThrows
    private ErrorEntryDto mapRow(java.sql.ResultSet rs) {
        String json = rs.getString("details_json");
        Map<String, Object> details = (json == null) ? null : mapper.readValue(json, MAP_TYPE);
        Timestamp resolved = rs.getTimestamp("resolved_at");
        return new ErrorEntryDto(
                rs.getLong("id"),
                rs.getTimestamp("occurred_at").toInstant(),
                rs.getString("source"),
                rs.getString("level"),
                rs.getString("code"),
                rs.getString("message"),
                details,
                rs.getObject("related_run_id") == null ? null : rs.getLong("related_run_id"),
                rs.getString("cluster_id"),
                rs.getString("cache_name"),
                resolved == null ? null : resolved.toInstant(),
                rs.getString("resolution_notes"));
    }
}
