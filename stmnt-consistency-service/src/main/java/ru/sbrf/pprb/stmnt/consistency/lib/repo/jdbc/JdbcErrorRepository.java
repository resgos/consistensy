package ru.sbrf.pprb.stmnt.consistency.lib.repo.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.sbrf.pprb.stmnt.consistency.api.dto.ErrorEntryDto;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.ErrorRepository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class JdbcErrorRepository implements ErrorRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    @Override
    public void insert(String source, String level, String code, String message,
                       String detailsJson, Long relatedRunId, String clusterId, String cacheName) {
        try {
            jdbc.update(
                    "INSERT INTO consistency_error " +
                            "(source, level, code, message, details_json, related_run_id, cluster_id, cache_name) " +
                            "VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?)",
                    source, level, code, message, detailsJson, relatedRunId, clusterId, cacheName);
        } catch (DataAccessException e) {
            // Registry must not break business ops. Log and continue.
            log.warn("ErrorRepository insert failed (source={}, code={}): {}",
                    source, code, e.toString());
        }
    }

    @Override
    public List<ErrorEntryDto> find(Filter f, int limit) {
        StringBuilder sql = new StringBuilder(
                "SELECT id, occurred_at, source, level, code, message, details_json, " +
                        "       related_run_id, cluster_id, cache_name, resolved_at, resolution_notes " +
                        "FROM consistency_error WHERE 1=1 ");
        List<Object> args = new ArrayList<>();
        if (f.source() != null)    { sql.append("AND source=? ");      args.add(f.source()); }
        if (f.level() != null)     { sql.append("AND level=? ");       args.add(f.level()); }
        if (f.code() != null)      { sql.append("AND code=? ");        args.add(f.code()); }
        if (f.clusterId() != null) { sql.append("AND cluster_id=? "); args.add(f.clusterId()); }
        if (f.cacheName() != null) { sql.append("AND cache_name=? "); args.add(f.cacheName()); }
        if (f.since() != null)     { sql.append("AND occurred_at >= ? "); args.add(Timestamp.from(f.since())); }
        if (f.unresolvedOnly())    { sql.append("AND resolved_at IS NULL "); }
        sql.append("ORDER BY occurred_at DESC LIMIT ?");
        args.add(limit);
        return jdbc.query(sql.toString(), args.toArray(), this::map);
    }

    @Override
    public Map<String, List<Map<String, Object>>> aggregateStats(Instant since) {
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();
        for (String col : new String[]{"source", "level", "code"}) {
            String sql = "SELECT " + col + " AS k, COUNT(*) AS c FROM consistency_error " +
                    (since != null ? "WHERE occurred_at >= ? " : "") +
                    "GROUP BY " + col + " ORDER BY c DESC";
            Object[] args = since != null ? new Object[]{Timestamp.from(since)} : new Object[0];
            List<Map<String, Object>> rows = jdbc.query(sql, args, (rs, n) -> Map.of(
                    "value", rs.getString("k") == null ? "null" : rs.getString("k"),
                    "count", rs.getLong("c")));
            result.put("by_" + col, rows);
        }
        return result;
    }

    @Override
    public int resolve(long id, String notes) {
        return jdbc.update(
                "UPDATE consistency_error SET resolved_at=now(), resolution_notes=? " +
                        "WHERE id=? AND resolved_at IS NULL",
                notes, id);
    }

    @SneakyThrows
    private ErrorEntryDto map(ResultSet rs, int n) throws SQLException {
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
