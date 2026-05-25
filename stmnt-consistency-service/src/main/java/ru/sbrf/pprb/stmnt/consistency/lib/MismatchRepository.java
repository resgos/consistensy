package ru.sbrf.pprb.stmnt.consistency.lib;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import ru.sbrf.pprb.stmnt.consistency.api.dto.ConsistencyRunDto;
import ru.sbrf.pprb.stmnt.consistency.api.dto.MismatchDto;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Repository
@RequiredArgsConstructor
public class MismatchRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public long startRun(String cacheNameOrNull) {
        KeyHolder kh = new GeneratedKeyHolder();
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO consistency_run (cache_name, status) VALUES (?, 'RUNNING')",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, cacheNameOrNull);
            return ps;
        }, kh);
        return ((Number) kh.getKeys().get("id")).longValue();
    }

    @Transactional
    public void finishRun(long runId, String status, int mismatchCount, String errorMessage) {
        jdbc.update("UPDATE consistency_run " +
                        "SET finished_at = now(), status = ?, mismatch_count = ?, error_message = ? " +
                        "WHERE id = ?",
                status, mismatchCount, errorMessage, runId);
    }

    public void saveHashesBatch(long runId, String clusterId, String cacheName,
                                Map<String, String> hashes) {
        if (hashes.isEmpty()) return;
        List<Object[]> batch = hashes.entrySet().stream()
                .<Object[]>map(e -> new Object[]{runId, clusterId, cacheName, e.getKey(), e.getValue()})
                .toList();
        jdbc.batchUpdate(
                "INSERT INTO consistency_hash (run_id, cluster_id, cache_name, business_key, hash_hex) " +
                        "VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                batch);
    }

    @SneakyThrows
    public void saveMismatches(long runId, String cacheName, List<Mismatch> list) {
        if (list.isEmpty()) return;
        List<Object[]> batch = list.stream()
                .<Object[]>map(m -> {
                    try {
                        return new Object[]{
                                runId, cacheName, m.businessKey(),
                                mapper.writeValueAsString(m.clusterHashes())
                        };
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
        jdbc.batchUpdate(
                "INSERT INTO consistency_mismatch (run_id, cache_name, business_key, hashes_json) " +
                        "VALUES (?, ?, ?, ?::jsonb)",
                batch);
    }

    public List<ConsistencyRunDto> listRuns(int limit, String statusFilter) {
        String sql = "SELECT id, started_at, finished_at, cache_name, status, mismatch_count, error_message " +
                "FROM consistency_run " +
                (statusFilter != null ? "WHERE status = ? " : "") +
                "ORDER BY started_at DESC LIMIT ?";
        Object[] args = statusFilter != null
                ? new Object[]{statusFilter, limit}
                : new Object[]{limit};
        return jdbc.query(sql, args, (rs, n) -> new ConsistencyRunDto(
                rs.getLong("id"),
                rs.getTimestamp("started_at").toInstant(),
                rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null,
                rs.getString("cache_name"),
                rs.getString("status"),
                rs.getInt("mismatch_count"),
                rs.getString("error_message")
        ));
    }

    public ConsistencyRunDto getRun(long runId) {
        return jdbc.query(
                "SELECT id, started_at, finished_at, cache_name, status, mismatch_count, error_message " +
                        "FROM consistency_run WHERE id = ?",
                new Object[]{runId},
                rs -> rs.next() ? new ConsistencyRunDto(
                        rs.getLong("id"),
                        rs.getTimestamp("started_at").toInstant(),
                        rs.getTimestamp("finished_at") != null ? rs.getTimestamp("finished_at").toInstant() : null,
                        rs.getString("cache_name"),
                        rs.getString("status"),
                        rs.getInt("mismatch_count"),
                        rs.getString("error_message")
                ) : null);
    }

    @SneakyThrows
    public List<MismatchDto> listMismatches(String cacheName, Instant since, boolean unresolvedOnly, int limit) {
        StringBuilder sql = new StringBuilder(
                "SELECT id, run_id, cache_name, business_key, hashes_json, detected_at, resolved_at, notes " +
                        "FROM consistency_mismatch WHERE 1=1 ");
        java.util.List<Object> args = new java.util.ArrayList<>();
        if (cacheName != null) {
            sql.append("AND cache_name = ? ");
            args.add(cacheName);
        }
        if (since != null) {
            sql.append("AND detected_at >= ? ");
            args.add(Timestamp.from(since));
        }
        if (unresolvedOnly) {
            sql.append("AND resolved_at IS NULL ");
        }
        sql.append("ORDER BY detected_at DESC LIMIT ?");
        args.add(limit);
        return jdbc.query(sql.toString(), args.toArray(), (rs, n) -> {
            try {
                Map<String, String> hashes = mapper.readValue(
                        rs.getString("hashes_json"),
                        mapper.getTypeFactory().constructMapType(
                                java.util.HashMap.class, String.class, String.class));
                return new MismatchDto(
                        rs.getLong("id"),
                        rs.getLong("run_id"),
                        rs.getString("cache_name"),
                        rs.getString("business_key"),
                        hashes,
                        rs.getTimestamp("detected_at").toInstant(),
                        rs.getTimestamp("resolved_at") != null ? rs.getTimestamp("resolved_at").toInstant() : null,
                        rs.getString("notes")
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public int resolveMismatch(long id, String notes) {
        return jdbc.update(
                "UPDATE consistency_mismatch SET resolved_at = now(), notes = ? " +
                        "WHERE id = ? AND resolved_at IS NULL",
                notes, id);
    }

    public int purgeOldHashes(int retentionDays) {
        return jdbc.update(
                "DELETE FROM consistency_run WHERE finished_at < now() - (? || ' days')::interval",
                retentionDays);
    }

    public record Mismatch(String businessKey, Map<String, String> clusterHashes) {}
}
