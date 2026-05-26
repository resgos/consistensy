package ru.sbrf.pprb.stmnt.consistency.lib.repo.jdbc;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import ru.sbrf.pprb.stmnt.consistency.api.dto.ConsistencyRunDto;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.ConsistencyRunRepository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class JdbcConsistencyRunRepository implements ConsistencyRunRepository {

    private final JdbcTemplate jdbc;

    @Override
    public long createRunning(String cacheName) {
        KeyHolder kh = new GeneratedKeyHolder();
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO consistency_run (cache_name, status) VALUES (?, 'RUNNING')",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, cacheName);
            return ps;
        }, kh);
        return ((Number) kh.getKeys().get("id")).longValue();
    }

    @Override
    public void markFinished(long id, String status, int mismatchCount, String errorMessage) {
        jdbc.update("UPDATE consistency_run " +
                        "SET finished_at = now(), status = ?, mismatch_count = ?, error_message = ? " +
                        "WHERE id = ?",
                status, mismatchCount, errorMessage, id);
    }

    @Override
    public Optional<ConsistencyRunDto> findById(long id) {
        List<ConsistencyRunDto> r = jdbc.query(
                "SELECT id, started_at, finished_at, cache_name, status, mismatch_count, error_message " +
                        "FROM consistency_run WHERE id = ?",
                new Object[]{id}, JdbcConsistencyRunRepository::map);
        return r.isEmpty() ? Optional.empty() : Optional.of(r.get(0));
    }

    @Override
    public List<ConsistencyRunDto> findRecent(int limit, String statusFilter) {
        String sql = "SELECT id, started_at, finished_at, cache_name, status, mismatch_count, error_message " +
                "FROM consistency_run " +
                (statusFilter != null ? "WHERE status = ? " : "") +
                "ORDER BY started_at DESC LIMIT ?";
        Object[] args = statusFilter != null
                ? new Object[]{statusFilter, limit}
                : new Object[]{limit};
        return jdbc.query(sql, args, JdbcConsistencyRunRepository::map);
    }

    @Override
    public int deleteOlderThan(int retentionDays) {
        return jdbc.update(
                "DELETE FROM consistency_run WHERE finished_at < now() - (? || ' days')::interval",
                retentionDays);
    }

    static ConsistencyRunDto map(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        Timestamp finished = rs.getTimestamp("finished_at");
        return new ConsistencyRunDto(
                rs.getLong("id"),
                rs.getTimestamp("started_at").toInstant(),
                finished == null ? null : finished.toInstant(),
                rs.getString("cache_name"),
                rs.getString("status"),
                rs.getInt("mismatch_count"),
                rs.getString("error_message"));
    }
}
