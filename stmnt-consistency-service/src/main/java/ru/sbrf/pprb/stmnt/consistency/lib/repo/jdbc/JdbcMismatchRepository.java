package ru.sbrf.pprb.stmnt.consistency.lib.repo.jdbc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.sbrf.pprb.stmnt.consistency.api.dto.MismatchDto;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.MismatchRepository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
public class JdbcMismatchRepository implements MismatchRepository {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    private static final TypeReference<Map<String, String>> HASHES_TYPE = new TypeReference<>() {};

    @Override
    @SneakyThrows
    public void saveBatch(long runId, String cacheName, List<Mismatch> list) {
        if (list.isEmpty()) return;
        List<Object[]> batch = new ArrayList<>(list.size());
        for (Mismatch m : list) {
            batch.add(new Object[]{
                    runId, cacheName, m.businessKey(), mapper.writeValueAsString(m.clusterHashes())
            });
        }
        jdbc.batchUpdate(
                "INSERT INTO consistency_mismatch (run_id, cache_name, business_key, hashes_json) " +
                        "VALUES (?, ?, ?, ?::jsonb)",
                batch);
    }

    @Override
    public List<MismatchDto> find(String cacheName, Instant since, boolean unresolvedOnly, int limit) {
        StringBuilder sql = new StringBuilder(
                "SELECT id, run_id, cache_name, business_key, hashes_json, " +
                        "       detected_at, resolved_at, notes " +
                        "FROM consistency_mismatch WHERE 1=1 ");
        List<Object> args = new ArrayList<>();
        if (cacheName != null) { sql.append("AND cache_name = ? "); args.add(cacheName); }
        if (since != null)     { sql.append("AND detected_at >= ? "); args.add(Timestamp.from(since)); }
        if (unresolvedOnly)    { sql.append("AND resolved_at IS NULL "); }
        sql.append("ORDER BY detected_at DESC LIMIT ?");
        args.add(limit);

        return jdbc.query(sql.toString(), args.toArray(), (rs, n) -> {
            try {
                Map<String, String> hashes = mapper.readValue(rs.getString("hashes_json"), HASHES_TYPE);
                Timestamp resolved = rs.getTimestamp("resolved_at");
                return new MismatchDto(
                        rs.getLong("id"),
                        rs.getLong("run_id"),
                        rs.getString("cache_name"),
                        rs.getString("business_key"),
                        hashes,
                        rs.getTimestamp("detected_at").toInstant(),
                        resolved == null ? null : resolved.toInstant(),
                        rs.getString("notes"));
            } catch (Exception e) { throw new RuntimeException(e); }
        });
    }

    @Override
    public int resolve(long id, String notes) {
        return jdbc.update(
                "UPDATE consistency_mismatch SET resolved_at = now(), notes = ? " +
                        "WHERE id = ? AND resolved_at IS NULL",
                notes, id);
    }
}
