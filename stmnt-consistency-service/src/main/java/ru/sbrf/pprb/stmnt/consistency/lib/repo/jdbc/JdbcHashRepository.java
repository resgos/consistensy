package ru.sbrf.pprb.stmnt.consistency.lib.repo.jdbc;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.HashRepository;

import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
public class JdbcHashRepository implements HashRepository {

    private final JdbcTemplate jdbc;

    @Override
    public void saveBatch(long runId, String clusterId, String cacheName, Map<String, String> hashes) {
        if (hashes.isEmpty()) return;
        List<Object[]> batch = hashes.entrySet().stream()
                .<Object[]>map(e -> new Object[]{runId, clusterId, cacheName, e.getKey(), e.getValue()})
                .toList();
        jdbc.batchUpdate(
                "INSERT INTO consistency_hash (run_id, cluster_id, cache_name, business_key, hash_hex) " +
                        "VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                batch);
    }
}
