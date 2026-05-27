package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.sbrf.pprb.stmnt.consistency.api.dto.ClusterHealthDto;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Эндпоинт здоровья кластеров — потребляется внешними сервисами
 * (например getStatementSummary IgniteClusterPool) для weighted-random
 * выбора кластера при чтении.
 *
 *   GET /api/clusters/health
 *   →  ClusterHealthDto { asOf, stalenessMs, clusters: { clusterId → ClusterState } }
 *
 * Возвращает per-cluster:
 *   - mismatches — сколько unresolved расхождений касается этого кластера
 *   - lastEventTs — когда последний CDC event прилетал
 *   - isHealthy — свежий ли (last event < stalenessMs назад)
 *   - divergentCaches — список кешей с проблемами (макс 5, остальное обрезаем)
 *
 * Контракт стабильный; этот эндпоинт может опрашиваться часто (раз в 10-30 сек
 * на каждый consumer-сервис), поэтому делаем дёшево — два SQL-запроса с
 * подготовленным индексом, без транзакций.
 */
@RestController
@RequestMapping("/api/clusters")
@RequiredArgsConstructor
public class ClusterHealthController {

    private final JdbcTemplate jdbc;

    @Value("${consistency.cluster-health.staleness-ms:300000}")
    private long stalenessMs;            // default 5 min

    @GetMapping("/health")
    public ClusterHealthDto health() {
        Instant now = Instant.now();
        Instant stalenessCutoff = now.minusMillis(stalenessMs);

        // 1) last_event_ts per cluster (используется и для isHealthy и для discovery).
        Map<String, Instant> lastEventTs = new LinkedHashMap<>();
        jdbc.query(
                "SELECT cluster_id, MAX(event_ts) AS last_ts " +
                        "FROM consistency_hash_latest GROUP BY cluster_id",
                rs -> {
                    Timestamp ts = rs.getTimestamp("last_ts");
                    lastEventTs.put(rs.getString("cluster_id"),
                            ts == null ? null : ts.toInstant());
                });

        // 2) per-cluster unresolved-mismatch count + sample of cache names.
        //    consistency_mismatch.hashes_json — JSONB { clusterId → hash }. Ключи JSON
        //    дают список кластеров причастных к расхождению. Считаем для каждого ключа.
        Map<String, Integer> mismatchCount = new LinkedHashMap<>();
        Map<String, List<String>> divergentCaches = new LinkedHashMap<>();
        jdbc.query(
                "SELECT key AS cluster_id, m.cache_name, COUNT(*) AS cnt " +
                        "FROM consistency_mismatch m, jsonb_object_keys(m.hashes_json) AS key " +
                        "WHERE m.resolved_at IS NULL " +
                        "GROUP BY key, m.cache_name " +
                        "ORDER BY cnt DESC",
                rs -> {
                    String cid = rs.getString("cluster_id");
                    String cache = rs.getString("cache_name");
                    int cnt = rs.getInt("cnt");
                    mismatchCount.merge(cid, cnt, Integer::sum);
                    divergentCaches
                            .computeIfAbsent(cid, k -> new ArrayList<>())
                            .add(cache);
                });

        // 3) Собираем DTO. Если cluster не появлялся в hash_latest вообще — не включаем
        //    (consumer может фильтровать пустых). Если только в mismatch — тоже добавляем.
        Map<String, ClusterHealthDto.ClusterState> out = new LinkedHashMap<>();
        // union of cluster ids from both maps
        java.util.Set<String> allClusters = new java.util.TreeSet<>();
        allClusters.addAll(lastEventTs.keySet());
        allClusters.addAll(mismatchCount.keySet());
        for (String cid : allClusters) {
            Instant lastTs = lastEventTs.get(cid);
            boolean healthy = lastTs != null && lastTs.isAfter(stalenessCutoff);
            List<String> caches = divergentCaches.getOrDefault(cid, List.of());
            if (caches.size() > 5) caches = caches.subList(0, 5);
            out.put(cid, new ClusterHealthDto.ClusterState(
                    cid,
                    mismatchCount.getOrDefault(cid, 0),
                    lastTs,
                    healthy,
                    caches));
        }
        return new ClusterHealthDto(now, stalenessMs, out);
    }
}
