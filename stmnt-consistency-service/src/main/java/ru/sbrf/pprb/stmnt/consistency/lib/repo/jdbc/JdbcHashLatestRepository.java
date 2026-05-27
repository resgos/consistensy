package ru.sbrf.pprb.stmnt.consistency.lib.repo.jdbc;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.sbrf.pprb.stmnt.consistency.hashers.CdcEvent;
import ru.sbrf.pprb.stmnt.consistency.lib.repo.HashLatestRepository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
public class JdbcHashLatestRepository implements HashLatestRepository {

    private final JdbcTemplate jdbc;

    private static final String UPSERT_SQL = """
            INSERT INTO consistency_hash_latest
                (cluster_id, cache_name, business_key, hash, op, event_ts, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (cluster_id, cache_name, business_key) DO UPDATE SET
                hash       = EXCLUDED.hash,
                op         = EXCLUDED.op,
                event_ts   = EXCLUDED.event_ts,
                updated_at = now()
            WHERE consistency_hash_latest.event_ts < EXCLUDED.event_ts
            """;
    // event_ts < EXCLUDED — защита от out-of-order: старое событие не перетирает
    // более свежее, если consumer переиграет через seek.

    @Override
    public void upsert(CdcEvent e) {
        jdbc.update(UPSERT_SQL,
                e.clusterId(), e.cacheName(), e.businessKey(), e.hash(),
                e.op().name(), new Timestamp(e.ts()));
    }

    @Override
    public void upsertBatch(List<CdcEvent> events) {
        if (events.isEmpty()) return;
        List<Object[]> batch = new ArrayList<>(events.size());
        for (CdcEvent e : events) {
            batch.add(new Object[]{
                    e.clusterId(), e.cacheName(), e.businessKey(), e.hash(),
                    e.op().name(), new Timestamp(e.ts())
            });
        }
        jdbc.batchUpdate(UPSERT_SQL, batch);
    }

    @Override
    public List<Divergence> findDivergences(String cacheNameFilter, int limit) {
        // Расхождение детектится по двум условиям:
        //   1. РАЗНЫЕ hash'и между кластерами (REMOVED считаем как отдельный hash '__REMOVED__'
        //      → присутствие в одном vs удалено в другом — mismatch).
        //   2. КОЛИЧЕСТВО кластеров для business_key меньше общего числа кластеров
        //      (CDC ожидает события от ВСЕХ; если ключ виден только на 1 из 3 — это
        //      one-sided "extra", тоже mismatch).
        //
        // Общее число кластеров берём из самой таблицы — DISTINCT cluster_id из hash_latest.
        StringBuilder sql = new StringBuilder("""
                WITH cluster_count AS (
                  SELECT COUNT(DISTINCT cluster_id) AS n FROM consistency_hash_latest
                ),
                divs AS (
                  SELECT cache_name, business_key
                  FROM consistency_hash_latest
                  WHERE 1=1
                """);
        List<Object> args = new ArrayList<>();
        if (cacheNameFilter != null) {
            sql.append("    AND cache_name = ?\n");
            args.add(cacheNameFilter);
        }
        sql.append("""
                  GROUP BY cache_name, business_key
                  HAVING COUNT(DISTINCT CASE WHEN op = 'REMOVED' THEN '__REMOVED__' ELSE hash END) > 1
                     OR COUNT(DISTINCT cluster_id) < (SELECT n FROM cluster_count)
                  LIMIT ?
                )
                SELECT h.cache_name, h.business_key, h.cluster_id, h.hash, h.op
                FROM consistency_hash_latest h
                JOIN divs d
                  ON d.cache_name = h.cache_name AND d.business_key = h.business_key
                ORDER BY h.cache_name, h.business_key, h.cluster_id
                """);
        args.add(limit);

        Map<String, Divergence> acc = new LinkedHashMap<>();
        jdbc.query(sql.toString(), args.toArray(), rs -> {
            String cache = rs.getString("cache_name");
            String bk    = rs.getString("business_key");
            String cid   = rs.getString("cluster_id");
            String h     = rs.getString("hash");
            String op    = rs.getString("op");
            String key   = cache + "" + bk;
            Divergence d = acc.computeIfAbsent(key,
                    k -> new Divergence(cache, bk, new LinkedHashMap<>()));
            d.clusterHashes().put(cid, "REMOVED".equals(op) ? "__REMOVED__" : h);
        });
        return new ArrayList<>(acc.values());
    }

    @Override
    public int pruneRemovedBefore(Instant cutoff) {
        return jdbc.update(
                "DELETE FROM consistency_hash_latest WHERE op = 'REMOVED' AND updated_at < ?",
                Timestamp.from(cutoff));
    }
}
