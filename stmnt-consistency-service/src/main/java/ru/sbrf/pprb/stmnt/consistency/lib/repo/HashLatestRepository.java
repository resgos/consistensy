package ru.sbrf.pprb.stmnt.consistency.lib.repo;

import ru.sbrf.pprb.stmnt.consistency.hashers.CdcEvent;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Зеркало CDC-потока: текущий hash на (cluster, cache, businessKey).
 *
 * CDC consumer делает {@link #upsert} на каждое сообщение.
 * Sweep-job вызывает {@link #findDivergences} чтобы найти бизнес-ключи где
 * COUNT(DISTINCT hash) > 1 — это и есть детектированные расхождения.
 */
public interface HashLatestRepository {

    /** UPSERT по PK (cluster_id, cache_name, business_key). */
    void upsert(CdcEvent event);

    /** Batch-вариант (consumer обычно дёргает несколько событий за poll). */
    void upsertBatch(List<CdcEvent> events);

    /**
     * Найти бизнес-ключи где hash расходится между кластерами.
     *
     * @param cacheNameFilter    null = все кеши
     * @param limit              максимум строк
     * @return  list of (cacheName, businessKey, clusterId -> hash)
     */
    List<Divergence> findDivergences(String cacheNameFilter, int limit);

    /** Удалить REMOVED-tombstones старше указанного момента (retention). */
    int pruneRemovedBefore(Instant cutoff);

    record Divergence(String cacheName,
                      String businessKey,
                      Map<String, String> clusterHashes) {}
}
