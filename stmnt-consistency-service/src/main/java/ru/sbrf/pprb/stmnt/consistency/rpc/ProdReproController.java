package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Воспроизводит prod-сценарий горячего регистра + диагностика planner-bug:
 *
 * 1. Создаёт TURNDOCCUR + индексы как на проде:
 *      - TURNDOCCUR_AGG_COVER_IDX           (REGISTER, CCTYPEOPER, CCOPERATIONDAY, CCDT, CCSUM, CCDATE)  inlineSize=100
 *      - TURNDOCCUR_AGG_COVER_IDX_DAY_FIRST (REGISTER, CCOPERATIONDAY, CCTYPEOPER, CCDT, CCSUM, CCDATE)  inlineSize=120
 *      - TDC_REG_TYPE_DAY_DESC_IDX          (REGISTER, CCTYPEOPER, CCOPERATIONDAY DESC)                  inlineSize=10  ← плохой
 *
 * 2. Bulk-fill: N turn-docs на ОДИН register через INSERT ... SELECT FROM SYSTEM_RANGE.
 *
 * 3. EXPLAIN-метрики (до фиксов): scanCount=N даже для MAX и LIMIT-1.
 *
 * 4. Фикс: DROP + CREATE с inlineSize=32.
 *
 * 5. EXPLAIN после фиксов + повторный perf замер.
 */
@Slf4j
@RestController
@RequestMapping("/api/debug/prod-repro")
@RequiredArgsConstructor
public class ProdReproController {

    private final IgniteClientFactory clientFactory;

    private static final String HOT_REGISTER = "HOT_REG_001";
    private static final String CLUSTER = "cluster-1";

    @PostMapping("/setup")
    public Map<String, Object> setup(@RequestParam(defaultValue = "100000") int turnDocs,
                                      @RequestParam(defaultValue = "30") int days) {
        IgniteClient c = clientFactory.get(CLUSTER);
        Map<String, Object> out = new LinkedHashMap<>();
        long t0 = System.currentTimeMillis();

        // Полный re-create таблицы — быстрее чем DELETE по миллионам строк.
        // DROP TABLE удаляет cache мгновенно (через cache.destroy()).
        try { exec(c, "DROP TABLE IF EXISTS TURNDOCCUR"); } catch (Exception ignore) {}

        exec(c, "CREATE TABLE TURNDOCCUR ("
                + "  OBJECTID VARCHAR PRIMARY KEY, CCRQUID VARCHAR, CCIDEKS VARCHAR, REGISTER VARCHAR,"
                + "  CCSUM DECIMAL, CCSUMNAT DECIMAL, CCSTARTSUM DECIMAL, CCSTARTSUMNAT DECIMAL,"
                + "  CCDT VARCHAR, CCTYPEOPER DECIMAL, CCDATE DATE, CCOPERATIONDAY DATE"
                + ") WITH \"CACHE_NAME=TURN_DOC_CUR, VALUE_TYPE=TurnDocCur\"");

        // Воспроизводим prod-индексы как они есть (с плохим inlineSize)
        exec(c, "CREATE INDEX TURNDOCCUR_AGG_COVER_IDX ON TURNDOCCUR " +
                "(REGISTER, CCTYPEOPER, CCOPERATIONDAY, CCDT, CCSUM, CCDATE) INLINE_SIZE 100");
        exec(c, "CREATE INDEX TURNDOCCUR_AGG_COVER_IDX_DAY_FIRST ON TURNDOCCUR " +
                "(REGISTER, CCOPERATIONDAY, CCTYPEOPER, CCDT, CCSUM, CCDATE) INLINE_SIZE 120");
        exec(c, "CREATE INDEX TDC_REG_TYPE_DAY_DESC_IDX ON TURNDOCCUR " +
                "(REGISTER, CCTYPEOPER, CCOPERATIONDAY DESC) INLINE_SIZE 10");

        // Bulk-insert через SYSTEM_RANGE — батчами по 2000 (один SQL=один тонкий
        // клиент с timeout 5 сек). Distribution: 30 дней; ccTypeOper: 1% type=50,
        // 33% type=0, 66% type=40
        log.info("Bulk-insert {} turn-docs for register={}, days={} in batches",
                turnDocs, HOT_REGISTER, days);
        long tIns = System.currentTimeMillis();
        int batchSize = 2000;
        for (int offset = 1; offset <= turnDocs; offset += batchSize) {
            int end = Math.min(offset + batchSize - 1, turnDocs);
            execArgs(c,
                    "INSERT INTO TURNDOCCUR (OBJECTID, CCRQUID, CCIDEKS, REGISTER, CCSUM, CCSUMNAT, " +
                            "CCDT, CCTYPEOPER, CCDATE, CCOPERATIONDAY) " +
                            "SELECT " +
                            "  '" + HOT_REGISTER + ":d' || X || ':r' || (X % 1000)," +
                            "  'RQ' || X," +
                            "  'EKS' || X," +
                            "  '" + HOT_REGISTER + "'," +
                            "  CAST(100 + (X % 1000) AS DECIMAL(20,2))," +
                            "  CAST(100 + (X % 1000) AS DECIMAL(20,2))," +
                            "  CASE WHEN MOD(X, 2) = 0 THEN '1' ELSE '0' END," +
                            "  CASE WHEN MOD(X, 100) = 0 THEN 50 WHEN MOD(X, 3) = 0 THEN 0 ELSE 40 END," +
                            "  DATEADD('DAY', -CAST(MOD(X, ?) AS INT), DATE '2026-05-26')," +
                            "  DATEADD('DAY', -CAST(MOD(X, ?) AS INT), DATE '2026-05-26') " +
                            "FROM SYSTEM_RANGE(?, ?)",
                    days, days, offset, end);
            if (offset % 20000 == 1) {
                log.info("inserted up to {}", end);
            }
        }
        long insMs = System.currentTimeMillis() - tIns;

        // Считаем сколько влилось
        long actual = ((Number) execScalar(c,
                "SELECT COUNT(*) FROM TURNDOCCUR WHERE REGISTER = '" + HOT_REGISTER + "'")).longValue();

        out.put("hotRegister", HOT_REGISTER);
        out.put("requestedTurnDocs", turnDocs);
        out.put("actualRows", actual);
        out.put("days", days);
        out.put("insertMs", insMs);
        out.put("totalMs", System.currentTimeMillis() - t0);
        return out;
    }

    /** Один прогон каждого ключевого SQL для замера latency. */
    @PostMapping("/measure")
    public Map<String, Object> measure(@RequestParam(defaultValue = "20") int iterations) {
        IgniteClient c = clientFactory.get(CLUSTER);
        Map<String, Object> r = new LinkedHashMap<>();

        // 1. SQL_PREV_OPER_DATE — текущий H2 MAX
        r.put("sql_prev_oper_date.MAX_H2",
                bench(c, iterations,
                        "SELECT GREATEST(" +
                                "  COALESCE((SELECT MAX(CCOPERATIONDAY) FROM TURNDOCCUR " +
                                "            WHERE REGISTER='" + HOT_REGISTER + "' AND CCTYPEOPER=0 " +
                                "              AND CCOPERATIONDAY < DATE '2026-05-26'), DATE '1900-01-01')," +
                                "  COALESCE((SELECT MAX(CCOPERATIONDAY) FROM TURNDOCCUR " +
                                "            WHERE REGISTER='" + HOT_REGISTER + "' AND CCTYPEOPER=40 " +
                                "              AND CCOPERATIONDAY < DATE '2026-05-26'), DATE '1900-01-01'))"));

        // 2. SQL_PREV_OPER_DATE — LIMIT-1 (без USE INDEX) — H2
        r.put("sql_prev_oper_date.LIMIT1_H2_no_hint",
                bench(c, iterations,
                        "SELECT GREATEST(" +
                                "  COALESCE((SELECT CCOPERATIONDAY FROM TURNDOCCUR " +
                                "            WHERE REGISTER='" + HOT_REGISTER + "' AND CCTYPEOPER=0 " +
                                "              AND CCOPERATIONDAY < DATE '2026-05-26' " +
                                "            ORDER BY CCOPERATIONDAY DESC LIMIT 1), DATE '1900-01-01')," +
                                "  COALESCE((SELECT CCOPERATIONDAY FROM TURNDOCCUR " +
                                "            WHERE REGISTER='" + HOT_REGISTER + "' AND CCTYPEOPER=40 " +
                                "              AND CCOPERATIONDAY < DATE '2026-05-26' " +
                                "            ORDER BY CCOPERATIONDAY DESC LIMIT 1), DATE '1900-01-01'))"));

        // 3. SQL_PREV_OPER_DATE — LIMIT-1 + USE INDEX(TDC_REG_TYPE_DAY_DESC_IDX)
        r.put("sql_prev_oper_date.LIMIT1_USE_DESC_IDX",
                bench(c, iterations,
                        "SELECT GREATEST(" +
                                "  COALESCE((SELECT CCOPERATIONDAY FROM TURNDOCCUR USE INDEX(TDC_REG_TYPE_DAY_DESC_IDX) " +
                                "            WHERE REGISTER='" + HOT_REGISTER + "' AND CCTYPEOPER=0 " +
                                "              AND CCOPERATIONDAY < DATE '2026-05-26' " +
                                "            ORDER BY CCOPERATIONDAY DESC LIMIT 1), DATE '1900-01-01')," +
                                "  COALESCE((SELECT CCOPERATIONDAY FROM TURNDOCCUR USE INDEX(TDC_REG_TYPE_DAY_DESC_IDX) " +
                                "            WHERE REGISTER='" + HOT_REGISTER + "' AND CCTYPEOPER=40 " +
                                "              AND CCOPERATIONDAY < DATE '2026-05-26' " +
                                "            ORDER BY CCOPERATIONDAY DESC LIMIT 1), DATE '1900-01-01'))"));

        // 4. SQL_SUM_BETWEEN — без хинта (текущий prod-вариант)
        r.put("sql_sum_between.NO_HINT",
                bench(c, iterations,
                        "SELECT  COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUM ELSE CCSUM END), 0)," +
                                "        COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUMNAT ELSE CCSUMNAT END), 0) " +
                                " FROM TURNDOCCUR " +
                                " WHERE REGISTER='" + HOT_REGISTER + "' AND CCTYPEOPER IN (0,40)" +
                                "   AND CCOPERATIONDAY >= DATE '2026-04-26' AND CCOPERATIONDAY < DATE '2026-05-26'"));

        // 5. SQL_SUM_BETWEEN — с USE INDEX(_DAY_FIRST)
        r.put("sql_sum_between.USE_DAY_FIRST",
                bench(c, iterations,
                        "SELECT  COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUM ELSE CCSUM END), 0)," +
                                "        COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUMNAT ELSE CCSUMNAT END), 0) " +
                                " FROM TURNDOCCUR USE INDEX(TURNDOCCUR_AGG_COVER_IDX_DAY_FIRST) " +
                                " WHERE REGISTER='" + HOT_REGISTER + "' AND CCTYPEOPER IN (0,40)" +
                                "   AND CCOPERATIONDAY >= DATE '2026-04-26' AND CCOPERATIONDAY < DATE '2026-05-26'"));

        // 6. controlled MAX — без всех WHERE по дате
        r.put("max_no_date.MAX_H2",
                bench(c, iterations,
                        "SELECT MAX(CCOPERATIONDAY) FROM TURNDOCCUR " +
                                "WHERE REGISTER='" + HOT_REGISTER + "' AND CCTYPEOPER=40"));

        r.put("max_no_date.LIMIT1_USE_DESC_IDX",
                bench(c, iterations,
                        "SELECT CCOPERATIONDAY FROM TURNDOCCUR USE INDEX(TDC_REG_TYPE_DAY_DESC_IDX) " +
                                "WHERE REGISTER='" + HOT_REGISTER + "' AND CCTYPEOPER=40 " +
                                "ORDER BY CCOPERATIONDAY DESC LIMIT 1"));

        return r;
    }

    /** Применить ФИКС: пересоздать DESC-индекс с inlineSize=32. */
    @PostMapping("/apply-fix")
    public Map<String, Object> applyFix() {
        IgniteClient c = clientFactory.get(CLUSTER);
        long t0 = System.currentTimeMillis();
        exec(c, "DROP INDEX IF EXISTS TDC_REG_TYPE_DAY_DESC_IDX");
        exec(c, "CREATE INDEX TDC_REG_TYPE_DAY_DESC_IDX ON TURNDOCCUR " +
                "(REGISTER, CCTYPEOPER, CCOPERATIONDAY DESC) INLINE_SIZE 32");
        return Map.of("ok", true, "newInlineSize", 32, "elapsedMs", System.currentTimeMillis() - t0);
    }

    /** Грубо собирает план запроса (текстовый H2). */
    @PostMapping("/explain")
    public Map<String, Object> explain(@RequestBody Map<String, String> body) {
        IgniteClient c = clientFactory.get(CLUSTER);
        String sql = body.get("sql");
        try (var cur = c.query(new SqlFieldsQuery("EXPLAIN " + sql))) {
            StringBuilder sb = new StringBuilder();
            for (var row : cur) {
                if (!row.isEmpty()) sb.append(row.get(0)).append("\n\n");
            }
            return Map.of("plan", sb.toString());
        } catch (Exception e) {
            return Map.of("error", e.toString());
        }
    }

    // ---- internals ----

    private Map<String, Object> bench(IgniteClient c, int iters, String sql) {
        // warmup
        try { drain(c.query(new SqlFieldsQuery(sql))); }
        catch (Exception e) { return Map.of("error", e.toString()); }

        long[] timings = new long[iters];
        for (int i = 0; i < iters; i++) {
            long t = System.nanoTime();
            try (var cur = c.query(new SqlFieldsQuery(sql))) {
                drain(cur);
            } catch (Exception e) {
                return Map.of("error", e.toString());
            }
            timings[i] = System.nanoTime() - t;
        }
        Arrays.sort(timings);
        long sum = 0;
        for (long t : timings) sum += t;
        return Map.of(
                "iterations", iters,
                "avgMs", round(sum / (double) iters / 1_000_000.0),
                "p50Ms", round(timings[iters / 2] / 1_000_000.0),
                "p95Ms", round(timings[Math.min(iters - 1, (int) (iters * 0.95))] / 1_000_000.0),
                "p99Ms", round(timings[Math.min(iters - 1, (int) (iters * 0.99))] / 1_000_000.0),
                "maxMs", round(timings[iters - 1] / 1_000_000.0));
    }

    private void drain(Iterable<?> cur) {
        for (var ignored : cur) {}
    }

    private void exec(IgniteClient c, String sql) {
        try (var cur = c.query(new SqlFieldsQuery(sql))) { drain(cur); }
    }

    private void execArgs(IgniteClient c, String sql, Object... args) {
        try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) { drain(cur); }
    }

    private Object execScalar(IgniteClient c, String sql) {
        try (var cur = c.query(new SqlFieldsQuery(sql))) {
            for (var row : cur) return row.get(0);
        }
        return null;
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
