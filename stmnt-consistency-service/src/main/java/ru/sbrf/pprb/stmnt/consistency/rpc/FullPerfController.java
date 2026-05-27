package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.config.ConsistencyProperties;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Полный набор перф-тестов на multi-cluster multi-node setup:
 *  1. DayBalancesRecalc flow (3 SQL/day × N регистров)
 *  2. GetStatementSummary — TURNDOCCUR-based (агрегация on-fly)
 *  3. GetStatementSummary — DAYBALANCES-based (pre-aggregated)
 *
 * Endpoints:
 *   POST /api/perf/full?registers=N&iterations=K&clusterId=cluster-X
 *
 * Возвращает все 3 замера для одного кластера. Для cross-cluster sweep
 * вызывать endpoint несколько раз с разными clusterId.
 */
@Slf4j
@RestController
@RequestMapping("/api/perf")
@RequiredArgsConstructor
public class FullPerfController {

    private final ConsistencyProperties props;
    private final IgniteClientFactory clientFactory;

    private static final LocalDate TODAY = LocalDate.of(2026, 5, 24);
    private static final Date FROM_30D = Date.valueOf(TODAY.minusDays(30));
    private static final Date TO_TODAY = Date.valueOf(TODAY);

    @PostMapping("/full")
    public Map<String, Object> full(
            @RequestParam(defaultValue = "10") int registers,
            @RequestParam(defaultValue = "10") int iterations,
            @RequestParam(defaultValue = "cluster-1") String clusterId) {

        IgniteClient c = clientFactory.get(clusterId);
        if (c == null) return Map.of("ok", false, "error", "cluster not connected: " + clusterId);

        // Подготовим N регистров для теста
        List<String> regs = new ArrayList<>();
        for (int i = 1; i <= registers; i++) {
            regs.add(String.format("R%03d", i));
        }

        // ---- Тест 1: DayBalancesRecalc — пересчёт одного дня для каждого регистра ----
        Map<String, Object> recalc = bench(c, iterations, "DAYBALANCES_RECALC", () -> {
            for (String r : regs) doRecalcDay(c, r);
        });

        // ---- Тест 2: GetStatementSummary — TURNDOCCUR-based ----
        Map<String, Object> turnBased = bench(c, iterations, "SUMMARY_TURNDOCCUR_BASED", () -> {
            for (String r : regs) doSummaryFromTurnDocCur(c, r);
        });

        // ---- Тест 3: GetStatementSummary — DAYBALANCES-based ----
        Map<String, Object> dbBased = bench(c, iterations, "SUMMARY_DAYBALANCES_BASED", () -> {
            for (String r : regs) doSummaryFromDayBalances(c, r);
        });

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("clusterId", clusterId);
        r.put("registers", registers);
        r.put("iterations", iterations);
        r.put("daybalances_recalc", recalc);
        r.put("summary_turndoccur_based", turnBased);
        r.put("summary_daybalances_based", dbBased);
        return r;
    }

    /**
     * Один день recalc DayBalances: повторяет реальный flow DayBalancesRecalcService:
     *   1. SQL_PREV_OPER_DATE через ORDER BY DESC LIMIT 1 — поиск предыдущей даты
     *   2. SQL_START_SUM_SV4 — start_sum агрегация до cutoff
     *   3. SQL_SUM_BETWEEN — обороты за [prev, day]
     */
    private void doRecalcDay(IgniteClient c, String register) {
        // 1. PREV_OPER_DATE (двойной LIMIT-1 через GREATEST)
        exec(c, "SELECT GREATEST(" +
                "  COALESCE((SELECT CCOPERATIONDAY FROM TURNDOCCUR " +
                "            WHERE REGISTER=? AND CCTYPEOPER=0 AND CCOPERATIONDAY<? " +
                "            ORDER BY CCOPERATIONDAY DESC LIMIT 1), DATE '1900-01-01'), " +
                "  COALESCE((SELECT CCOPERATIONDAY FROM TURNDOCCUR " +
                "            WHERE REGISTER=? AND CCTYPEOPER=40 AND CCOPERATIONDAY<? " +
                "            ORDER BY CCOPERATIONDAY DESC LIMIT 1), DATE '1900-01-01'))",
                register, TO_TODAY, register, TO_TODAY);

        // 2. SQL_START_SUM_SV4
        exec(c, "SELECT" +
                "  COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUM ELSE CCSUM END), 0)," +
                "  COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUMNAT ELSE CCSUMNAT END), 0)" +
                " FROM TURNDOCCUR" +
                " WHERE REGISTER=? AND CCTYPEOPER IN (0,40) AND CCOPERATIONDAY<?",
                register, TO_TODAY);

        // 3. SQL_SUM_BETWEEN — 1-day window
        exec(c, "SELECT" +
                "  COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUM ELSE CCSUM END), 0)," +
                "  COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUMNAT ELSE CCSUMNAT END), 0)" +
                " FROM TURNDOCCUR" +
                " WHERE REGISTER=? AND CCTYPEOPER IN (0,40)" +
                "   AND CCOPERATIONDAY>=? AND CCOPERATIONDAY<?",
                register, FROM_30D, TO_TODAY);
    }

    /**
     * Summary через TURNDOCCUR — текущий вариант getStatementSummary.
     * Агрегирует обороты прямо из turn_doc_cur каждый раз.
     */
    private void doSummaryFromTurnDocCur(IgniteClient c, String register) {
        // 1. Register
        exec(c, "SELECT OBJECTID, CURRENCY, CCBALANCERECALCDATE FROM REGISTER WHERE OBJECTID=?",
                register);
        // 2. DayBalances range (только чтение, без агрегации)
        exec(c, "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCDTSUM, CCKTSUM, CCFINISHSUM " +
                "FROM DAYBALANCES WHERE REGISTER=? AND CCOPERATIONDAY BETWEEN ? AND ?",
                register, FROM_30D, TO_TODAY);
        // 3. SUM из TURNDOCCUR — тяжёлый запрос
        exec(c, "SELECT" +
                "  COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUM ELSE CCSUM END), 0)," +
                "  COALESCE(SUM(CASE WHEN CCDT='1' THEN -1*CCSUMNAT ELSE CCSUMNAT END), 0) " +
                "FROM TURNDOCCUR WHERE REGISTER=? AND CCTYPEOPER IN (0,40) " +
                "AND CCOPERATIONDAY BETWEEN ? AND ?",
                register, FROM_30D, TO_TODAY);
        // 4. Type-50 lookup
        exec(c, "SELECT CCSTARTSUM, CCSTARTSUMNAT, CCOPERATIONDAY FROM TURNDOCCUR " +
                "WHERE REGISTER=? AND CCTYPEOPER=50 AND CCOPERATIONDAY BETWEEN ? AND ?",
                register, FROM_30D, TO_TODAY);
        // 5. Last operations list
        exec(c, "SELECT OBJECTID, CCRQUID, CCSUM, CCOPERATIONDAY, CCDT FROM TURNDOCCUR " +
                "WHERE REGISTER=? AND CCOPERATIONDAY BETWEEN ? AND ? " +
                "ORDER BY CCOPERATIONDAY DESC LIMIT 100",
                register, FROM_30D, TO_TODAY);
    }

    /**
     * Summary через DAYBALANCES — pre-aggregated вариант.
     * Использует уже посчитанные суммы из DAY_BALANCES вместо агрегации turn_doc_cur.
     */
    private void doSummaryFromDayBalances(IgniteClient c, String register) {
        // 1. Register
        exec(c, "SELECT OBJECTID, CURRENCY, CCBALANCERECALCDATE FROM REGISTER WHERE OBJECTID=?",
                register);
        // 2. DayBalances range — содержит уже агрегированные суммы по дням
        exec(c, "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCDTSUM, CCKTSUM, CCFINISHSUM " +
                "FROM DAYBALANCES WHERE REGISTER=? AND CCOPERATIONDAY BETWEEN ? AND ?",
                register, FROM_30D, TO_TODAY);
        // 3. Aggregated balance directly из DAYBALANCES — без TURNDOCCUR scan
        exec(c, "SELECT SUM(CCDTSUM), SUM(CCKTSUM), MAX(CCFINISHSUM) " +
                "FROM DAYBALANCES WHERE REGISTER=? AND CCOPERATIONDAY BETWEEN ? AND ?",
                register, FROM_30D, TO_TODAY);
        // 4. Type-50 lookup — всё ещё нужен из TURNDOCCUR (старт-балансы)
        exec(c, "SELECT CCSTARTSUM, CCSTARTSUMNAT, CCOPERATIONDAY FROM TURNDOCCUR " +
                "WHERE REGISTER=? AND CCTYPEOPER=50 AND CCOPERATIONDAY BETWEEN ? AND ?",
                register, FROM_30D, TO_TODAY);
        // 5. (опционально) last operations — этот шаг убран в DAYBALANCES-варианте,
        //    detail-список запрашивается клиентом отдельно when needed.
    }

    private void exec(IgniteClient c, String sql, Object... args) {
        try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
            for (var ignored : cur) {}
        }
    }

    private Map<String, Object> bench(IgniteClient c, int iters, String name, Runnable body) {
        // warmup
        try { body.run(); }
        catch (Exception e) { return Map.of("error", e.toString()); }

        long[] timings = new long[iters];
        for (int i = 0; i < iters; i++) {
            long t = System.nanoTime();
            try { body.run(); }
            catch (Exception e) { return Map.of("error", e.toString()); }
            timings[i] = System.nanoTime() - t;
        }
        Arrays.sort(timings);
        long sum = 0; for (long t : timings) sum += t;
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("avgMs", round(sum / (double) iters / 1_000_000.0));
        stats.put("p50Ms", round(timings[iters / 2] / 1_000_000.0));
        stats.put("p95Ms", round(timings[Math.min(iters - 1, (int) (iters * 0.95))] / 1_000_000.0));
        stats.put("p99Ms", round(timings[Math.min(iters - 1, (int) (iters * 0.99))] / 1_000_000.0));
        stats.put("maxMs", round(timings[iters - 1] / 1_000_000.0));
        return stats;
    }

    private static double round(double v) { return Math.round(v * 100.0) / 100.0; }
}
