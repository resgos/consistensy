package ru.sbrf.pprb.stmnt.consistency.rpc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.springframework.web.bind.annotation.*;
import ru.sbrf.pprb.stmnt.consistency.integration.ignite.IgniteClientFactory;

import java.sql.Date;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Perf-замер по фильтрам, которые РЕАЛЬНО эмитятся production-кодом
 * (GetStatementSummaryLibraryIgnite + DayBalancesRecalcService).
 *
 *   POST /api/perf/real-filters?N=10&iterations=20
 *
 * 13 query-shape'ов, сгруппированных по источнику:
 *   A. TURNDOCCUR oper50 (loadOper50ContextBatch, SQL_FIND_TYPE50)
 *   B. TURNDOCCUR turnover-agg (queryDayTurnoversBatch, querySumBetween)
 *   C. TURNDOCCUR distinct days (getNonZeroDaysBatch)
 *   D. TURNDOCCUR prevOperDates (executePrevOperDates)
 *   E. DAYBALANCES range (readDayBalancesBatch / readDayBalancesNonZeroBatch)
 *   F. REGISTER lookup (getRegisterMap)
 */
@Slf4j
@RestController
@RequestMapping("/api/perf")
@RequiredArgsConstructor
public class RealFiltersPerfController {

    private final IgniteClientFactory clientFactory;

    private static final LocalDate TODAY = LocalDate.of(2026, 5, 24);
    private static final Date DAY_30D_AGO = Date.valueOf(TODAY.minusDays(30));
    private static final Date DAY_TODAY = Date.valueOf(TODAY);

    @PostMapping("/real-filters")
    public Map<String, Object> realFilters(
            @RequestParam(defaultValue = "10") int N,
            @RequestParam(defaultValue = "20") int iterations,
            @RequestParam(defaultValue = "cluster-1") String clusterId) {

        IgniteClient c = clientFactory.get(clusterId);
        if (c == null) return Map.of("ok", false, "error", "cluster not connected: " + clusterId);

        // N регистров: R001..R{N}
        List<String> regList = IntStream.rangeClosed(1, N)
                .mapToObj(i -> String.format("R%03d", i))
                .collect(Collectors.toList());
        String inPlaceholders = regList.stream().map(a -> "?").collect(Collectors.joining(","));
        String firstReg = regList.get(0);

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("_meta", Map.of("N", N, "iterations", iterations, "registers", regList));

        // =====================================================================
        // A. TURNDOCCUR oper50 — loadOper50ContextBatch / SQL_FIND_TYPE50
        // =====================================================================

        // A1. Period type50 в [fromDate, toDate]
        Object[] a1Args = concat(regList.toArray(), DAY_30D_AGO, DAY_TODAY);
        r.put("A1_oper50_period", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCSTARTSUMNAT " +
                        "FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") AND CCTYPEOPER=50 " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ? " +
                        "ORDER BY REGISTER, CCOPERATIONDAY ASC",
                a1Args));

        // A2. MAX(date) до fromDate с GROUP BY REGISTER
        Object[] a2Args = concat(regList.toArray(), DAY_30D_AGO);
        r.put("A2_oper50_max_before", bench(c, iterations,
                "SELECT REGISTER, MAX(CCOPERATIONDAY) " +
                        "FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") AND CCTYPEOPER=50 " +
                        "AND CCOPERATIONDAY < ? " +
                        "GROUP BY REGISTER", a2Args));

        // A3. Tuple (register, day) IN ((?,?), ...) — точечные чтения
        StringBuilder tuplePh = new StringBuilder();
        List<Object> tupleArgs = new ArrayList<>();
        for (String reg : regList) {
            if (tuplePh.length() > 0) tuplePh.append(",");
            tuplePh.append("(?,?)");
            tupleArgs.add(reg);
            tupleArgs.add(DAY_30D_AGO);
        }
        r.put("A3_oper50_tuples", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCSTARTSUMNAT " +
                        "FROM TURNDOCCUR WHERE CCTYPEOPER=50 " +
                        "AND (REGISTER, CCOPERATIONDAY) IN (" + tuplePh + ")",
                tupleArgs.toArray()));

        // A4. SQL_FIND_TYPE50 — точечный lookup на один регистр (DayBalancesRecalc)
        r.put("A4_find_type50_single", bench(c, iterations,
                "SELECT OBJECTID, REGISTER, CCSTARTSUM, CCSTARTSUMNAT " +
                        "FROM TURNDOCCUR WHERE REGISTER=? AND CCTYPEOPER=50 AND CCOPERATIONDAY=? LIMIT 1",
                firstReg, DAY_30D_AGO));

        // =====================================================================
        // B. TURNDOCCUR turnover-agg — queryDayTurnoversBatch / querySumBetween
        // =====================================================================

        // B1. Aggregation TYPE IN (0,40) для batch регистров
        Object[] b1Args = concat(regList.toArray(), DAY_30D_AGO, DAY_TODAY);
        r.put("B1_turnover_agg_type_in", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCDT, " +
                        "SUM(COALESCE(CCSUM, 0)), SUM(COALESCE(CCSUMNAT, 0)), COUNT(*) " +
                        "FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") AND CCTYPEOPER IN (0, 40) " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ? " +
                        "GROUP BY REGISTER, CCOPERATIONDAY, CCDT", b1Args));

        // B2. Aggregation TYPE != 50 для batch регистров
        r.put("B2_turnover_agg_type_neq", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCDT, " +
                        "SUM(COALESCE(CCSUM, 0)), SUM(COALESCE(CCSUMNAT, 0)), COUNT(*) " +
                        "FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") AND CCTYPEOPER != 50 " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ? " +
                        "GROUP BY REGISTER, CCOPERATIONDAY, CCDT", b1Args));

        // B3. querySumBetween — single register, CASE WHEN
        r.put("B3_sum_between_single_reg", bench(c, iterations,
                "SELECT COALESCE(SUM(CASE WHEN CCDT = '1' THEN -1 * CCSUM ELSE CCSUM END), 0), " +
                        "COALESCE(SUM(CASE WHEN CCDT = '1' THEN -1 * CCSUMNAT ELSE CCSUMNAT END), 0) " +
                        "FROM TURNDOCCUR WHERE REGISTER=? AND CCTYPEOPER != 50 " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY < ?",
                firstReg, DAY_30D_AGO, DAY_TODAY));

        // =====================================================================
        // C. TURNDOCCUR distinct days — getNonZeroDaysBatch
        // =====================================================================

        r.put("C1_distinct_days", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ? " +
                        "AND CCTYPEOPER != 50 " +
                        "GROUP BY REGISTER, CCOPERATIONDAY", b1Args));

        // =====================================================================
        // D. TURNDOCCUR PrevOperDates (executePrevOperDates)
        // =====================================================================

        // D1. ORDER BY DESC LIMIT 1 — последняя дата до периода
        r.put("D1_prev_oper_date_before", bench(c, iterations,
                "SELECT CCDATE FROM TURNDOCCUR " +
                        "WHERE REGISTER=? AND CCOPERATIONDAY < ? AND CCTYPEOPER != 50 " +
                        "ORDER BY CCDATE DESC LIMIT 1",
                firstReg, DAY_30D_AGO));

        // D2. MAX(CCDATE) GROUP BY CCOPERATIONDAY в периоде
        r.put("D2_prev_oper_date_in_period", bench(c, iterations,
                "SELECT CCOPERATIONDAY, MAX(CCDATE) FROM TURNDOCCUR " +
                        "WHERE REGISTER=? AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY < ? AND CCTYPEOPER != 50 " +
                        "GROUP BY CCOPERATIONDAY",
                firstReg, DAY_30D_AGO, DAY_TODAY));

        // =====================================================================
        // E. DAYBALANCES — readDayBalancesBatch
        // =====================================================================

        // E1. Plain range read
        // NB: в production используется CCBALANCEDATE, в seed — CCOPERATIONDAY (одно и то же поле).
        Object[] e1Args = concat(regList.toArray(), DAY_30D_AGO, DAY_TODAY);
        r.put("E1_daybalances_range", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCSTARTSUMNAT, " +
                        "CCDTSUM, CCDTSUMNAT, CCKTSUM, CCKTSUMNAT, " +
                        "CCFINISHSUM, CCFINISHSUMNAT " +
                        "FROM DAYBALANCES " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ?", e1Args));

        // E2. NonZero filter
        r.put("E2_daybalances_nonzero", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCSTARTSUMNAT, " +
                        "CCDTSUM, CCDTSUMNAT, CCKTSUM, CCKTSUMNAT, " +
                        "CCFINISHSUM, CCFINISHSUMNAT " +
                        "FROM DAYBALANCES " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ? " +
                        "AND (CCDTSUM != 0 OR CCKTSUM != 0 OR CCDTSUMNAT != 0 OR CCKTSUMNAT != 0)",
                e1Args));

        // =====================================================================
        // F. REGISTER lookup — getRegisterMap (без JOIN, тк в seed нет CLIENT/CURRENCY)
        // =====================================================================

        r.put("F1_register_by_id_in", bench(c, iterations,
                "SELECT OBJECTID, CCOPENDATE, CCCLOSEDATE, CURRENCY, CCBALANCERECALCDATE " +
                        "FROM REGISTER WHERE OBJECTID IN (" + inPlaceholders + ")",
                regList.toArray()));

        return r;
    }

    // =====================================================================

    private static Object[] concat(Object[] arr, Object... tail) {
        Object[] out = new Object[arr.length + tail.length];
        System.arraycopy(arr, 0, out, 0, arr.length);
        System.arraycopy(tail, 0, out, arr.length, tail.length);
        return out;
    }

    private Map<String, Object> bench(IgniteClient c, int iters, String sql, Object... args) {
        // warmup
        try {
            try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
                for (var ignored : cur) {}
            }
        } catch (Exception e) {
            return Map.of("error", e.toString(), "sql", sql);
        }

        long[] timings = new long[iters];
        int rowsLast = 0;
        for (int i = 0; i < iters; i++) {
            long t = System.nanoTime();
            int rows = 0;
            try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
                for (var ignored : cur) rows++;
            } catch (Exception e) {
                return Map.of("error", e.toString(), "sql", sql);
            }
            timings[i] = System.nanoTime() - t;
            rowsLast = rows;
        }
        Arrays.sort(timings);
        long sum = 0;
        for (long t : timings) sum += t;
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("rows", rowsLast);
        r.put("avgMs", round(sum / (double) iters / 1_000_000.0));
        r.put("p50Ms", round(timings[iters / 2] / 1_000_000.0));
        r.put("p95Ms", round(timings[Math.min(iters - 1, (int) (iters * 0.95))] / 1_000_000.0));
        r.put("p99Ms", round(timings[Math.min(iters - 1, (int) (iters * 0.99))] / 1_000_000.0));
        r.put("maxMs", round(timings[iters - 1] / 1_000_000.0));
        return r;
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
