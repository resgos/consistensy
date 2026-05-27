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
 * Дополнительные perf-сценарии не покрытые в RealFiltersPerfController:
 *
 *   POST /api/perf/extra?N=10&iterations=20
 *
 * Покрывает:
 *   G. F1 (registerId IN) vs F2 (accNum+ucpId tuple IN) — accBy paths
 *   H. flagZeroTurns=false полный путь (C1+B2+E2) — для A/B vs true (B1+E1)
 *   I. noPeriod=true — full register history (без BETWEEN)
 *   J. pageSize=50, offset=10000 — стоимость in-memory skip (теоретическая)
 *   K. CB rates (G1 query) — single-day lookup
 */
@Slf4j
@RestController
@RequestMapping("/api/perf")
@RequiredArgsConstructor
public class ExtraFiltersPerfController {

    private final IgniteClientFactory clientFactory;

    private static final LocalDate TODAY = LocalDate.of(2026, 5, 24);
    private static final Date DAY_30D_AGO = Date.valueOf(TODAY.minusDays(30));
    private static final Date DAY_TODAY = Date.valueOf(TODAY);

    @PostMapping("/extra")
    public Map<String, Object> extra(
            @RequestParam(defaultValue = "10") int N,
            @RequestParam(defaultValue = "20") int iterations,
            @RequestParam(defaultValue = "cluster-1") String clusterId) {

        IgniteClient c = clientFactory.get(clusterId);
        if (c == null) return Map.of("ok", false, "error", "cluster not connected: " + clusterId);

        List<String> regList = IntStream.rangeClosed(1, N)
                .mapToObj(i -> String.format("R%03d", i))
                .collect(Collectors.toList());
        String inPlaceholders = regList.stream().map(a -> "?").collect(Collectors.joining(","));

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("_meta", Map.of("N", N, "iterations", iterations));

        // =====================================================================
        // G. F1 vs F2 — register lookup paths
        // =====================================================================

        // G1 = F1 — registerId IN
        r.put("G1_F1_registerId_IN", bench(c, iterations,
                "SELECT OBJECTID, CCOPENDATE, CCCLOSEDATE, CURRENCY, CCBALANCERECALCDATE " +
                        "FROM REGISTER WHERE OBJECTID IN (" + inPlaceholders + ")",
                regList.toArray()));

        // G2 = F2 — accNum+ucpId tuple IN
        // Эмулируем структуру: 2-полевой tuple IN. У нас в seed нет CCACCNUM,
        // но REGISTER+CURRENCY имеют структурно ту же форму tuple-IN.
        StringBuilder tuplePh = new StringBuilder();
        List<Object> tupleArgs = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            if (tuplePh.length() > 0) tuplePh.append(",");
            tuplePh.append("(?,?)");
            tupleArgs.add(regList.get(i));
            tupleArgs.add(i % 5 == 0 ? "USD" : "RUB");
        }
        r.put("G2_F2_accNum_ucpId_tuple", bench(c, iterations,
                "SELECT OBJECTID, CCOPENDATE, CCCLOSEDATE, CURRENCY, CCBALANCERECALCDATE " +
                        "FROM REGISTER WHERE (OBJECTID, CURRENCY) IN (" + tuplePh + ")",
                tupleArgs.toArray()));

        // =====================================================================
        // H. flagZeroTurns=true vs false — критичный A/B
        // =====================================================================

        Object[] periodArgs = concat(regList.toArray(), DAY_30D_AGO, DAY_TODAY);

        // H1 = B1 (TYPE IN (0,40)) — flagZeroTurns=true path
        r.put("H1_flagTrue_B1_agg", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCDT, " +
                        "SUM(COALESCE(CCSUM, 0)), SUM(COALESCE(CCSUMNAT, 0)), COUNT(*) " +
                        "FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") AND CCTYPEOPER IN (0, 40) " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ? " +
                        "GROUP BY REGISTER, CCOPERATIONDAY, CCDT", periodArgs));

        // H2 = C1 + B2 — flagZeroTurns=false path (2 SQL!)
        long h2t0 = System.nanoTime();
        Map<String, Object> c1 = bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ? " +
                        "AND CCTYPEOPER != 50 " +
                        "GROUP BY REGISTER, CCOPERATIONDAY", periodArgs);
        Map<String, Object> b2 = bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCDT, " +
                        "SUM(COALESCE(CCSUM, 0)), SUM(COALESCE(CCSUMNAT, 0)), COUNT(*) " +
                        "FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") AND CCTYPEOPER != 50 " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ? " +
                        "GROUP BY REGISTER, CCOPERATIONDAY, CCDT", periodArgs);
        r.put("H2a_flagFalse_C1_distinct", c1);
        r.put("H2b_flagFalse_B2_agg", b2);
        r.put("H2_flagFalse_total_estimate_ms",
                round(((Number) c1.get("avgMs")).doubleValue() + ((Number) b2.get("avgMs")).doubleValue()));

        // =====================================================================
        // I. noPeriod=true — без BETWEEN (вся история регистра)
        // =====================================================================

        // I1. B1 без BETWEEN
        r.put("I1_noPeriod_B1_agg", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCDT, " +
                        "SUM(COALESCE(CCSUM, 0)), SUM(COALESCE(CCSUMNAT, 0)), COUNT(*) " +
                        "FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") AND CCTYPEOPER IN (0, 40) " +
                        "GROUP BY REGISTER, CCOPERATIONDAY, CCDT", regList.toArray()));

        // I2. A1 без BETWEEN (все type50)
        r.put("I2_noPeriod_A1_oper50", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCSTARTSUMNAT " +
                        "FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") AND CCTYPEOPER=50 " +
                        "ORDER BY REGISTER, CCOPERATIONDAY ASC", regList.toArray()));

        // I3. E1 без BETWEEN (все daybalances)
        r.put("I3_noPeriod_E1_daybalances", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCSTARTSUM, CCSTARTSUMNAT, " +
                        "CCDTSUM, CCDTSUMNAT, CCKTSUM, CCKTSUMNAT, " +
                        "CCFINISHSUM, CCFINISHSUMNAT " +
                        "FROM DAYBALANCES WHERE REGISTER IN (" + inPlaceholders + ")",
                regList.toArray()));

        // =====================================================================
        // J. Pagination cost (теоретическая — без SQL, чисто in-memory)
        // =====================================================================

        // J1. ORDER BY DESC LIMIT 50 OFFSET 10000 — что было бы если pagination
        // эмитилось в SQL. Сейчас НЕТ в production-коде (in-memory).
        // На небольшом dataset вернёт 0 строк (всего 2700 на N=100), но shape
        // покажет cost для случая когда result-set большой.
        r.put("J1_sql_offset_10000", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCDT, CCSUM FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") AND CCTYPEOPER != 50 " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ? " +
                        "ORDER BY CCOPERATIONDAY DESC LIMIT 50 OFFSET 10000", periodArgs));

        // J2. ORDER BY DESC LIMIT 50 OFFSET 0 — первая страница
        r.put("J2_sql_offset_0", bench(c, iterations,
                "SELECT REGISTER, CCOPERATIONDAY, CCDT, CCSUM FROM TURNDOCCUR " +
                        "WHERE REGISTER IN (" + inPlaceholders + ") AND CCTYPEOPER != 50 " +
                        "AND CCOPERATIONDAY >= ? AND CCOPERATIONDAY <= ? " +
                        "ORDER BY CCOPERATIONDAY DESC LIMIT 50 OFFSET 0", periodArgs));

        // =====================================================================
        // K. CB rates — single-day lookup per AccBalance row
        // =====================================================================

        // K1. CB_RATE lookup (если в seed создана таблица). Тут просто эмулируем
        // через REGISTER lookup т.к. CB_RATE отсутствует в тестовом seed.
        r.put("K1_cb_rate_emulated_lookup", bench(c, iterations,
                "SELECT CURRENCY FROM REGISTER WHERE OBJECTID = ? LIMIT 1",
                regList.get(0)));

        return r;
    }

    private static Object[] concat(Object[] arr, Object... tail) {
        Object[] out = new Object[arr.length + tail.length];
        System.arraycopy(arr, 0, out, 0, arr.length);
        System.arraycopy(tail, 0, out, arr.length, tail.length);
        return out;
    }

    private Map<String, Object> bench(IgniteClient c, int iters, String sql, Object... args) {
        try {
            try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
                for (var ignored : cur) {}
            }
        } catch (Exception e) {
            return Map.of("error", e.toString());
        }

        long[] timings = new long[iters];
        int rowsLast = 0;
        for (int i = 0; i < iters; i++) {
            long t = System.nanoTime();
            int rows = 0;
            try (var cur = c.query(new SqlFieldsQuery(sql).setArgs(args))) {
                for (var ignored : cur) rows++;
            } catch (Exception e) {
                return Map.of("error", e.toString());
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
